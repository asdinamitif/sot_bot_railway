import logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List

import pandas as pd
import json
import requests
from io import BytesIO

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

# ----------------- ENV -----------------
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))
ANALYTICS_PASSWORD = "051995"

GOOGLE_SHEET_URL_DEFAULT = (
    "https://docs.google.com/spreadsheets/d/"
    "1FlhN7grvku5tSj2SAreEHxHC55K9E7N91r8eWOkzOFY/edit?usp=sharing"
)

# Google Sheets
GSHEETS_SERVICE_ACCOUNT_JSON = os.getenv("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()
GSHEETS_SPREADSHEET_ID = os.getenv(
    "GSHEETS_SPREADSHEET_ID",
    "1FlhN7grvku5tSj2SAreEHxHC55K9E7N91r8eWOkzOFY",
).strip()

SHEETS_SERVICE = None  # кеш клиента Google Sheets

DEFAULT_APPROVERS = [
    "@asdinamitif",
    "@FrolovAlNGSN",
    "@cappit_G59",
    "@sergeybektiashkin",
    "@scri4",
    "@Kirill_Victorovi4",
]

RESPONSIBLE_USERNAMES = {
    "бектяшкин": ["sergeybektiashkin"],
    "смирнов": ["scri4"],
}

INSPECTOR_SHEET_NAME = "ПБ, АР,ММГН, АГО (2025)"
HARD_CODED_ADMINS = {398960707}

SCHEDULE_NOTIFY_CHAT_ID_ENV = os.getenv("SCHEDULE_NOTIFY_CHAT_ID", "").strip()


def is_admin(uid: int) -> bool:
    return uid in HARD_CODED_ADMINS


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


def get_current_remarks_sheet_name() -> str:
    year = local_now().year
    return f"ПБ, АР,ММГН, АГО ({year})"


# ----------------- Google Sheets helpers -----------------


def get_sheets_service():
    """
    Возвращает объект сервиса Google Sheets (кешируется в SHEETS_SERVICE).
    Используется для раздела «График» и записи инспектора.
    """
    global SHEETS_SERVICE

    if SHEETS_SERVICE is not None:
        return SHEETS_SERVICE

    if not GSHEETS_SERVICE_ACCOUNT_JSON:
        log.error("GSHEETS_SERVICE_ACCOUNT_JSON не задан – Google Sheets API недоступен.")
        return None

    try:
        info = json.loads(GSHEETS_SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        service = build("sheets", "v4", credentials=creds)
        SHEETS_SERVICE = service
        return service
    except Exception as e:
        log.error("Ошибка создания клиента Google Sheets: %s", e)
        return None


def build_export_url(spreadsheet_id: str) -> str:
    """
    Ссылка на экспорт Google Sheets в .xlsx по ID таблицы.
    """
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"


def detect_header_row(values: List[List[str]]) -> int:
    """
    Пытается найти строку заголовков по наличию слова 'дата выезда'.
    Если не находит — возвращает 0.
    """
    for i, row in enumerate(values[:30]):
        row_lower = [str(c).lower() for c in row]
        if any("дата выезда" in c for c in row_lower):
            return i
    return 0


def read_sheet_to_dataframe(
    sheet_id: str, sheet_name: str, header_row_index: Optional[int] = None
) -> Optional[pd.DataFrame]:
    """
    Считывает данные с указанного листа Google Sheets в DataFrame.
    Если header_row_index не задан, пытается найти строку заголовков автоматически.
    """
    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets сервис недоступен – невозможно прочитать лист.")
        return None

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{sheet_name}'!A1:ZZZ1000",
        ).execute()
        values = result.get("values", [])

        if not values:
            log.warning("Лист '%s' пуст.", sheet_name)
            return pd.DataFrame()

        if header_row_index is None:
            header_row_index = detect_header_row(values)

        headers = values[header_row_index]
        data_rows = values[header_row_index + 1 :]

        df = pd.DataFrame(data_rows, columns=headers)
        df = df.dropna(how="all").reset_index(drop=True)
        return df
    except Exception as e:
        log.error("Ошибка чтения листа '%s' из Google Sheets: %s", sheet_name, e)
        return None


# ----------------- Вспомогательные функции -----------------


def find_col(df: pd.DataFrame, hints) -> Optional[str]:
    if isinstance(hints, str):
        hints = [hints]
    hints = [h.lower() for h in hints]
    for col in df.columns:
        low = str(col).lower()
        if any(h in low for h in hints):
            return col
    return None


def excel_col_to_index(col: str) -> int:
    col = col.upper().strip()
    idx = 0
    for ch in col:
        if "A" <= ch <= "Z":
            idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_col_by_letter(df: pd.DataFrame, letters: str) -> Optional[str]:
    idx = excel_col_to_index(letters)
    if 0 <= idx < len(df.columns):
        return df.columns[idx]
    return None


# ----------------- Инспектор: запись в Google Sheets -----------------


def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    """
    Записываем новую строку в Google Sheet (лист INSPECTOR_SHEET_NAME):

    B – Дата выезда
    C – Дата начала итоговой проверки
    D – Площадь / Этажность (одной ячейкой)
    E – ОНзС
    F – Наименование застройщика
    G – Наименование объекта
    H – Строительный адрес
    I – Номер дела
    J – Вид проверки
    """
    service = get_sheets_service()
    if service is None or not GSHEETS_SPREADSHEET_ID:
        log.error("Google Sheets сервис недоступен – некуда писать выезд.")
        return False

    # Даты
    date_dep = form.get("date_departure")
    if isinstance(date_dep, datetime):
        dep_str = date_dep.strftime("%d.%m.%Y")
    elif isinstance(date_dep, date):
        dep_str = date_dep.strftime("%d.%m.%Y")
    else:
        dep_str = str(date_dep or "")

    date_fin = form.get("date_final")
    if isinstance(date_fin, datetime):
        fin_str = date_fin.strftime("%d.%m.%Y")
    elif isinstance(date_fin, date):
        fin_str = date_fin.strftime("%d.%m.%Y")
    else:
        fin_str = str(date_fin or "")

    area = form.get("area") or ""
    floors = form.get("floors") or ""
    d_cell = f"Площадь (кв.м): {area}\nКоличество этажей: {floors}"

    onzs = form.get("onzs") or ""
    developer = form.get("developer") or ""
    obj_name = form.get("object") or ""
    address = form.get("address") or ""
    case_no = form.get("case_no") or ""
    check_type = form.get("check_type") or ""

    values = [[
        dep_str,    # B
        fin_str,    # C
        d_cell,     # D
        onzs,       # E
        developer,  # F
        obj_name,   # G
        address,    # H
        case_no,    # I
        check_type  # J
    ]]

    body = {"values": values}

    try:
        service.spreadsheets().values().append(
            spreadsheetId=GSHEETS_SPREADSHEET_ID,
            range=f"'{INSPECTOR_SHEET_NAME}'!B:J",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
        log.info("Инспектор: строка успешно добавлена в Google Sheet.")
        return True
    except Exception as e:
        log.error("Ошибка записи в Google Sheet (Инспектор): %s", e)
        return False


# ----------------- БАЗА ДАННЫХ -----------------


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    c = conn.cursor()

    c.execute(
        """CREATE TABLE IF NOT EXISTS approvals (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               user_id INTEGER,
               username TEXT,
               approver TEXT,
               decision TEXT,
               comment TEXT,
               decided_at TEXT,
               schedule_version INTEGER
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS schedule_settings (
               key TEXT PRIMARY KEY,
               value TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS approvers (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               label TEXT UNIQUE
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS remarks_status (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               excel_row INTEGER,
               pb_status TEXT,
               pbzk_status TEXT,
               ar_status TEXT,
               updated_by INTEGER,
               updated_at TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS attachments (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               excel_row INTEGER,
               file_id TEXT,
               file_name TEXT,
               uploaded_by INTEGER,
               uploaded_at TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS users (
               user_id INTEGER PRIMARY KEY,
               username TEXT,
               first_seen_at TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS admins (
               user_id INTEGER PRIMARY KEY,
               username TEXT,
               first_seen_at TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS schedule_files (
               version INTEGER PRIMARY KEY,
               name TEXT,
               uploaded_at TEXT
           )"""
    )

    # approvers
    c.execute("SELECT COUNT(*) AS c FROM approvers")
    if c.fetchone()["c"] == 0:
        c.executemany(
            "INSERT OR IGNORE INTO approvers (label) VALUES (?)",
            [(lbl,) for lbl in DEFAULT_APPROVERS],
        )

    # schedule_version
    c.execute("SELECT value FROM schedule_settings WHERE key='schedule_version'")
    row_ver = c.fetchone()
    if not row_ver:
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) "
            "VALUES ('schedule_version', '1')"
        )

    # last_notified_version
    c.execute("SELECT value FROM schedule_settings WHERE key='last_notified_version'")
    row_ln = c.fetchone()
    if not row_ln:
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) "
            "VALUES ('last_notified_version', '0')"
        )

    # группа уведомлений из ENV
    if SCHEDULE_NOTIFY_CHAT_ID_ENV:
        c.execute(
            "INSERT OR IGNORE INTO schedule_settings (key, value) "
            "VALUES ('schedule_notify_chat_id', ?)",
            (SCHEDULE_NOTIFY_CHAT_ID_ENV,),
        )

    conn.commit()
    conn.close()


def get_schedule_state() -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT key, value FROM schedule_settings")
    rows = c.fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


def get_schedule_version(settings: dict) -> int:
    try:
        return int(settings.get("schedule_version") or "1")
    except Exception:
        return 1


def get_current_approvers(settings: dict) -> List[str]:
    val = settings.get("current_approvers")
    if val:
        items = [v.strip() for v in val.split(",") if v.strip()]
        if items:
            return items

    val2 = settings.get("current_approver")
    if val2:
        return [val2]

    return []


def get_schedule_notify_chat_id(settings: dict) -> Optional[int]:
    val = settings.get("schedule_notify_chat_id")
    if not val:
        return None
    try:
        return int(val)
    except Exception:
        return None


def set_schedule_file_name(version: int, name: str) -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO schedule_files (version, name, uploaded_at) "
        "VALUES (?, ?, ?)",
        (version, name, local_now().isoformat()),
    )
    conn.commit()
    conn.close()


def get_schedule_file_names() -> Dict[int, str]:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT version, name FROM schedule_files")
    rows = c.fetchall()
    conn.close()
    res: Dict[int, str] = {}
    for r in rows:
        try:
            v = int(r["version"])
        except Exception:
            continue
        res[v] = r["name"]
    return res


def get_schedule_name_for_version(version: int) -> str:
    names = get_schedule_file_names()
    name = names.get(version)
    if name:
        return name
    return f"Версия {version}"


# ----------------- Клавиатуры -----------------


def main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📅 График", "📊 Итоговая"],
        ["📝 Замечания", "🏗 ОНзС"],
        ["Инспектор", "📈 Аналитика"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def build_schedule_inline(is_admin_flag: bool, settings: dict) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("🔄 Обновить", callback_data="schedule_refresh"),
            InlineKeyboardButton("📥 Скачать", callback_data="schedule_download"),
        ]
    ]

    if is_admin_flag:
        buttons.append(
            [
                InlineKeyboardButton("📤 Загрузить", callback_data="schedule_upload"),
                InlineKeyboardButton("👥 Согласующие", callback_data="schedule_approvers"),
            ]
        )
    else:
        buttons.append(
            [
                InlineKeyboardButton("📤 Загрузить", callback_data="schedule_upload"),
            ]
        )

    return InlineKeyboardMarkup(buttons)


def remarks_menu_inline() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done"),
        ],
        [
            InlineKeyboardButton("📥 Скачать файл", callback_data="remarks_download"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def onzs_menu_inline() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("Показать ОНзС по делу", callback_data="onzs_by_case"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def inspector_menu_inline() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("➕ Добавить выезд", callback_data="inspector_add"),
        ]
    ]
    return InlineKeyboardMarkup(buttons)


# ----------------- Тексты -----------------


def build_schedule_text(is_admin_flag: bool, settings: dict) -> str:
    version = get_schedule_version(settings)
    file_names = get_schedule_file_names()
    name = get_schedule_name_for_version(version)
    approvers = get_current_approvers(settings)

    last_notified_version = int(settings.get("last_notified_version", "0"))
    notify_chat_id = get_schedule_notify_chat_id(settings)

    lines = [
        f"📅 График выездов (версия {version})",
        f"Файл: {name}",
    ]
    if approvers:
        lines.append("Согласующие:")
        for a in approvers:
            lines.append(f"• {a}")
    else:
        lines.append("Согласующие не назначены.")

    if notify_chat_id:
        lines.append(f"\nУведомления отправляются в чат: {notify_chat_id}")
        lines.append(f"Последняя уведомлённая версия: {last_notified_version}")
    else:
        lines.append("\nГруппа для уведомлений по графику не настроена.")

    if is_admin_flag:
        lines.append("\nВы администратор. Вам доступны загрузка файла и настройка согласующих.")
    else:
        lines.append("\nВы можете просмотреть актуальный график и скачать файл.")

    return "\n".join(lines)


def build_remarks_not_done_text(df: pd.DataFrame) -> str:
    """
    Строит текст по строкам, где Q/R/Y/AE == 'нет'
    Группировка по номеру дела.
    """
    df_copy = df.copy()

    col_case = find_col(df_copy, ["дело", "номер дела", "номер_дела", "номер дела (номер объекта)"])
    if not col_case:
        col_case = get_col_by_letter(df_copy, "I")

    col_pb = get_col_by_letter(df_copy, "Q")
    col_ar = get_col_by_letter(df_copy, "R")
    col_mmr = get_col_by_letter(df_copy, "Y")
    col_ago = get_col_by_letter(df_copy, "AE")

    col_pb_cat = get_col_by_letter(df_copy, "K")
    col_ar_cat = get_col_by_letter(df_copy, "L")
    col_mmr_cat = get_col_by_letter(df_copy, "M")
    col_ago_cat = get_col_by_letter(df_copy, "N")

    col_pb = col_pb or (col_pb_cat if col_pb_cat in df_copy.columns else None)
    col_ar = col_ar or (col_ar_cat if col_ar_cat in df_copy.columns else None)
    col_mmr = col_mmr or (col_mmr_cat if col_mmr_cat in df_copy.columns else None)
    col_ago = col_ago or (col_ago_cat if col_ago_cat in df_copy.columns else None)

    if not col_case:
        return "Не удалось определить колонку с номером дела (I)."

    has_no = []
    for _, row in df_copy.iterrows():
        case_val = str(row.get(col_case, "")).strip()
        if not case_val:
            continue

        blocks = []

        if col_pb and str(row.get(col_pb, "")).strip().lower() == "нет":
            blocks.append("Пожарная безопасность")

        if col_ar and str(row.get(col_ar, "")).strip().lower() == "нет":
            blocks.append("Архитектура")

        if col_mmr and str(row.get(col_mmr, "")).strip().lower() == "нет":
            blocks.append("ММГН")

        if col_ago and str(row.get(col_ago, "")).strip().lower() == "нет":
            blocks.append("АГО")

        if blocks:
            has_no.append((case_val, blocks))

    if not has_no:
        return "Во всех строках статусы устранения не содержат «нет»."

    grouped: Dict[str, List[str]] = {}
    for case_no, blocks in has_no:
        grouped.setdefault(case_no, [])
        for b in blocks:
            if b not in grouped[case_no]:
                grouped[case_no].append(b)

    lines = [
        "Строки со статусом «НЕ УСТРАНЕНЫ (нет)»",
        f"Лист: «{get_current_remarks_sheet_name()}»",
        "",
    ]
    for case_no, blocks in grouped.items():
        lines.append(f"• {case_no} — " + "; ".join(blocks))

    return "\n".join(lines)


def build_onzs_text_for_case(df: pd.DataFrame, case_no: str) -> str:
    """
    Строит текст по ОНзС для заданного номера дела.
    """
    col_case = find_col(df, ["дело", "номер дела", "номер_дела", "номер дела (номер объекта)"])
    if not col_case:
        col_case = get_col_by_letter(df, "I")

    if not col_case:
        return "Не удалось определить колонку номера дела (I)."

    col_onzs = get_col_by_letter(df, "E")
    if not col_onzs:
        col_onzs = find_col(df, ["онзс"])

    if not col_onzs:
        return "Не удалось определить колонку ОНзС (E)."

    df_f = df[df[col_case].astype(str).str.strip() == case_no.strip()]
    if df_f.empty:
        return f"Не найдено строк по делу {case_no}."

    values = df_f[col_onzs].dropna().astype(str).unique().tolist()
    if not values:
        return f"Для дела {case_no} нет данных по ОНзС."

    return f"ОНзС по делу {case_no}:\n" + "\n".join(f"• {v}" for v in values)


# ----------------- Работа с пользователями и правами -----------------


def ensure_user(update: Update) -> None:
    user = update.effective_user
    if not user:
        return

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE user_id=?", (user.id,))
    row = c.fetchone()
    if not row:
        c.execute(
            "INSERT INTO users (user_id, username, first_seen_at) VALUES (?, ?, ?)",
            (user.id, user.username or "", local_now().isoformat()),
        )
        conn.commit()
    conn.close()


def ensure_admin(user_id: int, username: str) -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))
    row = c.fetchone()
    if not row:
        c.execute(
            "INSERT INTO admins (user_id, username, first_seen_at) VALUES (?, ?, ?)",
            (user_id, username or "", local_now().isoformat()),
        )
        conn.commit()
    conn.close()


def is_db_admin(user_id: int) -> bool:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row is not None


# ----------------- Основное меню -----------------


async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip().lower()

    if text == "📅 график".lower():
        settings = get_schedule_state()
        is_admin_flag = is_admin(update.effective_user.id)
        txt = build_schedule_text(is_admin_flag, settings)
        kb = build_schedule_inline(is_admin_flag, settings)
        await update.message.reply_text(txt, reply_markup=kb)
        return

    if text == "📊 итоговая".lower():
        df = get_schedule_df()
        if df is None:
            await update.message.reply_text(
                "Не удалось получить данные графика. "
                "Проверьте подключение к Google Sheets или доступ по ссылке."
            )
            return

        col_date = find_col(df, ["дата выезда", "дата итоговой", "дата проверки"])
        if not col_date:
            await update.message.reply_text("Не удалось найти колонку с датой выезда.")
            return

        col_case = find_col(df, ["дело", "номер дела", "номер_дела", "номер дела (номер объекта)"])
        if not col_case:
            col_case = get_col_by_letter(df, "I")

        col_type = find_col(df, ["вид проверки", "тип проверки"])
        if not col_type:
            col_type = get_col_by_letter(df, "J")

        if not col_case or not col_type:
            await update.message.reply_text(
                "Не удалось определить колонки номера дела (I) или вида проверки (J)."
            )
            return

        today = local_now().date()
        future = today + timedelta(days=30)

        records = []

        for _, row in df.iterrows():
            raw_date = str(row.get(col_date, "")).strip()
            if not raw_date:
                continue

            try:
                if "." in raw_date:
                    d = datetime.strptime(raw_date, "%d.%m.%Y").date()
                else:
                    d = datetime.fromisoformat(raw_date).date()
            except Exception:
                continue

            if not (today <= d <= future):
                continue

            check_type = str(row.get(col_type, "")).strip().lower()
            if "итог" not in check_type:
                continue

            case_no = str(row.get(col_case, "")).strip()
            records.append((d, check_type, case_no))

        if not records:
            await update.message.reply_text("Нет ближайших итоговых проверок в ближайшие 30 дней.")
            return

        records.sort(key=lambda x: x[0])

        lines = ["Ближайшие итоговые проверки:"]
        for d, ctype, case_no in records[:20]:
            lines.append(f"• {d.strftime('%d.%m.%Y')} — {ctype} — дело: {case_no}")

        await update.message.reply_text("\n".join(lines))
        return

    if text == "📝 замечания".lower():
        kb = remarks_menu_inline()
        await update.message.reply_text("Раздел «Замечания»:", reply_markup=kb)
        return

    if text == "🏗 онзс".lower():
        kb = onzs_menu_inline()
        await update.message.reply_text("Раздел «ОНзС»:", reply_markup=kb)
        return

    if text == "инспектор":
        kb = inspector_menu_inline()
        await update.message.reply_text("Раздел «Инспектор»:", reply_markup=kb)
        return

    if text == "📈 аналитика".lower():
        await update.message.reply_text(
            "Раздел «📈 Аналитика» пока в разработке. В будущем здесь будет история "
            "согласований, изменения статусов и другая статистика."
        )
        return

    await update.message.reply_text(
        "Я вас не понял. Выберите пункт меню или введите команду /start.",
        reply_markup=main_menu(),
    )


# ----------------- Коллбэки (inline-кнопки) -----------------


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data
    user = query.from_user
    await query.answer()

    if data == "schedule_refresh":
        settings = get_schedule_state()
        is_admin_flag = is_admin(user.id)
        txt = build_schedule_text(is_admin_flag, settings)
        kb = build_schedule_inline(is_admin_flag, settings)
        await query.edit_message_text(txt, reply_markup=kb)
        return

    if data == "schedule_download":
        await query.message.reply_text(
            "Скачивание графика пока реализовано как чтение из Google Sheets. "
            f"Откройте таблицу по ссылке:\n{GOOGLE_SHEET_URL_DEFAULT}"
        )
        return

    if data == "schedule_upload":
        if not is_admin(user.id):
            await query.message.reply_text("Только администратор может загружать файл графика.")
            return
        await query.message.reply_text(
            "Отправьте новый файл графика (Excel/xlsx). "
            "После загрузки будет увеличена версия и сброшены согласования."
        )
        context.user_data["awaiting_schedule_file"] = True
        return

    if data == "schedule_approvers":
        if not is_admin(user.id):
            await query.message.reply_text("Только администратор может изменять согласующих.")
            return

        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT label FROM approvers")
        rows = c.fetchall()
        conn.close()

        labels = [r["label"] for r in rows] if rows else []

        if not labels:
            await query.message.reply_text(
                "Список согласующих пуст. Добавьте их командами админа (пока не реализовано)."
            )
            return

        text_lines = ["Текущий список возможных согласующих:"]
        for lbl in labels:
            text_lines.append(f"• {lbl}")

        await query.message.reply_text("\n".join(text_lines))
        return

    if data == "remarks_not_done":
        # Обязательно что-то отвечаем сразу
        await query.message.reply_text("Ищу строки со статусом «нет» в файле замечаний...")

        try:
            df = get_remarks_df()
        except Exception as e:
            log.exception("Критическая ошибка в get_remarks_df: %s", e)
            await query.message.reply_text(
                "Произошла внутренняя ошибка при чтении файла замечаний."
            )
            return

        if df is None:
            await query.message.reply_text(
                "Не удалось получить файл замечаний. "
                "Проверьте подключение к Google Sheets или доступ по ссылке."
            )
            return

        try:
            text = build_remarks_not_done_text(df)
        except Exception as e:
            log.exception("Ошибка в build_remarks_not_done_text: %s", e)
            await query.message.reply_text(
                "Не удалось сформировать список неустранённых замечаний."
            )
            return

        await query.message.reply_text(text)
        return

    if data == "remarks_download":
        await query.message.reply_text(
            "Файл с замечаниями хранится в той же Google-таблице. "
            f"Откройте её по ссылке:\n{GOOGLE_SHEET_URL_DEFAULT}"
        )
        return

    if data == "onzs_by_case":
        context.user_data["awaiting_onzs_case"] = True
        await query.message.reply_text("Введите номер дела (формат 00-00-000000):")
        return

    if data == "inspector_add":
        context.user_data["inspector_form"] = {
            "step": "date_departure",
        }
        await query.message.reply_text("Введите дату выезда (ДД.ММ.ГГГГ):")
        return


# ----------------- Обработка текстов (ОНзС + Инспектор) -----------------


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()

    if context.user_data.get("awaiting_onzs_case"):
        context.user_data["awaiting_onzs_case"] = False
        df = get_remarks_df()
        if df is None:
            await update.message.reply_text(
                "Не удалось получить данные замечаний для расчёта ОНзС."
            )
            return

        resp = build_onzs_text_for_case(df, text)
        await update.message.reply_text(resp)
        return

    if context.user_data.get("inspector_form"):
        form = context.user_data["inspector_form"]
        step = form.get("step")

        if step == "date_departure":
            try:
                dep_date = datetime.strptime(text, "%d.%m.%Y").date()
                form["date_departure"] = dep_date
            except Exception:
                await update.message.reply_text(
                    "Неверный формат даты. Введите в формате ДД.ММ.ГГГГ"
                )
                return

            form["step"] = "date_final"
            await update.message.reply_text("Введите дату начала итоговой проверки (ДД.ММ.ГГГГ):")
            return

        if step == "date_final":
            try:
                fin_date = datetime.strptime(text, "%d.%m.%Y").date()
                form["date_final"] = fin_date
            except Exception:
                await update.message.reply_text(
                    "Неверный формат даты. Введите в формате ДД.ММ.ГГГГ"
                )
                return

            form["step"] = "area"
            await update.message.reply_text("Введите площадь (кв.м):")
            return

        if step == "area":
            form["area"] = text
            form["step"] = "floors"
            await update.message.reply_text("Введите количество этажей:")
            return

        if step == "floors":
            form["floors"] = text
            form["step"] = "onzs"
            await update.message.reply_text("Введите ОНзС (1-12):")
            return

        if step == "onzs":
            form["onzs"] = text
            form["step"] = "developer"
            await update.message.reply_text("Введите наименование застройщика:")
            return

        if step == "developer":
            form["developer"] = text
            form["step"] = "object"
            await update.message.reply_text("Введите наименование объекта:")
            return

        if step == "object":
            form["object"] = text
            form["step"] = "address"
            await update.message.reply_text("Введите строительный адрес:")
            return

        if step == "address":
            form["address"] = text
            form["step"] = "case_no"
            await update.message.reply_text("Введите номер дела (00-00-000000):")
            return

        if step == "case_no":
            form["case_no"] = text
            form["step"] = "check_type"
            await update.message.reply_text(
                "Введите вид проверки (ПП, итоговая, профвизит, запрос ОНзС, поручение руководства):"
            )
            return

        if step == "check_type":
            form["check_type"] = text

            ok = append_inspector_row_to_excel(form)
            if ok:
                await update.message.reply_text(
                    "Выезд успешно сохранён в лист "
                    f"«{INSPECTOR_SHEET_NAME}» Google-таблицы."
                )
            else:
                await update.message.reply_text(
                    "Не удалось сохранить выезд в Google Sheets. "
                    "Проверьте настройки сервисного аккаунта и доступ к таблице."
                )

            context.user_data["inspector_form"] = None
            return

    await main_menu_handler(update, context)


# ----------------- Работа с Google Sheets: чтение графика и замечаний -----------------


def get_schedule_df() -> Optional[pd.DataFrame]:
    """
    Получает данные графика из первого листа Google Sheets.
    """
    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets сервис недоступен – невозможно получить график.")
        return None

    try:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=GSHEETS_SPREADSHEET_ID
        ).execute()
        sheets = spreadsheet.get("sheets", [])
        if not sheets:
            log.error("Нет листов в таблице.")
            return None

        first_sheet_name = sheets[0]["properties"]["title"]
        df = read_sheet_to_dataframe(GSHEETS_SPREADSHEET_ID, first_sheet_name)
        return df
    except Exception as e:
        log.error("Ошибка получения данных графика из Google Sheets: %s", e)
        return None


def get_remarks_df() -> Optional[pd.DataFrame]:
    """
    Получает данные замечаний из всех листов (кроме листа инспектора),
    добавляя колонку _sheet с названием листа.

    Читает через HTTP-экспорт Google Sheets как .xlsx
    (без использования Google Sheets API).
    """
    if not GSHEETS_SPREADSHEET_ID:
        log.error("GSHEETS_SPREADSHEET_ID не задан – не можем получить замечания.")
        return None

    url = build_export_url(GSHEETS_SPREADSHEET_ID)
    log.info("Замечания: скачиваем таблицу по HTTP: %s", url)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error("Ошибка HTTP-запроса при получении замечаний: %s", e)
        return None

    try:
        bio = BytesIO(resp.content)
        xls = pd.ExcelFile(bio)
    except Exception as e:
        log.error("Ошибка чтения Excel из HTTP-ответа: %s", e)
        return None

    frames: List[pd.DataFrame] = []

    for sheet_name in xls.sheet_names:
        if sheet_name == INSPECTOR_SHEET_NAME:
            log.info("Замечания: пропускаем лист инспектора '%s'", sheet_name)
            continue

        try:
            df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception as e_sheet:
            log.error("Ошибка чтения листа '%s' из Excel: %s", sheet_name, e_sheet)
            continue

        if df_sheet is None or df_sheet.empty:
            continue

        df_sheet["_sheet"] = sheet_name
        frames.append(df_sheet)

    if not frames:
        log.error("Не удалось прочитать ни один лист замечаний (HTTP-Excel).")
        return None

    return pd.concat(frames, ignore_index=True)


# ----------------- Обработка документов (файлы) -----------------


async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ensure_user(update)

    if context.user_data.get("awaiting_schedule_file"):
        context.user_data["awaiting_schedule_file"] = False

        if not is_admin(user.id):
            await update.message.reply_text("Только администратор может загружать файл графика.")
            return

        doc = update.message.document
        if not doc:
            await update.message.reply_text("Не найден файл в сообщении.")
            return

        file = await doc.get_file()
        file_path = "uploaded_schedule.xlsx"
        await file.download_to_drive(file_path)

        settings = get_schedule_state()
        conn = get_db()
        c = conn.cursor()
        new_version = get_schedule_version(settings) + 1
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_version', ?)",
            (str(new_version),),
        )
        conn.commit()
        conn.close()

        set_schedule_file_name(new_version, doc.file_name or file_path)

        await update.message.reply_text(
            f"Новый файл графика загружен и сохранён как версия {new_version}.\n"
            f"Имя файла: {doc.file_name or file_path}"
        )
        return

    await update.message.reply_text(
        "Я получил файл, но сейчас он не используется ни в одном сценарии."
    )


# ----------------- Команды -----------------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ensure_user(update)

    if is_admin(user.id):
        ensure_admin(user.id, user.username or "")

    await update.message.reply_text(
        "Добро пожаловать в бота отдела СОТ.\n"
        "Выберите нужный раздел в меню.",
        reply_markup=main_menu(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Доступные разделы:\n"
        "• 📅 График — график выездов\n"
        "• 📊 Итоговая — ближайшие итоговые проверки\n"
        "• 📝 Замечания — статусы устранения\n"
        "• 🏗 ОНзС — поиск по ОНзС\n"
        "• Инспектор — добавление выездов в таблицу\n"
        "• 📈 Аналитика — в разработке"
    )


async def admin_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Эта команда доступна только администраторам.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "Ответьте этой командой на сообщение пользователя, которого нужно сделать админом."
        )
        return

    target = update.message.reply_to_message.from_user
    ensure_admin(target.id, target.username or "")
    await update.message.reply_text(
        f"Пользователь {target.mention_html()} добавлен в администраторы.",
        parse_mode="HTML",
    )


async def set_notify_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Эта команда доступна только администраторам.")
        return

    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text(
            "Команду /set_notify_group нужно вызывать из группы или супергруппы."
        )
        return

    chat_id_str = str(chat.id)
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) "
        "VALUES ('schedule_notify_chat_id', ?)",
        (chat_id_str,),
    )
    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"Группа для уведомлений по графику обновлена: {chat_id_str}"
    )


# ----------------- MAIN -----------------


def main() -> None:
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан.")
        raise SystemExit("Укажи BOT_TOKEN в переменных окружения или .env")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin_add", admin_add))
    app.add_handler(CommandHandler("set_notify_group", set_notify_group))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    log.info("Запуск бота...")
    app.run_polling()


if __name__ == "__main__":
    main()

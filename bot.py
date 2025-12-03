import logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List

import pandas as pd
import json

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

# Google Sheets API
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


def build_schedule_text(is_admin_flag: bool, settings: dict) -> str:
    approvers = get_current_approvers(settings)
    version = get_schedule_version(settings)
    file_name = get_schedule_name_for_version(version)

    lines: List[str] = []
    lines.append("Раздел «График».")
    lines.append("")
    lines.append(f"Текущий файл графика: {file_name} (версия {version})")
    lines.append("")
    lines.append(
        "Порядок работы:\n"
        "1) Администратор выбирает, КТО согласует.\n"
        "2) Согласующие получают уведомление.\n"
        "3) Нажимают «✅ Согласовать» или «✏ На доработку»."
    )
    lines.append("")
    lines.append("Статусы согласования:")

    if not approvers:
        lines.append("• Согласующие ещё не выбраны.")
        return "\n".join(lines)

    conn = get_db()
    c = conn.cursor()
    placeholders = ",".join("?" * len(approvers))
    params: List[Any] = [version] + approvers
    c.execute(
        f"""SELECT approver, decision, decided_at 
            FROM approvals
            WHERE schedule_version = ? 
              AND approver IN ({placeholders})
            ORDER BY datetime(decided_at) DESC""",
        params,
    )
    rows = c.fetchall()
    conn.close()

    last_by_approver: Dict[str, sqlite3.Row] = {}
    for r in rows:
        appr = r["approver"]
        if appr not in last_by_approver:
            last_by_approver[appr] = r

    total = len(approvers)
    approved_count = 0
    rework_count = 0

    for appr in approvers:
        r = last_by_approver.get(appr)
        if not r:
            lines.append(f"• {appr} — ожидает согласования")
            continue

        decision = r["decision"]
        dt_raw = r["decided_at"] or ""
        try:
            dt_obj = datetime.fromisoformat(dt_raw)
            dt_str = dt_obj.strftime("%d.%m.%Y %H:%M")
        except Exception:
            dt_str = dt_raw

        if decision == "approve":
            approved_count += 1
            lines.append(f"• {appr} — ✅ согласовано ({dt_str})")
        elif decision == "rework":
            rework_count += 1
            lines.append(f"• {appr} — ✏ на доработку ({dt_str})")
        else:
            lines.append(f"• {appr} — {decision or 'ожидает'} ({dt_str})")

    lines.append("")
    if rework_count > 0:
        lines.append("Итог: график направлен на доработку.")
    elif approved_count == total and total > 0:
        lines.append("Итог: все согласующие утвердили график.")
    else:
        lines.append(
            f"Итог: согласовали {approved_count} из {total}, остальные в ожидании."
        )

    return "\n".join(lines)


def build_schedule_inline(is_admin_flag: bool, settings: dict) -> InlineKeyboardMarkup:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT label FROM approvers ORDER BY id")
    labels = [r["label"] for r in c.fetchall()]
    conn.close()

    app_buttons = [
        InlineKeyboardButton(lbl, callback_data=f"schedule_set_approver:{lbl}")
        for lbl in labels
    ]

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for btn in app_buttons:
        row.append(btn)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    header: List[List[InlineKeyboardButton]] = []
    if is_admin_flag:
        header.append(
            [
                InlineKeyboardButton("⬆ Загрузить", callback_data="schedule_upload"),
                InlineKeyboardButton("⬇ Скачать", callback_data="schedule_download"),
            ]
        )
        header.append(
            [
                InlineKeyboardButton(
                    "➕ Добавить согласующего", callback_data="schedule_add_custom"
                )
            ]
        )
    else:
        header.append(
            [InlineKeyboardButton("⬇ Скачать", callback_data="schedule_download")]
        )
        header.append(
            [InlineKeyboardButton("Статусы согласования", callback_data="noop")]
        )

    footer: List[List[InlineKeyboardButton]] = []
    status = settings.get("schedule_status")
    if status in (None, "", "pending"):
        footer.append(
            [
                InlineKeyboardButton("✅ Согласовать", callback_data="schedule_approve"),
                InlineKeyboardButton("✏ На доработку", callback_data="schedule_rework"),
            ]
        )

    return InlineKeyboardMarkup(header + rows + footer)


def remarks_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done"),
            ],
            [
                InlineKeyboardButton(
                    "⬆ Загрузить График выездов", callback_data="remarks_upload"
                ),
                InlineKeyboardButton(
                    "⬇ Скачать График выездов", callback_data="remarks_download"
                ),
            ],
        ]
    )


def onzs_menu_inline() -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton(str(i), callback_data=f"onzs_{i}") for i in range(1, 7)
    ]
    row2 = [
        InlineKeyboardButton(str(i), callback_data=f"onzs_{i}") for i in range(7, 13)
    ]
    return InlineKeyboardMarkup([row1, row2])


def onzs_period_inline(onzs_num: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🗓 За 30 дней", callback_data=f"onzsperiod:{onzs_num}:30"
                ),
                InlineKeyboardButton(
                    "🗓 За 90 дней", callback_data=f"onzsperiod:{onzs_num}:90"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📅 Ввести даты", callback_data=f"onzsperiod:{onzs_num}:custom"
                ),
                InlineKeyboardButton(
                    "Все даты", callback_data=f"onzsperiod:{onzs_num}:all"
                ),
            ],
        ]
    )


def inspector_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("➕ Добавить выезд", callback_data="insp_add_trip")]]
    )


# ----------------- Команды -----------------


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT OR IGNORE INTO users (user_id, username, first_seen_at)
           VALUES (?, ?, ?)""",
        (user.id, user.username or "", local_now().isoformat()),
    )
    conn.commit()
    conn.close()

    msg = "Привет! Это бот отдела СОТ.\nВыберите раздел на клавиатуре."
    await update.message.reply_text(msg, reply_markup=main_menu())


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    chat = update.effective_chat
    txt = f"Ваш id: {user.id}\nusername: @{user.username or ''}"
    if chat:
        txt += f"\nID текущего чата: {chat.id}"
    await update.message.reply_text(txt)


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Команда доступна только администраторам.")
        return

    await update.message.reply_text(
        "Администраторы заданы жёстко в коде:\n• @asdinamitif (398960707)"
    )


async def cmd_set_schedule_group(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    /set_schedule_group <chat_id>
    Настройка группы, куда отправлять уведомление после того, как график согласован.
    Только админ.
    """
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Команда доступна только администратору.")
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "Укажи chat_id группы.\n"
            "Подсказка: добавь бота в нужную группу и введи там /id — бот вернёт ID чата."
        )
        return

    chat_id_str = args[0].strip()
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


# ----------------- Работа с Google Sheets: чтение графика и замечаний -----------------


def get_schedule_df() -> Optional[pd.DataFrame]:
    """
    Получает данные графика из первого листа Google Sheets.
    При необходимости можно заменить на конкретное имя листа.
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
    """
    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets сервис недоступен – невозможно получить замечания.")
        return None

    try:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=GSHEETS_SPREADSHEET_ID
        ).execute()
        sheet_props = spreadsheet.get("sheets", [])
        if not sheet_props:
            log.error("Нет листов в таблице.")
            return None

        frames = []
        for s in sheet_props:
            sheet_name = s["properties"]["title"]
            # пропускаем лист инспектора
            if sheet_name == INSPECTOR_SHEET_NAME:
                continue

            df_sheet = read_sheet_to_dataframe(GSHEETS_SPREADSHEET_ID, sheet_name)
            if df_sheet is not None and not df_sheet.empty:
                df_sheet["_sheet"] = sheet_name
                frames.append(df_sheet)

        if not frames:
            log.error("Не удалось прочитать ни один лист с замечаниями.")
            return None

        return pd.concat(frames, ignore_index=True)
    except Exception as e:
        log.error("Ошибка получения данных замечаний из Google Sheets: %s", e)
        return None


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
                "Не удалось получить данные графика из Google Sheets."
            )
            return

        col_date = find_col(df, ["дата"])
        col_type = find_col(df, ["итоговая", "тип"])
        col_case = find_col(df, ["дело"])

        lines = ["ИТОГОВАЯ ИНФОРМАЦИЯ", ""]
        if col_date is None or col_type is None:
            lines.append("Не удалось определить столбцы итоговой.")
        else:
            today = local_now().date()
            upcoming = df[
                (pd.to_datetime(df[col_date], errors="coerce").dt.date >= today)
                & (df[col_type].astype(str).str.contains("итог", case=False))
            ]
            if upcoming.empty:
                lines.append("Ближайших итоговых проверок не найдено.")
            else:
                for _, r in upcoming.head(10).iterrows():
                    try:
                        d_str = pd.to_datetime(r[col_date]).strftime("%d.%m.%Y")
                    except Exception:
                        d_str = str(r[col_date])
                    typ = str(r[col_type])
                    case_no = (
                        str(r[col_case]) if col_case in df.columns else "(нет дела)"
                    )
                    lines.append(f"• {d_str} — {typ} — дело: {case_no}")

        await update.message.reply_text("\n".join(lines))
        return

    if text == "📝 замечания".lower():
        df = get_remarks_df()
        if df is None:
            await update.message.reply_text(
                "Не удалось получить файл замечаний из Google Sheets."
            )
            return

        await update.message.reply_text(
            "Раздел «Замечания». Выберите действие:", reply_markup=remarks_menu_inline()
        )
        return

    if text == "🏗 онзс".lower():
        await update.message.reply_text(
            "Выберите уровень ОНзС (1–12):", reply_markup=onzs_menu_inline()
        )
        return

    if text == "инспектор".lower():
        await update.message.reply_text(
            "Раздел «Инспектор». Выберите действие:",
            reply_markup=inspector_menu_inline(),
        )
        return

    if text == "📈 аналитика".lower():
        context.user_data["await_analytics_pass"] = True
        await update.message.reply_text("Введите пароль для доступа к аналитике:")
        return

    await update.message.reply_text("Не понял команду. Выберите раздел на клавиатуре.")


# ----------------- Замечания: «Не устранены» -----------------


async def show_remarks_not_done(query) -> None:
    df_all = get_remarks_df()
    if df_all is None:
        await query.edit_message_text("Файл замечаний не найден.")
        return

    sheet_name = get_current_remarks_sheet_name()

    if "_sheet" in df_all.columns:
        df = df_all[df_all["_sheet"].astype(str) == sheet_name].copy()
    else:
        df = df_all.copy()

    if df.empty:
        sheets = (
            df_all["_sheet"].unique().tolist()
            if "_sheet" in df_all.columns
            else []
        )
        text = (
            f"На листе «{sheet_name}» нет данных.\n"
            f"Доступные листы: {', '.join(map(str, sheets)) or 'не удалось определить'}."
        )
        await query.edit_message_text(text)
        return

    col_case = find_col(df, ["номер дела", "дело"])
    if col_case is None:
        col_case = get_col_by_letter(df, "I")

    if col_case is None:
        await query.edit_message_text("Не удалось найти столбец «Номер дела».")
        return

    col_pb_q = get_col_by_letter(df, "Q")
    col_pb_r = get_col_by_letter(df, "R")
    col_ar_y = get_col_by_letter(df, "Y")
    col_eom_ae = get_col_by_letter(df, "AE")

    blocks: List[tuple[str, List[str]]] = [
        ("Пожарная безопасность", [c for c in [col_pb_q, col_pb_r] if c]),
        (
            "Архитектура, Доступ инвалидов, Архитектурный облик",
            [col_ar_y] if col_ar_y else [],
        ),
        ("Электроснабжение", [col_eom_ae] if col_eom_ae else []),
    ]
    blocks = [(name, cols) for name, cols in blocks if cols]

    if not blocks:
        await query.edit_message_text(
            "Не удалось найти столбцы Q, R, Y, AE на листе с замечаниями."
        )
        return

    case_blocks: Dict[str, set[str]] = {}
    order: List[str] = []

    for _, row in df.iterrows():
        case_no = str(row.get(col_case, "")).strip()
        if not case_no:
            continue

        row_blocks: List[str] = []
        for block_name, cols in blocks:
            values = [
                str(row.get(col, "") or "").strip().lower()
                for col in cols
            ]
            if any(v == "нет" for v in values):
                row_blocks.append(block_name)

        if not row_blocks:
            continue

        if case_no not in case_blocks:
            case_blocks[case_no] = set()
            order.append(case_no)

        case_blocks[case_no].update(row_blocks)

    if not case_blocks:
        await query.edit_message_text(
            f"На листе «{sheet_name}» нет дел с неустранёнными нарушениями (значение «нет»)."
        )
        return

    lines: List[str] = [
        "Строки со статусом «НЕ УСТРАНЕНЫ (нет)»",
        f"Лист: «{sheet_name}»",
        "",
    ]

    for case_no in order[:50]:
        blocks_list = sorted(case_blocks[case_no])
        line_blocks = "; ".join(blocks_list)
        lines.append(f"• {case_no} — {line_blocks}")

    if len(order) > 50:
        lines.append("")
        lines.append(f"Всего дел: {len(order)}, показаны первые 50.")

    await query.edit_message_text("\n".join(lines))


# ----------------- Уведомление в группу после согласования -----------------


async def check_and_notify_schedule_approved(
    context: ContextTypes.DEFAULT_TYPE, settings_after: dict
) -> None:
    version = get_schedule_version(settings_after)
    approvers = get_current_approvers(settings_after)
    if not approvers:
        return

    conn = get_db()
    c = conn.cursor()
    placeholders = ",".join("?" * len(approvers))
    params: List[Any] = [version] + approvers
    c.execute(
        f"""SELECT approver, decision, decided_at
            FROM approvals
            WHERE schedule_version = ?
              AND approver IN ({placeholders})
            ORDER BY datetime(decided_at) DESC""",
        params,
    )
    rows = c.fetchall()
    conn.close()

    if not rows:
        return

    last_by_approver: Dict[str, sqlite3.Row] = {}
    for r in rows:
        appr = r["approver"]
        if appr not in last_by_approver:
            last_by_approver[appr] = r

    # все ли согласовали
    for appr in approvers:
        r = last_by_approver.get(appr)
        if not r or r["decision"] != "approve":
            return

    notify_chat_id = get_schedule_notify_chat_id(settings_after)
    if not notify_chat_id:
        return

    last_notified_raw = settings_after.get("last_notified_version") or "0"
    try:
        last_notified = int(last_notified_raw)
    except Exception:
        last_notified = 0
    if last_notified >= version:
        return

    file_name = get_schedule_name_for_version(version)
    lines = [
        "✅ График выездов согласован.",
        f"Файл: {file_name} (версия {version})",
        "",
        "Согласующие:",
    ]
    for appr in approvers:
        r = last_by_approver.get(appr)
        dt_raw = r["decided_at"] or ""
        try:
            dt_obj = datetime.fromisoformat(dt_raw)
            dt_str = dt_obj.strftime("%d.%m.%Y %H:%M")
        except Exception:
            dt_str = dt_raw
        lines.append(f"• {appr} — согласовано {dt_str}")

    text = "\n".join(lines)

    try:
        await context.bot.send_message(chat_id=notify_chat_id, text=text)
    except Exception as e:
        log.error("Не удалось отправить уведомление в группу: %s", e)
        return

    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) "
        "VALUES ('last_notified_version', ?)",
        (str(version),),
    )
    conn.commit()
    conn.close()


# ----------------- Callback -----------------


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user = update.effective_user
    if not user:
        return

    settings = get_schedule_state()
    is_admin_flag = is_admin(user.id)

    # График
    if data == "schedule_upload":
        if not is_admin_flag:
            await query.edit_message_text("Команда доступна только администратору.")
            return
        await query.edit_message_text(
            "График теперь редактируется напрямую в Google Sheets. "
            "Загрузка файлов через бота отключена."
        )
        return

    if data == "schedule_download":
        await query.edit_message_text(
            "Скачивание графика через бота не поддерживается.\n"
            "Откройте файл в Google Sheets."
        )
        return

    if data.startswith("schedule_set_approver:"):
        appr = data.split(":", 1)[1].strip()
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) "
            "VALUES ('current_approver', ?)",
            (appr,),
        )
        conn.commit()
        conn.close()

        new_settings = get_schedule_state()
        txt = build_schedule_text(is_admin_flag, new_settings)
        kb = build_schedule_inline(is_admin_flag, new_settings)
        await query.edit_message_text(txt, reply_markup=kb)
        return

    if data == "schedule_add_custom":
        if not is_admin_flag:
            await query.edit_message_text("Добавлять согласующих может только админ.")
            return
        context.user_data["await_custom_approver"] = True
        await query.edit_message_text("Введите username в формате @username:")
        return

    if data == "schedule_approve":
        appr = user.username
        if not appr:
            await query.edit_message_text("У вас нет username, невозможно согласовать.")
            return

        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT value FROM schedule_settings WHERE key='current_approver'")
        row = c.fetchone()
        current = row["value"] if row else None

        if current and current != f"@{appr}":
            await query.edit_message_text("Вы не являетесь назначенным согласующим.")
            conn.close()
            return

        ver = get_schedule_version(settings)
        now = local_now().isoformat()
        c.execute(
            """INSERT INTO approvals (user_id, username, approver, decision, decided_at, schedule_version)
               VALUES (?, ?, ?, 'approve', ?, ?)""",
            (user.id, user.username, f"@{appr}", now, ver),
        )
        conn.commit()
        conn.close()

        new_settings = get_schedule_state()
        txt = build_schedule_text(is_admin_flag, new_settings)
        kb = build_schedule_inline(is_admin_flag, new_settings)
        await query.edit_message_text(txt, reply_markup=kb)

        await check_and_notify_schedule_approved(context, new_settings)
        return

    if data == "schedule_rework":
        appr = user.username
        if not appr:
            await query.edit_message_text(
                "У вас нет username, нельзя отправить на доработку."
            )
            return

        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT value FROM schedule_settings WHERE key='current_approver'")
        row = c.fetchone()
        current = row["value"] if row else None

        if current and current != f"@{appr}":
            await query.edit_message_text("Вы не являетесь назначенным согласующим.")
            conn.close()
            return

        ver = get_schedule_version(settings)
        now = local_now().isoformat()
        c.execute(
            """INSERT INTO approvals (user_id, username, approver, decision, decided_at, schedule_version)
               VALUES (?, ?, ?, 'rework', ?, ?)""",
            (user.id, user.username, f"@{appr}", now, ver),
        )
        conn.commit()
        conn.close()

        new_settings = get_schedule_state()
        txt = build_schedule_text(is_admin_flag, new_settings)
        kb = build_schedule_inline(is_admin_flag, new_settings)
        await query.edit_message_text(txt, reply_markup=kb)
        return

    # Замечания
    if data == "remarks_upload":
        await query.edit_message_text(
            "Замечания теперь ведутся в Google Sheets.\n"
            "Загрузка файлов через бота отключена."
        )
        return

    if data == "remarks_download":
        await query.edit_message_text(
            "Скачивание файла замечаний через бота не поддерживается.\n"
            "Откройте таблицу в Google Sheets."
        )
        return

    if data == "remarks_not_done":
        await show_remarks_not_done(query)
        return

    # Инспектор
    if data == "insp_add_trip":
        context.user_data["insp_form"] = {}
        context.user_data["insp_step"] = "date_departure"
        await query.edit_message_text(
            "Пошаговый мастер инспектора.\n"
            "Введите дату выезда (ДД.ММ.ГГГГ):"
        )
        return

    # ОНзС
    if data.startswith("onzs_"):
        num = data.split("_", 1)[1]
        await query.edit_message_text(
            f"Вы выбрали ОНзС {num}. Теперь выберите период:",
            reply_markup=onzs_period_inline(num),
        )
        return

    if data.startswith("onzsperiod:"):
        _, num, mode = data.split(":", 2)

        if mode == "custom":
            context.user_data["onzs_num"] = num
            context.user_data["onzs_custom"] = True
            await query.edit_message_text(
                "Введите диапазон дат в формате ДД.ММ.ГГГГ–ДД.ММ.ГГГГ"
            )
            return

        df = get_remarks_df()
        if df is None:
            await query.edit_message_text("Файл замечаний не найден.")
            return

        col_onzs = find_col(df, ["онзс"])
        col_date = find_col(df, ["дата"])
        if col_onzs is None or col_date is None:
            await query.edit_message_text("Не удалось определить столбцы для ОНзС.")
            return

        df2 = df[df[col_onzs].astype(str).str.contains(str(num))]
        if df2.empty:
            await query.edit_message_text(f"Нет данных по ОНзС {num}.")
            return

        if mode != "all":
            days = int(mode)
            dt_min = local_now().date() - timedelta(days=days)
            df2 = df2[
                pd.to_datetime(df2[col_date], errors="coerce").dt.date >= dt_min
            ]

        if df2.empty:
            await query.edit_message_text("Нет данных для выбранного периода.")
            return

        lines = [f"ОНзС {num}:"]
        for _, r in df2.head(50).iterrows():
            try:
                d_str = pd.to_datetime(r[col_date]).strftime("%d.%m.%Y")
            except Exception:
                d_str = str(r[col_date])
            lines.append(f"• {d_str} — {r.to_dict()}")

        await query.edit_message_text("\n".join(lines))
        return

    await query.edit_message_text("Команда не распознана.")


# ----------------- Обработка состояний -----------------


async def handle_custom_approver_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not context.user_data.get("await_custom_approver"):
        return

    user = update.effective_user
    if not user or not is_admin(user.id):
        context.user_data["await_custom_approver"] = False
        await update.message.reply_text("Добавлять согласующих может только администратор.")
        return

    text = (update.message.text or "").strip()
    context.user_data["await_custom_approver"] = False

    if not text:
        await update.message.reply_text(
            "Не понял username. Введите, например: @ivanov"
        )
        return

    if not text.startswith("@"):
        text = "@" + text

    label = text
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO approvers (label) VALUES (?)", (label,))
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) "
        "VALUES ('current_approver', ?)",
        (label,),
    )
    conn.commit()
    conn.close()

    settings = get_schedule_state()
    txt = build_schedule_text(is_admin(user.id), settings)
    kb = build_schedule_inline(is_admin(user.id), settings)
    await update.message.reply_text(
        f"Согласующий {label} добавлен и выбран.", reply_markup=kb
    )


async def handle_remarks_row_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    # резерв под будущий функционал
    return


async def handle_onzs_custom_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not context.user_data.get("onzs_custom"):
        return

    user = update.effective_user
    if not user:
        return

    text = (update.message.text or "").strip()
    context.user_data["onzs_custom"] = False
    num = context.user_data.get("onzs_num")

    if not num:
        await update.message.reply_text(
            "ОНзС не определён. Начните заново из раздела «ОНзС»."
        )
        return

    try:
        t = text.replace("—", "-").replace("–", "-")
        s1, s2 = [p.strip() for p in t.split("-", 1)]
        d1 = datetime.strptime(s1, "%d.%m.%Y").date()
        d2 = datetime.strptime(s2, "%d.%m.%Y").date()
        if d2 < d1:
            d1, d2 = d2, d1
    except Exception:
        await update.message.reply_text(
            "Не понял формат. Нужен вид ДД.ММ.ГГГГ–ДД.ММ.ГГГГ, например 01.01.2025–31.01.2025."
        )
        return

    df = get_remarks_df()
    if df is None:
        await update.message.reply_text("Файл замечаний не найден.")
        return

    col_onzs = find_col(df, ["онзс"])
    col_date = find_col(df, ["дата"])
    if col_onzs is None or col_date is None:
        await update.message.reply_text("Не удалось определить столбцы для ОНзС.")
        return

    df2 = df[df[col_onzs].astype(str).str.contains(str(num))]
    if df2.empty:
        await update.message.reply_text(f"Нет данных по ОНзС {num}.")
        return

    df2["__date_parsed"] = pd.to_datetime(df2[col_date], errors="coerce").dt.date
    df2 = df2[(df2["__date_parsed"] >= d1) & (df2["__date_parsed"] <= d2)]

    if df2.empty:
        await update.message.reply_text("Нет данных для заданного периода.")
        return

    lines = [
        f"ОНзС {num} за период {d1.strftime('%d.%m.%Y')}–{d2.strftime('%d.%m.%Y')}:"
    ]
    for _, r in df2.head(50).iterrows():
        try:
            d_str = pd.to_datetime(r[col_date]).strftime("%d.%m.%Y")
        except Exception:
            d_str = str(r[col_date])
        lines.append(f"• {d_str} — {r.to_dict()}")

    await update.message.reply_text("\n".join(lines))


async def handle_inspector_step(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    step = context.user_data.get("insp_step")
    if not step:
        return

    form = context.user_data.get("insp_form", {})
    text = (update.message.text or "").strip()

    if step == "date_departure":
        try:
            d = datetime.strptime(text, "%d.%m.%Y").date()
        except Exception:
            await update.message.reply_text(
                "Не понял дату выезда. Введите в формате ДД.ММ.ГГГГ, например 03.12.2025."
            )
            return
        form["date_departure"] = d
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "date_final"
        await update.message.reply_text(
            "Дата начала итоговой проверки (ДД.ММ.ГГГГ) "
            "или напишите «нет», если пока нет:"
        )
        return

    if step == "date_final":
        if text.lower() in ("нет", "-", "—", "0", "n/a", "na"):
            form["date_final"] = ""
        else:
            try:
                d = datetime.strptime(text, "%d.%m.%Y").date()
            except Exception:
                await update.message.reply_text(
                    "Не понял дату. Введите в формате ДД.ММ.ГГГГ или «нет»."
                )
                return
            form["date_final"] = d
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "area"
        await update.message.reply_text("Площадь (кв.м):")
        return

    if step == "area":
        form["area"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "floors"
        await update.message.reply_text("Количество этажей:")
        return

    if step == "floors":
        form["floors"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "onzs"
        await update.message.reply_text("ОНзС (1–12):")
        return

    if step == "onzs":
        form["onzs"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "developer"
        await update.message.reply_text("Наименование застройщика:")
        return

    if step == "developer":
        form["developer"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "object"
        await update.message.reply_text("Наименование объекта:")
        return

    if step == "object":
        form["object"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "address"
        await update.message.reply_text("Строительный адрес:")
        return

    if step == "address":
        form["address"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "case_no"
        await update.message.reply_text("Номер дела (формат 00-00-000000):")
        return

    if step == "case_no":
        form["case_no"] = text
        context.user_data["insp_form"] = form
        context.user_data["insp_step"] = "check_type"
        await update.message.reply_text(
            "Вид проверки (ПП, итоговая, профвизит, запрос ОНзС, поручение руководства):"
        )
        return

    if step == "check_type":
        form["check_type"] = text
        ok = append_inspector_row_to_excel(form)
        context.user_data["insp_form"] = {}
        context.user_data["insp_step"] = None

        if ok:
            await update.message.reply_text(
                f"Выезд сохранён в лист «{INSPECTOR_SHEET_NAME}».",
                reply_markup=main_menu(),
            )
        else:
            await update.message.reply_text(
                "Не удалось сохранить выезд в Google Sheets. Сообщите администратору.",
                reply_markup=main_menu(),
            )
        return


async def handle_analytics_password(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not context.user_data.get("await_analytics_pass"):
        return

    pwd = (update.message.text or "").strip()
    context.user_data["await_analytics_pass"] = False

    if pwd != ANALYTICS_PASSWORD:
        await update.message.reply_text("Неверный пароль.")
        return

    file_names = get_schedule_file_names()

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT decision, COUNT(*) AS c FROM approvals GROUP BY decision")
    appr = {row["decision"]: row["c"] for row in c.fetchall()}

    c.execute(
        """SELECT COUNT(*) AS c FROM remarks_status
           WHERE pb_status='нет' OR pbzk_status='нет' OR ar_status='нет'"""
    )
    not_done = c.fetchone()["c"]

    c.execute(
        """SELECT COUNT(*) AS c FROM remarks_status
           WHERE pb_status='да' OR pbzk_status='да' OR ar_status='да'"""
    )
    done = c.fetchone()["c"]

    c.execute(
        """SELECT approver, decision, COUNT(*) AS c 
           FROM approvals GROUP BY approver, decision"""
    )
    rows = c.fetchall()

    c.execute(
        """SELECT schedule_version, approver, decision, comment, decided_at
           FROM approvals
           ORDER BY datetime(decided_at) DESC
           LIMIT 10"""
    )
    hist = c.fetchall()
    conn.close()

    lines = ["📈 Аналитика:", ""]
    lines.append("1️⃣ Согласование графика:")
    lines.append(f" • Согласовано: {appr.get('approve', 0)}")
    lines.append(f" • На доработку: {appr.get('rework', 0)}")
    lines.append("")
    lines.append("2️⃣ Замечания (по вручную изменённым статусам):")
    lines.append(f" • Есть устранённые (есть «да»): {done}")
    lines.append(f" • Есть неустранённые (есть «нет»): {not_done}")
    lines.append("")
    lines.append("3️⃣ По согласующим:")

    if rows:
        for r in rows:
            lines.append(
                f" • {r['approver'] or '—'}: {r['decision']} — {r['c']} раз(а)"
            )
    else:
        lines.append(" • пока нет данных")

    lines.append("")
    lines.append("4️⃣ Последние решения по графику:")

    if hist:
        for r in hist:
            ver = r["schedule_version"] or "-"
            try:
                ver_int = int(ver)
            except Exception:
                ver_int = 0

            name = file_names.get(ver_int)
            if name:
                file_label = f"{name} (версия {ver_int})"
            else:
                file_label = f"Версия {ver}"

            appr_label = r["approver"] or "—"
            decision = r["decision"]
            if decision == "approve":
                dec_text = "Согласовано"
            elif decision == "rework":
                dec_text = "На доработку"
            else:
                dec_text = decision or "—"

            dt_raw = r["decided_at"] or ""
            try:
                dt_obj = datetime.fromisoformat(dt_raw)
                dt_str = dt_obj.strftime("%d.%m.%Y %H:%M")
            except Exception:
                dt_str = dt_raw

            comment = f" (комментарий: {r['comment']})" if r["comment"] else ""
            lines.append(
                f" • {file_label}: {appr_label} — {dec_text} {dt_str}{comment}"
            )
    else:
        lines.append(" • пока нет решений по графику")

    await update.message.reply_text("\n".join(lines))


async def handle_schedule_name_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    ver = context.user_data.get("await_schedule_name")
    if not ver:
        return

    name = (update.message.text or "").strip()
    context.user_data["await_schedule_name"] = None

    if not name:
        await update.message.reply_text(
            "Пустое название. Имя файла не сохранено.", reply_markup=main_menu()
        )
        return

    try:
        ver_int = int(ver)
    except Exception:
        ver_int = 0

    set_schedule_file_name(ver_int, name)
    await update.message.reply_text(
        f"Название графика сохранено: {name} (версия {ver_int}).",
        reply_markup=main_menu(),
    )


# ----------------- Документы -----------------
# (загрузка файлов отключена, т.к. всё ведётся в Google Sheets)


async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Оставляем хендлер, но просто сообщаем, что загрузка не нужна
    if not update.message or not update.message.document:
        return

    await update.message.reply_text(
        "Файлы больше загружать не нужно — данные ведутся напрямую в Google Sheets."
    )


# ----------------- Роутер текста -----------------


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("await_analytics_pass"):
        await handle_analytics_password(update, context)
        return

    if context.user_data.get("await_schedule_name"):
        await handle_schedule_name_input(update, context)
        return

    if context.user_data.get("insp_step"):
        await handle_inspector_step(update, context)
        return

    if context.user_data.get("await_custom_approver"):
        await handle_custom_approver_input(update, context)
        return

    if context.user_data.get("await_remarks_row"):
        await handle_remarks_row_input(update, context)
        return

    if context.user_data.get("onzs_custom"):
        await handle_onzs_custom_input(update, context)
        return

    await main_menu_handler(update, context)


# ----------------- MAIN -----------------


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в переменных окружения или .env")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("set_schedule_group", cmd_set_schedule_group))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    log.info("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()

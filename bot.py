import logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from io import BytesIO
from typing import Optional, Dict, Any, List

import json
import requests
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

AnyType = Any

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

# ----------------- НАСТРОЙКИ И .ENV -----------------
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))
ANALYTICS_PASSWORD = "051995"

# Сервисный аккаунт для Google API
GSHEETS_SERVICE_ACCOUNT_JSON = os.getenv("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()

# Старый ID оставляем как fallback
GSHEETS_SPREADSHEET_ID_ENV = os.getenv(
    "GSHEETS_SPREADSHEET_ID",
    "",
).strip()

# URL файлов (важно: здесь уже стоит НОВАЯ таблица)
REMARKS_URL = os.getenv("REMARKS_URL", "").strip()
SCHEDULE_URL = os.getenv("SCHEDULE_URL", "").strip()

SHEETS_SERVICE = None  # кеш клиента Google Sheets

DEFAULT_APPROVERS = [
    "@asdinamitif",
    "@FrolovAlNGSN",
    "@cappit_G59",
    "@sergeybektiashkin",
    "@scri4",
    "@Kirill_Victorovi4",
]

RESPONSIBLE_USERNAMES: Dict[str, List[str]] = {
    "бектяшкин": ["sergeybektiashkin"],
    "смирнов": ["scri4"],
}

# Лист для инспектора
INSPECTOR_SHEET_NAME = "ПБ, АР,ММГН, АГО (2025)"

# Администраторы бота
HARD_CODED_ADMINS = {398960707}

# Куда слать готовый согласованный график
SCHEDULE_NOTIFY_CHAT_ID_ENV = os.getenv("SCHEDULE_NOTIFY_CHAT_ID", "").strip()
SCHEDULE_NOTIFY_CHAT_ID = (
    int(SCHEDULE_NOTIFY_CHAT_ID_ENV) if SCHEDULE_NOTIFY_CHAT_ID_ENV else None
)


def is_admin(uid: int) -> bool:
    return uid in HARD_CODED_ADMINS


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


def get_current_remarks_sheet_name() -> str:
    year = local_now().year
    return f"ПБ, АР,ММГН, АГО ({year})"


# -------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ: ID таблицы из URL
# -------------------------------------------------
def _extract_sheet_id_from_url(url: str) -> Optional[str]:
    if "spreadsheets/d/" not in url:
        return None
    try:
        part = url.split("spreadsheets/d/")[1]
        part = part.split("/")[0]
        part = part.split("?")[0]
        return part
    except Exception:
        return None


SPREADSHEET_ID = (
    _extract_sheet_id_from_url(SCHEDULE_URL)
    or _extract_sheet_id_from_url(REMARKS_URL)
    or GSHEETS_SPREADSHEET_ID_ENV
)

if not SPREADSHEET_ID:
    log.error("Не удалось определить ID Google Sheets. Проверьте переменные окружения.")


# -------------------------------------------------
# Google Sheets helpers
# -------------------------------------------------
def get_sheets_service():
    """
    Возвращает объект сервиса Google Sheets (кешируется в SHEETS_SERVICE).
    Используется для графика, замечаний и записи инспектора.
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
    """Ссылка на экспорт Google Sheets в .xlsx по ID таблицы."""
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"


def detect_header_row(values: List[List[str]]) -> int:
    """Пытается найти строку заголовков по наличию 'дата выезда'."""
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


# -------------------------------------------------
# Работа со столбцами Excel
# -------------------------------------------------
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


# -------------------------------------------------
# БАЗА ДАННЫХ (график + согласование)
# -------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """
    Создаёт все таблицы:
    - schedule_settings
    - approvers
    - schedule_files
    - schedule_approvals
    """
    conn = get_db()
    c = conn.cursor()

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
        """CREATE TABLE IF NOT EXISTS schedule_files (
               version INTEGER PRIMARY KEY,
               name TEXT,
               uploaded_at TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS schedule_approvals (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               version INTEGER,
               approver TEXT,
               status TEXT,           -- pending / approved / rework
               comment TEXT,
               decided_at TEXT,
               requested_at TEXT
           )"""
    )

    # начальные настройки
    c.execute("SELECT COUNT(*) AS c FROM approvers")
    if c.fetchone()["c"] == 0:
        c.executemany(
            "INSERT OR IGNORE INTO approvers (label) VALUES (?)",
            [(lbl,) for lbl in DEFAULT_APPROVERS],
        )

    c.execute("SELECT value FROM schedule_settings WHERE key='schedule_version'")
    if not c.fetchone():
        c.execute(
            "INSERT INTO schedule_settings (key, value) VALUES ('schedule_version', '1')"
        )

    c.execute("SELECT value FROM schedule_settings WHERE key='last_notified_version'")
    if not c.fetchone():
        c.execute(
            "INSERT INTO schedule_settings (key, value) VALUES ('last_notified_version', '0')"
        )

    conn.commit()
    conn.close()


# -------------------------------------------------
# Получение состояния графика
# -------------------------------------------------
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
        arr = [v.strip() for v in val.split(",") if v.strip()]
        if arr:
            return arr
    return []


def set_current_approvers_for_version(approvers: List[str], version: int) -> None:
    conn = get_db()
    c = conn.cursor()

    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('current_approvers', ?)",
        (",".join(approvers),),
    )

    # очистка старых статусов
    c.execute("DELETE FROM schedule_approvals WHERE version = ?", (version,))

    now = local_now().isoformat()
    for appr in approvers:
        c.execute(
            """INSERT INTO schedule_approvals
               (version, approver, status, comment, decided_at, requested_at)
               VALUES (?, ?, 'pending', NULL, NULL, ?)""",
            (version, appr, now),
        )

    conn.commit()
    conn.close()


def get_schedule_approvals(version: int) -> List[sqlite3.Row]:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM schedule_approvals WHERE version = ? ORDER BY approver",
        (version,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def update_schedule_approval_status(
    version: int, approver: str, status: str, comment: Optional[str] = None
):
    conn = get_db()
    c = conn.cursor()
    now = local_now().isoformat()

    c.execute(
        """UPDATE schedule_approvals
           SET status=?, comment=?, decided_at=?
         WHERE version=? AND approver=?""",
        (status, comment, now, version, approver),
    )
    conn.commit()
    conn.close()


def get_last_notified_version() -> int:
    settings = get_schedule_state()
    try:
        return int(settings.get("last_notified_version") or "0")
    except Exception:
        return 0


def set_last_notified_version(version: int) -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('last_notified_version', ?)",
        (str(version),),
    )
    conn.commit()
    conn.close()


def is_schedule_fully_approved(version: int) -> bool:
    approvals = get_schedule_approvals(version)
    if not approvals:
        return False
    return all(r["status"] == "approved" for r in approvals)


# -------------------------------------------------
# Клавиатуры
# -------------------------------------------------
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📅 График", "📊 Итоговая"],
            ["📝 Замечания", "🏗 ОНзС"],
            ["Инспектор", "📈 Аналитика"],
        ],
        resize_keyboard=True,
    )


def build_schedule_inline(is_admin_flag: bool, settings: dict):
    buttons = [
        [
            InlineKeyboardButton("🔄 Обновить", callback_data="schedule_refresh"),
            InlineKeyboardButton("📥 Скачать", callback_data="schedule_download"),
        ],
        [InlineKeyboardButton("📤 Загрузить", callback_data="schedule_upload")],
    ]
    if is_admin_flag:
        buttons.append(
            [InlineKeyboardButton("👥 Согласующие", callback_data="schedule_approvers")]
        )
    return InlineKeyboardMarkup(buttons)


def remarks_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done")],
            [InlineKeyboardButton("📥 Скачать файл", callback_data="remarks_download")],
        ]
    )


def inspector_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("➕ Добавить выезд", callback_data="inspector_add")]]
    )


# -------------------------------------------------
# График: чтение листа «График»
# -------------------------------------------------
def get_schedule_df() -> Optional[pd.DataFrame]:
    SHEET = "График"
    if not SPREADSHEET_ID:
        return None

    url = build_export_url(SPREADSHEET_ID)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error("Ошибка скачивания Excel для графика: %s", e)
        return None

    try:
        xls = pd.ExcelFile(BytesIO(resp.content))
        if SHEET not in xls.sheet_names:
            return None
        df = pd.read_excel(xls, sheet_name=SHEET)
        df = df.dropna(how="all").reset_index(drop=True)
        return df
    except Exception as e:
        log.error("Ошибка чтения листа графика: %s", e)
        return None


# -------------------------------------------------
# Вспомогательное: заголовок по датам согласования
# -------------------------------------------------
def _format_dt(iso_str: Optional[str]) -> str:
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso_str


def _compute_period_label_from_approvals(version: int, approvals: List[sqlite3.Row]) -> str:
    approved_dates: List[datetime] = []
    for r in approvals:
        if r["status"] == "approved" and r["decided_at"]:
            try:
                approved_dates.append(datetime.fromisoformat(r["decided_at"]))
            except Exception:
                pass

    if not approved_dates:
        return f"📅 График выездов (версия {version})"

    # Берём дату последнего согласования
    last = max(approved_dates).date()
    date_from = last
    date_to = last + timedelta(days=4)  # 5 дней включая дату согласования

    return f"📅 График выездов с {date_from:%d.%m.%Y} по {date_to:%d.%m.%Y} г"


def compute_period_label(version: int) -> str:
    approvals = get_schedule_approvals(version)
    return _compute_period_label_from_approvals(version, approvals)


# -------------------------------------------------
# Текст графика со статусами
# -------------------------------------------------
def build_schedule_text(is_admin_flag: bool, settings: dict) -> str:
    version = get_schedule_version(settings)
    approvers = get_current_approvers(settings)
    approvals = get_schedule_approvals(version)

    lines: List[str] = []

    header = _compute_period_label_from_approvals(version, approvals)
    lines.append(header)
    lines.append("")

    if not approvers:
        lines.append("Согласующие не назначены.")
        return "\n".join(lines)

    pending: List[str] = []
    approved_rows: List[sqlite3.Row] = []
    rework: List[sqlite3.Row] = []

    by_approver = {r["approver"]: r for r in approvals}

    for a in approvers:
        r = by_approver.get(a)
        if not r or r["status"] == "pending":
            pending.append(a)
        elif r["status"] == "approved":
            approved_rows.append(r)
        elif r["status"] == "rework":
            rework.append(r)

    if rework:
        lines.append("Отправлено на доработку:")
        for r in rework:
            lines.append(
                f"• {r['approver']} — {_format_dt(r['decided_at'])} "
                f"(Комментарий: {r['comment'] or 'нет'})"
            )
    elif pending:
        lines.append("На согласовании у:")
        for a in pending:
            req = _format_dt(by_approver[a]["requested_at"])
            lines.append(f"• {a} — запрошено {req}")
        if approved_rows:
            lines.append("")
            lines.append("Уже согласовали:")
            for r in approved_rows:
                lines.append(f"• {r['approver']} — {_format_dt(r['decided_at'])} ✅")
    else:
        lines.append("Согласовано всеми:")
        for r in approved_rows:
            lines.append(f"• {r['approver']} — {_format_dt(r['decided_at'])} ✅")

    return "\n".join(lines)


# -------------------------------------------------
# Замечания: НЕ УСТРАНЕНЫ
# -------------------------------------------------
def build_remarks_not_done_text(df: pd.DataFrame) -> str:
    COLS = {
        "case": "I",
        "pb": "Q",
        "pb_zk": "R",
        "ar": "X",
        "eom": "AD",
    }

    TITLES = {
        "pb": "Отметка об устранении замечаний ПБ да/нет",
        "pb_zk": "Отметка об устранении замечаний ПБ в ЗК КНД да/нет",
        "ar": "Отметка об устранении нарушений АР, ММГН, АГО да/нет",
        "eom": "Отметка об устранении нарушений ЭОМ да/нет",
    }

    idx_case = excel_col_to_index(COLS["case"])
    idx_pb = excel_col_to_index(COLS["pb"])
    idx_pb_zk = excel_col_to_index(COLS["pb_zk"])
    idx_ar = excel_col_to_index(COLS["ar"])
    idx_eom = excel_col_to_index(COLS["eom"])

    def is_net(val):
        if val is None:
            return False
        text = str(val).lower().replace("\n", " ").strip()
        if not text or text in {"-", "н/д"}:
            return False
        return text.startswith("нет")

    grouped: Dict[str, Dict[str, set]] = {}

    for _, row in df.iterrows():
        case = str(row.iloc[idx_case]).strip()
        if not case:
            continue

        flags = {
            "pb": is_net(row.iloc[idx_pb]),
            "pb_zk": is_net(row.iloc[idx_pb_zk]),
            "ar": is_net(row.iloc[idx_ar]),
            "eom": is_net(row.iloc[idx_eom]),
        }

        if not any(flags.values()):
            continue

        if case not in grouped:
            grouped[case] = {"pb": set(), "ar": set(), "eom": set()}

        if flags["pb"]:
            grouped[case]["pb"].add(TITLES["pb"])
        if flags["pb_zk"]:
            grouped[case]["pb"].add(TITLES["pb_zk"])
        if flags["ar"]:
            grouped[case]["ar"].add(TITLES["ar"])
        if flags["eom"]:
            grouped[case]["eom"].add(TITLES["eom"])

    if not grouped:
        return "Во всех строках нет статусов «нет»."

    lines = [
        "Строки со статусом «НЕ УСТРАНЕНЫ (нет)»",
        "",
        "Лист: " + get_current_remarks_sheet_name(),
        "",
    ]

    for case, blocks in grouped.items():
        parts: List[str] = []
        if blocks["pb"]:
            parts.append(
                "Пожарная безопасность: "
                + ", ".join(b + " - нет" for b in blocks["pb"])
            )
        if blocks["ar"]:
            parts.append(
                "Архитектура, ММГН, АГО: "
                + ", ".join(b + " - нет" for b in blocks["ar"])
            )
        if blocks["eom"]:
            parts.append(
                "Электроснабжение: "
                + ", ".join(b + " - нет" for b in blocks["eom"])
            )
        lines.append(f"• {case} — " + "; ".join(parts))

    return "\n".join(lines)


# -------------------------------------------------
# Отправка длинного текста
# -------------------------------------------------
async def send_long_text(chat, text: str, chunk_size=3500):
    lines = text.split("\n")
    buf = ""

    for line in lines:
        if len(buf) + len(line) + 1 > chunk_size:
            await chat.send_message(buf)
            buf = line
        else:
            buf = buf + "\n" + line if buf else line

    if buf:
        await chat.send_message(buf)


# -------------------------------------------------
# Считывание листа замечаний
# -------------------------------------------------
def get_remarks_df_current() -> Optional[pd.DataFrame]:
    sheet = get_current_remarks_sheet_name()
    if not SPREADSHEET_ID:
        return None

    url = build_export_url(SPREADSHEET_ID)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        xls = pd.ExcelFile(BytesIO(resp.content))
        if sheet not in xls.sheet_names:
            return None
        return pd.read_excel(xls, sheet_name=sheet)
    except Exception as e:
        log.error("Ошибка чтения файла замечаний: %s", e)
        return None


# -------------------------------------------------
# Функция записи инспектора в Google Sheets
# -------------------------------------------------
def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets API недоступен (ключ или права).")
        return False

    if not SPREADSHEET_ID:
        log.error("SPREADSHEET_ID не задан.")
        return False

    try:
        # B – Дата выезда
        dep_date = form.get("date")
        if isinstance(dep_date, (datetime, date)):
            dep_str = dep_date.strftime("%d.%m.%Y")
        else:
            dep_str = str(dep_date or "")

        # C – Дата начала итоговой проверки (может быть пустой)
        fin_date = form.get("final_date")
        if isinstance(fin_date, (datetime, date)):
            fin_str = fin_date.strftime("%d.%m.%Y")
        else:
            fin_str = str(fin_date or "")

        # D – Площадь / Этажность
        d_value = (
            f"Площадь (кв.м): {form.get('area', '')}; "
            f"Количество этажей: {form.get('floors', '')}"
        )

        row = [
            dep_str,                    # B – Дата выезда
            fin_str,                    # C – Дата начала итоговой
            d_value,                    # D – Площадь/этажи
            form.get("onzs", ""),       # E – ОНзС
            form.get("developer", ""),  # F – Застройщик
            form.get("object", ""),     # G – Объект
            form.get("address", ""),    # H – Адрес
            form.get("case", ""),       # I – Номер дела
            form.get("check_type", ""), # J – Вид проверки
        ]

        body = {"values": [row]}

        response = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{INSPECTOR_SHEET_NAME}'!B:J",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )

        log.info("Инспектор: запись добавлена: %s", response)
        return True

    except Exception as e:
        log.error("Ошибка записи инспектора в Google Sheets: %s", e)
        return False


# -------------------------------------------------
# Инспектор — пошаговое заполнение
# -------------------------------------------------
async def inspector_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    form = context.user_data.get("inspector_form", {})
    step = form.get("step")

    # 1) Дата выезда
    if step == "date":
        try:
            form["date"] = datetime.strptime(text, "%d.%m.%Y").date()
            form["step"] = "final_date"
            await update.message.reply_text(
                "Дата начала итоговой проверки (ДД.ММ.ГГГГ).\n"
                "Если ещё не назначена — отправьте «-»."
            )
        except Exception:
            await update.message.reply_text("Введите дату в формате ДД.ММ.ГГГГ")
        return

    # 2) Дата начала итоговой проверки
    if step == "final_date":
        t = text.replace(" ", "")
        if t in {"-", "—", "нет", "н/д", ""}:
            form["final_date"] = ""
        else:
            try:
                form["final_date"] = datetime.strptime(text, "%d.%m.%Y").date()
            except Exception:
                await update.message.reply_text(
                    "Введите дату в формате ДД.ММ.ГГГГ или «-», если ещё нет даты."
                )
                return
        form["step"] = "area"
        await update.message.reply_text("Площадь (кв.м):")
        return

    # 3) Площадь
    if step == "area":
        form["area"] = text
        form["step"] = "floors"
        await update.message.reply_text("Количество этажей:")
        return

    # 4) Этажность
    if step == "floors":
        form["floors"] = text
        form["step"] = "onzs"
        await update.message.reply_text("ОНзС (1–12):")
        return

    # 5) ОНзС
    if step == "onzs":
        form["onzs"] = text
        form["step"] = "developer"
        await update.message.reply_text("Застройщик:")
        return

    # 6) Застройщик
    if step == "developer":
        form["developer"] = text
        form["step"] = "object"
        await update.message.reply_text("Название объекта:")
        return

    # 7) Объект
    if step == "object":
        form["object"] = text
        form["step"] = "address"
        await update.message.reply_text("Строительный адрес:")
        return

    # 8) Адрес
    if step == "address":
        form["address"] = text
        form["step"] = "case"
        await update.message.reply_text("Номер дела (00-00-000000):")
        return

    # 9) Номер дела
    if step == "case":
        form["case"] = text
        form["step"] = "check_type"
        await update.message.reply_text(
            "Введите вид проверки (ПП, итоговая, профвизит):"
        )
        return

    # 10) Вид проверки + запись
    if step == "check_type":
        form["check_type"] = text
        form["step"] = "done"

        await update.message.reply_text("Записываю в Google Sheets...")

        ok = append_inspector_row_to_excel(form)
        if ok:
            await update.message.reply_text("Выезд успешно записан в таблицу.")
        else:
            await update.message.reply_text(
                "Ошибка записи в таблицу: Google Sheets API недоступен (ключ или права)."
            )

        context.user_data["inspector_form"] = None
        return


# -------------------------------------------------
# ОНзС — клавиатура и вывод по цифре 1–12
# -------------------------------------------------
def onzs_menu_inline() -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for i in range(1, 13):
        row.append(InlineKeyboardButton(str(i), callback_data=f"onzs_filter_{i}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


def build_onzs_list_by_number(df: pd.DataFrame, number: str) -> str:
    col_case = get_col_by_letter(df, "I")  # Номер дела
    col_onzs = get_col_by_letter(df, "E")  # ОНзС
    col_addr = get_col_by_letter(df, "H")  # Адрес

    if not col_case or not col_onzs:
        return "Не удалось определить структуру файла."

    df_f = df[df[col_onzs].astype(str).str.strip() == str(number).strip()]

    if df_f.empty:
        return f"Нет объектов с ОНзС = {number}."

    lines = [f"ОНзС = {number}", ""]

    for _, row in df_f.iterrows():
        case_no = str(row[col_case]).strip()
        addr = str(row[col_addr]).strip() if col_addr else ""
        if addr:
            lines.append(f"• {case_no} — {addr}")
        else:
            lines.append(f"• {case_no}")

    return "\n".join(lines)


# -------------------------------------------------
# Отправка графика в группу после полного согласования
# -------------------------------------------------
async def notify_schedule_approved(
    version: int, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not SCHEDULE_NOTIFY_CHAT_ID:
        return

    if not is_schedule_fully_approved(version):
        return

    last_notified = get_last_notified_version()
    if version <= last_notified:
        return

    df = get_schedule_df()
    if df is None or df.empty:
        await context.bot.send_message(
            chat_id=SCHEDULE_NOTIFY_CHAT_ID,
            text=f"{compute_period_label(version)}\n(Не удалось приложить файл графика.)",
        )
    else:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="График", index=False)
        buf.seek(0)
        filename = f"График_версия_{version}.xlsx"

        await context.bot.send_document(
            chat_id=SCHEDULE_NOTIFY_CHAT_ID,
            document=InputFile(buf, filename=filename),
            caption=compute_period_label(version),
        )

    set_last_notified_version(version)


# -------------------------------------------------
# CALLBACK HANDLER
# -------------------------------------------------
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user = query.from_user
    await query.answer()

    settings = get_schedule_state()
    version = get_schedule_version(settings)

    # ---------- ГРАФИК ----------
    if data == "schedule_refresh":
        df = get_schedule_df()
        if df is None:
            await query.message.reply_text("Не удалось прочитать лист «График».")
        else:
            await query.message.reply_text(f"Лист «График» прочитан, строк: {len(df)}.")
        return

    if data == "schedule_download":
        df = get_schedule_df()
        if df is None or df.empty:
            await query.message.reply_text(
                "Не удалось получить лист «График» для выгрузки."
            )
            return

        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="График", index=False)
        buf.seek(0)

        filename = f"График_{local_now().date().isoformat()}.xlsx"
        await query.message.reply_document(
            document=InputFile(buf, filename=filename),
            caption="Файл графика (только лист «График»).",
        )
        return

    if data == "schedule_upload":
        await query.message.reply_text("Загрузка графика в этой сборке не реализована.")
        return

    if data == "schedule_approvers":
        if not is_admin(user.id):
            await query.message.reply_text(
                "Только администратор может настраивать согласующих."
            )
            return
        context.user_data["awaiting_approvers_input"] = {"version": version}
        await query.message.reply_text(
            "Отправьте список согласующих (юзернеймы через пробел/запятую/новую строку), например:\n"
            "@asdinamitif @FrolovAlNGSN @cappit_G59"
        )
        return

    # ---------- Согласование графика ----------
    if data.startswith("schedule_approve:") or data.startswith("schedule_rework:"):
        action, approver_tag = data.split(":", 1)
        user_username = user.username or ""
        user_tag = f"@{user_username}" if user_username else ""

        if user_tag.lower() != approver_tag.lower():
            await query.answer(
                text=f"Эта кнопка предназначена для {approver_tag}.",
                show_alert=True,
            )
            return

        if action == "schedule_approve":
            update_schedule_approval_status(version, approver_tag, "approved", None)
            await query.message.reply_text(
                f"{approver_tag} согласовал(а) график. Спасибо!"
            )
            # Проверяем, не стало ли всё согласовано
            await notify_schedule_approved(version, context)
            return

        if action == "schedule_rework":
            context.user_data["awaiting_rework_comment"] = {
                "version": version,
                "approver": approver_tag,
            }
            await query.message.reply_text(
                "Напишите комментарий, почему график нужно доработать."
            )
            return

    # ---------- ЗАМЕЧАНИЯ ----------
    if data == "remarks_not_done":
        await query.message.reply_text("Ищу строки со статусом «нет»...")
        df = get_remarks_df_current()
        if df is None:
            await query.message.reply_text(
                "Не удалось получить файл замечаний. Проверьте доступ к таблице."
            )
            return
        text = build_remarks_not_done_text(df)
        await send_long_text(query.message.chat, text)
        return

    if data == "remarks_download":
        if REMARKS_URL:
            await query.message.reply_text(
                "Файл замечаний можно открыть по ссылке:\n" f"{REMARKS_URL}"
            )
        else:
            await query.message.reply_text("Ссылка на файл замечаний не настроена.")
        return

    # ---------- ОНЗС (1–12) ----------
    if data.startswith("onzs_filter_"):
        number = data.replace("onzs_filter_", "")
        df = get_remarks_df_current()
        if df is None:
            await query.message.reply_text("Не удалось открыть таблицу ОНзС.")
            return
        text = build_onzs_list_by_number(df, number)
        await send_long_text(query.message.chat, text)
        return

    # ---------- ИНСПЕКТОР ----------
    if data == "inspector_add":
        context.user_data["inspector_form"] = {"step": "date"}
        await query.message.reply_text("Дата выезда (ДД.ММ.ГГГГ):")
        return


# -------------------------------------------------
# TEXT ROUTER
# -------------------------------------------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    chat = update.message.chat

    # --- комментарий от "На доработку" ---
    if context.user_data.get("awaiting_rework_comment"):
        info = context.user_data.pop("awaiting_rework_comment")
        version = info["version"]
        approver = info["approver"]
        comment = text
        update_schedule_approval_status(version, approver, "rework", comment)
        await update.message.reply_text(
            "Комментарий сохранён. График помечен как отправленный на доработку."
        )
        return

    # --- ввод согласующих ---
    if context.user_data.get("awaiting_approvers_input"):
        info = context.user_data.pop("awaiting_approvers_input")
        version = info["version"]

        raw = text.replace(",", " ").split()
        approvers: List[str] = []
        for token in raw:
            token = token.strip()
            if not token:
                continue
            if not token.startswith("@"):
                token = "@" + token
            approvers.append(token)
        approvers = list(dict.fromkeys(approvers))

        if not approvers:
            await update.message.reply_text("Не найдено ни одного юзернейма.")
            return

        set_current_approvers_for_version(approvers, version)

        lines = [
            "График на новую неделю, необходимо согласовать.",
            compute_period_label(version),
            "",
            "Согласующие:",
        ]
        for a in approvers:
            lines.append(f"• {a}")

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"✅ Согласовать ({a})", callback_data=f"schedule_approve:{a}"
                    ),
                    InlineKeyboardButton(
                        f"✏️ На доработку ({a})", callback_data=f"schedule_rework:{a}"
                    ),
                ]
                for a in approvers
            ]
        )

        await chat.send_message("\n".join(lines), reply_markup=kb)
        await update.message.reply_text("Согласующие сохранены и уведомлены.")
        return

    # --- обработка инспектора ---
    if context.user_data.get("inspector_form"):
        await inspector_process(update, context)
        return

    low = text.lower()

    # ---------- МЕНЮ ----------
    if low == "📅 график".lower():
        settings = get_schedule_state()
        is_adm = is_admin(update.effective_user.id)
        msg = build_schedule_text(is_adm, settings)
        kb = build_schedule_inline(is_adm, settings)
        await update.message.reply_text(msg, reply_markup=kb)
        return

    if low == "📊 итоговая".lower():
        await update.message.reply_text("Раздел «Итоговая» пока в упрощённом виде.")
        return

    if low == "📝 замечания".lower():
        kb = remarks_menu_inline()
        await update.message.reply_text("Раздел «Замечания»:", reply_markup=kb)
        return

    if low == "🏗 онзс".lower():
        kb = onzs_menu_inline()
        await update.message.reply_text("Выберите ОНзС (1–12):", reply_markup=kb)
        return

    if low == "инспектор":
        kb = inspector_menu_inline()
        await update.message.reply_text("Раздел «Инспектор»:", reply_markup=kb)
        return

    if low == "📈 аналитика".lower():
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """SELECT version, approver, status, comment, decided_at, requested_at
               FROM schedule_approvals
               ORDER BY version DESC, approver"""
        )
        rows = c.fetchall()
        conn.close()

        if not rows:
            await update.message.reply_text("Пока нет данных по согласованию графика.")
            return

        lines: List[str] = ["📈 Аналитика по согласованию графика:", ""]
        cur_ver: Optional[int] = None

        for r in rows:
            ver = r["version"]
            if ver != cur_ver:
                cur_ver = ver
                lines.append("")
                lines.append(compute_period_label(ver))

            appr = r["approver"]
            status = r["status"] or "pending"
            decided = _format_dt(r["decided_at"])
            requested = _format_dt(r["requested_at"])
            comment = r["comment"] or ""

            if status == "pending":
                lines.append(f"• {appr} — ожидает, запрошено {requested}")
            elif status == "approved":
                lines.append(f"• {appr} — Согласовано {decided} ✅")
            elif status == "rework":
                if comment:
                    lines.append(
                        f"• {appr} — На доработку {decided} (Комментарий: {comment})"
                    )
                else:
                    lines.append(f"• {appr} — На доработку {decided}")

        await send_long_text(chat, "\n".join(lines))
        return

    # --- DEFAULT ---
    await update.message.reply_text(
        "Я вас не понял. Выберите пункт меню или нажмите /start.",
        reply_markup=main_menu(),
    )


# -------------------------------------------------
# DOCUMENT HANDLER
# -------------------------------------------------
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Загрузка файлов отключена. Используйте Google Sheets."
    )


# -------------------------------------------------
# START / HELP
# -------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добро пожаловать в бота отдела СОТ.",
        reply_markup=main_menu(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Доступные разделы:\n"
        "• 📅 График\n"
        "• 📊 Итоговая\n"
        "• 📝 Замечания\n"
        "• 🏗 ОНзС\n"
        "• Инспектор\n"
        "• 📈 Аналитика"
    )


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан.")
        raise SystemExit("Укажите BOT_TOKEN.")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(CallbackQueryHandler(callback_handler))

    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    log.info("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()

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

from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.worksheet.table import Table, TableStyleInfo

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


def _extract_spreadsheet_id_from_url(url: str) -> str:
    """
    Пытается вытащить ID Google Spreadsheet из URL вида
    https://docs.google.com/spreadsheets/d/<ID>/edit...
    """
    try:
        if "/d/" in url:
            return url.split("/d/")[1].split("/")[0]
    except Exception:
        pass
    return ""


SCHEDULE_URL_ENV = (os.getenv("SCHEDULE_URL") or "").strip()

_default_sheet_id = _extract_spreadsheet_id_from_url(SCHEDULE_URL_ENV)
if not _default_sheet_id:
    _default_sheet_id = (os.getenv("GSHEETS_SPREADSHEET_ID") or "").strip()
if not _default_sheet_id:
    _default_sheet_id = "1W_9Cs-LaX6KR4cE9xN71CliE6Lm_TyQqk8t3kQa4FCc"

GSHEETS_SPREADSHEET_ID = _default_sheet_id

if SCHEDULE_URL_ENV:
    GOOGLE_SHEET_URL_DEFAULT = SCHEDULE_URL_ENV
else:
    GOOGLE_SHEET_URL_DEFAULT = (
        f"https://docs.google.com/spreadsheets/d/{GSHEETS_SPREADSHEET_ID}/edit?usp=sharing"
    )

GSHEETS_SERVICE_ACCOUNT_JSON = (os.getenv("GSHEETS_SERVICE_ACCOUNT_JSON") or "").strip()
SHEETS_SERVICE = None

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

INSPECTOR_SHEET_NAME = "ПБ, АР,ММГН, АГО (2025)"
HARD_CODED_ADMINS = {398960707}

SCHEDULE_NOTIFY_CHAT_ID_ENV = (os.getenv("SCHEDULE_NOTIFY_CHAT_ID") or "").strip()
SCHEDULE_NOTIFY_CHAT_ID = (
    int(SCHEDULE_NOTIFY_CHAT_ID_ENV) if SCHEDULE_NOTIFY_CHAT_ID_ENV else None
)

# ВТОРАЯ ТАБЛИЦА — итоговые проверки
FINAL_CHECKS_SPREADSHEET_ID = (
    os.getenv(
        "FINAL_CHECKS_SPREADSHEET_ID",
        "1dUO3neTKzKI3D8P6fs_LJLmWlL7jw-FhohtJkjz4KuE",
    ).strip()
)


FINAL_CHECKS_LOCAL_PATH = os.getenv(
    "FINAL_CHECKS_LOCAL_PATH",
    "final_checks.xlsx",
).strip()


def is_admin(uid: int) -> bool:
    return uid in HARD_CODED_ADMINS


def local_now() -> datetime:
    """Текущее время с учётом сдвига часового пояса."""
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


def get_current_remarks_sheet_name() -> str:
    """Имя листа замечаний по текущему году."""
    year = local_now().year
    return f"ПБ, АР,ММГН, АГО ({year})"


# -------------------------------------------------
# Google Sheets helpers
# -------------------------------------------------
def get_sheets_service():
    """
    Создаёт и кэширует клиент Google Sheets.
    """
    global SHEETS_SERVICE

    if SHEETS_SERVICE is not None:
        return SHEETS_SERVICE

    if not GSHEETS_SERVICE_ACCOUNT_JSON:
        log.error(
            "GSHEETS_SERVICE_ACCOUNT_JSON не задан – Google Sheets API недоступен."
        )
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
    Строит URL экспорта всей книги в формате XLSX.
    """
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"


def detect_header_row(values: List[List[str]]) -> int:
    """
    Пытается определить строку заголовков в первых 30 строках.
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
    Читает указанный лист Google Sheets в pandas.DataFrame.
    """
    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets сервис недоступен – невозможно прочитать лист.")
        return None

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=f"'{sheet_name}'!A1:ZZZ1000")
            .execute()
        )
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
    """
    Преобразует букву столбца Excel (например, "A", "AB") в индекс 0-based.
    """
    col = col.upper().strip()
    idx = 0
    for ch in col:
        if "A" <= ch <= "Z":
            idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_col_by_letter(df: pd.DataFrame, letters: str) -> Optional[str]:
    """
    Возвращает название столбца по букве Excel, если оно есть в df.columns.
    """
    idx = excel_col_to_index(letters)
    if 0 <= idx < len(df.columns):
        return df.columns[idx]
    return None


def get_col_index_by_header(
    df: pd.DataFrame, search_substr: str, fallback_letter: str
) -> Optional[int]:
    """
    Находит индекс столбца по подстроке в заголовке, либо возвращает индекс fallback_letter.
    """
    search_substr = search_substr.lower()
    for i, col in enumerate(df.columns):
        if search_substr in str(col).lower():
            return i
    idx = excel_col_to_index(fallback_letter)
    if 0 <= idx < len(df.columns):
        return idx
    return None


def normalize_onzs_value(val) -> Optional[str]:
    """
    Нормализация значения ОНзС:
    6, 6.0, '6 ', '6.0' -> '6'
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        n = int(float(s.replace(",", ".")))
        return str(n)
    except Exception:
        pass
    return s


def normalize_case_number(val) -> str:
    """
    Нормализация номера дела:
    - приводим тире к обычному '-'
    - убираем пробелы
    - оставляем только цифры и дефисы
    """
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""

    hyphens = ["\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"]
    for h in hyphens:
        s = s.replace(h, "-")

    s = s.replace(" ", "")

    cleaned_chars = []
    for ch in s:
        if ch.isdigit() or ch == "-":
            cleaned_chars.append(ch)

    return "".join(cleaned_chars)


def get_case_col_index(df: pd.DataFrame) -> Optional[int]:
    """
    Индекс столбца с номером дела в замечаниях.
    """
    idx_i = excel_col_to_index("I")
    if 0 <= idx_i < len(df.columns):
        return idx_i
    return get_col_index_by_header(df, "номер дела", "I")


# -------------------------------------------------
# БАЗА ДАННЫХ
# -------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
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
               status TEXT,
               comment TEXT,
               decided_at TEXT,
               requested_at TEXT
           )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS inspector_visits (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               date TEXT,
               area TEXT,
               floors TEXT,
               onzs TEXT,
               developer TEXT,
               object TEXT,
               address TEXT,
               case_no TEXT,
               check_type TEXT,
               created_at TEXT
           )"""
    )

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

    if SCHEDULE_NOTIFY_CHAT_ID_ENV:
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES (?, ?)",
            ("schedule_notify_chat_id", SCHEDULE_NOTIFY_CHAT_ID_ENV),
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


# -------------------------------------------------
# Инспектор: БД
# -------------------------------------------------
def save_inspector_to_db(form: Dict[str, Any]) -> bool:
    try:
        conn = get_db()
        c = conn.cursor()
        date_obj = form.get("date")
        date_str = date_obj.strftime("%Y-%m-%d") if date_obj else None
        c.execute(
            """INSERT INTO inspector_visits
               (date, area, floors, onzs, developer, object, address,
                case_no, check_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                date_str,
                form.get("area", ""),
                form.get("floors", ""),
                form.get("onzs", ""),
                form.get("developer", ""),
                form.get("object", ""),
                form.get("address", ""),
                form.get("case", ""),
                form.get("check_type", ""),
                local_now().isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log.error("Ошибка сохранения инспектора в локную БД: %s", e)
        return False


def fetch_inspector_visits(limit: int = 50) -> List[sqlite3.Row]:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """SELECT * FROM inspector_visits
           ORDER BY date DESC, id DESC
           LIMIT ?""",
        (limit,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def clear_inspector_visits() -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM inspector_visits")
    conn.commit()
    conn.close()


# -------------------------------------------------
# Клавиатуры
# -------------------------------------------------
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📅 График", "📝 Замечания"],
            ["Инспектор", "📈 Аналитика"],
            ["Итоговые проверки"],
        ],
        resize_keyboard=True,
    )


def build_schedule_inline(
    is_admin_flag: bool, settings: dict, user_tag: Optional[str] = None
) -> InlineKeyboardMarkup:
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

    approvers = get_current_approvers(settings)
    if user_tag and user_tag in approvers:
        buttons.append(
            [
                InlineKeyboardButton(
                    f"✅ Согласовать ({user_tag})",
                    callback_data=f"schedule_approve:{user_tag}",
                ),
                InlineKeyboardButton(
                    f"✏️ На доработку ({user_tag})",
                    callback_data=f"schedule_rework:{user_tag}",
                ),
            ]
        )

    return InlineKeyboardMarkup(buttons)


def remarks_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔎 Поиск по номеру дела", callback_data="remarks_search_case"
                )
            ],
            [InlineKeyboardButton("🏗 ОНзС", callback_data="remarks_onzs")],
            [InlineKeyboardButton("📥 Открыть файл", callback_data="remarks_download")],
        ]
    )


def inspector_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Добавить выезд", callback_data="inspector_add")],
            [
                InlineKeyboardButton("📋 Список выездов", callback_data="inspector_list"),
                InlineKeyboardButton(
                    "📥 Скачать Excel", callback_data="inspector_download"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔄 Обновить", callback_data="inspector_reset"
                )
            ],
        ]
    )


def final_checks_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 За неделю", callback_data="final_week"),
                InlineKeyboardButton("📆 За месяц", callback_data="final_month"),
            ],
            [
                InlineKeyboardButton(
                    "📊 Выбрать период", callback_data="final_period"
                )
            ],
            [
                InlineKeyboardButton(
                    "🔎 По номеру дела", callback_data="final_search_case"
                )
            ],
        ]
    )


# -------------------------------------------------
# График
# -------------------------------------------------
def get_schedule_df() -> Optional[pd.DataFrame]:
    """
    Загружает лист "График" из основной книги.
    """
    SHEET = "График"
    url = build_export_url(GSHEETS_SPREADSHEET_ID)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error("Ошибка скачивания Excel (график): %s", e)
        return None

    try:
        xls = pd.ExcelFile(BytesIO(resp.content))
        if SHEET not in xls.sheet_names:
            log.error("В файле нет листа '%s'", SHEET)
            return None
        df = pd.read_excel(xls, sheet_name=SHEET)
        df = df.dropna(how="all").reset_index(drop=True)
        return df
    except Exception as e:
        log.error("Ошибка чтения листа графика: %s", e)
        return None


HEADER_FILL = PatternFill(start_color="305496", end_color="305496", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)

... 

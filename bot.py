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
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


def get_current_remarks_sheet_name() -> str:
    year = local_now().year
    return f"ПБ, АР,ММГН, АГО ({year})"


# -------------------------------------------------
# Google Sheets helpers
# -------------------------------------------------
def get_sheets_service():
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
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"


def detect_header_row(values: List[List[str]]) -> int:
    for i, row in enumerate(values[:30]):
        row_lower = [str(c).lower() for c in row]
        if any("дата выезда" in c for c in row_lower):
            return i
    return 0


def read_sheet_to_dataframe(
    sheet_id: str, sheet_name: str, header_row_index: Optional[int] = None
) -> Optional[pd.DataFrame]:
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


def get_col_index_by_header(
    df: pd.DataFrame, search_substr: str, fallback_letter: str
) -> Optional[int]:
    search_substr = search_substr.lower()
    for i, col in enumerate(df.columns):
        if search_substr in str(col).lower():
            return i
    idx = excel_col_to_index(fallback_letter)
    if 0 <= idx < len(df.columns):
        return idx
    return None


def normalize_onzs_value(val) -> Optional[str]:
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
    Нормализация номера дела.
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


async def send_schedule_xlsx(
    chat_id: int, dataframe: pd.DataFrame, context: ContextTypes.DEFAULT_TYPE
):
    df = dataframe.copy().reset_index(drop=True)
    headers = list(df.columns)

    date_col_name: Optional[str] = None
    for h in headers:
        if "дата выезда" in str(h).lower():
            date_col_name = h
            break
    if date_col_name:
        try:
            df[date_col_name] = pd.to_datetime(
                df[date_col_name], errors="coerce", dayfirst=True
            )
        except Exception:
            pass

    settings = get_schedule_state()
    version = get_schedule_version(settings)
    approvals = get_schedule_approvals(version)

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(
            writer,
            sheet_name="График выездов",
            index=False,
            startrow=2,
            header=False,
        )

        wb = writer.book
        ws = writer.sheets["График выездов"]

        for col_num, value in enumerate(headers, 1):
            cell = ws.cell(row=2, column=col_num, value=value)
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for column in ws.columns:
            max_length = 0
            col_letter = column[0].column_letter
            for cell in column:
                try:
                    if cell.value is not None and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except Exception:
                    pass
            ws.column_dimensions[col_letter].width = min(max_length + 4, 50)

        ws.freeze_panes = ws["A3"]

        last_col_letter = ws.cell(row=2, column=len(headers)).column_letter
        ws.auto_filter.ref = f"A2:{last_col_letter}{len(df) + 2}"

        for row in ws[f"A3:{last_col_letter}{len(df) + 2}"]:
            for cell in row:
                cell.border = BORDER

        LIGHT_FILL = PatternFill(
            start_color="F0F0F0", end_color="F0F0F0", fill_type="solid"
        )
        for idx, row in enumerate(
            ws.iter_rows(min_row=3, max_row=len(df) + 2), start=3
        ):
            if idx % 2 == 0:
                for cell in row:
                    cell.fill = LIGHT_FILL

        tab = Table(
            displayName="ScheduleTable",
            ref=f"A2:{last_col_letter}{len(df) + 2}",
        )
        tab.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium9",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        ws.add_table(tab)

        date_idx = None
        onzs_idx = None
        dev_idx = None
        obj_idx = None

        for i, h in enumerate(headers, start=1):
            h_low = str(h).lower()
            if date_idx is None and "дата выезда" in h_low:
                date_idx = i
            if onzs_idx is None and "онзс" in h_low:
                onzs_idx = i
            if dev_idx is None and "наименование застройщика" in h_low:
                dev_idx = i
            if obj_idx is None and "наименование объекта" in h_low:
                obj_idx = i

        for row_idx in range(3, len(df) + 3):
            if date_idx:
                cell = ws.cell(row=row_idx, column=date_idx)
                cell.number_format = "DD.MM.YYYY"
            if onzs_idx:
                cell = ws.cell(row=row_idx, column=onzs_idx)
                cell.alignment = Alignment(
                    horizontal="center", vertical="center", wrap_text=False
                )
            if dev_idx:
                cell = ws.cell(row=row_idx, column=dev_idx)
                cell.alignment = Alignment(
                    horizontal="left", vertical="center", wrap_text=True
                )
            if obj_idx:
                cell = ws.cell(row=row_idx, column=obj_idx)
                cell.alignment = Alignment(
                    horizontal="left", vertical="center", wrap_text=True
                )

        if approvals:
            last_data_row = len(df) + 2
            summary_start = last_data_row + 2

            header = build_schedule_header(version, approvals)
            ws.merge_cells(f"A{summary_start}:{last_col_letter}{summary_start}")
            cell_header = ws[f"A{summary_start}"]
            cell_header.value = header
            cell_header.font = Font(bold=True, size=12, color="FFFFFF")
            cell_header.fill = PatternFill(
                start_color="4F81BD", end_color="4F81BD", fill_type="solid"
            )
            cell_header.alignment = Alignment(horizontal="center", vertical="center")

            sub_row = summary_start + 1
            ws.merge_cells(f"A{sub_row}:{last_col_letter}{sub_row}")
            cell_sub = ws[f"A{sub_row}"]
            cell_sub.value = "Согласовано всеми:"
            cell_sub.font = Font(bold=True, size=11)
            cell_sub.alignment = Alignment(horizontal="left", vertical="center")

            row_ptr = sub_row + 1
            approved_rows = [r for r in approvals if r["status"] == "approved"]
            others = [r for r in approvals if r["status"] != "approved"]

            list_fill = PatternFill(
                start_color="D9E1F2", end_color="D9E1F2", fill_type="solid"
            )

            for r in approved_rows:
                line = f"• {r['approver']} — {_format_dt(r['decided_at'])} ✅"
                ws.merge_cells(f"A{row_ptr}:{last_col_letter}{row_ptr}")
                cell = ws[f"A{row_ptr}"]
                cell.value = line
                cell.fill = list_fill
                cell.font = Font(size=11)
                cell.alignment = Alignment(horizontal="left", vertical="center")
                for col_idx in range(1, len(headers) + 1):
                    ws.cell(row=row_ptr, column=col_idx).border = BORDER
                row_ptr += 1

            if others:
                ws.merge_cells(f"A{row_ptr}:{last_col_letter}{row_ptr}")
                cell_pending = ws[f"A{row_ptr}"]
                cell_pending.value = "⚠ Есть несогласованные/на доработке."
                cell_pending.font = Font(italic=True, color="C00000")
                cell_pending.alignment = Alignment(
                    horizontal="left", vertical="center"
                )
                for col_idx in range(1, len(headers) + 1):
                    ws.cell(row=row_ptr, column=col_idx).border = BORDER

    bio.seek(0)
    filename = f"График_выездов_СОТ_{date.today().strftime('%d.%m.%Y')}.xlsx"

    await context.bot.send_document(
        chat_id=chat_id,
        document=InputFile(bio, filename=filename),
        caption="График выездов отдела СОТ",
    )


# -------------------------------------------------
# Текст графика
# -------------------------------------------------
def _format_dt(iso_str: Optional[str]) -> str:
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%d.%м.%Y %H:%M")
    except Exception:
        return iso_str


def _compute_schedule_dates(
    approvals: List[sqlite3.Row],
) -> (Optional[date], Optional[date]):
    dates: List[date] = []
    for r in approvals:
        if r["status"] == "approved" and r["decided_at"]:
            try:
                dt = datetime.fromisoformat(r["decided_at"])
                dates.append(dt.date())
            except Exception:
                pass
    if not dates:
        return None, None
    base = max(dates)
    d_from = base
    d_to = base + timedelta(days=4)
    return d_from, d_to


def build_schedule_header(version: int, approvals: List[sqlite3.Row]) -> str:
    d_from, d_to = _compute_schedule_dates(approvals)
    if not d_from or not d_to:
        return f"📅 График выездов (версия {version})"
    return f"📅 График выездов с {d_from:%d.%м.%Y} по {d_to:%d.%м.%Y} г"


def write_schedule_summary_to_sheet(version: int, approvals: List[sqlite3.Row]) -> None:
    service = get_sheets_service()
    if service is None:
        log.error(
            "Google Sheets сервис недоступен – не могу записать итог согласования в 'График'."
        )
    else:
        sheet_name = "График"
        header = build_schedule_header(version, approvals)
        rows = [
            [""],
            [header],
            ["Согласовано всеми:"],
        ]
        for r in approvals:
            rows.append(
                [f"{r['approver']} — {_format_dt(r['decided_at'])} ✅"]
            )

        body = {"values": rows}

        try:
            service.spreadsheets().values().append(
                spreadsheetId=GSHEETS_SPREADSHEET_ID,
                range=f"'{sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            log.info(
                "Итог согласования версии %s дописан в лист '%s'.",
                version,
                sheet_name,
            )
        except Exception as e:
            log.error(
                "Ошибка записи итога согласования в лист '%s': %s", sheet_name, e
            )


def build_schedule_text(is_admin_flag: bool, settings: dict) -> str:
    version = get_schedule_version(settings)
    approvals = get_schedule_approvals(version)
    approvers = get_current_approvers(settings)

    header = build_schedule_header(version, approvals)
    lines = [header, ""]

    if not approvers:
        lines.append("Согласующие не назначены.")
        return "\n".join(lines)

    pending: List[str] = []
    approved: List[sqlite3.Row] = []
    rework: List[sqlite3.Row] = []

    by_approver = {r["approver"]: r for r in approvals}

    for a in approvers:
        r = by_approver.get(a)
        if not r or r["status"] == "pending":
            pending.append(a)
        elif r["status"] == "approved":
            approved.append(r)
        elif r["status"] == "rework":
            rework.append(r)

    if rework:
        lines.append("Отправлено на доработку:")
        for r in rework:
            lines.append(
                f"• {r['approver']} — {_format_dt(r['decided_at'])} (Комментарий: {r['comment'] or 'нет'})"
            )
    elif pending:
        lines.append("На согласовании у:")
        for a in pending:
            lines.append(
                f"• {a} — запрошено {_format_dt(by_approver[a]['requested_at'])}"
            )
        if approved:
            lines.append("")
            lines.append("Уже согласовали:")
            for r in approved:
                lines.append(f"• {r['approver']} — {_format_dt(r['decided_at'])} ✅")
    else:
        lines.append("Согласовано всеми:")
        for r in approved:
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

    grouped = {}

    for _, row in df.iterrows():
        case = str(row.iloc[idx_case]).strip()
        if not case:
            continue

        flags = {
            "pb": is_net(row.iloc[idx_pb]) if idx_pb < len(row) else False,
            "pb_zk": is_net(row.iloc[idx_pb_zk]) if idx_pb_zk < len(row) else False,
            "ar": is_net(row.iloc[idx_ar]) if idx_ar < len(row) else False,
            "eom": is_net(row.iloc[idx_eom]) if idx_eom < len(row) else False,
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
        parts = []
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


def build_remarks_not_done_by_onzs(df: pd.DataFrame, onzs_value: str) -> str:
    sheet_name = get_current_remarks_sheet_name()

    onzs_idx = get_col_index_by_header(df, "онзс", "D")
    if onzs_idx is None:
        return "Не удалось определить столбец ОНзС в файле замечаний."

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

    grouped = {}

    num_str = normalize_onzs_value(onzs_value)

    for _, row in df.iterrows():
        try:
            val_raw = row.iloc[onzs_idx]
        except Exception:
            val_raw = None

        val_norm = normalize_onzs_value(val_raw)
        if val_norm != num_str:
            continue

        case = ""
        try:
            case = str(row.iloc[idx_case]).strip()
        except Exception:
            pass

        if not case:
            continue

        flags = {
            "pb": is_net(row.iloc[idx_pb]) if idx_pb < len(row) else False,
            "pb_zk": is_net(row.iloc[idx_pb_zk]) if idx_pb_zk < len(row) else False,
            "ar": is_net(row.iloc[idx_ar]) if idx_ar < len(row) else False,
            "eom": is_net(row.iloc[idx_eom]) if idx_eom < len(row) else False,
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
        return (
            f"По ОНзС {onzs_value} нет строк со статусом «нет».\n"
            f"Лист: {sheet_name}"
        )

    lines = [
        f"Строки со статусом «НЕ УСТРАНЕНЫ (нет)» по ОНзС {onzs_value}",
        "",
        "Лист: " + sheet_name,
        "",
    ]

    for case, blocks in grouped.items():
        parts = []
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


def build_case_cards_text(df: pd.DataFrame, case_no: str) -> str:
    sheet_name = get_current_remarks_sheet_name()

    case_no = case_no.strip()
    if not case_no:
        return "Номер дела не указан."

    target = normalize_case_number(case_no)

    idx_case = get_case_col_index(df)
    if idx_case is None:
        return (
            "Не удалось определить столбец «Номер дела (I)» в файле замечаний. "
            "Проверьте структуру листа."
        )

    idx_date = get_col_index_by_header(df, "дата выезда", "B")
    idx_onzs = get_col_index_by_header(df, "онзс", "D")
    idx_dev = get_col_index_by_header(df, "наименование застройщика", "F")
    idx_obj = get_col_index_by_header(df, "наименование объекта", "G")
    idx_addr = get_col_index_by_header(df, "строительный адрес", "H")

    idx_pb = excel_col_to_index("Q")
    idx_pb_zk = excel_col_to_index("R")
    idx_ar = excel_col_to_index("X")
    idx_eom = excel_col_to_index("AD")

    mask: List[bool] = []
    for _, row in df.iterrows():
        try:
            val_raw = row.iloc[idx_case]
        except Exception:
            val_raw = None
        val_norm = normalize_case_number(val_raw)
        mask.append(val_norm == target)

    if not any(mask):
        return (
            f"По номеру дела {case_no} ничего не найдено.\n"
            f"Лист: {sheet_name}"
        )

    df_sel = df[mask]

    lines: List[str] = [
        f"Результаты поиска по номеру дела: {case_no}",
        "",
        f"Лист: {sheet_name}",
        "",
    ]

    for _, row in df_sel.iterrows():

        def safe(idx: Optional[int]) -> str:
            if idx is None:
                return ""
            try:
                return str(row.iloc[idx]).strip()
            except Exception:
                return ""

        date_raw = safe(idx_date)
        date_fmt = date_raw
        try:
            if date_raw:
                dt = pd.to_datetime(date_raw, dayfirst=True, errors="ignore")
                if isinstance(dt, (datetime, pd.Timestamp)):
                    date_fmt = dt.strftime("%d.%м.%Y")
        except Exception:
            pass

        onzs_val = safe(idx_onzs)
        dev_val = safe(idx_dev)
        obj_val = safe(idx_obj)
        addr_val = safe(idx_addr)

        def safe_status(idx: int) -> str:
            try:
                if idx < len(row):
                    return str(row.iloc[idx]).strip()
            except Exception:
                pass
            return ""

        pb_val = safe_status(idx_pb)
        pb_zk_val = safe_status(idx_pb_zk)
        ar_val = safe_status(idx_ar)
        eom_val = safe_status(idx_eom)

        lines.append(f"Номер дела: {case_no}")
        if date_fmt:
            lines.append(f"Дата выезда: {date_fmt}")
        if onzs_val:
            lines.append(f"ОНзС: {onzs_val}")
        if dev_val:
            lines.append(f"Застройщик: {dev_val}")
        if obj_val:
            lines.append(f"Объект: {obj_val}")
        if addr_val:
            lines.append(f"Адрес: {addr_val}")

        lines.append("")
        lines.append(f"ПБ: {pb_val or '-'}")
        lines.append(f"ПБ ЗК: {pb_zk_val or '-'}")
        lines.append(f"АР/ММГН/АГО: {ar_val or '-'}")
        lines.append(f"ЭОМ: {eom_val or '-'}")
        lines.append("")
        lines.append("────────────")
        lines.append("")

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
# Лист замечаний
# -------------------------------------------------
def get_remarks_df_current() -> Optional[pd.DataFrame]:
    sheet = get_current_remarks_sheet_name()
    url = build_export_url(GSHEETS_SPREADSHEET_ID)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        xls = pd.ExcelFile(BytesIO(resp.content))
        if sheet not in xls.sheet_names:
            log.error("В файле нет листа '%s'", sheet)
            return None
        return pd.read_excel(xls, sheet_name=sheet)
    except Exception as e:
        log.error("Ошибка чтения листа замечаний: %s", e)
        return None


# -------------------------------------------------
# Итоговые проверки: чтение, фильтр, текст, Excel
# -------------------------------------------------
def refresh_final_checks_local_file() -> bool:
    sheet_id = FINAL_CHECKS_SPREADSHEET_ID
    if not sheet_id:
        log.error("FINAL_CHECKS_SPREADSHEET_ID не задан.")
        return False

    url = build_export_url(sheet_id)
    path = FINAL_CHECKS_LOCAL_PATH

    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        log.warning(
            "Не удалось удалить старый файл итоговых проверок %s: %s",
            path,
            e,
        )

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error("Ошибка скачивания Excel (итоговые проверки): %s", e)
        return False

    try:
        with open(path, "wb") as f:
            f.write(resp.content)
        log.info("Файл итоговых проверок сохранён локально: %s", path)
        return True
    except Exception as e:
        log.error(
            "Ошибка записи локального файла итоговых проверок %s: %s",
            path,
            e,
        )
        return False


def get_final_checks_df() -> Optional[pd.DataFrame]:
    path = FINAL_CHECKS_LOCAL_PATH
    if not path:
        log.error("FINAL_CHECKS_LOCAL_PATH не задан.")
        return None

    if not os.path.exists(path):
        log.error("Локальный файл итоговых проверок не найден: %s", path)
        return None

    try:
        xls = pd.ExcelFile(path)
        if not xls.sheet_names:
            log.error("Файл итоговых проверок пуст (нет листов).")
            return None

        frames: List[pd.DataFrame] = []
        for sheet_name in xls.sheet_names:
            try:
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
                df_sheet = df_sheet.dropna(how="all")
                if not df_sheet.empty:
                    frames.append(df_sheet)
            except Exception as e:
                log.warning(
                    "Ошибка чтения листа '%s' итоговых проверок: %s",
                    sheet_name,
                    e,
                )

        if not frames:
            log.error("Во всех листах итоговых проверок нет данных.")
            return None

        df = pd.concat(frames, ignore_index=True)
        df = df.reset_index(drop=True)
        return df
    except Exception as e:
        log.error("Ошибка чтения локального файла итоговых проверок: %s", e)
        return None


def _parse_final_date(val) -> Optional[date]:
    """
    Преобразует значение из столбцов O/P в дату.
    """
    if val is None:
        return None

    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    try:
        if isinstance(val, (datetime, pd.Timestamp)):
            d = val.date()
            try:
                if pd.isna(d):
                    return None
            except Exception:
                pass
            return d
        if isinstance(val, date):
            try:
                if pd.isna(val):
                    return None
            except Exception:
                pass
            return val

        if isinstance(val, (int, float)):
            dt = pd.to_datetime(val, errors="coerce")
            try:
                if pd.isna(dt):
                    return None
            except Exception:
                pass
            if isinstance(dt, (datetime, pd.Timestamp)):
                d = dt.date()
                try:
                    if pd.isna(d):
                        return None
                except Exception:
                    pass
                return d
            return None

        dt = pd.to_datetime(str(val), dayfirst=True, errors="coerce")
        try:
            if pd.isna(dt):
                return None
        except Exception:
            pass
        if isinstance(dt, (datetime, pd.Timestamp)):
            d = dt.date()
            try:
                if pd.isna(d):
                    return None
            except Exception:
                pass
            return d
    except Exception:
        return None

    return None


def filter_final_checks_df(
    df: pd.DataFrame,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    case_no: Optional[str] = None,
    basis: str = "any",
) -> pd.DataFrame:
    idx_case = excel_col_to_index("B")
    idx_start = excel_col_to_index("O")
    idx_end = excel_col_to_index("P")

    basis = (basis or "any").lower()
    if basis not in ("start", "end", "any"):
        basis = "any"

    case_filter_norm = normalize_case_number(case_no) if case_no else None

    mask: List[bool] = []
    for _, row in df.iterrows():
        include = True

        if case_filter_norm:
            try:
                case_val = row.iloc[idx_case]
            except Exception:
                case_val = None
            val_norm = normalize_case_number(case_val)
            if not val_norm or val_norm != case_filter_norm:
                include = False

        if include and start_date and end_date:
            try:
                s_raw = row.iloc[idx_start]
            except Exception:
                s_raw = None
            try:
                e_raw = row.iloc[idx_end]
            except Exception:
                e_raw = None

            d_start = _parse_final_date(s_raw)
            d_end = _parse_final_date(e_raw)

            if basis == "start":
                base = d_start
            elif basis == "end":
                base = d_end
            else:
                base = d_start if d_start is not None else d_end

            if base is None:
                include = False
            else:
                if isinstance(base, pd.Timestamp):
                    base_date = base.date()
                elif isinstance(base, datetime):
                    base_date = base.date()
                else:
                    base_date = base

                try:
                    if pd.isna(base_date):
                        include = False
                    else:
                        if base_date < start_date or base_date > end_date:
                            include = False
                except TypeError:
                    include = False

        mask.append(include)

    if not mask:
        return df.iloc[0:0].copy()

    df_f = df[mask].copy().reset_index(drop=True)
    return df_f


def compute_auto_period_for_final(
    df: pd.DataFrame, basis: str, days: int
) -> (Optional[date], Optional[date]):
    """
    Определяет период последних N дней по максимальной дате в O/P.
    Используется для режимов «За неделю» и «За месяц».
    """
    idx_start = excel_col_to_index("O")
    idx_end = excel_col_to_index("P")

    basis = (basis or "any").lower()
    if basis not in ("start", "end", "any"):
        basis = "any"

    all_dates: List[date] = []

    for _, row in df.iterrows():
        try:
            s_raw = row.iloc[idx_start]
        except Exception:
            s_raw = None
        try:
            e_raw = row.iloc[idx_end]
        except Exception:
            e_raw = None

        d_start = _parse_final_date(s_raw)
        d_end = _parse_final_date(e_raw)

        if basis == "start":
            d = d_start
        elif basis == "end":
            d = d_end
        else:
            d = d_start if d_start is not None else d_end

        if d is not None:
            all_dates.append(d)

    if not all_dates:
        return None, None

    max_d = max(all_dates)
    return max_d - timedelta(days=days), max_d


def build_final_checks_text_filtered(
    df: pd.DataFrame,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    case_no: Optional[str] = None,
    header: str = "📋 Итоговые проверки",
    basis: str = "any",
) -> str:
    df_f = filter_final_checks_df(
        df,
        start_date=start_date,
        end_date=end_date,
        case_no=case_no,
        basis=basis,
    )

    idx_case = excel_col_to_index("B")
    idx_obj = excel_col_to_index("D")
    idx_addr = excel_col_to_index("E")
    idx_start = excel_col_to_index("O")
    idx_end = excel_col_to_index("P")

    lines: List[str] = [header, ""]

    if df_f.empty:
        if case_no:
            return (
                f"По номеру дела {case_no} в таблице итоговых проверок ничего не найдено."
            )
        if start_date and end_date:
            return (
                f"За период {start_date:%d.%м.%Y} — {end_date:%d.%м.%Y} "
                f"итоговые проверки не найдены."
            )
        return "В таблице итоговых проверок нет строк с заполненным номером дела (B)."

    for _, row in df_f.iterrows():

        def safe_text(idx: int) -> str:
            try:
                val = row.iloc[idx]
            except Exception:
                return ""
            if pd.isna(val):
                return ""
            return str(val).strip()

        case_val = safe_text(idx_case)
        if not case_val:
            continue

        obj = safe_text(idx_obj)
        addr = safe_text(idx_addr)

        d_start_raw = row.iloc[idx_start] if idx_start < len(row) else None
        d_end_raw = row.iloc[idx_end] if idx_end < len(row) else None

        row_start = _parse_final_date(d_start_raw)
        row_end = _parse_final_date(d_end_raw)

        def fmt_date(d: Optional[date]) -> str:
            return d.strftime("%d.%м.%Y") if d else ""

        d_start = fmt_date(row_start)
        d_end = fmt_date(row_end)

        lines.append(f"Номер дела: {case_val}")
        if obj:
            lines.append(f"Объект: {obj}")
        if addr:
            lines.append(f"Адрес: {addr}")
        if d_start or d_end:
            if d_start and d_end:
                lines.append(f"Период итоговой проверки: {d_start} — {d_end}")
            elif d_start:
                lines.append(f"Дата начала итоговой проверки: {d_start}")
            else:
                lines.append(f"Дата окончания итоговой проверки: {d_end}")
        lines.append("")
        lines.append("────────────")
        lines.append("")

    return "\n".join(lines)


def build_final_checks_text(df: pd.DataFrame) -> str:
    return build_final_checks_text_filtered(df)


async def send_final_checks_xlsx_filtered(
    chat_id: int,
    df: pd.DataFrame,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    case_no: Optional[str] = None,
    filename_suffix: str = "",
    basis: str = "any",
):
    df_f = filter_final_checks_df(
        df,
        start_date=start_date,
        end_date=end_date,
        case_no=case_no,
        basis=basis,
    )
    if df_f.empty:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Нет данных для выгрузки по выбранным условиям.",
        )
        return

    bio = BytesIO()
    df_f.to_excel(bio, sheet_name="Итоговые проверки", index=False)
    bio.seek(0)

    fname = "Итоговые_проверки"
    parts = []
    if case_no:
        parts.append(f"дело_{case_no}")
    if start_date and end_date:
        parts.append(f"{start_date:%d.%м.%Y}-{end_date:%d.%м.%Y}")
    if filename_suffix:
        parts.append(filename_suffix)
    if parts:
        fname += "_" + "_".join(parts)
    fname += ".xlsx"

    await context.bot.send_document(
        chat_id=chat_id,
        document=InputFile(bio, filename=fname),
        caption="Итоговые проверки (фильтрованный список)",
    )


# -------------------------------------------------
# Инспектор → Google Sheets
# -------------------------------------------------
def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets API недоступен.")
    ...


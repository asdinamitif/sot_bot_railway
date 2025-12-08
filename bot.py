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

from openpyxl import Workbook, load_workbook
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


def now_moscow() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


# Google Sheets / Drive
GS_SERVICE_ACCOUNT_JSON = os.getenv("GS_SERVICE_ACCOUNT_JSON", "")
GCAL_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

REMARKS_SPREADSHEET_ID = os.getenv("REMARKS_SPREADSHEET_ID", "")
REMARKS_DOWNLOAD_URL = os.getenv("REMARKS_DOWNLOAD_URL", "")
REMARKS_SHEET_NAME = os.getenv("REMARKS_SHEET_NAME", "ПБ, АР,ММГН, АГО (2025)")

GRAPHICS_SPREADSHEET_ID = os.getenv("GRAPHICS_SPREADSHEET_ID", "")
GRAPHICS_DOWNLOAD_URL = os.getenv("GRAPHICS_DOWNLOAD_URL", "")
GRAPHICS_SHEET_NAME = os.getenv("GRAPHICS_SHEET_NAME", "График выездов отдела СОТ")

INSPECTOR_SHEET_NAME = os.getenv(
    "INSPECTOR_SHEET_NAME", "ПБ, АР,ММГН, АГО (2025)"
)

LOCAL_REMARKS_PATH = os.getenv("LOCAL_REMARKS_PATH", "remarks.xlsx")
LOCAL_GRAPHICS_PATH = os.getenv("LOCAL_GRAPHICS_PATH", "graphics.xlsx")

ADMINS = {
    int(x)
    for x in (os.getenv("ADMINS") or "").replace(" ", "").split(",")
    if x.isdigit()
}

DEFAULT_APPROVERS = [
    x.strip()
    for x in (os.getenv("DEFAULT_APPROVERS") or "@FrolovAlNGSN,@Gusev_GGSN").split(
        ","
    )
    if x.strip()
]

RESPONSIBLE_USERNAMES = [
    x.strip()
    for x in (
        os.getenv("RESPONSIBLE_USERNAMES")
        or "@FrolovAlNGSN,@Gusev_GGSN,@Zalimkhan_GGSN"
    ).split(",")
    if x.strip()
]

MENU_MAIN = [
    ["📅 График", "📝 Замечания"],
    ["🏗 ОНзС", "📈 Аналитика"],
    ["👮‍♂️ Инспектор"],
]


def is_admin(user_id: int) -> bool:
    return user_id in ADMINS


# -------------------------------------------------
# GOOGLE CREDS
# -------------------------------------------------
def get_gs_creds() -> Optional[Credentials]:
    if not GS_SERVICE_ACCOUNT_JSON.strip():
        log.error("GS_SERVICE_ACCOUNT_JSON не задан")
        return None

    try:
        info = json.loads(GS_SERVICE_ACCOUNT_JSON)
    except Exception as e:
        log.exception("Ошибка парсинга service account JSON: %s", e)
        return None

    try:
        creds = Credentials.from_service_account_info(
            info, scopes=GCAL_SCOPES
        )
        return creds
    except Exception as e:
        log.exception("Ошибка создания Credentials: %s", e)
        return None


def download_worksheet_to_excel(
    spreadsheet_id: str, sheet_name: str, local_path: str
) -> bool:
    """
    Скачивает указанный лист Google Sheets и сохраняет в локальный Excel (xlsx).
    """
    creds = get_gs_creds()
    if not creds:
        return False

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        result = (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range=sheet_name)
            .execute()
        )
        values = result.get("values", [])
    except Exception as e:
        log.exception("Ошибка чтения Google Sheets: %s", e)
        return False

    if not values:
        log.warning("Пустой лист при скачивании: %s", sheet_name)
        return False

    try:
        df = pd.DataFrame(values)
        headers = df.iloc[0].tolist()
        df = df[1:]
        df.columns = headers

        with pd.ExcelWriter(local_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        log.info(
            "Скачан лист '%s' (%s строк) в %s",
            sheet_name,
            len(df),
            local_path,
        )
        return True
    except Exception as e:
        log.exception("Ошибка записи Excel: %s", e)
        return False


# -------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ EXCEL / PANDAS
# -------------------------------------------------
def excel_col_to_index(col_letter: str) -> int:
    col_letter = col_letter.strip().upper()
    if not col_letter:
        return -1
    result = 0
    for ch in col_letter:
        if not ("A" <= ch <= "Z"):
            return -1
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1


def get_col_by_letter(df: pd.DataFrame, letter: str) -> Optional[int]:
    idx = excel_col_to_index(letter)
    if idx < 0 or idx >= len(df.columns):
        return None
    return idx


def get_col_index_by_header(
    df: pd.DataFrame, search_substr: str, fallback_letter: str
) -> Optional[int]:
    """
    Возвращает индекс столбца по части заголовка (без регистра),
    при неудаче — индекс по букве столбца.
    """
    search_substr = search_substr.lower()
    for i, col in enumerate(df.columns):
        if search_substr in str(col).lower():
            return i
    # fallback по букве
    idx = excel_col_to_index(fallback_letter)
    if 0 <= idx < len(df.columns):
        return idx
    return None


def normalize_onzs_value(val) -> Optional[str]:
    """
    Приводит значение ОНзС к строке без .0, пробелов и т.п.
    6, 6.0, '6 ', '6.0'  -> '6'
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # пробуем как число
    try:
        n = int(float(s.replace(",", ".")))
        return str(n)
    except Exception:
        pass
    return s


# -------------------------------------------------
# БАЗА ДАННЫХ (график + согласование + инспектор)
# -------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schedule_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            revision INTEGER NOT NULL,
            case_no TEXT,
            date TEXT,
            approver_username TEXT,
            status TEXT,
            decided_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schedule_revision (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            description TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inspector_trips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            area REAL,
            floors INTEGER,
            onzs TEXT,
            developer TEXT,
            object_name TEXT,
            address TEXT,
            case_no TEXT,
            visit_type TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()


def get_current_schedule_revision(conn: Optional[sqlite3.Connection] = None) -> int:
    close_conn = False
    if conn is None:
        conn = get_db()
        close_conn = True
    cur = conn.cursor()
    cur.execute(
        "SELECT revision FROM schedule_revision ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if close_conn:
        conn.close()
    return row["revision"] if row else 1


def bump_schedule_revision(description: str = "") -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT revision FROM schedule_revision ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    new_rev = (row["revision"] + 1) if row else 1
    cur.execute(
        """
        INSERT INTO schedule_revision (revision, created_at, description)
        VALUES (?, ?, ?)
        """,
        (new_rev, now_moscow().isoformat(), description),
    )
    conn.commit()
    conn.close()
    log.info("Новая ревизия графика: %s", new_rev)
    return new_rev


def add_schedule_approval(
    revision: int,
    case_no: str,
    date: str,
    approver_username: str,
    status: str,
) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO schedule_approvals
        (revision, case_no, date, approver_username, status, decided_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (revision, case_no, date, approver_username, status, now_moscow().isoformat()),
    )
    conn.commit()
    conn.close()


def get_approvals_for_case_and_revision(case_no: str, revision: int) -> List[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM schedule_approvals
        WHERE case_no = ? AND revision = ?
        ORDER BY id ASC
        """,
        (case_no, revision),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_schedule_analytics() -> List[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT revision, created_at, description, COUNT(*) AS decisions
        FROM schedule_revision
        LEFT JOIN schedule_approvals
          ON schedule_revision.revision = schedule_approvals.revision
        GROUP BY schedule_revision.revision, schedule_revision.created_at, schedule_revision.description
        ORDER BY schedule_revision.revision DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    """
    Добавляет новую строку выезда в лист INSPECTOR_SHEET_NAME файла REMARKS_PATH.
    """
    path = LOCAL_REMARKS_PATH
    sheet_name = INSPECTOR_SHEET_NAME

    try:
        if os.path.exists(path):
            wb = load_workbook(path)
            if sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
            else:
                ws = wb.create_sheet(sheet_name)
                ws.append(
                    [
                        "Дата выезда",
                        "Площадь (кв.м)",
                        "Количество этажей",
                        "ОНзС",
                        "Наименование застройщика",
                        "Наименование объекта",
                        "Строительный адрес",
                        "Номер дела",
                        "Вид проверки",
                    ]
                )
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = sheet_name
            ws.append(
                [
                    "Дата выезда",
                    "Площадь (кв.м)",
                    "Количество этажей",
                    "ОНзС",
                    "Наименование застройщика",
                    "Наименование объекта",
                    "Строительный адрес",
                    "Номер дела",
                    "Вид проверки",
                ]
            )

        ws.append(
            [
                form.get("date", ""),
                form.get("area", ""),
                form.get("floors", ""),
                form.get("onzs", ""),
                form.get("developer", ""),
                form.get("object_name", ""),
                form.get("address", ""),
                form.get("case_no", ""),
                form.get("visit_type", ""),
            ]
        )

        wb.save(path)
        log.info("Добавлена строка инспектора в %s (лист %s)", path, sheet_name)
        return True
    except Exception as e:
        log.exception("Ошибка при добавлении строки инспектора в Excel: %s", e)
        return False


def save_inspector_trip_to_db(form: Dict[str, Any]) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO inspector_trips
        (date, area, floors, onzs, developer, object_name, address, case_no, visit_type, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            form.get("date", ""),
            float(form.get("area", 0) or 0),
            int(form.get("floors", 0) or 0),
            form.get("onzs", ""),
            form.get("developer", ""),
            form.get("object_name", ""),
            form.get("address", ""),
            form.get("case_no", ""),
            form.get("visit_type", ""),
            now_moscow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def get_last_inspector_trips(limit: int = 20) -> List[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM inspector_trips
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# -------------------------------------------------
# ТЕКУЩИЕ ИМЕНА ЛИСТОВ
# -------------------------------------------------
def get_current_remarks_sheet_name() -> str:
    return REMARKS_SHEET_NAME


def get_current_graphics_sheet_name() -> str:
    return GRAPHICS_SHEET_NAME


# -------------------------------------------------
# ЗАГРУЗКА ЛОКАЛЬНЫХ EXCEL
# -------------------------------------------------
def load_remarks_df() -> Optional[pd.DataFrame]:
    if not os.path.exists(LOCAL_REMARKS_PATH):
        log.warning("Файл замечаний не найден: %s", LOCAL_REMARKS_PATH)
        return None

    try:
        df = pd.read_excel(LOCAL_REMARKS_PATH, sheet_name=get_current_remarks_sheet_name())
        return df
    except Exception as e:
        log.exception("Ошибка чтения Excel замечаний: %s", e)
        return None


def load_graphics_df() -> Optional[pd.DataFrame]:
    if not os.path.exists(LOCAL_GRAPHICS_PATH):
        log.warning("Файл графика не найден: %s", LOCAL_GRAPHICS_PATH)
        return None
    try:
        df = pd.read_excel(LOCAL_GRAPHICS_PATH, sheet_name=get_current_graphics_sheet_name())
        return df
    except Exception as e:
        log.exception("Ошибка чтения Excel графика: %s", e)
        return None


# -------------------------------------------------
# ПОСТРОЕНИЕ ТЕКСТОВ ПО ГРАФИКУ
# -------------------------------------------------
def build_graphics_list_text(df: pd.DataFrame) -> str:
    """
    Строим список выездов из df (график).
    """
    col_date = get_col_index_by_header(df, "дата выезда", "C")
    col_onzs = get_col_index_by_header(df, "онзс", "E")
    col_dev = get_col_index_by_header(df, "наименование застройщика", "F")
    col_obj = get_col_index_by_header(df, "наименование объекта", "G")
    col_addr = get_col_index_by_header(df, "строительный адрес", "H")
    col_case = get_col_index_by_header(df, "номер дела", "I")
    col_type = get_col_index_by_header(df, "вид проверки", "J")
    col_inspector = get_col_index_by_header(df, "должностное лицо", "K")

    lines: List[str] = []

    for _, row in df.iterrows():
        def safe(idx: Optional[int]) -> str:
            if idx is None:
                return ""
            try:
                return str(row.iloc[idx]).strip()
            except Exception:
                return ""

        raw_date = safe(col_date)
        if raw_date:
            try:
                dt = pd.to_datetime(raw_date, dayfirst=True, errors="coerce")
                if pd.isna(dt):
                    date_str = raw_date
                else:
                    date_str = dt.strftime("%d.%m.%Y")
            except Exception:
                date_str = raw_date
        else:
            date_str = ""

        onzs = safe(col_onzs)
        dev = safe(col_dev)
        obj = safe(col_obj)
        addr = safe(col_addr)
        case_no = safe(col_case)
        visit_type = safe(col_type)
        inspector = safe(col_inspector)

        if not case_no and not addr and not dev and not obj and not date_str:
            continue

        parts = []
        if date_str:
            parts.append(f"{date_str}")
        if onzs:
            parts.append(f"ОНзС: {onzs}")
        if case_no:
            parts.append(f"дело: {case_no}")
        if visit_type:
            parts.append(f"{visit_type}")
        if inspector:
            parts.append(f"инспектор: {inspector}")

        header = " — ".join(parts) if parts else "Выезд"

        second_line_parts = []
        if dev:
            second_line_parts.append(dev)
        if obj:
            second_line_parts.append(obj)
        if addr:
            second_line_parts.append(addr)

        second_line = "; ".join(second_line_parts)

        if second_line:
            lines.append(f"• {header}\n    {second_line}")
        else:
            lines.append(f"• {header}")

    if not lines:
        return "В графике выездов пока нет строк."
    return "\n".join(lines)


def build_upcoming_final_checks_text(df: pd.DataFrame) -> str:
    """
    Для раздела «📊 Итоговая» — показываем только итоговые ближайшие проверки.
    """
    col_date = get_col_index_by_header(df, "дата выезда", "C")
    col_case = get_col_index_by_header(df, "номер дела", "I")
    col_type = get_col_index_by_header(df, "вид проверки", "J")

    if col_date is None or col_case is None or col_type is None:
        return "Не удалось определить нужные столбцы (дата/номер/вид проверки)."

    lines: List[str] = []
    today = now_moscow().date()

    for _, row in df.iterrows():
        try:
            raw_date = str(row.iloc[col_date]).strip()
        except Exception:
            continue

        if not raw_date:
            continue

        try:
            dt = pd.to_datetime(raw_date, dayfirst=True, errors="coerce")
            if pd.isna(dt):
                continue
            d = dt.date()
        except Exception:
            continue

        if d < today:
            continue

        try:
            visit_type = str(row.iloc[col_type]).strip().lower()
        except Exception:
            continue

        if "итог" not in visit_type:
            continue

        try:
            case_no = str(row.iloc[col_case]).strip()
        except Exception:
            case_no = ""

        date_str = d.strftime("%d.%m.%Y")

        parts = [f"{date_str} — итоговая"]
        if case_no:
            parts.append(f"дело: {case_no}")

        lines.append("• " + " — ".join(parts))

    if not lines:
        return "Нет запланированных итоговых проверок."

    return "Ближайшие итоговые проверки:\n" + "\n".join(lines)


# -------------------------------------------------
# ПОСТРОЕНИЕ ТЕКСТОВ ПО ЗАМЕЧАНИЯМ
# -------------------------------------------------
def is_value_net(val: Any) -> bool:
    """
    Проверка, что в ячейке статус «нет» (не устранено).
    """
    if val is None:
        return False
    s = str(val).lower().replace("\n", " ").strip()
    if not s or s in {"-", "н/д"}:
        return False
    return s.startswith("нет")


def build_remarks_not_done_text(df: pd.DataFrame) -> str:
    """
    Строим агрегированный список дел и блоков, где есть статус «нет»
    по ПБ, ПБ в ЗК, АР/ММГН/АГО, ЭОМ.
    """
    sheet_name = get_current_remarks_sheet_name()

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

    grouped: Dict[str, Dict[str, set]] = {}

    for _, row in df.iterrows():
        case = ""
        try:
            case = str(row.iloc[idx_case]).strip()
        except Exception:
            pass

        if not case:
            continue

        flags = {
            "pb": is_value_net(row.iloc[idx_pb]) if idx_pb < len(row) else False,
            "pb_zk": is_value_net(row.iloc[idx_pb_zk])
            if idx_pb_zk < len(row)
            else False,
            "ar": is_value_net(row.iloc[idx_ar]) if idx_ar < len(row) else False,
            "eom": is_value_net(row.iloc[idx_eom])
            if idx_eom < len(row)
            else False,
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
            "По листу замечаний нет строк со статусом «нет».\n"
            f"Лист: {sheet_name}"
        )

    lines = [
        "Строки со статусом «НЕ УСТРАНЕНЫ (нет)»",
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


def build_remarks_not_done_by_onzs(df: pd.DataFrame, onzs_value: str) -> str:
    """
    Строки со статусом «нет» только для выбранного ОНзС.
    """
    sheet_name = get_current_remarks_sheet_name()

    # Столбец ОНзС
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
        # фильтрация по ОНзС
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
    """
    Поиск по номеру дела в листе замечаний и красивый вывод блоков ПБ/АР/ЭОМ и статусов.
    """
    sheet_name = get_current_remarks_sheet_name()

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

    lines: List[str] = []
    target_case = case_no.strip()

    for _, row in df.iterrows():
        try:
            row_case = str(row.iloc[idx_case]).strip()
        except Exception:
            continue

        if not row_case:
            continue

        if row_case != target_case:
            continue

        flags = {
            "pb": str(row.iloc[idx_pb]).strip()
            if idx_pb < len(row)
            else "",
            "pb_zk": str(row.iloc[idx_pb_zk]).strip()
            if idx_pb_zk < len(row)
            else "",
            "ar": str(row.iloc[idx_ar]).strip()
            if idx_ar < len(row)
            else "",
            "eom": str(row.iloc[idx_eom]).strip()
            if idx_eom < len(row)
            else "",
        }

        lines.append(f"Дело {target_case} — лист: {sheet_name}")
        for key, title in TITLES.items():
            status = flags.get(key, "")
            if status:
                lines.append(f"• {title}: {status}")
        break

    if not lines:
        return f"Номер дела {case_no} не найден в листе {sheet_name}."

    return "\n".join(lines)


def build_onzs_list_by_number(df: pd.DataFrame, number: str) -> str:
    """
    Список дел по ОНзС с количеством.
    Ищем столбцы по заголовкам, без жёсткой привязки к буквам.
    """
    # ОНзС обычно в столбце D, но ищем по заголовку
    onzs_idx = get_col_index_by_header(df, "онзс", "D")
    if onzs_idx is None:
        return "Не удалось определить столбец ОНзС в файле замечаний."

    # Номер дела: заголовок содержит «номер дела», по умолчанию H
    case_idx = get_col_index_by_header(df, "номер дела", "H")
    # Адрес: «строительный адрес», по умолчанию H
    addr_idx = get_col_index_by_header(df, "строительный адрес", "H")

    num_str = normalize_onzs_value(number)
    mask: List[bool] = []
    for _, row in df.iterrows():
        try:
            val_raw = row.iloc[onzs_idx]
        except Exception:
            val_raw = None
        val_norm = normalize_onzs_value(val_raw)
        mask.append(val_norm == num_str)

    if not any(mask):
        return f"Нет объектов с ОНзС = {number}."

    df_f = df[mask]

    lines = [f"ОНзС = {number}", f"Найдено дел: {len(df_f)}", ""]

    for _, row in df_f.iterrows():
        def safe(idx: Optional[int]) -> str:
            if idx is None:
                return ""
            try:
                return str(row.iloc[idx]).strip()
            except Exception:
                return ""

        case_no = safe(case_idx)
        addr = safe(addr_idx)

        if case_no and addr:
            lines.append(f"• {case_no} — {addr}")
        elif case_no:
            lines.append(f"• {case_no}")
        elif addr:
            lines.append(f"• {addr}")

    return "\n".join(lines)


# -------------------------------------------------
# Инспектор — просмотр и Excel
# -------------------------------------------------
def build_inspector_list_text(rows: List[sqlite3.Row]) -> str:
    if not rows:
        return "Пока нет сохранённых выездов инспектора."

    lines: List[str] = ["Последние выезды инспектора:", ""]
    for r in rows:
        d = r["date"] or ""
        try:
            d_fmt = datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m.%Y")
        except Exception:
            d_fmt = d

        parts = [f"{d_fmt}"]
        if r["onzs"]:
            parts.append(f"ОНзС {r['onzs']}")
        if r["case_no"]:
            parts.append(f"дело: {r['case_no']}")
        if r["visit_type"]:
            parts.append(r["visit_type"])

        header = " — ".join(parts)
        second_line = "; ".join(
            [
                x
                for x in [
                    r["developer"],
                    r["object_name"],
                    r["address"],
                ]
                if x
            ]
        )
        if second_line:
            lines.append(f"• {header}\n    {second_line}")
        else:
            lines.append(f"• {header}")

    return "\n".join(lines)


def build_inspector_excel_bytes(rows: List[sqlite3.Row]) -> BytesIO:
    wb = Workbook()
    ws = wb.active
    ws.title = "Выезды инспектора"

    ws.append(
        [
            "Дата выезда",
            "Площадь (кв.м)",
            "Количество этажей",
            "ОНзС",
            "Наименование застройщика",
            "Наименование объекта",
            "Строительный адрес",
            "Номер дела",
            "Вид проверки",
            "Создано в БД",
        ]
    )

    for r in rows:
        ws.append(
            [
                r["date"],
                r["area"],
                r["floors"],
                r["onzs"],
                r["developer"],
                r["object_name"],
                r["address"],
                r["case_no"],
                r["visit_type"],
                r["created_at"],
            ]
        )

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# -------------------------------------------------
# КЛАВИАТУРЫ
# -------------------------------------------------
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MENU_MAIN, resize_keyboard=True)


def graphics_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Загрузить график", callback_data="graphics_upload"),
                InlineKeyboardButton("📤 Скачать график", callback_data="graphics_download"),
            ],
            [
                InlineKeyboardButton("📊 Итоговая", callback_data="graphics_final"),
            ],
        ]
    )


def remarks_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Загрузить", callback_data="remarks_upload"),
                InlineKeyboardButton("📤 Скачать", callback_data="remarks_download"),
            ],
            [
                InlineKeyboardButton(
                    "❌ Не устранены", callback_data="remarks_not_done"
                ),
            ],
        ]
    )


def onzs_menu_inline() -> InlineKeyboardMarkup:
    buttons = []
    for i in range(1, 13):
        buttons.append(
            InlineKeyboardButton(str(i), callback_data=f"onzs_{i}")
        )
    rows = [buttons[i : i + 4] for i in range(0, len(buttons), 4)]
    return InlineKeyboardMarkup(rows)


def analytics_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📈 История согласований графика", callback_data="analytics_schedule"
                )
            ],
        ]
    )


def inspector_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Добавить выезд", callback_data="inspector_add_trip"
                )
            ],
            [
                InlineKeyboardButton(
                    "📋 Последние выезды", callback_data="inspector_list"
                ),
                InlineKeyboardButton(
                    "📊 Выгрузка в Excel", callback_data="inspector_excel"
                ),
            ],
        ]
    )


# -------------------------------------------------
# ИНСПЕКТОР — ПОШАГОВЫЙ МАСТЕР
# -------------------------------------------------
INSPECTOR_STEPS = [
    "date",
    "area",
    "floors",
    "onzs",
    "developer",
    "object_name",
    "address",
    "case_no",
    "visit_type",
]


INSPECTOR_PROMPTS = {
    "date": "Укажите дату выезда в формате ДД.ММ.ГГГГ:",
    "area": "Укажите площадь (кв.м). Можно просто число:",
    "floors": "Укажите количество этажей (целое число):",
    "onzs": "Укажите ОНзС (1–12):",
    "developer": "Укажите наименование застройщика:",
    "object_name": "Укажите наименование объекта:",
    "address": "Укажите строительный адрес:",
    "case_no": "Укажите номер дела (формат 00-00-000000):",
    "visit_type": "Укажите вид проверки (ПП, итоговая, профвизит, запрос ОНзС, поручение руководства):",
}


def get_inspector_form(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    return context.user_data.setdefault("inspector_form", {})


def reset_inspector_form(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["inspector_form"] = {}
    context.user_data["inspector_step"] = 0


async def start_inspector_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reset_inspector_form(context)
    await ask_next_inspector_step(update, context)


async def ask_next_inspector_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    step_idx = context.user_data.get("inspector_step", 0)
    if step_idx >= len(INSPECTOR_STEPS):
        form = get_inspector_form(context)
        await finalize_inspector_form(update, context, form)
        return

    field = INSPECTOR_STEPS[step_idx]
    prompt = INSPECTOR_PROMPTS.get(field, "Введите значение:")

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(prompt)
    else:
        await update.message.reply_text(prompt)


async def handle_inspector_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if "inspector_step" not in context.user_data:
        return

    text = (update.message.text or "").strip()
    step_idx = context.user_data.get("inspector_step", 0)
    field = INSPECTOR_STEPS[step_idx]

    form = get_inspector_form(context)

    if field == "date":
        try:
            dt = datetime.strptime(text, "%d.%m.%Y").date()
            form["date"] = dt.strftime("%Y-%m-%d")
        except Exception:
            await update.message.reply_text(
                "Не получилось распознать дату. Попробуйте ещё раз в формате ДД.ММ.ГГГГ."
            )
            return
    elif field == "area":
        try:
            form["area"] = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text(
                "Площадь должна быть числом. Попробуйте ещё раз."
            )
            return
    elif field == "floors":
        try:
            form["floors"] = int(text)
        except Exception:
            await update.message.reply_text(
                "Количество этажей должно быть целым числом. Попробуйте ещё раз."
            )
            return
    elif field == "onzs":
        form["onzs"] = text
    else:
        form[field] = text

    context.user_data["inspector_step"] = step_idx + 1
    await ask_next_inspector_step(update, context)


async def finalize_inspector_form(
    update: Update, context: ContextTypes.DEFAULT_TYPE, form: Dict[str, Any]
) -> None:
    ok_db = False
    ok_excel = False

    try:
        save_inspector_trip_to_db(form)
        ok_db = True
    except Exception as e:
        log.exception("Ошибка сохранения выезда инспектора в БД: %s", e)

    ok_excel = append_inspector_row_to_excel(form)

    parts = []
    if ok_db:
        parts.append("в БД")
    if ok_excel:
        parts.append("в Excel")

    if parts:
        msg = "Выезд инспектора сохранён: " + ", ".join(parts) + "."
    else:
        msg = "Не удалось сохранить выезд инспектора — обратитесь к администратору."

    if update.callback_query:
        await update.callback_query.message.reply_text(msg)
    else:
        await update.message.reply_text(msg)

    reset_inspector_form(context)


# -------------------------------------------------
# HANDLERS
# -------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    log.info("Команда /start от %s", user.id if user else "unknown")
    await update.message.reply_text(
        "Добро пожаловать в бот отдела СОТ.\nВыберите раздел:",
        reply_markup=main_menu_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Это бот отдела СОТ.\n"
        "Доступные разделы:\n"
        "• 📅 График — работа с графиком выездов\n"
        "• 📝 Замечания — работа с листом замечаний\n"
        "• 🏗 ОНзС — сводка по объектам по выбранному ОНзС\n"
        "• 📈 Аналитика — история согласований\n"
        "• 👮‍♂️ Инспектор — добавление и просмотр выездов инспектора",
        reply_markup=main_menu_keyboard(),
    )


async def text_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()

    if "inspector_step" in context.user_data:
        await handle_inspector_message(update, context)
        return

    if text == "📅 График":
        await update.message.reply_text(
            "Раздел «📅 График». Выберите действие:", reply_markup=graphics_menu_keyboard()
        )
    elif text == "📝 Замечания":
        await update.message.reply_text(
            "Раздел «📝 Замечания». Выберите действие:",
            reply_markup=remarks_menu_keyboard(),
        )
    elif text == "🏗 ОНзС":
        await update.message.reply_text(
            "Выберите номер ОНзС (1–12):", reply_markup=onzs_menu_inline()
        )
    elif text == "📈 Аналитика":
        await update.message.reply_text(
            "Раздел «📈 Аналитика». Выберите действие:",
            reply_markup=analytics_menu_keyboard(),
        )
    elif text == "👮‍♂️ Инспектор":
        await update.message.reply_text(
            "Раздел «👮‍♂️ Инспектор». Выберите действие:",
            reply_markup=inspector_menu_keyboard(),
        )
    else:
        await update.message.reply_text(
            "Не понял команду. Используйте меню или /help.",
            reply_markup=main_menu_keyboard(),
        )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data
    user = update.effective_user
    user_id = user.id if user else 0

    if data == "graphics_upload":
        if not is_admin(user_id):
            await query.answer("Загружать график может только администратор.", show_alert=True)
            return
        await query.answer()
        await query.message.reply_text(
            "Отправьте Excel-файл с графиком выездов (лист "
            f"«{get_current_graphics_sheet_name()}»)."
        )
        context.user_data["awaiting_graphics_file"] = True
        return

    if data == "graphics_download":
        await query.answer()
        if not os.path.exists(LOCAL_GRAPHICS_PATH):
            await query.message.reply_text(
                "Локальный файл графика не найден. Сначала загрузите его."
            )
            return
        with open(LOCAL_GRAPHICS_PATH, "rb") as f:
            await query.message.reply_document(
                document=InputFile(f, filename=os.path.basename(LOCAL_GRAPHICS_PATH)),
                caption="Текущий локальный файл графика выездов.",
            )
        return

    if data == "graphics_final":
        await query.answer()
        df = load_graphics_df()
        if df is None:
            await query.message.reply_text(
                "Не удалось прочитать локальный файл графика."
            )
            return
        text = build_upcoming_final_checks_text(df)
        await query.message.reply_text(text)
        return

    if data == "remarks_upload":
        if not is_admin(user_id):
            await query.answer(
                "Только администратор может загружать рабочий файл.", show_alert=True
            )
            return
        await query.answer()
        await query.message.reply_text(
            "Отправьте Excel-файл с замечаниями (лист "
            f"«{get_current_remarks_sheet_name()}»)."
        )
        context.user_data["awaiting_remarks_file"] = True
        return

    if data == "remarks_download":
        await query.answer()
        if not os.path.exists(LOCAL_REMARKS_PATH):
            await query.message.reply_text(
                "Локальный файл замечаний не найден. Сначала загрузите его."
            )
            return
        with open(LOCAL_REMARKS_PATH, "rb") as f:
            await query.message.reply_document(
                document=InputFile(f, filename=os.path.basename(LOCAL_REMARKS_PATH)),
                caption="Текущий локальный файл замечаний.",
            )
        return

    if data == "remarks_not_done":
        await query.answer()
        df = load_remarks_df()
        if df is None:
            await query.message.reply_text(
                "Не удалось прочитать локальный файл замечаний."
            )
            return
        text = build_remarks_not_done_text(df)
        await query.message.reply_text(text)
        return

    if data.startswith("onzs_"):
        await query.answer()
        onzs_value = data.split("_", 1)[1]
        df = load_remarks_df()
        if df is None:
            await query.message.reply_text(
                "Не удалось прочитать локальный файл замечаний."
            )
            return
        text = build_remarks_not_done_by_onzs(df, onzs_value)
        await query.message.reply_text(text)
        return

    if data == "analytics_schedule":
        await query.answer()
        rows = get_schedule_analytics()
        if not rows:
            await query.message.reply_text("Нет данных по согласованиям графика.")
            return
        lines = ["История согласований графика:", ""]
        for r in rows:
            rev = r["revision"]
            created = r["created_at"]
            desc = r["description"] or ""
            dec = r["decisions"] or 0
            lines.append(
                f"• ревизия {rev}, создана {created}, решений: {dec}, описание: {desc}"
            )
        await query.message.reply_text("\n".join(lines))
        return

    if data == "inspector_add_trip":
        await start_inspector_wizard(update, context)
        return

    if data == "inspector_list":
        rows = get_last_inspector_trips(limit=20)
        text = build_inspector_list_text(rows)
        await query.message.reply_text(text)
        return

    if data == "inspector_excel":
        rows = get_last_inspector_trips(limit=1000)
        if not rows:
            await query.answer("Нет данных выездов инспектора.")
            return
        buf = build_inspector_excel_bytes(rows)
        await query.message.reply_document(
            document=InputFile(buf, filename="inspector_trips.xlsx"),
            caption="Выезды инспектора (Excel).",
        )
        return

    await query.answer("Неизвестное действие.")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id if user else 0

    if context.user_data.get("awaiting_graphics_file"):
        if not is_admin(user_id):
            await update.message.reply_text(
                "Загружать график может только администратор."
            )
            return

        doc = update.message.document
        if not doc:
            await update.message.reply_text("Не вижу файла. Пришлите Excel-файл.")
            return

        file = await doc.get_file()
        file_bytes = await file.download_as_bytearray()

        with open(LOCAL_GRAPHICS_PATH, "wb") as f:
            f.write(file_bytes)

        df = load_graphics_df()
        if df is None:
            await update.message.reply_text(
                "Файл получен, но не удалось прочитать лист графика."
            )
            return

        rev = bump_schedule_revision(description=f"Загрузка файла {doc.file_name}")

        await update.message.reply_text(
            f"Файл графика загружен и сохранён локально.\n"
            f"Новая ревизия графика: {rev}.",
            reply_markup=graphics_menu_keyboard(),
        )
        context.user_data["awaiting_graphics_file"] = False
        return

    if context.user_data.get("awaiting_remarks_file"):
        if not is_admin(user_id):
            await update.message.reply_text(
                "Только администратор может загружать рабочий файл."
            )
            return

        doc = update.message.document
        if not doc:
            await update.message.reply_text("Не вижу файла. Пришлите Excel-файл.")
            return

        file = await doc.get_file()
        file_bytes = await file.download_as_bytearray()

        with open(LOCAL_REMARKS_PATH, "wb") as f:
            f.write(file_bytes)

        df = load_remarks_df()
        if df is None:
            await update.message.reply_text(
                "Файл получен, но не удалось прочитать лист замечаний."
            )
            return

        await update.message.reply_text(
            "Файл замечаний загружен и сохранён локально.",
            reply_markup=remarks_menu_keyboard(),
        )
        context.user_data["awaiting_remarks_file"] = False
        return

    await update.message.reply_text(
        "Файл получен, но в данный момент бот не ожидает загрузки.\n"
        "Используйте меню для выбора действия.",
        reply_markup=main_menu_keyboard(),
    )


def build_application() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Не указан BOT_TOKEN в переменных окружения.")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(CallbackQueryHandler(button_callback))

    app.add_handler(
        MessageHandler(filters.Document.ALL, handle_document)
    )

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_menu_router)
    )

    return app


def main() -> None:
    app = build_application()
    log.info("Бот запущен. Ожидаем обновления...")
    app.run_polling()


if __name__ == "__main__":
    main()

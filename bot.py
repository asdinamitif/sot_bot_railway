Pythonimport logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List
import time as time_module

import pandas as pd
import requests
from dotenv import load_dotenv
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile,
    Document,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

# ----------------- ENV -----------------
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "8274616381:AAE4Av9RgX8iSRfM1n2U9V8oPoWAf-bB_hA").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

# По умолчанию используем твою Google-таблицу
GOOGLE_SHEET_URL_DEFAULT = (
    "https://docs.google.com/spreadsheets/d/"
    "1FlhN7grvku5tSj2SAreEHxHC55K9E7N91r8eWOkzOFY/edit?usp=sharing"
)

SCHEDULE_URL = os.getenv("SCHEDULE_URL", GOOGLE_SHEET_URL_DEFAULT).strip()
REMARKS_URL = os.getenv("REMARKS_URL", GOOGLE_SHEET_URL_DEFAULT).strip()

SCHEDULE_SYNC_TTL_SEC = int(os.getenv("SCHEDULE_SYNC_TTL_SEC", "3600"))
REMARKS_SYNC_TTL_SEC = int(os.getenv("REMARKS_SYNC_TTL_SEC", "3600"))

GSHEETS_SERVICE_ACCOUNT_JSON = os.getenv("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()
GSHEETS_SPREADSHEET_ID = os.getenv(
    "GSHEETS_SPREADSHEET_ID", "1FlhN7grvku5tSj2SAreEHxHC55K9E7N91r8eWOkzOFY"
).strip()

SHEETS_SERVICE = None  # кеш клиента Google Sheets

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))
ANALYTICS_PASSWORD = "051995"

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

INSPECTOR_SHEET_NAME = "ПБ, АР,ММГН, АГО (2025)"  # лист для мастера инспектора, графика и замечаний

HARD_CODED_ADMINS = {398960707}

# Группа для уведомлений по графику (можно задать через env или командой /set_schedule_group)
SCHEDULE_NOTIFY_CHAT_ID_ENV = os.getenv("SCHEDULE_NOTIFY_CHAT_ID", "").strip()


def is_admin(uid: int) -> bool:
    return uid in HARD_CODED_ADMINS


SCHEDULE_CACHE = {"last_fetch": 0, "df": None}
REMARKS_CACHE = {"last_fetch": 0, "df": None}


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


def get_current_remarks_sheet_name() -> str:
    """Имя листа с замечаниями на текущий год."""
    year = local_now().year
    return f"ПБ, АР,ММГН, АГО ({year})"


def get_sheets_service():
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


def load_sheet_values(service, sheet_name):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=GSHEETS_SPREADSHEET_ID,
            range=f"'{sheet_name}'!A:ZZ",
        ).execute()
        return result.get('values', [])
    except Exception as e:
        log.error(f"Ошибка чтения листа {sheet_name}: %s", e)
        return []


def find_header_row(values):
    for i in range(min(30, len(values))):
        row = [str(c).lower() for c in values[i]]
        if any("дата выезда" in c for c in row):
            return i
    return 0


def load_gsheet_single_sheet(cache):
    now = time_module.time()
    if cache["df"] is not None and now - cache["last_fetch"] < SCHEDULE_SYNC_TTL_SEC:
        return cache["df"]

    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets сервис недоступен.")
        return None

    sheet_name = INSPECTOR_SHEET_NAME
    values = load_sheet_values(service, sheet_name)
    if not values:
        return None

    header_row = find_header_row(values)
    headers = values[header_row]
    data = values[header_row + 1:]

    max_len = max(len(row) for row in data) if data else len(headers)
    headers += [None] * (max_len - len(headers))
    for row in data:
        row += [None] * (max_len - len(row))

    df = pd.DataFrame(data, columns=headers)
    df = df.dropna(how="all").reset_index(drop=True)

    cache["df"] = df
    cache["last_fetch"] = now
    return df


def load_gsheet_all_sheets(cache):
    now = time_module.time()
    if cache["df"] is not None and now - cache["last_fetch"] < REMARKS_SYNC_TTL_SEC:
        return cache["df"]

    service = get_sheets_service()
    if service is None:
        log.error("Google Sheets сервис недоступен.")
        return None

    frames = []
    meta = service.spreadsheets().get(spreadsheetId=GSHEETS_SPREADSHEET_ID).execute()
    for sheet in meta.get('sheets', []):
        sheet_name = sheet['properties']['title']
        values = load_sheet_values(service, sheet_name)
        if not values:
            continue

        header_row = find_header_row(values)
        headers = values[header_row]
        data = values[header_row + 1:]

        max_len = max(len(row) for row in data) if data else len(headers)
        headers += [None] * (max_len - len(headers))
        for row in data:
            row += [None] * (max_len - len(row))

        df_sheet = pd.DataFrame(data, columns=headers)
        df_sheet = df_sheet.dropna(how="all").reset_index(drop=True)
        df_sheet["_sheet"] = sheet_name
        frames.append(df_sheet)

    if not frames:
        log.error("Нет данных в листах.")
        return None

    df_all = pd.concat(frames, ignore_index=True)
    cache["df"] = df_all
    cache["last_fetch"] = now
    return df_all


def get_schedule_df() -> Optional[pd.DataFrame]:
    return load_gsheet_single_sheet(SCHEDULE_CACHE)


def get_remarks_df() -> Optional[pd.DataFrame]:
    return load_gsheet_all_sheets(REMARKS_CACHE)


# ----------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ -----------------

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


def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    service = get_sheets_service()
    if service is None or not GSHEETS_SPREADSHEET_ID:
        log.error("Google Sheets сервис недоступен – некуда писать выезд.")
        return False

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
        dep_str,
        fin_str,
        d_cell,
        onzs,
        developer,
        obj_name,
        address,
        case_no,
        check_type,
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
        REMARKS_CACHE["df"] = None
        SCHEDULE_CACHE["df"] = None
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

    c.execute("SELECT COUNT(*) AS c FROM approvers")
    if c.fetchone()["c"] == 0:
        c.executemany(
            "INSERT OR IGNORE INTO approvers (label) VALUES (?)",
            [(lbl,) for lbl in DEFAULT_APPROVERS],
        )

    c.execute("SELECT value FROM schedule_settings WHERE key='schedule_version'")
    row_ver = c.fetchone()
    if not row_ver:
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) "
            "VALUES ('schedule_version', '1')"
        )

    c.execute("SELECT value FROM schedule_settings WHERE key='last_notified_version'")
    row_ln = c.fetchone()
    if not row_ln:
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) "
            "VALUES ('last_notified_version', '0')"
        )

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
    res = {}
    for r in rows:
        try:
            v = int(r["version"])
        except Exception:
            continue
        res[v] = r["name"]
    return res


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO users (user_id, username, first_seen_at) "
        "VALUES (?, ?, ?)",
        (user.id, user.username, local_now().isoformat()),
    )
    if is_admin(user.id):
        c.execute(
            "INSERT OR REPLACE INTO admins (user_id, username, first_seen_at) "
            "VALUES (?, ?, ?)",
            (user.id, user.username, local_now().isoformat()),
        )
    conn.commit()
    conn.close()

    await update.message.reply_text(
        "Добро пожаловать в бота СOT! Выберите раздел.",
        reply_markup=main_menu(),
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update.message.reply_text(f"Ваш ID: {user.id}")


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id, username FROM admins")
    rows = c.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("Нет администраторов.")
        return

    lines = ["Администраторы:"]
    for r in rows:
        lines.append(f" • {r['username'] or '—'} (ID: {r['user_id']})")

    await update.message.reply_text("\n".join(lines))


async def cmd_set_schedule_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Только администратор может установить группу уведомлений.")
        return

    args = context.args
    if not args:
        await update.message.reply_text("Использование: /set_schedule_group <chat_id>")
        return

    chat_id = args[0].strip()
    try:
        chat_id_int = int(chat_id)
    except Exception:
        await update.message.reply_text("Неверный chat_id – должен быть числом.")
        return

    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_notify_chat_id', ?)",
        (str(chat_id_int),),
    )
    conn.commit()
    conn.close()

    await update.message.reply_text(f"Группа уведомлений по графику установлена: {chat_id_int}")


def main_menu() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add("График", "Замечания")
    keyboard.add("Инспектор", "Аналитика")
    return keyboard


async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip().lower()
    if "график" in text:
        await handle_schedule(update, context)
    elif "замечания" in text:
        await handle_remarks(update, context)
    elif "инспектор" in text:
        await handle_inspector_menu(update, context)
    elif "аналитика" in text:
        context.user_data["await_analytics_pass"] = True
        await update.message.reply_text("Введите пароль для аналитики:")
    else:
        await update.message.reply_text("Выберите раздел.", reply_markup=main_menu())


async def handle_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    df = get_schedule_df()
    if df is None:
        await update.message.reply_text("Не удалось загрузить график.")
        return

    settings = get_schedule_state()
    ver = get_schedule_version(settings)
    file_names = get_schedule_file_names()
    name = file_names.get(ver, f"Версия {ver}")

    col_date = find_col(df, ["дата выезда"])
    col_object = find_col(df, ["наименование объекта"])
    col_address = find_col(df, ["строительный адрес"])
    col_type = find_col(df, ["вид проверки"])

    if not all([col_date, col_object, col_address, col_type]):
        await update.message.reply_text("Не удалось распознать столбцы в графике.")
        return

    lines = [f"📅 График выездов ({name}):"]
    today = local_now().date()

    for i, row in df.iterrows():
        raw_date = row[col_date]
        try:
            dep_date = pd.to_datetime(raw_date).date()
            date_str = dep_date.strftime("%d.%m.%Y")
        except Exception:
            date_str = str(raw_date or "—")

        obj = str(row[col_object] or "—")
        addr = str(row[col_address] or "—")
        typ = str(row[col_type] or "—")

        if dep_date == today:
            lines.append(f" • **Сегодня ({date_str})**: {obj}, {addr} ({typ})")
        elif dep_date > today:
            lines.append(f" • {date_str}: {obj}, {addr} ({typ})")

    if len(lines) == 1:
        lines.append("Нет предстоящих выездов.")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    if is_admin(update.effective_user.id):
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Загрузить новый график", callback_data="upload_schedule")],
            ]
        )
        await update.message.reply_text("Админ: действия с графиком", reply_markup=keyboard)


async def handle_remarks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    df = get_remarks_df()
    if df is None:
        await update.message.reply_text("Не удалось загрузить замечания.")
        return

    col_object = find_col(df, ["наименование объекта"])
    col_remark = find_col(df, ["замечания"])
    col_status = find_col(df, ["статус"])

    if not all([col_object, col_remark]):
        await update.message.reply_text("Не удалось распознать столбцы в замечаниях.")
        return

    lines = ["📝 Замечания:"]
    for i, row in df.iterrows():
        obj = str(row[col_object] or "—")
        remark = str(row[col_remark] or "—")
        status = str(row[col_status] or "—")
        lines.append(f" • {obj}: {remark} ({status})")

    if len(lines) == 1:
        lines.append("Нет замечаний.")

    await update.message.reply_text("\n".join(lines))

    if is_admin(update.effective_user.id):
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Загрузить новый файл замечаний", callback_data="upload_remarks")],
            ]
        )
        await update.message.reply_text("Админ: действия с замечаниями", reply_markup=keyboard)


async def handle_inspector_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add("➕ Добавить выезд")
    keyboard.add("Назад")
    await update.message.reply_text("Меню инспектора", reply_markup=keyboard)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    data = query.data

    if data == "upload_schedule":
        context.user_data["await_schedule_file"] = True
        await query.answer("Отправьте файл графика (.xlsx)")
    elif data == "upload_remarks":
        context.user_data["await_remarks_file"] = True
        await query.answer("Отправьте файл замечаний (.xlsx)")
    # Добавьте другие callback, если есть


async def handle_inspector_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    step = context.user_data.get("insp_step", 0)
    form = context.user_data.get("insp_form", {})
    text = update.message.text.strip()

    keys = [
        "date_departure", "date_final", "area", "floors", "onzs",
        "developer", "object", "address", "case_no", "check_type"
    ]

    if step in [0, 1]:
        try:
            dt = datetime.strptime(text, "%d.%m.%Y")
            form[keys[step]] = dt
        except ValueError:
            await update.message.reply_text("Неверный формат даты (дд.мм.гггг). Попробуйте снова.")
            return
    else:
        form[keys[step]] = text

    step += 1
    context.user_data["insp_step"] = step
    context.user_data["insp_form"] = form

    questions = [
        "Введите дату выезда (дд.мм.гггг):",
        "Введите дату начала итоговой проверки (дд.мм.гггг):",
        "Введите площадь (кв.м):",
        "Введите количество этажей:",
        "Введите ОНзС:",
        "Введите наименование застройщика:",
        "Введите наименование объекта:",
        "Введите строительный адрес:",
        "Введите номер дела:",
        "Введите вид проверки:"
    ]

    if step < len(questions):
        await update.message.reply_text(questions[step])
    else:
        if append_inspector_row_to_excel(form):
            await update.message.reply_text(
                f"Выезд сохранён в лист «{INSPECTOR_SHEET_NAME}».",
                reply_markup=main_menu(),
            )
        else:
            await update.message.reply_text(
                "Не удалось сохранить выезд. Сообщите администратору.",
                reply_markup=main_menu(),
            )
        del context.user_data["insp_step"]
        del context.user_data["insp_form"]


async def handle_analytics_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
            lines.append(f" • {r['approver'] or '—'}: {r['decision']} — {r['c']} раз(а)")
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
            lines.append(f" • {file_label}: {appr_label} — {dec_text} {dt_str}{comment}")
    else:
        lines.append(" • пока нет решений по графику")

    await update.message.reply_text("\n".join(lines))


async def handle_schedule_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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


# ----------------- ДОКУМЕНТЫ -----------------

async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.document:
        return

    user = update.effective_user
    if not user:
        return

    doc: Document = msg.document
    if not doc.file_name.lower().endswith(".xlsx"):
        await msg.reply_text("Нужен файл в формате .xlsx")
        return

    # график
    if context.user_data.get("await_schedule_file"):
        if not is_admin(user.id):
            await msg.reply_text("Только администратор может загружать график.")
            return
        context.user_data["await_schedule_file"] = False
        await msg.reply_text("Загрузка локальных файлов отключена. Используйте Google Sheets напрямую.")
        return

    # замечания
    if context.user_data.get("await_remarks_file"):
        if not is_admin(user.id):
            await msg.reply_text("Только администратор может загружать рабочий файл.")
            return
        context.user_data["await_remarks_file"] = False
        await msg.reply_text("Загрузка локальных файлов отключена. Используйте Google Sheets напрямую.")
        return


# ----------------- РОУТЕР ТЕКСТА -----------------

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()
    if context.user_data.get("await_analytics_pass"):
        await handle_analytics_password(update, context)
        return

    if context.user_data.get("await_schedule_name"):
        await handle_schedule_name_input(update, context)
        return

    if "insp_step" in context.user_data:
        await handle_inspector_step(update, context)
        return

    if text == "➕ Добавить выезд":
        context.user_data["insp_step"] = 0
        context.user_data["insp_form"] = {}
        await update.message.reply_text("Введите дату выезда (дд.мм.гггг):", reply_markup=ReplyKeyboardRemove())
        return

    if text == "Назад":
        await update.message.reply_text("Возврат в главное меню.", reply_markup=main_menu())
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

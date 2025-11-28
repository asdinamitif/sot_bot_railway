import logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List

import time as time_module
import pandas as pd
import requests
from dotenv import load_dotenv
from openpyxl import load_workbook
from urllib.parse import urlencode
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
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

# -------------------------------------------------------------
#                      ЛОГИ И БАЗА ОКРУЖЕНИЯ
# -------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

SCHEDULE_PATH = os.getenv("SCHEDULE_PATH", "График выездов отдела СОТ.xlsx")
REMARKS_PATH = os.getenv("REMARKS_PATH", "График выездов отдела СОТ.xlsx")

REMARKS_URL = os.getenv("REMARKS_URL", "").strip()

# ------------------ АВТО-ИСПРАВЛЕНИЕ .by → .ru -------------------
if "disk.yandex.by" in REMARKS_URL:
    corrected = REMARKS_URL.replace("disk.yandex.by", "disk.yandex.ru")
    log.info("Исправляю REMARKS_URL: %s → %s", REMARKS_URL, corrected)
    REMARKS_URL = corrected

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# Дефолтный админ — ты
DEFAULT_ADMIN_USERNAMES = ["asdinamitif"]

# Кэш
SCHEDULE_CACHE: Dict[str, Any] = {"mtime": None, "df": None}
REMARKS_CACHE: Dict[str, Any] = {"mtime": None, "df": None}


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)

# -------------------------------------------------------------
#               НАДЁЖНОЕ ЧТЕНИЕ EXCEL (ГРАФИК)
# -------------------------------------------------------------
def load_excel_cached(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Чтение excel для '📅 График' с безопасностью и кэшированием."""
    if not os.path.exists(path):
        return None

    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]

    log.info("Загружаю Excel (График): %s", path)

    try:
        raw = pd.read_excel(path, sheet_name=0, header=None)
    except Exception as e:
        log.warning("Файл %s не похож на Excel (%s)", path, e)
        return None

    # Ищем строку заголовков (где есть 'Дата выезда')
    header_row = 0
    for i in range(min(30, len(raw))):
        row = [str(c).lower() for c in raw.iloc[i].tolist()]
        if any("дата выезда" in c for c in row):
            header_row = i
            break

    try:
        df = pd.read_excel(path, sheet_name=0, header=header_row)
    except Exception as e:
        log.warning("Ошибка повторного чтения Excel %s: %s", path, e)
        return None

    df = df.dropna(how="all").reset_index(drop=True)
    cache["mtime"] = mtime
    cache["df"] = df

    return df

# -------------------------------------------------------------
#            НАДЁЖНОЕ ЧТЕНИЕ EXCEL ДЛЯ ЗАМЕЧАНИЙ / ОНЗС
# -------------------------------------------------------------
def load_remarks_cached(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Чтение всех листов (2023/24/25). Без падений."""
    if not os.path.exists(path):
        return None

    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]

    log.info("Загружаю REMARKS (все листы): %s", path)

    try:
        xls = pd.ExcelFile(path)
    except Exception as e:
        log.warning("Файл REMARKS не Excel (%s)", e)
        return None

    frames = []

    for sheet in xls.sheet_names:
        try:
            raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        except:
            continue

        header_row = 0
        for i in range(min(30, len(raw))):
            row = [str(c).lower() for c in raw.iloc[i].tolist()]
            if any("дата выезда" in c for c in row):
                header_row = i
                break

        try:
            df_sheet = pd.read_excel(xls, sheet_name=sheet, header=header_row)
        except:
            continue

        df_sheet = df_sheet.dropna(how="all").reset_index(drop=True)
        df_sheet["_sheet"] = sheet
        frames.append(df_sheet)

    if not frames:
        return None

    df_all = pd.concat(frames, ignore_index=True)
    cache["mtime"] = mtime
    cache["df"] = df_all
    return df_all

# -------------------------------------------------------------
#        КОРРЕКТНАЯ ЗАГРУЗКА Excel С Яндекс.Диска (API)
# -------------------------------------------------------------
def download_remarks_if_needed() -> None:
    """
    Качает Excel только если локального файла нет.
    Использует API:
    https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key=...
    """
    if not REMARKS_URL:
        return

    if os.path.exists(REMARKS_PATH):
        return

    try:
        log.info("Скачиваю REMARKS из Яндекс.Диска…")

        # 1) Получаем прямую ссылку (href)
        api = (
            "https://cloud-api.yandex.net/v1/disk/public/resources/download?"
            + urlencode({"public_key": REMARKS_URL})
        )
        meta = requests.get(api, timeout=20)
        meta.raise_for_status()
        data = meta.json()
        href = data.get("href")

        if not href:
            log.warning("Яндекс не дал href. Ответ: %s", str(data)[:300])
            return

        # 2) Качаем сам Excel
        file = requests.get(href, timeout=60)
        file.raise_for_status()

        with open(REMARKS_PATH, "wb") as f:
            f.write(file.content)

        REMARKS_CACHE["mtime"] = None
        REMARKS_CACHE["df"] = None

        log.info("Файл REMARKS успешно скачан.")

    except Exception as e:
        log.warning("Ошибка скачивания REMARKS: %s", e)

def get_schedule_df(): 
    return load_excel_cached(SCHEDULE_PATH, SCHEDULE_CACHE)

def get_remarks_df():
    download_remarks_if_needed()
    return load_remarks_cached(REMARKS_PATH, REMARKS_CACHE)

# -------------------------------------------------------------
#                  ПОИСК КОЛОНОК В EXCEL
# -------------------------------------------------------------
def find_col(df: pd.DataFrame, hints) -> Optional[str]:
    """Поиск колонки по подстроке."""
    if isinstance(hints, str):
        hints = [hints]

    hints = [h.lower() for h in hints]

    for col in df.columns:
        low = str(col).lower()
        if any(h in low for h in hints):
            return col

    return None


# -------------------------------------------------------------
#              МОДУЛЬ «ИНСПЕКТОР» — запись строки
# -------------------------------------------------------------
INSPECTOR_SHEET_NAME = os.getenv(
    "INSPECTOR_SHEET_NAME", "ПБ, АР,ММГН, АГО (2025)"
)

def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    """Добавление строки инспектора в Excel."""

    if not os.path.exists(REMARKS_PATH):
        log.warning("Нет REMARKS_PATH для записи инспектора.")
        return False

    try:
        wb = load_workbook(REMARKS_PATH)
    except Exception as e:
        log.warning("Ошибка открытия REMARKS_PATH: %s", e)
        return False

    ws = wb[INSPECTOR_SHEET_NAME] if INSPECTOR_SHEET_NAME in wb.sheetnames else wb.create_sheet(INSPECTOR_SHEET_NAME)

    # найти последнюю строку по колонке B
    last = 1
    for r in range(1, (ws.max_row or 1) + 1):
        if ws.cell(row=r, column=2).value not in (None, ""):
            last = r

    row = last + 1

    # B — дата
    dt = form.get("date")
    if isinstance(dt, datetime) or isinstance(dt, date):
        dt = dt.strftime("%d.%m.%Y")
    ws.cell(row=row, column=2).value = dt or ""

    # D — площадь + этажность
    ws.cell(row=row, column=4).value = (
        f"Площадь (кв.м): {form.get('area','')}\n"
        f"Количество этажей: {form.get('floors','')}"
    )

    ws.cell(row=row, column=5).value = form.get("onzs", "")
    ws.cell(row=row, column=6).value = form.get("developer", "")
    ws.cell(row=row, column=7).value = form.get("object", "")
    ws.cell(row=row, column=8).value = form.get("address", "")
    ws.cell(row=row, column=9).value = form.get("case_no", "")
    ws.cell(row=row, column=10).value = form.get("check_type", "")

    try:
        wb.save(REMARKS_PATH)
    except Exception as e:
        log.warning("Ошибка сохранения Excel: %s", e)
        return False

    REMARKS_CACHE["mtime"] = None
    REMARKS_CACHE["df"] = None
    return True


# -------------------------------------------------------------
#                       БАЗА ДАННЫХ
# -------------------------------------------------------------
def init_db() -> None:
    """Создание таблиц + автодобавление администратора."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule_meta (
            id INTEGER PRIMARY KEY,
            current_rev INTEGER NOT NULL,
            file_name TEXT,
            uploaded_at TEXT,
            approvers TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            schedule_rev INTEGER NOT NULL,
            username TEXT NOT NULL,
            approved_at TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS remarks_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            approvers TEXT,
            approved_by TEXT,
            status TEXT
        );
    """)

    cur.execute("SELECT COUNT(*) FROM admins;")
    if cur.fetchone()[0] == 0:
        for u in DEFAULT_ADMIN_USERNAMES:
            cur.execute("INSERT OR IGNORE INTO admins (username) VALUES (?);", (u,))

    conn.commit()
    conn.close()
    log.info("База данных инициализирована.")


def get_admins() -> List[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT username FROM admins ORDER BY username;")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows


def add_admin(username: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO admins (username) VALUES (?);", (username,))
    conn.commit()
    conn.close()


def del_admin(username: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM admins WHERE username = ?;", (username,))
    conn.commit()
    conn.close()


def is_super_admin(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False

    # проверка по ID
    if ADMIN_ID and user.id == ADMIN_ID:
        return True

    # проверка по username
    uname = (user.username or "").lower()
    return uname in [a.lower() for a in get_admins()]


# -------------------------------------------------------------
#                          КОМАНДЫ
# -------------------------------------------------------------
MAIN_MENU_KEYBOARD = [
    ["📅 График", "📝 Замечания"],
    ["🏗 ОНзС", "📈 Аналитика"],
    ["👮‍♂️ Инспектор"],
]

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = ReplyKeyboardMarkup(MAIN_MENU_KEYBOARD, resize_keyboard=True)
    await update.message.reply_text(
        "Добро пожаловать в бота отдела СОТ.\n\nВыберите раздел.",
        reply_markup=kb
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    c = update.effective_chat
    await update.message.reply_text(
        f"user_id = {u.id}\nchat_id = {c.id}"
    )


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super_admin(update):
        await update.message.reply_text("Нет прав.")
        return

    admins = get_admins()
    if not admins:
        await update.message.reply_text("Админов нет.")
        return

    txt = "Администраторы:\n" + "\n".join(f"• {a}" for a in admins)
    await update.message.reply_text(txt)


async def cmd_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super_admin(update):
        await update.message.reply_text("Нет прав.")
        return

    if not context.args:
        await update.message.reply_text("Использование: /add_admin @username")
        return

    username = context.args[0].lstrip("@")
    add_admin(username)
    await update.message.reply_text(f"@{username} добавлен как админ.")


async def cmd_del_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super_admin(update):
        await update.message.reply_text("Нет прав.")
        return

    if not context.args:
        await update.message.reply_text("Использование: /del_admin @username")
        return

    username = context.args[0].lstrip("@")
    del_admin(username)
    await update.message.reply_text(f"@{username} удалён из админов.")

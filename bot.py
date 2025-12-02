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

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

# ----------------- ENV -----------------
load_dotenv()

# Если BOT_TOKEN нет в переменных окружения – берём жёстко прописанный
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "8274616381:AAE4Av9RgX8iSRfM1n2U9V8oPoWAf-bB_hA").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

# Один Excel для всего
SCHEDULE_URL = os.getenv("SCHEDULE_URL", "").strip()
REMARKS_URL = os.getenv("REMARKS_URL", "").strip()

SCHEDULE_PATH = os.getenv("SCHEDULE_PATH", "schedule.xlsx")
REMARKS_PATH = os.getenv("REMARKS_PATH", "remarks.xlsx")

SCHEDULE_SYNC_TTL_SEC = int(os.getenv("SCHEDULE_SYNC_TTL_SEC", "3600"))
REMARKS_SYNC_TTL_SEC = int(os.getenv("REMARKS_SYNC_TTL_SEC", "3600"))

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

INSPECTOR_SHEET_NAME = "ПБ, АР,ММГН, АГО (2025)"

HARD_CODED_ADMINS = {398960707}


def is_admin(uid: int) -> bool:
    return uid in HARD_CODED_ADMINS


SCHEDULE_CACHE = {"mtime": None, "df": None}
REMARKS_CACHE = {"mtime": None, "df": None}


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


# ----------------- ЗАГРУЗКА ФАЙЛОВ (ЯНДЕКС / GOOGLE / ПРЯМОЙ URL) -----------------

def download_file_from_yandex(public_url: str) -> bytes:
    """
    Скачивает файл по публичной ссылке.

    Поддерживает:
      • Яндекс.Диск: https://disk.yandex.ru/... или https://disk.yandex.by/...
      • Google Sheets: https://docs.google.com/spreadsheets/d/... (экспорт в .xlsx)
      • Любые прямые URL – скачиваются как есть.
    """
    try:
        # -------- Google Sheets --------
        if "docs.google.com" in public_url and "/spreadsheets/" in public_url:
            log.info("Скачиваю Google Sheets как .xlsx: %s", public_url)
            part = public_url.split("/spreadsheets/d/", 1)[1]
            sheet_id = part.split("/", 1)[0]
            export_url = (
                f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
            )
            resp = requests.get(export_url, timeout=60)
            resp.raise_for_status()
            log.info("Google Sheets скачан, размер: %s байт", len(resp.content))
            return resp.content

        # -------- Яндекс.Диск --------
        if "disk.yandex" in public_url:
            log.info("Пробую получить прямой href Яндекс.Диска для URL: %s", public_url)
            api = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
            r = requests.get(api, params={"public_key": public_url}, timeout=30)
            r.raise_for_status()
            data = r.json()

            href = data.get("href")
            if not href:
                raise RuntimeError(f"Нет href в ответе Яндекс.Диска: {data}")

            log.info("Получен прямой href: %s", href)
            file_resp = requests.get(href, timeout=60)
            file_resp.raise_for_status()
            log.info(
                "Файл скачан с Яндекс.Диска, размер: %s байт", len(file_resp.content)
            )
            return file_resp.content

        # -------- Прямой URL --------
        log.info("Скачиваю файл по прямому URL: %s", public_url)
        resp = requests.get(public_url, timeout=60)
        resp.raise_for_status()
        log.info("Файл скачан по прямому URL, размер: %s байт", len(resp.content))
        return resp.content

    except Exception as e:
        log.error("Ошибка скачивания файла (%s): %s", public_url, e)
        raise


def download_file_if_needed(url: str, local_path: str, ttl_seconds: int) -> None:
    """
    Универсальная функция:
      – если файла нет → скачиваем
      – если устарел → скачиваем
      – иначе ничего не делаем
    """
    if not url:
        log.warning(f"URL не задан для {local_path}.")
        return

    need = False

    if not os.path.exists(local_path):
        need = True
        log.info(f"Файл {local_path} отсутствует — требуется загрузка.")
    else:
        age = time_module.time() - os.path.getmtime(local_path)
        if age > ttl_seconds:
            need = True
            log.info(f"Файл {local_path} старше TTL → требуется обновить.")

    if not need:
        return

    try:
        log.info(f"Скачиваю файл {local_path} из: {url}")
        content = download_file_from_yandex(url)
        with open(local_path, "wb") as f:
            f.write(content)
        log.info(f"Файл сохранён: {local_path}")
    except Exception as e:
        log.error(f"Не удалось скачать или сохранить {local_path}: {e}")


# ----------------- ЧТЕНИЕ EXCEL С КЕШИРОВАНИЕМ -----------------

def load_excel_single_sheet(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Загружает первый лист Excel. Используется для разделов 'График' и 'Итоговая'."""
    if not os.path.exists(path):
        log.error(f"Файл {path} не найден при загрузке.")
        return None

    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]

    try:
        log.info(f"Загружаю Excel (1 лист): {path}")
        raw = pd.read_excel(path, sheet_name=0, header=None)

        # ищем строку заголовков
        header_row = 0
        for i in range(min(30, len(raw))):
            row = raw.iloc[i].astype(str).tolist()
            if any("дата выезда" in c.lower() for c in row):
                header_row = i
                break

        df = pd.read_excel(path, sheet_name=0, header=header_row)
        df = df.dropna(how="all").reset_index(drop=True)

        cache["mtime"] = mtime
        cache["df"] = df

        log.info(f"Excel загружен: {path}, строк={df.shape[0]}, столбцов={df.shape[1]}")
        return df

    except Exception as e:
        log.error(f"Ошибка чтения Excel {path}: {e}")
        return None


def load_excel_all_sheets(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Загружает ВСЕ листы Excel и объединяет в один DataFrame.
    Используется для Замечания / ОНЗС.
    """
    if not os.path.exists(path):
        log.error(f"Файл {path} не найден при чтении всех листов.")
        return None

    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]

    try:
        log.info(f"Читаю Excel (все листы): {path}")
        xls = pd.ExcelFile(path)
    except Exception as e:
        log.error(f"Не удалось открыть Excel {path}: {e}")
        return None

    frames = []

    for sheet in xls.sheet_names:
        try:
            raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        except Exception as e:
            log.warning(f"Не удалось прочитать лист {sheet}: {e}")
            continue

        # ищем заголовки
        header_row = 0
        for i in range(min(30, len(raw))):
            row = raw.iloc[i].astype(str).tolist()
            if any("дата выезда" in c.lower() for c in row):
                header_row = i
                break

        try:
            df_sheet = pd.read_excel(xls, sheet_name=sheet, header=header_row)
            df_sheet = df_sheet.dropna(how="all").reset_index(drop=True)
            df_sheet["_sheet"] = sheet
            frames.append(df_sheet)
        except Exception as e:
            log.warning(f"Ошибка чтения листа {sheet} c header={header_row}: {e}")

    if not frames:
        log.error("Excel прочитан, но листы пустые или нераспознаны.")
        return None

    df_all = pd.concat(frames, ignore_index=True)
    cache["mtime"] = mtime
    cache["df"] = df_all

    log.info(f"Excel полностью загружен: строк={df_all.shape[0]}, столбцов={df_all.shape[1]}")
    return df_all


# ----------------- ОБЁРТКИ ДЛЯ ИСПОЛЬЗОВАНИЯ -----------------

def get_schedule_df() -> Optional[pd.DataFrame]:
    download_file_if_needed(SCHEDULE_URL, SCHEDULE_PATH, SCHEDULE_SYNC_TTL_SEC)
    return load_excel_single_sheet(SCHEDULE_PATH, SCHEDULE_CACHE)


def get_remarks_df() -> Optional[pd.DataFrame]:
    download_file_if_needed(REMARKS_URL, REMARKS_PATH, REMARKS_SYNC_TTL_SEC)
    return load_excel_all_sheets(REMARKS_PATH, REMARKS_CACHE)


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


# ----------------- ЗАПИСЬ В ИНСПЕКТОРСКИЙ ЛИСТ -----------------

def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    """
    Добавляет новую строку выезда в лист INSPECTOR_SHEET_NAME файла REMARKS_PATH.
    Лист уже существует в файле.
    """
    if not os.path.exists(REMARKS_PATH):
        log.warning("REMARKS_PATH не найден, некуда сохранять выезд инспектора.")
        return False

    try:
        wb = load_workbook(REMARKS_PATH)
    except Exception as e:
        log.warning(f"Не удалось открыть REMARKS_PATH для записи инспектора: {e}")
        return False

    if INSPECTOR_SHEET_NAME not in wb.sheetnames:
        log.warning(f"Лист {INSPECTOR_SHEET_NAME} не найден в REMARKS_PATH.")
        return False

    ws = wb[INSPECTOR_SHEET_NAME]

    # ищем последнюю занятую строку по столбцу B (дата выезда)
    last_data_row = 1
    max_row = ws.max_row or 1
    for r in range(1, max_row + 1):
        val = ws.cell(row=r, column=2).value  # B
        if val not in (None, ""):
            last_data_row = r

    new_row = last_data_row + 1

    # B — дата выезда
    date_obj = form.get("date")
    if isinstance(date_obj, datetime):
        date_str = date_obj.strftime("%d.%m.%Y")
    elif isinstance(date_obj, date):
        date_str = date_obj.strftime("%d.%m.%Y")
    else:
        date_str = str(date_obj or "")
    ws.cell(row=new_row, column=2).value = date_str

    # D — площадь + этажность
    area = form.get("area") or ""
    floors = form.get("floors") or ""
    ws.cell(row=new_row, column=4).value = (
        f"Площадь (кв.м): {area}\nКоличество этажей: {floors}"
    )

    # E — ОНзС
    ws.cell(row=new_row, column=5).value = form.get("onzs") or ""
    # F — Застройщик
    ws.cell(row=new_row, column=6).value = form.get("developer") or ""
    # G — Объект
    ws.cell(row=new_row, column=7).value = form.get("object") or ""
    # H — Адрес
    ws.cell(row=new_row, column=8).value = form.get("address") or ""
    # I — Номер дела
    ws.cell(row=new_row, column=9).value = form.get("case_no") or ""
    # J — Вид проверки
    ws.cell(row=new_row, column=10).value = form.get("check_type") or ""

    try:
        wb.save(REMARKS_PATH)
    except Exception as e:
        log.warning(f"Не удалось сохранить REMARKS_PATH после добавления выезда: {e}")
        return False

    # сбрасываем кеш, чтобы при следующем чтении прочитать обновлённый файл
    REMARKS_CACHE["mtime"] = None
    REMARKS_CACHE["df"] = None

    log.info(
        f"Инспектор добавил выезд (строка {new_row}) в лист {INSPECTOR_SHEET_NAME}"
    )
    return True


# ----------------- БАЗА ДАННЫХ -----------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    c = conn.cursor()

    c.execute(
        """CREATE TABLE IF NOT EXISTS admins (
               user_id INTEGER PRIMARY KEY,
               username TEXT,
               first_seen_at TEXT
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
        """CREATE TABLE IF NOT EXISTS schedule_files (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               version INTEGER,
               uploaded_by INTEGER,
               uploaded_at TEXT,
               path TEXT
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
        """CREATE TABLE IF NOT EXISTS approvers (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               label TEXT UNIQUE
           )"""
    )

    # наполняем approvers по умолчанию
    c.execute("SELECT COUNT(*) AS c FROM approvers")
    if c.fetchone()["c"] == 0:
        c.executemany(
            "INSERT OR IGNORE INTO approvers (label) VALUES (?)",
            [(lbl,) for lbl in DEFAULT_APPROVERS],
        )

    # версия графика по умолчанию
    c.execute("SELECT value FROM schedule_settings WHERE key='schedule_version'")
    row_ver = c.fetchone()
    if not row_ver:
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) "
            "VALUES ('schedule_version', '1')"
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


def get_schedule_version(settings: dict) -> int:
    try:
        return int(settings.get("schedule_version") or "1")
    except Exception:
        return 1


# ----------------- УПРАВЛЕНИЕ АДМИНАМИ (СПРАВОЧНО) -----------------

async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Команда доступна только администраторам.")
        return

    await update.message.reply_text(
        "Администраторы заданы жёстко в коде:\n• @asdinamitif (398960707)"
    )


# ----------------- КЛАВИАТУРА МЕНЮ -----------------

def main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📅 График", "📊 Итоговая"],
        ["📝 Замечания", "🏗 ОНзС"],
        ["Инспектор", "📈 Аналитика"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# ----------------- ТЕКСТ И КНОПКИ ДЛЯ 📅 ГРАФИК -----------------

def build_schedule_text(is_admin_flag: bool, settings: dict) -> str:
    approvers = get_current_approvers(settings)
    version = get_schedule_version(settings)

    lines: List[str] = []
    lines.append("Раздел «График».")
    lines.append("")
    lines.append(f"Текущая версия файла графика: {version}")
    lines.append("")
    lines.append(
        "Порядок работы:\n"
        "1) Администратор выбирает, КТО согласует (из списка @... или добавляет своего).\n"
        "2) У выбранных появится уведомление «У вас на рассмотрении новый график».\n"
        "3) Каждый согласующий нажимает «✅ Согласовать» или «✏ На доработку».\n"
        "4) Внизу видно, кто уже согласовал и когда, а кто ещё в ожидании."
    )
    lines.append("")
    lines.append("Статусы согласования:")

    if not approvers:
        lines.append("• Согласующие ещё не выбраны.")
        return "\n".join(lines)

    conn = get_db()
    c = conn.cursor()

    placeholders = ",".join("?" * len(approvers))
    params: List[Any] = [get_schedule_version(settings)] + approvers

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
            dt_str = dt_obj.strftime("%d.%m.%Y %H:%М")
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
        lines.append("Итог: график по текущей версии направлен на доработку.")
    elif approved_count == total and total > 0:
        lines.append("Итог: все выбранные согласующие утвердили график.")
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


# ----------------- КНОПКИ ДРУГИХ РАЗДЕЛОВ -----------------

def remarks_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Устранены", callback_data="remarks_done"),
                InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done"),
            ],
            [
                InlineKeyboardButton(
                    "➖ Не требуется", callback_data="remarks_not_required"
                ),
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


# ----------------- КОМАНДЫ /start, /id -----------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    msg = "Привет! Это бот отдела СОТ.\n"
    if is_admin(user.id):
        msg += "Вы — администратор бота (жёстко задано в коде).\n"
    msg += "Выберите раздел на клавиатуре ниже."

    # регистрируем пользователя в БД (для уведомлений согласования)
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT OR IGNORE INTO users (user_id, username, first_seen_at)
           VALUES (?, ?, ?)""",
        (user.id, user.username or "", local_now().isoformat()),
    )
    conn.commit()
    conn.close()

    await update.message.reply_text(msg, reply_markup=main_menu())


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    await update.message.reply_text(
        f"Ваш id: {user.id}\nusername: @{user.username or ''}"
    )


# ----------------- ОБРАБОТКА КНОПОК МЕНЮ -----------------

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
            await update.message.reply_text("Файл графика не загружен или повреждён.")
            return

        lines = ["ИТОГОВАЯ ИНФОРМАЦИЯ", ""]

        col_date = find_col(df, ["дата"])
        col_type = find_col(df, ["итоговая", "тип"])
        col_case = find_col(df, ["дело"])

        if col_date is None or col_type is None:
            await update.message.reply_text("Не удалось определить столбцы итоговой.")
            return

        today = local_now().date()
        upcoming = df[
            (pd.to_datetime(df[col_date], errors="coerce").dt.date >= today)
            & (df[col_type].astype(str).str.contains("итог", case=False))
        ]

        if upcoming.empty:
            lines.append("Ближайших итоговых проверок не найдено.")
        else:
            for _, r in upcoming.head(10).iterrows():
                d = ""
                try:
                    d = pd.to_datetime(r[col_date]).strftime("%d.%m.%Y")
                except Exception:
                    d = str(r[col_date])

                typ = str(r[col_type])
                case_no = (
                    str(r[col_case]) if col_case in df.columns else "(нет дела)"
                )

                lines.append(f"• {d} — {typ} — дело: {case_no}")

        await update.message.reply_text("\n".join(lines))
        return

    if text == "📝 замечания".lower():
        df = get_remarks_df()
        if df is None:
            await update.message.reply_text(
                "Рабочий файл замечаний ещё не загружен или повреждён."
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
        context.user_data["insp_form"] = {}
        await update.message.reply_text(
            "Пошаговый мастер заполнения инспектора.\nВведите дату выезда (ДД.ММ.ГГГГ):"
        )
        context.user_data["insp_step"] = "date"
        return

    if text == "📈 аналитика".lower():
        await update.message.reply_text(
            "Введите пароль для доступа к аналитике:"
        )
        context.user_data["await_analytics_pass"] = True
        return

    await update.message.reply_text("Не понял команду. Выберите раздел на клавиатуре.")


# ----------------- CALLBACK-КНОПКИ -----------------

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    user = update.effective_user
    if not user:
        return

    settings = get_schedule_state()
    is_admin_flag = is_admin(user.id)

    # ----------------- ГРАФИК -----------------
    if data == "schedule_upload":
        if not is_admin_flag:
            await query.edit_message_text("Команда доступна только администратору.")
            return
        context.user_data["await_schedule_file"] = True
        await query.edit_message_text("Отправьте файл графика в формате .xlsx")
        return

    if data == "schedule_download":
        df = get_schedule_df()
        if df is None:
            await query.edit_message_text(
                "Файл графика не найден / повреждён. Проверь ссылку SCHEDULE_URL."
            )
            return

        with open(SCHEDULE_PATH, "rb") as f:
            await query.message.reply_document(
                InputFile(f, filename=os.path.basename(SCHEDULE_PATH))
            )
        return

    if data.startswith("schedule_set_approver:"):
        appr = data.split(":", 1)[1].strip()

        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('current_approver', ?)",
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
        return

    if data == "schedule_rework":
        appr = user.username
        if not appr:
            await query.edit_message_text("У вас нет username, нельзя отправить на доработку.")
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

    # ----------------- ЗАМЕЧАНИЯ -----------------

    if data == "remarks_upload":
        context.user_data["await_remarks_file"] = True
        await query.edit_message_text("Отправьте Excel-файл замечаний (.xlsx)")
        return

    if data == "remarks_download":
        df = get_remarks_df()
        if df is None:
            await query.edit_message_text("Файл замечаний не найден.")
            return

        with open(REMARKS_PATH, "rb") as f:
            await query.message.reply_document(
                InputFile(f, filename=os.path.basename(REMARKS_PATH))
            )
        return

    if data.startswith("remarks_"):
        status = data.replace("remarks_", "")
        context.user_data["remarks_status"] = status
        await query.edit_message_text(
            f"Введите номер строки в Excel для установки статуса '{status}':"
        )
        context.user_data["await_remarks_row"] = True
        return

    # ----------------- ОНЗС -----------------

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

        if mode == "all":
            pass
        else:
            days = int(mode)
            dt_min = local_now().date() - timedelta(days=days)

            df2 = df2[
                pd.to_datetime(df2[col_date], errors="coerce").dt.date >= dt_min
            ]

        if df2.empty:
            await query.edit_message_text(f"Нет данных для выбранного периода.")
            return

        lines = [f"ОНзС {num}:"]
        for _, r in df2.head(50).iterrows():
            d = ""
            try:
                d = pd.to_datetime(r[col_date]).strftime("%d.%m.%Y")
            except Exception:
                d = str(r[col_date])
            lines.append(f"• {d} — {r.to_dict()}")

        await query.edit_message_text("\n".join(lines))
        return

    await query.edit_message_text("Команда не распознана.")


# ----------------- ДОП. ОБРАБОТЧИКИ СОСТОЯНИЙ -----------------

async def handle_custom_approver_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        context.user_data["await_custom_approver"] = False
        await update.message.reply_text("Добавлять согласующих может только администратор.")
        return

    text = (update.message.text or "").strip()
    context.user_data["await_custom_approver"] = False

    if not text:
        await update.message.reply_text("Не понял username. Введите, например: @ivanov")
        return

    if not text.startswith("@"):
        text = "@" + text

    label = text

    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO approvers (label) VALUES (?)", (label,))
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('current_approver', ?)",
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


async def handle_remarks_row_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get("await_remarks_row"):
        return

    user = update.effective_user
    if not user:
        return

    text = (update.message.text or "").strip()
    try:
        row_num = int(text)
    except ValueError:
        await update.message.reply_text("Нужно ввести номер строки (целое число). Попробуйте ещё раз.")
        return

    status_key = context.user_data.get("remarks_status")  # done / not_done / not_required
    context.user_data["await_remarks_row"] = False

    if not status_key:
        await update.message.reply_text("Не удалось определить статус. Начните заново из раздела «Замечания».")
        return

    # Маппинг на да/нет для аналитики
    if status_key == "done":
        pb = "да"
        pbzk = "да"
        ar = "да"
        status_text = "УСТРАНЕНЫ"
    elif status_key == "not_done":
        pb = "нет"
        pbzk = "нет"
        ar = "нет"
        status_text = "НЕ УСТРАНЕНЫ"
    else:
        pb = None
        pbzk = None
        ar = None
        status_text = "НЕ ТРЕБУЕТСЯ"

    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT INTO remarks_status (excel_row, pb_status, pbzk_status, ar_status, updated_by, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (row_num, pb, pbzk, ar, user.id, local_now().isoformat()),
    )
    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"Для строки {row_num} установлен статус: {status_text}."
    )


async def handle_onzs_custom_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get("onzs_custom"):
        return

    user = update.effective_user
    if not user:
        return

    text = (update.message.text or "").strip()
    context.user_data["onzs_custom"] = False
    num = context.user_data.get("onzs_num")

    if not num:
        await update.message.reply_text("ОНзС не определён. Начните заново из раздела «ОНзС».")
        return

    try:
        # поддержка разных тире
        t = text.replace("—", "-").replace("–", "-")
        s1, s2 = [p.strip() for p in t.split("-", 1)]
        d1 = datetime.strptime(s1, "%d.%м.%Y").date()
        d2 = datetime.strptime(s2, "%d.%м.%Y").date()
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

    lines = [f"ОНзС {num} за период {d1.strftime('%d.%м.%Y')}–{d2.strftime('%d.%м.%Y')}:"]

    for _, r in df2.head(50).iterrows():
        dstr = ""
        try:
            dstr = pd.to_datetime(r[col_date]).strftime("%d.%m.%Y")
        except Exception:
            dstr = str(r[col_date])
        lines.append(f"• {dstr} — {r.to_dict()}")

    await update.message.reply_text("\n".join(lines))


# ----------------- ИНСПЕКТОР: ПОШАГОВЫЙ МАСТЕР -----------------

async def handle_inspector_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    step = context.user_data.get("insp_step")
    if not step:
        return

    form = context.user_data.get("insp_form", {})
    text = (update.message.text or "").strip()

    if step == "date":
        try:
            d = datetime.strptime(text, "%d.%м.%Y").date()
        except Exception:
            await update.message.reply_text(
                "Не понял дату. Введите в формате ДД.ММ.ГГГГ, например 03.12.2025."
            )
            return
        form["date"] = d
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
                "Не удалось сохранить выезд в Excel. Сообщите администратору.",
                reply_markup=main_menu(),
            )
        return


# ----------------- АНАЛИТИКА -----------------

async def handle_analytics_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get("await_analytics_pass"):
        return

    pwd = (update.message.text or "").strip()
    context.user_data["await_analytics_pass"] = False

    if pwd != ANALYTICS_PASSWORD:
        await update.message.reply_text("Неверный пароль.")
        return

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
            lines.append(f" • Версия {ver}: {appr_label} — {dec_text} {dt_str}{comment}")
    else:
        lines.append(" • пока нет решений по графику")

    await update.message.reply_text("\n".join(lines))


# ----------------- ОБРАБОТКА ДОКУМЕНТОВ (ЗАГРУЗКА EXCEL) -----------------

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

    # Загрузка графика
    if context.user_data.get("await_schedule_file"):
        if not is_admin(user.id):
            await msg.reply_text("Только администратор может загружать график.")
            return

        f = await doc.get_file()
        await f.download_to_drive(SCHEDULE_PATH)
        context.user_data["await_schedule_file"] = False

        SCHEDULE_CACHE["mtime"] = None
        SCHEDULE_CACHE["df"] = None

        settings = get_schedule_state()
        ver = get_schedule_version(settings) + 1

        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_version', ?)",
            (str(ver),),
        )
        conn.commit()
        conn.close()

        await msg.reply_text(
            f"Файл графика сохранён (версия {ver}).\nОткройте раздел «📅 График».",
            reply_markup=main_menu(),
        )
        return

    # Загрузка файла замечаний
    if context.user_data.get("await_remarks_file"):
        if not is_admin(user.id):
            await msg.reply_text("Только администратор может загружать рабочий файл.")
            return

        f = await doc.get_file()
        await f.download_to_drive(REMARKS_PATH)
        context.user_data["await_remarks_file"] = False

        REMARKS_CACHE["mtime"] = None
        REMARKS_CACHE["df"] = None

        await msg.reply_text(
            "Рабочий файл замечаний сохранён. Он используется в разделах «Замечания» и «ОНзС».",
            reply_markup=main_menu(),
        )
        return


# ----------------- ОБЩИЙ РОУТЕР ТЕКСТА -----------------

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # приоритет: пароли, инспектор, доп.состояния
    if context.user_data.get("await_analytics_pass"):
        await handle_analytics_password(update, context)
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

    # иначе — обычное меню
    await main_menu_handler(update, context)


# ----------------- MAIN -----------------

def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в переменных окружения или .env")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("admins", cmd_admins))

    # Callback-кнопки
    app.add_handler(CallbackQueryHandler(callback_handler))

    # Документы (Excel)
    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))

    # Текст
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_router)
    )

    log.info("Бот запущен в режиме polling...")
    app.run_polling()


if __name__ == "__main__":
    main()

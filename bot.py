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

# ----------------- ENV / НАСТРОЙКИ -----------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

# 1-й файл: для 📅 График и 📊 Итоговая
SCHEDULE_PATH = os.getenv("SCHEDULE_PATH", "График выездов отдела СОТ.xlsx")
# 2-й файл: для 📝 Замечания и 🏗 ОНзС — по умолчанию тот же, что и SCHEDULE_PATH
REMARKS_PATH = os.getenv("REMARKS_PATH", SCHEDULE_PATH)

# URL и TTL для авто-синхронизации графика
SCHEDULE_URL = os.getenv("SCHEDULE_URL", "").strip()
SCHEDULE_SYNC_TTL_SEC = int(os.getenv("SCHEDULE_SYNC_TTL_SEC", "3600"))

# URL и TTL для авто-синхронизации замечаний
REMARKS_URL = os.getenv("REMARKS_URL", "").strip()
REMARKS_SYNC_TTL_SEC = int(os.getenv("REMARKS_SYNC_TTL_SEC", "3600"))

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))  # МСК: +3
ANALYTICS_PASSWORD = "051995"

# стандартный список согласующих (кнопки)
DEFAULT_APPROVERS = [
    "@asdinamitif",
    "@FrolovAlNGSN",
    "@cappit_G59",
    "@sergeybektiashkin",
    "@scri4",
    "@Kirill_Victorovi4",
]

# Для прав на замечания по ФИО в столбце K
RESPONSIBLE_USERNAMES = {
    "бектяшкин": ["sergeybektiashkin"],
    "смирнов": ["scri4"],
}

INSPECTOR_SHEET_NAME = os.getenv(
    "INSPECTOR_SHEET_NAME", "ПБ, АР,ММГН, АГО (2025)"
)

# ----------------- ЖЁСТКО ЗАДАННЫЕ АДМИНЫ -----------------
# Эти user_id всегда имеют права администратора, независимо от БД.
HARD_CODED_ADMINS = {398960707}  # @asdinamitif

def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором (жёсткая проверка)."""
    return user_id in HARD_CODED_ADMINS

# Кэши Excel
SCHEDULE_CACHE: Dict[str, Any] = {"mtime": None, "df": None}
REMARKS_CACHE: Dict[str, Any] = {"mtime": None, "df": None}


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


# ----------------- РАБОТА С EXCEL / ЗАГРУЗКА ФАЙЛОВ -----------------
def download_file_from_url(url: str) -> bytes:
    """
    Скачивает файл по URL.
    Если это публичная ссылка Яндекс.Диска (disk.yandex.*),
    сначала запрашивает прямой href через cloud-api.yandex.net.
    """
    if "disk.yandex" in url:
        api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
        try:
            # 1. Получаем ссылку на скачивание по public_key (публичная ссылка)
            resp = requests.get(api_url, params={"public_key": url}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            href = data.get("href")
            if not href:
                raise RuntimeError("В ответе Яндекс.Диска нет поля 'href'")
            # 2. Скачиваем сам файл по href
            file_resp = requests.get(href, timeout=60)
            file_resp.raise_for_status()
            return file_resp.content
        except Exception as e:
            log.warning("Ошибка скачивания с Яндекс.Диска (%s): %s", url, e)
            raise

    # Обычный HTTP/HTTPS URL
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def load_excel_cached(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]
    log.info("Загружаю Excel (1 лист): %s", path)
    raw = pd.read_excel(path, sheet_name=0, header=None)
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
    log.info("Файл %s загружен: строк=%s, столбцов=%s", path, df.shape[0], df.shape[1])
    return df


def load_remarks_cached(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]
    log.info("Загружаю рабочий Excel (все листы): %s", path)
    xls = pd.ExcelFile(path)
    frames: List[pd.DataFrame] = []
    for sheet in xls.sheet_names:
        try:
            raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        except Exception as e:
            log.warning("Не удалось прочитать лист %s: %s", sheet, e)
            continue
        header_row = 0
        for i in range(min(30, len(raw))):
            row = raw.iloc[i].astype(str).tolist()
            if any("дата выезда" in c.lower() for c in row):
                header_row = i
                break
        try:
            df_sheet = pd.read_excel(xls, sheet_name=sheet, header=header_row)
        except Exception as e:
            log.warning("Не удалось прочитать лист %s c header=%s: %s", sheet, header_row, e)
            continue
        df_sheet = df_sheet.dropna(how="all").reset_index(drop=True)
        df_sheet["_sheet"] = sheet
        frames.append(df_sheet)
    if not frames:
        log.warning("В рабочем файле нет пригодных листов.")
        return None
    df_all = pd.concat(frames, ignore_index=True)
    cache["mtime"] = mtime
    cache["df"] = df_all
    log.info(
        "Рабочий файл %s загружен (все листы): строк=%s, столбцов=%s",
        path,
        df_all.shape[0],
        df_all.shape[1],
    )
    return df_all


def download_schedule_if_needed() -> None:
    """Автоматическая загрузка файла графика из SCHEDULE_URL, если он отсутствует или устарел."""
    if not SCHEDULE_URL:
        return

    need_download = False
    if not os.path.exists(SCHEDULE_PATH):
        need_download = True
    else:
        try:
            mtime = os.path.getmtime(SCHEDULE_PATH)
            age = time_module.time() - mtime
            if age > SCHEDULE_SYNC_TTL_SEC:
                need_download = True
        except Exception as e:
            log.warning("Не удалось проверить возраст SCHEDULE_PATH: %s", e)
            need_download = True

    if not need_download:
        return

    try:
        log.info("Скачиваю файл графика из SCHEDULE_URL (авто-синхронизация)...")
        content = download_file_from_url(SCHEDULE_URL)
        with open(SCHEDULE_PATH, "wb") as f:
            f.write(content)
        SCHEDULE_CACHE["mtime"] = None
        SCHEDULE_CACHE["df"] = None
        log.info("Файл графика успешно скачан и сохранён в %s", SCHEDULE_PATH)
    except Exception as e:
        log.warning("Не удалось скачать файл графика из SCHEDULE_URL: %s", e)


def get_schedule_df() -> Optional[pd.DataFrame]:
    download_schedule_if_needed()
    return load_excel_cached(SCHEDULE_PATH, SCHEDULE_CACHE)


def download_remarks_if_needed() -> None:
    if not REMARKS_URL:
        return
    need_download = False
    if not os.path.exists(REMARKS_PATH):
        need_download = True
    else:
        try:
            mtime = os.path.getmtime(REMARKS_PATH)
            age = time_module.time() - mtime
            if age > REMARKS_SYNC_TTL_SEC:
                need_download = True
        except Exception as e:
            log.warning("Не удалось проверить возраст REMARKS_PATH: %s", e)
            need_download = True
    if not need_download:
        return
    try:
        log.info("Скачиваю файл замечаний из REMARKS_URL (авто-синхронизация)...")
        content = download_file_from_url(REMARKS_URL)
        with open(REMARKS_PATH, "wb") as f:
            f.write(content)
        REMARKS_CACHE["mtime"] = None
        REMARKS_CACHE["df"] = None
        log.info("Файл замечаний успешно скачан и сохранён в %s", REMARKS_PATH)
    except Exception as e:
        log.warning("Не удалось скачать файл замечаний из REMARKS_URL: %s", e)


def get_remarks_df() -> Optional[pd.DataFrame]:
    download_remarks_if_needed()
    return load_remarks_cached(REMARKS_PATH, REMARKS_CACHE)


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
        if not ("A" <= ch <= "Z"):
            continue
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_col_by_letter(df: pd.DataFrame, col_letters: str) -> Optional[str]:
    idx = excel_col_to_index(col_letters)
    if 0 <= idx < len(df.columns):
        return df.columns[idx]
    return None


def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    """Добавляет новую строку выезда в лист INSPECTOR_SHEET_NAME файла REMARKS_PATH."""
    if not os.path.exists(REMARKS_PATH):
        log.warning("REMARKS_PATH не найден, некуда сохранять выезд инспектора.")
        return False
    try:
        wb = load_workbook(REMARKS_PATH)
    except Exception as e:
        log.warning("Не удалось открыть REMARKS_PATH для записи инспектора: %s", e)
        return False

    if INSPECTOR_SHEET_NAME in wb.sheetnames:
        ws = wb[INSPECTOR_SHEET_NAME]
    else:
        ws = wb.create_sheet(INSPECTOR_SHEET_NAME)

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
        log.warning("Не удалось сохранить REMARKS_PATH после добавления выезда: %s", e)
        return False

    REMARKS_CACHE["mtime"] = None
    REMARKS_CACHE["df"] = None
    log.info("Инспектор добавил выезд (строка %s) в лист %s", new_row, INSPECTOR_SHEET_NAME)
    return True


# ----------------- БАЗА -----------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """ CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen_at TEXT
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen_at TEXT
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                approver TEXT,
                decision TEXT,
                comment TEXT,
                decided_at TEXT,
                schedule_version INTEGER
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS schedule_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS schedule_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version INTEGER,
                uploaded_by INTEGER,
                uploaded_at TEXT,
                path TEXT
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS remarks_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                excel_row INTEGER,
                pb_status TEXT,
                pbzk_status TEXT,
                ar_status TEXT,
                updated_by INTEGER,
                updated_at TEXT
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                excel_row INTEGER,
                file_id TEXT,
                file_name TEXT,
                uploaded_by INTEGER,
                uploaded_at TEXT
            ) """
    )
    c.execute(
        """ CREATE TABLE IF NOT EXISTS approvers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT UNIQUE
            ) """
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
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_version', '1')"
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


# ----------------- УПРАВЛЕНИЕ АДМИНАМИ (ТОЛЬКО СПРАВОЧНО) -----------------
# Все проверки admin выполняются через HARD_CODED_ADMINS, но команды /admins и т.п. оставлены для удобства.

async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда доступна только администраторам.")
        return
    await update.message.reply_text("Администраторы заданы жёстко в коде:\n• @asdinamitif (398960707)")


# ----------------- КНОПКИ -----------------
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
    params: List[Any] = [version] + approvers
    c.execute(
        f""" SELECT approver, decision, decided_at FROM approvals
              WHERE schedule_version = ? AND approver IN ({placeholders})
              ORDER BY datetime(decided_at) DESC """,
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
        header.append([InlineKeyboardButton("⬇ Скачать", callback_data="schedule_download")])
        header.append([InlineKeyboardButton("Статусы согласования", callback_data="noop")])
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
                InlineKeyboardButton("✅ Устранены", callback_data="remarks_done"),
                InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done"),
            ],
            [
                InlineKeyboardButton("➖ Не требуется", callback_data="remarks_not_required"),
            ],
            [
                InlineKeyboardButton("⬆ Загрузить График выездов", callback_data="remarks_upload"),
                InlineKeyboardButton("⬇ Скачать График выездов", callback_data="remarks_download"),
            ],
        ]
    )


def onzs_menu_inline() -> InlineKeyboardMarkup:
    row1 = [InlineKeyboardButton(str(i), callback_data=f"onzs_{i}") for i in range(1, 7)]
    row2 = [InlineKeyboardButton(str(i), callback_data=f"onzs_{i}") for i in range(7, 13)]
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
                InlineKeyboardButton("Все даты", callback_data=f"onzsperiod:{onzs_num}:all"),
            ],
        ]
    )


# ----------------- КОМАНДЫ -----------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    msg = "Привет! Это бот отдела СОТ.\n"
    if is_admin(user.id):
        msg += "Вы — администратор бота (жёстко задано в коде).\n"
    msg += "Выберите раздел на клавиатуре ниже."
    await update.message.reply_text(msg, reply_markup=main_menu())


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update.message.reply_text(
        f"Ваш id: {user.id}\nusername: @{user.username}"
    )


# ----------------- ОБЩИЙ РОУТЕР ТЕКСТА -----------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    low = text.lower()
    # 0) Шаги мастера «Инспектор»
    if context.user_data.get("inspector_state"):
        await handle_inspector_step(update, context)
        return
    # 1) Ввод кастомного согласующего
    if context.user_data.get("await_custom_approver"):
        await handle_custom_approver_input(update, context)
        return
    # 2) Ввод кастомного периода для ОНзС
    if context.user_data.get("onzs_wait_custom_period"):
        await handle_onzs_custom_period(update, context)
        return
    # 3) Кнопки меню
    if low == "📅 график".lower():
        await handle_menu_schedule(update, context)
        return
    if low == "📊 итоговая".lower():
        await handle_menu_final(update, context)
        return
    if low == "📝 замечания".lower():
        await handle_menu_remarks(update, context)
        return
    if low == "🏗 онзс".lower():
        await handle_menu_onzs(update, context)
        return
    if low == "инспектор":
        await handle_menu_inspector(update, context)
        return
    if low == "📈 аналитика".lower():
        await handle_menu_analytics(update, context)
        return
    # 4) Остальное: комментарий к доработке / пароль аналитики
    await handle_rework_comment(update, context)
    await handle_analytics_password(update, context)


# --------- 📅 ГРАФИК ---------
async def handle_menu_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    admin_flag = is_admin(user.id)
    settings = get_schedule_state()
    text = build_schedule_text(admin_flag, settings)
    await update.message.reply_text(text, reply_markup=build_schedule_inline(admin_flag, settings))


async def schedule_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    if not user:
        return
    if data == "noop":
        return
    if data == "schedule_upload":
        if not is_admin(user.id):
            await query.edit_message_text("Только администратор может загружать файл графика.")
            return
        context.user_data["await_schedule_file"] = True
        await query.edit_message_text("Отправьте Excel (.xlsx) с графиком.", reply_markup=None)
        return
    if data == "schedule_download":
        download_schedule_if_needed()
        if not os.path.exists(SCHEDULE_PATH):
            await query.edit_message_text("Файл графика ещё не загружен.")
            return
        with open(SCHEDULE_PATH, "rb") as f:
            await query.message.reply_document(InputFile(f, filename=os.path.basename(SCHEDULE_PATH)))
        return
    if data == "schedule_add_custom":
        if not is_admin(user.id):
            await query.answer("Только администратор может добавлять согласующих.", show_alert=True)
            return
        context.user_data["await_custom_approver"] = True
        await query.message.reply_text("Отправьте @username согласующего (можно несколько через пробел).")
        return
    if data.startswith("schedule_set_approver:"):
        if not is_admin(user.id):
            await query.answer("Только администратор выбирает согласующих.", show_alert=True)
            return
        _, label = data.split(":", 1)
        settings = get_schedule_state()
        current = get_current_approvers(settings)
        if label in current:
            current.remove(label)
        else:
            current.append(label)
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('current_approvers', ?)",
            (",".join(current),),
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_status', 'pending')"
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_by', '')"
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_at', '')"
        )
        for appr in current:
            c.execute("SELECT user_id FROM users WHERE username = ?", (appr.lstrip("@"),))
            row = c.fetchone()
            if row:
                try:
                    await query.bot.send_message(
                        chat_id=row["user_id"],
                        text=(
                            "У вас на рассмотрении новый график выездов. "
                            "Откройте раздел «📅 График» в боте и примите решение."
                        ),
                    )
                except Exception as e:
                    log.warning("Не удалось отправить уведомление согласующему %s: %s", appr, e)
        conn.commit()
        conn.close()
        settings = get_schedule_state()
        text = build_schedule_text(is_admin(user.id), settings)
        await query.edit_message_text(text, reply_markup=build_schedule_inline(is_admin(user.id), settings))
        return
    if data == "schedule_approve":
        settings = get_schedule_state()
        approvers = get_current_approvers(settings)
        user_at = f"@{user.username}" if user.username else None
        allowed = False
        if is_admin(user.id):
            allowed = True
        if user_at:
            for a in approvers:
                if a.lower() == user_at.lower():
                    allowed = True
                    break
        if approvers and not allowed:
            await query.edit_message_text("Согласовать могут только: " + ", ".join(approvers))
            return
        approver_label = user_at or (approvers[0] if approvers else "")
        version = get_schedule_version(settings)
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """ INSERT INTO approvals (user_id, username, approver, decision, comment, decided_at, schedule_version)
                VALUES (?, ?, ?, ?, ?, ?, ?) """,
            (user.id, user.username or "", approver_label, "approve", "", local_now().isoformat(), version),
        )
        c.execute(
            "SELECT approver, decision FROM approvals WHERE schedule_version = ?",
            (version,),
        )
        all_rows = c.fetchall()
        last_by_approver: Dict[str, sqlite3.Row] = {}
        for r in all_rows:
            a = r["approver"]
            if a not in last_by_approver:
                last_by_approver[a] = r
        total = len(approvers)
        approved_count = 0
        rework_count = 0
        for a in approvers:
            r = last_by_approver.get(a)
            if not r:
                continue
            if r["decision"] == "approve":
                approved_count += 1
            elif r["decision"] == "rework":
                rework_count += 1
        if rework_count > 0:
            status = "rework"
            decided_by = approver_label
        elif approved_count == total and total > 0:
            status = "approved"
            decided_by = "Все согласовали"
        else:
            status = "pending"
            decided_by = ""
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_status', ?)",
            (status,),
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_by', ?)",
            (decided_by,),
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_at', ?)",
            (local_now().isoformat(),),
        )
        if status == "approved":
            c.execute("SELECT user_id FROM admins")
            admins = [r["user_id"] for r in c.fetchall()]
            c.execute("SELECT user_id FROM users")
            others = [r["user_id"] for r in c.fetchall()]
            text_notify = (
                f"График выездов СОТ (версия {version}) полностью согласован всеми согласующими."
            )
            for uid in set(admins + others):
                try:
                    await query.bot.send_message(chat_id=uid, text=text_notify)
                except Exception:
                    pass
        conn.commit()
        conn.close()
        settings = get_schedule_state()
        text = build_schedule_text(is_admin(user.id), settings)
        await query.edit_message_text(text, reply_markup=build_schedule_inline(is_admin(user.id), settings))
        return
    if data == "schedule_rework":
        settings = get_schedule_state()
        approvers = get_current_approvers(settings)
        user_at = f"@{user.username}" if user.username else None
        allowed = False
        if is_admin(user.id):
            allowed = True
        if user_at:
            for a in approvers:
                if a.lower() == user_at.lower():
                    allowed = True
                    break
        if approvers and not allowed:
            await query.edit_message_text("Отправить на доработку могут только: " + ", ".join(approvers))
            return
        context.user_data["await_rework_comment"] = True
        await query.message.reply_text("Напишите причину, по которой график отправляется на доработку.")
        return


async def handle_custom_approver_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        context.user_data["await_custom_approver"] = False
        await update.message.reply_text("Добавлять согласующих может только админ.")
        return
    text = (update.message.text or "").strip()
    context.user_data["await_custom_approver"] = False
    tokens = [t for t in text.replace(",", " ").split() if t]
    new_labels: List[str] = []
    for t in tokens:
        if not t.startswith("@"):
            t = "@" + t
        new_labels.append(t)
    if not new_labels:
        await update.message.reply_text(
            "Не нашёл @username. Отправьте, например: @ivanov или @ivanov @petrov"
        )
        return
    conn = get_db()
    c = conn.cursor()
    for lbl in new_labels:
        c.execute("INSERT OR IGNORE INTO approvers (label) VALUES (?)", (lbl,))
    settings = get_schedule_state()
    current = set(get_current_approvers(settings))
    for lbl in new_labels:
        current.add(lbl)
    current_list = list(current)
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('current_approvers', ?)",
        (",".join(current_list),),
    )
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_status', 'pending')"
    )
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_by', '')"
    )
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_at', '')"
    )
    for lbl in new_labels:
        c.execute("SELECT user_id FROM users WHERE username = ?", (lbl.lstrip("@"),))
        row = c.fetchone()
        if row:
            try:
                await update.get_bot().send_message(
                    chat_id=row["user_id"],
                    text=(
                        "У вас на рассмотрении новый график выездов. "
                        "Откройте раздел «📅 График» в боте и примите решение."
                    ),
                )
            except Exception as e:
                log.warning("Не удалось отправить уведомление согласующему %s: %s", lbl, e)
    conn.commit()
    conn.close()
    settings = get_schedule_state()
    await update.message.reply_text(
        "Согласующие обновлены.", reply_markup=build_schedule_inline(is_admin(user.id), settings)
    )


async def handle_rework_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get("await_rework_comment"):
        return
    user = update.effective_user
    if not user:
        return
    reason = update.message.text.strip()
    context.user_data["await_rework_comment"] = False
    settings = get_schedule_state()
    approvers = get_current_approvers(settings)
    approver_label = f"@{user.username}" if user.username else (approvers[0] if approvers else "")
    version = get_schedule_version(settings)
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """ INSERT INTO approvals (user_id, username, approver, decision, comment, decided_at, schedule_version)
            VALUES (?, ?, ?, ?, ?, ?, ?) """,
        (user.id, user.username or "", approver_label, "rework", reason, local_now().isoformat(), version),
    )
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_status', 'rework')"
    )
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_by', ?)",
        (approver_label,),
    )
    c.execute(
        "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_at', ?)",
        (local_now().isoformat(),),
    )
    c.execute("SELECT user_id FROM admins")
    admins = [r["user_id"] for r in c.fetchall()]
    conn.commit()
    conn.close()
    for uid in admins:
        try:
            await update.get_bot().send_message(
                chat_id=uid,
                text=(
                    f"График выездов СОТ (версия {version}) отправлен на доработку ({approver_label}).\n"
                    f"Причина: {reason}"
                ),
            )
        except Exception:
            pass
    await update.message.reply_text("Решение зафиксировано: график отправлен на доработку.")


# --------- 📊 ИТОГОВАЯ ---------
async def handle_menu_final(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    df = get_schedule_df()
    if df is None:
        await update.message.reply_text("Файл графика ещё не загружен.")
        return
    col_date = find_col(df, ["дата выезда"])
    col_type = find_col(df, ["вид проверки"])
    col_case = find_col(df, ["номер дела"])
    if not col_date or not col_type:
        await update.message.reply_text("Не удалось найти столбцы «Дата выезда» и «Вид проверки» в графике.")
        return
    rows = []
    for idx, row in df.iterrows():
        if idx + 1 < 14:
            continue
        vt = str(row.get(col_type, "")).strip()
        if not vt or vt.lower() == "nan":
            continue
        dt_val = row.get(col_date)
        try:
            if isinstance(dt_val, datetime):
                d = dt_val.date()
            else:
                d = pd.to_datetime(dt_val).date()
        except Exception:
            continue
        case_no = ""
        if col_case:
            case_no = str(row.get(col_case, "")).strip()
        rows.append((d, vt, case_no))
    if not rows:
        await update.message.reply_text("В графике нет строк с видами проверок (начиная с 14-й строки).")
        return
    today = local_now().date()
    upcoming = [r for r in rows if r[0] >= today]
    upcoming.sort(key=lambda x: x[0])
    lines = ["Ближайшие проверки:"]
    for d, vt, case_no in upcoming[:20]:
        date_str = d.strftime("%d.%m.%Y")
        if case_no:
            lines.append(f"• {date_str} — {vt} — дело: {case_no}")
        else:
            lines.append(f"• {date_str} — {vt}")
    await update.message.reply_text("\n".join(lines))


# --------- ЗАГРУЗКА Excel ---------
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.document:
        return
    doc: Document = msg.document
    user = update.effective_user
    if not user:
        return
    if not doc.file_name.lower().endswith(".xlsx"):
        await msg.reply_text("Нужен файл в формате .xlsx")
        return
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
        current_ver = get_schedule_version(settings)
        new_ver = current_ver + 1
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_version', ?)",
            (str(new_ver),),
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_status', 'pending')"
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_by', '')"
        )
        c.execute(
            "INSERT OR REPLACE INTO schedule_settings (key, value) VALUES ('schedule_decided_at', '')"
        )
        c.execute(
            """ INSERT INTO schedule_files (version, uploaded_by, uploaded_at, path)
                VALUES (?, ?, ?, ?) """,
            (new_ver, user.id, local_now().isoformat(), SCHEDULE_PATH),
        )
        conn.commit()
        conn.close()
        settings = get_schedule_state()
        admin_flag = is_admin(user.id)
        text = build_schedule_text(admin_flag, settings)
        await msg.reply_text(
            "Файл графика сохранён и запущен новый цикл согласования.\n" + text,
            reply_markup=build_schedule_inline(admin_flag, settings),
        )
        return
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
            "Рабочий файл замечаний сохранён. Он используется в «Замечаниях» и «ОНзС».",
            reply_markup=main_menu(),
        )
        return


# --------- 📝 ЗАМЕЧАНИЯ ---------
async def handle_menu_remarks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Раздел «Замечания».\n"
        "1) Через «⬆ Загрузить» админ загружает рабочий файл с замечаниями.\n"
        "2) Если настроен REMARKS_URL, бот периодически подтягивает свежий файл из Яндекс.Диска или другого URL.\n"
        "3) Статусы «Устранены» / «Не устранены» / «Не требуется» берутся из столбцов Q, R, Y, AD.\n"
        "4) Через кнопки ниже выводятся списки по этим статусам.",
        reply_markup=remarks_menu_inline(),
    )


async def remarks_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    if data == "remarks_upload":
        if not is_admin(user.id):
            await query.edit_message_text("Только администратор может загружать рабочий файл.")
            return
        context.user_data["await_remarks_file"] = True
        await query.edit_message_text("Отправьте Excel (.xlsx) рабочего файла (с ОНзС и замечаниями).")
        return
    if data == "remarks_download":
        if not os.path.exists(REMARKS_PATH):
            await query.edit_message_text("Рабочий файл ещё не загружен.")
            return
        with open(REMARKS_PATH, "rb") as f:
            await query.message.reply_document(InputFile(f, filename=os.path.basename(REMARKS_PATH)))
        return
    df = get_remarks_df()
    if df is None:
        await query.edit_message_text("Рабочий файл ещё не загружен.")
        return
    col_obj = find_col(df, ["наименование объекта", "объект"])
    col_addr = find_col(df, ["строительный адрес", "адрес"])
    col_onzs = find_col(df, ["онзс"])
    col_date = find_col(df, ["дата выезда"])
    col_pb_count = get_col_by_letter(df, "O")
    col_eom_count = get_col_by_letter(df, "AC")
    col_pb_mark = get_col_by_letter(df, "Q")
    col_pbzk_mark = get_col_by_letter(df, "R")
    col_ar_mark = get_col_by_letter(df, "Y")
    col_eom_mark = get_col_by_letter(df, "AD")
    if data == "remarks_done":
        caption = "Список объектов, где замечания УСТРАНЕНЫ (есть «да» и нет «нет» в Q/R/Y/AD):"
    elif data == "remarks_not_done":
        caption = "Список объектов, где замечания НЕ УСТРАНЕНЫ (есть хотя бы одно «нет» в Q/R/Y/AD):"
    else:
        caption = "Список объектов, где отметки об устранении НЕ ТРЕБУЮТСЯ (Q/R/Y/AD пустые):"
    lines: List[str] = [caption, ""]
    for idx, row in df.iterrows():
        excel_row = int(idx) + 1
        marks_raw: List[str] = []
        for col in (col_pb_mark, col_pbzk_mark, col_ar_mark, col_eom_mark):
            if not col:
                marks_raw.append("")
                continue
            v = str(row.get(col, "")).strip().lower()
            if v in ("да", "нет"):
                marks_raw.append(v)
            elif not v or v == "nan":
                marks_raw.append("")
            else:
                marks_raw.append(v)
        has_yes = any(v == "да" for v in marks_raw)
        has_no = any(v == "нет" for v in marks_raw)
        all_empty = all(not v for v in marks_raw)
        if has_no:
            row_category = "not_done"
        elif has_yes:
            row_category = "done"
        elif all_empty:
            row_category = "not_required"
        else:
            continue
        if data == "remarks_done" and row_category != "done":
            continue
        if data == "remarks_not_done" and row_category != "not_done":
            continue
        if data == "remarks_not_required" and row_category != "not_required":
            continue
        obj = row.get(col_obj, "") if col_obj else ""
        addr = row.get(col_addr, "") if col_addr else ""
        onzs = row.get(col_onzs, "") if col_onzs else ""
        date_str = ""
        if col_date:
            dv = row.get(col_date)
            try:
                if isinstance(dv, datetime):
                    date_str = dv.strftime("%d.%m.%Y")
                elif dv:
                    date_str = pd.to_datetime(dv).strftime("%d.%м.%Y")
            except Exception:
                date_str = str(dv)
        if row_category == "done":
            cat_text = "Устранены"
        elif row_category == "not_done":
            cat_text = "Не устранены"
        else:
            cat_text = "Не требуется"
        line = f"• Строка {excel_row} — статус по документу: {cat_text}"
        if date_str:
            line += f"\n Дата выезда: {date_str}"
        if onzs:
            line += f"\n ОНзС: {onzs}"
        if obj:
            line += f"\n Объект: {obj}"
        if addr:
            line += f"\n Адрес: {addr}"
        pb_mark_val = row.get(col_pb_mark, "") if col_pb_mark else ""
        pbzk_mark_val = row.get(col_pbzk_mark, "") if col_pbzk_mark else ""
        ar_mark_val = row.get(col_ar_mark, "") if col_ar_mark else ""
        eom_mark_val = row.get(col_eom_mark, "") if col_eom_mark else ""
        line += (
            f"\n Статусы (из Q/R/Y/AD): "
            f"ПБ={pb_mark_val or '-'}; "
            f"ПБ в ЗК КНД={pbzk_mark_val or '-'}; "
            f"АР/ММГН/АГО={ar_mark_val or '-'}; "
            f"ЭОМ={eom_mark_val or '-'}"
        )
        if col_pb_count:
            line += f"\n Кол-во нарушений ПБ: {row.get(col_pb_count, '') or '-'}"
        if col_eom_count:
            line += f"\n Кол-во нарушений ЭОМ: {row.get(col_eom_count, '') or '-'}"
        lines.append(line)
        lines.append("")
        if len("\n".join(lines)) > 3500:
            break
    if len(lines) == 2:
        lines.append("По текущему файлу таких строк нет.")
    await query.edit_message_text("\n".join(lines))


# --------- 🏗 ОНЗС ---------
def user_can_edit_row(user, inspector_text: str) -> bool:
    if is_admin(user.id):
        return True
    if not user.username:
        return False
    uname = user.username.lower()
    low = (inspector_text or "").lower()
    for key, usernames in RESPONSIBLE_USERNAMES.items():
        if key in low and uname in [u.lower() for u in usernames]:
            return True
    return False


async def handle_menu_onzs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    df = get_remarks_df()
    if df is None:
        await update.message.reply_text("Рабочий файл ещё не загружен.")
        return
    await update.message.reply_text("Выберите номер ОНзС:", reply_markup=onzs_menu_inline())


async def onzs_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith("onzs_"):
        return
    onzs_num = data.split("_", 1)[1]
    context.user_data["onzs_selected"] = onzs_num
    await query.edit_message_text(f"ОНзС {onzs_num}. Выберите период:", reply_markup=onzs_period_inline(onzs_num))


async def onzs_period_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    try:
        _, onzs_num, mode = data.split(":")
    except ValueError:
        return
    user = query.from_user
    chat_id = query.message.chat_id
    today = local_now().date()
    if mode == "all":
        date_from = None
        date_to = None
    elif mode.isdigit():
        days = int(mode)
        date_from = today - timedelta(days=days)
        date_to = None
    elif mode == "custom":
        context.user_data["onzs_wait_custom_period"] = onzs_num
        await query.message.reply_text(
            f"Введите период для ОНзС {onzs_num} в формате "
            f"ДД.ММ.ГГГГ-ДД.ММ.ГГГГ (например 01.01.2025-31.01.2025)."
        )
        return
    else:
        return
    await query.message.reply_text(f"Показываю объекты по ОНзС {onzs_num} за выбранный период...")
    await send_onzs_list(context.bot, chat_id, user, onzs_num, date_from, date_to)


async def handle_onzs_custom_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    onzs_num = context.user_data.get("onzs_wait_custom_period")
    if not onzs_num:
        return
    text = (update.message.text or "").strip()
    context.user_data["onzs_wait_custom_period"] = None
    try:
        s = text.replace("—", "-")
        p1, p2 = [p.strip() for p in s.split("-", 1)]
        d1 = datetime.strptime(p1, "%d.%m.%Y").date()
        d2 = datetime.strptime(p2, "%d.%m.%Y").date()
        if d2 < d1:
            d1, d2 = d2, d1
    except Exception:
        await update.message.reply_text(
            "Не понял формат. Нужен вид ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, например 01.01.2025-31.01.2025."
        )
        return
    await update.message.reply_text(
        f"Показываю объекты по ОНзС {onzs_num} за период "
        f"{d1.strftime('%d.%m.%Y')}–{d2.strftime('%d.%m.%Y')}..."
    )
    await send_onzs_list(
        update.get_bot(),
        update.effective_chat.id,
        update.effective_user,
        onzs_num,
        d1,
        d2,
    )


async def send_onzs_list(bot, chat_id: int, user, onzs_num: str, date_from: Optional[date], date_to: Optional[date]) -> None:
    df = get_remarks_df()
    if df is None:
        await bot.send_message(chat_id=chat_id, text="Рабочий файл ещё не загружен.")
        return
    col_onzs = find_col(df, ["онзс"])
    if not col_onzs:
        await bot.send_message(chat_id=chat_id, text="Не найден столбец ОНзС в рабочем файле.")
        return
    col_date = find_col(df, ["дата выезда"])
    col_area = find_col(df, ["площад", "этаж"])
    col_dev = find_col(df, ["застройщик"])
    col_obj = find_col(df, ["наименование объекта", "объект"])
    col_addr = find_col(df, ["строительный адрес", "адрес"])
    col_case = find_col(df, ["номер дела"])
    col_type = find_col(df, ["вид проверки"])
    col_inspector = find_col(df, ["должностное лицо", "упкисот"])
    col_pb_count = get_col_by_letter(df, "O")
    col_pb_rr = get_col_by_letter(df, "P")
    col_pb_mark = get_col_by_letter(df, "Q")
    col_pbzk_mark = get_col_by_letter(df, "R")
    col_pb_file = get_col_by_letter(df, "S")
    col_pb_act = get_col_by_letter(df, "T")
    col_pb_note = get_col_by_letter(df, "U")
    col_ar_count = get_col_by_letter(df, "V")
    col_mmgn_count = get_col_by_letter(df, "W")
    col_ago_count = get_col_by_letter(df, "X")
    col_ar_mark = get_col_by_letter(df, "Y")
    col_ar_file = get_col_by_letter(df, "Z")
    col_ar_act = get_col_by_letter(df, "AA")
    col_ar_note = get_col_by_letter(df, "AB")
    col_eom_count = get_col_by_letter(df, "AC")
    col_eom_mark = get_col_by_letter(df, "AD")
    col_eom_file = get_col_by_letter(df, "AE")
    col_eom_act = get_col_by_letter(df, "AF")
    col_eom_note = get_col_by_letter(df, "AG")
    col_common_note = get_col_by_letter(df, "AH")
    col_zos = get_col_by_letter(df, "AI")

    def norm_onzs(v):
        if pd.isna(v):
            return ""
        if isinstance(v, (int, float)):
            if float(v).is_integer():
                return str(int(v))
            return str(v)
        return str(v).strip()

    norm_series = df[col_onzs].apply(norm_onzs)
    subset = df[norm_series == str(onzs_num)]
    if subset.empty:
        await bot.send_message(chat_id=chat_id, text=f"По ОНзС {onzs_num} данных нет.")
        return

    conn = get_db()
    c = conn.cursor()
    sent_any = False
    for idx, row in subset.iterrows():
        excel_row = int(idx) + 1
        d_val = None
        date_str = ""
        if col_date:
            dv = row.get(col_date)
            try:
                if isinstance(dv, datetime):
                    d_val = dv.date()
                elif dv:
                    d_val = pd.to_datetime(dv).date()
            except Exception:
                d_val = None
            if d_val:
                date_str = d_val.strftime("%d.%m.%Y")
        if date_from and (not d_val or d_val < date_from):
            continue
        if date_to and (not d_val or d_val > date_to):
            continue
        inspector_text = str(row.get(col_inspector, "")) if col_inspector else ""
        text_lines = [f"ОНзС: {onzs_num}"]
        if date_str:
            text_lines.append(f"Дата выезда: {date_str}")
        if col_area:
            text_lines.append(f"Площадь / этажность: {row.get(col_area, '')}")
        if col_dev:
            text_lines.append(f"Застройщик: {row.get(col_dev, '')}")
        if col_obj:
            text_lines.append(f"Объект: {row.get(col_obj, '')}")
        if col_addr:
            text_lines.append(f"Адрес: {row.get(col_addr, '')}")
        if col_case:
            text_lines.append(f"Номер дела: {row.get(col_case, '')}")
        if col_type:
            text_lines.append(f"Вид проверки: {row.get(col_type, '')}")
        if col_inspector:
            text_lines.append(f"Должностное лицо: {inspector_text}")
        text_lines.append("")
        text_lines.append("Статусы устранения:")
        text_lines.append("Пожарная безопасность:")
        pb_cnt = row.get(col_pb_count, "") if col_pb_count else ""
        pb_rr = row.get(col_pb_rr, "") if col_pb_rr else ""
        pb_mark_val = row.get(col_pb_mark, "") if col_pb_mark else ""
        pbzk_mark_val = row.get(col_pbzk_mark, "") if col_pbzk_mark else ""
        pb_file = row.get(col_pb_file, "") if col_pb_file else ""
        pb_act = row.get(col_pb_act, "") if col_pb_act else ""
        pb_note = row.get(col_pb_note, "") if col_pb_note else ""
        text_lines.append(f"• Кол-во нарушений ПБ: {pb_cnt or '-'}")
        text_lines.append(f"• РР (нужен/не нужен): {pb_rr or '-'}")
        text_lines.append(f"• Отметка об устранении замечаний ПБ: {pb_mark_val or '-'}")
        text_lines.append(f"• Отметка об устранении замечаний ПБ в ЗК КНД: {pbzk_mark_val or '-'}")
        text_lines.append(f"• Ссылка на файл с замечаниями ПБ: {pb_file or '-'}")
        text_lines.append(f"• Ссылка на акт об устранении ПБ: {pb_act or '-'}")
        text_lines.append(f"• Примечание ПБ: {pb_note or '-'}")
        text_lines.append("")
        text_lines.append("Архитектура, доступ инвалидов, архитектурный облик:")
        ar_cnt = row.get(col_ar_count, "") if col_ar_count else ""
        mmgn_cnt = row.get(col_mmgn_count, "") if col_mmgn_count else ""
        ago_cnt = row.get(col_ago_count, "") if col_ago_count else ""
        ar_mark_val = row.get(col_ar_mark, "") if col_ar_mark else ""
        ar_file_val = row.get(col_ar_file, "") if col_ar_file else ""
        ar_act_val = row.get(col_ar_act, "") if col_ar_act else ""
        ar_note_val = row.get(col_ar_note, "") if col_ar_note else ""
        text_lines.append(f"• Кол-во нарушений АР: {ar_cnt or '-'}")
        text_lines.append(f"• Кол-во нарушений ММГН: {mmgn_cnt or '-'}")
        text_lines.append(f"• Кол-во нарушений АГО: {ago_cnt or '-'}")
        text_lines.append(f"• Отметка об устранении нарушений АР/ММГН/АГО: {ar_mark_val or '-'}")
        text_lines.append(f"• Ссылка на файл с замечаниями АР/ММГН/АГО: {ar_file_val or '-'}")
        text_lines.append(f"• Ссылка на акт об устранении АР/ММГН/АГО: {ar_act_val or '-'}")
        text_lines.append(f"• Примечание АР/ММГН/АГО: {ar_note_val or '-'}")
        text_lines.append("")
        text_lines.append("Электроснабжение:")
        eom_cnt = row.get(col_eom_count, "") if col_eom_count else ""
        eom_mark_val = row.get(col_eom_mark, "") if col_eom_mark else ""
        eom_file_val = row.get(col_eom_file, "") if col_eom_file else ""
        eom_act_val = row.get(col_eom_act, "") if col_eom_act else ""
        eom_note_val = row.get(col_eom_note, "") if col_eom_note else ""
        text_lines.append(f"• Кол-во нарушений ЭОМ: {eom_cnt or '-'}")
        text_lines.append(f"• Отметка об устранении нарушений ЭОМ: {eom_mark_val or '-'}")
        text_lines.append(f"• Ссылка на файл с замечаниями ЭОМ: {eom_file_val or '-'}")
        text_lines.append(f"• Ссылка на акт об устранении ЭОМ: {eom_act_val or '-'}")
        text_lines.append(f"• Примечание ЭОМ: {eom_note_val or '-'}")
        common_note_val = row.get(col_common_note, "") if col_common_note else ""
        zos_val = row.get(col_zos, "") if col_zos else ""
        if common_note_val or zos_val:
            text_lines.append("")
            if common_note_val:
                text_lines.append(f"Общие примечания: {common_note_val}")
            if zos_val:
                text_lines.append(f"ЗОС: {zos_val}")
        can_edit = user_can_edit_row(user, inspector_text)
        if not can_edit:
            text_lines.append("")
            text_lines.append(
                "Изменять статусы по этой строке могут только администратор "
                "или закреплённые за объектом исполнители."
            )
            await bot.send_message(chat_id=chat_id, text="\n".join(text_lines))
            sent_any = True
            continue
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✔ ПБ", callback_data=f"note_pb_yes_{excel_row}"),
                    InlineKeyboardButton("✖ ПБ", callback_data=f"note_pb_no_{excel_row}"),
                ],
                [
                    InlineKeyboardButton("✔ ПБ ЗК КНД", callback_data=f"note_pbzk_yes_{excel_row}"),
                    InlineKeyboardButton("✖ ПБ ЗК КНД", callback_data=f"note_pbzk_no_{excel_row}"),
                ],
                [
                    InlineKeyboardButton("✔ АР/ММГН/АГО", callback_data=f"note_ar_yes_{excel_row}"),
                    InlineKeyboardButton("✖ АР/ММГН/АГО", callback_data=f"note_ar_no_{excel_row}"),
                ],
                [
                    InlineKeyboardButton("📎 Прикрепить файл", callback_data=f"attach_{excel_row}"),
                ],
            ]
        )
        await bot.send_message(chat_id=chat_id, text="\n".join(text_lines), reply_markup=kb)
        sent_any = True
    conn.close()
    if not sent_any:
        await bot.send_message(chat_id=chat_id, text=f"По ОНзС {onzs_num} в выбранном периоде данных нет.")


async def notes_status_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    if not user:
        return
    if data.startswith("attach_"):
        _, row_str = data.split("_", 1)
        context.user_data["await_attachment_row"] = int(row_str)
        await query.message.reply_text(f"Пришлите файл (документ или фото), который нужно привязать к строке {row_str}.")
        return
    if not data.startswith("note_"):
        return
    _, kind, yn, row_str = data.split("_")
    excel_row = int(row_str)
    df = get_remarks_df()
    if df is None:
        await query.message.reply_text("Рабочий файл не найден.")
        return
    col_inspector = find_col(df, ["должностное лицо", "упкисот"])
    inspector_text = ""
    if col_inspector and excel_row - 1 < len(df):
        inspector_text = str(df.iloc[excel_row - 1].get(col_inspector, ""))
    if not user_can_edit_row(user, inspector_text):
        await query.message.reply_text("У вас нет прав изменять статусы по этой строке.")
        return
    status_value = "да" if yn == "yes" else "нет"
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """ SELECT pb_status, pbzk_status, ar_status FROM remarks_status
            WHERE excel_row = ? ORDER BY id DESC LIMIT 1 """,
        (excel_row,),
    )
    prev = c.fetchone()
    pb = prev["pb_status"] if prev else None
    pbzk = prev["pbzk_status"] if prev else None
    ar = prev["ar_status"] if prev else None
    if kind == "pb":
        pb = status_value
    elif kind == "pbzk":
        pbzk = status_value
    else:
        ar = status_value
    c.execute(
        """ INSERT INTO remarks_status (excel_row, pb_status, pbzk_status, ar_status, updated_by, updated_at)
            VALUES (?, ?, ?, ?, ?, ?) """,
        (excel_row, pb, pbzk, ar, user.id, local_now().isoformat()),
    )
    conn.commit()
    conn.close()
    await query.message.reply_text("Статус замечаний обновлён.")


async def attachment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg:
        return
    excel_row = context.user_data.get("await_attachment_row")
    if not excel_row:
        return
    file_obj = None
    if msg.document:
        file_obj = msg.document
    elif msg.photo:
        file_obj = msg.photo[-1]
    else:
        await msg.reply_text("Пришлите документ или фото.")
        return
    file = await file_obj.get_file()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """ INSERT INTO attachments (excel_row, file_id, file_name, uploaded_by, uploaded_at)
            VALUES (?, ?, ?, ?, ?) """,
        (
            excel_row,
            file.file_id,
            getattr(file, "file_path", "") or "",
            msg.from_user.id if msg.from_user else None,
            local_now().isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    context.user_data["await_attachment_row"] = None
    await msg.reply_text("Файл прикреплён к объекту.")


# --------- 👷 ИНСПЕКТОР ---------
async def handle_menu_inspector(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("➕ Добавить выезд", callback_data="insp_add_trip")]]
    )
    await update.message.reply_text(
        "Раздел «Инспектор».\nНажмите «➕ Добавить выезд», чтобы по шагам "
        "заполнить новую строку выезда.",
        reply_markup=kb,
    )


async def inspector_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "insp_add_trip":
        context.user_data["inspector_state"] = {
            "step": "date",
            "form": {},
        }
        await query.message.reply_text("Дата выезда (ДД.ММ.ГГГГ):")
        return


async def handle_inspector_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = context.user_data.get("inspector_state") or {}
    step = state.get("step")
    form = state.get("form", {})
    text = (update.message.text or "").strip()
    if step == "date":
        try:
            d = datetime.strptime(text, "%d.%m.%Y").date()
        except Exception:
            await update.message.reply_text(
                "Не понял дату. Введите в формате ДД.ММ.ГГГГ, например 03.12.2025."
            )
            return
        form["date"] = d
        state["step"] = "area"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("Площадь (кв.м):")
        return
    if step == "area":
        form["area"] = text
        state["step"] = "floors"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("Количество этажей:")
        return
    if step == "floors":
        form["floors"] = text
        state["step"] = "onzs"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("ОНзС (1–12):")
        return
    if step == "onzs":
        form["onzs"] = text
        state["step"] = "developer"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("Наименование застройщика:")
        return
    if step == "developer":
        form["developer"] = text
        state["step"] = "object"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("Наименование объекта:")
        return
    if step == "object":
        form["object"] = text
        state["step"] = "address"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("Строительный адрес:")
        return
    if step == "address":
        form["address"] = text
        state["step"] = "case_no"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text("Номер дела (формат 00-00-000000):")
        return
    if step == "case_no":
        form["case_no"] = text
        state["step"] = "check_type"
        state["form"] = form
        context.user_data["inspector_state"] = state
        await update.message.reply_text(
            "Вид проверки (ПП, итоговая, профвизит, запрос ОНзС, поручение руководства):"
        )
        return
    if step == "check_type":
        form["check_type"] = text
        ok = append_inspector_row_to_excel(form)
        context.user_data["inspector_state"] = None
        if ok:
            await update.message.reply_text(
                "Выезд сохранён в лист "
                f"«{INSPECTOR_SHEET_NAME}» файла с замечаниями.",
                reply_markup=main_menu(),
            )
        else:
            await update.message.reply_text(
                "Не удалось сохранить выезд в Excel. "
                "Сообщите администратору или проверьте файл.",
                reply_markup=main_menu(),
            )
        return
    context.user_data["inspector_state"] = None
    await update.message.reply_text(
        "Произошла ошибка в мастере «Инспектор». Попробуйте начать заново.",
        reply_markup=main_menu(),
    )


# --------- 📈 АНАЛИТИКА ---------
async def handle_menu_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["await_analytics_password"] = True
    await update.message.reply_text("Введите пароль для входа в раздел «Аналитика»:")
    

async def handle_analytics_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get("await_analytics_password"):
        return
    pwd = update.message.text.strip()
    if pwd != ANALYTICS_PASSWORD:
        context.user_data["await_analytics_password"] = False
        await update.message.reply_text("Неверный пароль.")
        return
    context.user_data["await_analytics_password"] = False
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT decision, COUNT(*) AS c FROM approvals GROUP BY decision")
    appr = {row["decision"]: row["c"] for row in c.fetchall()}
    c.execute(
        """ SELECT COUNT(*) AS c FROM remarks_status
            WHERE pb_status='нет' OR pbzk_status='нет' OR ar_status='нет' """
    )
    not_done = c.fetchone()["c"]
    c.execute(
        """ SELECT COUNT(*) AS c FROM remarks_status
            WHERE pb_status='да' OR pbzk_status='да' OR ar_status='да' """
    )
    done = c.fetchone()["c"]
    c.execute(
        """ SELECT approver, decision, COUNT(*) AS c FROM approvals
            GROUP BY approver, decision """
    )
    rows = c.fetchall()
    c.execute(
        """ SELECT schedule_version, approver, decision, comment, decided_at
            FROM approvals ORDER BY datetime(decided_at) DESC LIMIT 10 """
    )
    hist = c.fetchall()
    conn.close()
    lines = ["📈 Аналитика:", ""]
    lines.append("1️⃣ Согласование графика (общее количество решений):")
    lines.append(f" • Согласовано: {appr.get('approve', 0)}")
    lines.append(f" • На доработку: {appr.get('rework', 0)}")
    lines.append("")
    lines.append("2️⃣ Замечания (по вручную изменённым статусам в боте):")
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
    lines.append("4️⃣ История согласований графика (последние 10 решений):")
    if hist:
        for r in hist:
            ver = r["schedule_version"] if r["schedule_version"] is not None else "-"
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


# ----------------- MAIN -----------------
def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в переменных окружения или .env")
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()

    # Команды
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("id", id_cmd))
    application.add_handler(CommandHandler("admins", cmd_admins))

    # Меню (клавиатура)
    application.add_handler(MessageHandler(filters.Regex("^📅 График$"), handle_menu_schedule))
    application.add_handler(MessageHandler(filters.Regex("^📊 Итоговая$"), handle_menu_final))
    application.add_handler(MessageHandler(filters.Regex("^📝 Замечания$"), handle_menu_remarks))
    application.add_handler(MessageHandler(filters.Regex("^🏗 ОНзС$"), handle_menu_onzs))
    application.add_handler(MessageHandler(filters.Regex("^Инспектор$"), handle_menu_inspector))
    application.add_handler(MessageHandler(filters.Regex("^📈 Аналитика$"), handle_menu_analytics))

    # Callback-кнопки
    application.add_handler(CallbackQueryHandler(schedule_cb, pattern="^schedule_"))
    application.add_handler(CallbackQueryHandler(remarks_cb, pattern="^remarks_"))
    application.add_handler(CallbackQueryHandler(onzs_cb, pattern="^onzs_"))
    application.add_handler(CallbackQueryHandler(onzs_period_cb, pattern="^onzsperiod:"))
    application.add_handler(CallbackQueryHandler(notes_status_cb, pattern="^(note_|attach_)"))
    application.add_handler(CallbackQueryHandler(inspector_cb, pattern="^insp_"))

    # Документы (Excel)
    application.add_handler(MessageHandler(filters.Document.ALL, document_handler))

    # Прикреплённые файлы к ОНзС
    application.add_handler(MessageHandler((filters.Document.ALL | filters.PHOTO), attachment_handler))

    # Прочий текст
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    log.info("Бот стартует в режиме polling...")
    application.run_polling()


if __name__ == "__main__":
    main()

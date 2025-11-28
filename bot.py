import logging
import os
import sqlite3
from datetime import datetime, timedelta, time, date
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

# 2-й файл: для 📝 Замечания и 🏗 ОНзС
REMARKS_PATH = os.getenv("REMARKS_PATH", "График выездов отдела СОТ.xlsx")

# URL для скачивания Excel с замечаниями (Яндекс.Диск)
REMARKS_URL = os.getenv("REMARKS_URL", "").strip()

# TTL авто-синхронизации (сек)
REMARKS_SYNC_TTL_SEC = int(os.getenv("REMARKS_SYNC_TTL_SEC", "3600"))

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))  # МСК: +3

ANALYTICS_PASSWORD = "051995"

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# Дефолтный список возможных согласующих
DEFAULT_APPROVERS = [
    "@asdinamitif",
    "@FrolovAlNGSN",
    "@cappit_G59",
    "@sergeybektiashkin",
    "@scri4",
    "@Kirill_Victorovi4",
]

# Лист для мастера «Инспектор»
INSPECTOR_SHEET_NAME = os.getenv(
    "INSPECTOR_SHEET_NAME", "ПБ, АР,ММГН, АГО (2025)"
)

# Для назначения прав в «Замечаниях»
RESPONSIBLE_USERNAMES = {
    "бектяшкин": ["sergeybektiashkin"],
    "смирнов": ["scri4"],
}

# Кэш для Excel
SCHEDULE_CACHE: Dict[str, Any] = {"mtime": None, "df": None}
REMARKS_CACHE: Dict[str, Any] = {"mtime": None, "df": None}


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


# ----------------- РАБОТА С EXCEL -----------------
def load_excel_cached(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Загрузка Excel для раздела «📅 График» (только 1 лист).
    С кэшированием по mtime.
    """
    if not os.path.exists(path):
        return None

    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]

    log.info("Загружаю Excel (График): %s", path)

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

    log.info("График загружен: %s строк, %s столбцов", df.shape[0], df.shape[1])

    return df


def load_remarks_cached(path: str, cache: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Загрузка файла для:
    • 📝 Замечания
    • 🏗 ОНзС
    Читаются ВСЕ листы (2023/2024/2025).
    """
    if not os.path.exists(path):
        return None

    mtime = os.path.getmtime(path)
    if cache["mtime"] == mtime and cache["df"] is not None:
        return cache["df"]

    log.info("Загружаю REMARKS (все листы): %s", path)

    xls = pd.ExcelFile(path)
    frames = []

    for sheet in xls.sheet_names:
        try:
            raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        except Exception:
            continue

        header_row = 0
        for i in range(min(30, len(raw))):
            row = raw.iloc[i].astype(str).tolist()
            if any("дата выезда" in c.lower() for c in row):
                header_row = i
                break

        try:
            df_sheet = pd.read_excel(xls, sheet_name=sheet, header=header_row)
        except Exception:
            continue

        df_sheet = df_sheet.dropna(how="all").reset_index(drop=True)
        df_sheet["_sheet"] = sheet
        frames.append(df_sheet)

    if not frames:
        return None

    df_all = pd.concat(frames, ignore_index=True)

    cache["mtime"] = mtime
    cache["df"] = df_all

    log.info("REMARKS загружен: %s строк, %s столбцов", df_all.shape[0], df_all.shape[1])

    return df_all


def download_remarks_if_needed() -> None:
    """
    Авто-синхронизация REMARKS_PATH с REMARKS_URL.
    Если файл:
    — отсутствует
    — устарел (mtime > TTL)
    → скачиваем с Яндекс.Диска (публичная ссылка)
    """
    if not REMARKS_URL:
        return

    need = False

    if not os.path.exists(REMARKS_PATH):
        need = True
    else:
        try:
            mtime = os.path.getmtime(REMARKS_PATH)
            age = time_module.time() - mtime
            if age > REMARKS_SYNC_TTL_SEC:
                need = True
        except Exception:
            need = True

    if not need:
        return

    try:
        log.info("Скачиваю REMARKS из Яндекс.Диска…")
        resp = requests.get(REMARKS_URL, timeout=30)
        resp.raise_for_status()

        with open(REMARKS_PATH, "wb") as f:
            f.write(resp.content)

        REMARKS_CACHE["mtime"] = None
        REMARKS_CACHE["df"] = None
        log.info("REMARKS обновлён.")

    except Exception as e:
        log.warning("Ошибка загрузки REMARKS из URL: %s", e)


def download_remarks_force() -> bool:
    """Принудительное обновление (по кнопке «Обновить из Яндекс.Диска»)."""
    if not REMARKS_URL:
        return False

    try:
        log.info("Принудительная загрузка REMARKS…")
        resp = requests.get(REMARKS_URL, timeout=30)
        resp.raise_for_status()

        with open(REMARKS_PATH, "wb") as f:
            f.write(resp.content)

        REMARKS_CACHE["mtime"] = None
        REMARKS_CACHE["df"] = None
        return True

    except Exception as e:
        log.warning("Ошибка принудительной загрузки: %s", e)
        return False


def get_schedule_df() -> Optional[pd.DataFrame]:
    return load_excel_cached(SCHEDULE_PATH, SCHEDULE_CACHE)


def get_remarks_df() -> Optional[pd.DataFrame]:
    download_remarks_if_needed()
    return load_remarks_cached(REMARKS_PATH, REMARKS_CACHE)


# ----------------- ПОИСК КОЛОНОК В EXCEL -----------------
def find_col(df: pd.DataFrame, hints) -> Optional[str]:
    """
    Поиск колонки по частичному совпадению.
    hints: строка или список строк.
    """
    if isinstance(hints, str):
        hints = [hints]

    hints = [h.lower() for h in hints]

    for col in df.columns:
        low = str(col).lower()
        if any(h in low for h in hints):
            return col

    return None


# -------- Excel: "AC" → индекс --------
def excel_col_to_index(col: str) -> int:
    """
    Перевод буквенного номера столбца Excel (AC, AI, O и т.п.)
    в индекс (0-based).
    """
    col = col.upper().strip()
    idx = 0
    for ch in col:
        if "A" <= ch <= "Z":
            idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_col_by_letter(df: pd.DataFrame, col_letters: str) -> Optional[str]:
    """
    Возвращает имя столбца по буквам Excel (например "O", "AC", "AI").
    Если индекс выходит за пределы — вернёт None.
    """
    idx = excel_col_to_index(col_letters)
    if 0 <= idx < len(df.columns):
        return df.columns[idx]
    return None


# ----------------- МОДУЛЬ «ИНСПЕКТОР»: запись строки -----------------
def append_inspector_row_to_excel(form: Dict[str, Any]) -> bool:
    """
    Добавляет новую строку (выезд инспектора) в лист INSPECTOR_SHEET_NAME
    файла REMARKS_PATH.

    Колонки заполняются так:

      B — Дата выезда
      D — «Площадь / Этажность» в одном поле
      E — ОНзС
      F — Наименование застройщика
      G — Наименование объекта
      H — Строительный адрес
      I — Номер дела
      J — Вид проверки
    """

    if not os.path.exists(REMARKS_PATH):
        log.warning("REMARKS_PATH отсутствует, нельзя записать выезд инспектора.")
        return False

    try:
        wb = load_workbook(REMARKS_PATH)
    except Exception as e:
        log.warning("Ошибка открытия REMARKS_PATH для записи инспектора: %s", e)
        return False

    if INSPECTOR_SHEET_NAME in wb.sheetnames:
        ws = wb[INSPECTOR_SHEET_NAME]
    else:
        ws = wb.create_sheet(INSPECTOR_SHEET_NAME)

    # Находим последнюю заполненную строку по столбцу B (дата)
    last_data = 1
    max_row = ws.max_row or 1
    for r in range(1, max_row + 1):
        if ws.cell(row=r, column=2).value not in (None, ""):
            last_data = r

    new_row = last_data + 1

    # ---- B: Дата ----
    date_obj = form.get("date")
    if isinstance(date_obj, datetime):
        date_str = date_obj.strftime("%d.%m.%Y")
    elif isinstance(date_obj, date):
        date_str = date_obj.strftime("%d.%m.%Y")
    else:
        date_str = str(date_obj or "")

    ws.cell(row=new_row, column=2).value = date_str

    # ---- D: Площадь + Этажность ----
    area = form.get("area") or ""
    floors = form.get("floors") or ""
    ws.cell(row=new_row, column=4).value = (
        f"Площадь (кв.м): {area}\nКоличество этажей: {floors}"
    )

    # ---- E: ОНзС ----
    ws.cell(row=new_row, column=5).value = form.get("onzs") or ""

    # ---- F: Застройщик ----
    ws.cell(row=new_row, column=6).value = form.get("developer") or ""

    # ---- G: Объект ----
    ws.cell(row=new_row, column=7).value = form.get("object") or ""

    # ---- H: Адрес ----
    ws.cell(row=new_row, column=8).value = form.get("address") or ""

    # ---- I: Номер дела ----
    ws.cell(row=new_row, column=9).value = form.get("case_no") or ""

    # ---- J: Вид проверки ----
    ws.cell(row=new_row, column=10).value = form.get("check_type") or ""

    # ---- Сохраняем ----
    try:
        wb.save(REMARKS_PATH)
    except Exception as e:
        log.warning("Ошибка сохранения REMARKS_PATH после записи инспектора: %s", e)
        return False

    # Сброс кэша (чтобы новый выезд появился в ОНзС / Замечаниях)
    REMARKS_CACHE["mtime"] = None
    REMARKS_CACHE["df"] = None

    log.info("Инспектор: добавлена строка %s в лист %s", new_row, INSPECTOR_SHEET_NAME)
    return True


# ----------------- ПРОСТЫЕ ФУНКЦИИ ДЛЯ БД -----------------
def init_db() -> None:
    """
    Создаёт SQLite-базу и все необходимые таблицы,
    чтобы бот не падал при чтении согласований, аналитики и администраторов.
    """

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Администраторы
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        );
        """
    )

    # Метаданные файла 📅 Графика
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schedule_meta (
            id INTEGER PRIMARY KEY,
            current_rev INTEGER NOT NULL,
            file_name TEXT,
            uploaded_at TEXT,
            approvers TEXT
        );
        """
    )

    # История согласований графика
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schedule_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            schedule_rev INTEGER NOT NULL,
            username TEXT NOT NULL,
            approved_at TEXT NOT NULL
        );
        """
    )

    # История загрузок файлов 📝 Замечаний
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS remarks_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            approvers TEXT,
            approved_by TEXT,
            status TEXT
        );
        """
    )

    # Если ADMIN_ID задан – добавим в таблицу admins
    if ADMIN_ID != 0:
        # username мы не знаем, поэтому можно хранить как специальную запись,
        # но для простоты пока не добавляем – админ будет управлять через /add_admin
        pass

    conn.commit()
    conn.close()
    log.info("База данных инициализирована.")


def get_admins() -> List[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT username FROM admins ORDER BY username;")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def add_admin(username: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO admins (username) VALUES (?);",
        (username,),
    )
    conn.commit()
    conn.close()


def del_admin(username: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM admins WHERE username = ?;",
        (username,),
    )
    conn.commit()
    conn.close()


def is_super_admin(update: Update) -> bool:
    """
    Допуск к командам управления администраторами.
    Или по chat_id (ADMIN_ID), или по таблице admins.
    """
    user = update.effective_user
    if not user:
        return False

    if ADMIN_ID and user.id == ADMIN_ID:
        return True

    username = (user.username or "").lower()
    if not username:
        return False

    admins = [a.lower() for a in get_admins()]
    return username in admins


# ----------------- ОБРАБОТЧИКИ КОМАНД -----------------
MAIN_MENU_KEYBOARD = [
    ["📅 График", "📝 Замечания"],
    ["🏗 ОНзС", "📈 Аналитика"],
    ["👮‍♂️ Инспектор"],
]


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = ReplyKeyboardMarkup(
        keyboard=MAIN_MENU_KEYBOARD,
        resize_keyboard=True,
    )
    text = (
        "Добро пожаловать в бота отдела СОТ.\n\n"
        "Выберите раздел на клавиатуре ниже."
    )
    await update.message.reply_text(text, reply_markup=kb)


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    await update.message.reply_text(
        f"Ваш user_id: {user.id}\nchat_id: {chat.id}"
    )


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_super_admin(update):
        await update.message.reply_text("Недостаточно прав для просмотра администраторов.")
        return

    admins = get_admins()
    if not admins:
        await update.message.reply_text("Список администраторов пуст.")
        return

    text = "Текущие администраторы:\n" + "\n".join(f"• {a}" for a in admins)
    await update.message.reply_text(text)


async def cmd_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_super_admin(update):
        await update.message.reply_text("Недостаточно прав для добавления администратора.")
        return

    if not context.args:
        await update.message.reply_text("Укажи username, например: /add_admin @user")
        return

    username = context.args[0].strip()
    if username.startswith("@"):
        username = username[1:]

    if not username:
        await update.message.reply_text("Username не распознан.")
        return

    add_admin(username)
    await update.message.reply_text(f"Администратор @{username} добавлен.")


async def cmd_del_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_super_admin(update):
        await update.message.reply_text("Недостаточно прав для удаления администратора.")
        return

    if not context.args:
        await update.message.reply_text("Укажи username, например: /del_admin @user")
        return

    username = context.args[0].strip()
    if username.startswith("@"):
        username = username[1:]

    if not username:
        await update.message.reply_text("Username не распознан.")
        return

    del_admin(username)
    await update.message.reply_text(f"Администратор @{username} удалён.")


# ----------------- CALLBACK'И (УПРОЩЁННЫЕ) -----------------
async def schedule_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик всех callback'ов 'schedule_*' (упрощённый)."""
    query = update.callback_query
    await query.answer()

    df = get_schedule_df()
    if df is None:
        await query.edit_message_text("Файл графика не найден или не читается.")
        return

    # Простой пример: показать первые 5 строк с датами
    text_lines = ["Первые 5 выездов из графика:"]
    head = df.head(5)
    date_col = find_col(head, ["дата выезда", "дата"])
    obj_col = find_col(head, ["объект", "наименование объекта"])

    for _, row in head.iterrows():
        dt = row.get(date_col, "")
        obj = row.get(obj_col, "")
        text_lines.append(f"• {dt} — {obj}")

    await query.edit_message_text("\n".join(text_lines))


async def remarks_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик 'remarks_*' (упрощённый просмотр)."""
    query = update.callback_query
    await query.answer()

    df = get_remarks_df()
    if df is None:
        await query.edit_message_text("Рабочий файл с замечаниями не найден или не читается.")
        return

    text_lines = ["Рабочий файл загружен.", f"Всего строк: {len(df)}"]
    await query.edit_message_text("\n".join(text_lines))


async def onzs_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик номера ОНзС: onzs_1, onzs_2, ... (пока просто подтверждение)."""
    query = update.callback_query
    await query.answer()

    data = query.data  # onzs_X
    onzs_num = data.split("_", 1)[-1]

    await query.edit_message_text(f"Выбрана категория ОНзС №{onzs_num}.\nФильтрация по таблице пока упрощена.")


async def onzs_period_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик выбора периода (упрощённый)."""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text("Выбор периода ОНзС пока реализован в базовом виде. Детальная фильтрация не настроена.")


async def notes_status_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик статусов ПБ/АР и вложений (упрощённый)."""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text("Обновление статуса/вложений пока реализовано только в базовом виде.")


# ----------------- МАСТЕР «ИНСПЕКТОР» -----------------
INSPECTOR_STEPS = ["date", "area", "floors", "onzs", "developer", "object", "address", "case_no", "check_type"]

INSPECTOR_PROMPTS = {
    "date": "Введите дату выезда (в формате ДД.ММ.ГГГГ):",
    "area": "Введите площадь объекта (кв.м):",
    "floors": "Введите количество этажей:",
    "onzs": "Введите номер ОНзС (1–12):",
    "developer": "Введите наименование застройщика:",
    "object": "Введите наименование объекта:",
    "address": "Введите строительный адрес:",
    "case_no": "Введите номер дела (формат 00-00-000000):",
    "check_type": "Введите вид проверки (ПП, итоговая, профвизит, запрос ОНзС, поручение руководства):",
}


async def inspector_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback для insp_* (мастер инспектора)."""
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "insp_add_trip":
        # Запуск мастера
        context.user_data["insp_form"] = {}
        context.user_data["insp_step"] = "date"

        await query.edit_message_text(
            "Мастер добавления выезда инспектора.\n\n" + INSPECTOR_PROMPTS["date"]
        )
    else:
        await query.edit_message_text("Неизвестное действие инспектора.")


def build_inspector_menu() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("➕ Добавить выезд", callback_data="insp_add_trip")],
    ]
    return InlineKeyboardMarkup(kb)


# ----------------- ОБРАБОТЧИК ДОКУМЕНТОВ/ФОТО -----------------
async def attachment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик фото / файлов, которые пользователь отправляет.
    Пока только подтверждаем получение.
    """
    message = update.effective_message
    await message.reply_text("Файл/фото получен. Логика прикрепления к строкам пока упрощена.")


async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Отдельный обработчик документов (например, Excel).
    В упрощённой версии просто отвечаем, что файл получен.
    """
    doc: Document = update.message.document
    await update.message.reply_text(f"Получен файл: {doc.file_name}")


# ----------------- РОУТЕР ТЕКСТА -----------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Маршрутизатор обычного текста:
    - меню
    - шаги мастера «Инспектор»
    """
    text = (update.message.text or "").strip()

    # Проверка: мастер инспектора в процессе?
    if "insp_step" in context.user_data:
        step = context.user_data.get("insp_step")
        form = context.user_data.get("insp_form", {})

        # Сохраняем ответ
        if step == "date":
            # пробуем распарсить дату
            try:
                dt = datetime.strptime(text, "%d.%m.%Y").date()
                form["date"] = dt
            except Exception:
                await update.message.reply_text("Не удалось распознать дату. Введите в формате ДД.ММ.ГГГГ.")
                return

        elif step == "area":
            form["area"] = text

        elif step == "floors":
            form["floors"] = text

        elif step == "onzs":
            form["onzs"] = text

        elif step == "developer":
            form["developer"] = text

        elif step == "object":
            form["object"] = text

        elif step == "address":
            form["address"] = text

        elif step == "case_no":
            form["case_no"] = text

        elif step == "check_type":
            form["check_type"] = text

        context.user_data["insp_form"] = form

        # Переход к следующему шагу
        current_index = INSPECTOR_STEPS.index(step)
        if current_index + 1 < len(INSPECTOR_STEPS):
            next_step = INSPECTOR_STEPS[current_index + 1]
            context.user_data["insp_step"] = next_step
            await update.message.reply_text(INSPECTOR_PROMPTS[next_step])
            return
        else:
            # Завершаем мастер, пишем в Excel
            ok = append_inspector_row_to_excel(form)
            context.user_data.pop("insp_step", None)
            context.user_data.pop("insp_form", None)

            if ok:
                await update.message.reply_text(
                    "Запись успешно добавлена в лист "
                    f"«{INSPECTOR_SHEET_NAME}» файла REMARKS_PATH."
                )
            else:
                await update.message.reply_text(
                    "Не удалось записать выезд инспектора в файл. "
                    "Проверьте доступность REMARKS_PATH на сервере."
                )
            return

    # --- Меню ---
    if text == "📅 График":
        df = get_schedule_df()
        if df is None:
            await update.message.reply_text("Файл графика не найден или не читается.")
            return

        head = df.head(5)
        date_col = find_col(head, ["дата выезда", "дата"])
        obj_col = find_col(head, ["объект", "наименование объекта"])

        lines = ["Первые 5 выездов:", ""]
        for _, row in head.iterrows():
            dt = row.get(date_col, "")
            obj = row.get(obj_col, "")
            lines.append(f"• {dt} — {obj}")

        await update.message.reply_text("\n".join(lines))
        return

    if text == "📝 Замечания":
        df = get_remarks_df()
        if df is None:
            await update.message.reply_text("Рабочий файл с замечаниями не найден или не читается.")
            return

        await update.message.reply_text(
            f"Файл с замечаниями загружен.\nВсего строк: {len(df)}"
        )
        return

    if text == "🏗 ОНзС":
        # Простая клавиатура с номерами 1–12
        kb = [
            [
                InlineKeyboardButton(str(i), callback_data=f"onzs_{i}")
                for i in range(1, 7)
            ],
            [
                InlineKeyboardButton(str(i), callback_data=f"onzs_{i}")
                for i in range(7, 13)
            ],
        ]
        markup = InlineKeyboardMarkup(kb)
        await update.message.reply_text("Выберите номер ОНзС:", reply_markup=markup)
        return

    if text == "📈 Аналитика":
        await update.message.reply_text("Раздел 📈 Аналитика будет доработан отдельно.")
        return

    if text == "👮‍♂️ Инспектор":
        markup = build_inspector_menu()
        await update.message.reply_text(
            "Раздел «Инспектор».\nНажмите «➕ Добавить выезд» для запуска мастера.",
            reply_markup=markup,
        )
        return

    # По умолчанию
    await update.message.reply_text(
        "Команда не распознана. Используйте меню или /start."
    )


# ----------------- ОБРАБОТЧИК ОШИБОК -----------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Ошибка при обработке апдейта:", exc_info=context.error)
    # Аккуратно уведомляем пользователя, если возможно
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла внутренняя ошибка бота. Сообщите администратору."
            )
    except Exception:
        # вторичная ошибка нас уже не интересует
        pass


# ----------------- MAIN -----------------
def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в переменных окружения или .env")

    log.info("Запускаю бота отдела СОТ...")
    # Инициализируем БД (администраторы, настройки и т.п.)
    init_db()

    application = Application.builder().token(BOT_TOKEN).build()

    # --- Команды ---
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("id", id_cmd))
    application.add_handler(CommandHandler("admins", cmd_admins))
    application.add_handler(CommandHandler("add_admin", cmd_add_admin))
    application.add_handler(CommandHandler("del_admin", cmd_del_admin))

    # --- CallbackQuery (inline-кнопки) ---

    # 📅 График – все callback_data, начинающиеся с "schedule_"
    application.add_handler(
        CallbackQueryHandler(schedule_cb, pattern=r"^schedule_")
    )

    # 📝 Замечания – "remarks_*"
    application.add_handler(
        CallbackQueryHandler(remarks_cb, pattern=r"^remarks_")
    )

    # 🏗 ОНзС – выбор номера (onzs_1, onzs_2, ...)
    application.add_handler(
        CallbackQueryHandler(onzs_cb, pattern=r"^onzs_[0-9]+$")
    )

    # 🏗 ОНзС – выбор периода (onzsperiod:...)
    application.add_handler(
        CallbackQueryHandler(onzs_period_cb, pattern=r"^onzsperiod:")
    )

    # Статусы ПБ/ПБ ЗК КНД/АР/… и прикрепление файлов: note_* и attach_*
    application.add_handler(
        CallbackQueryHandler(notes_status_cb, pattern=r"^(note_|attach_)")
    )

    # Инспектор – мастер добавления выезда (insp_add_trip и др. в будущем)
    application.add_handler(
        CallbackQueryHandler(inspector_cb, pattern=r"^insp_")
    )

    # --- Документы / фото ---

    # Сначала обработчик прикреплений к строкам (📎 Прикрепить файл)
    application.add_handler(
        MessageHandler(
            filters.PHOTO | filters.Document.ALL,
            attachment_handler,
        )
    )

    # Затем – загрузка Excel-файлов (график / рабочий файл)
    application.add_handler(
        MessageHandler(
            filters.Document.ALL,
            document_handler,
        )
    )

    # --- Обычный текст (кнопки меню и пошаговые мастера) ---
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router,
        )
    )

    # --- Ошибки ---
    application.add_error_handler(error_handler)

    # Запуск long polling
    application.run_polling()


if __name__ == "__main__":
    main()

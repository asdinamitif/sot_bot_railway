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
        except:
            continue

        header_row = 0
        for i in range(min(30, len(raw))):
            row = raw.iloc[i].astype(str).tolist()
            if any("дата выезда" in c.lower() for c in row):
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
        except:
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

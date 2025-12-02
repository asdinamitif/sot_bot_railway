# ============================================
#   SOT BOT — FULL MONOLITH VERSION (PART 1)
#   Google Sheets + Google Drive + Telegram
#   All-in-one bot.py
# ============================================

import os
import logging
import sqlite3
import mimetypes
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List

import requests
import pandas as pd

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# Google API imports
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials

# --------------------------------------------
#               LOGGING
# --------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

# --------------------------------------------
#               CONFIG
# --------------------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ANALYTICS_PASSWORD = "051995"

# Spreadsheet ID (нужно заменить на ваш реальный ID)
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"

# Названия листов
SHEET_REMARKS = "ПБ, АР,ММГН, АГО (2025)"
SHEET_INSPECTOR = "ПБ, АР,ММГН, АГО (2025)"
SHEET_SCHEDULE = "График"

# Столбцы статусов (буквы)
COL_PB_STATUS = "Q"
COL_PBZK_STATUS = "R"
COL_AR_STATUS = "Y"
COL_EOM_STATUS = "AD"

# --------------------------------------------
#       GOOGLE API — ИНИЦИАЛИЗАЦИЯ
# --------------------------------------------

GOOGLE_CREDS_FILE = "credentials.json"

if not os.path.exists(GOOGLE_CREDS_FILE):
    raise SystemExit("credentials.json не найден. Загрузите его в проект!")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_file(
    GOOGLE_CREDS_FILE,
    scopes=SCOPES
)

# Клиенты Sheets и Drive
sheets_api = build("sheets", "v4", credentials=credentials)
drive_api = build("drive", "v3", credentials=credentials)


# --------------------------------------------
#       ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ GOOGLE API
# --------------------------------------------

def sheet_get(range_name: str):
    """Прочитать диапазон из Google Sheets"""
    try:
        result = sheets_api.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        return result.get("values", [])
    except Exception as e:
        log.error(f"Ошибка чтения Google Sheets: {e}")
        return []


def sheet_update(range_name: str, values: List[List[Any]]):
    """Обновить участок таблицы"""
    try:
        sheets_api.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()
    except Exception as e:
        log.error(f"Ошибка записи в Google Sheets: {e}")


def sheet_append(sheet_name: str, row: List[Any]):
    """Добавить новую строку в конец листа"""
    try:
        sheets_api.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A:Z",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]}
        ).execute()
    except Exception as e:
        log.error(f"Ошибка append в Google Sheets: {e}")


# --------------------------------------------
#       GOOGLE DRIVE — загрузка файлов
# --------------------------------------------

def create_drive_folder(name: str, parent_id: Optional[str] = None) -> str:
    """Создать папку в Google Drive"""
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder"
    }
    if parent_id:
        file_metadata["parents"] = [parent_id]

    folder = drive_api.files().create(body=file_metadata, fields="id").execute()
    return folder["id"]


def upload_to_drive(local_path: str, drive_folder_id: str) -> str:
    """Загрузить файл в Google Drive в нужную папку и вернуть публичную ссылку"""
    file_name = os.path.basename(local_path)
    mime_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"

    file_metadata = {
        "name": file_name,
        "parents": [drive_folder_id]
    }

    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)

    file = drive_api.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    # Делаем файл публичным
    drive_api.permissions().create(
        fileId=file["id"],
        body={"role": "reader", "type": "anyone"},
    ).execute()

    # Возвращаем ссылку
    return f"https://drive.google.com/uc?id={file['id']}&export=download"
# ============================================
#   PART 2 — SQLITE, ИСТОРИЯ, УТИЛИТЫ
# ============================================

DB_PATH = "sot_bot.db"


# --------------------------------------------
#           ИНИЦИАЛИЗАЦИЯ БД
# --------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # Пользователи
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_seen TEXT
        )
    """)

    # История изменений статусов по замечаниям
    c.execute("""
        CREATE TABLE IF NOT EXISTS remarks_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            excel_row INTEGER,
            pb_status TEXT,
            pbzk_status TEXT,
            ar_status TEXT,
            eom_status TEXT,
            updated_by_id INTEGER,
            updated_by_username TEXT,
            updated_at TEXT
        )
    """)

    # Файлы, прикреплённые к строкам
    c.execute("""
        CREATE TABLE IF NOT EXISTS attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            excel_row INTEGER,
            drive_url TEXT,
            file_name TEXT,
            uploaded_by INTEGER,
            uploaded_at TEXT
        )
    """)

    # Согласования графика (если понадобится)
    c.execute("""
        CREATE TABLE IF NOT EXISTS schedule_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            approver TEXT,
            decision TEXT,
            comment TEXT,
            decided_at TEXT,
            version INTEGER
        )
    """)

    conn.commit()
    conn.close()


# --------------------------------------------
#   ОБНОВЛЕНИЕ СТАТУСОВ В SQLite + Google Sheets
# --------------------------------------------

def record_status_change(
    row_number: int,
    pb: Optional[str],
    pbzk: Optional[str],
    ar: Optional[str],
    eom: Optional[str],
    user
):
    """Сохранить изменение статусов в историю SQLite"""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO remarks_history
        (excel_row, pb_status, pbzk_status, ar_status, eom_status,
         updated_by_id, updated_by_username, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row_number, pb, pbzk, ar, eom,
        user.id, user.username or "",
        datetime.utcnow().isoformat()
    ))
    conn.commit()
    conn.close()


# --------------------------------------------
#     ПОЛУЧЕНИЕ ВСЕХ ДАННЫХ ИЗ ЛИСТА ОНЗС
# --------------------------------------------

def load_sheet_data(sheet_name: str) -> List[List[str]]:
    """Загружает полный лист Google Sheets в виде массива строк"""
    try:
        result = sheets_api.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A:AZ"
        ).execute()
        return result.get("values", [])
    except Exception as e:
        log.error(f"Ошибка загрузки листа {sheet_name}: {e}")
        return []


# --------------------------------------------
#     ОБНОВЛЕНИЕ ОТДЕЛЬНОЙ ЯЧЕЙКИ ПО НОМЕРУ РЯДА
# --------------------------------------------

def update_status_cell(sheet_name: str, row: int, column_letter: str, value: str):
    """
    Обновляет ячейку вида COLUMN + ROW, например Q25.
    row — номер строки в Google Sheets (1-based!)
    """
    cell = f"{sheet_name}!{column_letter}{row}"
    sheet_update(cell, [[value]])


# --------------------------------------------
#     ПОИСК НУЖНОЙ СТРОКИ ПО НАЗВАНИЮ ОБЪЕКТА/АДРЕСУ/ОНЗС
# --------------------------------------------

def find_rows_by_onzs(onzs_number: str, sheet_name: str) -> List[int]:
    """
    Возвращает список строк (номера 1-based) таблицы, где столбец ОНЗС == onzs_number.
    Предположение: столбец ОНЗС находится в колонке D (4-й столбец).
    """
    data = load_sheet_data(sheet_name)
    result = []

    for i, row in enumerate(data, start=1):
        if len(row) >= 4:
            if str(row[3]).strip() == str(onzs_number):
                result.append(i)

    return result


# --------------------------------------------
#     СОЗДАНИЕ ПАПКИ ДЛЯ ФАЙЛОВ ОНЗС В GOOGLE DRIVE
# --------------------------------------------

def ensure_drive_folder_for_onzs(onzs: str, row_num: int) -> str:
    """
    Создаёт структуру:
    /ONZS/
        /<номер>/
            /row_<row_num>/
    Возвращает ID конечной папки.
    """
    # 1. Найти или создать корневую папку /ONZS
    query = "name = 'ONZS' and mimeType = 'application/vnd.google-apps.folder'"
    result = drive_api.files().list(q=query, fields="files(id, name)").execute()
    if result["files"]:
        root_id = result["files"][0]["id"]
    else:
        root_id = create_drive_folder("ONZS")

    # 2. Папка конкретного ОНЗС
    query = f"name = '{onzs}' and '{root_id}' in parents"
    result = drive_api.files().list(q=query, fields="files(id)").execute()
    if result["files"]:
        onzs_folder = result["files"][0]["id"]
    else:
        onzs_folder = create_drive_folder(onzs, parent_id=root_id)

    # 3. Папка для строки
    row_folder_name = f"row_{row_num}"
    query = f"name = '{row_folder_name}' and '{onzs_folder}' in parents"
    result = drive_api.files().list(q=query, fields="files(id)").execute()
    if result["files"]:
        row_folder = result["files"][0]["id"]
    else:
        row_folder = create_drive_folder(row_folder_name, parent_id=onzs_folder)

    return row_folder


# --------------------------------------------
#     СОХРАНЕНИЕ ФАЙЛА В БД ПО ПРОЙДЕННОМУ ОНЗС
# --------------------------------------------

def save_file_record(row_number: int, file_url: str, file_name: str, user):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO attachments (excel_row, drive_url, file_name, uploaded_by, uploaded_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        row_number,
        file_url,
        file_name,
        user.id,
        datetime.utcnow().isoformat()
    ))
    conn.commit()
    conn.close()
# ============================================
#       PART 3 — MAIN MENU & ROUTER
# ============================================

TIMEZONE_OFFSET = 3  # МСК

def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


# --------------------------------------------
#          ГЛАВНОЕ МЕНЮ ТЕЛЕГРАМ
# --------------------------------------------

def main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📅 График", "📊 Итоговая"],
        ["📝 Замечания", "🏗 ОНзС"],
        ["👷 Инспектор", "📈 Аналитика"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# --------------------------------------------
#            /start
# --------------------------------------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # записываем пользователя в БД
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT 1 FROM users WHERE user_id=?", (user.id,))
    if not c.fetchone():
        c.execute(
            "INSERT INTO users (user_id, username, first_seen) VALUES (?, ?, ?)",
            (user.id, user.username or "", datetime.utcnow().isoformat())
        )
        conn.commit()
    conn.close()

    await update.message.reply_text(
        "Привет! Я рабочий бот отдела СОТ.\nВыберите раздел:",
        reply_markup=main_menu()
    )


# --------------------------------------------
#           /id
# --------------------------------------------

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"Ваш ID: {user.id}\nВаш username: @{user.username}"
    )


# --------------------------------------------
#         РАСПОЗНАВАНИЕ ТЕКСТА
# --------------------------------------------

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Роутер текстовых сообщений (FSM блоки в других частях)"""
    text = (update.message.text or "").strip().lower()

    # Если активна FSM «Инспектор»
    if context.user_data.get("inspector_state"):
        await inspector_fsm(update, context)
        return

    # --------------------------------------------------
    # Простой роутинг по меню
    # --------------------------------------------------

    if text == "📅 график".lower():
        await handle_schedule(update, context)
        return

    if text == "📊 итоговая".lower():
        await handle_final(update, context)
        return

    if text == "📝 замечания".lower():
        await handle_remarks_menu(update, context)
        return

    if text == "🏗 онзс".lower():
        await handle_onzs_menu(update, context)
        return

    if text == "👷 инспектор".lower():
        await handle_inspector_start(update, context)
        return

    if text == "📈 аналитика".lower():
        await handle_analytics(update, context)
        return

    # Если текст не относится к меню — игнорируем
    await update.message.reply_text("Выберите действие из меню.", reply_markup=main_menu())
# ============================================
#     PART 4 — 📅 ГРАФИК и 📊 ИТОГОВАЯ
# ============================================

def parse_date_safe(val: Any) -> Optional[date]:
    """Пробует распарсить дату из ячейки Google Sheets"""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Часто Google хранит дату в формате ДД.ММ.ГГГГ
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # Попробуем через pandas
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None


def get_schedule_rows() -> List[Dict[str, Any]]:
    """
    Загружает лист 'График' и возвращает список словарей:
    {
      "row": номер_строки,
      "date": date | None,
      "onzs": str,
      "dev": str,
      "obj": str,
      "addr": str,
      "case": str,
      "type": str,
      "inspector": str,
    }
    Предполагается структура:
    A: № п/п
    B: Дата выезда
    C: Площадь. Этажность
    D: ОНзС
    E: Наименование застройщика
    F: Наименование объекта
    G: Строительный адрес
    H: Номер дела
    I: Вид проверки
    J: Должностное лицо УПКиСОТ, осуществляющее выезд
    """
    data = load_sheet_data(SHEET_SCHEDULE)
    if not data or len(data) < 2:
        return []

    header = data[0]
    rows = []

    for idx, row in enumerate(data[1:], start=2):  # строки с 2-й
        # защитные проверки длины
        while len(row) < 10:
            row.append("")

        dt = parse_date_safe(row[1])  # B
        onzs = str(row[3]).strip()    # D
        dev = str(row[4]).strip()     # E
        obj = str(row[5]).strip()     # F
        addr = str(row[6]).strip()    # G
        case_no = str(row[7]).strip() # H
        vt = str(row[8]).strip()      # I
        inspector = str(row[9]).strip()  # J

        rows.append({
            "row": idx,
            "date": dt,
            "onzs": onzs,
            "dev": dev,
            "obj": obj,
            "addr": addr,
            "case": case_no,
            "type": vt,
            "inspector": inspector,
        })
    return rows


# --------------------------------------------
#         📅 ГРАФИК — ОБЩИЙ РАЗДЕЛ
# --------------------------------------------

async def handle_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    all_rows = get_schedule_rows()
    if not all_rows:
        await update.message.reply_text(
            "Лист «График» пуст или не найден в Google Sheets.",
            reply_markup=main_menu()
        )
        return

    today = local_now().date()

    upcoming = [r for r in all_rows if r["date"] and r["date"] >= today]
    upcoming.sort(key=lambda x: x["date"] or date(2100, 1, 1))

    lines = ["📅 График выездов (по данным Google Sheets):", ""]

    # Покажем ближайшие 10 любых проверок
    for r in upcoming[:10]:
        d = r["date"].strftime("%d.%m.%Y") if r["date"] else "-"
        vt = r["type"] or "-"
        case_no = r["case"] or "-"
        onzs = r["onzs"] or "-"
        base_line = f"• {d} — {vt}"
        if case_no and case_no != "-":
            base_line += f" — дело: {case_no}"
        if onzs and onzs != "-":
            base_line += f" — ОНзС: {onzs}"
        lines.append(base_line)

    if len(upcoming) == 0:
        lines.append("Ближайших выездов в графике не найдено.")

    await update.message.reply_text("\n".join(lines), reply_markup=main_menu())


# --------------------------------------------
#         📊 ИТОГОВАЯ — ТОЛЬКО ИТОГОВЫЕ
# --------------------------------------------

async def handle_final(update: Update, context: ContextTypes.DEFAULT_TYPE):
    all_rows = get_schedule_rows()
    if not all_rows:
        await update.message.reply_text(
            "Лист «График» пуст или не найден в Google Sheets.",
            reply_markup=main_menu()
        )
        return

    today = local_now().date()

    # Фильтруем только итоговые
    filtered = []
    for r in all_rows:
        if not r["date"]:
            continue
        if r["date"] < today:
            continue
        vt = (r["type"] or "").lower()
        if "итог" in vt:  # «итоговая», «итоговая проверка» и т.п.
            filtered.append(r)

    filtered.sort(key=lambda x: x["date"])

    lines = ["📊 Ближайшие ИТОГОВЫЕ проверки:", ""]

    if not filtered:
        lines.append("Нет предстоящих итоговых проверок.")
    else:
        for r in filtered[:20]:
            d = r["date"].strftime("%d.%m.%Y") if r["date"] else "-"
            vt = r["type"] or "-"
            case_no = r["case"] or "-"
            onzs = r["onzs"] or "-"
            line = f"• {d} — {vt}"
            if case_no:
                line += f" — дело: {case_no}"
            if onzs:
                line += f" — ОНзС: {onzs}"
            lines.append(line)

    await update.message.reply_text("\n".join(lines), reply_markup=main_menu())
# ============================================
#     PART 5 — 📝 ЗАМЕЧАНИЯ (СТАТУСЫ)
# ============================================

def excel_col_to_index(col: str) -> int:
    """Преобразует букву столбца (например 'Q') в 0-based индекс."""
    col = col.upper().strip()
    idx = 0
    for ch in col:
        if 'A' <= ch <= 'Z':
            idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1  # A -> 0, B -> 1, ...


def load_remarks_raw() -> List[List[str]]:
    """Сырой лист REMARKS из Google Sheets (все строки, A:AZ)."""
    return load_sheet_data(SHEET_REMARKS)


def build_remarks_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Устранены", callback_data="remarks_done"),
                InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done"),
            ],
            [
                InlineKeyboardButton("➖ Не требуется", callback_data="remarks_not_required"),
            ],
        ]
    )


async def handle_remarks_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вход в раздел «Замечания»."""
    await update.message.reply_text(
        "Раздел «📝 Замечания».\n"
        "Данные берутся из листа Google Sheets "
        f"«{SHEET_REMARKS}».\n"
        "Выберите категорию:",
        reply_markup=build_remarks_keyboard()
    )


async def remarks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка inline-кнопок в разделе «Замечания»."""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data not in ("remarks_done", "remarks_not_done", "remarks_not_required"):
        return

    raw = load_remarks_raw()
    if not raw or len(raw) < 2:
        await query.edit_message_text(
            "Рабочий лист замечаний пуст или не найден в Google Sheets."
        )
        return

    header = raw[0]
    rows = raw[1:]  # данные с 2-й строки

    # Индексы нужных столбцов
    idx_date = excel_col_to_index("B")   # Дата выезда
    idx_onzs = excel_col_to_index("D")   # ОНзС
    idx_obj = excel_col_to_index("F")    # Наименование объекта
    idx_addr = excel_col_to_index("G")   # Строительный адрес

    idx_pb_count = excel_col_to_index("O")   # Кол-во нарушений ПБ
    idx_eom_count = excel_col_to_index("AC") # Кол-во нарушений ЭОМ

    idx_pb_mark = excel_col_to_index("Q")    # Отметка об устранении ПБ
    idx_pbzk_mark = excel_col_to_index("R")  # Отметка об устранении ПБ в ЗК КНД
    idx_ar_mark = excel_col_to_index("Y")    # Отметка об устранении АР/ММГН/АГО
    idx_eom_mark = excel_col_to_index("AD")  # Отметка об устранении ЭОМ

    # Заголовок
    if data == "remarks_done":
        caption = "Список объектов, где замечания УСТРАНЕНЫ (есть «да» и нет «нет» в Q/R/Y/AD):"
        target_category = "done"
    elif data == "remarks_not_done":
        caption = "Список объектов, где замечания НЕ УСТРАНЕНЫ (есть хотя бы одно «нет» в Q/R/Y/AD):"
        target_category = "not_done"
    else:
        caption = "Список объектов, где отметки об устранении НЕ ТРЕБУЮТСЯ (все Q/R/Y/AD пустые):"
        target_category = "not_required"

    lines: List[str] = [caption, ""]

    # Обход строк
    for i, row in enumerate(rows, start=2):  # i — реальный номер строки в таблице
        # защитное расширение строки до нужного числа колонок
        while len(row) <= idx_eom_mark:
            row.append("")

        def get(idx: int) -> str:
            if idx < len(row):
                return str(row[idx]).strip()
            return ""

        # Сырые статусы
        raw_vals = [
            get(idx_pb_mark).lower(),
            get(idx_pbzk_mark).lower(),
            get(idx_ar_mark).lower(),
            get(idx_eom_mark).lower(),
        ]

        norm_vals = []
        for v in raw_vals:
            if v in ("да", "нет"):
                norm_vals.append(v)
            elif v in ("nan", ""):
                norm_vals.append("")
            else:
                norm_vals.append(v)

        has_yes = any(v == "да" for v in norm_vals)
        has_no = any(v == "нет" for v in norm_vals)
        all_empty = all(v == "" for v in norm_vals)

        if has_no:
            category = "not_done"
        elif has_yes:
            category = "done"
        elif all_empty:
            category = "not_required"
        else:
            # странная комбинация — пропускаем
            continue

        if category != target_category:
            continue

        # Собираем карточку по строке
        date_str = ""
        dv = get(idx_date)
        d_parsed = parse_date_safe(dv)
        if d_parsed:
            date_str = d_parsed.strftime("%d.%m.%Y")

        onzs = get(idx_onzs)
        obj = get(idx_obj)
        addr = get(idx_addr)

        pb_mark = get(idx_pb_mark) or "-"
        pbzk_mark = get(idx_pbzk_mark) or "-"
        ar_mark = get(idx_ar_mark) or "-"
        eom_mark = get(idx_eom_mark) or "-"

        pb_count = get(idx_pb_count) or "-"
        eom_count = get(idx_eom_count) or "-"

        if category == "done":
            cat_text = "Устранены"
        elif category == "not_done":
            cat_text = "Не устранены"
        else:
            cat_text = "Не требуется"

        line = f"• Строка {i} — статус по документу: {cat_text}"
        if date_str:
            line += f"\n  Дата выезда: {date_str}"
        if onzs:
            line += f"\n  ОНзС: {onzs}"
        if obj:
            line += f"\n  Объект: {obj}"
        if addr:
            line += f"\n  Адрес: {addr}"

        line += (
            f"\n  Статусы (Q/R/Y/AD): "
            f"ПБ={pb_mark}; ПБ в ЗК КНД={pbzk_mark}; "
            f"АР/ММГН/АГО={ar_mark}; ЭОМ={eom_mark}"
        )
        line += f"\n  Кол-во нарушений ПБ: {pb_count}"
        line += f"\n  Кол-во нарушений ЭОМ: {eom_count}"

        lines.append(line)
        lines.append("")

        # Ограничение по длине сообщения Telegram
        if len("\n".join(lines)) > 3500:
            break

    if len(lines) == 2:
        lines.append("По текущему файлу таких строк нет.")

    await query.edit_message_text("\n".join(lines))
# ============================================
#   PART 6 — 🏗 ОНзС + СТАТУСЫ + ФАЙЛЫ (DRIVE)
# ============================================

def build_onzs_keyboard() -> InlineKeyboardMarkup:
    row1 = [InlineKeyboardButton(str(i), callback_data=f"onzs_select_{i}") for i in range(1, 7)]
    row2 = [InlineKeyboardButton(str(i), callback_data=f"onzs_select_{i}") for i in range(7, 13)]
    return InlineKeyboardMarkup([row1, row2])


def build_onzs_period_keyboard(onzs: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🗓 За 30 дней", callback_data=f"onzs_period_{onzs}_30"),
                InlineKeyboardButton("🗓 За 90 дней", callback_data=f"onzs_period_{onzs}_90"),
            ],
            [
                InlineKeyboardButton("📅 Ввести даты", callback_data=f"onzs_period_{onzs}_custom"),
                InlineKeyboardButton("Все даты", callback_data=f"onzs_period_{onzs}_all"),
            ],
        ]
    )


async def handle_onzs_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вход в раздел «🏗 ОНзС» — выбор номера."""
    await update.message.reply_text(
        "Раздел «🏗 ОНзС».\nВыберите номер ОНзС:",
        reply_markup=build_onzs_keyboard()
    )


async def onzs_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора номера ОНзС (1–12)."""
    query = update.callback_query
    await query.answer()
    data = query.data  # onzs_select_X

    try:
        _, _, num_str = data.split("_", 2)
    except ValueError:
        return

    context.user_data["onzs_selected"] = num_str
    await query.edit_message_text(
        f"ОНзС {num_str}. Выберите период:",
        reply_markup=build_onzs_period_keyboard(num_str)
    )


async def onzs_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора периода для ОНзС."""
    query = update.callback_query
    await query.answer()
    data = query.data  # onzs_period_<onzs>_<mode>

    try:
        _, _, onzs_str, mode = data.split("_", 3)
    except ValueError:
        return

    today = local_now().date()
    date_from = None
    date_to = None

    if mode == "30":
        date_from = today - timedelta(days=30)
    elif mode == "90":
        date_from = today - timedelta(days=90)
    elif mode == "all":
        date_from = None
        date_to = None
    elif mode == "custom":
        # попросим ввести период
        context.user_data["onzs_wait_custom_period"] = onzs_str
        await query.message.reply_text(
            f"Введите период для ОНзС {onzs_str} в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ\n"
            f"Например: 01.01.2025-31.01.2025"
        )
        return
    else:
        return

    await query.message.reply_text(
        f"Показываю объекты по ОНзС {onzs_str} за выбранный период..."
    )
    await send_onzs_list(
        bot=query.bot,
        chat_id=query.message.chat_id,
        user=query.from_user,
        onzs_num=onzs_str,
        date_from=date_from,
        date_to=date_to
    )


async def onzs_custom_period_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Парсинг текста периода для ОНзС (когда выбран 'custom')."""
    onzs_str = context.user_data.get("onzs_wait_custom_period")
    if not onzs_str:
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
            "Не понял формат. Нужен вид ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.\n"
            "Например: 01.01.2025-31.01.2025"
        )
        return

    await update.message.reply_text(
        f"Показываю объекты по ОНзС {onzs_str} за период "
        f"{d1.strftime('%d.%m.%Y')}–{d2.strftime('%d.%m.%Y')}..."
    )

    await send_onzs_list(
        bot=update.get_bot(),
        chat_id=update.effective_chat.id,
        user=update.effective_user,
        onzs_num=onzs_str,
        date_from=d1,
        date_to=d2
    )


async def send_onzs_list(
    bot,
    chat_id: int,
    user,
    onzs_num: str,
    date_from: Optional[date],
    date_to: Optional[date]
):
    """
    Формирует карточки по строкам листа REMARKS (ПБ, АР,ММГН, АГО (2025))
    с заданным ОНзС и периодом.
    """
    raw = load_remarks_raw()
    if not raw or len(raw) < 2:
        await bot.send_message(chat_id=chat_id, text="Рабочий лист замечаний пуст или не найден.")
        return

    header = raw[0]
    rows = raw[1:]

    idx_date = excel_col_to_index("B")   # Дата выезда
    idx_onzs = excel_col_to_index("D")   # ОНзС
    idx_dev = excel_col_to_index("E")    # Застройщик
    idx_obj = excel_col_to_index("F")    # Объект
    idx_addr = excel_col_to_index("G")   # Адрес
    idx_case = excel_col_to_index("H")   # Номер дела
    idx_type = excel_col_to_index("I")   # Вид проверки
    idx_inspector = excel_col_to_index("J")  # Должностное лицо

    idx_pb_count = excel_col_to_index("O")
    idx_pb_rr = excel_col_to_index("P")
    idx_pb_mark = excel_col_to_index("Q")
    idx_pbzk_mark = excel_col_to_index("R")
    idx_pb_file = excel_col_to_index("S")
    idx_pb_act = excel_col_to_index("T")
    idx_pb_note = excel_col_to_index("U")

    idx_ar_count = excel_col_to_index("V")
    idx_mmgn_count = excel_col_to_index("W")
    idx_ago_count = excel_col_to_index("X")
    idx_ar_mark = excel_col_to_index("Y")
    idx_ar_file = excel_col_to_index("Z")
    idx_ar_act = excel_col_to_index("AA")
    idx_ar_note = excel_col_to_index("AB")

    idx_eom_count = excel_col_to_index("AC")
    idx_eom_mark = excel_col_to_index("AD")
    idx_eom_file = excel_col_to_index("AE")
    idx_eom_act = excel_col_to_index("AF")
    idx_eom_note = excel_col_to_index("AG")

    idx_common_note = excel_col_to_index("AH")
    idx_zos = excel_col_to_index("AI")

    sent_any = False

    for i, row in enumerate(rows, start=2):  # реальный номер строки
        # расширяем строку при необходимости
        while len(row) <= idx_zos:
            row.append("")

        def get(idx: int) -> str:
            if idx < len(row):
                return str(row[idx]).strip()
            return ""

        # фильтр по ОНзС
        onzs_val = get(idx_onzs)
        if str(onzs_val).strip() != str(onzs_num).strip():
            continue

        # фильтр по дате
        d_parsed = parse_date_safe(get(idx_date))
        if date_from and (not d_parsed or d_parsed < date_from):
            continue
        if date_to and (not d_parsed or d_parsed > date_to):
            continue

        # Собираем данные
        date_str = d_parsed.strftime("%d.%m.%Y") if d_parsed else "-"
        dev = get(idx_dev)
        obj = get(idx_obj)
        addr = get(idx_addr)
        case_no = get(idx_case)
        vtype = get(idx_type)
        inspector = get(idx_inspector)

        pb_cnt = get(idx_pb_count) or "-"
        pb_rr = get(idx_pb_rr) or "-"
        pb_mark = get(idx_pb_mark) or "-"
        pbzk_mark = get(idx_pbzk_mark) or "-"
        pb_file_url = get(idx_pb_file) or "-"
        pb_act_url = get(idx_pb_act) or "-"
        pb_note = get(idx_pb_note) or "-"

        ar_cnt = get(idx_ar_count) or "-"
        mmgn_cnt = get(idx_mmgn_count) or "-"
        ago_cnt = get(idx_ago_count) or "-"
        ar_mark = get(idx_ar_mark) or "-"
        ar_file_url = get(idx_ar_file) or "-"
        ar_act_url = get(idx_ar_act) or "-"
        ar_note = get(idx_ar_note) or "-"

        eom_cnt = get(idx_eom_count) or "-"
        eom_mark = get(idx_eom_mark) or "-"
        eom_file_url = get(idx_eom_file) or "-"
        eom_act_url = get(idx_eom_act) or "-"
        eom_note = get(idx_eom_note) or "-"

        common_note = get(idx_common_note) or "-"
        zos_val = get(idx_zos) or "-"

        lines: List[str] = []
        lines.append(f"ОНзС: {onzs_num}")
        lines.append(f"Строка в таблице: {i}")
        lines.append(f"Дата выезда: {date_str}")
        if vtype:
            lines.append(f"Вид проверки: {vtype}")
        if case_no:
            lines.append(f"Номер дела: {case_no}")
        if dev:
            lines.append(f"Застройщик: {dev}")
        if obj:
            lines.append(f"Объект: {obj}")
        if addr:
            lines.append(f"Адрес: {addr}")
        if inspector:
            lines.append(f"Должностное лицо: {inspector}")

        lines.append("")
        lines.append("Пожарная безопасность:")
        lines.append(f"• Кол-во нарушений ПБ: {pb_cnt}")
        lines.append(f"• РР (нужен/не нужен): {pb_rr}")
        lines.append(f"• Устранение ПБ (Q): {pb_mark}")
        lines.append(f"• Устранение ПБ в ЗК КНД (R): {pbzk_mark}")
        lines.append(f"• Файл замечаний ПБ (S): {pb_file_url}")
        lines.append(f"• Акт ПБ (T): {pb_act_url}")
        lines.append(f"• Примечание ПБ (U): {pb_note}")

        lines.append("")
        lines.append("АР / ММГН / АГО:")
        lines.append(f"• Нарушений АР (V): {ar_cnt}")
        lines.append(f"• Нарушений ММГН (W): {mmgn_cnt}")
        lines.append(f"• Нарушений АГО (X): {ago_cnt}")
        lines.append(f"• Устранение АР/ММГН/АГО (Y): {ar_mark}")
        lines.append(f"• Файл замечаний АР/ММГН/АГО (Z): {ar_file_url}")
        lines.append(f"• Акт АР/ММГН/АГО (AA): {ar_act_url}")
        lines.append(f"• Примечание АР/ММГН/АГО (AB): {ar_note}")

        lines.append("")
        lines.append("Электроснабжение (ЭОМ):")
        lines.append(f"• Нарушений ЭОМ (AC): {eom_cnt}")
        lines.append(f"• Устранение ЭОМ (AD): {eom_mark}")
        lines.append(f"• Файл замечаний ЭОМ (AE): {eom_file_url}")
        lines.append(f"• Акт ЭОМ (AF): {eom_act_url}")
        lines.append(f"• Примечание ЭОМ (AG): {eom_note}")

        if common_note and common_note != "-":
            lines.append("")
            lines.append(f"Общие примечания (AH): {common_note}")
        if zos_val and zos_val != "-":
            lines.append(f"ЗОС (AI): {zos_val}")

        text_msg = "\n".join(lines)

        # Кнопки статусов и вложений
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✔ ПБ", callback_data=f"status_pb_yes_{i}"),
                    InlineKeyboardButton("✖ ПБ", callback_data=f"status_pb_no_{i}"),
                ],
                [
                    InlineKeyboardButton("✔ ПБ ЗК КНД", callback_data=f"status_pbzk_yes_{i}"),
                    InlineKeyboardButton("✖ ПБ ЗК КНД", callback_data=f"status_pbzk_no_{i}"),
                ],
                [
                    InlineKeyboardButton("✔ АР/ММГН/АГО", callback_data=f"status_ar_yes_{i}"),
                    InlineKeyboardButton("✖ АР/ММГН/АГО", callback_data=f"status_ar_no_{i}"),
                ],
                [
                    InlineKeyboardButton("✔ ЭОМ", callback_data=f"status_eom_yes_{i}"),
                    InlineKeyboardButton("✖ ЭОМ", callback_data=f"status_eom_no_{i}"),
                ],
                [
                    InlineKeyboardButton("📎 Прикрепить файл", callback_data=f"attach_onzs_{onzs_num}_{i}")
                ]
            ]
        )

        await bot.send_message(chat_id=chat_id, text=text_msg, reply_markup=kb)
        sent_any = True

    if not sent_any:
        await bot.send_message(
            chat_id=chat_id,
            text=f"По ОНзС {onzs_num} в указанном периоде подходящих строк не найдено."
        )


# --------------------------------------------
#     ОБРАБОТКА СТАТУСОВ (ДА / НЕТ) ПО КНОПКАМ
# --------------------------------------------

async def onzs_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает callback_data вида:
    status_pb_yes_25
    status_pb_no_25
    status_pbzk_yes_25
    status_ar_no_30
    status_eom_yes_40
    """
    query = update.callback_query
    await query.answer()
    data = query.data

    if not data.startswith("status_"):
        return

    try:
        _, kind, yn, row_str = data.split("_", 3)
        row_num = int(row_str)
    except ValueError:
        return

    value = "да" if yn == "yes" else "нет"

    # Определяем столбец
    if kind == "pb":
        col = COL_PB_STATUS          # Q
    elif kind == "pbzk":
        col = COL_PBZK_STATUS        # R
    elif kind == "ar":
        col = COL_AR_STATUS          # Y
    elif kind == "eom":
        col = COL_EOM_STATUS         # AD
    else:
        return

    # Обновляем ячейку в Google Sheets
    update_status_cell(SHEET_REMARKS, row_num, col, value)

    # Пишем в историю (мы не знаем остальные статусы => только один)
    pb = pbzk = ar = eom = None
    if kind == "pb":
        pb = value
    elif kind == "pbzk":
        pbzk = value
    elif kind == "ar":
        ar = value
    elif kind == "eom":
        eom = value

    record_status_change(row_num, pb, pbzk, ar, eom, query.from_user)

    await query.message.reply_text(
        f"Статус по {kind.upper()} в строке {row_num} обновлён на «{value}»."
    )


# --------------------------------------------
#     ПРИКРЕПЛЕНИЕ ФАЙЛА (ФОТО / ДОК / PDF) ДЛЯ ОНЗС
# --------------------------------------------

async def onzs_attach_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает callback_data вида: attach_onzs_<onzs>_<row>
    Ставит флаг в user_data, чтобы следующий файл привязать к этой строке.
    """
    query = update.callback_query
    await query.answer()
    data = query.data

    try:
        _, _, onzs_str, row_str = data.split("_", 3)
        row_num = int(row_str)
    except ValueError:
        return

    context.user_data["await_onzs_attachment"] = {
        "onzs": onzs_str,
        "row": row_num
    }

    await query.message.reply_text(
        f"Пришлите файл (фото или документ), который нужно привязать к ОНзС {onzs_str}, строка {row_num}."
    )


async def generic_attachment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает присланный файл, если ранее был выставлен контекст
    await_onzs_attachment.
    """
    msg = update.message
    if not msg:
        return

    attach_ctx = context.user_data.get("await_onzs_attachment")
    if not attach_ctx:
        # нет привязки к ОНзС — можно расширить логику при необходимости
        return

    onzs_str = attach_ctx["onzs"]
    row_num = attach_ctx["row"]

    file_obj = None
    file_name = "file"

    if msg.document:
        file_obj = msg.document
        file_name = msg.document.file_name or "document"
    elif msg.photo:
        file_obj = msg.photo[-1]
        file_name = "photo.jpg"
    else:
        await msg.reply_text("Нужно отправить документ или фото.")
        return

    f = await file_obj.get_file()
    local_path = f"temp_{file_name}"
    await f.download_to_drive(custom_path=local_path)

    try:
        # Создаём/получаем папку в Google Drive
        folder_id = ensure_drive_folder_for_onzs(onzs_str, row_num)
        drive_url = upload_to_drive(local_path, folder_id)

        # Сохраняем запись в SQLite
        save_file_record(row_num, drive_url, file_name, msg.from_user)

        await msg.reply_text(
            f"Файл загружен в Google Drive и привязан к строке {row_num}.\n"
            f"Ссылка: {drive_url}"
        )
    finally:
        try:
            os.remove(local_path)
        except Exception:
            pass

    # Сбрасываем контекст
    context.user_data["await_onzs_attachment"] = None
# ============================================
#      PART 7 — 👷 ИНСПЕКТОР (МАСТЕР)
# ============================================

async def handle_inspector_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Вход в раздел «Инспектор».
    Запускаем FSM-цепочку вопросов.
    """
    context.user_data["inspector_state"] = {
        "step": "date",
        "form": {}
    }
    await update.message.reply_text(
        "Раздел «👷 Инспектор».\n"
        "Сейчас по шагам заполним данные выезда.\n\n"
        "Шаг 1/8.\n"
        "Введите дату выезда в формате ДД.ММ.ГГГГ:"
    )


async def inspector_fsm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    FSM-мастер для заполнения выезда инспектора.
    Последовательно спрашивает поля и в конце пишет строку в Google Sheets (SHEET_INSPECTOR).
    """
    state = context.user_data.get("inspector_state") or {}
    step = state.get("step")
    form = state.get("form", {})
    text = (update.message.text or "").strip()

    # --- Шаг 1: дата выезда ---
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

        await update.message.reply_text(
            "Шаг 2/8.\n"
            "Площадь (кв.м):"
        )
        return

    # --- Шаг 2: площадь ---
    if step == "area":
        form["area"] = text
        state["step"] = "floors"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Шаг 3/8.\n"
            "Количество этажей:"
        )
        return

    # --- Шаг 3: этажность ---
    if step == "floors":
        form["floors"] = text
        state["step"] = "onzs"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Шаг 4/8.\n"
            "ОНзС (1–12):"
        )
        return

    # --- Шаг 4: ОНзС ---
    if step == "onzs":
        form["onzs"] = text
        state["step"] = "developer"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Шаг 5/8.\n"
            "Наименование застройщика:"
        )
        return

    # --- Шаг 5: застройщик ---
    if step == "developer":
        form["developer"] = text
        state["step"] = "object"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Шаг 6/8.\n"
            "Наименование объекта:"
        )
        return

    # --- Шаг 6: объект ---
    if step == "object":
        form["object"] = text
        state["step"] = "address"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Шаг 7/8.\n"
            "Строительный адрес:"
        )
        return

    # --- Шаг 7: адрес ---
    if step == "address":
        form["address"] = text
        state["step"] = "case_no"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Шаг 8/8.\n"
            "Номер дела (формат 00-00-000000):"
        )
        return

    # --- Шаг 8: номер дела + вид проверки ---
    if step == "case_no":
        form["case_no"] = text
        state["step"] = "check_type"
        state["form"] = form
        context.user_data["inspector_state"] = state

        await update.message.reply_text(
            "Дополнительно укажите вид проверки\n"
            "(ПП, итоговая, профвизит, запрос ОНзС, поручение руководства):"
        )
        return

    if step == "check_type":
        form["check_type"] = text

        # Всё собрали — записываем в Google Sheets
        context.user_data["inspector_state"] = None

        try:
            # Загружаем текущие данные листа инспектора, чтобы понять номер следующей строки и порядковый номер
            data = load_sheet_data(SHEET_INSPECTOR)
            if not data:
                # Если лист пустой — создадим заголовок + первый ряд
                # Но обычно у вас уже есть заголовок, поэтому этот кейс — запасной
                header = [
                    "№ п/п",                # A
                    "Дата выезда",          # B
                    "Площадь. Этажность",   # C
                    "ОНзС",                 # D
                    "Наименование застройщика",  # E
                    "Наименование объекта",      # F
                    "Строительный адрес",        # G
                    "Номер дела",                # H
                    "Вид проверки",              # I
                    "Должностное лицо УПКиСОТ"   # J
                ]
                sheets_api.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_INSPECTOR}!A1:J1",
                    valueInputOption="USER_ENTERED",
                    body={"values": [header]}
                ).execute()
                data = [header]

            # data[0] — заголовок, далее строки
            # Номер следующего объекта:
            # количество фактических строк (без заголовка) = len(data) - 1
            current_count = max(len(data) - 1, 0)
            new_index = current_count + 1   # это пойдёт в колонку A (№ п/п)

            # Преобразуем дату в текст ДД.ММ.ГГГГ
            date_obj = form.get("date")
            if isinstance(date_obj, (datetime, date)):
                date_str = date_obj.strftime("%d.%m.%Y")
            else:
                date_str = str(date_obj or "")

            area = form.get("area", "")
            floors = form.get("floors", "")
            area_floors = f"Площадь: {area}; этажность: {floors}"

            onzs = form.get("onzs", "")
            developer = form.get("developer", "")
            obj = form.get("object", "")
            address = form.get("address", "")
            case_no = form.get("case_no", "")
            check_type = form.get("check_type", "")

            inspector_name = ""
            user = update.effective_user
            if user and (user.full_name or user.username):
                inspector_name = user.full_name or f"@{user.username}"

            # Формируем строку для записи (A..J)
            row_to_append = [
                new_index,      # A: № п/п
                date_str,       # B: Дата выезда
                area_floors,    # C: Площадь. Этажность
                onzs,           # D: ОНзС
                developer,      # E: Наименование застройщика
                obj,            # F: Наименование объекта
                address,        # G: Строительный адрес
                case_no,        # H: Номер дела
                check_type,     # I: Вид проверки
                inspector_name  # J: Должностное лицо УПКиСОТ
            ]

            # Добавляем строку в лист инспектора
            sheet_append(SHEET_INSPECTOR, row_to_append)

            await update.message.reply_text(
                "Выезд сохранён в Google Sheets "
                f"на лист «{SHEET_INSPECTOR}».\n"
                f"№ п/п: {new_index}",
                reply_markup=main_menu()
            )

        except Exception as e:
            log.error(f"Ошибка записи выезда инспектора в Google Sheets: {e}")
            await update.message.reply_text(
                "Не удалось сохранить выезд в Google Sheets.\n"
                "Сообщите администратору или проверьте доступы.",
                reply_markup=main_menu()
            )
        return

    # Если по какой-то причине шаг неизвестен — сбросим FSM
    context.user_data["inspector_state"] = None
    await update.message.reply_text(
        "Произошла ошибка в мастере «Инспектор». Попробуйте начать заново.",
        reply_markup=main_menu()
    )
# ============================================
#      PART 8 — 📈 АНАЛИТИКА И MAIN()
# ============================================

# --------------------------------------------
#              📈 АНАЛИТИКА
# --------------------------------------------

def build_analytics_text() -> str:
    """
    Строит текстовый отчёт по данным из SQLite:
    - сколько раз ставили «да» / «нет» по ПБ / ПБЗК / АР / ЭОМ
    - сколько вложений прикреплено
    - последние 10 изменений статусов
    """
    conn = get_db()
    c = conn.cursor()

    lines: List[str] = []
    lines.append("📈 Аналитика по данным бота")
    lines.append("")

    # 1. Сводка по статусам
    c.execute("""
        SELECT
          pb_status,
          pbzk_status,
          ar_status,
          eom_status
        FROM remarks_history
    """)
    rows = c.fetchall()

    def count_values(field: str, value: str) -> int:
        cnt = 0
        for r in rows:
            if r[field] == value:
                cnt += 1
        return cnt

    pb_yes = count_values("pb_status", "да")
    pb_no = count_values("pb_status", "нет")
    pbzk_yes = count_values("pbzk_status", "да")
    pbzk_no = count_values("pbzk_status", "нет")
    ar_yes = count_values("ar_status", "да")
    ar_no = count_values("ar_status", "нет")
    eom_yes = count_values("eom_status", "да")
    eom_no = count_values("eom_status", "нет")

    lines.append("1️⃣ Статусы устранения (по истории изменений):")
    lines.append(f"• ПБ: да = {pb_yes}, нет = {pb_no}")
    lines.append(f"• ПБ в ЗК КНД: да = {pbzk_yes}, нет = {pbzk_no}")
    lines.append(f"• АР/ММГН/АГО: да = {ar_yes}, нет = {ar_no}")
    lines.append(f"• ЭОМ: да = {eom_yes}, нет = {eom_no}")
    lines.append("")

    # 2. Кол-во прикреплённых файлов
    c.execute("SELECT COUNT(*) AS c FROM attachments")
    attachments_total = c.fetchone()["c"]
    lines.append("2️⃣ Вложения:")
    lines.append(f"• Всего прикреплённых файлов: {attachments_total}")
    lines.append("")

    # 3. Последние 10 изменений статусов
    c.execute("""
        SELECT excel_row, pb_status, pbzk_status, ar_status, eom_status,
               updated_by_id, updated_by_username, updated_at
        FROM remarks_history
        ORDER BY datetime(updated_at) DESC
        LIMIT 10
    """)
    hist = c.fetchall()
    lines.append("3️⃣ Последние 10 изменений статусов:")

    if not hist:
        lines.append("• пока нет данных по изменениям")
    else:
        for r in hist:
            row_num = r["excel_row"]
            pb = r["pb_status"] or "-"
            pbzk = r["pbzk_status"] or "-"
            ar = r["ar_status"] or "-"
            eom = r["eom_status"] or "-"
            uid = r["updated_by_id"] or "-"
            uname = r["updated_by_username"] or "-"
            dt_raw = r["updated_at"] or ""
            try:
                dt_obj = datetime.fromisoformat(dt_raw)
                dt_str = dt_obj.strftime("%d.%m.%Y %H:%M")
            except Exception:
                dt_str = dt_raw

            lines.append(
                f"• Строка {row_num} — ПБ={pb}, ПБЗК={pbzk}, АР={ar}, ЭОМ={eom}; "
                f"изменил {uname or uid} в {dt_str}"
            )

    conn.close()
    return "\n".join(lines)


async def handle_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Вход в раздел «📈 Аналитика».
    Сначала просим пароль, затем показываем отчёт.
    """
    context.user_data["await_analytics_password"] = True
    await update.message.reply_text("Введите пароль для входа в раздел «📈 Аналитика»:")


async def analytics_password_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка ввода пароля аналитики.
    Вызывается из text_router, если стоит флаг await_analytics_password.
    """
    if not context.user_data.get("await_analytics_password"):
        return

    pwd = (update.message.text or "").strip()
    if pwd != ANALYTICS_PASSWORD:
        context.user_data["await_analytics_password"] = False
        await update.message.reply_text("Неверный пароль.")
        return

    # Пароль верный
    context.user_data["await_analytics_password"] = False
    text = build_analytics_text()
    await update.message.reply_text(text, disable_web_page_preview=True, reply_markup=main_menu())


# --------------------------------------------
#  ОБНОВЛЁННЫЙ РОУТЕР ТЕКСТА (ЗАМЕНЯЕТ СТАРЫЙ)
# --------------------------------------------

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Финальный роутер текстовых сообщений.

    Приоритет:
    1) FSM «Инспектор»
    2) ожидание ввода периода ОНзС (custom)
    3) ожидание пароля аналитики
    4) кнопки главного меню
    5) прочее — напоминание про меню
    """
    text_raw = (update.message.text or "").strip()
    text = text_raw.lower()

    # 1) Мастер «Инспектор»
    if context.user_data.get("inspector_state"):
        await inspector_fsm(update, context)
        return

    # 2) Ожидаем период для ОНзС
    if context.user_data.get("onzs_wait_custom_period"):
        await onzs_custom_period_text(update, context)
        return

    # 3) Ожидаем пароль аналитики
    if context.user_data.get("await_analytics_password"):
        await analytics_password_text(update, context)
        return

    # 4) Кнопки главного меню
    if text == "📅 график".lower():
        await handle_schedule(update, context)
        return

    if text == "📊 итоговая".lower():
        await handle_final(update, context)
        return

    if text == "📝 замечания".lower():
        await handle_remarks_menu(update, context)
        return

    if text == "🏗 онзс".lower():
        await handle_onzs_menu(update, context)
        return

    if text == "👷 инспектор".lower():
        await handle_inspector_start(update, context)
        return

    if text == "📈 аналитика".lower():
        await handle_analytics(update, context)
        return

    # 5) Остальное
    await update.message.reply_text("Выберите действие из меню.", reply_markup=main_menu())


# --------------------------------------------
#          РЕГИСТРАЦИЯ HANDLERS И MAIN
# --------------------------------------------

def main():
    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в переменных окружения или .env")

    # Инициализация БД
    init_db()

    application = Application.builder().token(BOT_TOKEN).build()

    # Команды
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("id", id_cmd))

    # Callback-кнопки для «Замечаний»
    application.add_handler(CallbackQueryHandler(remarks_callback, pattern="^remarks_"))

    # Callback-кнопки для «ОНзС»
    application.add_handler(CallbackQueryHandler(onzs_select_callback, pattern="^onzs_select_"))
    application.add_handler(CallbackQueryHandler(onzs_period_callback, pattern="^onzs_period_"))
    application.add_handler(CallbackQueryHandler(onzs_status_callback, pattern="^status_"))
    application.add_handler(CallbackQueryHandler(onzs_attach_callback, pattern="^attach_onzs_"))

    # Документы / фото (для прикрепления к ОНзС)
    application.add_handler(MessageHandler(
        filters.Document.ALL | filters.PHOTO,
        generic_attachment_handler
    ))

    # Прочий текст — общий роутер
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        text_router
    ))

    log.info("Бот запущен в режиме polling...")
    application.run_polling()


if __name__ == "__main__":
    main()

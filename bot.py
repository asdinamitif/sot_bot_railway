import logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from io import BytesIO
from typing import Optional, Dict, Any, List, Any as AnyType

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
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# -------------------------------------------------
# ЛОГИ
# -------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sot_bot")

# -------------------------------------------------
# ENV / НАСТРОЙКИ
# -------------------------------------------------
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DB_PATH = os.getenv("DB_PATH", "sot_bot.db")

TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "3"))
ANALYTICS_PASSWORD = "051995"

GOOGLE_SHEET_URL_DEFAULT = (
    "https://docs.google.com/spreadsheets/d/"
    "1FlhN7grvku5tSj2SAreEHxHC55K9E7N91r8eWOkzOFY/edit?usp=sharing"
)

GSHEETS_SERVICE_ACCOUNT_JSON = os.getenv("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()
GSHEETS_SPREADSHEET_ID = os.getenv(
    "GSHEETS_SPREADSHEET_ID",
    "1FlhN7grvku5tSj2SAreEHxHC55K9E7N91r8eWOkzOFY",
).strip()

SHEETS_SERVICE = None  # кеш клиента Google Sheets

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

SCHEDULE_NOTIFY_CHAT_ID_ENV = os.getenv("SCHEDULE_NOTIFY_CHAT_ID", "").strip()


def is_admin(uid: int) -> bool:
    return uid in HARD_CODED_ADMINS


def local_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)


def get_current_remarks_sheet_name() -> str:
    year = local_now().year
    return f"ПБ, АР,ММГН, АГО ({year})"



# ================================
# Google Sheets
# ================================
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
        return None

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{sheet_name}'!A1:ZZZ1000",
        ).execute()
        values = result.get("values", [])
        if not values:
            return pd.DataFrame()

        if header_row_index is None:
            header_row_index = detect_header_row(values)

        headers = values[header_row_index]
        data_rows = values[header_row_index + 1 :]

        df = pd.DataFrame(data_rows, columns=headers)
        df = df.dropna(how="all").reset_index(drop=True)
        return df

    except Exception as e:
        log.error("Ошибка чтения листа '%s': %s", sheet_name, e)
        return None


# =====================
# Вспомогательные
# =====================
def excel_col_to_index(col: str) -> int:
    col = col.upper().strip()
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_col_by_letter(df: pd.DataFrame, letters: str) -> Optional[str]:
    idx = excel_col_to_index(letters)
    if 0 <= idx < len(df.columns):
        return df.columns[idx]
    return None
# ============================
# Клавиатуры
# ============================
def main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📅 График", "📊 Итоговая"],
        ["📝 Замечания", "🏗 ОНзС"],
        ["Инспектор", "📈 Аналитика"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def remarks_menu_inline() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("❌ Не устранены", callback_data="remarks_not_done")],
        [InlineKeyboardButton("📥 Скачать файл", callback_data="remarks_download")],
    ]
    return InlineKeyboardMarkup(buttons)


def onzs_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Показать ОНзС по делу", callback_data="onzs_by_case")]]
    )


def inspector_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("➕ Добавить выезд", callback_data="inspector_add")]]
    )


# ============================
# Тексты графика
# ============================
def build_schedule_text(is_admin_flag: bool, settings: dict) -> str:
    version = int(settings.get("schedule_version", "1"))
    name = "График.xlsx"

    lines = [
        f"📅 График выездов (версия {version})",
        f"Файл: {name}",
    ]

    approvers = settings.get("current_approvers", "")
    if approvers:
        lines.append("Согласующие:")
        for a in approvers.split(","):
            lines.append(f"• {a.strip()}")

    if is_admin_flag:
        lines.append("\nВы администратор.")
    return "\n".join(lines)


# ============================
# Получение данных замечаний
# ============================
def get_remarks_df_current() -> Optional[pd.DataFrame]:
    """
    Только текущий лист, который соответствует году бота.
    """
    sheet_name = get_current_remarks_sheet_name()
    url = build_export_url(GSHEETS_SPREADSHEET_ID)

    try:
        resp = requests.get(url, timeout=40)
        resp.raise_for_status()
    except Exception as e:
        log.error("HTTP ошибка при обращении к файлу замечаний: %s", e)
        return None

    try:
        xls = pd.ExcelFile(BytesIO(resp.content))
    except Exception as e:
        log.error("Ошибка чтения Excel: %s", e)
        return None

    if sheet_name not in xls.sheet_names:
        log.error("Лист '%s' отсутствует", sheet_name)
        return None

    try:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        return df
    except Exception as e:
        log.error("Ошибка чтения листа '%s': %s", sheet_name, e)
        return None


def get_remarks_df() -> Optional[pd.DataFrame]:
    url = build_export_url(GSHEETS_SPREADSHEET_ID)

    try:
        resp = requests.get(url, timeout=40)
        resp.raise_for_status()
    except Exception as e:
        log.error("HTTP ошибка при чтении замечаний (all): %s", e)
        return None

    try:
        xls = pd.ExcelFile(BytesIO(resp.content))
    except Exception as e:
        log.error("Ошибка открытия Excel (all): %s", e)
        return None

    frames = []
    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception:
            continue
        df["_sheet"] = sheet_name
        frames.append(df)

    if not frames:
        return None

    return pd.concat(frames, ignore_index=True)


# ============================
# ОНзС
# ============================
def build_onzs_text_for_case(df: pd.DataFrame, case_no: str) -> str:
    # колонка I (номер дела)
    col_case = get_col_by_letter(df, "I")

    # колонка E (ОНзС)
    col_onzs = get_col_by_letter(df, "E")

    if not col_case or not col_onzs:
        return "Не удалось определить структуру файла."

    df_f = df[df[col_case].astype(str).str.strip() == case_no.strip()]
    if df_f.empty:
        return f"Не найдено строк для дела {case_no}."

    values = df_f[col_onzs].dropna().astype(str).unique().tolist()
    if not values:
        return f"У дела {case_no} нет данных ОНзС."

    return f"ОНзС по делу {case_no}:\n" + "\n".join(f"• {v}" for v in values)


# ============================================
# Инспектор (пошаговое заполнение)
# ============================================
async def inspector_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    form = context.user_data.get("inspector_form")

    step = form.get("step", "")

    if step == "date_departure":
        try:
            form["date_departure"] = datetime.strptime(text, "%d.%m.%Y").date()
        except:
            await update.message.reply_text("Введите дату формата ДД.ММ.ГГГГ")
            return
        form["step"] = "date_final"
        await update.message.reply_text("Дата начала итоговой проверки (ДД.ММ.ГГГГ):")
        return

    if step == "date_final":
        try:
            form["date_final"] = datetime.strptime(text, "%d.%m.%Y").date()
        except:
            await update.message.reply_text("Введите дату формата ДД.ММ.ГГГГ")
            return
        form["step"] = "area"
        await update.message.reply_text("Введите площадь (кв.м):")
        return

    if step == "area":
        form["area"] = text
        form["step"] = "floors"
        await update.message.reply_text("Введите количество этажей:")
        return

    if step == "floors":
        form["floors"] = text
        form["step"] = "onzs"
        await update.message.reply_text("Введите ОНзС:")
        return

    if step == "onzs":
        form["onzs"] = text
        form["step"] = "developer"
        await update.message.reply_text("Введите застройщика:")
        return

    if step == "developer":
        form["developer"] = text
        form["step"] = "object"
        await update.message.reply_text("Введите наименование объекта:")
        return

    if step == "object":
        form["object"] = text
        form["step"] = "address"
        await update.message.reply_text("Введите строительный адрес:")
        return

    if step == "address":
        form["address"] = text
        form["step"] = "case_no"
        await update.message.reply_text("Введите номер дела:")
        return

    if step == "case_no":
        form["case_no"] = text
        form["step"] = "check_type"
        await update.message.reply_text("Введите вид проверки:")
        return

    if step == "check_type":
        form["check_type"] = text
        ok = append_inspector_row_to_excel(form)
        if ok:
            await update.message.reply_text(
                "Выезд успешно добавлен в Google Sheet."
            )
        else:
            await update.message.reply_text(
                "Не удалось записать в Google Sheet."
            )
        context.user_data["inspector_form"] = None
        return
        if is_net_value(val_eom):
            eom_cols.add(TITLES["eom"])

        # Если нет ни одного "нет" — пропускаем
        if not (pb_cols or ar_cols or eom_cols):
            continue

        if case_val not in grouped:
            grouped[case_val] = {"pb": set(), "ar": set(), "eom": set()}

        grouped[case_val]["pb"].update(pb_cols)
        grouped[case_val]["ar"].update(ar_cols)
        grouped[case_val]["eom"].update(eom_cols)

    # Ничего не найдено
    if not grouped:
        return "Во всех строках статусы устранения не содержат «нет»."

    # Формируем вывод
    lines = [
        "Строки со статусом «НЕ УСТРАНЕНЫ (нет)»",
        f"Лист: «{get_current_remarks_sheet_name()}»",
        "",
    ]

    for case_no, blocks in grouped.items():
        parts = []

        if blocks["pb"]:
            parts.append(
                "Пожарная безопасность: " +
                ", ".join(f"{title} - нет" for title in sorted(blocks["pb"]))
            )

        if blocks["ar"]:
            parts.append(
                "Архитектура, ММГН, АГО: " +
                ", ".join(f"{title} - нет" for title in sorted(blocks["ar"]))
            )

        if blocks["eom"]:
            parts.append(
                "Электроснабжение: " +
                ", ".join(f"{title} - нет" for title in sorted(blocks["eom"]))
            )

        lines.append(f"• {case_no} — " + "; ".join(parts))

    return "\n".join(lines)


# -------------------------------------------------
# Отправка длинного текста
# -------------------------------------------------
async def send_long_text(chat, text: str, chunk_size: int = 3500):
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
# CALLBACK HANDLER
# -------------------------------------------------
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()

    # === Замечания → Не устранены ===
    if data == "remarks_not_done":
        await query.message.reply_text("Ищу строки со статусом «нет»...")

        df = get_remarks_df_current()
        if df is None:
            await query.message.reply_text(
                "Не удалось получить файл замечаний. Проверьте доступ."
            )
            return

        text = build_remarks_not_done_text(df)
        await send_long_text(query.message.chat, text)
        return

    # === Замечания → Скачать ===
    if data == "remarks_download":
        await query.message.reply_text(
            "Файл замечаний можно открыть по ссылке:\n"
            f"{GOOGLE_SHEET_URL_DEFAULT}"
        )
        return

    # === ОНзС ===
    if data == "onzs_by_case":
        context.user_data["awaiting_onzs_case"] = True
        await query.message.reply_text("Введите номер дела (формат 00-00-000000):")
        return

    # === Инспектор: добавить выезд ===
    if data == "inspector_add":
        context.user_data["inspector_form"] = {"step": "date_departure"}
        await query.message.reply_text("Введите дату выезда (ДД.ММ.ГГГГ):")
        return


# -------------------------------------------------
# TEXT ROUTER
# -------------------------------------------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # === ОНзС: ввод номера дела ===
    if context.user_data.get("awaiting_onzs_case"):
        context.user_data["awaiting_onzs_case"] = False
        df = get_remarks_df()
        if df is None:
            await update.message.reply_text("Ошибка чтения данных замечаний.")
            return
        resp = build_onzs_text_for_case(df, text)
        await update.message.reply_text(resp)
        return

    # === Инспектор ===
    if context.user_data.get("inspector_form"):
        await inspector_process(update, context)
        return

    # === Основное меню ===
    if text.lower() == "📅 график".lower():
        settings = get_schedule_state()
        is_admin_flag = is_admin(update.effective_user.id)
        msg = build_schedule_text(is_admin_flag, settings)
        kb = build_schedule_inline(is_admin_flag, settings)
        await update.message.reply_text(msg, reply_markup=kb)
        return

    if text.lower() == "📊 итоговая".lower():
        await update.message.reply_text("Раздел «Итоговая» пока в упрощённом виде.")
        return

    if text.lower() == "📝 замечания".lower():
        kb = remarks_menu_inline()
        await update.message.reply_text("Раздел «Замечания»:", reply_markup=kb)
        return

    if text.lower() == "🏗 онзс".lower():
        kb = onzs_menu_inline()
        await update.message.reply_text("Раздел «ОНзС»:", reply_markup=kb)
        return

    if text.lower() == "инспектор":
        kb = inspector_menu_inline()
        await update.message.reply_text("Раздел «Инспектор»:", reply_markup=kb)
        return

    if text.lower() == "📈 аналитика".lower():
        await update.message.reply_text("Аналитика появится позже.")
        return

    await update.message.reply_text(
        "Я вас не понял. Выберите пункт меню или нажмите /start.",
        reply_markup=main_menu(),
    )


# -------------------------------------------------
# DOCUMENT HANDLER
# -------------------------------------------------
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Функция загрузки файлов пока доступна только администратору.")
    return


# -------------------------------------------------
# START / HELP
# -------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добро пожаловать в бота отдела СОТ.",
        reply_markup=main_menu(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Доступные разделы:\n"
        "• 📅 График\n"
        "• 📊 Итоговая\n"
        "• 📝 Замечания\n"
        "• 🏗 ОНзС\n"
        "• Инспектор\n"
        "• 📈 Аналитика"
    )


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан.")
        raise SystemExit("Укажите BOT_TOKEN в .env или переменных окружения.")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # HANDLERS
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(CallbackQueryHandler(callback_handler))

    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    log.info("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()

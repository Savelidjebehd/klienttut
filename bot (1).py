import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

from telethon import TelegramClient
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import User
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "YOUR_API_HASH")
PHONE = os.environ.get("PHONE", "+79000000000")

DB_PATH = "bot_data.db"

(
    STATE_MAIN_MENU,
    STATE_SPHERE_MENU,
    STATE_ADD_SPHERE_NAME,
    STATE_AFTER_STAGE,
    STATE_ADD_STAGE_DESC,
    STATE_ADD_STAGE_SCRIPT,
    STATE_ADD_KEYWORDS,
    STATE_SHOW_CLIENT,
) = range(8)

telethon_client: Optional[TelegramClient] = None


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS spheres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            keywords TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS stages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sphere_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            description TEXT NOT NULL,
            script TEXT NOT NULL,
            FOREIGN KEY (sphere_id) REFERENCES spheres(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sphere_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sphere_id INTEGER NOT NULL,
            group_username TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sphere_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            group_username TEXT NOT NULL,
            found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used INTEGER DEFAULT 0,
            UNIQUE(sphere_id, username)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS group_round_robin (
            sphere_id INTEGER PRIMARY KEY,
            last_group_index INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()


def get_spheres():
    conn = get_db()
    rows = conn.execute("SELECT * FROM spheres").fetchall()
    conn.close()
    return rows


def get_sphere_by_name(name):
    conn = get_db()
    row = conn.execute("SELECT * FROM spheres WHERE name = ?", (name,)).fetchone()
    conn.close()
    return row


def get_stages(sphere_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM stages WHERE sphere_id = ? ORDER BY position", (sphere_id,)
    ).fetchall()
    conn.close()
    return rows


def add_sphere(name, keywords):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO spheres (name, keywords) VALUES (?, ?)", (name, keywords))
    sphere_id = c.lastrowid
    conn.commit()
    conn.close()
    return sphere_id


def add_stage(sphere_id, description, script):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as cnt FROM stages WHERE sphere_id = ?", (sphere_id,))
    row = c.fetchone()
    position = (row[0] if row else 0) + 1
    c.execute(
        "INSERT INTO stages (sphere_id, position, description, script) VALUES (?, ?, ?, ?)",
        (sphere_id, position, description, script),
    )
    conn.commit()
    conn.close()


def add_groups_for_sphere(sphere_id, group_usernames):
    conn = get_db()
    c = conn.cursor()
    for g in group_usernames:
        c.execute(
            "INSERT INTO sphere_groups (sphere_id, group_username) VALUES (?, ?)",
            (sphere_id, g),
        )
    conn.commit()
    conn.close()


def get_groups_for_sphere(sphere_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT group_username FROM sphere_groups WHERE sphere_id = ?", (sphere_id,)
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def cleanup_old_clients():
    conn = get_db()
    cutoff = datetime.now() - timedelta(days=7)
    conn.execute("DELETE FROM clients WHERE found_at < ?", (cutoff,))
    conn.commit()
    conn.close()


def count_available_clients(sphere_id):
    cleanup_old_clients()
    conn = get_db()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM clients WHERE sphere_id = ? AND used = 0",
        (sphere_id,),
    ).fetchone()
    conn.close()
    return row[0] if row else 0


def get_next_client(sphere_id):
    cleanup_old_clients()
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM clients WHERE sphere_id = ? AND used = 0 ORDER BY id LIMIT 1",
        (sphere_id,),
    ).fetchone()
    conn.close()
    return row


def mark_client_used(client_id):
    conn = get_db()
    conn.execute("UPDATE clients SET used = 1 WHERE id = ?", (client_id,))
    conn.commit()
    conn.close()


def is_username_used_or_known(sphere_id, username):
    conn = get_db()
    row = conn.execute(
        "SELECT id FROM clients WHERE sphere_id = ? AND username = ?",
        (sphere_id, username),
    ).fetchone()
    conn.close()
    return row is not None


def get_round_robin_index(sphere_id):
    conn = get_db()
    row = conn.execute(
        "SELECT last_group_index FROM group_round_robin WHERE sphere_id = ?",
        (sphere_id,),
    ).fetchone()
    conn.close()
    return row[0] if row else 0


def set_round_robin_index(sphere_id, idx):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO group_round_robin (sphere_id, last_group_index) VALUES (?, ?)",
        (sphere_id, idx),
    )
    conn.commit()
    conn.close()


async def ensure_telethon():
    global telethon_client
    if telethon_client is None or not telethon_client.is_connected():
        telethon_client = TelegramClient("session_bot", API_ID, API_HASH)
        await telethon_client.start(phone=PHONE)
    return telethon_client


async def search_groups_by_keywords(keywords: list) -> list:
    client = await ensure_telethon()
    found_groups = []
    seen = set()
    for kw in keywords:
        try:
            result = await client(SearchRequest(q=kw, limit=10))
            for chat in result.chats:
                username = getattr(chat, "username", None)
                if username and username not in seen:
                    seen.add(username)
                    found_groups.append(username)
        except Exception as e:
            logger.warning(f"Group search error for '{kw}': {e}")
        if len(found_groups) >= 20:
            break
    return found_groups


async def collect_clients_from_group(group_username: str, sphere_id: int, limit: int = 200):
    client = await ensure_telethon()
    six_months_ago = datetime.now() - timedelta(days=180)
    collected = 0
    seen_users = set()
    try:
        entity = await client.get_entity(group_username)
        history = await client(
            GetHistoryRequest(
                peer=entity,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0,
            )
        )
        conn = get_db()
        c = conn.cursor()
        for msg in history.messages:
            if not msg.date:
                continue
            msg_date = msg.date.replace(tzinfo=None)
            if msg_date < six_months_ago:
                continue
            if not hasattr(msg, "from_id") or msg.from_id is None:
                continue
            user_id = getattr(msg.from_id, "user_id", None)
            if not user_id or user_id in seen_users:
                continue
            seen_users.add(user_id)
            try:
                user = await client.get_entity(user_id)
                if isinstance(user, User) and user.username:
                    uname = user.username
                    if not is_username_used_or_known(sphere_id, uname):
                        try:
                            c.execute(
                                "INSERT OR IGNORE INTO clients (sphere_id, username, group_username) VALUES (?, ?, ?)",
                                (sphere_id, uname, group_username),
                            )
                            collected += 1
                        except Exception:
                            pass
            except Exception:
                pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Error collecting from group {group_username}: {e}")
    return collected


async def fill_client_buffer(sphere_id: int):
    if count_available_clients(sphere_id) >= 10:
        return
    groups = get_groups_for_sphere(sphere_id)
    if not groups:
        return
    idx = get_round_robin_index(sphere_id)
    attempts = 0
    while count_available_clients(sphere_id) < 10 and attempts < len(groups):
        group = groups[idx % len(groups)]
        await collect_clients_from_group(group, sphere_id)
        idx = (idx + 1) % len(groups)
        attempts += 1
    set_round_robin_index(sphere_id, idx)


def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🖐🏻 Ручной поиск")]],
        resize_keyboard=True,
    )


def sphere_menu_keyboard():
    spheres = get_spheres()
    buttons = [[KeyboardButton(s["name"])] for s in spheres]
    buttons.append([KeyboardButton("➕ Добавить сферу")])
    buttons.append([KeyboardButton("Главное меню")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def client_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("➡️ Следующий"), KeyboardButton("Главное меню")]],
        resize_keyboard=True,
    )


def after_stage_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("+Этап"), KeyboardButton("Продолжить")]],
        resize_keyboard=True,
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Добро пожаловать! Выберите действие:",
        reply_markup=main_menu_keyboard(),
    )
    return STATE_MAIN_MENU


async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "🖐🏻 Ручной поиск":
        await update.message.reply_text(
            "Выберите сферу:", reply_markup=sphere_menu_keyboard()
        )
        return STATE_SPHERE_MENU
    return STATE_MAIN_MENU


async def handle_sphere_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "Главное меню":
        await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU

    if text == "➕ Добавить сферу":
        context.user_data["new_sphere"] = {"stages": []}
        await update.message.reply_text(
            "Введите название сферы:", reply_markup=ReplyKeyboardRemove()
        )
        return STATE_ADD_SPHERE_NAME

    sphere = get_sphere_by_name(text)
    if sphere:
        context.user_data["current_sphere_id"] = sphere["id"]
        context.user_data["current_sphere_name"] = sphere["name"]
        asyncio.create_task(fill_client_buffer(sphere["id"]))
        return await show_next_client(update, context)

    await update.message.reply_text("Сфера не найдена.", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU


async def show_next_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sphere_id = context.user_data.get("current_sphere_id")
    sphere_name = context.user_data.get("current_sphere_name")

    if not sphere_id:
        await update.message.reply_text("Ошибка. Вернитесь в главное меню.", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU

    client_row = get_next_client(sphere_id)
    if not client_row:
        await update.message.reply_text(
            "Клиентов пока нет. Идёт поиск, попробуйте чуть позже.",
            reply_markup=client_keyboard(),
        )
        asyncio.create_task(fill_client_buffer(sphere_id))
        return STATE_SHOW_CLIENT

    context.user_data["current_client_id"] = client_row["id"]
    username = client_row["username"]
    stages = get_stages(sphere_id)

    lines = [f"<b>@{username}</b>\n", f"<b>{sphere_name}</b>"]
    for i, stage in enumerate(stages, 1):
        lines.append(f"\n<b>{i} этап — {stage['description']}</b>")
        lines.append(f"<blockquote>{stage['script']}</blockquote>")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=client_keyboard(),
    )

    asyncio.create_task(fill_client_buffer(sphere_id))
    return STATE_SHOW_CLIENT


async def handle_show_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "➡️ Следующий":
        client_id = context.user_data.get("current_client_id")
        if client_id:
            mark_client_used(client_id)
        return await show_next_client(update, context)

    if text == "Главное меню":
        client_id = context.user_data.get("current_client_id")
        if client_id:
            mark_client_used(client_id)
        for key in ("current_sphere_id", "current_sphere_name", "current_client_id"):
            context.user_data.pop(key, None)
        await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU

    return STATE_SHOW_CLIENT


async def handle_add_sphere_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Введите название сферы:")
        return STATE_ADD_SPHERE_NAME
    context.user_data["new_sphere"]["name"] = name
    await update.message.reply_text(
        "Теперь добавьте этапы переписки:",
        reply_markup=after_stage_keyboard(),
    )
    return STATE_AFTER_STAGE


async def handle_after_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "+Этап":
        await update.message.reply_text(
            "Введите пояснение этапа:", reply_markup=ReplyKeyboardRemove()
        )
        return STATE_ADD_STAGE_DESC
    if text == "Продолжить":
        await update.message.reply_text(
            "Введите ключевые слова для поиска групп (каждое с новой строки):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_ADD_KEYWORDS
    return STATE_AFTER_STAGE


async def handle_add_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_sphere"]["current_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:")
    return STATE_ADD_STAGE_SCRIPT


async def handle_add_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip()
    desc = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text(
        "Этап сохранён. Добавьте ещё или продолжите:",
        reply_markup=after_stage_keyboard(),
    )
    return STATE_AFTER_STAGE


async def handle_add_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = [kw.strip() for kw in update.message.text.strip().splitlines() if kw.strip()]
    new_sphere = context.user_data.get("new_sphere", {})
    name = new_sphere.get("name", "")
    stages = new_sphere.get("stages", [])

    sphere_id = add_sphere(name, json.dumps(keywords, ensure_ascii=False))
    for stage in stages:
        add_stage(sphere_id, stage["desc"], stage["script"])

    await update.message.reply_text(
        f"Сфера «{name}» сохранена! Ищу группы по ключевым словам..."
    )

    try:
        groups = await search_groups_by_keywords(keywords)
        if groups:
            add_groups_for_sphere(sphere_id, groups)
            await update.message.reply_text(
                f"Найдено групп: {len(groups)}. Начинаю сбор клиентов..."
            )
            asyncio.create_task(fill_client_buffer(sphere_id))
        else:
            await update.message.reply_text(
                "Группы не найдены. Попробуйте другие ключевые слова."
            )
    except Exception as e:
        logger.error(f"Error searching groups: {e}")
        await update.message.reply_text(
            "Ошибка при поиске групп. Проверьте настройки Telethon."
        )

    context.user_data.pop("new_sphere", None)
    await update.message.reply_text("Выберите сферу:", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU


async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используйте кнопки меню.", reply_markup=main_menu_keyboard())
    return STATE_MAIN_MENU


def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)],
            STATE_SPHERE_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sphere_menu)],
            STATE_ADD_SPHERE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_sphere_name)],
            STATE_AFTER_STAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_after_stage)],
            STATE_ADD_STAGE_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_desc)],
            STATE_ADD_STAGE_SCRIPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_script)],
            STATE_ADD_KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_keywords)],
            STATE_SHOW_CLIENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_show_client)],
        },
        fallbacks=[CommandHandler("start", cmd_start), MessageHandler(filters.ALL, fallback)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()

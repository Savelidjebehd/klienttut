# ============================================================
#  TELEGRAM LEAD BOT — ОДИН ФАЙЛ
#  Запуск: python bot.py
#
#  Нужные файлы рядом с bot.py:
#    .env          — переменные окружения (см. ниже)
#    sessions/     — папка для Telethon-сессий (создаётся сама)
#    leads_bot.db  — SQLite БД (создаётся сама)
#
#  .env содержит:
#    BOT_TOKEN=...
#    TELEGRAM_API_ID=...
#    TELEGRAM_API_HASH=...
#    DATABASE_URL=sqlite+aiosqlite:///./leads_bot.db
#    OWNER_CHAT_ID=...
# ============================================================

# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 1: ИМПОРТЫ
# ─────────────────────────────────────────────────────────────
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    delete,
    select,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, relationship
from telethon import TelegramClient, events
from telethon.errors import (
    ChatAdminRequiredError,
    FloodWaitError,
    SessionPasswordNeededError,
)
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel, Chat

load_dotenv()

# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 2: КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────────────────────
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
TELEGRAM_API_ID: int = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH: str = os.getenv("TELEGRAM_API_HASH", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./leads_bot.db")
OWNER_CHAT_ID: int = int(os.getenv("OWNER_CHAT_ID", "0"))
LEAD_TTL_DAYS: int = 7
SESSION_DIR: str = "./sessions"
os.makedirs(SESSION_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 3: МОДЕЛИ БАЗЫ ДАННЫХ
# ─────────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    username = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class TelegramAccount(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    phone = Column(String(20), unique=True, nullable=False)
    username = Column(String(64), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class GroupPack(Base):
    __tablename__ = "group_packs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(128), nullable=False)
    groups = Column(Text, nullable=False)  # JSON list
    created_at = Column(DateTime, default=datetime.utcnow)


class KeywordPack(Base):
    __tablename__ = "keyword_packs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(128), nullable=False)
    keywords = Column(Text, nullable=False)  # JSON list
    created_at = Column(DateTime, default=datetime.utcnow)


class SearchTask(Base):
    __tablename__ = "search_tasks"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    group_pack_id = Column(Integer, ForeignKey("group_packs.id"), nullable=True)
    keyword_pack_id = Column(Integer, ForeignKey("keyword_packs.id"), nullable=True)
    mode = Column(String(16), default="auto")
    status = Column(String(16), default="active")
    messages_checked = Column(Integer, default=0)
    leads_found = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    stopped_at = Column(DateTime, nullable=True)


class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True)
    search_task_id = Column(Integer, ForeignKey("search_tasks.id"), nullable=False)
    message_text = Column(Text, nullable=False)
    source_link = Column(String(256), nullable=True)
    author_username = Column(String(64), nullable=True)
    author_id = Column(BigInteger, nullable=True)
    message_id = Column(BigInteger, nullable=False)
    chat_id = Column(BigInteger, nullable=False)
    found_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)


class MessageCache(Base):
    __tablename__ = "messages_cache"
    id = Column(Integer, primary_key=True)
    message_id = Column(BigInteger, nullable=False)
    chat_id = Column(BigInteger, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)


class MessageTemplate(Base):
    __tablename__ = "message_templates"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    sphere = Column(String(128), nullable=False)
    template_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 4: БАЗА ДАННЫХ — ДВИЖОК И СЕССИИ
# ─────────────────────────────────────────────────────────────
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _get_user(session: AsyncSession, tg_id: int) -> User | None:
    result = await session.execute(select(User).where(User.telegram_id == tg_id))
    return result.scalar_one_or_none()


async def ensure_user(session: AsyncSession, tg_id: int, username: str | None) -> User:
    user = await _get_user(session, tg_id)
    if not user:
        user = User(telegram_id=tg_id, username=username)
        session.add(user)
        await session.commit()
    return user


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 5: ANTI-DUPLICATE + CLEANUP
# ─────────────────────────────────────────────────────────────
async def is_duplicate(session: AsyncSession, message_id: int, chat_id: int) -> bool:
    result = await session.execute(
        select(MessageCache).where(
            MessageCache.message_id == message_id,
            MessageCache.chat_id == chat_id,
        )
    )
    return result.scalar_one_or_none() is not None


async def mark_processed(session: AsyncSession, message_id: int, chat_id: int):
    session.add(MessageCache(message_id=message_id, chat_id=chat_id))
    await session.commit()


async def delete_expired_leads():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            delete(Lead).where(Lead.expires_at < datetime.utcnow())
        )
        await session.commit()
        if result.rowcount:
            logger.info(f"Cleanup: deleted {result.rowcount} expired leads")


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 6: TELETHON — УПРАВЛЕНИЕ АККАУНТАМИ
# ─────────────────────────────────────────────────────────────
_clients: dict[str, TelegramClient] = {}
_pending_auth: dict[int, dict] = {}   # tg_user_id -> {phone, hash, code}


def _session_path(phone: str) -> str:
    return os.path.join(SESSION_DIR, "session_" + phone.replace("+", "").replace(" ", ""))


async def _create_client(phone: str) -> TelegramClient:
    client = TelegramClient(_session_path(phone), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.connect()
    return client


async def acc_send_code(phone: str) -> tuple[TelegramClient, str]:
    client = await _create_client(phone)
    result = await client.send_code_request(phone)
    _clients[phone] = client
    return client, result.phone_code_hash


async def acc_sign_in(
    phone: str, code: str, phone_code_hash: str, password: str | None = None
) -> TelegramClient:
    client = _clients.get(phone) or await _create_client(phone)
    _clients[phone] = client
    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
    except SessionPasswordNeededError:
        if password:
            await client.sign_in(password=password)
        else:
            raise
    return client


async def get_client(phone: str) -> TelegramClient | None:
    if phone in _clients and _clients[phone].is_connected():
        return _clients[phone]
    sp = _session_path(phone) + ".session"
    if not os.path.exists(sp):
        return None
    client = await _create_client(phone)
    if not await client.is_user_authorized():
        await client.disconnect()
        return None
    _clients[phone] = client
    return client


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 7: ПАРСЕР ГРУПП
# ─────────────────────────────────────────────────────────────
async def search_groups_by_keywords(
    client: TelegramClient,
    keywords: list[str],
    source_type: str = "groups",
    limit: int = 20,
) -> list[dict]:
    found, seen = [], set()
    for kw in keywords:
        try:
            result = await client(SearchRequest(q=kw, limit=limit))
            for chat in result.chats:
                if chat.id in seen:
                    continue
                seen.add(chat.id)
                is_ch = isinstance(chat, Channel)
                is_grp = isinstance(chat, Chat) or (is_ch and chat.megagroup)
                if source_type == "groups" and not is_grp:
                    continue
                if source_type == "chats" and is_grp:
                    continue
                uname = getattr(chat, "username", None)
                found.append({
                    "id": chat.id,
                    "title": chat.title,
                    "username": uname,
                    "link": f"https://t.me/{uname}" if uname else f"ID:{chat.id}",
                    "members": getattr(chat, "participants_count", 0) or 0,
                })
        except FloodWaitError as e:
            logger.warning(f"FloodWait search: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            logger.error(f"search_groups '{kw}': {e}")
    return found


async def get_group_members(
    client: TelegramClient,
    group_id: int,
    user_keywords: list[str] | None = None,
    freshness: str = "all",
    limit: int = 200,
) -> list[dict]:
    members = []
    try:
        entity = await client.get_entity(group_id)
        participants = await client.get_participants(entity, limit=limit)
        cutoff_new = datetime.utcnow() - timedelta(days=30)
        cutoff_old = datetime.utcnow() - timedelta(days=45)
        for user in participants:
            if user.bot:
                continue
            full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
            last_seen = getattr(user.status, "was_online", None)
            if freshness == "new" and last_seen and last_seen < cutoff_new:
                continue
            if freshness == "old" and last_seen and last_seen > cutoff_old:
                continue
            if user_keywords and not any(k.lower() in full_name.lower() for k in user_keywords):
                continue
            members.append({"id": user.id, "username": user.username, "full_name": full_name})
    except ChatAdminRequiredError:
        logger.warning(f"Admin required: {group_id}")
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        logger.error(f"get_members {group_id}: {e}")
    return members


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 8: ДЕТЕКТОР ЛИДОВ + SEARCH MANAGER
# ─────────────────────────────────────────────────────────────
_stop_events: dict[int, asyncio.Event] = {}
_bg_tasks: dict[int, asyncio.Task] = {}


async def notify_owner(bot: Bot, owner_id: int, text: str, source: str, author: str):
    msg = (
        f"🔔 <b>Новый лид!</b>\n\n"
        f"💬 {text}\n\n"
        f"📍 <b>Источник:</b>\n{source}\n\n"
        f"👤 <b>Пользователь:</b>\n{author}"
    )
    try:
        await bot.send_message(owner_id, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"notify_owner: {e}")


async def _monitor(
    client: TelegramClient,
    bot: Bot,
    owner_id: int,
    task_id: int,
    groups: list[str],
    keywords: list[str],
    stop_event: asyncio.Event,
):
    @client.on(events.NewMessage(chats=groups))
    async def handler(event):
        if stop_event.is_set():
            return
        msg = event.message
        text = msg.message or ""
        if not text or not any(kw.lower() in text.lower() for kw in keywords):
            return
        chat = await event.get_chat()
        sender = await event.get_sender()
        chat_uname = getattr(chat, "username", None)
        source = f"https://t.me/{chat_uname}" if chat_uname else f"ID:{event.chat_id}"
        author_uname = getattr(sender, "username", None)
        author_id = sender.id if sender else None
        author = f"@{author_uname}" if author_uname else f"ID:{author_id}"

        async with AsyncSessionLocal() as session:
            if await is_duplicate(session, msg.id, event.chat_id):
                return
            lead = Lead(
                search_task_id=task_id,
                message_text=text[:1000],
                source_link=source,
                author_username=author_uname,
                author_id=author_id,
                message_id=msg.id,
                chat_id=event.chat_id,
                found_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(days=LEAD_TTL_DAYS),
            )
            session.add(lead)
            task_row = await session.get(SearchTask, task_id)
            if task_row:
                task_row.leads_found += 1
                task_row.messages_checked += 1
            await mark_processed(session, msg.id, event.chat_id)
            await session.commit()
        await notify_owner(bot, owner_id, text[:500], source, author)

    logger.info(f"Monitoring task #{task_id}, {len(groups)} groups")
    while not stop_event.is_set():
        await asyncio.sleep(5)
    client.remove_event_handler(handler)
    logger.info(f"Monitoring stopped task #{task_id}")


async def launch_task(bot: Bot, owner_id: int, task_id: int):
    async with AsyncSessionLocal() as session:
        task = await session.get(SearchTask, task_id)
        if not task:
            return
        gpack = await session.get(GroupPack, task.group_pack_id)
        kpack = await session.get(KeywordPack, task.keyword_pack_id)
        groups = json.loads(gpack.groups) if gpack else []
        keywords = json.loads(kpack.keywords) if kpack else []
        result = await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == task.user_id,
                TelegramAccount.is_active == True,
            )
        )
        accounts = result.scalars().all()

    if not accounts or not groups:
        logger.warning(f"Task #{task_id}: no accounts or groups")
        return

    stop_event = asyncio.Event()
    _stop_events[task_id] = stop_event
    chunk = max(1, len(groups) // len(accounts))
    coros = []
    for i, acc in enumerate(accounts):
        sl = groups[i * chunk: (i + 1) * chunk if i < len(accounts) - 1 else len(groups)]
        if not sl:
            continue
        client = await get_client(acc.phone)
        if client:
            coros.append(_monitor(client, bot, owner_id, task_id, sl, keywords, stop_event))
    if coros:
        t = asyncio.create_task(asyncio.gather(*coros))
        _bg_tasks[task_id] = t


async def stop_task(task_id: int):
    if task_id in _stop_events:
        _stop_events[task_id].set()
        del _stop_events[task_id]
    if task_id in _bg_tasks:
        _bg_tasks[task_id].cancel()
        del _bg_tasks[task_id]
    async with AsyncSessionLocal() as session:
        task = await session.get(SearchTask, task_id)
        if task:
            task.status = "stopped"
            task.stopped_at = datetime.utcnow()
            await session.commit()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 9: КЛАВИАТУРЫ
# ─────────────────────────────────────────────────────────────
def kb_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✋ Ручной поиск", callback_data="manual_search"),
        InlineKeyboardButton(text="💻 Автопоиск", callback_data="auto_search"),
    )
    b.row(InlineKeyboardButton(text="🔄 Продолжить работу", callback_data="continue_work"))
    b.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
        InlineKeyboardButton(text="📥 Лиды", callback_data="leads"),
    )
    b.row(
        InlineKeyboardButton(text="💬 Шаблоны", callback_data="templates"),
        InlineKeyboardButton(text="✏️ Пачки", callback_data="edit_packs"),
    )
    return b.as_markup()


def kb_back(cb: str = "main_menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=cb)]
    ])


def kb_back_confirm(back: str, confirm: str, confirm_text: str = "▶️ Продолжить") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=confirm_text, callback_data=confirm)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back)],
    ])


def kb_manual_source() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="👥 Группы", callback_data="msrc_groups"),
        InlineKeyboardButton(text="💬 Чаты", callback_data="msrc_chats"),
    )
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_users_filter() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все пользователи", callback_data="musers_all")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="manual_search")],
    ])


def kb_freshness() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Новые <1 мес", callback_data="fresh_new"),
            InlineKeyboardButton(text="🔵 Старые >1.5 мес", callback_data="fresh_old"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="step_users")],
    ])


def kb_radius() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚡ Абсолютно новые", callback_data="radius_new"),
            InlineKeyboardButton(text="🌐 Все", callback_data="radius_all"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="step_freshness")],
    ])


def kb_manual_summary() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏️ Изменить", callback_data="manual_edit"),
            InlineKeyboardButton(text="▶️ Продолжить", callback_data="manual_continue"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
    ])


def kb_manual_run() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Запустить поиск", callback_data="manual_run"),
            InlineKeyboardButton(text="🔴 Главное меню", callback_data="main_menu"),
        ]
    ])


def kb_auto_menu() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="➕ Создать поиск", callback_data="auto_create"))
    b.row(InlineKeyboardButton(text="📋 Активные поиски", callback_data="auto_active"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_group_packs(packs: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for p in packs:
        b.row(InlineKeyboardButton(text=f"📑 #{p.id} {p.name}", callback_data=f"sgp_{p.id}"))
    b.row(InlineKeyboardButton(text="➕ Новая пачка", callback_data="new_gpack"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="auto_search"))
    return b.as_markup()


def kb_keyword_packs(packs: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for p in packs:
        b.row(InlineKeyboardButton(text=f"🔑 #{p.id} {p.name}", callback_data=f"skp_{p.id}"))
    b.row(InlineKeyboardButton(text="➕ Новая пачка", callback_data="new_kpack"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="auto_create"))
    return b.as_markup()


def kb_accounts(accounts: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for a in accounts:
        label = f"@{a.username}" if a.username else a.phone
        b.row(InlineKeyboardButton(text=label, callback_data=f"acc_{a.id}"))
    b.row(InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_account"))
    b.row(
        InlineKeyboardButton(text="🟢 Запустить", callback_data="auto_run"),
        InlineKeyboardButton(text="🔴 Главное меню", callback_data="main_menu"),
    )
    return b.as_markup()


def kb_stats() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🟢 Активные", callback_data="stats_active"))
    b.row(InlineKeyboardButton(text="✅ Завершённые", callback_data="stats_done"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_pack_actions(pid: int, ptype: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"pedit_{ptype}_{pid}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"pdel_{ptype}_{pid}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="edit_packs")],
    ])


def kb_templates(tpls: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in tpls:
        b.row(InlineKeyboardButton(text=f"💬 {t.sphere}", callback_data=f"tpl_{t.id}"))
    b.row(InlineKeyboardButton(text="➕ Создать", callback_data="tpl_create"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_active_tasks(tasks: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in tasks:
        b.row(InlineKeyboardButton(text=f"🔴 Остановить #{t.id}", callback_data=f"tstop_{t.id}"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="auto_search"))
    return b.as_markup()


def kb_packs_menu() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📑 Пачки групп", callback_data="packs_g"))
    b.row(InlineKeyboardButton(text="🔑 Пачки запросов", callback_data="packs_k"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"))
    return b.as_markup()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 10: FSM STATES
# ─────────────────────────────────────────────────────────────
class ManualSG(StatesGroup):
    source_type = State()
    source_keywords = State()
    user_keywords = State()
    freshness = State()
    radius = State()
    summary = State()
    group_id = State()
    position = State()


class AutoSG(StatesGroup):
    select_gpack = State()
    new_gpack_groups = State()
    new_gpack_name = State()
    select_kpack = State()
    new_kpack_keywords = State()
    new_kpack_name = State()
    select_accounts = State()
    phone = State()
    code = State()
    password = State()


class PackSG(StatesGroup):
    edit_g_groups = State()
    edit_g_name = State()
    edit_k_keywords = State()
    edit_k_name = State()


class TplSG(StatesGroup):
    sphere = State()
    text = State()
    edit_text = State()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 11: ХЭНДЛЕРЫ — START + ГЛАВНОЕ МЕНЮ
# ─────────────────────────────────────────────────────────────
router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, session: AsyncSession):
    await state.clear()
    await ensure_user(session, message.from_user.id, message.from_user.username)
    await message.answer("👋 <b>Добро пожаловать!</b>\n\nВыберите действие:", reply_markup=kb_main())


@router.callback_query(F.data == "main_menu")
async def cb_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=kb_main())
    await callback.answer()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 12: РУЧНОЙ ПОИСК
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "manual_search")
async def manual_start(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(ManualSG.source_type)
    await callback.message.edit_text(
        "✋ <b>Ручной поиск</b>\n\nВыберите источник:", reply_markup=kb_manual_source()
    )
    await callback.answer()


@router.callback_query(F.data.in_({"msrc_groups", "msrc_chats"}))
async def manual_src(callback: CallbackQuery, state: FSMContext):
    await state.update_data(source_type="groups" if callback.data == "msrc_groups" else "chats")
    await state.set_state(ManualSG.source_keywords)
    await callback.message.edit_text(
        "🔑 <b>Ключевые слова источника</b>\n\nКаждое с новой строки:\n\n<i>маникюр\nпедикюр\nсалон</i>",
        reply_markup=kb_back("manual_search"),
    )
    await callback.answer()


@router.message(ManualSG.source_keywords)
async def manual_src_kw(message: Message, state: FSMContext):
    kw = [l.strip() for l in message.text.splitlines() if l.strip()]
    await state.update_data(source_keywords=kw)
    await state.set_state(ManualSG.user_keywords)
    await message.answer(
        "👥 <b>Ключевые слова пользователей</b>\n\nКаждое с новой строки или нажмите «Все»:",
        reply_markup=kb_users_filter(),
    )


@router.callback_query(F.data == "musers_all")
async def manual_users_all(callback: CallbackQuery, state: FSMContext):
    await state.update_data(user_keywords=None)
    await state.set_state(ManualSG.freshness)
    await callback.message.edit_text("⏱ <b>Свежесть активности</b>", reply_markup=kb_freshness())
    await callback.answer()


@router.message(ManualSG.user_keywords)
async def manual_usr_kw(message: Message, state: FSMContext):
    kw = [l.strip() for l in message.text.splitlines() if l.strip()]
    await state.update_data(user_keywords=kw)
    await state.set_state(ManualSG.freshness)
    await message.answer("⏱ <b>Свежесть активности</b>", reply_markup=kb_freshness())


@router.callback_query(F.data == "step_users")
async def back_to_users(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ManualSG.user_keywords)
    await callback.message.edit_text(
        "👥 <b>Ключевые слова пользователей</b>", reply_markup=kb_users_filter()
    )
    await callback.answer()


@router.callback_query(F.data.in_({"fresh_new", "fresh_old"}))
async def manual_fresh(callback: CallbackQuery, state: FSMContext):
    await state.update_data(freshness="new" if callback.data == "fresh_new" else "old")
    await state.set_state(ManualSG.radius)
    await callback.message.edit_text("🔭 <b>Радиус поиска</b>", reply_markup=kb_radius())
    await callback.answer()


@router.callback_query(F.data == "step_freshness")
async def back_to_fresh(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ManualSG.freshness)
    await callback.message.edit_text("⏱ <b>Свежесть активности</b>", reply_markup=kb_freshness())
    await callback.answer()


@router.callback_query(F.data.in_({"radius_new", "radius_all"}))
async def manual_radius_cb(callback: CallbackQuery, state: FSMContext):
    await state.update_data(radius="new" if callback.data == "radius_new" else "all")
    data = await state.get_data()
    await state.set_state(ManualSG.summary)
    src = data.get("source_type", "—")
    src_kw = ", ".join(data.get("source_keywords", []))
    usr_kw = ", ".join(data.get("user_keywords") or ["Все"])
    fresh = "Новые <1 мес" if data.get("freshness") == "new" else "Старые >1.5 мес"
    radius = "Абсолютно новые" if data.get("radius") == "new" else "Все"
    await callback.message.edit_text(
        f"📋 <b>Сводка поиска</b>\n\n"
        f"📍 Источник: <b>{src}</b>\n"
        f"🔑 Источник KW: <b>{src_kw}</b>\n"
        f"👥 Пользователи KW: <b>{usr_kw}</b>\n"
        f"⏱ Свежесть: <b>{fresh}</b>\n"
        f"🔭 Радиус: <b>{radius}</b>",
        reply_markup=kb_manual_summary(),
    )
    await callback.answer()


@router.callback_query(F.data == "manual_edit", ManualSG.summary)
async def manual_edit(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ManualSG.source_type)
    await callback.message.edit_text(
        "✋ <b>Ручной поиск</b>\n\nВыберите источник:", reply_markup=kb_manual_source()
    )
    await callback.answer()


@router.callback_query(F.data == "manual_continue", ManualSG.summary)
async def manual_continue(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    result = await session.execute(
        select(TelegramAccount).where(
            TelegramAccount.user_id == user.id, TelegramAccount.is_active == True
        )
    )
    accounts = result.scalars().all()
    if not accounts:
        await callback.answer("⚠️ Нет аккаунтов! Добавьте через Автопоиск.", show_alert=True)
        return
    client = await get_client(accounts[0].phone)
    if not client:
        await callback.answer("Аккаунт недоступен.", show_alert=True)
        return

    await callback.message.edit_text("🔍 Ищу группы...")
    data = await state.get_data()
    groups = await search_groups_by_keywords(
        client, data.get("source_keywords", []), data.get("source_type", "groups")
    )
    if not groups:
        await callback.message.edit_text("😔 Группы не найдены.", reply_markup=kb_back("manual_search"))
        return

    await state.update_data(found_groups=groups)
    lines = ["📋 <b>Найденные группы:</b>\n"]
    for g in groups:
        lines.append(f"• {g['title']}\n  👥 {g['members']} | ID: <code>{g['id']}</code>\n  🔗 {g['link']}")
    text = "\n".join(lines)[:4000]
    await state.set_state(ManualSG.group_id)
    await callback.message.edit_text(
        text + "\n\n✉️ <b>Отправьте ID группы:</b>",
        reply_markup=kb_back("manual_search"),
    )
    await callback.answer()


@router.message(ManualSG.group_id)
async def manual_group_id(message: Message, state: FSMContext, session: AsyncSession):
    try:
        gid = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите числовой ID.")
        return
    user = await _get_user(session, message.from_user.id)
    result = await session.execute(
        select(TelegramAccount).where(
            TelegramAccount.user_id == user.id, TelegramAccount.is_active == True
        )
    )
    accounts = result.scalars().all()
    if not accounts:
        await message.answer("⚠️ Нет аккаунтов.")
        return
    client = await get_client(accounts[0].phone)
    if not client:
        await message.answer("Аккаунт недоступен.")
        return
    data = await state.get_data()
    await message.answer("⏳ Собираю участников...")
    members = await get_group_members(
        client, gid,
        user_keywords=data.get("user_keywords"),
        freshness=data.get("freshness", "all"),
    )
    if not members:
        await message.answer("😔 Участники не найдены.", reply_markup=kb_back("manual_search"))
        return
    await state.update_data(members=members)
    lines = ["👥 <b>Список пользователей:</b>\n"]
    for i, m in enumerate(members, 1):
        uname = f"@{m['username']}" if m["username"] else f"ID:{m['id']}"
        lines.append(f"{i}. {uname}")
    for chunk in ["\n".join(lines)[i:i+4000] for i in range(0, len("\n".join(lines)), 4000)]:
        await message.answer(chunk)
    await state.set_state(ManualSG.position)
    await message.answer(
        "📌 Введите номер где остановились (или 0):",
        reply_markup=kb_manual_run(),
    )


@router.message(ManualSG.position)
async def manual_position(message: Message, state: FSMContext):
    try:
        pos = int(message.text.strip())
        await state.update_data(current_pos=pos)
        await message.answer(f"✅ Позиция {pos} сохранена.", reply_markup=kb_manual_run())
    except ValueError:
        await message.answer("Введите число.", reply_markup=kb_manual_run())


@router.callback_query(F.data == "manual_run")
async def manual_run(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "✅ Данные сохранены. Используйте автопоиск для мониторинга.",
        reply_markup=kb_back("main_menu"),
    )
    await callback.answer()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 13: АВТОМАТИЧЕСКИЙ ПОИСК
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "auto_search")
async def auto_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("💻 <b>Автоматический поиск</b>", reply_markup=kb_auto_menu())
    await callback.answer()


@router.callback_query(F.data == "auto_create")
async def auto_create(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(select(GroupPack).where(GroupPack.user_id == user.id))
    packs = result.scalars().all()
    await state.set_state(AutoSG.select_gpack)
    await callback.message.edit_text("📑 <b>Выберите пачку групп:</b>", reply_markup=kb_group_packs(packs))
    await callback.answer()


@router.callback_query(F.data.startswith("sgp_"))
async def auto_select_gpack(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    pid = int(callback.data.split("_")[1])
    await state.update_data(group_pack_id=pid)
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(select(KeywordPack).where(KeywordPack.user_id == user.id))
    packs = result.scalars().all()
    await state.set_state(AutoSG.select_kpack)
    await callback.message.edit_text("🔑 <b>Выберите пачку запросов:</b>", reply_markup=kb_keyword_packs(packs))
    await callback.answer()


@router.callback_query(F.data == "new_gpack")
async def auto_new_gpack(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AutoSG.new_gpack_groups)
    await callback.message.edit_text(
        "➕ <b>Новая пачка групп</b>\n\nСписок групп (каждая с новой строки):\n\n<i>@group1\n@group2</i>",
        reply_markup=kb_back("auto_create"),
    )
    await callback.answer()


@router.message(AutoSG.new_gpack_groups)
async def auto_new_gpack_groups(message: Message, state: FSMContext):
    groups = [l.strip() for l in message.text.splitlines() if l.strip()]
    await state.update_data(new_groups=groups)
    await state.set_state(AutoSG.new_gpack_name)
    await message.answer("✏️ Название пачки (сфера):\n<i>Пример: Мастера маникюра</i>", reply_markup=kb_back("new_gpack"))


@router.message(AutoSG.new_gpack_name)
async def auto_new_gpack_name(message: Message, state: FSMContext, session: AsyncSession):
    user = await _get_user(session, message.from_user.id)
    data = await state.get_data()
    pack = GroupPack(user_id=user.id, name=message.text.strip(), groups=json.dumps(data.get("new_groups", [])))
    session.add(pack)
    await session.commit()
    await state.update_data(group_pack_id=pack.id)
    await message.answer(f"✅ Пачка «{pack.name}» создана!")
    result = await session.execute(select(KeywordPack).where(KeywordPack.user_id == user.id))
    packs = result.scalars().all()
    await state.set_state(AutoSG.select_kpack)
    await message.answer("🔑 <b>Выберите пачку запросов:</b>", reply_markup=kb_keyword_packs(packs))


@router.callback_query(F.data.startswith("skp_"))
async def auto_select_kpack(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    pid = int(callback.data.split("_")[1])
    await state.update_data(keyword_pack_id=pid)
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(
        select(TelegramAccount).where(TelegramAccount.user_id == user.id, TelegramAccount.is_active == True)
    )
    accounts = result.scalars().all()
    await state.set_state(AutoSG.select_accounts)
    await callback.message.edit_text("📱 <b>Аккаунты для поиска:</b>", reply_markup=kb_accounts(accounts))
    await callback.answer()


@router.callback_query(F.data == "new_kpack")
async def auto_new_kpack(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AutoSG.new_kpack_keywords)
    await callback.message.edit_text(
        "➕ <b>Новая пачка запросов</b>\n\nЗапросы (каждый с новой строки):\n\n<i>нужен сайт\nищу разработчика</i>",
        reply_markup=kb_back("auto_create"),
    )
    await callback.answer()


@router.message(AutoSG.new_kpack_keywords)
async def auto_new_kpack_kw(message: Message, state: FSMContext):
    kw = [l.strip() for l in message.text.splitlines() if l.strip()]
    await state.update_data(new_keywords=kw)
    await state.set_state(AutoSG.new_kpack_name)
    await message.answer("✏️ Название пачки запросов:", reply_markup=kb_back("new_kpack"))


@router.message(AutoSG.new_kpack_name)
async def auto_new_kpack_name(message: Message, state: FSMContext, session: AsyncSession):
    user = await _get_user(session, message.from_user.id)
    data = await state.get_data()
    pack = KeywordPack(user_id=user.id, name=message.text.strip(), keywords=json.dumps(data.get("new_keywords", [])))
    session.add(pack)
    await session.commit()
    await state.update_data(keyword_pack_id=pack.id)
    await message.answer(f"✅ Пачка запросов «{pack.name}» создана!")
    result = await session.execute(
        select(TelegramAccount).where(TelegramAccount.user_id == user.id, TelegramAccount.is_active == True)
    )
    accounts = result.scalars().all()
    await state.set_state(AutoSG.select_accounts)
    await message.answer("📱 <b>Аккаунты для поиска:</b>", reply_markup=kb_accounts(accounts))


# ─── Добавление аккаунта ─────────────────────────────────────
@router.callback_query(F.data == "add_account")
async def add_acc_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AutoSG.phone)
    await callback.message.edit_text(
        "📱 <b>Добавление аккаунта</b>\n\nВведите номер телефона:\n<i>+79991234567</i>",
        reply_markup=kb_back("auto_create"),
    )
    await callback.answer()


@router.message(AutoSG.phone)
async def acc_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    try:
        _, code_hash = await acc_send_code(phone)
        _pending_auth[message.from_user.id] = {"phone": phone, "hash": code_hash}
        await state.set_state(AutoSG.code)
        await message.answer(f"📨 Код отправлен на {phone}\n\nВведите код:")
    except Exception as e:
        logger.error(f"send_code: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=kb_back("add_account"))


@router.message(AutoSG.code)
async def acc_code(message: Message, state: FSMContext, session: AsyncSession):
    code = message.text.strip()
    auth = _pending_auth.get(message.from_user.id, {})
    phone, code_hash = auth.get("phone"), auth.get("hash")
    if not phone:
        await message.answer("❌ Сессия устарела.", reply_markup=kb_back("add_account"))
        return
    _pending_auth[message.from_user.id]["code"] = code
    try:
        client = await acc_sign_in(phone, code, code_hash)
        me = await client.get_me()
        user = await _get_user(session, message.from_user.id)
        ex = await session.execute(select(TelegramAccount).where(TelegramAccount.phone == phone))
        acc = ex.scalar_one_or_none()
        if acc:
            acc.is_active = True
            acc.username = me.username if me else None
        else:
            acc = TelegramAccount(user_id=user.id, phone=phone, username=me.username if me else None)
            session.add(acc)
        await session.commit()
        _pending_auth.pop(message.from_user.id, None)
        await state.set_state(AutoSG.select_accounts)
        await message.answer(f"✅ Аккаунт @{acc.username or phone} добавлен!", reply_markup=kb_back("auto_create"))
    except SessionPasswordNeededError:
        await state.set_state(AutoSG.password)
        await message.answer("🔐 Введите пароль 2FA:")
    except Exception as e:
        logger.error(f"sign_in: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=kb_back("add_account"))


@router.message(AutoSG.password)
async def acc_password(message: Message, state: FSMContext, session: AsyncSession):
    auth = _pending_auth.get(message.from_user.id, {})
    phone, code_hash, code = auth.get("phone"), auth.get("hash"), auth.get("code", "")
    try:
        client = await acc_sign_in(phone, code, code_hash, password=message.text.strip())
        me = await client.get_me()
        user = await _get_user(session, message.from_user.id)
        acc = TelegramAccount(user_id=user.id, phone=phone, username=me.username if me else None)
        session.add(acc)
        await session.commit()
        _pending_auth.pop(message.from_user.id, None)
        await state.set_state(AutoSG.select_accounts)
        await message.answer(f"✅ Аккаунт @{acc.username or phone} добавлен!", reply_markup=kb_back("auto_create"))
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}", reply_markup=kb_back("add_account"))


# ─── Запуск автопоиска ────────────────────────────────────────
@router.callback_query(F.data == "auto_run")
async def auto_run(callback: CallbackQuery, state: FSMContext, session: AsyncSession, bot: Bot):
    data = await state.get_data()
    gp_id, kp_id = data.get("group_pack_id"), data.get("keyword_pack_id")
    if not gp_id or not kp_id:
        await callback.answer("⚠️ Выберите пачки групп и запросов", show_alert=True)
        return
    user = await _get_user(session, callback.from_user.id)
    task = SearchTask(user_id=user.id, group_pack_id=gp_id, keyword_pack_id=kp_id, mode="auto", status="active")
    session.add(task)
    await session.commit()
    owner = OWNER_CHAT_ID or callback.from_user.id
    await launch_task(bot, owner, task.id)
    await state.clear()
    await callback.message.edit_text(
        f"🟢 <b>Поиск #{task.id} запущен!</b>\n\nПолучите уведомление при нахождении лида.",
        reply_markup=kb_back("main_menu"),
    )
    await callback.answer()


@router.callback_query(F.data == "auto_active")
async def auto_active(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(
        select(SearchTask).where(SearchTask.user_id == user.id, SearchTask.status == "active")
    )
    tasks = result.scalars().all()
    if not tasks:
        await callback.message.edit_text("📋 Нет активных поисков.", reply_markup=kb_back("auto_search"))
        await callback.answer()
        return
    await callback.message.edit_text(
        "📋 <b>Активные поиски</b>\n\nНажмите чтобы остановить:",
        reply_markup=kb_active_tasks(tasks),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tstop_"))
async def task_stop(callback: CallbackQuery):
    tid = int(callback.data.split("_")[1])
    await stop_task(tid)
    await callback.message.edit_text(f"🔴 Поиск #{tid} остановлен.", reply_markup=kb_back("auto_search"))
    await callback.answer()


@router.callback_query(F.data == "continue_work")
async def continue_work(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(
        select(SearchTask).where(SearchTask.user_id == user.id, SearchTask.status == "active")
    )
    tasks = result.scalars().all()
    if not tasks:
        await callback.message.edit_text("ℹ️ Нет активных задач.", reply_markup=kb_back("main_menu"))
        await callback.answer()
        return
    await callback.message.edit_text(
        "🔄 <b>Активные задачи:</b>", reply_markup=kb_active_tasks(tasks)
    )
    await callback.answer()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 14: РЕДАКТИРОВАНИЕ ПАЧЕК
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "edit_packs")
async def edit_packs(callback: CallbackQuery):
    await callback.message.edit_text("✏️ <b>Редактирование пачек</b>", reply_markup=kb_packs_menu())
    await callback.answer()


@router.callback_query(F.data == "packs_g")
async def packs_g_list(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(select(GroupPack).where(GroupPack.user_id == user.id))
    packs = result.scalars().all()
    if not packs:
        await callback.message.edit_text("📋 Нет пачек групп.", reply_markup=kb_back("edit_packs"))
        await callback.answer()
        return
    b = InlineKeyboardBuilder()
    for p in packs:
        b.row(InlineKeyboardButton(text=f"📑 #{p.id} {p.name}", callback_data=f"gpview_{p.id}"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="edit_packs"))
    await callback.message.edit_text("📑 <b>Пачки групп</b>", reply_markup=b.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("gpview_"))
async def gpview(callback: CallbackQuery, session: AsyncSession):
    pid = int(callback.data.split("_")[1])
    pack = await session.get(GroupPack, pid)
    if not pack:
        await callback.answer("Не найдено", show_alert=True)
        return
    groups = json.loads(pack.groups)
    text = f"📑 <b>#{pack.id} {pack.name}</b>\n\n" + "\n".join(groups)
    await callback.message.edit_text(text[:4000], reply_markup=kb_pack_actions(pid, "g"))
    await callback.answer()


@router.callback_query(F.data.startswith("pedit_g_"))
async def gpack_edit_start(callback: CallbackQuery, state: FSMContext):
    pid = int(callback.data.split("_")[-1])
    await state.update_data(edit_pid=pid)
    await state.set_state(PackSG.edit_g_groups)
    await callback.message.edit_text("✏️ Новый список групп (каждая с новой строки):", reply_markup=kb_back("packs_g"))
    await callback.answer()


@router.message(PackSG.edit_g_groups)
async def gpack_edit_groups(message: Message, state: FSMContext):
    groups = [l.strip() for l in message.text.splitlines() if l.strip()]
    await state.update_data(new_groups=groups)
    await state.set_state(PackSG.edit_g_name)
    await message.answer("✏️ Новое название (или '-' оставить прежнее):")


@router.message(PackSG.edit_g_name)
async def gpack_edit_name(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    pack = await session.get(GroupPack, data["edit_pid"])
    if pack:
        pack.groups = json.dumps(data["new_groups"])
        if message.text.strip() != "-":
            pack.name = message.text.strip()
        await session.commit()
    await state.clear()
    await message.answer("✅ Пачка обновлена!", reply_markup=kb_back("packs_g"))


@router.callback_query(F.data.startswith("pdel_g_"))
async def gpack_delete(callback: CallbackQuery, session: AsyncSession):
    pid = int(callback.data.split("_")[-1])
    pack = await session.get(GroupPack, pid)
    if pack:
        await session.delete(pack)
        await session.commit()
    await callback.message.edit_text("🗑 Удалено.", reply_markup=kb_back("packs_g"))
    await callback.answer()


@router.callback_query(F.data == "packs_k")
async def packs_k_list(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(select(KeywordPack).where(KeywordPack.user_id == user.id))
    packs = result.scalars().all()
    if not packs:
        await callback.message.edit_text("🔑 Нет пачек запросов.", reply_markup=kb_back("edit_packs"))
        await callback.answer()
        return
    b = InlineKeyboardBuilder()
    for p in packs:
        b.row(InlineKeyboardButton(text=f"🔑 #{p.id} {p.name}", callback_data=f"kpview_{p.id}"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="edit_packs"))
    await callback.message.edit_text("🔑 <b>Пачки запросов</b>", reply_markup=b.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("kpview_"))
async def kpview(callback: CallbackQuery, session: AsyncSession):
    pid = int(callback.data.split("_")[1])
    pack = await session.get(KeywordPack, pid)
    if not pack:
        await callback.answer("Не найдено", show_alert=True)
        return
    kw = json.loads(pack.keywords)
    text = f"🔑 <b>#{pack.id} {pack.name}</b>\n\n" + "\n".join(kw)
    await callback.message.edit_text(text[:4000], reply_markup=kb_pack_actions(pid, "k"))
    await callback.answer()


@router.callback_query(F.data.startswith("pedit_k_"))
async def kpack_edit_start(callback: CallbackQuery, state: FSMContext):
    pid = int(callback.data.split("_")[-1])
    await state.update_data(edit_pid=pid)
    await state.set_state(PackSG.edit_k_keywords)
    await callback.message.edit_text("✏️ Новый список запросов (каждый с новой строки):", reply_markup=kb_back("packs_k"))
    await callback.answer()


@router.message(PackSG.edit_k_keywords)
async def kpack_edit_kw(message: Message, state: FSMContext):
    kw = [l.strip() for l in message.text.splitlines() if l.strip()]
    await state.update_data(new_keywords=kw)
    await state.set_state(PackSG.edit_k_name)
    await message.answer("✏️ Новое название (или '-' оставить прежнее):")


@router.message(PackSG.edit_k_name)
async def kpack_edit_name(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    pack = await session.get(KeywordPack, data["edit_pid"])
    if pack:
        pack.keywords = json.dumps(data["new_keywords"])
        if message.text.strip() != "-":
            pack.name = message.text.strip()
        await session.commit()
    await state.clear()
    await message.answer("✅ Пачка обновлена!", reply_markup=kb_back("packs_k"))


@router.callback_query(F.data.startswith("pdel_k_"))
async def kpack_delete(callback: CallbackQuery, session: AsyncSession):
    pid = int(callback.data.split("_")[-1])
    pack = await session.get(KeywordPack, pid)
    if pack:
        await session.delete(pack)
        await session.commit()
    await callback.message.edit_text("🗑 Удалено.", reply_markup=kb_back("packs_k"))
    await callback.answer()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 15: ШАБЛОНЫ
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "templates")
async def tpl_menu(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    result = await session.execute(select(MessageTemplate).where(MessageTemplate.user_id == user.id))
    tpls = result.scalars().all()
    await callback.message.edit_text("💬 <b>Шаблоны ответов</b>", reply_markup=kb_templates(tpls))
    await callback.answer()


@router.callback_query(F.data == "tpl_create")
async def tpl_create(callback: CallbackQuery, state: FSMContext):
    await state.set_state(TplSG.sphere)
    await callback.message.edit_text(
        "➕ <b>Новый шаблон</b>\n\nВведите название сферы:\n<i>Мастера маникюра</i>",
        reply_markup=kb_back("templates"),
    )
    await callback.answer()


@router.message(TplSG.sphere)
async def tpl_sphere(message: Message, state: FSMContext):
    await state.update_data(sphere=message.text.strip())
    await state.set_state(TplSG.text)
    await message.answer("📝 Текст шаблона:", reply_markup=kb_back("templates"))


@router.message(TplSG.text)
async def tpl_text(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    user = await _get_user(session, message.from_user.id)
    tpl = MessageTemplate(user_id=user.id, sphere=data["sphere"], template_text=message.text)
    session.add(tpl)
    await session.commit()
    await state.clear()
    await message.answer(f"✅ Шаблон «{data['sphere']}» сохранён!", reply_markup=kb_back("templates"))


@router.callback_query(F.data.startswith("tpl_"))
async def tpl_view(callback: CallbackQuery, session: AsyncSession):
    tid = int(callback.data.split("_")[1])
    tpl = await session.get(MessageTemplate, tid)
    if not tpl:
        await callback.answer("Не найдено", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"tpl_edit_{tid}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"tpl_del_{tid}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="templates")],
    ])
    await callback.message.edit_text(f"💬 <b>{tpl.sphere}</b>\n\n{tpl.template_text}", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("tpl_edit_"))
async def tpl_edit(callback: CallbackQuery, state: FSMContext):
    tid = int(callback.data.split("_")[-1])
    await state.update_data(edit_tpl_id=tid)
    await state.set_state(TplSG.edit_text)
    await callback.message.edit_text("✏️ Новый текст шаблона:", reply_markup=kb_back("templates"))
    await callback.answer()


@router.message(TplSG.edit_text)
async def tpl_edit_text(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    tpl = await session.get(MessageTemplate, data["edit_tpl_id"])
    if tpl:
        tpl.template_text = message.text
        await session.commit()
    await state.clear()
    await message.answer("✅ Шаблон обновлён!", reply_markup=kb_back("templates"))


@router.callback_query(F.data.startswith("tpl_del_"))
async def tpl_delete(callback: CallbackQuery, session: AsyncSession):
    tid = int(callback.data.split("_")[-1])
    tpl = await session.get(MessageTemplate, tid)
    if tpl:
        await session.delete(tpl)
        await session.commit()
    await callback.message.edit_text("🗑 Шаблон удалён.", reply_markup=kb_back("templates"))
    await callback.answer()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 16: СТАТИСТИКА И ЛИДЫ
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "stats")
async def stats_menu(callback: CallbackQuery):
    await callback.message.edit_text("📊 <b>Статистика поиска</b>", reply_markup=kb_stats())
    await callback.answer()


@router.callback_query(F.data.in_({"stats_active", "stats_done"}))
async def stats_list(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    status_filter = "active" if callback.data == "stats_active" else None
    if status_filter:
        q = select(SearchTask).where(SearchTask.user_id == user.id, SearchTask.status == "active")
        title = "🟢 Активные поиски"
    else:
        q = select(SearchTask).where(SearchTask.user_id == user.id, SearchTask.status != "active")
        title = "✅ Завершённые поиски"
    result = await session.execute(q)
    tasks = result.scalars().all()
    if not tasks:
        await callback.message.edit_text(f"{title}\n\nПоисков нет.", reply_markup=kb_back("stats"))
        await callback.answer()
        return
    lines = [f"<b>{title}</b>\n"]
    for t in tasks:
        gp = await session.get(GroupPack, t.group_pack_id) if t.group_pack_id else None
        kp = await session.get(KeywordPack, t.keyword_pack_id) if t.keyword_pack_id else None
        lines.append(
            f"\n📌 <b>Поиск #{t.id}</b> — {gp.name if gp else '—'}\n"
            f"   🔑 {kp.name if kp else '—'}\n"
            f"   📩 Проверено: {t.messages_checked} | 🎯 Лидов: {t.leads_found}\n"
            f"   📅 {t.created_at.strftime('%d.%m.%Y %H:%M')}"
        )
    text = "\n".join(lines)[:4000]
    await callback.message.edit_text(text, reply_markup=kb_back("stats"))
    await callback.answer()


@router.callback_query(F.data == "leads")
async def leads_list(callback: CallbackQuery, session: AsyncSession):
    user = await _get_user(session, callback.from_user.id)
    tasks_r = await session.execute(select(SearchTask.id).where(SearchTask.user_id == user.id))
    task_ids = [r[0] for r in tasks_r.all()]
    if not task_ids:
        await callback.message.edit_text("📥 Лидов нет.", reply_markup=kb_back("main_menu"))
        await callback.answer()
        return
    leads_r = await session.execute(
        select(Lead).where(Lead.search_task_id.in_(task_ids)).order_by(Lead.found_at.desc()).limit(30)
    )
    leads = leads_r.scalars().all()
    if not leads:
        await callback.message.edit_text("📥 Лидов нет.", reply_markup=kb_back("main_menu"))
        await callback.answer()
        return
    lines = ["📥 <b>Найденные лиды (последние 30)</b>\n"]
    for ld in leads:
        author = f"@{ld.author_username}" if ld.author_username else f"ID:{ld.author_id}"
        lines.append(
            f"\n🕐 {ld.found_at.strftime('%d.%m %H:%M')} | {author}\n"
            f"   📍 {ld.source_link or '—'}\n"
            f"   💬 {ld.message_text[:100].replace(chr(10), ' ')}"
        )
    text = "\n".join(lines)[:4000]
    await callback.message.edit_text(text, reply_markup=kb_back("main_menu"))
    await callback.answer()


# ─────────────────────────────────────────────────────────────
#  СЕКЦИЯ 17: MIDDLEWARE + ЗАПУСК
# ─────────────────────────────────────────────────────────────
class DbMiddleware:
    async def __call__(
        self,
        handler: Callable[[Update, dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: dict[str, Any],
    ) -> Any:
        async with AsyncSessionLocal() as session:
            data["session"] = session
            return await handler(event, data)


async def main():
    await init_db()
    logger.info("Database initialized")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.outer_middleware(DbMiddleware())
    dp.include_router(router)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(delete_expired_leads, "interval", hours=6)
    scheduler.start()
    logger.info("Bot started. Polling...")

    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())

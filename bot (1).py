from telethon.sync import TelegramClient

api_id = 20533422       # твой api_id
api_hash = '744e04f35f8aae6803294a5f3989c35b'  # твой api_hash

client = TelegramClient('session', api_id, api_hash)
client.start()
import asyncio
import json
import logging
import os
import random
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker
from telethon import TelegramClient
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User, Channel, Chat
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)

# ===========================================================================
# LOGGING
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ===========================================================================
# CONFIG
# ===========================================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN")
API_ID    = int(os.environ.get("API_ID", "0"))
API_HASH  = os.environ.get("API_HASH", "YOUR_API_HASH")
PHONE     = os.environ.get("PHONE", "+79000000000")

DATABASE_URL          = "sqlite:///bot_data.db"
MIN_GROUPS_PER_SPHERE = 50    # минимум групп в базе для сферы
MIN_CLIENT_BUFFER     = 10    # минимум клиентов в очереди
CLIENT_TTL_DAYS       = 7     # удалять клиентов старше N дней
MESSAGE_LOOKBACK_DAYS = 180   # смотреть сообщения за последние 6 месяцев
GROUP_CLIENT_LIMIT    = 150   # после стольких показов — удалить группу из базы

# ===========================================================================
# DATABASE — MODELS
# ===========================================================================

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class Sphere(Base):
    __tablename__ = "spheres"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name            = Column(String,  nullable=False)
    group_keywords  = Column(Text,    nullable=False)   # ключевые слова для поиска групп
    user_keywords   = Column(Text,    nullable=False, default="[]")  # ключевые слова для фильтрации юзеров

    stages      = relationship("Stage",           back_populates="sphere", cascade="all, delete-orphan")
    groups      = relationship("Group",           back_populates="sphere", cascade="all, delete-orphan")
    clients     = relationship("Client",          back_populates="sphere", cascade="all, delete-orphan")
    round_robin = relationship("GroupRoundRobin", back_populates="sphere", uselist=False, cascade="all, delete-orphan")


class Stage(Base):
    __tablename__ = "stages"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    sphere_id   = Column(Integer, ForeignKey("spheres.id"), nullable=False)
    position    = Column(Integer, nullable=False)
    description = Column(Text,    nullable=False)
    script      = Column(Text,    nullable=False)

    sphere = relationship("Sphere", back_populates="stages")


class Group(Base):
    __tablename__ = "groups"

    id           = Column(Integer,  primary_key=True, autoincrement=True)
    sphere_id    = Column(Integer,  ForeignKey("spheres.id"), nullable=False)
    sphere_name  = Column(String,   nullable=False)
    group_link   = Column(String,   nullable=False)
    parsed       = Column(Boolean,  default=False,           nullable=False)
    shown_count  = Column(Integer,  default=0,               nullable=False)  # сколько юзеров из группы показано
    created_at   = Column(DateTime, default=datetime.utcnow, nullable=False)

    sphere = relationship("Sphere", back_populates="groups")

    __table_args__ = (UniqueConstraint("group_link", name="uq_group_link"),)


class Client(Base):
    __tablename__ = "clients"

    id             = Column(Integer,  primary_key=True, autoincrement=True)
    sphere_id      = Column(Integer,  ForeignKey("spheres.id"), nullable=False)
    username       = Column(String,   nullable=False)
    group_username = Column(String,   nullable=False)
    found_at       = Column(DateTime, default=datetime.utcnow, nullable=False)
    used           = Column(Boolean,  default=False,           nullable=False)

    sphere = relationship("Sphere", back_populates="clients")

    __table_args__ = (UniqueConstraint("sphere_id", "username", name="uq_sphere_username"),)


class GroupRoundRobin(Base):
    __tablename__ = "group_round_robin"

    sphere_id        = Column(Integer, ForeignKey("spheres.id"), primary_key=True)
    last_group_index = Column(Integer, default=0, nullable=False)

    sphere = relationship("Sphere", back_populates="round_robin")


def init_db() -> None:
    Base.metadata.create_all(bind=engine)

# ===========================================================================
# DATABASE — HELPERS
# ===========================================================================

# --- Spheres ---

def db_get_spheres() -> list[Sphere]:
    with SessionLocal() as s:
        rows = s.query(Sphere).all()
        s.expunge_all()
        return rows


def db_get_sphere_by_name(name: str) -> Optional[Sphere]:
    with SessionLocal() as s:
        row = s.query(Sphere).filter(Sphere.name == name).first()
        if row:
            s.expunge(row)
        return row


def db_add_sphere(name: str, group_keywords: str, user_keywords: str) -> int:
    with SessionLocal() as s:
        sphere = Sphere(name=name, group_keywords=group_keywords, user_keywords=user_keywords)
        s.add(sphere)
        s.commit()
        return sphere.id


def db_add_stage(sphere_id: int, description: str, script: str) -> None:
    with SessionLocal() as s:
        count = s.query(Stage).filter(Stage.sphere_id == sphere_id).count()
        s.add(Stage(sphere_id=sphere_id, position=count + 1, description=description, script=script))
        s.commit()


def db_get_stages(sphere_id: int) -> list[Stage]:
    with SessionLocal() as s:
        rows = s.query(Stage).filter(Stage.sphere_id == sphere_id).order_by(Stage.position).all()
        s.expunge_all()
        return rows


def db_get_user_keywords(sphere_id: int) -> list[str]:
    with SessionLocal() as s:
        row = s.query(Sphere.user_keywords).filter(Sphere.id == sphere_id).first()
        if row:
            return json.loads(row[0] or "[]")
        return []


# --- Groups ---

def db_count_groups(sphere_id: int) -> int:
    with SessionLocal() as s:
        return s.query(Group).filter(Group.sphere_id == sphere_id).count()


def db_save_groups(sphere_id: int, sphere_name: str, links: list[str]) -> int:
    saved = 0
    with SessionLocal() as s:
        for link in links:
            s.add(Group(sphere_id=sphere_id, sphere_name=sphere_name, group_link=link, parsed=False, shown_count=0))
            try:
                s.flush()
                saved += 1
            except IntegrityError:
                s.rollback()
        s.commit()
    return saved


def db_get_all_group_usernames(sphere_id: int) -> list[str]:
    with SessionLocal() as s:
        rows = s.query(Group.group_link).filter(Group.sphere_id == sphere_id).all()
    result = []
    for (link,) in rows:
        username = link.rstrip("/").split("/")[-1]
        if username:
            result.append(username)
    return result


def db_mark_group_parsed(group_link: str) -> None:
    with SessionLocal() as s:
        group = s.query(Group).filter(Group.group_link == group_link).first()
        if group:
            group.parsed = True
            s.commit()


def db_increment_group_shown(group_username: str, sphere_id: int) -> None:
    """Увеличить счётчик показанных клиентов из группы. Удалить группу если лимит достигнут."""
    group_link = f"https://t.me/{group_username}"
    with SessionLocal() as s:
        group = s.query(Group).filter(
            Group.group_link == group_link,
            Group.sphere_id == sphere_id,
        ).first()
        if group:
            group.shown_count = (group.shown_count or 0) + 1
            if group.shown_count >= GROUP_CLIENT_LIMIT:
                logger.info("Group '%s' reached limit %d, removing.", group_username, GROUP_CLIENT_LIMIT)
                s.delete(group)
            s.commit()


# --- Clients ---

def db_cleanup_old_clients() -> None:
    cutoff = datetime.utcnow() - timedelta(days=CLIENT_TTL_DAYS)
    with SessionLocal() as s:
        s.query(Client).filter(Client.found_at < cutoff).delete()
        s.commit()


def db_count_available_clients(sphere_id: int) -> int:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        return s.query(Client).filter(
            Client.sphere_id == sphere_id, Client.used == False
        ).count()


def db_get_random_client(sphere_id: int) -> Optional[Client]:
    """Выбрать случайного клиента из разных групп."""
    db_cleanup_old_clients()
    with SessionLocal() as s:
        # Получаем все доступные группы у которых есть клиенты
        groups_with_clients = (
            s.query(Client.group_username)
            .filter(Client.sphere_id == sphere_id, Client.used == False)
            .distinct()
            .all()
        )
        if not groups_with_clients:
            return None

        # Случайно выбираем группу
        group_username = random.choice(groups_with_clients)[0]

        # Берём первого неиспользованного клиента из этой группы
        row = (
            s.query(Client)
            .filter(
                Client.sphere_id == sphere_id,
                Client.used == False,
                Client.group_username == group_username,
            )
            .order_by(Client.id)
            .first()
        )
        if row:
            s.expunge(row)
        return row


def db_mark_client_used(client_id: int) -> Optional[str]:
    """Пометить клиента использованным. Вернуть group_username для счётчика."""
    with SessionLocal() as s:
        c = s.query(Client).filter(Client.id == client_id).first()
        if c:
            c.used = True
            group = c.group_username
            s.commit()
            return group
        return None


def db_is_username_known(sphere_id: int, username: str) -> bool:
    with SessionLocal() as s:
        return s.query(Client).filter(
            Client.sphere_id == sphere_id, Client.username == username
        ).first() is not None


def db_save_clients(sphere_id: int, group_username: str, usernames: list[str]) -> int:
    saved = 0
    with SessionLocal() as s:
        for uname in usernames:
            s.add(Client(sphere_id=sphere_id, username=uname, group_username=group_username))
            try:
                s.flush()
                saved += 1
            except IntegrityError:
                s.rollback()
        s.commit()
    return saved


# --- Round-robin ---

def db_get_round_robin_index(sphere_id: int) -> int:
    with SessionLocal() as s:
        row = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sphere_id).first()
        return row.last_group_index if row else 0


def db_set_round_robin_index(sphere_id: int, idx: int) -> None:
    with SessionLocal() as s:
        row = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sphere_id).first()
        if row:
            row.last_group_index = idx
        else:
            s.add(GroupRoundRobin(sphere_id=sphere_id, last_group_index=idx))
        s.commit()


# ===========================================================================
# TELETHON — клиент (единственный экземпляр)
# ===========================================================================

telethon_client: Optional[TelegramClient] = None


async def ensure_telethon() -> TelegramClient:
    global telethon_client
    if telethon_client is None or not telethon_client.is_connected():
        telethon_client = TelegramClient("session_bot", API_ID, API_HASH)
        await telethon_client.start(phone=PHONE)
    return telethon_client


# ===========================================================================
# GROUP FINDER — через Telethon SearchRequest (contacts.search)
# ===========================================================================

def _is_group_or_megagroup(chat) -> bool:
    """Вернуть True если объект — группа или мегагруппа, но не канал и не бот."""
    if isinstance(chat, Chat):
        return True
    if isinstance(chat, Channel):
        return bool(getattr(chat, "megagroup", False))
    return False


async def _search_groups_by_keyword(
    keyword: str,
    limit: int = 100,
) -> list[str]:
    """
    Ищет группы через Telegram API (contacts.Search).
    Возвращает список username'ов в формате 'https://t.me/xxx'.
    """
    client = await ensure_telethon()
    found: list[str] = []

    try:
        result = await client(SearchRequest(q=keyword, limit=limit))
        for chat in result.chats:
            if not _is_group_or_megagroup(chat):
                continue
            username = getattr(chat, "username", None)
            if username:
                found.append(f"https://t.me/{username.lower()}")
        logger.info(
            "contacts.Search '%s' → %d chats total, %d groups with username",
            keyword, len(result.chats), len(found),
        )
    except Exception as exc:
        logger.warning("SearchRequest failed for '%s': %s", keyword, exc)

    return found


async def find_groups_for_sphere(
    sphere_id: int,
    sphere_name: str,
    keywords: list[str],
    min_groups: int = MIN_GROUPS_PER_SPHERE,
    status_callback=None,
) -> int:
    current = db_count_groups(sphere_id)
    if current >= min_groups:
        logger.info("Sphere '%s' already has %d groups, skipping search.", sphere_name, current)
        return current

    all_links: list[str] = []

    for keyword in keywords:
        if db_count_groups(sphere_id) + len(all_links) >= min_groups:
            break
        logger.info("Searching groups via Telegram API for keyword '%s'", keyword)
        try:
            links = await _search_groups_by_keyword(keyword, limit=100)
            new_links = [l for l in links if l not in all_links]
            all_links.extend(new_links)
            if status_callback:
                await status_callback(
                    f"🔍 «{keyword}»: найдено {len(new_links)} групп "
                    f"(всего накоплено: {len(all_links)})"
                )
            # небольшая пауза между запросами чтобы не флудить
            await asyncio.sleep(1.5)
        except Exception as exc:
            logger.error("Error searching groups for keyword '%s': %s", keyword, exc)

    unique_links = list(dict.fromkeys(all_links))
    saved = db_save_groups(sphere_id, sphere_name, unique_links)
    total = db_count_groups(sphere_id)

    logger.info(
        "Sphere '%s': saved %d new groups, total in DB: %d",
        sphere_name, saved, total,
    )
    if total < min_groups:
        logger.warning(
            "Sphere '%s': only %d groups found (target %d). Add more keywords.",
            sphere_name, total, min_groups,
        )
    return total


# ===========================================================================
# TELETHON — CLIENT COLLECTION
# ===========================================================================

def _user_matches_keywords(
    username: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
    bio: Optional[str],
    pinned_text: Optional[str],
    keywords: list[str],
) -> bool:
    """Вернуть True если хотя бы одно ключевое слово найдено в полях пользователя."""
    if not keywords:
        return True  # нет фильтра — пропускаем всех
    haystack = " ".join(filter(None, [username, first_name, last_name, bio, pinned_text])).lower()
    return any(kw.lower() in haystack for kw in keywords)


async def _get_user_bio(client: TelegramClient, user_id: int) -> str:
    """Получить bio пользователя через GetFullUserRequest."""
    try:
        full = await client(GetFullUserRequest(user_id))
        return full.full_user.about or ""
    except Exception:
        return ""


async def collect_clients_from_group(
    group_username: str,
    sphere_id: int,
    user_keywords: list[str],
    limit: int = 500,
) -> int:
    client = await ensure_telethon()
    lookback   = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids:   set[int]  = set()
    collected:  list[str] = []

    try:
        entity = await client.get_entity(group_username)

        # Собираем сообщения пачками по 100
        offset_id = 0
        total_fetched = 0

        while total_fetched < limit:
            batch_size = min(100, limit - total_fetched)
            history = await client(
                GetHistoryRequest(
                    peer=entity,
                    limit=batch_size,
                    offset_date=None,
                    offset_id=offset_id,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0,
                )
            )
            if not history.messages:
                break

            stop_early = False
            for msg in history.messages:
                if not msg.date:
                    continue
                msg_date = msg.date.replace(tzinfo=None)
                if msg_date < lookback:
                    stop_early = True
                    break
                if not getattr(msg, "from_id", None):
                    continue
                user_id = getattr(msg.from_id, "user_id", None)
                if not user_id or user_id in seen_ids:
                    continue
                seen_ids.add(user_id)

                try:
                    user = await client.get_entity(user_id)
                    if not isinstance(user, User) or not user.username:
                        continue
                    uname = user.username.lower()
                    if db_is_username_known(sphere_id, uname):
                        continue

                    # Фильтрация по ключевым словам пользователя
                    bio = ""
                    if user_keywords:
                        bio = await _get_user_bio(client, user_id)

                    if _user_matches_keywords(
                        username   = user.username,
                        first_name = user.first_name,
                        last_name  = user.last_name,
                        bio        = bio,
                        pinned_text= None,
                        keywords   = user_keywords,
                    ):
                        collected.append(uname)

                except Exception:
                    pass

            total_fetched += len(history.messages)
            offset_id      = history.messages[-1].id
            if stop_early or len(history.messages) < batch_size:
                break
            await asyncio.sleep(0.5)  # небольшая пауза между пачками

    except Exception as exc:
        logger.warning("Error collecting from group '%s': %s", group_username, exc)

    saved = db_save_clients(sphere_id, group_username, collected)
    db_mark_group_parsed(f"https://t.me/{group_username}")
    logger.info("Collected %d new clients from '%s' (filtered by keywords)", saved, group_username)
    return saved


async def fill_client_buffer(sphere_id: int) -> None:
    """Подгружает клиентов пока буфер не достигнет MIN_CLIENT_BUFFER."""
    if db_count_available_clients(sphere_id) >= MIN_CLIENT_BUFFER:
        return

    # Если групп меньше MIN_GROUPS_PER_SPHERE — сначала ищем новые
    if db_count_groups(sphere_id) < MIN_GROUPS_PER_SPHERE:
        with SessionLocal() as s:
            sphere = s.query(Sphere).filter(Sphere.id == sphere_id).first()
            if sphere:
                keywords = json.loads(sphere.group_keywords or "[]")
                name     = sphere.name
        await find_groups_for_sphere(
            sphere_id   = sphere_id,
            sphere_name = name,
            keywords    = keywords,
        )

    groups = db_get_all_group_usernames(sphere_id)
    if not groups:
        logger.warning("No groups in DB for sphere_id=%d", sphere_id)
        return

    user_keywords = db_get_user_keywords(sphere_id)

    idx      = db_get_round_robin_index(sphere_id)
    attempts = 0

    while db_count_available_clients(sphere_id) < MIN_CLIENT_BUFFER and attempts < len(groups):
        group = groups[idx % len(groups)]
        await collect_clients_from_group(group, sphere_id, user_keywords)
        idx      = (idx + 1) % len(groups)
        attempts += 1

    db_set_round_robin_index(sphere_id, idx)


# ===========================================================================
# BOT STATES
# ===========================================================================

(
    STATE_MAIN_MENU,
    STATE_SPHERE_MENU,
    STATE_ADD_SPHERE_NAME,
    STATE_AFTER_STAGE,
    STATE_ADD_STAGE_DESC,
    STATE_ADD_STAGE_SCRIPT,
    STATE_ADD_GROUP_KEYWORDS,   # ввод ключевых слов для поиска групп
    STATE_ADD_USER_KEYWORDS,    # ввод ключевых слов для фильтрации юзеров (НОВЫЙ)
    STATE_SHOW_CLIENT,
) = range(9)

# ===========================================================================
# KEYBOARDS
# ===========================================================================

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🖐🏻 Ручной поиск")]],
        resize_keyboard=True,
    )


def sphere_menu_keyboard() -> ReplyKeyboardMarkup:
    spheres = db_get_spheres()
    buttons = [[KeyboardButton(s.name)] for s in spheres]
    buttons.append([KeyboardButton("➕ Добавить сферу")])
    buttons.append([KeyboardButton("Главное меню")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def client_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("➡️ Следующий"), KeyboardButton("Главное меню")]],
        resize_keyboard=True,
    )


def after_stage_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("+Этап"), KeyboardButton("Продолжить")]],
        resize_keyboard=True,
    )


# ===========================================================================
# HANDLERS
# ===========================================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Добро пожаловать! Выберите действие:",
        reply_markup=main_menu_keyboard(),
    )
    return STATE_MAIN_MENU


async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "🖐🏻 Ручной поиск":
        await update.message.reply_text("Выберите сферу:", reply_markup=sphere_menu_keyboard())
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
            "Введите название сферы:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_ADD_SPHERE_NAME

    sphere = db_get_sphere_by_name(text)
    if sphere:
        context.user_data["current_sphere_id"]   = sphere.id
        context.user_data["current_sphere_name"] = sphere.name
        asyncio.create_task(fill_client_buffer(sphere.id))
        return await show_next_client(update, context)

    await update.message.reply_text("Сфера не найдена.", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU


async def show_next_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sphere_id:   Optional[int] = context.user_data.get("current_sphere_id")
    sphere_name: Optional[str] = context.user_data.get("current_sphere_name")

    if not sphere_id:
        await update.message.reply_text(
            "Ошибка. Вернитесь в главное меню.",
            reply_markup=main_menu_keyboard(),
        )
        return STATE_MAIN_MENU

    client_row = db_get_random_client(sphere_id)
    if not client_row:
        await update.message.reply_text(
            "⏳ Клиентов пока нет. Идёт поиск — попробуйте чуть позже.",
            reply_markup=client_keyboard(),
        )
        asyncio.create_task(fill_client_buffer(sphere_id))
        return STATE_SHOW_CLIENT

    context.user_data["current_client_id"] = client_row.id
    stages = db_get_stages(sphere_id)

    lines = [f"<b>@{client_row.username}</b>\n", f"<b>{sphere_name}</b>"]
    for i, stage in enumerate(stages, 1):
        lines.append(f"\n<b>{i} этап — {stage.description}</b>")
        lines.append(f"<blockquote>{stage.script}</blockquote>")

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
        if cid := context.user_data.get("current_client_id"):
            sphere_id    = context.user_data.get("current_sphere_id")
            group_uname  = db_mark_client_used(cid)
            if group_uname and sphere_id:
                db_increment_group_shown(group_uname, sphere_id)
        return await show_next_client(update, context)

    if text == "Главное меню":
        if cid := context.user_data.get("current_client_id"):
            sphere_id   = context.user_data.get("current_sphere_id")
            group_uname = db_mark_client_used(cid)
            if group_uname and sphere_id:
                db_increment_group_shown(group_uname, sphere_id)
        for key in ("current_sphere_id", "current_sphere_name", "current_client_id"):
            context.user_data.pop(key, None)
        await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU

    return STATE_SHOW_CLIENT


# --- Добавление сферы ---

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
            "Введите пояснение этапа:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_ADD_STAGE_DESC
    if text == "Продолжить":
        await update.message.reply_text(
            "📌 Шаг 1 из 2: Введите ключевые слова для поиска групп/чатов\n"
            "(каждое слово с новой строки):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_ADD_GROUP_KEYWORDS
    return STATE_AFTER_STAGE


async def handle_add_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_sphere"]["current_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:")
    return STATE_ADD_STAGE_SCRIPT


async def handle_add_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip()
    desc   = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text(
        "✅ Этап сохранён. Добавьте ещё или продолжите:",
        reply_markup=after_stage_keyboard(),
    )
    return STATE_AFTER_STAGE


async def handle_add_group_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 1: ключевые слова для поиска групп."""
    keywords = [kw.strip() for kw in update.message.text.strip().splitlines() if kw.strip()]
    if not keywords:
        await update.message.reply_text("Введите хотя бы одно ключевое слово:")
        return STATE_ADD_GROUP_KEYWORDS

    context.user_data["new_sphere"]["group_keywords"] = keywords
    await update.message.reply_text(
        "📌 Шаг 2 из 2: Введите ключевые слова для фильтрации пользователей\n"
        "(ищутся в username, имени, bio, закреплённых сообщениях)\n\n"
        "Пример:\n"
        "маникюр\n"
        "nail master\n"
        "мастер ногтей\n\n"
        "Если фильтрация не нужна — напишите <b>нет</b>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_ADD_USER_KEYWORDS


async def handle_add_user_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 2: ключевые слова для фильтрации пользователей. Создаём сферу."""
    raw = update.message.text.strip()
    if raw.lower() in ("нет", "no", "-", ""):
        user_keywords: list[str] = []
    else:
        user_keywords = [kw.strip() for kw in raw.splitlines() if kw.strip()]

    new_sphere    = context.user_data.get("new_sphere", {})
    name: str     = new_sphere.get("name", "")
    stages        = new_sphere.get("stages", [])
    group_kws     = new_sphere.get("group_keywords", [])

    sphere_id = db_add_sphere(
        name           = name,
        group_keywords = json.dumps(group_kws,  ensure_ascii=False),
        user_keywords  = json.dumps(user_keywords, ensure_ascii=False),
    )
    for stage in stages:
        db_add_stage(sphere_id, stage["desc"], stage["script"])

    kw_info = f"Ключевых слов для фильтрации: {len(user_keywords)}" if user_keywords else "Фильтрация пользователей: отключена"
    await update.message.reply_text(
        f"💾 Сфера «{name}» сохранена!\n"
        f"Ключевых слов для групп: {len(group_kws)}\n"
        f"{kw_info}\n\n"
        f"🔍 Ищу группы..."
    )

    async def status_cb(msg: str) -> None:
        try:
            await update.message.reply_text(msg)
        except Exception:
            pass

    try:
        total = await find_groups_for_sphere(
            sphere_id    = sphere_id,
            sphere_name  = name,
            keywords     = group_kws,
            status_callback = status_cb,
        )
        if total > 0:
            await update.message.reply_text(
                f"✅ Найдено групп: {total}. Начинаю сбор клиентов..."
            )
            asyncio.create_task(fill_client_buffer(sphere_id))
        else:
            await update.message.reply_text(
                "⚠️ Группы не найдены. Попробуйте другие ключевые слова."
            )
    except Exception as exc:
        logger.error("Error in find_groups_for_sphere: %s", exc)
        await update.message.reply_text("❌ Ошибка при поиске групп. Проверьте логи.")

    context.user_data.pop("new_sphere", None)
    await update.message.reply_text("Выберите сферу:", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU


async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Используйте кнопки меню.",
        reply_markup=main_menu_keyboard(),
    )
    return STATE_MAIN_MENU


# ===========================================================================
# ENTRY POINT
# ===========================================================================

def main() -> None:
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_MAIN_MENU:         [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)],
            STATE_SPHERE_MENU:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sphere_menu)],
            STATE_ADD_SPHERE_NAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_sphere_name)],
            STATE_AFTER_STAGE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_after_stage)],
            STATE_ADD_STAGE_DESC:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_desc)],
            STATE_ADD_STAGE_SCRIPT:  [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_script)],
            STATE_ADD_GROUP_KEYWORDS:[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_group_keywords)],
            STATE_ADD_USER_KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_user_keywords)],
            STATE_SHOW_CLIENT:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_show_client)],
        },
        fallbacks=[CommandHandler("start", cmd_start), MessageHandler(filters.ALL, fallback)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()

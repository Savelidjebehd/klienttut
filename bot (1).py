import asyncio
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
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

DATABASE_URL        = "sqlite:///bot_data.db"
MIN_GROUPS_PER_SPHERE = 50
MIN_CLIENT_BUFFER     = 10
CLIENT_TTL_DAYS       = 7
MESSAGE_LOOKBACK_DAYS = 180

# ===========================================================================
# DATABASE — MODELS
# ===========================================================================

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class Sphere(Base):
    __tablename__ = "spheres"

    id       = Column(Integer, primary_key=True, autoincrement=True)
    name     = Column(String,  nullable=False)
    keywords = Column(Text,    nullable=False)

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

    id          = Column(Integer,  primary_key=True, autoincrement=True)
    sphere_id   = Column(Integer,  ForeignKey("spheres.id"), nullable=False)
    sphere_name = Column(String,   nullable=False)
    group_link  = Column(String,   nullable=False)
    parsed      = Column(Boolean,  default=False,           nullable=False)
    created_at  = Column(DateTime, default=datetime.utcnow, nullable=False)

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


def db_add_sphere(name: str, keywords: str) -> int:
    with SessionLocal() as s:
        sphere = Sphere(name=name, keywords=keywords)
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


# --- Groups ---

def db_count_groups(sphere_id: int) -> int:
    with SessionLocal() as s:
        return s.query(Group).filter(Group.sphere_id == sphere_id).count()


def db_save_groups(sphere_id: int, sphere_name: str, links: list[str]) -> int:
    saved = 0
    with SessionLocal() as s:
        for link in links:
            s.add(Group(sphere_id=sphere_id, sphere_name=sphere_name, group_link=link, parsed=False))
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


# --- Clients ---

def db_cleanup_old_clients() -> None:
    cutoff = datetime.utcnow() - timedelta(days=CLIENT_TTL_DAYS)
    with SessionLocal() as s:
        s.query(Client).filter(Client.found_at < cutoff).delete()
        s.commit()


def db_count_available_clients(sphere_id: int) -> int:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        return s.query(Client).filter(Client.sphere_id == sphere_id, Client.used == False).count()


def db_get_next_client(sphere_id: int) -> Optional[Client]:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        row = (
            s.query(Client)
            .filter(Client.sphere_id == sphere_id, Client.used == False)
            .order_by(Client.id)
            .first()
        )
        if row:
            s.expunge(row)
        return row


def db_mark_client_used(client_id: int) -> None:
    with SessionLocal() as s:
        c = s.query(Client).filter(Client.id == client_id).first()
        if c:
            c.used = True
            s.commit()


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
# PARSER SOURCES
# ===========================================================================

TELEGRAM_LINK_RE = re.compile(r"https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})")

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}
_REQUEST_TIMEOUT  = 15
_RATE_LIMIT_DELAY = 2.0

_BLACKLIST_EXACT = {
    "joinchat", "share", "iv", "s", "addstickers",
    "addtheme", "login", "confirmphone", "setlanguage",
}
_BLACKLIST_SUFFIXES = ("bot", "_bot")

_SEED_AGGREGATORS = [
    "groups_tg", "tgchats", "ru_groups", "best_tg_chats", "tgchat_search",
]


def _http_get(url: str, params: Optional[dict] = None) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=_HTTP_HEADERS, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        logger.warning("HTTP GET %s failed: %s", url, exc)
        return None


def _is_valid_group_username(username: str) -> bool:
    u = username.lower()
    if u in _BLACKLIST_EXACT:
        return False
    if any(u.endswith(s) for s in _BLACKLIST_SUFFIXES):
        return False
    if len(u) < 4:
        return False
    return True


def _extract_tme_links(html: str) -> list[str]:
    links: list[str] = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("a", href=True):
        m = TELEGRAM_LINK_RE.match(tag["href"])
        if m:
            u = m.group(1).lower()
            if _is_valid_group_username(u):
                links.append(f"https://t.me/{u}")
    for u in TELEGRAM_LINK_RE.findall(html):
        u = u.lower()
        if _is_valid_group_username(u):
            candidate = f"https://t.me/{u}"
            if candidate not in links:
                links.append(candidate)
    return list(dict.fromkeys(links))


def _parse_tlgrm(keyword: str) -> list[str]:
    links: list[str] = []
    for page in range(1, 4):
        resp = _http_get("https://tlgrm.ru/channels", params={"q": keyword, "page": page})
        if resp is None:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("a.channel-card__link, a[href*='t.me/'], a[href*='/channels/']")
        for card in cards:
            href: str = card.get("href", "")
            if "t.me/" in href:
                m = TELEGRAM_LINK_RE.search(href)
                if m:
                    u = m.group(1).lower()
                    if _is_valid_group_username(u):
                        links.append(f"https://t.me/{u}")
            elif href.startswith("/channels/"):
                parts = href.strip("/").split("/")
                if len(parts) >= 2:
                    u = parts[-1].lower()
                    if _is_valid_group_username(u):
                        links.append(f"https://t.me/{u}")
        links.extend(_extract_tme_links(resp.text))
        time.sleep(_RATE_LIMIT_DELAY)
        if not cards:
            break
    result = list(dict.fromkeys(links))
    logger.info("tlgrm.ru → %d links for '%s'", len(result), keyword)
    return result


def _parse_tgstat(keyword: str) -> list[str]:
    links: list[str] = []
    for page in range(1, 5):
        resp = _http_get("https://tgstat.ru/search", params={"q": keyword, "page": page})
        if resp is None:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.select("a[href]"):
            href: str = tag["href"]
            m = TELEGRAM_LINK_RE.search(href)
            if m:
                u = m.group(1).lower()
                if _is_valid_group_username(u):
                    links.append(f"https://t.me/{u}")
            elif href.startswith("/channel/") or href.startswith("/chat/"):
                parts = href.strip("/").split("/")
                if len(parts) >= 2:
                    u = parts[-1].lstrip("@").lower()
                    if _is_valid_group_username(u):
                        links.append(f"https://t.me/{u}")
        links.extend(_extract_tme_links(resp.text))
        time.sleep(_RATE_LIMIT_DELAY)
        if not soup.select_one("a[rel='next'], .pagination .next"):
            break
    result = list(dict.fromkeys(links))
    logger.info("tgstat.ru → %d links for '%s'", len(result), keyword)
    return result


def _parse_tme_aggregators(keyword: str) -> list[str]:
    links: list[str] = []
    kw_lower = keyword.lower()
    for seed in _SEED_AGGREGATORS:
        resp = _http_get(f"https://t.me/s/{seed}")
        if resp is None:
            continue
        for link in _extract_tme_links(resp.text):
            if kw_lower in link.lower():
                links.append(link)
        time.sleep(_RATE_LIMIT_DELAY)
    result = list(dict.fromkeys(links))
    logger.info("t.me/s/ → %d links for '%s'", len(result), keyword)
    return result


def scrape_all_sources(keyword: str) -> list[str]:
    logger.info("Scraping sources for keyword: '%s'", keyword)
    results: list[str] = []
    results.extend(_parse_tlgrm(keyword))
    results.extend(_parse_tgstat(keyword))
    results.extend(_parse_tme_aggregators(keyword))
    unique = list(dict.fromkeys(results))
    logger.info("Total unique links for '%s': %d", keyword, len(unique))
    return unique


# ===========================================================================
# GROUP FINDER
# ===========================================================================

_executor = ThreadPoolExecutor(max_workers=2)


async def _scrape_async(keyword: str) -> list[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, scrape_all_sources, keyword)


async def find_groups_for_sphere(
    sphere_id: int,
    sphere_name: str,
    keywords: list[str],
    min_groups: int = MIN_GROUPS_PER_SPHERE,
    status_callback=None,
) -> int:
    if db_count_groups(sphere_id) >= min_groups:
        logger.info("Sphere '%s' already has enough groups, skipping scrape.", sphere_name)
        return db_count_groups(sphere_id)

    all_links: list[str] = []
    for keyword in keywords:
        if db_count_groups(sphere_id) + len(all_links) >= min_groups:
            break
        logger.info("Searching groups for keyword '%s' (sphere '%s')", keyword, sphere_name)
        try:
            links = await _scrape_async(keyword)
            all_links.extend(links)
            if status_callback:
                await status_callback(f"🔍 Ключевое слово «{keyword}»: найдено {len(links)} ссылок")
        except Exception as exc:
            logger.error("Error scraping keyword '%s': %s", keyword, exc)

    unique_links = list(dict.fromkeys(all_links))
    saved = db_save_groups(sphere_id, sphere_name, unique_links)
    total = db_count_groups(sphere_id)

    logger.info("Sphere '%s': saved %d new groups, total in DB: %d", sphere_name, saved, total)
    if total < min_groups:
        logger.warning(
            "Sphere '%s': only %d groups found (target %d). Add more keywords.",
            sphere_name, total, min_groups,
        )
    return total


# ===========================================================================
# TELETHON — CLIENT COLLECTION
# ===========================================================================

telethon_client: Optional[TelegramClient] = None


async def ensure_telethon() -> TelegramClient:
    global telethon_client
    if telethon_client is None or not telethon_client.is_connected():
        telethon_client = TelegramClient("session_bot", API_ID, API_HASH)
        await telethon_client.start(phone=PHONE)
    return telethon_client


async def collect_clients_from_group(group_username: str, sphere_id: int, limit: int = 200) -> int:
    client = await ensure_telethon()
    lookback = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set[int] = set()
    collected: list[str] = []

    try:
        entity = await client.get_entity(group_username)
        history = await client(
            GetHistoryRequest(
                peer=entity, limit=limit,
                offset_date=None, offset_id=0,
                max_id=0, min_id=0, add_offset=0, hash=0,
            )
        )
        for msg in history.messages:
            if not msg.date:
                continue
            if msg.date.replace(tzinfo=None) < lookback:
                continue
            if not getattr(msg, "from_id", None):
                continue
            user_id = getattr(msg.from_id, "user_id", None)
            if not user_id or user_id in seen_ids:
                continue
            seen_ids.add(user_id)
            try:
                user = await client.get_entity(user_id)
                if isinstance(user, User) and user.username:
                    uname = user.username.lower()
                    if not db_is_username_known(sphere_id, uname):
                        collected.append(uname)
            except Exception:
                pass
    except Exception as exc:
        logger.warning("Error collecting from group '%s': %s", group_username, exc)

    saved = db_save_clients(sphere_id, group_username, collected)
    db_mark_group_parsed(f"https://t.me/{group_username}")
    logger.info("Collected %d new clients from '%s'", saved, group_username)
    return saved


async def fill_client_buffer(sphere_id: int) -> None:
    if db_count_available_clients(sphere_id) >= MIN_CLIENT_BUFFER:
        return

    groups = db_get_all_group_usernames(sphere_id)
    if not groups:
        logger.warning("No groups in DB for sphere_id=%d", sphere_id)
        return

    idx      = db_get_round_robin_index(sphere_id)
    attempts = 0

    while db_count_available_clients(sphere_id) < MIN_CLIENT_BUFFER and attempts < len(groups):
        await collect_clients_from_group(groups[idx % len(groups)], sphere_id)
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
    STATE_ADD_KEYWORDS,
    STATE_SHOW_CLIENT,
) = range(8)

# ===========================================================================
# KEYBOARDS
# ===========================================================================

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton("🖐🏻 Ручной поиск")]], resize_keyboard=True)


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
    await update.message.reply_text("Добро пожаловать! Выберите действие:", reply_markup=main_menu_keyboard())
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
        await update.message.reply_text("Введите название сферы:", reply_markup=ReplyKeyboardRemove())
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
        await update.message.reply_text("Ошибка. Вернитесь в главное меню.", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU

    client_row = db_get_next_client(sphere_id)
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

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=client_keyboard())
    asyncio.create_task(fill_client_buffer(sphere_id))
    return STATE_SHOW_CLIENT


async def handle_show_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "➡️ Следующий":
        if cid := context.user_data.get("current_client_id"):
            db_mark_client_used(cid)
        return await show_next_client(update, context)

    if text == "Главное меню":
        if cid := context.user_data.get("current_client_id"):
            db_mark_client_used(cid)
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
    await update.message.reply_text("Теперь добавьте этапы переписки:", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE


async def handle_after_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "+Этап":
        await update.message.reply_text("Введите пояснение этапа:", reply_markup=ReplyKeyboardRemove())
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
    desc   = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text("✅ Этап сохранён. Добавьте ещё или продолжите:", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE


async def handle_add_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords   = [kw.strip() for kw in update.message.text.strip().splitlines() if kw.strip()]
    new_sphere = context.user_data.get("new_sphere", {})
    name: str  = new_sphere.get("name", "")
    stages     = new_sphere.get("stages", [])

    sphere_id = db_add_sphere(name, json.dumps(keywords, ensure_ascii=False))
    for stage in stages:
        db_add_stage(sphere_id, stage["desc"], stage["script"])

    await update.message.reply_text(f"💾 Сфера «{name}» сохранена!\n\n🔍 Ищу группы по ключевым словам...")

    async def status_cb(msg: str) -> None:
        try:
            await update.message.reply_text(msg)
        except Exception:
            pass

    try:
        total = await find_groups_for_sphere(
            sphere_id=sphere_id,
            sphere_name=name,
            keywords=keywords,
            status_callback=status_cb,
        )
        if total > 0:
            await update.message.reply_text(f"✅ Найдено групп: {total}. Начинаю сбор клиентов...")
            asyncio.create_task(fill_client_buffer(sphere_id))
        else:
            await update.message.reply_text("⚠️ Группы не найдены. Попробуйте другие ключевые слова.")
    except Exception as exc:
        logger.error("Error in find_groups_for_sphere: %s", exc)
        await update.message.reply_text("❌ Ошибка при поиске групп. Проверьте логи.")

    context.user_data.pop("new_sphere", None)
    await update.message.reply_text("Выберите сферу:", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU


async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используйте кнопки меню.", reply_markup=main_menu_keyboard())
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
            STATE_MAIN_MENU:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)],
            STATE_SPHERE_MENU:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sphere_menu)],
            STATE_ADD_SPHERE_NAME:[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_sphere_name)],
            STATE_AFTER_STAGE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_after_stage)],
            STATE_ADD_STAGE_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_desc)],
            STATE_ADD_STAGE_SCRIPT:[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_script)],
            STATE_ADD_KEYWORDS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_keywords)],
            STATE_SHOW_CLIENT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_show_client)],
        },
        fallbacks=[CommandHandler("start", cmd_start), MessageHandler(filters.ALL, fallback)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()

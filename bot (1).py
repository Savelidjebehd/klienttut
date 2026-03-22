"""
Telegram-бот: поиск клиентов + автопостинг сторис.
Стек: python-telegram-bot + Telethon + Pyrogram + SQLAlchemy + SQLite
"""

import asyncio
import json
import logging
import random
import re
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer,
    String, Text, UniqueConstraint, create_engine, text, Float,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User, Channel, Chat

from pyrogram import Client as PyrogramClient
from pyrogram import enums as pyrogram_enums
from pyrogram import types as pyrogram_types

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ===========================================================================
# CONFIG
# ===========================================================================

BOT_TOKEN      = "8683047774:AAGWKswZoyzH7oK4nhXN6_FqhdA1fMXO_DA"
API_ID         = 20533422
API_HASH       = "744e04f35f8aae6803294a5f3989c35b"
SESSION_STRING = "1ApWapzMBu8GfTi-Z98bxTWatfDWW-pdK8Y3hdqE10ze5ljZlnhMlbaR-FnQ59CfZiJvVB9W-tTJueR64dOddGajpri6gIy7JUau5XrjOm38tfYs3oz3GJhhKAsn_yHFw-eYDc_TJigD78qEA8VYTS1GiFurA8lNOV_UxlE6jOcjwjdObGMSC8KVOC_uaLNzpP6ghT9ons-S3t5GsGUbRNWkAGJBilAwKo8eXwvb5TAfST9FaCDlZn98SDgsdyqcmqbrXZdlDF2hRLrs8qGaho8NDiuC2-oFDCKyHKtS78XOEVUVd8OZ0wn-At7msPzLOjEXdxQfiFJ1qp1V_xdCYLDyas213SO4="

# Pyrogram — тот же аккаунт, но через Pyrogram для публикации сторис
PYROGRAM_SESSION = "pyrogram_session"  # файл сессии Pyrogram

DATABASE_URL          = "sqlite:///bot_data.db"
MAX_GROUPS            = 100
MAX_CLIENTS           = 150
LOW_CLIENTS           = 50
CRITICAL_CLIENTS      = 20
GROUP_CLIENT_LIMIT    = 150
CLIENT_TTL_DAYS       = 30
MESSAGE_LOOKBACK_DAYS = 730   # до 2 лет
BG_CHECK_INTERVAL     = 120
BG_SLEEP_AFTER_GROUPS = 3600
BG_KEYWORDS_PER_PASS  = 5

# Позиции кнопки в истории (Y координата в %)
BUTTON_POSITIONS = {
    "top":    15.0,
    "center": 50.0,
    "bottom": 80.0,
}

# ===========================================================================
# DATABASE
# ===========================================================================

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


# --- Существующие таблицы (не трогаем структуру) ---

class Sphere(Base):
    __tablename__ = "spheres"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    name           = Column(String,  nullable=False)
    group_keywords = Column(Text,    nullable=False)
    user_keywords  = Column(Text,    nullable=False, default="[]")
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
    shown_count = Column(Integer,  default=0,               nullable=False)
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


# --- Новые таблицы для системы сторис ---

class UsersDB(Base):
    """Широкая база всех пользователей — без фильтрации при парсинге."""
    __tablename__ = "users_db"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    sphere_id     = Column(Integer, ForeignKey("spheres.id"), nullable=False)
    user_id       = Column(String,  nullable=False)   # Telegram user_id
    username      = Column(String,  nullable=True)
    first_name    = Column(String,  nullable=True)
    last_name     = Column(String,  nullable=True)
    bio           = Column(Text,    nullable=True)
    found_at      = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("sphere_id", "user_id", name="uq_sphere_user"),)


class StoryTemplate(Base):
    """Шаблон для публикации историй."""
    __tablename__ = "story_templates"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    sphere_id   = Column(Integer, ForeignKey("spheres.id"), nullable=False)
    name        = Column(String,  nullable=False)
    photo_ids   = Column(Text,    nullable=False, default="[]")   # JSON список file_id
    texts       = Column(Text,    nullable=False, default="[]")   # JSON список текстов
    button_text = Column(String,  nullable=True)                  # текст кнопки
    button_url  = Column(String,  nullable=True)                  # ссылка (t.me/...?text=...)
    button_pos  = Column(String,  nullable=False, default="bottom")  # top/center/bottom
    created_at  = Column(DateTime, default=datetime.utcnow)
    flows = relationship("StoryFlow", back_populates="template", cascade="all, delete-orphan")


class StoryMode(Base):
    """Режим постинга."""
    __tablename__ = "story_modes"
    id                   = Column(Integer, primary_key=True, autoincrement=True)
    name                 = Column(String,  nullable=False)
    filter_percent       = Column(Integer, nullable=False, default=100)
    mentions_min         = Column(Integer, nullable=False, default=3)
    mentions_max         = Column(Integer, nullable=False, default=6)
    interval_min         = Column(Integer, nullable=False, default=20)  # минуты
    interval_max         = Column(Integer, nullable=False, default=30)
    stories_per_batch    = Column(Integer, nullable=False, default=5)
    rest_minutes_min     = Column(Integer, nullable=False, default=120)
    rest_minutes_max     = Column(Integer, nullable=False, default=180)
    daily_limit          = Column(Integer, nullable=False, default=30)
    flows = relationship("StoryFlow", back_populates="mode")


class StoryFlow(Base):
    """Поток публикации историй."""
    __tablename__ = "story_flows"
    id           = Column(Integer,  primary_key=True, autoincrement=True)
    sphere_id    = Column(Integer,  ForeignKey("spheres.id"),       nullable=False)
    template_id  = Column(Integer,  ForeignKey("story_templates.id"), nullable=False)
    mode_id      = Column(Integer,  ForeignKey("story_modes.id"),   nullable=False)
    status       = Column(String,   nullable=False, default="running")  # running/stopped/done
    end_time     = Column(DateTime, nullable=True)
    stories_sent = Column(Integer,  default=0)
    users_tagged = Column(Integer,  default=0)
    created_at   = Column(DateTime, default=datetime.utcnow)
    logs         = Column(Text,     nullable=False, default="[]")
    template = relationship("StoryTemplate", back_populates="flows")
    mode     = relationship("StoryMode",     back_populates="flows")


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        # Безопасная миграция существующих таблиц
        for sql in [
            "ALTER TABLE spheres ADD COLUMN user_keywords TEXT NOT NULL DEFAULT '[]'",
        ]:
            try:
                conn.execute(text(sql)); conn.commit()
            except OperationalError:
                pass
    _seed_modes()


def _seed_modes() -> None:
    """Создаёт стандартные режимы если их нет."""
    defaults = [
        dict(name="🟢 Безопасный",  filter_percent=100, mentions_min=3,  mentions_max=6,
             interval_min=20, interval_max=30, stories_per_batch=5,
             rest_minutes_min=120, rest_minutes_max=180, daily_limit=30),
        dict(name="🟡 Умеренный",   filter_percent=60,  mentions_min=6,  mentions_max=12,
             interval_min=10, interval_max=15, stories_per_batch=10,
             rest_minutes_min=60,  rest_minutes_max=120, daily_limit=60),
        dict(name="🔴 Быстрый",     filter_percent=0,   mentions_min=12, mentions_max=20,
             interval_min=2,  interval_max=5,  stories_per_batch=20,
             rest_minutes_min=30,  rest_minutes_max=60,  daily_limit=100),
    ]
    with SessionLocal() as s:
        if s.query(StoryMode).count() == 0:
            for d in defaults:
                s.add(StoryMode(**d))
            s.commit()

# ===========================================================================
# DB HELPERS — существующие
# ===========================================================================

def db_get_spheres():
    with SessionLocal() as s:
        rows = s.query(Sphere).all(); s.expunge_all(); return rows

def db_get_sphere(sphere_id: int) -> Optional[Sphere]:
    with SessionLocal() as s:
        row = s.query(Sphere).filter(Sphere.id == sphere_id).first()
        if row: s.expunge(row)
        return row

def db_get_sphere_by_name(name: str) -> Optional[Sphere]:
    with SessionLocal() as s:
        row = s.query(Sphere).filter(Sphere.name == name).first()
        if row: s.expunge(row)
        return row

def db_add_sphere(name, group_keywords, user_keywords) -> int:
    with SessionLocal() as s:
        obj = Sphere(name=name, group_keywords=group_keywords, user_keywords=user_keywords)
        s.add(obj); s.commit(); return obj.id

def db_delete_sphere(sphere_id: int) -> None:
    with SessionLocal() as s:
        row = s.query(Sphere).filter(Sphere.id == sphere_id).first()
        if row: s.delete(row); s.commit()

def db_update_sphere_keywords(sphere_id: int, group_keywords: str, user_keywords: str) -> None:
    with SessionLocal() as s:
        row = s.query(Sphere).filter(Sphere.id == sphere_id).first()
        if row:
            row.group_keywords = group_keywords
            row.user_keywords  = user_keywords
            s.commit()

def db_add_stage(sphere_id, description, script) -> None:
    with SessionLocal() as s:
        cnt = s.query(Stage).filter(Stage.sphere_id == sphere_id).count()
        s.add(Stage(sphere_id=sphere_id, position=cnt+1, description=description, script=script))
        s.commit()

def db_get_stages(sphere_id):
    with SessionLocal() as s:
        rows = s.query(Stage).filter(Stage.sphere_id == sphere_id).order_by(Stage.position).all()
        s.expunge_all(); return rows

def db_update_stage_script(stage_id: int, description: str, script: str) -> None:
    with SessionLocal() as s:
        row = s.query(Stage).filter(Stage.id == stage_id).first()
        if row:
            row.description = description; row.script = script; s.commit()

def db_delete_stages(sphere_id: int) -> None:
    with SessionLocal() as s:
        s.query(Stage).filter(Stage.sphere_id == sphere_id).delete(); s.commit()

def db_get_user_keywords(sphere_id: int) -> list:
    with SessionLocal() as s:
        row = s.query(Sphere.user_keywords).filter(Sphere.id == sphere_id).first()
        return json.loads(row[0] or "[]") if row else []

def db_count_groups(sphere_id: int) -> int:
    with SessionLocal() as s:
        return s.query(Group).filter(Group.sphere_id == sphere_id).count()

def db_save_groups(sphere_id, sphere_name, links) -> int:
    saved = 0
    with SessionLocal() as s:
        for link in links:
            s.add(Group(sphere_id=sphere_id, sphere_name=sphere_name,
                        group_link=link, parsed=False, shown_count=0))
            try:
                s.flush(); saved += 1
            except IntegrityError:
                s.rollback()
        s.commit()
    return saved

def db_get_all_group_usernames(sphere_id: int) -> list:
    with SessionLocal() as s:
        rows = s.query(Group.group_link).filter(Group.sphere_id == sphere_id).all()
    return [l.rstrip("/").split("/")[-1] for (l,) in rows if l.rstrip("/").split("/")[-1]]

def db_mark_group_parsed(group_link: str) -> None:
    with SessionLocal() as s:
        g = s.query(Group).filter(Group.group_link == group_link).first()
        if g: g.parsed = True; s.commit()

def db_increment_group_shown(group_username: str, sphere_id: int) -> None:
    link = f"https://t.me/{group_username}"
    with SessionLocal() as s:
        g = s.query(Group).filter(Group.group_link == link, Group.sphere_id == sphere_id).first()
        if g:
            g.shown_count = (g.shown_count or 0) + 1
            if g.shown_count >= GROUP_CLIENT_LIMIT: s.delete(g)
            s.commit()

def db_cleanup_old_clients() -> None:
    cutoff = datetime.utcnow() - timedelta(days=CLIENT_TTL_DAYS)
    with SessionLocal() as s:
        s.query(Client).filter(Client.found_at < cutoff).delete(); s.commit()

def db_count_available_clients(sphere_id: int) -> int:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        return s.query(Client).filter(Client.sphere_id == sphere_id, Client.used == False).count()

def db_get_random_client(sphere_id: int) -> Optional[Client]:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        grps = s.query(Client.group_username).filter(
            Client.sphere_id == sphere_id, Client.used == False
        ).distinct().all()
        if not grps: return None
        gu  = random.choice(grps)[0]
        row = s.query(Client).filter(
            Client.sphere_id == sphere_id, Client.used == False, Client.group_username == gu,
        ).order_by(Client.id).first()
        if row: s.expunge(row)
        return row

def db_mark_client_used(client_id: int) -> Optional[str]:
    with SessionLocal() as s:
        c = s.query(Client).filter(Client.id == client_id).first()
        if c:
            c.used = True; g = c.group_username; s.commit(); return g
        return None

def db_is_username_known(sphere_id: int, username: str) -> bool:
    with SessionLocal() as s:
        return s.query(Client).filter(
            Client.sphere_id == sphere_id, Client.username == username
        ).first() is not None

def db_save_clients(sphere_id, group_username, usernames) -> int:
    saved = 0
    with SessionLocal() as s:
        for u in usernames:
            s.add(Client(sphere_id=sphere_id, username=u, group_username=group_username))
            try:
                s.flush(); saved += 1
            except IntegrityError:
                s.rollback()
        s.commit()
    return saved

def db_get_round_robin_index(sphere_id: int) -> int:
    with SessionLocal() as s:
        row = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sphere_id).first()
        return row.last_group_index if row else 0

def db_set_round_robin_index(sphere_id: int, idx: int) -> None:
    with SessionLocal() as s:
        row = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sphere_id).first()
        if row: row.last_group_index = idx
        else: s.add(GroupRoundRobin(sphere_id=sphere_id, last_group_index=idx))
        s.commit()

# ===========================================================================
# DB HELPERS — users_db (широкая база для сторис)
# ===========================================================================

def db_save_user(sphere_id: int, user_id: str, username: str,
                 first_name: str, last_name: str, bio: str) -> bool:
    with SessionLocal() as s:
        s.add(UsersDB(sphere_id=sphere_id, user_id=user_id, username=username,
                      first_name=first_name, last_name=last_name, bio=bio))
        try:
            s.flush(); s.commit(); return True
        except IntegrityError:
            s.rollback(); return False

def db_get_all_users(sphere_id: int) -> list:
    with SessionLocal() as s:
        rows = s.query(UsersDB).filter(UsersDB.sphere_id == sphere_id).all()
        s.expunge_all(); return rows

def db_get_filtered_users(sphere_id: int, keywords: list) -> list:
    """Фильтрация при запуске — не при парсинге."""
    all_users = db_get_all_users(sphere_id)
    if not keywords:
        return all_users
    result = []
    for u in all_users:
        haystack = " ".join(filter(None, [
            u.bio, u.username, u.first_name, u.last_name
        ])).lower()
        if any(kw.lower() in haystack for kw in keywords):
            result.append(u)
    return result

def db_count_users(sphere_id: int) -> int:
    with SessionLocal() as s:
        return s.query(UsersDB).filter(UsersDB.sphere_id == sphere_id).count()

# ===========================================================================
# DB HELPERS — шаблоны
# ===========================================================================

def db_create_template(sphere_id: int, name: str) -> int:
    with SessionLocal() as s:
        obj = StoryTemplate(sphere_id=sphere_id, name=name)
        s.add(obj); s.commit(); return obj.id

def db_get_templates(sphere_id: int) -> list:
    with SessionLocal() as s:
        rows = s.query(StoryTemplate).filter(StoryTemplate.sphere_id == sphere_id).all()
        s.expunge_all(); return rows

def db_get_template(template_id: int) -> Optional[StoryTemplate]:
    with SessionLocal() as s:
        row = s.query(StoryTemplate).filter(StoryTemplate.id == template_id).first()
        if row: s.expunge(row)
        return row

def db_update_template(template_id: int, **kwargs) -> None:
    with SessionLocal() as s:
        row = s.query(StoryTemplate).filter(StoryTemplate.id == template_id).first()
        if row:
            for k, v in kwargs.items():
                setattr(row, k, v)
            s.commit()

def db_delete_template(template_id: int) -> None:
    with SessionLocal() as s:
        row = s.query(StoryTemplate).filter(StoryTemplate.id == template_id).first()
        if row: s.delete(row); s.commit()

# ===========================================================================
# DB HELPERS — режимы и потоки
# ===========================================================================

def db_get_modes() -> list:
    with SessionLocal() as s:
        rows = s.query(StoryMode).all(); s.expunge_all(); return rows

def db_get_mode(mode_id: int) -> Optional[StoryMode]:
    with SessionLocal() as s:
        row = s.query(StoryMode).filter(StoryMode.id == mode_id).first()
        if row: s.expunge(row)
        return row

def db_create_flow(sphere_id: int, template_id: int, mode_id: int,
                   end_time: Optional[datetime]) -> int:
    with SessionLocal() as s:
        obj = StoryFlow(sphere_id=sphere_id, template_id=template_id,
                        mode_id=mode_id, end_time=end_time)
        s.add(obj); s.commit(); return obj.id

def db_get_flows(sphere_id: int) -> list:
    with SessionLocal() as s:
        rows = s.query(StoryFlow).filter(StoryFlow.sphere_id == sphere_id).all()
        s.expunge_all(); return rows

def db_get_flow(flow_id: int) -> Optional[StoryFlow]:
    with SessionLocal() as s:
        row = s.query(StoryFlow).filter(StoryFlow.id == flow_id).first()
        if row: s.expunge(row)
        return row

def db_stop_flow(flow_id: int) -> None:
    with SessionLocal() as s:
        row = s.query(StoryFlow).filter(StoryFlow.id == flow_id).first()
        if row: row.status = "stopped"; s.commit()

def db_update_flow_stats(flow_id: int, stories_sent: int, users_tagged: int, log_entry: str) -> None:
    with SessionLocal() as s:
        row = s.query(StoryFlow).filter(StoryFlow.id == flow_id).first()
        if row:
            row.stories_sent = stories_sent
            row.users_tagged = users_tagged
            logs = json.loads(row.logs or "[]")
            logs.append(f"{datetime.utcnow().strftime('%H:%M:%S')} — {log_entry}")
            logs = logs[-50:]  # храним последние 50 записей
            row.logs = json.dumps(logs, ensure_ascii=False)
            s.commit()

# ===========================================================================
# TELETHON
# ===========================================================================

telethon_client: Optional[TelegramClient] = None

async def ensure_telethon() -> TelegramClient:
    global telethon_client
    if telethon_client is None or not telethon_client.is_connected():
        telethon_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await telethon_client.connect()
    return telethon_client

# ===========================================================================
# PYROGRAM — для публикации историй
# ===========================================================================

pyrogram_app: Optional[PyrogramClient] = None

async def ensure_pyrogram() -> PyrogramClient:
    global pyrogram_app
    if pyrogram_app is None:
        pyrogram_app = PyrogramClient(
            PYROGRAM_SESSION,
            api_id=API_ID,
            api_hash=API_HASH,
        )
    if not pyrogram_app.is_connected:
        await pyrogram_app.start()
    return pyrogram_app

# ===========================================================================
# ПРОГРЕСС-БАР
# ===========================================================================

def make_progress_bar(current: int, total: int, width: int = 16) -> str:
    pct    = min(100, int(current / total * 100)) if total else 0
    filled = int(width * pct / 100)
    return f"{'Ӏ' * filled}{' ' * (width - filled)} {pct}%"

# ===========================================================================
# ПОИСК ГРУПП И КЛИЕНТОВ (существующая логика)
# ===========================================================================

_collecting: set = set()
_last_group_search: dict = {}


def _is_public_group(chat) -> bool:
    if isinstance(chat, Chat):    return bool(getattr(chat, "username", None))
    if isinstance(chat, Channel): return bool(getattr(chat, "megagroup", False)) and bool(getattr(chat, "username", None))
    return False

async def _search_one_keyword(keyword: str) -> list:
    client = await ensure_telethon()
    found  = []
    try:
        result = await client(SearchRequest(q=keyword, limit=100))
        for chat in result.chats:
            if not _is_public_group(chat): continue
            u = getattr(chat, "username", None)
            if u: found.append(f"https://t.me/{u.lower()}")
    except Exception as exc:
        logger.warning("SearchRequest failed «%s»: %s", keyword, exc)
    await asyncio.sleep(0)
    return found

async def _find_groups_bg(sphere_id: int, sphere_name: str, keywords: list, progress_message=None) -> int:
    if db_count_groups(sphere_id) >= MAX_GROUPS:
        return db_count_groups(sphere_id)
    batch     = keywords[:BG_KEYWORDS_PER_PASS]
    total_kw  = len(batch)
    all_links: list = []
    last_edit = 0.0
    for i, kw in enumerate(batch):
        if db_count_groups(sphere_id) + len(all_links) >= MAX_GROUPS: break
        links     = await _search_one_keyword(kw)
        new_links = [l for l in links if l not in all_links]
        all_links.extend(new_links)
        now = asyncio.get_event_loop().time()
        if progress_message and (now - last_edit) >= 3.0:
            bar = make_progress_bar(i + 1, total_kw)
            try:
                await progress_message.edit_text(
                    f"🔍 Поиск групп...\n\n{bar}\n\n"
                    f"Обработано: {i+1}/{total_kw}\nНайдено: {len(all_links)}\n\n"
                    f"✅ Бот работает!"
                )
                last_edit = now
            except Exception: pass
        await asyncio.sleep(1.5)
    saved = db_save_groups(sphere_id, sphere_name, list(dict.fromkeys(all_links)))
    total = db_count_groups(sphere_id)
    if progress_message:
        try:
            await progress_message.edit_text(f"✅ Поиск завершён!\n\n{make_progress_bar(1,1)}\n\nГрупп: {total}")
        except Exception: pass
    return total

def _user_matches_keywords(username, first_name, last_name, bio, keywords) -> bool:
    if not keywords: return True
    h = " ".join(filter(None, [username, first_name, last_name, bio])).lower()
    return any(k.lower() in h for k in keywords)

async def _get_user_bio(client, user_id) -> str:
    try:
        full = await client(GetFullUserRequest(user_id))
        return full.full_user.about or ""
    except Exception:
        return ""

async def _collect_one_group(group_username: str, sphere_id: int, user_keywords: list, limit: int = 300) -> int:
    """
    Собирает пользователей из группы.
    Этап 1 (парсинг): сохраняем ВСЕХ в users_db без фильтрации.
    Этап 1б (для существующих clients): применяем user_keywords.
    """
    client   = await ensure_telethon()
    lookback = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set   = set()
    collected_clients: list = []

    try:
        entity = await client.get_entity(group_username)
        await asyncio.sleep(0)
        offset_id, total_fetched = 0, 0

        while total_fetched < limit:
            batch = min(100, limit - total_fetched)
            history = await client(GetHistoryRequest(
                peer=entity, limit=batch, offset_date=None,
                offset_id=offset_id, max_id=0, min_id=0, add_offset=0, hash=0,
            ))
            await asyncio.sleep(0)
            if not history.messages: break
            stop = False
            for msg in history.messages:
                if not msg.date: continue
                if msg.date.replace(tzinfo=None) < lookback: stop = True; break
                if not getattr(msg, "from_id", None): continue
                uid = getattr(msg.from_id, "user_id", None)
                if not uid or uid in seen_ids: continue
                seen_ids.add(uid)
                try:
                    user = await client.get_entity(uid)
                    await asyncio.sleep(0)
                    if not isinstance(user, User): continue
                    bio = await _get_user_bio(client, uid)

                    # Этап 1: сохраняем ВСЕХ в users_db (без фильтра)
                    db_save_user(
                        sphere_id=sphere_id,
                        user_id=str(uid),
                        username=user.username or "",
                        first_name=user.first_name or "",
                        last_name=user.last_name or "",
                        bio=bio,
                    )

                    # Для clients (показ менеджеру) — применяем фильтр
                    if user.username:
                        un = user.username.lower()
                        if not db_is_username_known(sphere_id, un):
                            if _user_matches_keywords(user.username, user.first_name, user.last_name, bio, user_keywords):
                                collected_clients.append(un)
                except Exception:
                    pass
            total_fetched += len(history.messages)
            offset_id      = history.messages[-1].id
            if stop or len(history.messages) < batch: break
            await asyncio.sleep(0.5)
    except Exception as exc:
        logger.warning("Error collecting «%s»: %s", group_username, exc)

    saved = db_save_clients(sphere_id, group_username, collected_clients)
    db_mark_group_parsed(f"https://t.me/{group_username}")
    logger.info("Collected %d clients + saved to users_db from «%s»", saved, group_username)
    return saved

async def _fill_buffer_bg(sphere_id: int, progress_message=None) -> None:
    if sphere_id in _collecting: return
    _collecting.add(sphere_id)
    try:
        sphere = db_get_sphere(sphere_id)
        if not sphere: return
        group_kws = json.loads(sphere.group_keywords or "[]")
        user_kws  = db_get_user_keywords(sphere_id)
        if db_count_groups(sphere_id) < 10:
            await _find_groups_bg(sphere_id=sphere_id, sphere_name=sphere.name,
                                  keywords=group_kws, progress_message=progress_message)
        groups = db_get_all_group_usernames(sphere_id)
        if not groups: return
        if progress_message:
            try:
                await progress_message.edit_text(
                    f"👥 Собираю клиентов...\n\n{make_progress_bar(0, len(groups))}\n\n✅ Бот работает!"
                )
            except Exception: pass
        idx, att  = db_get_round_robin_index(sphere_id), 0
        last_edit = asyncio.get_event_loop().time()
        while db_count_available_clients(sphere_id) < MAX_CLIENTS and att < len(groups):
            await _collect_one_group(groups[idx % len(groups)], sphere_id, user_kws)
            idx = (idx + 1) % len(groups); att += 1
            now = asyncio.get_event_loop().time()
            if progress_message and (now - last_edit) >= 3.0:
                bar = make_progress_bar(att, len(groups))
                cnt = db_count_available_clients(sphere_id)
                try:
                    await progress_message.edit_text(
                        f"👥 Собираю клиентов...\n\n{bar}\n\nНайдено: {cnt}\n✅ Бот работает!"
                    )
                    last_edit = now
                except Exception: pass
            await asyncio.sleep(0)
        db_set_round_robin_index(sphere_id, idx)
        if db_count_available_clients(sphere_id) < CRITICAL_CLIENTS:
            await _find_groups_bg(sphere_id=sphere_id, sphere_name=sphere.name, keywords=group_kws)
            groups2 = db_get_all_group_usernames(sphere_id)
            idx2, att2 = db_get_round_robin_index(sphere_id), 0
            while db_count_available_clients(sphere_id) < MAX_CLIENTS and att2 < len(groups2):
                await _collect_one_group(groups2[idx2 % len(groups2)], sphere_id, user_kws)
                idx2 = (idx2 + 1) % len(groups2); att2 += 1
                await asyncio.sleep(0)
            db_set_round_robin_index(sphere_id, idx2)
        total = db_count_available_clients(sphere_id)
        if progress_message:
            try:
                await progress_message.edit_text(f"✅ Готово! Найдено клиентов: {total}\nНажмите «➡️ Следующий».")
            except Exception: pass
    except Exception as exc:
        logger.error("[FILL] sphere_id=%d: %s", sphere_id, exc)
    finally:
        _collecting.discard(sphere_id)

async def background_worker() -> None:
    logger.info("Background worker started.")
    while True:
        try:
            now = asyncio.get_event_loop().time()
            for sphere in db_get_spheres():
                sid = sphere.id
                cc  = db_count_available_clients(sid)
                gc  = db_count_groups(sid)
                if cc < LOW_CLIENTS and sid not in _collecting:
                    asyncio.create_task(_fill_buffer_bg(sid))
                last = _last_group_search.get(sid, 0)
                if gc < MAX_GROUPS and (now - last) >= BG_SLEEP_AFTER_GROUPS:
                    _last_group_search[sid] = now
                    kws = json.loads(sphere.group_keywords or "[]")
                    asyncio.create_task(_find_groups_bg(sphere_id=sid, sphere_name=sphere.name, keywords=kws))
                await asyncio.sleep(1)
        except Exception as exc:
            logger.error("[BG] loop: %s", exc)
        await asyncio.sleep(BG_CHECK_INTERVAL)

# ===========================================================================
# ЛОГИКА ПУБЛИКАЦИИ ИСТОРИЙ
# ===========================================================================

# Активные потоки flow_id → asyncio.Task
_active_flows: dict = {}


def _pick_users_for_mode(sphere_id: int, mode: StoryMode, keywords: list, count: int) -> list:
    """
    Выбирает пользователей согласно режиму:
    Безопасный (100%) → только filtered
    Умеренный (60%)   → 60% filtered, 40% all
    Быстрый (0%)      → все без фильтра
    """
    all_users      = db_get_all_users(sphere_id)
    filtered_users = db_get_filtered_users(sphere_id, keywords)

    if not all_users: return []

    selected = []
    for _ in range(count):
        if mode.filter_percent == 100:
            pool = filtered_users or all_users
        elif mode.filter_percent == 0:
            pool = all_users
        else:
            if filtered_users and random.random() < (mode.filter_percent / 100):
                pool = filtered_users
            else:
                pool = all_users
        if pool:
            selected.append(random.choice(pool))

    # убираем дубли по user_id
    seen = set()
    unique = []
    for u in selected:
        if u.user_id not in seen:
            seen.add(u.user_id)
            unique.append(u)
    return unique


async def _publish_story(
    template: StoryTemplate,
    users: list,
    sphere_id: int,
) -> bool:
    """Публикует одну историю через Pyrogram."""
    try:
        pyro = await ensure_pyrogram()

        # Случайное фото и текст
        photo_ids = json.loads(template.photo_ids or "[]")
        texts     = json.loads(template.texts     or "[]")
        if not photo_ids: return False

        photo_id = random.choice(photo_ids)
        caption  = random.choice(texts) if texts else ""

        # Добавляем отметки пользователей в подпись
        if users:
            mentions = " ".join(
                f"@{u.username}" for u in users if u.username
            )
            # разбиваем на 2 строки
            mention_list = mentions.split()
            mid = len(mention_list) // 2
            line1 = " ".join(mention_list[:mid])
            line2 = " ".join(mention_list[mid:])
            caption = f"{caption}\n\n{line1}\n{line2}".strip()

        # Кнопка — MediaAreaUrl по координатам
        media_areas = []
        if template.button_url:
            y_pos = BUTTON_POSITIONS.get(template.button_pos, 80.0)
            media_areas.append(
                pyrogram_types.MediaAreaUrl(
                    coordinates=pyrogram_types.MediaAreaCoordinates(
                        x=30.0, y=y_pos, width=40.0, height=8.0, rotation=0.0,
                    ),
                    url=template.button_url,
                )
            )

        await pyro.send_story(
            chat_id="me",
            media=photo_id,
            caption=caption,
            period=86400,
            privacy=pyrogram_enums.StoriesPrivacyRules.PUBLIC,
            media_areas=media_areas if media_areas else None,
        )
        return True

    except Exception as exc:
        logger.error("[STORY] publish error: %s", exc)
        return False


async def _run_flow(flow_id: int) -> None:
    """
    Основной цикл потока публикации историй.
    Работает как отдельная asyncio задача.
    """
    logger.info("[FLOW %d] started", flow_id)
    stories_sent = 0
    users_tagged = 0
    daily_count  = 0
    day_reset    = datetime.utcnow().date()

    while True:
        flow = db_get_flow(flow_id)
        if not flow or flow.status != "running":
            logger.info("[FLOW %d] stopped", flow_id)
            break

        # Проверяем время окончания
        if flow.end_time and datetime.utcnow() >= flow.end_time:
            db_stop_flow(flow_id)
            logger.info("[FLOW %d] time ended", flow_id)
            break

        # Сброс дневного счётчика
        today = datetime.utcnow().date()
        if today != day_reset:
            daily_count = 0; day_reset = today

        mode     = db_get_mode(flow.mode_id)
        template = db_get_template(flow.template_id)
        if not mode or not template: break

        # Проверяем дневной лимит
        if daily_count >= mode.daily_limit:
            sleep_sec = 3600  # ждём час
            log = f"Дневной лимит {mode.daily_limit} достигнут. Жду 1 час."
            logger.info("[FLOW %d] %s", flow_id, log)
            db_update_flow_stats(flow_id, stories_sent, users_tagged, log)
            await asyncio.sleep(sleep_sec)
            continue

        sphere = db_get_sphere(flow.sphere_id)
        keywords = db_get_user_keywords(flow.sphere_id) if sphere else []

        # Публикуем батч историй
        for _ in range(mode.stories_per_batch):
            flow = db_get_flow(flow_id)
            if not flow or flow.status != "running": break
            if flow.end_time and datetime.utcnow() >= flow.end_time: break
            if daily_count >= mode.daily_limit: break

            # Выбираем пользователей
            n_mentions = random.randint(mode.mentions_min, mode.mentions_max)
            users      = _pick_users_for_mode(flow.sphere_id, mode, keywords, n_mentions)

            # Публикуем
            ok = await _publish_story(template, users, flow.sphere_id)

            if ok:
                stories_sent += 1
                daily_count  += 1
                users_tagged += len(users)
                log = (
                    f"История #{stories_sent} опубликована, "
                    f"отмечено {len(users)} чел."
                )
                logger.info("[FLOW %d] %s", flow_id, log)
                db_update_flow_stats(flow_id, stories_sent, users_tagged, log)

            # Интервал между историями в батче
            interval_sec = random.randint(
                mode.interval_min * 60, mode.interval_max * 60
            )
            log = f"Жду {interval_sec // 60} мин до следующей истории"
            db_update_flow_stats(flow_id, stories_sent, users_tagged, log)
            await asyncio.sleep(interval_sec)

        # Пауза после батча
        rest_sec = random.randint(
            mode.rest_minutes_min * 60, mode.rest_minutes_max * 60
        )
        log = f"Пауза {rest_sec // 60} мин после батча"
        logger.info("[FLOW %d] %s", flow_id, log)
        db_update_flow_stats(flow_id, stories_sent, users_tagged, log)
        await asyncio.sleep(rest_sec)

    _active_flows.pop(flow_id, None)
    logger.info("[FLOW %d] finished. Sent: %d, Tagged: %d", flow_id, stories_sent, users_tagged)

# ===========================================================================
# STATES
# ===========================================================================

(
    # Основные
    STATE_MAIN_MENU,
    STATE_SPHERE_MENU,
    STATE_ADD_SPHERE_NAME,
    STATE_AFTER_STAGE,
    STATE_ADD_STAGE_DESC,
    STATE_ADD_STAGE_SCRIPT,
    STATE_ADD_GROUP_KEYWORDS,
    STATE_ADD_USER_KEYWORDS,
    STATE_SHOW_CLIENT,
    STATE_EDIT_SCRIPT_CHOOSE,
    STATE_EDIT_SCRIPT_DESC,
    STATE_EDIT_SCRIPT_TEXT,
    STATE_EDIT_KEYS_GROUP,
    STATE_EDIT_KEYS_USER,
    STATE_REDO_STAGE_DESC,
    STATE_REDO_STAGE_SCRIPT,
    # Сторис
    STATE_STORY_MENU,
    STATE_STORY_CHOOSE_SPHERE,
    STATE_STORY_TEMPLATE_MENU,
    STATE_STORY_NEW_TEMPLATE_NAME,
    STATE_STORY_ADD_PHOTO,
    STATE_STORY_ADD_TEXT,
    STATE_STORY_BUTTON_TEXT,
    STATE_STORY_BUTTON_MSG,
    STATE_STORY_BUTTON_POS,
    STATE_STORY_CHOOSE_MODE,
    STATE_STORY_CHOOSE_TIME,
    STATE_STORY_CUSTOM_TIME,
    STATE_STORY_FLOWS_MENU,
) = range(29)

# ===========================================================================
# KEYBOARDS
# ===========================================================================

def main_menu_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🖐🏻 Ручной поиск")],
        [KeyboardButton("📸 Отметки сторис")],
    ], resize_keyboard=True)

def sphere_menu_keyboard():
    spheres = db_get_spheres()
    btns    = [[KeyboardButton(s.name)] for s in spheres]
    btns.append([KeyboardButton("➕ Добавить сферу")])
    btns.append([KeyboardButton("🏠 Главное меню")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def client_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("➡️ Следующий")],
        [KeyboardButton("✏️ Изменить скрипт"), KeyboardButton("🔑 Изменить ключи")],
        [KeyboardButton("🏠 Главное меню"),     KeyboardButton("🗑 Удалить сферу")],
    ], resize_keyboard=True)

def after_stage_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("+Этап"), KeyboardButton("Продолжить")]], resize_keyboard=True)

def edit_script_keyboard(stages: list):
    btns, row = [], []
    for i in range(len(stages)):
        row.append(KeyboardButton(str(i + 1)))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([KeyboardButton("Все"), KeyboardButton("Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def story_main_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("▶️ Запустить")],
        [KeyboardButton("🛠 Потоки")],
        [KeyboardButton("🏠 Главное меню")],
    ], resize_keyboard=True)

def story_template_keyboard(templates: list):
    btns = [[KeyboardButton(f"📋 {t.name}")] for t in templates]
    btns.append([KeyboardButton("➕ Новый шаблон")])
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def story_mode_keyboard(modes: list):
    btns = [[KeyboardButton(m.name)] for m in modes]
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def story_time_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⏱ 20 минут"),    KeyboardButton("📅 1 день")],
        [KeyboardButton("♾ До отключения"), KeyboardButton("🗓 Своя дата")],
        [KeyboardButton("🔙 Назад")],
    ], resize_keyboard=True)

def story_pos_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⬆️ Вверху"), KeyboardButton("⏺ По центру"), KeyboardButton("⬇️ Внизу")],
    ], resize_keyboard=True)

def story_flows_keyboard(flows: list):
    btns = []
    for f in flows:
        status = "🟢" if f.status == "running" else "🔴"
        btns.append([KeyboardButton(f"{status} Поток #{f.id} | {f.stories_sent} ист.")])
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def confirm_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("✅ Опубликовать"), KeyboardButton("❌ Отмена")],
    ], resize_keyboard=True)

# ===========================================================================
# HANDLERS — основные (без изменений логики)
# ===========================================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Добро пожаловать!", reply_markup=main_menu_keyboard())
    return STATE_MAIN_MENU

async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🖐🏻 Ручной поиск":
        await update.message.reply_text("Выберите сферу:", reply_markup=sphere_menu_keyboard())
        return STATE_SPHERE_MENU
    if text == "📸 Отметки сторис":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=story_main_keyboard())
        return STATE_STORY_MENU
    return STATE_MAIN_MENU

async def handle_sphere_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🏠 Главное меню":
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
        if db_count_available_clients(sphere.id) > 0:
            if sphere.id not in _collecting:
                asyncio.create_task(_fill_buffer_bg(sphere.id))
            return await show_next_client(update, context)
        prog_msg = await update.message.reply_text(
            f"🔍 Начинаю поиск для «{sphere.name}»...\n\n{make_progress_bar(0,1)}\n\n✅ Бот работает!",
            reply_markup=client_keyboard(),
        )
        asyncio.create_task(_fill_buffer_bg(sphere.id, progress_message=prog_msg))
        return STATE_SHOW_CLIENT
    await update.message.reply_text("Сфера не найдена.", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU

async def show_next_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid   = context.user_data.get("current_sphere_id")
    sname = context.user_data.get("current_sphere_name")
    if not sid:
        await update.message.reply_text("Ошибка.", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU
    row = db_get_random_client(sid)
    if not row:
        status = "Идёт сбор..." if sid in _collecting else "Запускаю сбор..."
        await update.message.reply_text(
            f"⏳ Клиентов пока нет. {status}\nПопробуйте через минуту.",
            reply_markup=client_keyboard(),
        )
        if sid not in _collecting:
            asyncio.create_task(_fill_buffer_bg(sid))
        return STATE_SHOW_CLIENT
    context.user_data["current_client_id"] = row.id
    stages = db_get_stages(sid)
    lines  = [f"<b>@{row.username}</b>\n", f"<b>{sname}</b>"]
    for i, st in enumerate(stages, 1):
        lines.append(f"\n<b>{i} этап — {st.description}</b>")
        lines.append(f"<blockquote>{st.script}</blockquote>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=client_keyboard())
    if db_count_available_clients(sid) < LOW_CLIENTS and sid not in _collecting:
        asyncio.create_task(_fill_buffer_bg(sid))
    return STATE_SHOW_CLIENT

async def handle_show_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    def _mark():
        if cid := context.user_data.get("current_client_id"):
            sid = context.user_data.get("current_sphere_id")
            gu  = db_mark_client_used(cid)
            if gu and sid: db_increment_group_shown(gu, sid)
    if text == "➡️ Следующий":
        _mark(); return await show_next_client(update, context)
    if text == "🏠 Главное меню":
        _mark()
        for k in ("current_sphere_id","current_sphere_name","current_client_id"):
            context.user_data.pop(k, None)
        await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU
    if text == "🗑 Удалить сферу":
        sid   = context.user_data.get("current_sphere_id")
        sname = context.user_data.get("current_sphere_name")
        if sid:
            db_delete_sphere(sid); _collecting.discard(sid)
            for k in ("current_sphere_id","current_sphere_name","current_client_id"):
                context.user_data.pop(k, None)
            await update.message.reply_text(f"🗑 Сфера «{sname}» удалена.", reply_markup=sphere_menu_keyboard())
        return STATE_SPHERE_MENU
    if text == "✏️ Изменить скрипт":
        sid    = context.user_data.get("current_sphere_id")
        stages = db_get_stages(sid)
        if not stages:
            await update.message.reply_text("Этапов нет.", reply_markup=client_keyboard())
            return STATE_SHOW_CLIENT
        context.user_data["edit_stages"] = [{"id": st.id, "desc": st.description, "script": st.script} for st in stages]
        await update.message.reply_text("Какой этап изменить?", reply_markup=edit_script_keyboard(stages))
        return STATE_EDIT_SCRIPT_CHOOSE
    if text == "🔑 Изменить ключи":
        sid = context.user_data.get("current_sphere_id")
        sp  = db_get_sphere(sid)
        if sp:
            kws = json.loads(sp.group_keywords or "[]")
            await update.message.reply_text(
                f"Текущие ключи групп:\n{chr(10).join(kws)}\n\nВведите новые:",
                reply_markup=ReplyKeyboardRemove(),
            )
            return STATE_EDIT_KEYS_GROUP
    return STATE_SHOW_CLIENT

# --- Изменение скрипта ---
async def handle_edit_script_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text   = update.message.text
    stages = context.user_data.get("edit_stages", [])
    if text == "Назад":
        await update.message.reply_text("Возврат:", reply_markup=client_keyboard())
        return STATE_SHOW_CLIENT
    if text == "Все":
        sid = context.user_data.get("current_sphere_id")
        db_delete_stages(sid)
        context.user_data["new_sphere"] = {"stages": [], "redo": True}
        await update.message.reply_text("Введите пояснение 1-го этапа:", reply_markup=ReplyKeyboardRemove())
        return STATE_REDO_STAGE_DESC
    try:
        idx = int(text) - 1
        if 0 <= idx < len(stages):
            context.user_data["editing_stage_idx"] = idx
            st = stages[idx]
            await update.message.reply_text(f"Этап {idx+1}: «{st['desc']}»\n\nВведите новое пояснение:", reply_markup=ReplyKeyboardRemove())
            return STATE_EDIT_SCRIPT_DESC
    except ValueError: pass
    await update.message.reply_text("Выберите номер:", reply_markup=edit_script_keyboard(stages))
    return STATE_EDIT_SCRIPT_CHOOSE

async def handle_edit_script_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите новый текст скрипта:")
    return STATE_EDIT_SCRIPT_TEXT

async def handle_edit_script_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_script = update.message.text.strip()
    new_desc   = context.user_data.pop("new_stage_desc", "")
    idx        = context.user_data.get("editing_stage_idx", 0)
    stages     = context.user_data.get("edit_stages", [])
    if 0 <= idx < len(stages):
        db_update_stage_script(stages[idx]["id"], new_desc, new_script)
    await update.message.reply_text("Данные изменены ✅", reply_markup=main_menu_keyboard())
    return STATE_MAIN_MENU

async def handle_redo_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_sphere"]["current_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:")
    return STATE_REDO_STAGE_SCRIPT

async def handle_redo_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip()
    desc   = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE

async def handle_edit_keys_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kws = [k.strip() for k in update.message.text.strip().splitlines() if k.strip()]
    context.user_data["new_group_kws"] = kws
    sid  = context.user_data.get("current_sphere_id")
    sp   = db_get_sphere(sid)
    ukws = json.loads(sp.user_keywords or "[]") if sp else []
    await update.message.reply_text(
        f"Ключи пользователей:\n{chr(10).join(ukws) if ukws else '(не заданы)'}\n\nВведите новые (или «нет»):",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_EDIT_KEYS_USER

async def handle_edit_keys_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = [] if raw.lower() in ("нет","no","-","") else [k.strip() for k in raw.splitlines() if k.strip()]
    sid  = context.user_data.get("current_sphere_id")
    gkws = context.user_data.pop("new_group_kws", [])
    db_update_sphere_keywords(sid, json.dumps(gkws, ensure_ascii=False), json.dumps(ukws, ensure_ascii=False))
    await update.message.reply_text("Ключи обновлены ✅", reply_markup=main_menu_keyboard())
    return STATE_MAIN_MENU

async def handle_add_sphere_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Введите название:"); return STATE_ADD_SPHERE_NAME
    context.user_data["new_sphere"]["name"] = name
    await update.message.reply_text("Добавьте этапы:", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE

async def handle_after_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    ns   = context.user_data.get("new_sphere", {})
    if text == "+Этап":
        await update.message.reply_text("Введите пояснение этапа:", reply_markup=ReplyKeyboardRemove())
        return STATE_REDO_STAGE_DESC if ns.get("redo") else STATE_ADD_STAGE_DESC
    if text == "Продолжить":
        if ns.get("redo"):
            sid = context.user_data.get("current_sphere_id")
            for st in ns.get("stages", []): db_add_stage(sid, st["desc"], st["script"])
            context.user_data.pop("new_sphere", None)
            await update.message.reply_text("Данные изменены ✅", reply_markup=main_menu_keyboard())
            return STATE_MAIN_MENU
        await update.message.reply_text("📌 Шаг 1 из 2: Ключевые слова для групп\n(каждое с новой строки):", reply_markup=ReplyKeyboardRemove())
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
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE

async def handle_add_group_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kws = [k.strip() for k in update.message.text.strip().splitlines() if k.strip()]
    if not kws:
        await update.message.reply_text("Введите хотя бы одно слово:"); return STATE_ADD_GROUP_KEYWORDS
    context.user_data["new_sphere"]["group_keywords"] = kws
    await update.message.reply_text(
        "📌 Шаг 2 из 2: Ключевые слова для фильтрации\n(или «нет»):",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_ADD_USER_KEYWORDS

async def handle_add_user_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = [] if raw.lower() in ("нет","no","-","") else [k.strip() for k in raw.splitlines() if k.strip()]
    ns   = context.user_data.get("new_sphere", {})
    name = ns.get("name",""); stages = ns.get("stages",[]); gkws = ns.get("group_keywords",[])
    sid  = db_add_sphere(name=name, group_keywords=json.dumps(gkws, ensure_ascii=False), user_keywords=json.dumps(ukws, ensure_ascii=False))
    for st in stages: db_add_stage(sid, st["desc"], st["script"])
    await update.message.reply_text(f"💾 Сфера «{name}» сохранена!\n🔍 Поиск запущен в фоне.")
    prog = await update.message.reply_text(f"🔍 Поиск групп...\n\n{make_progress_bar(0,1)}")
    asyncio.create_task(_fill_buffer_bg(sid, progress_message=prog))
    context.user_data.pop("new_sphere", None)
    await update.message.reply_text("Выберите сферу:", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU

# ===========================================================================
# HANDLERS — СТОРИС
# ===========================================================================

async def handle_story_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🏠 Главное меню":
        await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU
    if text == "▶️ Запустить":
        spheres = db_get_spheres()
        if not spheres:
            await update.message.reply_text("Нет сфер. Создайте сначала.", reply_markup=story_main_keyboard())
            return STATE_STORY_MENU
        btns = [[KeyboardButton(s.name)] for s in spheres]
        btns.append([KeyboardButton("🔙 Назад")])
        await update.message.reply_text(
            "Выберите сферу для сторис:",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True),
        )
        return STATE_STORY_CHOOSE_SPHERE
    if text == "🛠 Потоки":
        sid = context.user_data.get("story_sphere_id")
        if not sid:
            spheres = db_get_spheres()
            if not spheres:
                await update.message.reply_text("Нет сфер.", reply_markup=story_main_keyboard())
                return STATE_STORY_MENU
            btns = [[KeyboardButton(s.name)] for s in spheres]
            btns.append([KeyboardButton("🔙 Назад")])
            await update.message.reply_text(
                "Выберите сферу для просмотра потоков:",
                reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True),
            )
            context.user_data["story_flows_mode"] = True
            return STATE_STORY_CHOOSE_SPHERE
        flows = db_get_flows(sid)
        if not flows:
            await update.message.reply_text("Потоков нет.", reply_markup=story_main_keyboard())
            return STATE_STORY_MENU
        await update.message.reply_text("Активные потоки:", reply_markup=story_flows_keyboard(flows))
        return STATE_STORY_FLOWS_MENU
    return STATE_STORY_MENU

async def handle_story_choose_sphere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=story_main_keyboard())
        return STATE_STORY_MENU
    sphere = db_get_sphere_by_name(text)
    if not sphere:
        await update.message.reply_text("Сфера не найдена.")
        return STATE_STORY_CHOOSE_SPHERE
    context.user_data["story_sphere_id"]   = sphere.id
    context.user_data["story_sphere_name"] = sphere.name

    if context.user_data.pop("story_flows_mode", False):
        flows = db_get_flows(sphere.id)
        if not flows:
            await update.message.reply_text("Потоков нет.", reply_markup=story_main_keyboard())
            return STATE_STORY_MENU
        await update.message.reply_text("Активные потоки:", reply_markup=story_flows_keyboard(flows))
        return STATE_STORY_FLOWS_MENU

    templates = db_get_templates(sphere.id)
    users_cnt = db_count_users(sphere.id)
    await update.message.reply_text(
        f"Сфера: {sphere.name}\n"
        f"Пользователей в базе: {users_cnt}\n\n"
        f"Выберите шаблон или создайте новый:",
        reply_markup=story_template_keyboard(templates),
    )
    return STATE_STORY_TEMPLATE_MENU

async def handle_story_template_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=story_main_keyboard())
        return STATE_STORY_MENU
    if text == "➕ Новый шаблон":
        await update.message.reply_text("Введите название шаблона:", reply_markup=ReplyKeyboardRemove())
        return STATE_STORY_NEW_TEMPLATE_NAME

    # Выбор существующего шаблона
    name = text.replace("📋 ", "")
    sid  = context.user_data.get("story_sphere_id")
    templates = db_get_templates(sid)
    tmpl = next((t for t in templates if t.name == name), None)
    if not tmpl:
        await update.message.reply_text("Шаблон не найден.")
        return STATE_STORY_TEMPLATE_MENU

    context.user_data["story_template_id"] = tmpl.id

    # Выбор режима
    modes = db_get_modes()
    await update.message.reply_text("Выберите режим:", reply_markup=story_mode_keyboard(modes))
    return STATE_STORY_CHOOSE_MODE

async def handle_story_new_template_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Введите название:"); return STATE_STORY_NEW_TEMPLATE_NAME
    sid = context.user_data.get("story_sphere_id")
    tid = db_create_template(sid, name)
    context.user_data["story_template_id"] = tid
    await update.message.reply_text(
        "Отправьте фото для истории (можно несколько по одному).\n"
        "Когда закончите — напишите <b>готово</b>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("готово")]], resize_keyboard=True),
    )
    context.user_data["story_photos"] = []
    return STATE_STORY_ADD_PHOTO

async def handle_story_add_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.lower() == "готово":
        photos = context.user_data.get("story_photos", [])
        if not photos:
            await update.message.reply_text("Добавьте хотя бы одно фото!")
            return STATE_STORY_ADD_PHOTO
        tid = context.user_data.get("story_template_id")
        db_update_template(tid, photo_ids=json.dumps(photos, ensure_ascii=False))
        await update.message.reply_text(
            f"✅ Добавлено фото: {len(photos)}\n\n"
            f"Введите варианты подписи (каждый с новой строки):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_STORY_ADD_TEXT

    if update.message.photo:
        file_id = update.message.photo[-1].file_id
        context.user_data["story_photos"].append(file_id)
        cnt = len(context.user_data["story_photos"])
        await update.message.reply_text(f"📷 Фото {cnt} добавлено. Отправьте ещё или напишите «готово».")
        return STATE_STORY_ADD_PHOTO

    await update.message.reply_text("Отправьте фото или напишите «готово».")
    return STATE_STORY_ADD_PHOTO

async def handle_story_add_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texts = [t.strip() for t in update.message.text.strip().splitlines() if t.strip()]
    if not texts:
        await update.message.reply_text("Введите хотя бы один текст:"); return STATE_STORY_ADD_TEXT
    tid = context.user_data.get("story_template_id")
    db_update_template(tid, texts=json.dumps(texts, ensure_ascii=False))
    await update.message.reply_text(
        f"✅ Текстов: {len(texts)}\n\nВведите текст кнопки (например «Написать мне»):\n"
        f"Или напишите «пропустить» если кнопка не нужна.",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("пропустить")]], resize_keyboard=True),
    )
    return STATE_STORY_BUTTON_TEXT

async def handle_story_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.lower() == "пропустить":
        modes = db_get_modes()
        await update.message.reply_text("Выберите режим:", reply_markup=story_mode_keyboard(modes))
        return STATE_STORY_CHOOSE_MODE
    context.user_data["story_button_text"] = text
    await update.message.reply_text(
        "Введите ваш username (без @) чтобы сформировать ссылку на ЛС.\n"
        "Например: myusername",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_STORY_BUTTON_MSG

async def handle_story_button_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().lstrip("@")
    msg_text = context.user_data.get("story_button_text", "Написать")
    url      = f"https://t.me/{username}"
    tid      = context.user_data.get("story_template_id")
    db_update_template(tid, button_text=msg_text, button_url=url)
    await update.message.reply_text(
        f"Ссылка: {url}\n\nВыберите положение кнопки:",
        reply_markup=story_pos_keyboard(),
    )
    return STATE_STORY_BUTTON_POS

async def handle_story_button_pos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    pos_map = {"⬆️ Вверху": "top", "⏺ По центру": "center", "⬇️ Внизу": "bottom"}
    pos = pos_map.get(text, "bottom")
    tid = context.user_data.get("story_template_id")
    db_update_template(tid, button_pos=pos)
    modes = db_get_modes()
    await update.message.reply_text("✅ Шаблон сохранён!\n\nВыберите режим:", reply_markup=story_mode_keyboard(modes))
    return STATE_STORY_CHOOSE_MODE

async def handle_story_choose_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        sid = context.user_data.get("story_sphere_id")
        templates = db_get_templates(sid)
        await update.message.reply_text("Выберите шаблон:", reply_markup=story_template_keyboard(templates))
        return STATE_STORY_TEMPLATE_MENU
    modes = db_get_modes()
    mode  = next((m for m in modes if m.name == text), None)
    if not mode:
        await update.message.reply_text("Выберите режим из списка:", reply_markup=story_mode_keyboard(modes))
        return STATE_STORY_CHOOSE_MODE
    context.user_data["story_mode_id"] = mode.id
    await update.message.reply_text(
        f"Режим: {mode.name}\n"
        f"Отметок: {mode.mentions_min}–{mode.mentions_max}\n"
        f"Интервал: {mode.interval_min}–{mode.interval_max} мин\n"
        f"Сторис в батче: {mode.stories_per_batch}\n"
        f"Лимит в день: {mode.daily_limit}\n\n"
        f"Выберите время работы:",
        reply_markup=story_time_keyboard(),
    )
    return STATE_STORY_CHOOSE_TIME

async def handle_story_choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        modes = db_get_modes()
        await update.message.reply_text("Выберите режим:", reply_markup=story_mode_keyboard(modes))
        return STATE_STORY_CHOOSE_MODE

    end_time = None
    if text == "⏱ 20 минут":
        end_time = datetime.utcnow() + timedelta(minutes=20)
    elif text == "📅 1 день":
        end_time = datetime.utcnow() + timedelta(days=1)
    elif text == "♾ До отключения":
        end_time = None
    elif text == "🗓 Своя дата":
        await update.message.reply_text(
            "Введите дату и время в формате:\nДД.ММ.ГГГГ ЧЧ:ММ\n\nПример: 25.12.2025 18:00",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_STORY_CUSTOM_TIME

    return await _launch_flow(update, context, end_time)

async def handle_story_custom_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        end_time = datetime.strptime(update.message.text.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await update.message.reply_text("Неверный формат. Введите как: 25.12.2025 18:00")
        return STATE_STORY_CUSTOM_TIME
    return await _launch_flow(update, context, end_time)

async def _launch_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, end_time: Optional[datetime]):
    """Финальный предпросмотр и запуск потока."""
    sid  = context.user_data.get("story_sphere_id")
    tid  = context.user_data.get("story_template_id")
    mid  = context.user_data.get("story_mode_id")

    tmpl = db_get_template(tid)
    mode = db_get_mode(mid)

    photos = json.loads(tmpl.photo_ids or "[]")
    texts  = json.loads(tmpl.texts or "[]")

    time_str = end_time.strftime("%d.%m.%Y %H:%M") if end_time else "до отключения"
    preview  = (
        f"📋 Шаблон: {tmpl.name}\n"
        f"📷 Фото: {len(photos)} шт.\n"
        f"💬 Текстов: {len(texts)} вариантов\n"
        f"🔗 Кнопка: {tmpl.button_url or 'нет'}\n"
        f"📍 Позиция кнопки: {tmpl.button_pos}\n\n"
        f"⚙️ Режим: {mode.name}\n"
        f"⏱ Время работы: {time_str}\n"
        f"👥 Пользователей в базе: {db_count_users(sid)}\n\n"
        f"Запустить поток?"
    )
    await update.message.reply_text(preview, reply_markup=confirm_keyboard())
    context.user_data["story_end_time"] = end_time
    return STATE_STORY_FLOWS_MENU

async def handle_story_flows_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "✅ Опубликовать":
        sid      = context.user_data.get("story_sphere_id")
        tid      = context.user_data.get("story_template_id")
        mid      = context.user_data.get("story_mode_id")
        end_time = context.user_data.get("story_end_time")

        if not all([sid, tid, mid]):
            await update.message.reply_text("Ошибка. Начните заново.", reply_markup=story_main_keyboard())
            return STATE_STORY_MENU

        flow_id = db_create_flow(sid, tid, mid, end_time)
        task    = asyncio.create_task(_run_flow(flow_id))
        _active_flows[flow_id] = task

        await update.message.reply_text(
            f"🚀 Поток #{flow_id} запущен!\n"
            f"Можете продолжать работу с ботом.",
            reply_markup=story_main_keyboard(),
        )
        return STATE_STORY_MENU

    if text == "❌ Отмена":
        await update.message.reply_text("Отменено.", reply_markup=story_main_keyboard())
        return STATE_STORY_MENU

    # Остановка потока по кнопке
    if text.startswith("🟢 Поток #") or text.startswith("🔴 Поток #"):
        try:
            flow_id = int(re.search(r"#(\d+)", text).group(1))
            flow    = db_get_flow(flow_id)
            if flow and flow.status == "running":
                db_stop_flow(flow_id)
                if flow_id in _active_flows:
                    _active_flows[flow_id].cancel()
                    _active_flows.pop(flow_id, None)
                logs = json.loads(flow.logs or "[]")
                log_text = "\n".join(logs[-10:]) if logs else "нет логов"
                await update.message.reply_text(
                    f"🔴 Поток #{flow_id} остановлен\n\n"
                    f"📊 Опубликовано: {flow.stories_sent} ист.\n"
                    f"👥 Отмечено: {flow.users_tagged} чел.\n\n"
                    f"Последние логи:\n{log_text}",
                    reply_markup=story_main_keyboard(),
                )
            else:
                logs = json.loads(flow.logs or "[]") if flow else []
                log_text = "\n".join(logs[-10:]) if logs else "нет логов"
                await update.message.reply_text(
                    f"Поток #{flow_id}\n"
                    f"Статус: {flow.status if flow else 'не найден'}\n"
                    f"Опубликовано: {flow.stories_sent if flow else 0}\n\n"
                    f"Логи:\n{log_text}",
                    reply_markup=story_main_keyboard(),
                )
        except Exception:
            await update.message.reply_text("Ошибка.", reply_markup=story_main_keyboard())
        return STATE_STORY_MENU

    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=story_main_keyboard())
        return STATE_STORY_MENU

    return STATE_STORY_FLOWS_MENU

async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используйте кнопки меню.", reply_markup=main_menu_keyboard())
    return STATE_MAIN_MENU

# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_MAIN_MENU:              [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)],
            STATE_SPHERE_MENU:            [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sphere_menu)],
            STATE_ADD_SPHERE_NAME:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_sphere_name)],
            STATE_AFTER_STAGE:            [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_after_stage)],
            STATE_ADD_STAGE_DESC:         [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_desc)],
            STATE_ADD_STAGE_SCRIPT:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_script)],
            STATE_ADD_GROUP_KEYWORDS:     [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_group_keywords)],
            STATE_ADD_USER_KEYWORDS:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_user_keywords)],
            STATE_SHOW_CLIENT:            [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_show_client)],
            STATE_EDIT_SCRIPT_CHOOSE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_script_choose)],
            STATE_EDIT_SCRIPT_DESC:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_script_desc)],
            STATE_EDIT_SCRIPT_TEXT:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_script_text)],
            STATE_EDIT_KEYS_GROUP:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_keys_group)],
            STATE_EDIT_KEYS_USER:         [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_keys_user)],
            STATE_REDO_STAGE_DESC:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_redo_stage_desc)],
            STATE_REDO_STAGE_SCRIPT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_redo_stage_script)],
            STATE_STORY_MENU:             [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_menu)],
            STATE_STORY_CHOOSE_SPHERE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_choose_sphere)],
            STATE_STORY_TEMPLATE_MENU:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_template_menu)],
            STATE_STORY_NEW_TEMPLATE_NAME:[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_new_template_name)],
            STATE_STORY_ADD_PHOTO:        [MessageHandler(filters.PHOTO | filters.TEXT,    handle_story_add_photo)],
            STATE_STORY_ADD_TEXT:         [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_add_text)],
            STATE_STORY_BUTTON_TEXT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_button_text)],
            STATE_STORY_BUTTON_MSG:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_button_msg)],
            STATE_STORY_BUTTON_POS:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_button_pos)],
            STATE_STORY_CHOOSE_MODE:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_choose_mode)],
            STATE_STORY_CHOOSE_TIME:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_choose_time)],
            STATE_STORY_CUSTOM_TIME:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_custom_time)],
            STATE_STORY_FLOWS_MENU:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_flows_menu)],
        },
        fallbacks=[CommandHandler("start", cmd_start), MessageHandler(filters.ALL, fallback)],
        allow_reentry=True,
    )
    app.add_handler(conv)

    async def on_startup(application):
        asyncio.create_task(background_worker())
        # Восстанавливаем активные потоки после перезапуска
        for sphere in db_get_spheres():
            for flow in db_get_flows(sphere.id):
                if flow.status == "running":
                    logger.info("Restoring flow #%d", flow.id)
                    task = asyncio.create_task(_run_flow(flow.id))
                    _active_flows[flow.id] = task

    app.post_init = on_startup
    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()

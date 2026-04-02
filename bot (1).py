"""
Telegram-бот: поиск клиентов + автопостинг сторис.
Стек: python-telegram-bot + Telethon + Pyrogram + SQLAlchemy + SQLite

КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ:
- Бот НЕ ищет группы сам — пользователь вводит ссылки вручную
- Парсинг: все пользователи за 2 года, без фильтра, боты удаляются сразу
- Фильтрация только при запуске потока
- Мульти-аккаунты в одном потоке с весовым распределением
- Полная аналитика аккаунтов и потоков
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
    String, Text, UniqueConstraint, create_engine, text,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User

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

DATABASE_URL          = "sqlite:///bot_data.db"
MAX_CLIENTS           = 150
LOW_CLIENTS           = 50
CRITICAL_CLIENTS      = 20
GROUP_CLIENT_LIMIT    = 150
CLIENT_TTL_DAYS       = 30
MESSAGE_LOOKBACK_DAYS = 730   # 2 года
BG_CHECK_INTERVAL     = 120

BUTTON_POSITIONS = {"top": 15.0, "center": 50.0, "bottom": 80.0}
BOT_KEYWORDS     = ["bot", "support", "notify", "news", "official", "info", "help", "admin", "channel"]

# ===========================================================================
# DATABASE
# ===========================================================================

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class Sphere(Base):
    __tablename__ = "spheres"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String,  nullable=False)
    group_links   = Column(Text,    nullable=False, default="[]")   # ссылки на группы — вводит пользователь
    user_keywords = Column(Text,    nullable=False, default="[]")   # ключевые слова для фильтрации
    # Устаревшие поля — оставляем для совместимости с существующей БД
    group_keywords = Column(Text,   nullable=True,  default="[]")
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


class UsersDB(Base):
    """Широкая база ВСЕХ пользователей без фильтрации."""
    __tablename__ = "users_db"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    sphere_id  = Column(Integer, ForeignKey("spheres.id"), nullable=False)
    user_id    = Column(String,  nullable=False)
    username   = Column(String,  nullable=True)
    first_name = Column(String,  nullable=True)
    last_name  = Column(String,  nullable=True)
    bio        = Column(Text,    nullable=True)
    found_at   = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("sphere_id", "user_id", name="uq_sphere_user"),)


class TgAccount(Base):
    """Telegram аккаунты для публикации историй."""
    __tablename__ = "tg_accounts"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    phone          = Column(String,  nullable=False, unique=True)
    username       = Column(String,  nullable=True)
    session_string = Column(Text,    nullable=False)
    is_active      = Column(Boolean, default=True)
    stories_today  = Column(Integer, default=0)
    stories_total  = Column(Integer, default=0)
    tags_total     = Column(Integer, default=0)
    avg_interval   = Column(Integer, default=0)   # средний интервал в минутах
    last_story_at  = Column(DateTime, nullable=True)
    created_at     = Column(DateTime, default=datetime.utcnow)
    flow_accounts  = relationship("FlowAccount", back_populates="account", cascade="all, delete-orphan")


class StoryTemplate(Base):
    __tablename__ = "story_templates"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    sphere_id   = Column(Integer, ForeignKey("spheres.id"), nullable=False)
    name        = Column(String,  nullable=False)
    photo_ids   = Column(Text,    nullable=False, default="[]")
    texts       = Column(Text,    nullable=False, default="[]")
    button_text = Column(String,  nullable=True)
    button_url  = Column(String,  nullable=True)
    button_pos  = Column(String,  nullable=False, default="bottom")
    created_at  = Column(DateTime, default=datetime.utcnow)
    flows = relationship("StoryFlow", back_populates="template", cascade="all, delete-orphan")


class StoryMode(Base):
    __tablename__ = "story_modes"
    id                = Column(Integer, primary_key=True, autoincrement=True)
    name              = Column(String,  nullable=False)
    filter_percent    = Column(Integer, nullable=False, default=100)
    mentions_min      = Column(Integer, nullable=False, default=3)
    mentions_max      = Column(Integer, nullable=False, default=6)
    interval_min      = Column(Integer, nullable=False, default=20)
    interval_max      = Column(Integer, nullable=False, default=30)
    stories_per_batch = Column(Integer, nullable=False, default=5)
    rest_minutes_min  = Column(Integer, nullable=False, default=120)
    rest_minutes_max  = Column(Integer, nullable=False, default=180)
    daily_limit       = Column(Integer, nullable=False, default=30)
    flows = relationship("StoryFlow", back_populates="mode")


class StoryFlow(Base):
    __tablename__ = "story_flows"
    id            = Column(Integer,  primary_key=True, autoincrement=True)
    sphere_id     = Column(Integer,  ForeignKey("spheres.id"),         nullable=False)
    template_id   = Column(Integer,  ForeignKey("story_templates.id"), nullable=False)
    mode_id       = Column(Integer,  ForeignKey("story_modes.id"),     nullable=False)
    status        = Column(String,   nullable=False, default="running")
    end_time      = Column(DateTime, nullable=True)
    stories_sent  = Column(Integer,  default=0)
    users_tagged  = Column(Integer,  default=0)
    created_at    = Column(DateTime, default=datetime.utcnow)
    logs          = Column(Text,     nullable=False, default="[]")
    template      = relationship("StoryTemplate", back_populates="flows")
    mode          = relationship("StoryMode",     back_populates="flows")
    flow_accounts = relationship("FlowAccount",   back_populates="flow", cascade="all, delete-orphan")


class FlowAccount(Base):
    """Связь поток ↔ аккаунт с % нагрузки."""
    __tablename__ = "flow_accounts"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    flow_id    = Column(Integer, ForeignKey("story_flows.id"),  nullable=False)
    account_id = Column(Integer, ForeignKey("tg_accounts.id"), nullable=False)
    load_pct   = Column(Integer, nullable=False, default=100)
    flow    = relationship("StoryFlow",  back_populates="flow_accounts")
    account = relationship("TgAccount",  back_populates="flow_accounts")
    __table_args__ = (UniqueConstraint("flow_id", "account_id", name="uq_flow_account"),)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    # Безопасная миграция существующих БД
    with engine.connect() as conn:
        for sql in [
            "ALTER TABLE spheres ADD COLUMN user_keywords TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE spheres ADD COLUMN group_links TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE spheres ADD COLUMN group_keywords TEXT",
            "ALTER TABLE tg_accounts ADD COLUMN avg_interval INTEGER DEFAULT 0",
        ]:
            try: conn.execute(text(sql)); conn.commit()
            except OperationalError: pass
    _seed_modes()


def _seed_modes() -> None:
    with SessionLocal() as s:
        if s.query(StoryMode).count() == 0:
            s.add_all([
                StoryMode(name="🟢 Безопасный",  filter_percent=100, mentions_min=3,  mentions_max=6,
                          interval_min=20, interval_max=30, stories_per_batch=5,
                          rest_minutes_min=120, rest_minutes_max=180, daily_limit=30),
                StoryMode(name="🟡 Умеренный",   filter_percent=60,  mentions_min=6,  mentions_max=12,
                          interval_min=10, interval_max=15, stories_per_batch=10,
                          rest_minutes_min=60,  rest_minutes_max=120, daily_limit=60),
                StoryMode(name="🔴 Быстрый",     filter_percent=0,   mentions_min=12, mentions_max=20,
                          interval_min=2,  interval_max=5,  stories_per_batch=20,
                          rest_minutes_min=30,  rest_minutes_max=60,  daily_limit=100),
            ])
            s.commit()

# ===========================================================================
# DB HELPERS
# ===========================================================================

def db_get_spheres():
    with SessionLocal() as s:
        rows = s.query(Sphere).all(); s.expunge_all(); return rows

def db_get_sphere(sid: int) -> Optional[Sphere]:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.id == sid).first()
        if r: s.expunge(r); return r

def db_get_sphere_by_name(name: str) -> Optional[Sphere]:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.name == name).first()
        if r: s.expunge(r); return r

def db_add_sphere(name: str, group_links: list, user_keywords: list) -> int:
    with SessionLocal() as s:
        obj = Sphere(
            name=name,
            group_links=json.dumps(group_links, ensure_ascii=False),
            user_keywords=json.dumps(user_keywords, ensure_ascii=False),
            group_keywords="[]",
        )
        s.add(obj); s.commit(); return obj.id

def db_delete_sphere(sid: int) -> None:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.id == sid).first()
        if r: s.delete(r); s.commit()

def db_update_sphere(sid: int, group_links: list = None, user_keywords: list = None) -> None:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.id == sid).first()
        if r:
            if group_links   is not None: r.group_links   = json.dumps(group_links,   ensure_ascii=False)
            if user_keywords is not None: r.user_keywords = json.dumps(user_keywords, ensure_ascii=False)
            s.commit()

def db_add_stage(sid, desc, script) -> None:
    with SessionLocal() as s:
        cnt = s.query(Stage).filter(Stage.sphere_id == sid).count()
        s.add(Stage(sphere_id=sid, position=cnt+1, description=desc, script=script)); s.commit()

def db_get_stages(sid):
    with SessionLocal() as s:
        rows = s.query(Stage).filter(Stage.sphere_id == sid).order_by(Stage.position).all()
        s.expunge_all(); return rows

def db_update_stage(stage_id: int, desc: str, script: str) -> None:
    with SessionLocal() as s:
        r = s.query(Stage).filter(Stage.id == stage_id).first()
        if r: r.description = desc; r.script = script; s.commit()

def db_delete_stages(sid: int) -> None:
    with SessionLocal() as s:
        s.query(Stage).filter(Stage.sphere_id == sid).delete(); s.commit()

def db_get_user_keywords(sid: int) -> list:
    with SessionLocal() as s:
        r = s.query(Sphere.user_keywords).filter(Sphere.id == sid).first()
        return json.loads(r[0] or "[]") if r else []

def db_get_group_links(sid: int) -> list:
    with SessionLocal() as s:
        r = s.query(Sphere.group_links).filter(Sphere.id == sid).first()
        return json.loads(r[0] or "[]") if r else []

# --- Groups & Clients ---

def db_save_groups(sid, sname, links) -> int:
    saved = 0
    with SessionLocal() as s:
        for link in links:
            s.add(Group(sphere_id=sid, sphere_name=sname, group_link=link, parsed=False, shown_count=0))
            try: s.flush(); saved += 1
            except IntegrityError: s.rollback()
        s.commit()
    return saved

def db_get_all_group_usernames(sid: int) -> list:
    with SessionLocal() as s:
        rows = s.query(Group.group_link).filter(Group.sphere_id == sid).all()
    return [l.rstrip("/").split("/")[-1] for (l,) in rows if l.rstrip("/").split("/")[-1]]

def db_mark_group_parsed(gl: str) -> None:
    with SessionLocal() as s:
        g = s.query(Group).filter(Group.group_link == gl).first()
        if g: g.parsed = True; s.commit()

def db_increment_group_shown(gu: str, sid: int) -> None:
    link = f"https://t.me/{gu}"
    with SessionLocal() as s:
        g = s.query(Group).filter(Group.group_link == link, Group.sphere_id == sid).first()
        if g:
            g.shown_count = (g.shown_count or 0) + 1
            if g.shown_count >= GROUP_CLIENT_LIMIT: s.delete(g)
            s.commit()

def db_cleanup_old_clients() -> None:
    cutoff = datetime.utcnow() - timedelta(days=CLIENT_TTL_DAYS)
    with SessionLocal() as s:
        s.query(Client).filter(Client.found_at < cutoff).delete(); s.commit()

def db_count_available_clients(sid: int) -> int:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        return s.query(Client).filter(Client.sphere_id == sid, Client.used == False).count()

def db_get_random_client(sid: int) -> Optional[Client]:
    db_cleanup_old_clients()
    with SessionLocal() as s:
        grps = s.query(Client.group_username).filter(Client.sphere_id == sid, Client.used == False).distinct().all()
        if not grps: return None
        gu  = random.choice(grps)[0]
        row = s.query(Client).filter(Client.sphere_id == sid, Client.used == False, Client.group_username == gu).order_by(Client.id).first()
        if row: s.expunge(row); return row

def db_mark_client_used(cid: int) -> Optional[str]:
    with SessionLocal() as s:
        c = s.query(Client).filter(Client.id == cid).first()
        if c: c.used = True; g = c.group_username; s.commit(); return g

def db_is_username_known(sid: int, username: str) -> bool:
    with SessionLocal() as s:
        return s.query(Client).filter(Client.sphere_id == sid, Client.username == username).first() is not None

def db_save_clients(sid, gu, usernames) -> int:
    saved = 0
    with SessionLocal() as s:
        for u in usernames:
            s.add(Client(sphere_id=sid, username=u, group_username=gu))
            try: s.flush(); saved += 1
            except IntegrityError: s.rollback()
        s.commit()
    return saved

def db_get_round_robin_index(sid: int) -> int:
    with SessionLocal() as s:
        r = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sid).first()
        return r.last_group_index if r else 0

def db_set_round_robin_index(sid: int, idx: int) -> None:
    with SessionLocal() as s:
        r = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sid).first()
        if r: r.last_group_index = idx
        else: s.add(GroupRoundRobin(sphere_id=sid, last_group_index=idx))
        s.commit()

# --- users_db ---

def db_save_user(sid, user_id, username, first_name, last_name, bio) -> bool:
    with SessionLocal() as s:
        s.add(UsersDB(sphere_id=sid, user_id=user_id, username=username,
                      first_name=first_name, last_name=last_name, bio=bio))
        try: s.flush(); s.commit(); return True
        except IntegrityError: s.rollback(); return False

def db_get_all_users(sid: int) -> list:
    with SessionLocal() as s:
        rows = s.query(UsersDB).filter(UsersDB.sphere_id == sid).all()
        s.expunge_all(); return rows

def db_get_filtered_users(sid: int, keywords: list) -> list:
    all_u = db_get_all_users(sid)
    if not keywords: return all_u
    res = []
    for u in all_u:
        h = " ".join(filter(None, [u.bio, u.username, u.first_name, u.last_name])).lower()
        if any(k.lower() in h for k in keywords): res.append(u)
    return res

def db_count_users(sid: int) -> int:
    with SessionLocal() as s:
        return s.query(UsersDB).filter(UsersDB.sphere_id == sid).count()

# --- Accounts ---

def db_get_accounts() -> list:
    with SessionLocal() as s:
        rows = s.query(TgAccount).all(); s.expunge_all(); return rows

def db_get_account(aid: int) -> Optional[TgAccount]:
    with SessionLocal() as s:
        r = s.query(TgAccount).filter(TgAccount.id == aid).first()
        if r: s.expunge(r); return r

def db_add_account(phone: str, username: str, session_string: str) -> int:
    with SessionLocal() as s:
        ex = s.query(TgAccount).filter(TgAccount.phone == phone).first()
        if ex:
            ex.session_string = session_string; ex.username = username; s.commit(); return ex.id
        obj = TgAccount(phone=phone, username=username, session_string=session_string)
        s.add(obj); s.commit(); return obj.id

def db_update_account_stats(aid: int, stories_delta: int = 0, tags_delta: int = 0, interval_min: int = 0) -> None:
    with SessionLocal() as s:
        r = s.query(TgAccount).filter(TgAccount.id == aid).first()
        if r:
            r.stories_today = (r.stories_today or 0) + stories_delta
            r.stories_total = (r.stories_total or 0) + stories_delta
            r.tags_total    = (r.tags_total    or 0) + tags_delta
            if interval_min: r.avg_interval = interval_min
            r.last_story_at = datetime.utcnow()
            s.commit()

def db_get_account_load_pct(aid: int) -> int:
    with SessionLocal() as s:
        fas = (s.query(FlowAccount).join(StoryFlow)
               .filter(FlowAccount.account_id == aid, StoryFlow.status == "running").all())
        return min(100, sum(fa.load_pct for fa in fas))

def db_get_account_flow_count(aid: int) -> tuple:
    with SessionLocal() as s:
        total  = s.query(FlowAccount).filter(FlowAccount.account_id == aid).count()
        active = (s.query(FlowAccount).join(StoryFlow)
                  .filter(FlowAccount.account_id == aid, StoryFlow.status == "running").count())
        return total, active

def db_get_account_flows(aid: int) -> list:
    """Все потоки аккаунта с деталями."""
    with SessionLocal() as s:
        fas = s.query(FlowAccount).filter(FlowAccount.account_id == aid).all()
        result = []
        for fa in fas:
            flow = s.query(StoryFlow).filter(StoryFlow.id == fa.flow_id).first()
            if flow: result.append({"flow_id": flow.id, "status": flow.status, "load_pct": fa.load_pct, "stories_sent": flow.stories_sent})
        return result

# --- Templates ---

def db_create_template(sid: int, name: str) -> int:
    with SessionLocal() as s:
        obj = StoryTemplate(sphere_id=sid, name=name)
        s.add(obj); s.commit(); return obj.id

def db_get_templates(sid: int) -> list:
    with SessionLocal() as s:
        rows = s.query(StoryTemplate).filter(StoryTemplate.sphere_id == sid).all()
        s.expunge_all(); return rows

def db_get_template(tid: int) -> Optional[StoryTemplate]:
    with SessionLocal() as s:
        r = s.query(StoryTemplate).filter(StoryTemplate.id == tid).first()
        if r: s.expunge(r); return r

def db_update_template(tid: int, **kwargs) -> None:
    with SessionLocal() as s:
        r = s.query(StoryTemplate).filter(StoryTemplate.id == tid).first()
        if r:
            for k, v in kwargs.items(): setattr(r, k, v)
            s.commit()

# --- Modes & Flows ---

def db_get_modes() -> list:
    with SessionLocal() as s:
        rows = s.query(StoryMode).all(); s.expunge_all(); return rows

def db_get_mode(mid: int) -> Optional[StoryMode]:
    with SessionLocal() as s:
        r = s.query(StoryMode).filter(StoryMode.id == mid).first()
        if r: s.expunge(r); return r

def db_create_flow(sid, tid, mid, end_time) -> int:
    with SessionLocal() as s:
        obj = StoryFlow(sphere_id=sid, template_id=tid, mode_id=mid, end_time=end_time)
        s.add(obj); s.commit(); return obj.id

def db_add_flow_account(fid: int, aid: int, pct: int) -> None:
    with SessionLocal() as s:
        s.add(FlowAccount(flow_id=fid, account_id=aid, load_pct=pct))
        try: s.commit()
        except IntegrityError: s.rollback()

def db_get_flow_accounts(fid: int) -> list:
    with SessionLocal() as s:
        rows = s.query(FlowAccount).filter(FlowAccount.flow_id == fid).all()
        s.expunge_all(); return rows

def db_get_flows(sid: int) -> list:
    with SessionLocal() as s:
        rows = s.query(StoryFlow).filter(StoryFlow.sphere_id == sid).all()
        s.expunge_all(); return rows

def db_get_flow(fid: int) -> Optional[StoryFlow]:
    with SessionLocal() as s:
        r = s.query(StoryFlow).filter(StoryFlow.id == fid).first()
        if r: s.expunge(r); return r

def db_stop_flow(fid: int) -> None:
    with SessionLocal() as s:
        r = s.query(StoryFlow).filter(StoryFlow.id == fid).first()
        if r: r.status = "stopped"; s.commit()

def db_update_flow_stats(fid: int, sent: int, tagged: int, log_entry: str) -> None:
    with SessionLocal() as s:
        r = s.query(StoryFlow).filter(StoryFlow.id == fid).first()
        if r:
            r.stories_sent = sent; r.users_tagged = tagged
            logs = json.loads(r.logs or "[]")
            logs.append(f"{datetime.utcnow().strftime('%H:%M:%S')} — {log_entry}")
            r.logs = json.dumps(logs[-50:], ensure_ascii=False)
            s.commit()

# ===========================================================================
# TELETHON (парсинг)
# ===========================================================================

# Основной Pyrogram клиент для парсинга (SESSION_STRING)
_main_pyro_client: Optional[PyrogramClient] = None

async def ensure_pyro_parser() -> PyrogramClient:
    """Возвращает основной Pyrogram клиент для парсинга."""
    global _main_pyro_client
    if _main_pyro_client is None:
        _main_pyro_client = PyrogramClient(
            name="main_parser",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING,
            in_memory=True,
        )
    if not _main_pyro_client.is_connected:
        await _main_pyro_client.start()
    return _main_pyro_client


async def get_pyro_parser(session_string: str) -> Optional[PyrogramClient]:
    """Создаёт Pyrogram клиент из session string для парсинга."""
    try:
        client = PyrogramClient(
            name=f"parser_{abs(hash(session_string[:20])) % 999999}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
        )
        await client.start()
        return client
    except Exception as exc:
        logger.error("[PARSER] Failed to start client: %s", exc)
        return None

# ===========================================================================
# PYROGRAM (публикация историй)
# ===========================================================================

_pyrogram_clients: dict = {}

async def get_pyrogram_client(account: TgAccount) -> Optional[PyrogramClient]:
    """
    Возвращает Pyrogram клиент для публикации историй.
    session_string в формате Pyrogram.
    """
    aid = account.id
    if aid not in _pyrogram_clients:
        try:
            client = PyrogramClient(
                name=f"pyro_{aid}",
                api_id=API_ID, api_hash=API_HASH,
                session_string=account.session_string,
                in_memory=True,
            )
            await client.start()
            _pyrogram_clients[aid] = client
        except Exception as exc:
            logger.error("[PYRO] %s: %s", account.phone, exc)
            return None
    client = _pyrogram_clients.get(aid)
    if not client:
        return None
    if not client.is_connected:
        try: await client.start()
        except Exception: return None
    return client

# ===========================================================================
# ПАРСИНГ ПОЛЬЗОВАТЕЛЕЙ
# ===========================================================================

_collecting: set = set()


def _remove_client_from_users_db(sphere_id: int, client_id: int) -> None:
    """
    Удаляет пользователя из users_db когда его показали в ручном поиске.
    Это не даст ему попасть в отметки сторис.
    """
    with SessionLocal() as s:
        # Находим username клиента
        c = s.query(Client).filter(Client.id == client_id).first()
        if not c or not c.username: return
        # Удаляем из users_db по username
        s.query(UsersDB).filter(
            UsersDB.sphere_id == sphere_id,
            UsersDB.username == c.username,
        ).delete()
        s.commit()


def _is_bot(user) -> bool:
    if getattr(user, "bot", False): return True
    un = (user.username or "").lower()
    return un.endswith("bot") or any(k in un for k in BOT_KEYWORDS)

async def _get_user_bio_pyro(client: PyrogramClient, uid: int) -> str:
    """Получает bio пользователя через Pyrogram."""
    try:
        user = await client.get_users(uid)
        return user.bio or "" if user else ""
    except Exception:
        return ""

async def _collect_one_group(gu: str, sid: int, user_keywords: list, limit: int = 5000) -> int:
    """
    Парсинг через Pyrogram get_chat_history.
    Работает с Pyrogram session string.
    """
    client = await ensure_pyro_parser()
    return await _parse_group_with_pyro(client, gu, sid, user_keywords, limit=limit)


async def _parse_group_with_pyro(
    client: PyrogramClient,
    gu: str,
    sid: int,
    user_keywords: list,
    limit: int = 5000,
    offset_id: int = 0,
) -> int:
    """
    Парсит группу через Pyrogram.
    Собирает всех кто писал, боты удаляются сразу.
    """
    lookback  = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set   = set()
    collected: list = []
    count           = 0
    last_msg_id     = 0

    try:
        # Нормализуем username
        chat = gu if gu.startswith("@") else f"@{gu}"

        async for msg in client.get_chat_history(chat, limit=limit, offset_id=offset_id):
            count += 1
            if count % 100 == 0:
                await asyncio.sleep(0)  # yield event loop

            if not msg.date: continue
            if msg.date.replace(tzinfo=None) < lookback: break

            last_msg_id = msg.id
            sender = msg.from_user
            if not sender: continue

            uid = sender.id
            if uid in seen_ids: continue
            seen_ids.add(uid)

            # Убираем ботов сразу
            if getattr(sender, "is_bot", False): continue
            un_check = (sender.username or "").lower()
            if un_check.endswith("bot") or any(k in un_check for k in BOT_KEYWORDS):
                continue

            username   = sender.username   or ""
            first_name = sender.first_name or ""
            last_name  = sender.last_name  or ""

            bio = ""
            if user_keywords:
                bio = await _get_user_bio_pyro(client, uid)

            # Сохраняем всех в users_db без фильтра
            db_save_user(sid, str(uid), username, first_name, last_name, bio)

            # Для clients — с фильтром
            if username:
                un = username.lower()
                if not db_is_username_known(sid, un):
                    h = " ".join(filter(None, [username, first_name, last_name, bio])).lower()
                    if not user_keywords or any(k.lower() in h for k in user_keywords):
                        collected.append(un)

    except Exception as exc:
        logger.warning("[PYRO PARSE] «%s»: %s", gu, exc)

    saved = db_save_clients(sid, gu, collected)
    db_mark_group_parsed(f"https://t.me/{gu}")
    logger.info("[PYRO] %d unique, %d saved from «%s»", len(seen_ids), saved, gu)
    return saved

async def _collect_group_multi_account(
    group_username: str,
    sid: int,
    user_keywords: list,
    limit_per_account: int = 5000,
) -> int:
    """
    Парсинг одной группы несколькими аккаунтами последовательно.
    Каждый аккаунт парсит свою порцию сообщений и передаёт эстафету следующему.
    Это позволяет обойти лимиты и собрать максимум участников.
    """
    accounts = db_get_accounts()
    if not accounts:
        # Нет аккаунтов — парсим основным SESSION_STRING
        return await _collect_one_group(group_username, sid, user_keywords, limit=limit_per_account)

    total_saved = 0
    offset_id   = 0   # передаём между аккаунтами для продолжения с того же места

    for acc in accounts:
        if not acc.is_active: continue
        saved, last_offset = await _collect_group_with_account(
            group_username=group_username,
            sid=sid,
            user_keywords=user_keywords,
            session_string=acc.session_string,
            limit=limit_per_account,
            start_offset_id=offset_id,
        )
        total_saved += saved
        if last_offset > 0:
            offset_id = last_offset   # следующий аккаунт продолжает отсюда
        logger.info("[MULTI] @%s parsed «%s»: +%d clients (offset=%d)",
                    acc.username or acc.phone, group_username, saved, offset_id)
        await asyncio.sleep(2)   # пауза между аккаунтами

    # Если аккаунтов нет или все неактивны — используем основной
    if not any(a.is_active for a in accounts):
        total_saved = await _collect_one_group(group_username, sid, user_keywords, limit=limit_per_account)

    db_mark_group_parsed(f"https://t.me/{group_username}")
    return total_saved


async def _collect_group_with_account(
    group_username: str,
    sid: int,
    user_keywords: list,
    session_string: str,
    limit: int,
    start_offset_id: int = 0,
) -> tuple:
    """
    Парсит группу конкретным Pyrogram аккаунтом начиная с offset_id.
    Возвращает (кол-во сохранённых, последний msg_id).
    """
    lookback  = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set   = set()
    collected: list = []
    last_offset     = start_offset_id
    count           = 0

    try:
        client = await get_pyro_parser(session_string)
        if not client:
            return 0, start_offset_id

        chat = group_username if group_username.startswith("@") else f"@{group_username}"

        async for msg in client.get_chat_history(chat, limit=limit, offset_id=start_offset_id):
            count += 1
            if count % 100 == 0:
                await asyncio.sleep(0)

            if not msg.date: continue
            if msg.date.replace(tzinfo=None) < lookback: break

            last_offset = msg.id

            sender = msg.from_user
            if not sender: continue

            uid = sender.id
            if uid in seen_ids: continue
            seen_ids.add(uid)

            if getattr(sender, "is_bot", False): continue
            un_check = (sender.username or "").lower()
            if un_check.endswith("bot") or any(k in un_check for k in BOT_KEYWORDS):
                continue

            username   = sender.username   or ""
            first_name = sender.first_name or ""
            last_name  = sender.last_name  or ""

            bio = ""
            if user_keywords:
                bio = await _get_user_bio_pyro(client, uid)

            db_save_user(sid, str(uid), username, first_name, last_name, bio)

            if username:
                un = username.lower()
                if not db_is_username_known(sid, un):
                    h = " ".join(filter(None, [username, first_name, last_name, bio])).lower()
                    if not user_keywords or any(k.lower() in h for k in user_keywords):
                        collected.append(un)

        await client.stop()

    except Exception as exc:
        logger.warning("[MULTI PYRO] «%s»: %s", group_username, exc)

    saved = db_save_clients(sid, group_username, collected)
    return saved, last_offset


async def _fill_buffer_bg(sid: int, progress_message=None) -> None:
    """
    Фоновый сбор клиентов через несколько аккаунтов.
    Каждый аккаунт парсит группу с того места где закончил предыдущий.
    """
    if sid in _collecting: return
    _collecting.add(sid)
    try:
        sphere = db_get_sphere(sid)
        if not sphere: return
        ukws = db_get_user_keywords(sid)

        # Загружаем группы из ссылок
        raw_links = db_get_group_links(sid)
        links = []
        for l in raw_links:
            l = l.strip()
            if not l.startswith("https://"): l = f"https://t.me/{l.lstrip('@').lstrip('t.me/').lstrip('/')}"
            links.append(l)
        if links:
            db_save_groups(sid, sphere.name, links)

        groups = db_get_all_group_usernames(sid)
        if not groups:
            if progress_message:
                try: await progress_message.edit_text("⚠️ Нет групп. Добавьте ссылки через ➕ Добавить группы.")
                except Exception: pass
            return

        accounts = db_get_accounts()
        acc_info = f"{len(accounts)} аккаунтов" if accounts else "основной аккаунт"
        if progress_message:
            try: await progress_message.edit_text(
                f"👥 Парсю {len(groups)} групп через {acc_info}...\n\n✅ Бот работает!"
            )
            except Exception: pass

        idx, att  = db_get_round_robin_index(sid), 0
        last_edit = asyncio.get_event_loop().time()

        while db_count_available_clients(sid) < MAX_CLIENTS and att < len(groups):
            gu = groups[idx % len(groups)]
            # Используем мульти-аккаунтный парсинг
            await _collect_group_multi_account(gu, sid, ukws)
            idx = (idx + 1) % len(groups); att += 1

            now = asyncio.get_event_loop().time()
            if progress_message and (now - last_edit) >= 3.0:
                cnt = db_count_available_clients(sid)
                try:
                    await progress_message.edit_text(
                        f"👥 Парсю группы...\n\nОбработано: {att}/{len(groups)}\n"
                        f"Найдено: {cnt}\n\n✅ Бот работает!"
                    )
                    last_edit = now
                except Exception: pass
            await asyncio.sleep(0)

        db_set_round_robin_index(sid, idx)
        total = db_count_available_clients(sid)
        if progress_message:
            try: await progress_message.edit_text(
                f"✅ Готово! Найдено клиентов: {total}\nНажмите «➡️ Следующий»."
            )
            except Exception: pass

    except Exception as exc:
        logger.error("[FILL] sid=%d: %s", sid, exc)
    finally:
        _collecting.discard(sid)

async def background_worker() -> None:
    logger.info("Background worker started.")
    while True:
        try:
            for sphere in db_get_spheres():
                sid = sphere.id
                cc  = db_count_available_clients(sid)
                if cc < LOW_CLIENTS and sid not in _collecting:
                    asyncio.create_task(_fill_buffer_bg(sid))
                await asyncio.sleep(1)
        except Exception as exc:
            logger.error("[BG] %s", exc)
        await asyncio.sleep(BG_CHECK_INTERVAL)

# ===========================================================================
# УМНОЕ РАСПРЕДЕЛЕНИЕ НАГРУЗКИ
# ===========================================================================

def _smart_distribute(accounts: list) -> list:
    """Распределяет нагрузку по весам обратно пропорционально текущей нагрузке."""
    if not accounts: return []
    if len(accounts) == 1: return [(accounts[0].id, 100)]
    weights = [(acc, max(1, 100 - db_get_account_load_pct(acc.id))) for acc in accounts]
    total_w = sum(w for _, w in weights)
    result  = []; remaining = 100
    for i, (acc, w) in enumerate(weights):
        if i == len(weights) - 1: pct = remaining
        else: pct = round(w / total_w * 100); remaining -= pct
        result.append((acc.id, max(1, pct)))
    return result

# ===========================================================================
# ПУБЛИКАЦИЯ ИСТОРИЙ
# ===========================================================================

_active_flows: dict = {}


def _pick_users(sid: int, mode: StoryMode, keywords: list, count: int) -> list:
    """Фильтрация при запуске (Этап 2)."""
    all_u = db_get_all_users(sid)
    fil_u = db_get_filtered_users(sid, keywords)
    if not all_u: return []
    selected = []
    for _ in range(count):
        if mode.filter_percent == 100:   pool = fil_u or all_u
        elif mode.filter_percent == 0:   pool = all_u
        else: pool = fil_u if (fil_u and random.random() < mode.filter_percent / 100) else all_u
        if pool: selected.append(random.choice(pool))
    # Дедупликация
    seen = set(); unique = []
    for u in selected:
        if u.user_id not in seen: seen.add(u.user_id); unique.append(u)
    return unique

def _weighted_choice(load_list: list) -> int:
    """Выбор аккаунта по весу (вероятностно, не по очереди)."""
    total = sum(pct for _, pct in load_list)
    if total == 0: return load_list[0][0]
    r = random.randint(0, total - 1); acc_sum = 0
    for aid, pct in load_list:
        acc_sum += pct
        if r < acc_sum: return aid
    return load_list[-1][0]

async def _publish_story(account: TgAccount, template: StoryTemplate, users: list) -> bool:
    """
    Публикует историю через Pyrogram.
    Если session string в формате Telethon — логируем и пропускаем
    (публикация историй через Telethon не поддерживается).
    """
    try:
        pyro = await get_pyrogram_client(account)
        if not pyro:
            logger.warning("[STORY] %s: нет Pyrogram клиента (нужна Pyrogram сессия)", account.phone)
            return False

        photo_ids = json.loads(template.photo_ids or "[]")
        texts     = json.loads(template.texts     or "[]")
        if not photo_ids: return False

        caption = random.choice(texts) if texts else ""
        if users:
            mentions = [f"@{u.username}" for u in users if u.username]
            mid = len(mentions) // 2
            caption = f"{caption}\n\n{' '.join(mentions[:mid])}\n{' '.join(mentions[mid:])}".strip()

        media_areas = []
        if template.button_url:
            y = BUTTON_POSITIONS.get(template.button_pos, 80.0)
            media_areas.append(pyrogram_types.MediaAreaUrl(
                coordinates=pyrogram_types.MediaAreaCoordinates(x=30.0, y=y, width=40.0, height=8.0, rotation=0.0),
                url=template.button_url,
            ))

        await pyro.send_story(
            chat_id="me",
            media=random.choice(photo_ids),
            caption=caption,
            period=86400,
            privacy=pyrogram_enums.StoriesPrivacyRules.PUBLIC,
            media_areas=media_areas if media_areas else None,
        )
        return True
    except Exception as exc:
        logger.error("[STORY] %s: %s", account.phone, exc)
        return False

async def _run_flow(flow_id: int) -> None:
    logger.info("[FLOW %d] started", flow_id)
    sent = 0; tagged = 0; daily = 0
    day_reset = datetime.utcnow().date()

    while True:
        flow = db_get_flow(flow_id)
        if not flow or flow.status != "running": break
        if flow.end_time and datetime.utcnow() >= flow.end_time:
            db_stop_flow(flow_id); break

        today = datetime.utcnow().date()
        if today != day_reset: daily = 0; day_reset = today

        mode = db_get_mode(flow.mode_id); template = db_get_template(flow.template_id)
        if not mode or not template: break

        fas = db_get_flow_accounts(flow_id)
        if not fas: break
        load_list = [(fa.account_id, fa.load_pct) for fa in fas]

        if daily >= mode.daily_limit:
            db_update_flow_stats(flow_id, sent, tagged, f"Лимит {mode.daily_limit}/день. Жду 1ч.")
            await asyncio.sleep(3600); continue

        keywords = db_get_user_keywords(flow.sphere_id)

        for _ in range(mode.stories_per_batch):
            flow = db_get_flow(flow_id)
            if not flow or flow.status != "running": break
            if flow.end_time and datetime.utcnow() >= flow.end_time: break
            if daily >= mode.daily_limit: break

            # Весовой выбор аккаунта (вероятностно)
            aid     = _weighted_choice(load_list)
            account = db_get_account(aid)
            if not account or not account.is_active: continue

            n     = random.randint(mode.mentions_min, mode.mentions_max)
            users = _pick_users(flow.sphere_id, mode, keywords, n)
            ok    = await _publish_story(account, template, users)

            if ok:
                sent += 1; daily += 1; tagged += len(users)
                interval = random.randint(mode.interval_min * 60, mode.interval_max * 60)
                db_update_account_stats(aid, stories_delta=1, tags_delta=len(users), interval_min=interval // 60)
                log = f"История #{sent} | @{account.username or account.phone} | {len(users)} отметок"
                db_update_flow_stats(flow_id, sent, tagged, log)
                db_update_flow_stats(flow_id, sent, tagged, f"Жду {interval // 60} мин")
                await asyncio.sleep(interval)

        rest = random.randint(mode.rest_minutes_min * 60, mode.rest_minutes_max * 60)
        db_update_flow_stats(flow_id, sent, tagged, f"Пауза {rest // 60} мин после батча")
        await asyncio.sleep(rest)

    _active_flows.pop(flow_id, None)
    logger.info("[FLOW %d] done. sent=%d tagged=%d", flow_id, sent, tagged)

# ===========================================================================
# STATES
# ===========================================================================

(
    # Основное
    S_MAIN,          # 0
    S_SPHERE,        # 1
    S_ADD_SPHERE,    # 2
    S_AFTER_STAGE,   # 3
    S_STAGE_DESC,    # 4
    S_STAGE_SCRIPT,  # 5
    S_ADD_GRP_LINKS, # 6  ← ввод ссылок на группы
    S_ADD_USR_KWS,   # 7  ← ввод ключевых слов
    S_SHOW_CLIENT,   # 8
    S_EDIT_CHOOSE,   # 9
    S_EDIT_DESC,     # 10
    S_EDIT_TEXT,     # 11
    S_EDIT_GRP,      # 12
    S_EDIT_USR,      # 13
    S_REDO_DESC,     # 14
    S_REDO_SCRIPT,   # 15
    # Сторис главное
    S_STORY,         # 16
    # Аккаунты
    S_ACCOUNTS,      # 17
    S_ACC_PHONE,     # 18
    S_ACC_CODE,      # 19
    S_ACC_PASS,      # 20
    # Запуск потока
    S_RUN_SPHERE,    # 21
    S_RUN_TMPL,      # 22
    S_NEW_TMPL,      # 23
    S_PHOTO,         # 24
    S_TEXT,          # 25
    S_BTN_TEXT,      # 26
    S_BTN_MSG,       # 27
    S_BTN_POS,       # 28
    S_MODE,          # 29
    S_TIME,          # 30
    S_CUSTOM_TIME,   # 31
    # Выбор аккаунтов + нагрузка
    S_SEL_ACCS,      # 32
    S_LOAD_DIST,     # 33
    S_LOAD_MANUAL,   # 34
    # Потоки
    S_FLOWS,         # 35
    S_FLOW_DETAIL,   # 36
    # Новые состояния
    S_EDIT_DATA,     # 37  ← меню "Изменить данные"
    S_ADD_GRP_NOW,   # 38  ← добавление групп из экрана клиента
    S_CONFIRM_DEL,   # 39  ← подтверждение удаления сферы
) = range(40)

# ===========================================================================
# KEYBOARDS
# ===========================================================================

def kb(buttons, resize=True):
    """Быстрое создание клавиатуры из списка списков строк."""
    rows = [[KeyboardButton(b) for b in row] for row in buttons]
    return ReplyKeyboardMarkup(rows, resize_keyboard=resize)

def kb_main():
    return kb([["🖐🏻 Ручной поиск"], ["📸 Отметки сторис"]])

def kb_sphere():
    btns = [[s.name] for s in db_get_spheres()]
    btns += [["➕ Добавить сферу"], ["🏠 Главное меню"]]
    return kb(btns)

def kb_client():
    return kb([
        ["➡️ Следующий"],
        ["✏️ Изменить данные", "➕ Добавить группы"],
        ["🏠 Главное меню",   "🗑 Удалить сферу"],
    ])

def kb_edit_data():
    """Меню выбора что изменить."""
    return kb([["✏️ Изменить скрипт"], ["🔑 Изменить ключи"], ["🔙 Назад"]])

def kb_after_stage():
    return kb([["+Этап", "Продолжить"]])

def kb_edit_script(stages):
    btns = []; row = []
    for i in range(len(stages)):
        row.append(str(i + 1))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    btns.append(["Все", "Назад"])
    return kb(btns)

def kb_story():
    return kb([["▶️ Запустить"], ["🛠 Потоки", "👤 Аккаунты"], ["🏠 Главное меню"]])

def kb_accounts(accounts: list):
    btns = []
    for acc in accounts:
        load   = db_get_account_load_pct(acc.id)
        status = "🔴" if load > 70 else "🟢"
        btns.append([f"{status} @{acc.username or acc.phone} [id:{acc.id}]"])
    btns.append(["➕ Добавить аккаунт"])
    btns.append(["🔙 Назад"])
    return kb(btns)

def kb_templates(templates):
    btns = [[f"📋 {t.name}"] for t in templates]
    btns += [["➕ Новый шаблон"], ["🔙 Назад"]]
    return kb(btns)

def kb_modes(modes):
    btns = [[m.name] for m in modes]
    btns.append(["🔙 Назад"])
    return kb(btns)

def kb_time():
    return kb([["⏱ 20 минут", "📅 1 день"], ["♾ До отключения", "🗓 Своя дата"], ["🔙 Назад"]])

def kb_btn_pos():
    return kb([["⬆️ Вверху", "⏺ По центру", "⬇️ Внизу"]])

def kb_select_accs(accounts: list, selected_ids: list):
    btns = []
    for acc in accounts:
        mark = "✅" if acc.id in selected_ids else "◻️"
        load = db_get_account_load_pct(acc.id)
        st   = "🔴" if load > 70 else "🟢"
        btns.append([f"{mark} {st} @{acc.username or acc.phone} нагрузка {load}% [id:{acc.id}]"])
    btns.append(["➡️ Продолжить"])
    btns.append(["🔙 Назад"])
    return kb(btns)

def kb_load(load: list, show_change: bool = True):
    btns = []
    if show_change and len(load) > 1: btns.append(["🔧 Изменить нагрузку"])
    btns.append(["➕ Добавить аккаунт", "➡️ Продолжить"])
    btns.append(["🔙 Назад"])
    return kb(btns)

def kb_load_dist():
    return kb([["✋ Ручной"], ["⚖️ Равномерно"], ["🧠 Умное распределение"], ["🔙 Назад"]])

def kb_flows(flows):
    btns = []
    for f in flows:
        s = "🟢" if f.status == "running" else ("⚪" if f.status == "done" else "🔴")
        btns.append([f"{s} Поток #{f.id} — {f.stories_sent} сторис [id:{f.id}]"])
    btns.append(["🔙 Назад"])
    return kb(btns)

def kb_flow_detail():
    return kb([["🛑 СТОП", "📊 Нагрузка аккаунтов"], ["📋 Логи работы", "Подробнее"], ["🔙 Назад"]])

def kb_confirm():
    return kb([["✅ Запустить", "❌ Отмена"]])

# ===========================================================================
# HELPERS
# ===========================================================================

def _extract_id(text: str) -> Optional[int]:
    m = re.search(r"\[id:(\d+)\]", text)
    return int(m.group(1)) if m else None

def _format_load(load: list) -> str:
    lines = ["Распределение нагрузки:"]
    for aid, pct in load:
        a = db_get_account(aid)
        if a: lines.append(f"@{a.username or a.phone} 🛠️{pct}%")
    return "\n".join(lines)

def _minutes_ago(dt: datetime) -> str:
    if not dt: return "—"
    mins = int((datetime.utcnow() - dt).total_seconds() / 60)
    if mins < 60: return f"{mins} мин назад"
    h = mins // 60; return f"{h}ч {mins % 60}м назад"

def _work_duration(created_at: datetime) -> str:
    mins = int((datetime.utcnow() - created_at).total_seconds() / 60)
    d, rem = divmod(mins, 1440); h, m = divmod(rem, 60)
    parts = []
    if d: parts.append(f"{d}д")
    if h: parts.append(f"{h}ч")
    parts.append(f"{m}м")
    return " ".join(parts)

def _make_progress_bar(current: int, total: int, width: int = 16) -> str:
    pct    = min(100, int(current / total * 100)) if total else 0
    filled = int(width * pct / 100)
    return f"{'Ӏ' * filled}{' ' * (width - filled)} {pct}%"

# ===========================================================================
# HANDLERS — основное
# ===========================================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Добро пожаловать!", reply_markup=kb_main())
    return S_MAIN

async def h_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🖐🏻 Ручной поиск":
        await update.message.reply_text("Выберите сферу:", reply_markup=kb_sphere())
        return S_SPHERE
    if t == "📸 Отметки сторис":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story())
        return S_STORY
    return S_MAIN

async def h_sphere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🏠 Главное меню":
        await update.message.reply_text("Главное меню:", reply_markup=kb_main())
        return S_MAIN
    if t == "➕ Добавить сферу":
        context.user_data["ns"] = {"stages": []}
        await update.message.reply_text("Введите название сферы:", reply_markup=ReplyKeyboardRemove())
        return S_ADD_SPHERE
    sphere = db_get_sphere_by_name(t)
    if sphere:
        context.user_data["sid"]   = sphere.id
        context.user_data["sname"] = sphere.name
        if db_count_available_clients(sphere.id) > 0:
            if sphere.id not in _collecting:
                asyncio.create_task(_fill_buffer_bg(sphere.id))
            return await show_next_client(update, context)
        prog = await update.message.reply_text(
            f"🔍 Начинаю парсинг для «{sphere.name}»...\n\n✅ Бот работает!",
            reply_markup=kb_client(),
        )
        asyncio.create_task(_fill_buffer_bg(sphere.id, progress_message=prog))
        return S_SHOW_CLIENT
    await update.message.reply_text("Сфера не найдена.", reply_markup=kb_sphere())
    return S_SPHERE

async def show_next_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid   = context.user_data.get("sid")
    sname = context.user_data.get("sname")
    if not sid:
        await update.message.reply_text("Ошибка.", reply_markup=kb_main())
        return S_MAIN
    row = db_get_random_client(sid)
    if not row:
        status = "Идёт парсинг..." if sid in _collecting else "Запускаю парсинг..."
        await update.message.reply_text(f"⏳ Клиентов пока нет. {status}", reply_markup=kb_client())
        if sid not in _collecting: asyncio.create_task(_fill_buffer_bg(sid))
        return S_SHOW_CLIENT
    context.user_data["cid"] = row.id
    stages = db_get_stages(sid)
    lines  = [f"<b>@{row.username}</b>\n", f"<b>{sname}</b>"]
    for i, st in enumerate(stages, 1):
        lines.append(f"\n<b>{i} этап — {st.description}</b>")
        lines.append(f"<blockquote>{st.script}</blockquote>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=kb_client())
    if db_count_available_clients(sid) < LOW_CLIENTS and sid not in _collecting:
        asyncio.create_task(_fill_buffer_bg(sid))
    return S_SHOW_CLIENT

async def h_show_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text

    def _mark():
        if cid := context.user_data.get("cid"):
            sid = context.user_data.get("sid")
            gu  = db_mark_client_used(cid)
            if gu and sid:
                db_increment_group_shown(gu, sid)
                # Удаляем пользователя из users_db чтобы не попал в сторис
                _remove_client_from_users_db(sid, cid)

    if t == "➡️ Следующий":
        _mark(); return await show_next_client(update, context)

    if t == "🏠 Главное меню":
        _mark()
        for k in ("sid","sname","cid"): context.user_data.pop(k, None)
        await update.message.reply_text("Главное меню:", reply_markup=kb_main())
        return S_MAIN

    if t == "🗑 Удалить сферу":
        sname = context.user_data.get("sname", "")
        await update.message.reply_text(
            f"Вы уверены что хотите удалить сферу «{sname}»?\n\nВсе данные будут потеряны.",
            reply_markup=kb([["✅ Да, удалить", "❌ Отмена"]]),
        )
        return S_CONFIRM_DEL

    if t == "✏️ Изменить данные":
        await update.message.reply_text("Что хотите изменить?", reply_markup=kb_edit_data())
        return S_EDIT_DATA

    if t == "➕ Добавить группы":
        sid = context.user_data.get("sid")
        sp  = db_get_sphere(sid)
        gl  = json.loads(sp.group_links or "[]") if sp else []
        gl_text = "\n".join(gl) if gl else "(нет)"
        await update.message.reply_text(
            f"Добавленные группы/чаты:\n{gl_text}\n\nДобавьте группы/чаты списком:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return S_ADD_GRP_NOW

    return S_SHOW_CLIENT


async def h_confirm_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение удаления сферы."""
    t = update.message.text
    if t == "✅ Да, удалить":
        sid   = context.user_data.get("sid")
        sname = context.user_data.get("sname", "")
        if sid:
            db_delete_sphere(sid)
            _collecting.discard(sid)
            for k in ("sid","sname","cid"): context.user_data.pop(k, None)
            await update.message.reply_text(f"🗑 Сфера «{sname}» удалена.", reply_markup=kb_sphere())
            return S_SPHERE
    await update.message.reply_text("Отменено.", reply_markup=kb_client())
    return S_SHOW_CLIENT


async def h_edit_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню 'Изменить данные' — выбор что именно менять."""
    t = update.message.text

    if t == "🔙 Назад":
        await update.message.reply_text("Возврат:", reply_markup=kb_client())
        return S_SHOW_CLIENT

    if t == "✏️ Изменить скрипт":
        sid    = context.user_data.get("sid")
        stages = db_get_stages(sid)
        if not stages:
            await update.message.reply_text("Этапов нет.", reply_markup=kb_client())
            return S_SHOW_CLIENT
        context.user_data["edit_stages"] = [{"id": st.id, "desc": st.description, "script": st.script} for st in stages]
        await update.message.reply_text("Какой этап?", reply_markup=kb_edit_script(stages))
        return S_EDIT_CHOOSE

    if t == "🔑 Изменить ключи":
        sid = context.user_data.get("sid")
        sp  = db_get_sphere(sid)
        if sp:
            gl = json.loads(sp.group_links or "[]")
            await update.message.reply_text(
                f"Текущие ссылки ({len(gl)} шт.):\n{chr(10).join(gl) if gl else '(нет)'}\n\nВведите новые ссылки (каждая с новой строки):",
                reply_markup=ReplyKeyboardRemove(),
            )
            return S_EDIT_GRP

    return S_EDIT_DATA


async def h_add_grp_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавление групп прямо из экрана клиента."""
    raw  = update.message.text.strip()
    new_links = [l.strip() for l in raw.splitlines() if l.strip()]
    if not new_links:
        await update.message.reply_text("Введите хотя бы одну ссылку:")
        return S_ADD_GRP_NOW

    sid = context.user_data.get("sid")
    sp  = db_get_sphere(sid)
    if sp:
        existing = json.loads(sp.group_links or "[]")
        # Нормализуем новые ссылки
        normalized = []
        for l in new_links:
            if not l.startswith("https://"):
                l = f"https://t.me/{l.lstrip('@').lstrip('t.me/').lstrip('/')}"
            if l not in existing:
                normalized.append(l)
        combined = existing + normalized
        db_update_sphere(sid, group_links=combined)
        # Запускаем парсинг новых групп в фоне
        if normalized:
            asyncio.create_task(_fill_buffer_bg(sid))

    await update.message.reply_text(
        f"Группы добавлены✅",
        reply_markup=kb_main(),
    )
    return S_MAIN

async def h_edit_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text; stages = context.user_data.get("edit_stages", [])
    if t == "Назад":
        await update.message.reply_text("Возврат:", reply_markup=kb_client()); return S_SHOW_CLIENT
    if t == "Все":
        sid = context.user_data.get("sid"); db_delete_stages(sid)
        context.user_data["ns"] = {"stages": [], "redo": True}
        await update.message.reply_text("Введите пояснение 1-го этапа:", reply_markup=ReplyKeyboardRemove())
        return S_REDO_DESC
    try:
        idx = int(t) - 1
        if 0 <= idx < len(stages):
            context.user_data["edit_idx"] = idx; st = stages[idx]
            await update.message.reply_text(f"Этап {idx+1}: «{st['desc']}»\n\nНовое пояснение:", reply_markup=ReplyKeyboardRemove())
            return S_EDIT_DESC
    except ValueError: pass
    await update.message.reply_text("Выберите номер:", reply_markup=kb_edit_script(stages))
    return S_EDIT_CHOOSE

async def h_edit_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_new_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите новый скрипт:"); return S_EDIT_TEXT

async def h_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script   = update.message.text.strip()
    new_desc = context.user_data.pop("edit_new_desc", "")
    idx      = context.user_data.get("edit_idx", 0)
    stages   = context.user_data.get("edit_stages", [])
    if 0 <= idx < len(stages): db_update_stage(stages[idx]["id"], new_desc, script)
    await update.message.reply_text("Данные изменены ✅", reply_markup=kb_main())
    return S_MAIN

async def h_redo_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["ns"]["current_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:"); return S_REDO_SCRIPT

async def h_redo_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip(); desc = context.user_data["ns"].pop("current_desc", "")
    context.user_data["ns"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=kb_after_stage())
    return S_AFTER_STAGE

async def h_edit_grp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    links = [l.strip() for l in update.message.text.strip().splitlines() if l.strip()]
    context.user_data["new_links"] = links
    sid = context.user_data.get("sid"); sp = db_get_sphere(sid)
    ukws = json.loads(sp.user_keywords or "[]") if sp else []
    await update.message.reply_text(
        f"Текущие ключевые слова:\n{chr(10).join(ukws) if ukws else '(не заданы)'}\n\n"
        f"Введите новые ключевые слова (или «нет»):",
        reply_markup=ReplyKeyboardRemove(),
    )
    return S_EDIT_USR

async def h_edit_usr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = [] if raw.lower() in ("нет","no","-","") else [k.strip() for k in raw.splitlines() if k.strip()]
    sid  = context.user_data.get("sid")
    links = context.user_data.pop("new_links", [])
    db_update_sphere(sid, group_links=links, user_keywords=ukws)
    await update.message.reply_text("✅ Обновлено!", reply_markup=kb_main())
    return S_MAIN

async def h_add_sphere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name: await update.message.reply_text("Введите название:"); return S_ADD_SPHERE
    context.user_data["ns"]["name"] = name
    await update.message.reply_text("Добавьте этапы переписки:", reply_markup=kb_after_stage())
    return S_AFTER_STAGE

async def h_after_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t  = update.message.text; ns = context.user_data.get("ns", {})
    if t == "+Этап":
        await update.message.reply_text("Введите пояснение этапа:", reply_markup=ReplyKeyboardRemove())
        return S_REDO_DESC if ns.get("redo") else S_STAGE_DESC
    if t == "Продолжить":
        if ns.get("redo"):
            sid = context.user_data.get("sid")
            for st in ns.get("stages", []): db_add_stage(sid, st["desc"], st["script"])
            context.user_data.pop("ns", None)
            await update.message.reply_text("Данные изменены ✅", reply_markup=kb_main())
            return S_MAIN
        await update.message.reply_text(
            "📌 Шаг 1 из 2: Введите ссылки на группы/чаты для парсинга\n"
            "(каждая с новой строки)\n\nПример:\nhttps://t.me/groupname\nt.me/another",
            reply_markup=ReplyKeyboardRemove(),
        )
        return S_ADD_GRP_LINKS
    return S_AFTER_STAGE

async def h_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["ns"]["current_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:"); return S_STAGE_SCRIPT

async def h_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip(); desc = context.user_data["ns"].pop("current_desc", "")
    context.user_data["ns"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=kb_after_stage())
    return S_AFTER_STAGE

async def h_add_grp_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    links = [l.strip() for l in update.message.text.strip().splitlines() if l.strip()]
    if not links:
        await update.message.reply_text("Введите хотя бы одну ссылку:"); return S_ADD_GRP_LINKS
    context.user_data["ns"]["group_links"] = links
    await update.message.reply_text(
        "📌 Шаг 2 из 2: Ключевые слова для фильтрации пользователей\n"
        "(username, имя, bio)\n\nЕсли не нужно — напишите «нет»:",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("нет")]], resize_keyboard=True),
    )
    return S_ADD_USR_KWS

async def h_add_usr_kws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = [] if raw.lower() in ("нет","no","-","") else [k.strip() for k in raw.splitlines() if k.strip()]
    ns   = context.user_data.get("ns", {})
    name = ns.get("name",""); stages = ns.get("stages",[]); links = ns.get("group_links",[])

    sid = db_add_sphere(name=name, group_links=links, user_keywords=ukws)
    for st in stages: db_add_stage(sid, st["desc"], st["script"])

    kw_info = f"Ключевых слов: {len(ukws)}" if ukws else "Фильтрация: отключена"
    await update.message.reply_text(
        f"💾 Сфера «{name}» создана!\n"
        f"Групп для парсинга: {len(links)}\n{kw_info}\n\n"
        f"🔍 Парсинг запущен в фоне. Бот уже работает!"
    )
    prog = await update.message.reply_text("👥 Начинаю парсинг групп...")
    asyncio.create_task(_fill_buffer_bg(sid, progress_message=prog))
    context.user_data.pop("ns", None)
    await update.message.reply_text("Выберите сферу:", reply_markup=kb_sphere())
    return S_SPHERE

# ===========================================================================
# HANDLERS — 📸 Отметки сторис
# ===========================================================================

async def h_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🏠 Главное меню":
        await update.message.reply_text("Главное меню:", reply_markup=kb_main()); return S_MAIN
    if t == "👤 Аккаунты":
        accounts = db_get_accounts()
        await update.message.reply_text(
            f"👤 Аккаунты ({len(accounts)} шт.)\n\n🟢 — низкая нагрузка  🔴 — высокая нагрузка\n\nНажмите на аккаунт для карточки:",
            reply_markup=kb_accounts(accounts),
        )
        return S_ACCOUNTS
    if t == "▶️ Запустить":
        spheres = db_get_spheres()
        if not spheres:
            await update.message.reply_text("Нет сфер.", reply_markup=kb_story()); return S_STORY
        btns = [[s.name] for s in spheres] + [["🔙 Назад"]]
        await update.message.reply_text("Выберите сферу:", reply_markup=kb(btns))
        return S_RUN_SPHERE
    if t == "🛠 Потоки":
        sid = context.user_data.get("story_sid")
        if not sid:
            spheres = db_get_spheres()
            if not spheres:
                await update.message.reply_text("Нет сфер.", reply_markup=kb_story()); return S_STORY
            btns = [[s.name] for s in spheres] + [["🔙 Назад"]]
            context.user_data["flows_mode"] = True
            await update.message.reply_text("Выберите сферу:", reply_markup=kb(btns))
            return S_RUN_SPHERE
        flows = db_get_flows(sid)
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows))
        return S_FLOWS
    return S_STORY

# ===========================================================================
# HANDLERS — 👤 Аккаунты
# ===========================================================================

async def h_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story()); return S_STORY
    if t == "➕ Добавить аккаунт":
        await update.message.reply_text(
            "Вставьте session string аккаунта.\n\n"
            "Как получить:\n"
            "1. Запустите get_session.py на любом устройстве где есть доступ к Telegram\n"
            "2. Введите телефон и код\n"
            "3. Скопируйте длинную строку и вставьте сюда",
            reply_markup=ReplyKeyboardRemove(),
        )
        return S_ACC_PHONE

    aid = _extract_id(t)
    if aid:
        acc = db_get_account(aid)
        if acc:
            total, active = db_get_account_flow_count(acc.id)
            load          = db_get_account_load_pct(acc.id)
            load_label    = "🔴 Высокий" if load > 70 else ("🟡 Умеренный" if load > 40 else "🟢 Низкий")
            last_story    = _minutes_ago(acc.last_story_at)
            warnings      = []
            if load > 70:            warnings.append("• Высокая частота публикаций")
            if (acc.avg_interval or 0) < 5: warnings.append("• Короткие интервалы")
            if load > 80:            warnings.append("• Возможный риск ограничения")

            flows_info = db_get_account_flows(acc.id)
            heavy_flow = max(flows_info, key=lambda f: f["stories_sent"], default=None) if flows_info else None

            card = (
                f"Аккаунт: @{acc.username or acc.phone}\n"
                f"Статус: {'Активен 🟢' if acc.is_active else 'Неактивен 🔴'}\n\n"
                f"Задействован в потоках: {total}\n"
                f"Активных потоков: {active}\n\n"
                f"📊 Уровень нагрузки: {load_label} ({load}%)\n\n"
                f"⏱ Активность:\n"
                f"Последняя сторис: {last_story}\n\n"
                f"Статистика за сегодня:\n"
                f"Сторис: {acc.stories_today or 0}\n"
                f"Отметок: {acc.tags_total or 0}\n"
                f"Средний интервал: {acc.avg_interval or '—'} мин\n"
            )
            if warnings:
                card += "\n⚠️ Предупреждения:\n" + "\n".join(warnings)
            if heavy_flow:
                card += f"\n\n⚠️ Основная нагрузка: Поток #{heavy_flow['flow_id']}"

            btns = []
            if flows_info:
                btns.append([f"🛠 Потоки аккаунта [acc:{acc.id}]"])
            btns.append(["🔙 Назад"])
            await update.message.reply_text(card, reply_markup=kb(btns))
            context.user_data["viewing_acc_id"] = acc.id
            return S_ACCOUNTS

    # Кнопка "🛠 Потоки аккаунта"
    if "[acc:" in t:
        m = re.search(r"\[acc:(\d+)\]", t)
        if m:
            acc_id = int(m.group(1))
            acc    = db_get_account(acc_id)
            flows  = db_get_account_flows(acc_id)
            lines  = [f"Потоки аккаунта: @{acc.username or acc.phone}\n"]
            active_count = sum(1 for f in flows if f["status"] == "running")
            lines.append(f"Активных: {active_count} / Всего: {len(flows)}\n")
            lines.append("—" * 30)
            for f in flows:
                s = "🟢" if f["status"] == "running" else ("⚪" if f["status"] == "done" else "🔴")
                status_str = "Активен" if f["status"] == "running" else ("Завершён" if f["status"] == "done" else "Остановлен")
                lines.append(f"{s} Поток #{f['flow_id']} ({status_str}) — {f['stories_sent']} сторис")
            heavy = max(flows, key=lambda f: f["stories_sent"], default=None)
            if heavy and heavy["status"] == "running":
                lines.append(f"\n⚠️ Основная нагрузка идёт от Потока #{heavy['flow_id']}")
            await update.message.reply_text("\n".join(lines), reply_markup=kb_accounts(db_get_accounts()))
    return S_ACCOUNTS

async def h_acc_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Добавление аккаунта через session string.
    Принимает строки от Telethon (get_session.py на базе Telethon)
    и от Pyrogram (get_session.py на базе Pyrogram).
    Автоматически определяет формат и проверяет сессию.
    """
    session_str = update.message.text.strip()

    if len(session_str) < 20:
        await update.message.reply_text(
            "❌ Слишком короткая строка.\n\n"
            "Вставьте session string целиком."
        )
        return S_ACC_PHONE

    await update.message.reply_text("⏳ Проверяю сессию...")

    username = None
    phone    = None
    saved_session = session_str

    # Попытка 1: Telethon StringSession
    try:
        tc = TelegramClient(StringSession(session_str), API_ID, API_HASH)
        await tc.connect()
        if await tc.is_user_authorized():
            me       = await tc.get_me()
            username = me.username or me.first_name or str(me.id)
            phone    = str(getattr(me, "phone", None) or me.id)
            saved_session = session_str  # сохраняем как есть
            await tc.disconnect()
            logger.info("[ACC] Telethon session OK for @%s", username)
        else:
            await tc.disconnect()
    except Exception as e1:
        logger.info("[ACC] Telethon failed: %s", e1)
        # Попытка 2: Pyrogram session string
        try:
            pyro = PyrogramClient(
                name="check_session",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_str,
                in_memory=True,
            )
            await pyro.start()
            me       = await pyro.get_me()
            username = me.username or me.first_name or str(me.id)
            phone    = str(getattr(me, "phone_number", None) or me.id)
            saved_session = session_str
            await pyro.stop()
            logger.info("[ACC] Pyrogram session OK for @%s", username)
        except Exception as e2:
            logger.info("[ACC] Pyrogram failed: %s", e2)
            # Попытка 3: может строка содержит лишние пробелы/переносы
            clean = session_str.replace("\n", "").replace("\r", "").replace(" ", "")
            if clean != session_str and len(clean) > 20:
                try:
                    tc2 = TelegramClient(StringSession(clean), API_ID, API_HASH)
                    await tc2.connect()
                    if await tc2.is_user_authorized():
                        me       = await tc2.get_me()
                        username = me.username or me.first_name or str(me.id)
                        phone    = str(getattr(me, "phone", None) or me.id)
                        saved_session = clean
                        await tc2.disconnect()
                        logger.info("[ACC] Telethon clean session OK for @%s", username)
                    else:
                        await tc2.disconnect()
                except Exception as e3:
                    logger.info("[ACC] All attempts failed: %s", e3)

    if not username:
        await update.message.reply_text(
            "❌ Не удалось проверить сессию.\n\n"
            "Возможные причины:\n"
            "• Строка скопирована не полностью\n"
            "• Сессия устарела или отозвана\n"
            "• Неверный формат\n\n"
            "Создайте новую сессию через get_session.py и попробуйте снова."
        )
        return S_ACC_PHONE

    db_add_account(phone=phone, username=username, session_string=saved_session)
    await update.message.reply_text(
        f"Аккаунт добавлен✅\n@{username}",
        reply_markup=kb_accounts(db_get_accounts()),
    )
    return S_ACCOUNTS


async def h_acc_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Заглушка — не используется при добавлении через session string
    await update.message.reply_text(
        "Вставьте session string:", reply_markup=ReplyKeyboardRemove()
    )
    return S_ACC_PHONE


async def h_acc_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Заглушка
    await update.message.reply_text(
        "Вставьте session string:", reply_markup=ReplyKeyboardRemove()
    )
    return S_ACC_PHONE


async def _finish_acc(update, context, client, phone: str):
    return S_ACCOUNTS

# ===========================================================================
# HANDLERS — Запуск потока
# ===========================================================================

async def h_run_sphere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story()); return S_STORY
    sphere = db_get_sphere_by_name(t)
    if not sphere:
        await update.message.reply_text("Сфера не найдена."); return S_RUN_SPHERE
    context.user_data["story_sid"] = sphere.id
    if context.user_data.pop("flows_mode", False):
        flows = db_get_flows(sphere.id)
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows)); return S_FLOWS
    templates = db_get_templates(sphere.id)
    await update.message.reply_text(
        f"Сфера: {sphere.name}\nПользователей в базе: {db_count_users(sphere.id)}\n\nВыберите шаблон:",
        reply_markup=kb_templates(templates),
    )
    return S_RUN_TMPL

async def h_run_tmpl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story()); return S_STORY
    if t == "➕ Новый шаблон":
        await update.message.reply_text("Введите название шаблона:", reply_markup=ReplyKeyboardRemove()); return S_NEW_TMPL
    name = t.replace("📋 ", "")
    sid  = context.user_data.get("story_sid")
    tmpl = next((t for t in db_get_templates(sid) if t.name == name), None)
    if not tmpl:
        await update.message.reply_text("Шаблон не найден."); return S_RUN_TMPL
    context.user_data["story_tid"] = tmpl.id
    await update.message.reply_text("Выберите режим:", reply_markup=kb_modes(db_get_modes()))
    return S_MODE

async def h_new_tmpl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name: await update.message.reply_text("Введите название:"); return S_NEW_TMPL
    sid = context.user_data.get("story_sid")
    tid = db_create_template(sid, name)
    context.user_data["story_tid"] = tid
    context.user_data["story_photos"] = []
    await update.message.reply_text(
        "Отправьте фото (можно несколько). Когда закончите — «готово»",
        reply_markup=kb([["готово"]]),
    )
    return S_PHOTO

async def h_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.lower() == "готово":
        photos = context.user_data.get("story_photos", [])
        if not photos: await update.message.reply_text("Добавьте хотя бы одно фото!"); return S_PHOTO
        db_update_template(context.user_data["story_tid"], photo_ids=json.dumps(photos, ensure_ascii=False))
        await update.message.reply_text(f"✅ Фото: {len(photos)}\n\nВведите варианты подписи (каждый с новой строки):", reply_markup=ReplyKeyboardRemove())
        return S_TEXT
    if update.message.photo:
        context.user_data["story_photos"].append(update.message.photo[-1].file_id)
        await update.message.reply_text(f"📷 Фото {len(context.user_data['story_photos'])} добавлено. Ещё или «готово».")
        return S_PHOTO
    await update.message.reply_text("Отправьте фото."); return S_PHOTO

async def h_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texts = [t.strip() for t in update.message.text.strip().splitlines() if t.strip()]
    if not texts: await update.message.reply_text("Введите хотя бы один текст:"); return S_TEXT
    db_update_template(context.user_data["story_tid"], texts=json.dumps(texts, ensure_ascii=False))
    await update.message.reply_text(f"✅ Текстов: {len(texts)}\n\nТекст кнопки (или «пропустить»):", reply_markup=kb([["пропустить"]]))
    return S_BTN_TEXT

async def h_btn_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t.lower() == "пропустить":
        await update.message.reply_text("Выберите режим:", reply_markup=kb_modes(db_get_modes())); return S_MODE
    context.user_data["story_btn_text"] = t
    await update.message.reply_text("Введите ваш username (без @):", reply_markup=ReplyKeyboardRemove())
    return S_BTN_MSG

async def h_btn_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().lstrip("@")
    url = f"https://t.me/{username}"
    db_update_template(context.user_data["story_tid"], button_text=context.user_data.get("story_btn_text",""), button_url=url)
    await update.message.reply_text(f"Ссылка: {url}\n\nПоложение кнопки:", reply_markup=kb_btn_pos())
    return S_BTN_POS

async def h_btn_pos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pos = {"⬆️ Вверху": "top", "⏺ По центру": "center", "⬇️ Внизу": "bottom"}.get(update.message.text, "bottom")
    db_update_template(context.user_data["story_tid"], button_pos=pos)
    await update.message.reply_text("✅ Шаблон сохранён!\n\nВыберите режим:", reply_markup=kb_modes(db_get_modes()))
    return S_MODE

async def h_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🔙 Назад":
        sid = context.user_data.get("story_sid")
        await update.message.reply_text("Шаблон:", reply_markup=kb_templates(db_get_templates(sid))); return S_RUN_TMPL
    mode = next((m for m in db_get_modes() if m.name == t), None)
    if not mode:
        await update.message.reply_text("Выберите из списка:", reply_markup=kb_modes(db_get_modes())); return S_MODE
    context.user_data["story_mid"] = mode.id
    await update.message.reply_text(
        f"Режим: {mode.name}\nОтметок: {mode.mentions_min}–{mode.mentions_max}\n"
        f"Интервал: {mode.interval_min}–{mode.interval_max} мин\nЛимит/день: {mode.daily_limit}\n\n"
        f"Выберите время работы:", reply_markup=kb_time(),
    )
    return S_TIME

async def h_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if t == "🔙 Назад":
        await update.message.reply_text("Режим:", reply_markup=kb_modes(db_get_modes())); return S_MODE
    if t == "⏱ 20 минут":       end = datetime.utcnow() + timedelta(minutes=20)
    elif t == "📅 1 день":       end = datetime.utcnow() + timedelta(days=1)
    elif t == "♾ До отключения": end = None
    elif t == "🗓 Своя дата":
        await update.message.reply_text("Введите: ДД.ММ.ГГГГ ЧЧ:ММ", reply_markup=ReplyKeyboardRemove()); return S_CUSTOM_TIME
    else: end = None
    context.user_data["story_end"] = end
    return await _show_select_accs(update, context)

async def h_custom_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try: end = datetime.strptime(update.message.text.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await update.message.reply_text("Формат: 25.12.2025 18:00"); return S_CUSTOM_TIME
    context.user_data["story_end"] = end
    return await _show_select_accs(update, context)

async def _show_select_accs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает экран выбора аккаунтов — НОВЫЙ ЭТАП после выбора времени."""
    accounts = db_get_accounts()
    if not accounts:
        await update.message.reply_text(
            "⚠️ Нет аккаунтов!\n\nДобавьте через 👤 Аккаунты.",
            reply_markup=kb_story(),
        )
        return S_STORY
    context.user_data["story_sel_ids"] = []
    await update.message.reply_text(
        "Выберите аккаунт:\n\n🟢 — активен\n🔴 — перегружен / нежелательно использовать\n\n"
        "Нажмите на аккаунт чтобы выбрать/снять:",
        reply_markup=kb_select_accs(accounts, []),
    )
    return S_SEL_ACCS

async def h_sel_accs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t        = update.message.text
    accounts = db_get_accounts()
    selected = context.user_data.get("story_sel_ids", [])

    if t == "🔙 Назад":
        await update.message.reply_text("Время работы:", reply_markup=kb_time()); return S_TIME

    if t == "➡️ Продолжить":
        if not selected:
            await update.message.reply_text("Выберите хотя бы один аккаунт!"); return S_SEL_ACCS
        # Начальное распределение — равномерно
        n   = len(selected); pct = 100 // n; rem = 100 - pct * n
        context.user_data["story_load"] = [(aid, pct + (1 if i < rem else 0)) for i, aid in enumerate(selected)]
        return await _show_load_screen(update, context)

    aid = _extract_id(t)
    if aid:
        if aid in selected: selected.remove(aid)
        else: selected.append(aid)
        context.user_data["story_sel_ids"] = selected
        await update.message.reply_text(
            f"Выбрано аккаунтов: {len(selected)}",
            reply_markup=kb_select_accs(accounts, selected),
        )
        return S_SEL_ACCS

    return S_SEL_ACCS

async def _show_load_screen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load = context.user_data.get("story_load", [])
    await update.message.reply_text(_format_load(load), reply_markup=kb_load(load))
    return S_LOAD_DIST

async def h_load_dist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t    = update.message.text
    load = context.user_data.get("story_load", [])

    if t == "🔙 Назад":
        accounts = db_get_accounts(); selected = [aid for aid, _ in load]
        await update.message.reply_text("Выберите аккаунт:", reply_markup=kb_select_accs(accounts, selected))
        return S_SEL_ACCS

    if t == "➡️ Продолжить":
        return await _show_preview(update, context)

    if t == "🔧 Изменить нагрузку":
        await update.message.reply_text(
            f"{_format_load(load)}\n\nРежим перераспределения нагрузки:",
            reply_markup=kb_load_dist(),
        )
        return S_LOAD_DIST

    if t == "➕ Добавить аккаунт":
        # Показываем аккаунты которые ещё не добавлены
        already = [aid for aid, _ in load]
        available = [a for a in db_get_accounts() if a.id not in already]
        if not available:
            await update.message.reply_text("Все аккаунты уже добавлены.")
            return S_LOAD_DIST
        btns = []
        for acc in available:
            l = db_get_account_load_pct(acc.id); s = "🔴" if l > 70 else "🟢"
            btns.append([f"{s} @{acc.username or acc.phone} нагрузка {l}% [id:{acc.id}]"])
        btns.append(["🔙 Назад"])
        context.user_data["adding_to_load"] = True
        await update.message.reply_text("Выберите аккаунт:", reply_markup=kb(btns))
        return S_LOAD_DIST

    if context.user_data.pop("adding_to_load", False):
        aid = _extract_id(t)
        if aid:
            load.append((aid, 0))
            # Пересчитываем равномерно
            n = len(load); pct = 100 // n; rem = 100 - pct * n
            load = [(a, pct + (1 if i < rem else 0)) for i, (a, _) in enumerate(load)]
            context.user_data["story_load"] = load
            return await _show_load_screen(update, context)

    if t == "✋ Ручной":
        context.user_data["load_idx"] = 0
        a = db_get_account(load[0][0])
        await update.message.reply_text(
            f"Выберите % нагрузки для аккаунта @{a.username or a.phone}\nПример: 50",
            reply_markup=ReplyKeyboardRemove(),
        )
        return S_LOAD_MANUAL

    if t == "⚖️ Равномерно":
        n = len(load); pct = 100 // n; rem = 100 - pct * n
        context.user_data["story_load"] = [(aid, pct + (1 if i < rem else 0)) for i, (aid, _) in enumerate(load)]
        await update.message.reply_text("Данные изменены✅")
        return await _show_load_screen(update, context)

    if t == "🧠 Умное распределение":
        accs   = [db_get_account(aid) for aid, _ in load]
        result = _smart_distribute([a for a in accs if a])
        context.user_data["story_load"] = result
        # Показываем с пояснениями
        lines = ["Распределение нагрузки:"]
        for aid, pct in result:
            a    = db_get_account(aid)
            cur  = db_get_account_load_pct(aid)
            risk = "🟢 Аккаунт можно нагружать" if cur < 50 else "🔴 Аккаунт перегружен"
            if a: lines.append(f"@{a.username or a.phone} 🛠️{pct}%\n{risk}")
        await update.message.reply_text("\n".join(lines), reply_markup=kb_load(result))
        return S_LOAD_DIST

    if t == "🔙 Назад":
        return await _show_load_screen(update, context)

    return S_LOAD_DIST

async def h_load_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pct = int(update.message.text.strip())
        if not 0 <= pct <= 100: raise ValueError
    except ValueError:
        await update.message.reply_text("Введите число от 0 до 100:"); return S_LOAD_MANUAL
    load = context.user_data.get("story_load", [])
    idx  = context.user_data.get("load_idx", 0)
    if idx < len(load):
        aid = load[idx][0]; load[idx] = (aid, pct)
        context.user_data["story_load"] = load
    idx += 1; context.user_data["load_idx"] = idx
    if idx < len(load):
        a = db_get_account(load[idx][0])
        await update.message.reply_text(f"% для @{a.username or a.phone}:"); return S_LOAD_MANUAL
    total = sum(p for _, p in load)
    if total != 100:
        await update.message.reply_text(f"⚠️ Сумма = {total}%, нужно 100%. Начнём заново.")
        context.user_data["load_idx"] = 0
        a = db_get_account(load[0][0])
        await update.message.reply_text(f"% для @{a.username or a.phone}:"); return S_LOAD_MANUAL
    await update.message.reply_text("Данные изменены✅")
    return await _show_load_screen(update, context)

async def _show_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid  = context.user_data.get("story_sid")
    tid  = context.user_data.get("story_tid")
    mid  = context.user_data.get("story_mid")
    end  = context.user_data.get("story_end")
    load = context.user_data.get("story_load", [])
    tmpl = db_get_template(tid); mode = db_get_mode(mid)
    photos = json.loads(tmpl.photo_ids or "[]"); texts = json.loads(tmpl.texts or "[]")
    time_str = end.strftime("%d.%m.%Y %H:%M") if end else "до отключения"
    acc_lines = []
    for aid, pct in load:
        a = db_get_account(aid)
        if a: acc_lines.append(f"@{a.username or a.phone} 🛠️{pct}%")

    preview = (
        f"Предпросмотр🔒\n\n"
        f"📋 Шаблон: {tmpl.name}\n"
        f"📷 Фото: {len(photos)} шт. | 💬 Текстов: {len(texts)}\n"
        f"🔗 Кнопка: {tmpl.button_url or 'нет'} ({tmpl.button_pos})\n\n"
        f"{'—'*30}\n\n"
        f"Кнопка\n{tmpl.button_url or '—'}\n\n"
        f"{'—'*30}\n\n"
        f"Режим\n{mode.name}\n"
        f"⏱ Время: {time_str}\n"
        f"👥 Пользователей: {db_count_users(sid)}\n\n"
        f"{'—'*30}\n\n"
        f"Выбранные аккаунты:\n" + "\n".join(acc_lines) + "\n\n"
        f"Запустить поток?"
    )
    await update.message.reply_text(preview, reply_markup=kb_confirm())
    return S_FLOWS

# ===========================================================================
# HANDLERS — Потоки
# ===========================================================================

async def h_flows(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text

    if t == "✅ Запустить":
        sid  = context.user_data.get("story_sid")
        tid  = context.user_data.get("story_tid")
        mid  = context.user_data.get("story_mid")
        end  = context.user_data.get("story_end")
        load = context.user_data.get("story_load", [])
        if not load:
            await update.message.reply_text("Нет аккаунтов!", reply_markup=kb_story()); return S_STORY
        fid = db_create_flow(sid, tid, mid, end)
        for aid, pct in load: db_add_flow_account(fid, aid, pct)
        task = asyncio.create_task(_run_flow(fid))
        _active_flows[fid] = task
        acc_str = ", ".join(f"@{db_get_account(aid).username or db_get_account(aid).phone}" for aid, _ in load if db_get_account(aid))
        await update.message.reply_text(f"🚀 Поток #{fid} запущен!\nАккаунты: {acc_str}", reply_markup=kb_story())
        return S_STORY

    if t == "❌ Отмена":
        await update.message.reply_text("Отменено.", reply_markup=kb_story()); return S_STORY
    if t == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story()); return S_STORY

    fid = _extract_id(t)
    if fid:
        flow = db_get_flow(fid)
        if flow:
            context.user_data["flow_detail_id"] = fid
            fas = db_get_flow_accounts(fid)
            acc_lines = []
            for fa in fas:
                a = db_get_account(fa.account_id)
                if a: acc_lines.append(f"@{a.username or a.phone} 🛠️{fa.load_pct}%")
            detail = (
                f"Поток #{flow.id} {'Активен🟢' if flow.status == 'running' else '🔴'}\n\n"
                f"📷 Опубликованных сторис: {flow.stories_sent}\n"
                f"🧍 Кол-во отмеченных клиентов: {flow.users_tagged}\n"
                f"⏱ Общее время работы: {_work_duration(flow.created_at)}\n"
                f"👀 Просмотры: —\n"
                f"🏆 Успешные клиенты: —\n\n"
                f"Аккаунты в действии потока: {len(fas)}\n" + "\n".join(acc_lines)
            )
            await update.message.reply_text(detail, reply_markup=kb_flow_detail())
            return S_FLOW_DETAIL

    sid = context.user_data.get("story_sid")
    if sid:
        flows = db_get_flows(sid)
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows))
    return S_FLOWS

async def h_flow_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t   = update.message.text
    fid = context.user_data.get("flow_detail_id")
    flow = db_get_flow(fid) if fid else None

    if t == "🔙 Назад":
        sid = context.user_data.get("story_sid")
        flows = db_get_flows(sid) if sid else []
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows)); return S_FLOWS

    if t == "🛑 СТОП" and flow:
        db_stop_flow(fid)
        if fid in _active_flows: _active_flows[fid].cancel(); _active_flows.pop(fid, None)
        await update.message.reply_text(f"🔴 Поток #{fid} остановлен.", reply_markup=kb_story()); return S_STORY

    if t == "📊 Нагрузка аккаунтов" and flow:
        fas   = db_get_flow_accounts(fid)
        lines = ["Распределение нагрузки:"]
        for fa in fas:
            a = db_get_account(fa.account_id)
            if a:
                l = db_get_account_load_pct(a.id); r = "🔴" if l > 70 else ("🟡" if l > 40 else "🟢")
                lines.append(f"@{a.username or a.phone} 🛠️{fa.load_pct}% {r}")
        await update.message.reply_text("\n".join(lines), reply_markup=kb([ ["🔙 Назад"] ]))
        return S_FLOW_DETAIL

    if t == "📋 Логи работы" and flow:
        logs = json.loads(flow.logs or "[]")
        await update.message.reply_text(
            f"Логи работы🚧 #{fid}:\n\n" + ("\n".join(logs[-20:]) if logs else "Логов нет."),
            reply_markup=kb([["🔙 Назад"]]),
        )
        return S_FLOW_DETAIL

    if t == "Подробнее" and flow:
        fas   = db_get_flow_accounts(fid)
        tmpl  = db_get_template(flow.template_id)
        acc_lines = []
        for fa in fas:
            a = db_get_account(fa.account_id)
            if a: acc_lines.append(f"@{a.username or a.phone} 🛠️{fa.load_pct}%")
        texts = json.loads(tmpl.texts or "[]") if tmpl else []
        detail = (
            f"Предпросмотр🔒\n\n"
            f"{chr(10).join(texts[:2]) if texts else '—'}\n\n"
            f"{'—'*30}\n\n"
            f"Кнопка\n{tmpl.button_url if tmpl else '—'}\n\n"
            f"{'—'*30}\n\n"
            f"Режим\n{db_get_mode(flow.mode_id).name if db_get_mode(flow.mode_id) else '—'}\n\n"
            f"{'—'*30}\n\n"
            f"Выбранные аккаунты:\n" + "\n".join(acc_lines)
        )
        await update.message.reply_text(detail, reply_markup=kb([["Полный просмотр", "🔙 Назад"]]))
        return S_FLOW_DETAIL

    return S_FLOW_DETAIL

async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используйте кнопки меню.", reply_markup=kb_main())
    return S_MAIN

# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            S_MAIN:        [MessageHandler(filters.TEXT & ~filters.COMMAND, h_main)],
            S_SPHERE:      [MessageHandler(filters.TEXT & ~filters.COMMAND, h_sphere)],
            S_ADD_SPHERE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_sphere)],
            S_AFTER_STAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_after_stage)],
            S_STAGE_DESC:  [MessageHandler(filters.TEXT & ~filters.COMMAND, h_stage_desc)],
            S_STAGE_SCRIPT:[MessageHandler(filters.TEXT & ~filters.COMMAND, h_stage_script)],
            S_ADD_GRP_LINKS:[MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_grp_links)],
            S_ADD_USR_KWS: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_usr_kws)],
            S_SHOW_CLIENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_show_client)],
            S_EDIT_CHOOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_choose)],
            S_EDIT_DESC:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_desc)],
            S_EDIT_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_text)],
            S_EDIT_GRP:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_grp)],
            S_EDIT_USR:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_usr)],
            S_EDIT_DATA:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_data)],
            S_ADD_GRP_NOW: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_grp_now)],
            S_CONFIRM_DEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_confirm_del)],
            S_REDO_DESC:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_redo_desc)],
            S_REDO_SCRIPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_redo_script)],
            S_STORY:       [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story)],
            S_ACCOUNTS:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_accounts)],
            S_ACC_PHONE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_acc_phone)],
            S_ACC_CODE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_acc_code)],
            S_ACC_PASS:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_acc_pass)],
            S_RUN_SPHERE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, h_run_sphere)],
            S_RUN_TMPL:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_run_tmpl)],
            S_NEW_TMPL:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_new_tmpl)],
            S_PHOTO:       [MessageHandler(filters.PHOTO | filters.TEXT,    h_photo)],
            S_TEXT:        [MessageHandler(filters.TEXT & ~filters.COMMAND, h_text)],
            S_BTN_TEXT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_btn_text)],
            S_BTN_MSG:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_btn_msg)],
            S_BTN_POS:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_btn_pos)],
            S_MODE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, h_mode)],
            S_TIME:        [MessageHandler(filters.TEXT & ~filters.COMMAND, h_time)],
            S_CUSTOM_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_custom_time)],
            S_SEL_ACCS:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_sel_accs)],
            S_LOAD_DIST:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_load_dist)],
            S_LOAD_MANUAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_load_manual)],
            S_FLOWS:       [MessageHandler(filters.TEXT & ~filters.COMMAND, h_flows)],
            S_FLOW_DETAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_flow_detail)],
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
                    _active_flows[flow.id] = asyncio.create_task(_run_flow(flow.id))

    app.post_init = on_startup
    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()

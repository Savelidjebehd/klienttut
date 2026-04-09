"""
Telegram-бот: поиск клиентов + автопостинг сторис с мультиаккаунтами.
Стек: python-telegram-bot + Telethon + Pyrogram + SQLAlchemy + SQLite
"""

import asyncio
import os
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

from telethon import TelegramClient, functions
from telethon.tl.types import (
    InputMediaUploadedPhoto,
    InputMediaAreaUrl,
    MediaAreaCoordinates,
    InputPrivacyValueAllowAll,
)
from telethon.sessions import StringSession

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
MAX_GROUPS            = 100
MAX_CLIENTS           = 150
LOW_CLIENTS           = 50
CRITICAL_CLIENTS      = 20
GROUP_CLIENT_LIMIT    = 150
CLIENT_TTL_DAYS       = 30
MESSAGE_LOOKBACK_DAYS = 730
BG_CHECK_INTERVAL     = 120
BG_SLEEP_AFTER_GROUPS = 3600
BG_KEYWORDS_PER_PASS  = 5

BUTTON_POSITIONS = {"top": 15.0, "center": 50.0, "bottom": 80.0}
BOT_KEYWORDS     = ["bot", "support", "notify", "news", "official", "info", "help", "admin"]

# ===========================================================================
# DATABASE
# ===========================================================================

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class Sphere(Base):
    __tablename__ = "spheres"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    name           = Column(String,  nullable=False)
    group_keywords = Column(Text,    nullable=False)
    user_keywords  = Column(Text,    nullable=False, default="[]")
    group_links    = Column(Text,    nullable=False, default="[]")
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
    __tablename__ = "tg_accounts"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    phone          = Column(String,  nullable=False, unique=True)
    username       = Column(String,  nullable=True)
    session_string = Column(Text,    nullable=False)
    is_active      = Column(Boolean, default=True)
    stories_today  = Column(Integer, default=0)
    stories_total  = Column(Integer, default=0)
    tags_total     = Column(Integer, default=0)
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
    __tablename__ = "flow_accounts"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    flow_id    = Column(Integer, ForeignKey("story_flows.id"),  nullable=False)
    account_id = Column(Integer, ForeignKey("tg_accounts.id"), nullable=False)
    load_pct   = Column(Integer, nullable=False, default=100)
    flow    = relationship("StoryFlow",  back_populates="flow_accounts")
    account = relationship("TgAccount",  back_populates="flow_accounts")
    __table_args__ = (UniqueConstraint("flow_id", "account_id", name="uq_flow_account"),)


def init_db() -> None:
    os.makedirs("media/photos", exist_ok=True)
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        for sql in [
            "ALTER TABLE spheres ADD COLUMN user_keywords TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE spheres ADD COLUMN group_links TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE tg_accounts ADD COLUMN telethon_session TEXT",
            "ALTER TABLE tg_accounts ADD COLUMN pyrogram_session TEXT",
            "ALTER TABLE tg_accounts ADD COLUMN avg_interval INTEGER DEFAULT 0",
            "ALTER TABLE tg_accounts ADD COLUMN is_premium INTEGER DEFAULT 0",
            "ALTER TABLE story_templates ADD COLUMN button_message_text TEXT",
        ]:
            try:
                conn.execute(text(sql)); conn.commit()
            except OperationalError:
                pass
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
        if r: s.expunge(r)
        return r

def db_get_sphere_by_name(name: str) -> Optional[Sphere]:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.name == name).first()
        if r: s.expunge(r)
        return r

def db_add_sphere(name, group_keywords, user_keywords) -> int:
    with SessionLocal() as s:
        obj = Sphere(name=name, group_keywords=group_keywords, user_keywords=user_keywords)
        s.add(obj); s.commit(); return obj.id

def db_delete_sphere(sid: int) -> None:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.id == sid).first()
        if r: s.delete(r); s.commit()

def db_update_sphere_keywords(sid: int, gkw: str, ukw: str) -> None:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.id == sid).first()
        if r: r.group_keywords = gkw; r.user_keywords = ukw; s.commit()

def db_update_sphere_group_links(sid: int, links: list) -> None:
    with SessionLocal() as s:
        r = s.query(Sphere).filter(Sphere.id == sid).first()
        if r: r.group_links = json.dumps(links, ensure_ascii=False); s.commit()

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

def db_count_groups(sid: int) -> int:
    with SessionLocal() as s:
        return s.query(Group).filter(Group.sphere_id == sid).count()

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
        if row: s.expunge(row)
        return row

def db_mark_client_used(cid: int) -> Optional[str]:
    with SessionLocal() as s:
        c = s.query(Client).filter(Client.id == cid).first()
        if c: c.used = True; g = c.group_username; s.commit(); return g
        return None

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

# --- accounts ---

def db_get_accounts() -> list:
    with SessionLocal() as s:
        rows = s.query(TgAccount).all(); s.expunge_all(); return rows

def db_get_account(aid: int) -> Optional[TgAccount]:
    with SessionLocal() as s:
        r = s.query(TgAccount).filter(TgAccount.id == aid).first()
        if r: s.expunge(r)
        return r

def db_add_account(phone: str, username: str, session_string: str) -> int:
    with SessionLocal() as s:
        # Проверяем нет ли уже такого
        existing = s.query(TgAccount).filter(TgAccount.phone == phone).first()
        if existing:
            existing.session_string = session_string
            existing.username = username
            s.commit()
            return existing.id
        obj = TgAccount(phone=phone, username=username, session_string=session_string)
        s.add(obj); s.commit(); return obj.id

def db_update_account_stats(aid: int, stories_delta: int = 0, tags_delta: int = 0) -> None:
    with SessionLocal() as s:
        r = s.query(TgAccount).filter(TgAccount.id == aid).first()
        if r:
            r.stories_today = (r.stories_today or 0) + stories_delta
            r.stories_total = (r.stories_total or 0) + stories_delta
            r.tags_total    = (r.tags_total    or 0) + tags_delta
            r.last_story_at = datetime.utcnow()
            s.commit()

def db_get_account_flow_count(aid: int) -> tuple:
    with SessionLocal() as s:
        total  = s.query(FlowAccount).filter(FlowAccount.account_id == aid).count()
        active = (s.query(FlowAccount).join(StoryFlow)
                  .filter(FlowAccount.account_id == aid, StoryFlow.status == "running").count())
        return total, active

def db_get_account_load_pct(aid: int) -> int:
    with SessionLocal() as s:
        fas = (s.query(FlowAccount).join(StoryFlow)
               .filter(FlowAccount.account_id == aid, StoryFlow.status == "running").all())
        return min(100, sum(fa.load_pct for fa in fas))

# --- templates ---

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
        if r: s.expunge(r)
        return r

def db_update_template(tid: int, **kwargs) -> None:
    with SessionLocal() as s:
        r = s.query(StoryTemplate).filter(StoryTemplate.id == tid).first()
        if r:
            for k, v in kwargs.items(): setattr(r, k, v)
            s.commit()

# --- modes & flows ---

def db_get_modes() -> list:
    with SessionLocal() as s:
        rows = s.query(StoryMode).all(); s.expunge_all(); return rows

def db_get_mode(mid: int) -> Optional[StoryMode]:
    with SessionLocal() as s:
        r = s.query(StoryMode).filter(StoryMode.id == mid).first()
        if r: s.expunge(r)
        return r

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
        if r: s.expunge(r)
        return r

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
# PYROGRAM
# ===========================================================================

_pyrogram_clients: dict = {}

async def get_pyrogram_client(account: TgAccount) -> Optional[PyrogramClient]:
    aid = account.id
    if aid not in _pyrogram_clients:
        try:
            client = PyrogramClient(
                f"pyro_{account.phone}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=account.session_string,
            )
            await client.start()
            _pyrogram_clients[aid] = client
        except Exception as exc:
            logger.error("[PYRO] %s: %s", account.phone, exc)
            return None
    client = _pyrogram_clients[aid]
    if not client.is_connected:
        try: await client.start()
        except Exception: return None
    return client

# ===========================================================================
# ПРОГРЕСС-БАР
# ===========================================================================

def make_progress_bar(current: int, total: int, width: int = 16) -> str:
    pct    = min(100, int(current / total * 100)) if total else 0
    filled = int(width * pct / 100)
    return f"{'Ӏ' * filled}{' ' * (width - filled)} {pct}%"

# ===========================================================================
# ФОНОВЫЙ ПАРСИНГ
# ===========================================================================

_collecting: set = set()
_last_group_search: dict = {}


def _is_public_group(chat) -> bool:
    if isinstance(chat, Chat):    return bool(getattr(chat, "username", None))
    if isinstance(chat, Channel): return bool(getattr(chat, "megagroup", False)) and bool(getattr(chat, "username", None))
    return False

def _is_bot(user) -> bool:
    if getattr(user, "bot", False): return True
    un = (user.username or "").lower()
    return un.endswith("bot") or any(k in un for k in BOT_KEYWORDS)

async def _search_one_keyword(kw: str) -> list:
    client = await ensure_telethon()
    found  = []
    try:
        result = await client(SearchRequest(q=kw, limit=100))
        for chat in result.chats:
            if not _is_public_group(chat): continue
            u = getattr(chat, "username", None)
            if u: found.append(f"https://t.me/{u.lower()}")
    except Exception as exc:
        logger.warning("SearchRequest «%s»: %s", kw, exc)
    await asyncio.sleep(0)
    return found

async def _find_groups_bg(sid: int, sname: str, keywords: list, progress_message=None) -> int:
    if db_count_groups(sid) >= MAX_GROUPS:
        return db_count_groups(sid)
    batch    = keywords[:BG_KEYWORDS_PER_PASS]
    total_kw = len(batch)
    all_links: list = []
    last_edit = 0.0
    for i, kw in enumerate(batch):
        if db_count_groups(sid) + len(all_links) >= MAX_GROUPS: break
        links     = await _search_one_keyword(kw)
        new_links = [l for l in links if l not in all_links]
        all_links.extend(new_links)
        now = asyncio.get_event_loop().time()
        if progress_message and (now - last_edit) >= 3.0:
            bar = make_progress_bar(i + 1, total_kw)
            try:
                await progress_message.edit_text(
                    f"🔍 Поиск групп...\n\n{bar}\n\nОбработано: {i+1}/{total_kw}\nНайдено: {len(all_links)}\n\n✅ Бот работает!"
                )
                last_edit = now
            except Exception: pass
        await asyncio.sleep(1.5)
    saved = db_save_groups(sid, sname, list(dict.fromkeys(all_links)))
    total = db_count_groups(sid)
    if progress_message:
        try: await progress_message.edit_text(f"✅ Поиск завершён!\n\n{make_progress_bar(1,1)}\n\nГрупп: {total}")
        except Exception: pass
    return total

async def _get_user_bio(client, uid) -> str:
    try:
        full = await client(GetFullUserRequest(uid))
        return full.full_user.about or ""
    except Exception:
        return ""

async def _collect_one_group(gu: str, sid: int, user_keywords: list, limit: int = 300) -> int:
    client   = await ensure_telethon()
    lookback = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set   = set()
    collected: list = []
    try:
        entity = await client.get_entity(gu)
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
                    if _is_bot(user): continue
                    bio = await _get_user_bio(client, uid)
                    # Этап 1: все → users_db без фильтра
                    db_save_user(sid, str(uid), user.username or "",
                                 user.first_name or "", user.last_name or "", bio)
                    # Для clients — с фильтром
                    if user.username:
                        un = user.username.lower()
                        if not db_is_username_known(sid, un):
                            h = " ".join(filter(None, [user.username, user.first_name, user.last_name, bio])).lower()
                            if not user_keywords or any(k.lower() in h for k in user_keywords):
                                collected.append(un)
                except Exception:
                    pass
            total_fetched += len(history.messages)
            offset_id      = history.messages[-1].id
            if stop or len(history.messages) < batch: break
            await asyncio.sleep(0.5)
    except Exception as exc:
        logger.warning("Error collecting «%s»: %s", gu, exc)
    saved = db_save_clients(sid, gu, collected)
    db_mark_group_parsed(f"https://t.me/{gu}")
    return saved

async def _fill_buffer_bg(sid: int, progress_message=None) -> None:
    if sid in _collecting: return
    _collecting.add(sid)
    try:
        sphere = db_get_sphere(sid)
        if not sphere: return
        gkws = json.loads(sphere.group_keywords or "[]")
        ukws = db_get_user_keywords(sid)
        # Парсим группы добавленные пользователем
        gl = json.loads(getattr(sphere, "group_links", None) or "[]")
        if gl:
            links = []
            for l in gl:
                l = l.strip()
                if not l.startswith("https://"): l = f"https://t.me/{l.lstrip('@')}"
                links.append(l)
            db_save_groups(sid, sphere.name, links)
        if db_count_groups(sid) < 10:
            await _find_groups_bg(sid, sphere.name, gkws, progress_message)
        groups = db_get_all_group_usernames(sid)
        if not groups: return
        if progress_message:
            try: await progress_message.edit_text(f"👥 Собираю клиентов...\n\n{make_progress_bar(0, len(groups))}\n\n✅ Бот работает!")
            except Exception: pass
        idx, att  = db_get_round_robin_index(sid), 0
        last_edit = asyncio.get_event_loop().time()
        while db_count_available_clients(sid) < MAX_CLIENTS and att < len(groups):
            await _collect_one_group(groups[idx % len(groups)], sid, ukws)
            idx = (idx + 1) % len(groups); att += 1
            now = asyncio.get_event_loop().time()
            if progress_message and (now - last_edit) >= 3.0:
                bar = make_progress_bar(att, len(groups))
                cnt = db_count_available_clients(sid)
                try: await progress_message.edit_text(f"👥 Собираю клиентов...\n\n{bar}\n\nНайдено: {cnt}\n✅ Бот работает!"); last_edit = now
                except Exception: pass
            await asyncio.sleep(0)
        db_set_round_robin_index(sid, idx)
        if db_count_available_clients(sid) < CRITICAL_CLIENTS:
            await _find_groups_bg(sid, sphere.name, gkws)
            groups2 = db_get_all_group_usernames(sid)
            idx2, att2 = db_get_round_robin_index(sid), 0
            while db_count_available_clients(sid) < MAX_CLIENTS and att2 < len(groups2):
                await _collect_one_group(groups2[idx2 % len(groups2)], sid, ukws)
                idx2 = (idx2 + 1) % len(groups2); att2 += 1
                await asyncio.sleep(0)
            db_set_round_robin_index(sid, idx2)
        total = db_count_available_clients(sid)
        if progress_message:
            try: await progress_message.edit_text(f"✅ Готово! Найдено клиентов: {total}\nНажмите «➡️ Следующий».")
            except Exception: pass
    except Exception as exc:
        logger.error("[FILL] sid=%d: %s", sid, exc)
    finally:
        _collecting.discard(sid)

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
                    asyncio.create_task(_find_groups_bg(sid, sphere.name, kws))
                await asyncio.sleep(1)
        except Exception as exc:
            logger.error("[BG] %s", exc)
        await asyncio.sleep(BG_CHECK_INTERVAL)

# ===========================================================================
# УМНОЕ РАСПРЕДЕЛЕНИЕ
# ===========================================================================

def _smart_distribute(accounts: list) -> list:
    if not accounts: return []
    if len(accounts) == 1: return [(accounts[0].id, 100)]
    scores = [(acc, max(1, 100 - db_get_account_load_pct(acc.id))) for acc in accounts]
    total_w = sum(w for _, w in scores)
    result = []; remaining = 100
    for i, (acc, w) in enumerate(scores):
        if i == len(scores) - 1: pct = remaining
        else: pct = round(w / total_w * 100); remaining -= pct
        result.append((acc.id, pct))
    return result

# ===========================================================================
# ПУБЛИКАЦИЯ ИСТОРИЙ
# ===========================================================================

_active_flows: dict = {}


def _pick_users(sid: int, mode: StoryMode, keywords: list, count: int) -> list:
    all_u = db_get_all_users(sid)
    fil_u = db_get_filtered_users(sid, keywords)
    if not all_u: return []
    selected = []
    for _ in range(count):
        if mode.filter_percent == 100:   pool = fil_u or all_u
        elif mode.filter_percent == 0:   pool = all_u
        else: pool = fil_u if (fil_u and random.random() < mode.filter_percent / 100) else all_u
        if pool: selected.append(random.choice(pool))
    seen = set(); unique = []
    for u in selected:
        if u.user_id not in seen: seen.add(u.user_id); unique.append(u)
    return unique

async def _publish_story(account: TgAccount, template: StoryTemplate, users: list) -> bool:
    """
    Публикует историю ТОЛЬКО через Telethon raw API.
    Фото читается с диска — photo_ids хранит локальные пути (media/photos/...).
    Используем InputMediaAreaUrl из telethon.tl.types — НЕ telethon.types.
    """
    tc = None
    try:
        session = getattr(account, "telethon_session", None) or account.session_string
        if not session:
            logger.error("[STORY] ❌ %s: нет telethon_session", account.phone)
            return False

        tc = TelegramClient(StringSession(session), API_ID, API_HASH)
        await tc.connect()
        if not await tc.is_user_authorized():
            await tc.disconnect()
            logger.error("[STORY] ❌ %s: Telethon сессия не авторизована", account.phone)
            return False

        photo_paths = json.loads(template.photo_ids or "[]")
        texts       = json.loads(template.texts     or "[]")
        if not photo_paths:
            logger.error("[STORY] ❌ %s: нет фото в шаблоне #%d", account.phone, template.id)
            await tc.disconnect()
            return False

        caption = random.choice(texts) if texts else ""
        if users:
            mentions = [f"@{u.username}" for u in users if u.username]
            mid      = len(mentions) // 2
            caption  = f"{caption}\n\n{' '.join(mentions[:mid])}\n{' '.join(mentions[mid:])}".strip()

        # Выбираем случайный файл с диска
        photo_path = random.choice(photo_paths)
        logger.info("[STORY] %s: загружаю фото из %s", account.phone, photo_path)

        if not os.path.exists(photo_path):
            logger.error("[STORY] ❌ %s: файл не найден: %s", account.phone, photo_path)
            await tc.disconnect()
            return False

        # Загружаем файл через Telethon upload_file (путь к файлу на диске)
        uploaded = await tc.upload_file(photo_path)
        media    = InputMediaUploadedPhoto(file=uploaded)

        # Кнопка через InputMediaAreaUrl (правильный тип из telethon.tl.types)
        media_areas = []
        button_url  = _build_button_url(account, template)
        if button_url:
            pos_y = BUTTON_POSITIONS.get(template.button_pos, 0.85)
            media_areas.append(InputMediaAreaUrl(
                coordinates=MediaAreaCoordinates(
                    x=20.0,          # 20% от левого края
                    y=pos_y * 100,   # конвертируем 0-1 → 0-100
                    w=60.0,          # ширина 60%
                    h=10.0,          # высота 10%
                    rotation=0.0,
                ),
                url=button_url,
            ))
            logger.info("[STORY] %s: кнопка url=%s", account.phone, button_url)

        # Публикуем
        await tc(functions.stories.SendStoryRequest(
            peer=await tc.get_me(),
            media=media,
            caption=caption,
            media_areas=media_areas if media_areas else None,
            period=86400,
            privacy_rules=[InputPrivacyValueAllowAll()],
            random_id=random.randint(0, 2**31),
            pinned=False,
            noforwards=False,
        ))

        now_str = datetime.utcnow().strftime("%d.%m %H:%M:%S")
        logger.info("[STORY] ✅ %s: СТОРИС ОПУБЛИКОВАНА в %s (%d отметок, файл: %s)",
                    account.phone, now_str, len(users), photo_path)
        db_update_account_stats(account.id, stories_delta=1, tags_delta=len(users))
        return True

    except Exception as exc:
        logger.error("[STORY] ❌ %s: ОШИБКА при публикации: %s", account.phone, exc)
        return False
    finally:
        if tc:
            try: await tc.disconnect()
            except Exception: pass

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
        if daily >= mode.daily_limit:
            msg = f"Лимит {mode.daily_limit} сторис/день. Жду 1ч."
            db_update_flow_stats(flow_id, sent, tagged, msg)
            await asyncio.sleep(3600); continue
        keywords = db_get_user_keywords(flow.sphere_id)
        for _ in range(mode.stories_per_batch):
            flow = db_get_flow(flow_id)
            if not flow or flow.status != "running": break
            if flow.end_time and datetime.utcnow() >= flow.end_time: break
            if daily >= mode.daily_limit: break
            # Выбор аккаунта по нагрузке (весовой)
            total_pct = sum(fa.load_pct for fa in fas)
            r = random.randint(0, max(total_pct - 1, 0))
            chosen = fas[0]; acc_sum = 0
            for fa in fas:
                acc_sum += fa.load_pct
                if r < acc_sum: chosen = fa; break
            account = db_get_account(chosen.account_id)
            if not account or not account.is_active: continue
            n     = random.randint(mode.mentions_min, mode.mentions_max)
            users = _pick_users(flow.sphere_id, mode, keywords, n)
            ok    = await _publish_story(account, template, users)
            if ok:
                sent += 1; daily += 1; tagged += len(users)
                now_str = datetime.utcnow().strftime("%d.%m %H:%M:%S")
                db_update_flow_stats(flow_id, sent, tagged,
                    f"✅ Сторис #{sent} опубликована {now_str} | "
                    f"@{account.username or account.phone} | {len(users)} отметок")
                interval = random.randint(mode.interval_min * 60, mode.interval_max * 60)
                db_update_account_stats(account.id, stories_delta=1, tags_delta=len(users),
                                        interval_min=interval // 60)
                next_time = (datetime.utcnow() + timedelta(seconds=interval)).strftime("%H:%M")
                db_update_flow_stats(flow_id, sent, tagged,
                    f"⏳ Жду {interval // 60} мин (след. публикация ~{next_time})")
                await asyncio.sleep(interval)
            else:
                db_update_flow_stats(flow_id, sent, tagged,
                    f"❌ Ошибка публикации | @{account.username or account.phone} — смотри логи")
                # Небольшая пауза после ошибки чтобы не спамить
                await asyncio.sleep(30)

        rest = random.randint(mode.rest_minutes_min * 60, mode.rest_minutes_max * 60)
        wake_time = (datetime.utcnow() + timedelta(seconds=rest)).strftime("%H:%M")
        db_update_flow_stats(flow_id, sent, tagged,
            f"💤 Пауза после батча: {rest // 60} мин (продолжу в ~{wake_time})")
        await asyncio.sleep(rest)
    _active_flows.pop(flow_id, None)
    logger.info("[FLOW %d] done. sent=%d tagged=%d", flow_id, sent, tagged)

# ===========================================================================
# STATES
# ===========================================================================

(
    STATE_MAIN_MENU,           # 0
    STATE_SPHERE_MENU,         # 1
    STATE_ADD_SPHERE_NAME,     # 2
    STATE_AFTER_STAGE,         # 3
    STATE_ADD_STAGE_DESC,      # 4
    STATE_ADD_STAGE_SCRIPT,    # 5
    STATE_ADD_GROUP_KEYWORDS,  # 6
    STATE_ADD_USER_KEYWORDS,   # 7
    STATE_ADD_GROUP_LINKS,     # 8
    STATE_SHOW_CLIENT,         # 9
    STATE_EDIT_SCRIPT_CHOOSE,  # 10
    STATE_EDIT_SCRIPT_DESC,    # 11
    STATE_EDIT_SCRIPT_TEXT,    # 12
    STATE_EDIT_KEYS_GROUP,     # 13
    STATE_EDIT_KEYS_USER,      # 14
    STATE_REDO_STAGE_DESC,     # 15
    STATE_REDO_STAGE_SCRIPT,   # 16
    STATE_STORY_MENU,          # 17
    STATE_ACCOUNTS_MENU,       # 18  ← раздел аккаунтов
    STATE_ADD_ACC_PHONE,       # 19
    STATE_ADD_ACC_CODE,        # 20
    STATE_ADD_ACC_PASS,        # 21
    STATE_STORY_CHOOSE_SPHERE, # 22
    STATE_STORY_TMPL_MENU,     # 23
    STATE_STORY_NEW_TMPL_NAME, # 24
    STATE_STORY_ADD_PHOTO,     # 25
    STATE_STORY_ADD_TEXT,      # 26
    STATE_STORY_BTN_TEXT,      # 27
    STATE_STORY_BTN_MSG,       # 28
    STATE_STORY_BTN_POS,       # 29
    STATE_STORY_CHOOSE_MODE,   # 30
    STATE_STORY_CHOOSE_TIME,   # 31
    STATE_STORY_CUSTOM_TIME,   # 32
    STATE_STORY_SELECT_ACCS,   # 33  ← выбор аккаунтов для потока
    STATE_STORY_LOAD_DIST,     # 34
    STATE_STORY_LOAD_MANUAL,   # 35
    STATE_FLOWS_MENU,          # 36
    STATE_FLOW_DETAIL,         # 37
) = range(38)

# ===========================================================================
# KEYBOARDS
# ===========================================================================

def kb_main():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🖐🏻 Ручной поиск")],
        [KeyboardButton("📸 Отметки сторис")],
    ], resize_keyboard=True)

def kb_sphere():
    spheres = db_get_spheres()
    btns = [[KeyboardButton(s.name)] for s in spheres]
    btns += [[KeyboardButton("➕ Добавить сферу")], [KeyboardButton("🏠 Главное меню")]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_client():
    return ReplyKeyboardMarkup([
        [KeyboardButton("➡️ Следующий")],
        [KeyboardButton("✏️ Изменить скрипт"), KeyboardButton("🔑 Изменить ключи")],
        [KeyboardButton("🏠 Главное меню"),     KeyboardButton("🗑 Удалить сферу")],
    ], resize_keyboard=True)

def kb_after_stage():
    return ReplyKeyboardMarkup([[KeyboardButton("+Этап"), KeyboardButton("Продолжить")]], resize_keyboard=True)

def kb_edit_script(stages):
    btns, row = [], []
    for i in range(len(stages)):
        row.append(KeyboardButton(str(i + 1)))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([KeyboardButton("Все"), KeyboardButton("Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_story_main():
    return ReplyKeyboardMarkup([
        [KeyboardButton("▶️ Запустить")],
        [KeyboardButton("🛠 Потоки"), KeyboardButton("👤 Аккаунты")],
        [KeyboardButton("🏠 Главное меню")],
    ], resize_keyboard=True)

def kb_accounts(accounts: list):
    """Клавиатура раздела Аккаунты — каждый аккаунт отдельной кнопкой."""
    btns = []
    for acc in accounts:
        load   = db_get_account_load_pct(acc.id)
        status = "🟢" if acc.is_active else "🔴"
        # Формат: "acc_ID" — по нему будем искать в хендлере
        btns.append([KeyboardButton(f"{status} @{acc.username or acc.phone} ({load}%) [id:{acc.id}]")])
    btns.append([KeyboardButton("➕ Добавить аккаунт")])
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_templates(templates):
    btns = [[KeyboardButton(f"📋 {t.name}")] for t in templates]
    btns += [[KeyboardButton("➕ Новый шаблон")], [KeyboardButton("🔙 Назад")]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_modes(modes):
    btns = [[KeyboardButton(m.name)] for m in modes]
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_time():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⏱ 20 минут"),      KeyboardButton("📅 1 день")],
        [KeyboardButton("♾ До отключения"), KeyboardButton("🗓 Своя дата")],
        [KeyboardButton("🔙 Назад")],
    ], resize_keyboard=True)

def kb_btn_pos():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⬆️ Вверху"), KeyboardButton("⏺ По центру"), KeyboardButton("⬇️ Внизу")],
    ], resize_keyboard=True)

def kb_select_accs(accounts: list, selected_ids: list):
    """Выбор аккаунтов для потока — галочка у выбранных."""
    btns = []
    for acc in accounts:
        mark = "✅" if acc.id in selected_ids else "◻️"
        load = db_get_account_load_pct(acc.id)
        st   = "🟢" if acc.is_active else "🔴"
        btns.append([KeyboardButton(f"{mark} @{acc.username or acc.phone} {st} нагрузка {load}% [id:{acc.id}]")])
    btns.append([KeyboardButton("➡️ Продолжить")])
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_load_dist():
    return ReplyKeyboardMarkup([
        [KeyboardButton("✋ Ручной")],
        [KeyboardButton("⚖️ Равномерно")],
        [KeyboardButton("🧠 Умное распределение")],
        [KeyboardButton("🔙 Назад")],
    ], resize_keyboard=True)

def kb_flows(flows):
    btns = []
    for f in flows:
        s = "🟢" if f.status == "running" else ("⚪" if f.status == "done" else "🔴")
        btns.append([KeyboardButton(f"{s} Поток #{f.id} — {f.stories_sent} сторис [id:{f.id}]")])
    btns.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_flow_detail():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🛑 СТОП"),              KeyboardButton("📊 Нагрузка аккаунтов")],
        [KeyboardButton("📋 Логи работы"),       KeyboardButton("🔙 Назад")],
    ], resize_keyboard=True)

def kb_confirm():
    return ReplyKeyboardMarkup([[KeyboardButton("✅ Запустить"), KeyboardButton("❌ Отмена")]], resize_keyboard=True)

# ===========================================================================
# HELPERS
# ===========================================================================

def _extract_id(text: str) -> Optional[int]:
    """Извлекает [id:N] из текста кнопки."""
    m = re.search(r"\[id:(\d+)\]", text)
    return int(m.group(1)) if m else None

# ===========================================================================
# HANDLERS — основное меню и сферы
# ===========================================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Добро пожаловать!", reply_markup=kb_main())
    return STATE_MAIN_MENU

async def h_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🖐🏻 Ручной поиск":
        await update.message.reply_text("Выберите сферу:", reply_markup=kb_sphere())
        return STATE_SPHERE_MENU
    if text == "📸 Отметки сторис":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story_main())
        return STATE_STORY_MENU
    return STATE_MAIN_MENU

async def h_sphere_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🏠 Главное меню":
        await update.message.reply_text("Главное меню:", reply_markup=kb_main())
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
        prog = await update.message.reply_text(
            f"🔍 Поиск для «{sphere.name}»...\n\n{make_progress_bar(0,1)}\n\n✅ Бот работает!",
            reply_markup=kb_client(),
        )
        asyncio.create_task(_fill_buffer_bg(sphere.id, progress_message=prog))
        return STATE_SHOW_CLIENT
    await update.message.reply_text("Сфера не найдена.", reply_markup=kb_sphere())
    return STATE_SPHERE_MENU

async def show_next_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid   = context.user_data.get("current_sphere_id")
    sname = context.user_data.get("current_sphere_name")
    if not sid:
        await update.message.reply_text("Ошибка.", reply_markup=kb_main())
        return STATE_MAIN_MENU
    row = db_get_random_client(sid)
    if not row:
        status = "Идёт сбор..." if sid in _collecting else "Запускаю сбор..."
        await update.message.reply_text(f"⏳ Клиентов пока нет. {status}", reply_markup=kb_client())
        if sid not in _collecting: asyncio.create_task(_fill_buffer_bg(sid))
        return STATE_SHOW_CLIENT
    context.user_data["current_client_id"] = row.id
    stages = db_get_stages(sid)
    lines  = [f"<b>@{row.username}</b>\n", f"<b>{sname}</b>"]
    for i, st in enumerate(stages, 1):
        lines.append(f"\n<b>{i} этап — {st.description}</b>")
        lines.append(f"<blockquote>{st.script}</blockquote>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=kb_client())
    if db_count_available_clients(sid) < LOW_CLIENTS and sid not in _collecting:
        asyncio.create_task(_fill_buffer_bg(sid))
    return STATE_SHOW_CLIENT

async def h_show_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        for k in ("current_sphere_id","current_sphere_name","current_client_id"): context.user_data.pop(k, None)
        await update.message.reply_text("Главное меню:", reply_markup=kb_main())
        return STATE_MAIN_MENU
    if text == "🗑 Удалить сферу":
        sid = context.user_data.get("current_sphere_id"); sname = context.user_data.get("current_sphere_name")
        if sid:
            db_delete_sphere(sid); _collecting.discard(sid)
            for k in ("current_sphere_id","current_sphere_name","current_client_id"): context.user_data.pop(k, None)
            await update.message.reply_text(f"🗑 Сфера «{sname}» удалена.", reply_markup=kb_sphere())
        return STATE_SPHERE_MENU
    if text == "✏️ Изменить скрипт":
        sid = context.user_data.get("current_sphere_id"); stages = db_get_stages(sid)
        if not stages:
            await update.message.reply_text("Этапов нет.", reply_markup=kb_client()); return STATE_SHOW_CLIENT
        context.user_data["edit_stages"] = [{"id": st.id, "desc": st.description, "script": st.script} for st in stages]
        await update.message.reply_text("Какой этап?", reply_markup=kb_edit_script(stages))
        return STATE_EDIT_SCRIPT_CHOOSE
    if text == "🔑 Изменить ключи":
        sid = context.user_data.get("current_sphere_id"); sp = db_get_sphere(sid)
        if sp:
            kws = json.loads(sp.group_keywords or "[]")
            await update.message.reply_text(f"Ключи групп:\n{chr(10).join(kws)}\n\nВведите новые:", reply_markup=ReplyKeyboardRemove())
            return STATE_EDIT_KEYS_GROUP
    return STATE_SHOW_CLIENT

async def h_edit_script_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text; stages = context.user_data.get("edit_stages", [])
    if text == "Назад":
        await update.message.reply_text("Возврат:", reply_markup=kb_client()); return STATE_SHOW_CLIENT
    if text == "Все":
        sid = context.user_data.get("current_sphere_id"); db_delete_stages(sid)
        context.user_data["new_sphere"] = {"stages": [], "redo": True}
        await update.message.reply_text("Введите пояснение 1-го этапа:", reply_markup=ReplyKeyboardRemove())
        return STATE_REDO_STAGE_DESC
    try:
        idx = int(text) - 1
        if 0 <= idx < len(stages):
            context.user_data["editing_stage_idx"] = idx; st = stages[idx]
            await update.message.reply_text(f"Этап {idx+1}: «{st['desc']}»\n\nНовое пояснение:", reply_markup=ReplyKeyboardRemove())
            return STATE_EDIT_SCRIPT_DESC
    except ValueError: pass
    await update.message.reply_text("Выберите номер:", reply_markup=kb_edit_script(stages))
    return STATE_EDIT_SCRIPT_CHOOSE

async def h_edit_script_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите новый скрипт:"); return STATE_EDIT_SCRIPT_TEXT

async def h_edit_script_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_script = update.message.text.strip(); new_desc = context.user_data.pop("new_stage_desc", "")
    idx = context.user_data.get("editing_stage_idx", 0); stages = context.user_data.get("edit_stages", [])
    if 0 <= idx < len(stages): db_update_stage(stages[idx]["id"], new_desc, new_script)
    await update.message.reply_text("Данные изменены ✅", reply_markup=kb_main())
    return STATE_MAIN_MENU

async def h_redo_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_sphere"]["current_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:"); return STATE_REDO_STAGE_SCRIPT

async def h_redo_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip(); desc = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=kb_after_stage())
    return STATE_AFTER_STAGE

async def h_edit_keys_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kws = [k.strip() for k in update.message.text.strip().splitlines() if k.strip()]
    context.user_data["new_group_kws"] = kws
    sid = context.user_data.get("current_sphere_id"); sp = db_get_sphere(sid)
    ukws = json.loads(sp.user_keywords or "[]") if sp else []
    await update.message.reply_text(
        f"Ключи пользователей:\n{chr(10).join(ukws) if ukws else '(не заданы)'}\n\nВведите новые (или «нет»):",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_EDIT_KEYS_USER

async def h_edit_keys_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    ukws = [] if raw.lower() in ("нет","no","-","") else [k.strip() for k in raw.splitlines() if k.strip()]
    sid = context.user_data.get("current_sphere_id"); gkws = context.user_data.pop("new_group_kws", [])
    db_update_sphere_keywords(sid, json.dumps(gkws, ensure_ascii=False), json.dumps(ukws, ensure_ascii=False))
    await update.message.reply_text("Ключи обновлены ✅", reply_markup=kb_main())
    return STATE_MAIN_MENU

async def h_add_sphere_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name: await update.message.reply_text("Введите название:"); return STATE_ADD_SPHERE_NAME
    context.user_data["new_sphere"]["name"] = name
    await update.message.reply_text("Добавьте этапы:", reply_markup=kb_after_stage())
    return STATE_AFTER_STAGE

async def h_after_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text; ns = context.user_data.get("new_sphere", {})
    if text == "+Этап":
        await update.message.reply_text("Введите пояснение:", reply_markup=ReplyKeyboardRemove())
        return STATE_REDO_STAGE_DESC if ns.get("redo") else STATE_ADD_STAGE_DESC
    if text == "Продолжить":
        if ns.get("redo"):
            sid = context.user_data.get("current_sphere_id")
            for st in ns.get("stages", []): db_add_stage(sid, st["desc"], st["script"])
            context.user_data.pop("new_sphere", None)
            await update.message.reply_text("Данные изменены ✅", reply_markup=kb_main())
            return STATE_MAIN_MENU
        await update.message.reply_text("📌 Шаг 1 из 3: Ключевые слова для групп\n(каждое с новой строки):", reply_markup=ReplyKeyboardRemove())
        return STATE_ADD_GROUP_KEYWORDS
    return STATE_AFTER_STAGE

async def h_add_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_sphere"]["current_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:"); return STATE_ADD_STAGE_SCRIPT

async def h_add_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip(); desc = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=kb_after_stage())
    return STATE_AFTER_STAGE

async def h_add_group_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kws = [k.strip() for k in update.message.text.strip().splitlines() if k.strip()]
    if not kws: await update.message.reply_text("Введите хотя бы одно слово:"); return STATE_ADD_GROUP_KEYWORDS
    context.user_data["new_sphere"]["group_keywords"] = kws
    await update.message.reply_text("📌 Шаг 2 из 3: Ключевые слова для фильтрации пользователей\n(или «нет»):", reply_markup=ReplyKeyboardRemove())
    return STATE_ADD_USER_KEYWORDS

async def h_add_user_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = [] if raw.lower() in ("нет","no","-","") else [k.strip() for k in raw.splitlines() if k.strip()]
    context.user_data["new_sphere"]["user_keywords"] = ukws
    await update.message.reply_text(
        "📌 Шаг 3 из 3: Ссылки на группы для парсинга\n"
        "(каждая с новой строки)\n\nИли «пропустить» — бот найдёт сам:",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("пропустить")]], resize_keyboard=True),
    )
    return STATE_ADD_GROUP_LINKS

async def h_add_group_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    group_links = [] if raw.lower() == "пропустить" else [l.strip() for l in raw.splitlines() if l.strip()]
    ns = context.user_data.get("new_sphere", {})
    name = ns.get("name",""); stages = ns.get("stages",[]); gkws = ns.get("group_keywords",[]); ukws = ns.get("user_keywords",[])
    sid = db_add_sphere(name=name, group_keywords=json.dumps(gkws, ensure_ascii=False), user_keywords=json.dumps(ukws, ensure_ascii=False))
    if group_links: db_update_sphere_group_links(sid, group_links)
    for st in stages: db_add_stage(sid, st["desc"], st["script"])
    info = f"Групп добавлено: {len(group_links)}" if group_links else "Группы найдутся автоматически"
    await update.message.reply_text(f"💾 Сфера «{name}» сохранена!\n{info}\n\n🔍 Поиск запущен в фоне.")
    prog = await update.message.reply_text(f"🔍 Поиск групп...\n\n{make_progress_bar(0,1)}")
    asyncio.create_task(_fill_buffer_bg(sid, progress_message=prog))
    context.user_data.pop("new_sphere", None)
    await update.message.reply_text("Выберите сферу:", reply_markup=kb_sphere())
    return STATE_SPHERE_MENU

# ===========================================================================
# HANDLERS — 📸 Отметки сторис
# ===========================================================================

async def h_story_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🏠 Главное меню":
        await update.message.reply_text("Главное меню:", reply_markup=kb_main())
        return STATE_MAIN_MENU

    if text == "👤 Аккаунты":
        accounts = db_get_accounts()
        msg = f"👤 Аккаунты ({len(accounts)} шт.)\n\nНажмите на аккаунт для просмотра карточки:"
        await update.message.reply_text(msg, reply_markup=kb_accounts(accounts))
        return STATE_ACCOUNTS_MENU

    if text == "▶️ Запустить":
        spheres = db_get_spheres()
        if not spheres:
            await update.message.reply_text("Нет сфер. Создайте сначала.", reply_markup=kb_story_main())
            return STATE_STORY_MENU
        btns = [[KeyboardButton(s.name)] for s in spheres] + [[KeyboardButton("🔙 Назад")]]
        await update.message.reply_text("Выберите сферу:", reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
        return STATE_STORY_CHOOSE_SPHERE

    if text == "🛠 Потоки":
        sid = context.user_data.get("story_sphere_id")
        if not sid:
            spheres = db_get_spheres()
            if not spheres:
                await update.message.reply_text("Нет сфер.", reply_markup=kb_story_main())
                return STATE_STORY_MENU
            btns = [[KeyboardButton(s.name)] for s in spheres] + [[KeyboardButton("🔙 Назад")]]
            context.user_data["story_flows_mode"] = True
            await update.message.reply_text("Выберите сферу:", reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
            return STATE_STORY_CHOOSE_SPHERE
        flows = db_get_flows(sid)
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows))
        return STATE_FLOWS_MENU

    return STATE_STORY_MENU

# ===========================================================================
# HANDLERS — 👤 Аккаунты
# ===========================================================================

async def h_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story_main())
        return STATE_STORY_MENU

    if text == "➕ Добавить аккаунт":
        await update.message.reply_text(
            "Введите номер телефона в формате +7XXXXXXXXXX:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_ADD_ACC_PHONE

    # Нажали на карточку аккаунта — ищем [id:N]
    aid = _extract_id(text)
    if aid:
        acc = db_get_account(aid)
        if acc:
            total, active = db_get_account_flow_count(acc.id)
            load          = db_get_account_load_pct(acc.id)
            load_label    = "🔴 Высокий" if load > 70 else ("🟡 Умеренный" if load > 40 else "🟢 Низкий")
            last_story    = acc.last_story_at.strftime("%d.%m %H:%M") if acc.last_story_at else "—"
            warnings      = []
            if load > 70:   warnings.append("• Высокая нагрузка")
            if (acc.stories_today or 0) > 40: warnings.append("• Много сторис за день")

            card = (
                f"Аккаунт: @{acc.username or acc.phone}\n"
                f"Статус: {'Активен 🟢' if acc.is_active else 'Неактивен 🔴'}\n\n"
                f"Задействован в потоках: {total}\n"
                f"Активных потоков: {active}\n\n"
                f"📊 Нагрузка: {load_label} ({load}%)\n\n"
                f"⏱ Последняя сторис: {last_story}\n\n"
                f"Статистика:\n"
                f"  Сторис сегодня: {acc.stories_today or 0}\n"
                f"  Сторис всего: {acc.stories_total or 0}\n"
                f"  Отметок всего: {acc.tags_total or 0}\n"
            )
            if warnings:
                card += "\n⚠️ Предупреждения:\n" + "\n".join(warnings)

            await update.message.reply_text(card, reply_markup=kb_accounts(db_get_accounts()))
            return STATE_ACCOUNTS_MENU

    return STATE_ACCOUNTS_MENU

async def h_add_acc_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    if not phone.startswith("+"):
        await update.message.reply_text("Введите номер с +, например +79001234567:")
        return STATE_ADD_ACC_PHONE
    context.user_data["new_acc_phone"] = phone
    await update.message.reply_text(f"📱 Отправляю код на {phone}...")
    try:
        client = PyrogramClient(f"pyro_auth_{phone}", api_id=API_ID, api_hash=API_HASH, in_memory=True)
        await client.connect()
        sent = await client.send_code(phone)
        context.user_data["pyro_auth_client"] = client
        context.user_data["pyro_phone_hash"]  = sent.phone_code_hash
        await update.message.reply_text("Введите код из Telegram:", reply_markup=ReplyKeyboardRemove())
        return STATE_ADD_ACC_CODE
    except Exception as exc:
        await update.message.reply_text(f"❌ Ошибка: {exc}\n\nПопробуйте ещё раз.")
        return STATE_ADD_ACC_PHONE

async def h_add_acc_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code   = update.message.text.strip().replace(" ", "")
    phone  = context.user_data.get("new_acc_phone")
    client = context.user_data.get("pyro_auth_client")
    if not client:
        await update.message.reply_text("Ошибка. Начните заново.")
        return STATE_ACCOUNTS_MENU
    try:
        await client.sign_in(phone, code)
    except Exception as exc:
        if "PASSWORD" in str(exc).upper():
            await update.message.reply_text("Введите облачный пароль (2FA):", reply_markup=ReplyKeyboardRemove())
            return STATE_ADD_ACC_PASS
        await update.message.reply_text(f"❌ Ошибка: {exc}")
        return STATE_ADD_ACC_PHONE
    return await _finish_acc_auth(update, context, client, phone)

async def h_add_acc_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    phone    = context.user_data.get("new_acc_phone")
    client   = context.user_data.get("pyro_auth_client")
    try:
        await client.check_password(password)
    except Exception as exc:
        await update.message.reply_text(f"❌ Неверный пароль: {exc}")
        return STATE_ADD_ACC_PASS
    return await _finish_acc_auth(update, context, client, phone)

async def _finish_acc_auth(update: Update, context: ContextTypes.DEFAULT_TYPE, client, phone: str):
    try:
        session_string = await client.export_session_string()
        me = await client.get_me()
        username = me.username or me.first_name or phone
        await client.disconnect()
        context.user_data.pop("pyro_auth_client", None)
        db_add_account(phone=phone, username=username, session_string=session_string)
        accounts = db_get_accounts()
        await update.message.reply_text(
            f"✅ Аккаунт @{username} добавлен!",
            reply_markup=kb_accounts(accounts),
        )
    except Exception as exc:
        await update.message.reply_text(f"❌ Ошибка: {exc}", reply_markup=kb_story_main())
    return STATE_ACCOUNTS_MENU

# ===========================================================================
# HANDLERS — Запуск потока
# ===========================================================================

async def h_story_choose_sphere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story_main())
        return STATE_STORY_MENU
    sphere = db_get_sphere_by_name(text)
    if not sphere:
        await update.message.reply_text("Сфера не найдена."); return STATE_STORY_CHOOSE_SPHERE
    context.user_data["story_sphere_id"] = sphere.id
    if context.user_data.pop("story_flows_mode", False):
        flows = db_get_flows(sphere.id)
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows))
        return STATE_FLOWS_MENU
    templates = db_get_templates(sphere.id)
    await update.message.reply_text(
        f"Сфера: {sphere.name}\nПользователей: {db_count_users(sphere.id)}\n\nВыберите шаблон:",
        reply_markup=kb_templates(templates),
    )
    return STATE_STORY_TMPL_MENU

async def h_story_tmpl_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story_main())
        return STATE_STORY_MENU
    if text == "➕ Новый шаблон":
        await update.message.reply_text("Введите название шаблона:", reply_markup=ReplyKeyboardRemove())
        return STATE_STORY_NEW_TMPL_NAME
    name = text.replace("📋 ", "")
    sid  = context.user_data.get("story_sphere_id")
    tmpl = next((t for t in db_get_templates(sid) if t.name == name), None)
    if not tmpl:
        await update.message.reply_text("Шаблон не найден."); return STATE_STORY_TMPL_MENU
    context.user_data["story_template_id"] = tmpl.id
    await update.message.reply_text("Выберите режим:", reply_markup=kb_modes(db_get_modes()))
    return STATE_STORY_CHOOSE_MODE

async def h_story_new_tmpl_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name: await update.message.reply_text("Введите название:"); return STATE_STORY_NEW_TMPL_NAME
    sid = context.user_data.get("story_sphere_id")
    tid = db_create_template(sid, name)
    context.user_data["story_template_id"] = tid
    context.user_data["story_photos"] = []
    await update.message.reply_text(
        "Отправьте фото (можно несколько). Когда закончите — напишите <b>готово</b>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("готово")]], resize_keyboard=True),
    )
    return STATE_STORY_ADD_PHOTO

async def h_story_add_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.lower() == "готово":
        photos = context.user_data.get("story_photos", [])
        if not photos: await update.message.reply_text("Добавьте хотя бы одно фото!"); return STATE_STORY_ADD_PHOTO
        db_update_template(context.user_data["story_template_id"], photo_ids=json.dumps(photos, ensure_ascii=False))
        await update.message.reply_text(
            f"✅ Фото: {len(photos)} (сохранены на диск)\n\nВведите варианты подписи (каждый с новой строки):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_STORY_ADD_TEXT
    if update.message.photo:
        tid = context.user_data.get("story_template_id", "tmp")
        idx = len(context.user_data.get("story_photos", []))
        path = f"media/photos/{tid}_{idx}.jpg"
        # Скачиваем файл через Bot API на диск
        tg_file = await update.message.photo[-1].get_file()
        await tg_file.download_to_drive(path)
        context.user_data.setdefault("story_photos", []).append(path)
        await update.message.reply_text(f"📷 Фото {len(context.user_data['story_photos'])} сохранено. Ещё или «готово».")
        return STATE_STORY_ADD_PHOTO
    await update.message.reply_text("Отправьте фото или «готово».")
    return STATE_STORY_ADD_PHOTO

async def h_story_add_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texts = [t.strip() for t in update.message.text.strip().splitlines() if t.strip()]
    if not texts: await update.message.reply_text("Введите хотя бы один текст:"); return STATE_STORY_ADD_TEXT
    db_update_template(context.user_data["story_template_id"], texts=json.dumps(texts, ensure_ascii=False))
    await update.message.reply_text(
        f"✅ Текстов: {len(texts)}\n\nТекст кнопки (или «пропустить»):",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("пропустить")]], resize_keyboard=True),
    )
    return STATE_STORY_BTN_TEXT

async def h_story_btn_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.lower() == "пропустить":
        await update.message.reply_text("Выберите режим:", reply_markup=kb_modes(db_get_modes()))
        return STATE_STORY_CHOOSE_MODE
    context.user_data["story_button_text"] = text
    await update.message.reply_text("Введите ваш username (без @):", reply_markup=ReplyKeyboardRemove())
    return STATE_STORY_BTN_MSG

async def h_story_btn_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().lstrip("@")
    url = f"https://t.me/{username}"
    db_update_template(context.user_data["story_template_id"], button_text=context.user_data.get("story_button_text", ""), button_url=url)
    await update.message.reply_text(f"Ссылка: {url}\n\nПоложение кнопки:", reply_markup=kb_btn_pos())
    return STATE_STORY_BTN_POS

async def h_story_btn_pos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pos_map = {"⬆️ Вверху": "top", "⏺ По центру": "center", "⬇️ Внизу": "bottom"}
    db_update_template(context.user_data["story_template_id"], button_pos=pos_map.get(update.message.text, "bottom"))
    await update.message.reply_text("✅ Шаблон сохранён!\n\nВыберите режим:", reply_markup=kb_modes(db_get_modes()))
    return STATE_STORY_CHOOSE_MODE

async def h_story_choose_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        sid = context.user_data.get("story_sphere_id")
        await update.message.reply_text("Выберите шаблон:", reply_markup=kb_templates(db_get_templates(sid)))
        return STATE_STORY_TMPL_MENU
    mode = next((m for m in db_get_modes() if m.name == text), None)
    if not mode:
        await update.message.reply_text("Выберите из списка:", reply_markup=kb_modes(db_get_modes()))
        return STATE_STORY_CHOOSE_MODE
    context.user_data["story_mode_id"] = mode.id
    await update.message.reply_text(
        f"Режим: {mode.name}\nОтметок: {mode.mentions_min}–{mode.mentions_max}\n"
        f"Интервал: {mode.interval_min}–{mode.interval_max} мин\nЛимит/день: {mode.daily_limit}\n\n"
        f"Выберите время работы:", reply_markup=kb_time(),
    )
    return STATE_STORY_CHOOSE_TIME

async def h_story_choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🔙 Назад":
        await update.message.reply_text("Режим:", reply_markup=kb_modes(db_get_modes())); return STATE_STORY_CHOOSE_MODE
    if text == "⏱ 20 минут":       end_time = datetime.utcnow() + timedelta(minutes=20)
    elif text == "📅 1 день":       end_time = datetime.utcnow() + timedelta(days=1)
    elif text == "♾ До отключения": end_time = None
    elif text == "🗓 Своя дата":
        await update.message.reply_text("Введите: ДД.ММ.ГГГГ ЧЧ:ММ", reply_markup=ReplyKeyboardRemove())
        return STATE_STORY_CUSTOM_TIME
    else: end_time = None
    context.user_data["story_end_time"] = end_time
    return await _show_select_accounts(update, context)

async def h_story_custom_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try: end_time = datetime.strptime(update.message.text.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await update.message.reply_text("Неверный формат. Пример: 25.12.2025 18:00"); return STATE_STORY_CUSTOM_TIME
    context.user_data["story_end_time"] = end_time
    return await _show_select_accounts(update, context)

async def _show_select_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    accounts = db_get_accounts()
    if not accounts:
        await update.message.reply_text(
            "⚠️ Нет аккаунтов! Добавьте через 👤 Аккаунты.",
            reply_markup=kb_story_main(),
        )
        return STATE_STORY_MENU
    context.user_data["story_selected_acc_ids"] = []
    await update.message.reply_text(
        "Выберите аккаунты для потока\n(нажмите чтобы выбрать/снять, затем «➡️ Продолжить»):",
        reply_markup=kb_select_accs(accounts, []),
    )
    return STATE_STORY_SELECT_ACCS

async def h_story_select_accs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text     = update.message.text
    accounts = db_get_accounts()
    selected = context.user_data.get("story_selected_acc_ids", [])

    if text == "🔙 Назад":
        await update.message.reply_text("Время работы:", reply_markup=kb_time())
        return STATE_STORY_CHOOSE_TIME

    if text == "➡️ Продолжить":
        if not selected:
            await update.message.reply_text("Выберите хотя бы один аккаунт!")
            return STATE_STORY_SELECT_ACCS
        # Сразу равномерное распределение
        n   = len(selected)
        pct = 100 // n; rem = 100 - pct * n
        context.user_data["story_load"] = [(aid, pct + (1 if i < rem else 0)) for i, aid in enumerate(selected)]
        return await _show_load_screen(update, context)

    # Нажали на аккаунт — toggle
    aid = _extract_id(text)
    if aid:
        if aid in selected: selected.remove(aid)
        else: selected.append(aid)
        context.user_data["story_selected_acc_ids"] = selected
        await update.message.reply_text(
            f"Выбрано: {len(selected)} аккаунт(ов)",
            reply_markup=kb_select_accs(accounts, selected),
        )
        return STATE_STORY_SELECT_ACCS

    return STATE_STORY_SELECT_ACCS

async def _show_load_screen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load = context.user_data.get("story_load", [])
    lines = ["Распределение нагрузки:"]
    for aid, pct in load:
        a = db_get_account(aid)
        if a: lines.append(f"@{a.username or a.phone} 🛠️{pct}%")
    btns = []
    if len(load) > 1: btns.append([KeyboardButton("🔧 Изменить нагрузку")])
    btns.append([KeyboardButton("➡️ Продолжить"), KeyboardButton("🔙 Назад")])
    await update.message.reply_text("\n".join(lines), reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
    return STATE_STORY_LOAD_DIST

async def h_story_load_dist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    load = context.user_data.get("story_load", [])

    if text == "🔙 Назад":
        accounts = db_get_accounts()
        selected = [aid for aid, _ in load]
        await update.message.reply_text("Выберите аккаунты:", reply_markup=kb_select_accs(accounts, selected))
        return STATE_STORY_SELECT_ACCS

    if text == "➡️ Продолжить":
        return await _show_flow_preview(update, context)

    if text == "🔧 Изменить нагрузку":
        await update.message.reply_text("Режим перераспределения:", reply_markup=kb_load_dist())
        return STATE_STORY_LOAD_DIST

    if text == "✋ Ручной":
        context.user_data["load_manual_idx"] = 0
        a = db_get_account(load[0][0])
        await update.message.reply_text(f"% для @{a.username or a.phone} (0–100):", reply_markup=ReplyKeyboardRemove())
        return STATE_STORY_LOAD_MANUAL

    if text == "⚖️ Равномерно":
        n = len(load); pct = 100 // n; rem = 100 - pct * n
        context.user_data["story_load"] = [(aid, pct + (1 if i < rem else 0)) for i, (aid, _) in enumerate(load)]
        return await _show_load_screen(update, context)

    if text == "🧠 Умное распределение":
        accs = [db_get_account(aid) for aid, _ in load]
        result = _smart_distribute([a for a in accs if a])
        context.user_data["story_load"] = result
        return await _show_load_screen(update, context)

    return STATE_STORY_LOAD_DIST

async def h_story_load_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pct = int(update.message.text.strip())
        if not 0 <= pct <= 100: raise ValueError
    except ValueError:
        await update.message.reply_text("Введите число от 0 до 100:"); return STATE_STORY_LOAD_MANUAL
    load = context.user_data.get("story_load", [])
    idx  = context.user_data.get("load_manual_idx", 0)
    if idx < len(load):
        aid = load[idx][0]; load[idx] = (aid, pct)
        context.user_data["story_load"] = load
    idx += 1; context.user_data["load_manual_idx"] = idx
    if idx < len(load):
        a = db_get_account(load[idx][0])
        await update.message.reply_text(f"% для @{a.username or a.phone}:")
        return STATE_STORY_LOAD_MANUAL
    total = sum(p for _, p in load)
    if total != 100:
        await update.message.reply_text(f"⚠️ Сумма = {total}%, нужно 100%. Введите заново.")
        context.user_data["load_manual_idx"] = 0
        a = db_get_account(load[0][0])
        await update.message.reply_text(f"% для @{a.username or a.phone}:")
        return STATE_STORY_LOAD_MANUAL
    await update.message.reply_text("Данные изменены ✅")
    return await _show_load_screen(update, context)

async def _show_flow_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid      = context.user_data.get("story_sphere_id")
    tid      = context.user_data.get("story_template_id")
    mid      = context.user_data.get("story_mode_id")
    end_time = context.user_data.get("story_end_time")
    load     = context.user_data.get("story_load", [])
    tmpl     = db_get_template(tid)
    mode     = db_get_mode(mid)
    photos   = json.loads(tmpl.photo_ids or "[]")
    texts    = json.loads(tmpl.texts     or "[]")
    time_str = end_time.strftime("%d.%m.%Y %H:%M") if end_time else "до отключения"
    acc_lines = []
    for aid, pct in load:
        a = db_get_account(aid)
        if a: acc_lines.append(f"@{a.username or a.phone} 🛠️{pct}%")
    preview = (
        f"📋 Шаблон: {tmpl.name}\n"
        f"📷 Фото: {len(photos)} шт. | 💬 Текстов: {len(texts)}\n"
        f"🔗 Кнопка: {tmpl.button_url or 'нет'}\n\n"
        f"⚙️ Режим: {mode.name}\n"
        f"⏱ Время: {time_str}\n"
        f"👥 Пользователей: {db_count_users(sid)}\n\n"
        f"{'—'*30}\n"
        f"Аккаунты:\n" + "\n".join(acc_lines) + "\n\n"
        f"Запустить поток?"
    )
    await update.message.reply_text(preview, reply_markup=kb_confirm())
    return STATE_FLOWS_MENU

# ===========================================================================
# HANDLERS — Потоки
# ===========================================================================

async def h_flows_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "✅ Запустить":
        sid      = context.user_data.get("story_sphere_id")
        tid      = context.user_data.get("story_template_id")
        mid      = context.user_data.get("story_mode_id")
        end_time = context.user_data.get("story_end_time")
        load     = context.user_data.get("story_load", [])
        if not load:
            await update.message.reply_text("Нет аккаунтов!", reply_markup=kb_story_main())
            return STATE_STORY_MENU
        fid  = db_create_flow(sid, tid, mid, end_time)
        for aid, pct in load: db_add_flow_account(fid, aid, pct)
        task = asyncio.create_task(_run_flow(fid))
        _active_flows[fid] = task
        acc_list = ", ".join(f"@{db_get_account(aid).username or db_get_account(aid).phone}" for aid, _ in load if db_get_account(aid))
        await update.message.reply_text(f"🚀 Поток #{fid} запущен!\nАккаунты: {acc_list}", reply_markup=kb_story_main())
        return STATE_STORY_MENU

    if text == "❌ Отмена":
        await update.message.reply_text("Отменено.", reply_markup=kb_story_main())
        return STATE_STORY_MENU

    if text == "🔙 Назад":
        await update.message.reply_text("📸 Отметки сторис:", reply_markup=kb_story_main())
        return STATE_STORY_MENU

    # Нажали на поток — ищем [id:N]
    fid = _extract_id(text)
    if fid:
        flow = db_get_flow(fid)
        if flow:
            context.user_data["story_flow_detail_id"] = fid
            fas       = db_get_flow_accounts(fid)
            acc_lines = []
            for fa in fas:
                a = db_get_account(fa.account_id)
                if a: acc_lines.append(f"@{a.username or a.phone} 🛠️{fa.load_pct}%")
            mins = int((datetime.utcnow() - flow.created_at).total_seconds() / 60)
            h, m = divmod(mins, 60); d, h = divmod(h, 24)
            detail = (
                f"Поток #{flow.id} {'🟢 Активен' if flow.status == 'running' else '🔴 Остановлен'}\n\n"
                f"📷 Сторис: {flow.stories_sent}\n"
                f"🧍 Отмечено: {flow.users_tagged}\n"
                f"⏱ Время: {d}д {h}ч {m}м\n"
                f"Аккаунтов: {len(fas)}\n\n"
                f"Распределение:\n" + "\n".join(acc_lines)
            )
            await update.message.reply_text(detail, reply_markup=kb_flow_detail())
            return STATE_FLOW_DETAIL

    sid = context.user_data.get("story_sphere_id")
    if sid:
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(db_get_flows(sid)))
    return STATE_FLOWS_MENU

async def h_flow_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    fid  = context.user_data.get("story_flow_detail_id")
    flow = db_get_flow(fid) if fid else None

    if text == "🔙 Назад":
        sid = context.user_data.get("story_sphere_id")
        flows = db_get_flows(sid) if sid else []
        await update.message.reply_text("Потоки:", reply_markup=kb_flows(flows))
        return STATE_FLOWS_MENU

    if text == "🛑 СТОП" and flow:
        db_stop_flow(fid)
        if fid in _active_flows: _active_flows[fid].cancel(); _active_flows.pop(fid, None)
        await update.message.reply_text(f"🔴 Поток #{fid} остановлен.", reply_markup=kb_story_main())
        return STATE_STORY_MENU

    if text == "📊 Нагрузка аккаунтов" and flow:
        fas   = db_get_flow_accounts(fid)
        lines = ["Распределение нагрузки:"]
        for fa in fas:
            a = db_get_account(fa.account_id)
            if a:
                load = db_get_account_load_pct(a.id)
                risk = "🔴" if load > 70 else ("🟡" if load > 40 else "🟢")
                lines.append(f"@{a.username or a.phone} 🛠️{fa.load_pct}% {risk}")
        await update.message.reply_text("\n".join(lines), reply_markup=kb_flow_detail())
        return STATE_FLOW_DETAIL

    if text == "📋 Логи работы" and flow:
        logs = json.loads(flow.logs or "[]")
        log_text = "\n".join(logs[-20:]) if logs else "Логов нет."
        await update.message.reply_text(f"Логи #{fid}:\n\n{log_text}", reply_markup=kb_flow_detail())
        return STATE_FLOW_DETAIL

    return STATE_FLOW_DETAIL

async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используйте кнопки меню.", reply_markup=kb_main())
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
            STATE_MAIN_MENU:           [MessageHandler(filters.TEXT & ~filters.COMMAND, h_main_menu)],
            STATE_SPHERE_MENU:         [MessageHandler(filters.TEXT & ~filters.COMMAND, h_sphere_menu)],
            STATE_ADD_SPHERE_NAME:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_sphere_name)],
            STATE_AFTER_STAGE:         [MessageHandler(filters.TEXT & ~filters.COMMAND, h_after_stage)],
            STATE_ADD_STAGE_DESC:      [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_stage_desc)],
            STATE_ADD_STAGE_SCRIPT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_stage_script)],
            STATE_ADD_GROUP_KEYWORDS:  [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_group_keywords)],
            STATE_ADD_USER_KEYWORDS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_user_keywords)],
            STATE_ADD_GROUP_LINKS:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_group_links)],
            STATE_SHOW_CLIENT:         [MessageHandler(filters.TEXT & ~filters.COMMAND, h_show_client)],
            STATE_EDIT_SCRIPT_CHOOSE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_script_choose)],
            STATE_EDIT_SCRIPT_DESC:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_script_desc)],
            STATE_EDIT_SCRIPT_TEXT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_script_text)],
            STATE_EDIT_KEYS_GROUP:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_keys_group)],
            STATE_EDIT_KEYS_USER:      [MessageHandler(filters.TEXT & ~filters.COMMAND, h_edit_keys_user)],
            STATE_REDO_STAGE_DESC:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_redo_stage_desc)],
            STATE_REDO_STAGE_SCRIPT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_redo_stage_script)],
            STATE_STORY_MENU:          [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_menu)],
            STATE_ACCOUNTS_MENU:       [MessageHandler(filters.TEXT & ~filters.COMMAND, h_accounts_menu)],
            STATE_ADD_ACC_PHONE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_acc_phone)],
            STATE_ADD_ACC_CODE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_acc_code)],
            STATE_ADD_ACC_PASS:        [MessageHandler(filters.TEXT & ~filters.COMMAND, h_add_acc_pass)],
            STATE_STORY_CHOOSE_SPHERE: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_choose_sphere)],
            STATE_STORY_TMPL_MENU:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_tmpl_menu)],
            STATE_STORY_NEW_TMPL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_new_tmpl_name)],
            STATE_STORY_ADD_PHOTO:     [MessageHandler(filters.PHOTO | filters.TEXT,    h_story_add_photo)],
            STATE_STORY_ADD_TEXT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_add_text)],
            STATE_STORY_BTN_TEXT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_btn_text)],
            STATE_STORY_BTN_MSG:       [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_btn_msg)],
            STATE_STORY_BTN_POS:       [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_btn_pos)],
            STATE_STORY_CHOOSE_MODE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_choose_mode)],
            STATE_STORY_CHOOSE_TIME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_choose_time)],
            STATE_STORY_CUSTOM_TIME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_custom_time)],
            STATE_STORY_SELECT_ACCS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_select_accs)],
            STATE_STORY_LOAD_DIST:     [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_load_dist)],
            STATE_STORY_LOAD_MANUAL:   [MessageHandler(filters.TEXT & ~filters.COMMAND, h_story_load_manual)],
            STATE_FLOWS_MENU:          [MessageHandler(filters.TEXT & ~filters.COMMAND, h_flows_menu)],
            STATE_FLOW_DETAIL:         [MessageHandler(filters.TEXT & ~filters.COMMAND, h_flow_detail)],
        },
        fallbacks=[CommandHandler("start", cmd_start), MessageHandler(filters.ALL, fallback)],
        allow_reentry=True,
    )
    app.add_handler(conv)

    async def on_startup(application):
        asyncio.create_task(background_worker())
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

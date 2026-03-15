import asyncio
import json
import logging
import random
from datetime import datetime, timedelta
from typing import Optional
 
from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer,
    String, Text, UniqueConstraint, create_engine,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User, Channel, Chat
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler,
)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
 
# ===========================================================================
# CONFIG
# ===========================================================================
 
BOT_TOKEN      = "8683047774:AAGWKswZoyzH7oK4nhXN6_FqhdA1fMXO_DA"
API_ID         = 20533422
API_HASH       = "744e04f35f8aae6803294a5f3989c35b"
SESSION_STRING = "1ApWapzMBu8GfTi-Z98bxTWatfDWW-pdK8Y3hdqE10ze5ljZlnhMlbaR-FnQ59CfZiJvVB9W-tTJueR64dOddGajpri6gIy7JUau5XrjOm38tfYs3oz3GJhhKAsn_yHFw-eYDc_TJigD78qEA8VYTS1GiFurA8lNOV_UxlE6jOcjwjdObGMSC8KVOC_uaLNzpP6ghT9ons-S3t5GsGUbRNWkAGJBilAwKo8eXwvb5TAfST9FaCDlZn98SDgsdyqcmqbrXZdlDF2hRLrs8qGaho8NDiuC2-oFDCKyHKtS78XOEVUVd8OZ0wn-At7msPzLOjEXdxQfiFJ1qp1V_xdCYLDyas213SO4="
 
DATABASE_URL          = "sqlite:///bot_data.db"
MIN_GROUPS_PER_SPHERE = 50
MIN_CLIENT_BUFFER     = 10
CLIENT_TTL_DAYS       = 7
MESSAGE_LOOKBACK_DAYS = 180
GROUP_CLIENT_LIMIT    = 150
PRE_COLLECT_MINUTES   = 10   # сколько минут собирать до первой выдачи
 
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
 
 
def init_db():
    Base.metadata.create_all(bind=engine)
 
# ===========================================================================
# DB HELPERS
# ===========================================================================
 
def db_get_spheres():
    with SessionLocal() as s:
        rows = s.query(Sphere).all()
        s.expunge_all()
        return rows
 
def db_get_sphere_by_name(name):
    with SessionLocal() as s:
        row = s.query(Sphere).filter(Sphere.name == name).first()
        if row: s.expunge(row)
        return row
 
def db_add_sphere(name, group_keywords, user_keywords):
    with SessionLocal() as s:
        sphere = Sphere(name=name, group_keywords=group_keywords, user_keywords=user_keywords)
        s.add(sphere); s.commit()
        return sphere.id
 
def db_add_stage(sphere_id, description, script):
    with SessionLocal() as s:
        count = s.query(Stage).filter(Stage.sphere_id == sphere_id).count()
        s.add(Stage(sphere_id=sphere_id, position=count+1, description=description, script=script))
        s.commit()
 
def db_get_stages(sphere_id):
    with SessionLocal() as s:
        rows = s.query(Stage).filter(Stage.sphere_id == sphere_id).order_by(Stage.position).all()
        s.expunge_all()
        return rows
 
def db_get_user_keywords(sphere_id):
    with SessionLocal() as s:
        row = s.query(Sphere.user_keywords).filter(Sphere.id == sphere_id).first()
        return json.loads(row[0] or "[]") if row else []
 
def db_count_groups(sphere_id):
    with SessionLocal() as s:
        return s.query(Group).filter(Group.sphere_id == sphere_id).count()
 
def db_save_groups(sphere_id, sphere_name, links):
    saved = 0
    with SessionLocal() as s:
        for link in links:
            s.add(Group(sphere_id=sphere_id, sphere_name=sphere_name, group_link=link, parsed=False, shown_count=0))
            try:
                s.flush(); saved += 1
            except IntegrityError:
                s.rollback()
        s.commit()
    return saved
 
def db_get_all_group_usernames(sphere_id):
    with SessionLocal() as s:
        rows = s.query(Group.group_link).filter(Group.sphere_id == sphere_id).all()
    return [link.rstrip("/").split("/")[-1] for (link,) in rows if link.rstrip("/").split("/")[-1]]
 
def db_mark_group_parsed(group_link):
    with SessionLocal() as s:
        g = s.query(Group).filter(Group.group_link == group_link).first()
        if g: g.parsed = True; s.commit()
 
def db_increment_group_shown(group_username, sphere_id):
    group_link = f"https://t.me/{group_username}"
    with SessionLocal() as s:
        g = s.query(Group).filter(Group.group_link == group_link, Group.sphere_id == sphere_id).first()
        if g:
            g.shown_count = (g.shown_count or 0) + 1
            if g.shown_count >= GROUP_CLIENT_LIMIT:
                s.delete(g)
            s.commit()
 
def db_cleanup_old_clients():
    cutoff = datetime.utcnow() - timedelta(days=CLIENT_TTL_DAYS)
    with SessionLocal() as s:
        s.query(Client).filter(Client.found_at < cutoff).delete(); s.commit()
 
def db_count_available_clients(sphere_id):
    db_cleanup_old_clients()
    with SessionLocal() as s:
        return s.query(Client).filter(Client.sphere_id == sphere_id, Client.used == False).count()
 
def db_get_random_client(sphere_id):
    db_cleanup_old_clients()
    with SessionLocal() as s:
        groups_with_clients = (
            s.query(Client.group_username)
            .filter(Client.sphere_id == sphere_id, Client.used == False)
            .distinct().all()
        )
        if not groups_with_clients: return None
        group_username = random.choice(groups_with_clients)[0]
        row = (
            s.query(Client)
            .filter(Client.sphere_id == sphere_id, Client.used == False, Client.group_username == group_username)
            .order_by(Client.id).first()
        )
        if row: s.expunge(row)
        return row
 
def db_mark_client_used(client_id):
    with SessionLocal() as s:
        c = s.query(Client).filter(Client.id == client_id).first()
        if c:
            c.used = True; group = c.group_username; s.commit(); return group
        return None
 
def db_is_username_known(sphere_id, username):
    with SessionLocal() as s:
        return s.query(Client).filter(Client.sphere_id == sphere_id, Client.username == username).first() is not None
 
def db_save_clients(sphere_id, group_username, usernames):
    saved = 0
    with SessionLocal() as s:
        for uname in usernames:
            s.add(Client(sphere_id=sphere_id, username=uname, group_username=group_username))
            try:
                s.flush(); saved += 1
            except IntegrityError:
                s.rollback()
        s.commit()
    return saved
 
def db_get_round_robin_index(sphere_id):
    with SessionLocal() as s:
        row = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sphere_id).first()
        return row.last_group_index if row else 0
 
def db_set_round_robin_index(sphere_id, idx):
    with SessionLocal() as s:
        row = s.query(GroupRoundRobin).filter(GroupRoundRobin.sphere_id == sphere_id).first()
        if row: row.last_group_index = idx
        else: s.add(GroupRoundRobin(sphere_id=sphere_id, last_group_index=idx))
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
# GROUP FINDER
# ===========================================================================
 
def _is_public_group(chat) -> bool:
    if isinstance(chat, Chat):
        return bool(getattr(chat, "username", None))
    if isinstance(chat, Channel):
        return bool(getattr(chat, "megagroup", False)) and bool(getattr(chat, "username", None))
    return False
 
 
async def _search_groups_by_keyword(keyword: str, limit: int = 100) -> list:
    client = await ensure_telethon()
    found = []
    try:
        result = await client(SearchRequest(q=keyword, limit=limit))
        for chat in result.chats:
            if not _is_public_group(chat): continue
            username = getattr(chat, "username", None)
            if username:
                found.append(f"https://t.me/{username.lower()}")
        logger.info("contacts.Search «%s» → %d chats, %d groups", keyword, len(result.chats), len(found))
    except Exception as exc:
        logger.warning("SearchRequest failed «%s»: %s", keyword, exc)
    return found
 
 
async def find_groups_for_sphere(sphere_id, sphere_name, keywords, min_groups=MIN_GROUPS_PER_SPHERE, status_callback=None):
    current = db_count_groups(sphere_id)
    if current >= min_groups:
        return current
 
    all_links = []
    for keyword in keywords:
        if db_count_groups(sphere_id) + len(all_links) >= min_groups:
            break
        try:
            links = await _search_groups_by_keyword(keyword, limit=100)
            new_links = [l for l in links if l not in all_links]
            all_links.extend(new_links)
            if status_callback:
                await status_callback(f"🔍 «{keyword}»: найдено {len(new_links)} групп (накоплено: {len(all_links)})")
            await asyncio.sleep(1.5)
        except Exception as exc:
            logger.error("Error searching «%s»: %s", keyword, exc)
 
    saved = db_save_groups(sphere_id, sphere_name, list(dict.fromkeys(all_links)))
    total = db_count_groups(sphere_id)
    if total < min_groups:
        logger.warning("Sphere «%s»: only %d groups (target %d)", sphere_name, total, min_groups)
    return total
 
# ===========================================================================
# CLIENT COLLECTION
# ===========================================================================
 
def _user_matches_keywords(username, first_name, last_name, bio, pinned_text, keywords):
    if not keywords: return True
    haystack = " ".join(filter(None, [username, first_name, last_name, bio, pinned_text])).lower()
    return any(kw.lower() in haystack for kw in keywords)
 
 
async def _get_user_bio(client, user_id):
    try:
        full = await client(GetFullUserRequest(user_id))
        return full.full_user.about or ""
    except Exception:
        return ""
 
 
async def collect_clients_from_group(group_username, sphere_id, user_keywords, limit=500):
    client   = await ensure_telethon()
    lookback = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set = set()
    collected = []
 
    try:
        entity    = await client.get_entity(group_username)
        offset_id = 0
        total_fetched = 0
 
        while total_fetched < limit:
            batch_size = min(100, limit - total_fetched)
            history = await client(GetHistoryRequest(
                peer=entity, limit=batch_size, offset_date=None,
                offset_id=offset_id, max_id=0, min_id=0, add_offset=0, hash=0,
            ))
            if not history.messages: break
 
            stop_early = False
            for msg in history.messages:
                if not msg.date: continue
                if msg.date.replace(tzinfo=None) < lookback:
                    stop_early = True; break
                if not getattr(msg, "from_id", None): continue
                user_id = getattr(msg.from_id, "user_id", None)
                if not user_id or user_id in seen_ids: continue
                seen_ids.add(user_id)
 
                try:
                    user = await client.get_entity(user_id)
                    if not isinstance(user, User) or not user.username: continue
                    uname = user.username.lower()
                    if db_is_username_known(sphere_id, uname): continue
 
                    bio = await _get_user_bio(client, user_id) if user_keywords else ""
                    if _user_matches_keywords(user.username, user.first_name, user.last_name, bio, None, user_keywords):
                        collected.append(uname)
                except Exception:
                    pass
 
            total_fetched += len(history.messages)
            offset_id      = history.messages[-1].id
            if stop_early or len(history.messages) < batch_size: break
            await asyncio.sleep(0.5)
 
    except Exception as exc:
        logger.warning("Error collecting from «%s»: %s", group_username, exc)
 
    saved = db_save_clients(sphere_id, group_username, collected)
    db_mark_group_parsed(f"https://t.me/{group_username}")
    logger.info("Collected %d clients from «%s»", saved, group_username)
    return saved
 
 
async def fill_client_buffer(sphere_id):
    if db_count_available_clients(sphere_id) >= MIN_CLIENT_BUFFER: return
 
    if db_count_groups(sphere_id) < MIN_GROUPS_PER_SPHERE:
        with SessionLocal() as s:
            sphere = s.query(Sphere).filter(Sphere.id == sphere_id).first()
            if sphere:
                keywords = json.loads(sphere.group_keywords or "[]")
                name     = sphere.name
        await find_groups_for_sphere(sphere_id=sphere_id, sphere_name=name, keywords=keywords)
 
    groups = db_get_all_group_usernames(sphere_id)
    if not groups: return
 
    user_keywords = db_get_user_keywords(sphere_id)
    idx = db_get_round_robin_index(sphere_id)
    attempts = 0
 
    while db_count_available_clients(sphere_id) < MIN_CLIENT_BUFFER and attempts < len(groups):
        await collect_clients_from_group(groups[idx % len(groups)], sphere_id, user_keywords)
        idx = (idx + 1) % len(groups)
        attempts += 1
 
    db_set_round_robin_index(sphere_id, idx)
 
 
# ===========================================================================
# НОВАЯ ФУНКЦИЯ — 10 минут сбора с прогрессом
# ===========================================================================
 
async def pre_collect_for_minutes(sphere_id: int, sphere_name: str, keywords: list,
                                   user_keywords: list, minutes: int,
                                   progress_callback) -> int:
    """
    Собирает клиентов в течение `minutes` минут.
    Каждую минуту отправляет обновление через progress_callback.
    Возвращает количество собранных клиентов.
    """
    deadline = datetime.utcnow() + timedelta(minutes=minutes)
 
    # Шаг 1 — найти группы если их нет
    if db_count_groups(sphere_id) < MIN_GROUPS_PER_SPHERE:
        await find_groups_for_sphere(
            sphere_id=sphere_id,
            sphere_name=sphere_name,
            keywords=keywords,
        )
 
    groups = db_get_all_group_usernames(sphere_id)
    if not groups:
        await progress_callback("⚠️ Группы не найдены. Попробуйте другие ключевые слова.")
        return 0
 
    idx = db_get_round_robin_index(sphere_id)
    last_report_minute = minutes  # отсчёт идёт вниз
 
    while datetime.utcnow() < deadline:
        group = groups[idx % len(groups)]
        await collect_clients_from_group(group, sphere_id, user_keywords)
        idx = (idx + 1) % len(groups)
 
        # считаем сколько минут осталось
        remaining = (deadline - datetime.utcnow()).total_seconds()
        remaining_minutes = int(remaining / 60)
 
        # отправляем обновление раз в минуту
        if remaining_minutes < last_report_minute:
            last_report_minute = remaining_minutes
            count = db_count_available_clients(sphere_id)
            if remaining_minutes > 0:
                await progress_callback(
                    f"⏳ Сбор идёт... осталось {remaining_minutes} мин.\n"
                    f"Собрано клиентов: {count}"
                )
 
    db_set_round_robin_index(sphere_id, idx)
    total = db_count_available_clients(sphere_id)
    return total
 
 
# ===========================================================================
# BOT STATES
# ===========================================================================
 
(
    STATE_MAIN_MENU, STATE_SPHERE_MENU, STATE_ADD_SPHERE_NAME,
    STATE_AFTER_STAGE, STATE_ADD_STAGE_DESC, STATE_ADD_STAGE_SCRIPT,
    STATE_ADD_GROUP_KEYWORDS, STATE_ADD_USER_KEYWORDS, STATE_SHOW_CLIENT,
    STATE_COLLECTING,
) = range(10)
 
# ===========================================================================
# KEYBOARDS
# ===========================================================================
 
def main_menu_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("🖐🏻 Ручной поиск")]], resize_keyboard=True)
 
def sphere_menu_keyboard():
    spheres = db_get_spheres()
    buttons = [[KeyboardButton(s.name)] for s in spheres]
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
 
        # Если клиентов уже достаточно — сразу показываем
        if db_count_available_clients(sphere.id) >= MIN_CLIENT_BUFFER:
            asyncio.create_task(fill_client_buffer(sphere.id))
            return await show_next_client(update, context)
 
        # Иначе — запускаем 10-минутный сбор
        await update.message.reply_text(
            f"🔍 Запускаю сбор клиентов для сферы «{sphere.name}».\n"
            f"⏱ Это займёт {PRE_COLLECT_MINUTES} минут — я буду сообщать о прогрессе.\n"
            f"После окончания сразу покажу первого клиента.",
            reply_markup=ReplyKeyboardRemove(),
        )
 
        async def progress_cb(msg: str):
            try:
                await update.message.reply_text(msg)
            except Exception:
                pass
 
        with SessionLocal() as s:
            sp = s.query(Sphere).filter(Sphere.id == sphere.id).first()
            group_kws = json.loads(sp.group_keywords or "[]") if sp else []
            user_kws  = json.loads(sp.user_keywords  or "[]") if sp else []
            sp_name   = sp.name if sp else sphere.name
 
        total = await pre_collect_for_minutes(
            sphere_id       = sphere.id,
            sphere_name     = sp_name,
            keywords        = group_kws,
            user_keywords   = user_kws,
            minutes         = PRE_COLLECT_MINUTES,
            progress_callback = progress_cb,
        )
 
        if total == 0:
            await update.message.reply_text(
                "😔 Не удалось собрать клиентов. Попробуйте другие ключевые слова.",
                reply_markup=sphere_menu_keyboard(),
            )
            return STATE_SPHERE_MENU
 
        await update.message.reply_text(
            f"✅ Сбор завершён! Найдено клиентов: {total}\nПоказываю первого 👇"
        )
        return await show_next_client(update, context)
 
    await update.message.reply_text("Сфера не найдена.", reply_markup=sphere_menu_keyboard())
    return STATE_SPHERE_MENU
 
async def show_next_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sphere_id   = context.user_data.get("current_sphere_id")
    sphere_name = context.user_data.get("current_sphere_name")
    if not sphere_id:
        await update.message.reply_text("Ошибка. Вернитесь в главное меню.", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU
    client_row = db_get_random_client(sphere_id)
    if not client_row:
        await update.message.reply_text(
            "⏳ Клиенты закончились. Идёт дополнительный сбор — попробуйте чуть позже.",
            reply_markup=client_keyboard(),
        )
        asyncio.create_task(fill_client_buffer(sphere_id))
        return STATE_SHOW_CLIENT
    context.user_data["current_client_id"] = client_row.id
    stages = db_get_stages(sphere_id)
    lines  = [f"<b>@{client_row.username}</b>\n", f"<b>{sphere_name}</b>"]
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
            group_uname = db_mark_client_used(cid)
            if group_uname and (sid := context.user_data.get("current_sphere_id")):
                db_increment_group_shown(group_uname, sid)
        return await show_next_client(update, context)
    if text == "Главное меню":
        if cid := context.user_data.get("current_client_id"):
            group_uname = db_mark_client_used(cid)
            if group_uname and (sid := context.user_data.get("current_sphere_id")):
                db_increment_group_shown(group_uname, sid)
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
            "📌 Шаг 1 из 2: Введите ключевые слова для поиска групп\n(каждое слово с новой строки):",
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
    await update.message.reply_text("✅ Этап сохранён. Добавьте ещё или продолжите:", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE
 
async def handle_add_group_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = [kw.strip() for kw in update.message.text.strip().splitlines() if kw.strip()]
    if not keywords:
        await update.message.reply_text("Введите хотя бы одно ключевое слово:")
        return STATE_ADD_GROUP_KEYWORDS
    context.user_data["new_sphere"]["group_keywords"] = keywords
    await update.message.reply_text(
        "📌 Шаг 2 из 2: Ключевые слова для фильтрации пользователей\n"
        "(ищутся в username, имени, bio)\n\n"
        "Пример:\nманикюр\nnail master\n\n"
        "Если фильтрация не нужна — напишите <b>нет</b>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_ADD_USER_KEYWORDS
 
async def handle_add_user_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    user_keywords = [] if raw.lower() in ("нет", "no", "-", "") else [kw.strip() for kw in raw.splitlines() if kw.strip()]
 
    new_sphere = context.user_data.get("new_sphere", {})
    name       = new_sphere.get("name", "")
    stages     = new_sphere.get("stages", [])
    group_kws  = new_sphere.get("group_keywords", [])
 
    sphere_id = db_add_sphere(
        name=name,
        group_keywords=json.dumps(group_kws,     ensure_ascii=False),
        user_keywords =json.dumps(user_keywords, ensure_ascii=False),
    )
    for stage in stages:
        db_add_stage(sphere_id, stage["desc"], stage["script"])
 
    kw_info = f"Ключевых слов для фильтрации: {len(user_keywords)}" if user_keywords else "Фильтрация: отключена"
    await update.message.reply_text(
        f"💾 Сфера «{name}» сохранена!\n"
        f"Ключевых слов для групп: {len(group_kws)}\n"
        f"{kw_info}\n\n🔍 Ищу группы..."
    )
 
    async def status_cb(msg):
        try: await update.message.reply_text(msg)
        except Exception: pass
 
    try:
        total = await find_groups_for_sphere(
            sphere_id=sphere_id, sphere_name=name,
            keywords=group_kws, status_callback=status_cb,
        )
        if total > 0:
            await update.message.reply_text(
                f"✅ Найдено групп: {total}.\n"
                f"Когда выберете эту сферу — начнётся {PRE_COLLECT_MINUTES}-минутный сбор клиентов."
            )
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
 
def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_MAIN_MENU:          [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)],
            STATE_SPHERE_MENU:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sphere_menu)],
            STATE_ADD_SPHERE_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_sphere_name)],
            STATE_AFTER_STAGE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_after_stage)],
            STATE_ADD_STAGE_DESC:     [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_desc)],
            STATE_ADD_STAGE_SCRIPT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_stage_script)],
            STATE_ADD_GROUP_KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_group_keywords)],
            STATE_ADD_USER_KEYWORDS:  [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_user_keywords)],
            STATE_SHOW_CLIENT:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_show_client)],
        },
        fallbacks=[CommandHandler("start", cmd_start), MessageHandler(filters.ALL, fallback)],
        allow_reentry=True,
    )
    app.add_handler(conv)
    logger.info("Bot started.")
    app.run_polling()
 
 
if __name__ == "__main__":
    main()
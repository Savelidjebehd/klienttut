 
import asyncio
import json
import logging
import random
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
MAX_GROUPS            = 100    # лимит групп на сферу
MAX_CLIENTS           = 150    # лимит клиентов на сферу
LOW_CLIENTS           = 50     # порог запуска нового сбора
CRITICAL_CLIENTS      = 20     # порог подключения поиска групп
GROUP_CLIENT_LIMIT    = 150    # макс показов из одной группы
CLIENT_TTL_DAYS       = 30     # клиенты не старше N дней
MESSAGE_LOOKBACK_DAYS = 90     # сообщения не старше N дней
BG_CHECK_INTERVAL     = 120    # секунд между проверками воркера
BG_SLEEP_AFTER_GROUPS = 3600   # отдых после поиска групп (1 час)
BG_KEYWORDS_PER_PASS  = 5      # ключевых слов за один пуск поиска групп
 
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
 
 
def init_db() -> None:
    """Создаёт таблицы если нет. Существующие данные НЕ трогает."""
    Base.metadata.create_all(bind=engine)
    # Безопасная миграция — добавляем колонку если её нет
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE spheres ADD COLUMN user_keywords TEXT NOT NULL DEFAULT '[]'"))
            conn.commit()
            logger.info("Migration: added user_keywords column")
        except OperationalError:
            pass  # уже есть
 
# ===========================================================================
# DB HELPERS
# ===========================================================================
 
def db_get_spheres():
    with SessionLocal() as s:
        rows = s.query(Sphere).all()
        s.expunge_all()
        return rows
 
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
        s.add(obj); s.commit()
        return obj.id
 
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
        s.expunge_all()
        return rows
 
def db_update_stage_script(stage_id: int, description: str, script: str) -> None:
    with SessionLocal() as s:
        row = s.query(Stage).filter(Stage.id == stage_id).first()
        if row:
            row.description = description
            row.script = script
            s.commit()
 
def db_delete_stages(sphere_id: int) -> None:
    with SessionLocal() as s:
        s.query(Stage).filter(Stage.sphere_id == sphere_id).delete()
        s.commit()
 
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
            if g.shown_count >= GROUP_CLIENT_LIMIT:
                s.delete(g)
            s.commit()
 
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
    """Случайный клиент из случайной группы — не подряд из одной группы."""
    db_cleanup_old_clients()
    with SessionLocal() as s:
        grps = s.query(Client.group_username).filter(
            Client.sphere_id == sphere_id, Client.used == False
        ).distinct().all()
        if not grps: return None
        gu  = random.choice(grps)[0]
        row = s.query(Client).filter(
            Client.sphere_id == sphere_id,
            Client.used == False,
            Client.group_username == gu,
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
# ПРОГРЕСС-БАР
# ===========================================================================
 
def make_progress_bar(current: int, total: int, width: int = 16) -> str:
    pct    = min(100, int(current / total * 100)) if total else 0
    filled = int(width * pct / 100)
    return f"{'Ӏ' * filled}{' ' * (width - filled)} {pct}%"
 
# ===========================================================================
# ФОНОВЫЕ ЗАДАЧИ — ВСЁ ТОЛЬКО ЧЕРЕЗ create_task, НИКОГДА НЕ await В ХЕНДЛЕРАХ
# ===========================================================================
 
# Множество sphere_id для которых уже запущен сбор — защита от дублей
_collecting: set = set()
# Время последнего поиска групп по каждой сфере
_last_group_search: dict = {}
 
 
def _is_public_group(chat) -> bool:
    if isinstance(chat, Chat):
        return bool(getattr(chat, "username", None))
    if isinstance(chat, Channel):
        return bool(getattr(chat, "megagroup", False)) and bool(getattr(chat, "username", None))
    return False
 
 
async def _search_one_keyword(keyword: str) -> list:
    """Ищет группы по одному ключевому слову. Отдаёт управление после каждого запроса."""
    client = await ensure_telethon()
    found  = []
    try:
        result = await client(SearchRequest(q=keyword, limit=100))
        for chat in result.chats:
            if not _is_public_group(chat): continue
            u = getattr(chat, "username", None)
            if u: found.append(f"https://t.me/{u.lower()}")
        logger.info("Search «%s» → %d groups", keyword, len(found))
    except Exception as exc:
        logger.warning("SearchRequest failed «%s»: %s", keyword, exc)
    # Обязательный yield после сетевого запроса
    await asyncio.sleep(0)
    return found
 
 
async def _find_groups_bg(
    sphere_id: int,
    sphere_name: str,
    keywords: list,
    progress_message=None,
) -> int:
    """
    Поиск групп — берёт BG_KEYWORDS_PER_PASS слов за раз.
    Каждый запрос отдаёт управление event loop через await asyncio.sleep(0).
    progress_message обновляется не чаще раз в 3 сек.
    """
    if db_count_groups(sphere_id) >= MAX_GROUPS:
        return db_count_groups(sphere_id)
 
    batch     = keywords[:BG_KEYWORDS_PER_PASS]
    total_kw  = len(batch)
    all_links: list = []
    last_edit = 0.0
 
    for i, kw in enumerate(batch):
        if db_count_groups(sphere_id) + len(all_links) >= MAX_GROUPS:
            break
 
        # Сетевой запрос — внутри уже есть await asyncio.sleep(0)
        links     = await _search_one_keyword(kw)
        new_links = [l for l in links if l not in all_links]
        all_links.extend(new_links)
 
        # Обновляем прогресс-бар не чаще раз в 3 сек
        now = asyncio.get_event_loop().time()
        if progress_message and (now - last_edit) >= 3.0:
            bar = make_progress_bar(i + 1, total_kw)
            try:
                await progress_message.edit_text(
                    f"🔍 Поиск групп...\n"
                    f"Время поиска: ~10 минут\n\n"
                    f"{bar}\n\n"
                    f"Обработано: {i+1}/{total_kw} слов\n"
                    f"Найдено групп: {len(all_links)}\n\n"
                    f"✅ Бот работает — нажимайте кнопки!"
                )
                last_edit = now
            except Exception:
                pass
        # Пауза между запросами — отдаём управление
        await asyncio.sleep(1.5)
 
    saved = db_save_groups(sphere_id, sphere_name, list(dict.fromkeys(all_links)))
    total = db_count_groups(sphere_id)
 
    if progress_message:
        try:
            await progress_message.edit_text(
                f"✅ Поиск групп завершён!\n\n"
                f"{make_progress_bar(1, 1)}\n\n"
                f"Найдено групп: {total}"
            )
        except Exception:
            pass
 
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
    Собирает пользователей из одной группы.
    Условие: написал хотя бы 1 раз за MESSAGE_LOOKBACK_DAYS дней.
    После каждого батча сообщений — await asyncio.sleep(0) для yield.
    """
    client   = await ensure_telethon()
    lookback = datetime.utcnow() - timedelta(days=MESSAGE_LOOKBACK_DAYS)
    seen_ids: set   = set()
    collected: list = []
 
    try:
        entity = await client.get_entity(group_username)
        # yield после получения entity
        await asyncio.sleep(0)
 
        offset_id, total_fetched = 0, 0
 
        while total_fetched < limit:
            batch = min(100, limit - total_fetched)
            history = await client(GetHistoryRequest(
                peer=entity, limit=batch, offset_date=None,
                offset_id=offset_id, max_id=0, min_id=0, add_offset=0, hash=0,
            ))
            # yield после каждого батча истории
            await asyncio.sleep(0)
 
            if not history.messages: break
 
            stop = False
            for msg in history.messages:
                if not msg.date: continue
                if msg.date.replace(tzinfo=None) < lookback:
                    stop = True; break
                if not getattr(msg, "from_id", None): continue
                uid = getattr(msg.from_id, "user_id", None)
                if not uid or uid in seen_ids: continue
                seen_ids.add(uid)
 
                try:
                    user = await client.get_entity(uid)
                    # yield после каждого пользователя
                    await asyncio.sleep(0)
 
                    # Условие: просто есть username — написал хотя бы 1 раз
                    if not isinstance(user, User) or not user.username: continue
                    un = user.username.lower()
                    if db_is_username_known(sphere_id, un): continue
 
                    bio = await _get_user_bio(client, uid) if user_keywords else ""
                    if _user_matches_keywords(
                        user.username, user.first_name, user.last_name, bio, user_keywords
                    ):
                        collected.append(un)
                except Exception:
                    pass
 
            total_fetched += len(history.messages)
            offset_id      = history.messages[-1].id
            if stop or len(history.messages) < batch: break
            # Пауза между батчами
            await asyncio.sleep(0.5)
 
    except Exception as exc:
        logger.warning("Error collecting «%s»: %s", group_username, exc)
 
    saved = db_save_clients(sphere_id, group_username, collected)
    db_mark_group_parsed(f"https://t.me/{group_username}")
    logger.info("Collected %d clients from «%s»", saved, group_username)
    return saved
 
 
async def _fill_buffer_bg(sphere_id: int, progress_message=None) -> None:
    """
    Фоновая задача пополнения буфера клиентов.
    Запускается ТОЛЬКО через asyncio.create_task.
    
    Логика:
    1. Если групп < 10 → сначала ищем группы
    2. Собираем клиентов из групп
    3. Если клиентов всё ещё < CRITICAL_CLIENTS → ищем ещё группы и снова собираем
    """
    if sphere_id in _collecting:
        logger.info("[FILL] sphere_id=%d already collecting, skip", sphere_id)
        return
 
    _collecting.add(sphere_id)
    try:
        sphere = db_get_sphere(sphere_id)
        if not sphere: return
 
        group_kws = json.loads(sphere.group_keywords or "[]")
        user_kws  = db_get_user_keywords(sphere_id)
 
        # Шаг 1: если совсем мало групп — ищем
        if db_count_groups(sphere_id) < 10:
            await _find_groups_bg(
                sphere_id=sphere_id,
                sphere_name=sphere.name,
                keywords=group_kws,
                progress_message=progress_message,
            )
 
        # Шаг 2: собираем клиентов
        groups = db_get_all_group_usernames(sphere_id)
        if not groups: return
 
        if progress_message:
            try:
                await progress_message.edit_text(
                    f"👥 Собираю клиентов...\n\n"
                    f"{make_progress_bar(0, len(groups))}\n\n"
                    f"✅ Бот работает — нажимайте кнопки!"
                )
            except Exception:
                pass
 
        idx, att  = db_get_round_robin_index(sphere_id), 0
        last_edit = asyncio.get_event_loop().time()
 
        while db_count_available_clients(sphere_id) < MAX_CLIENTS and att < len(groups):
            await _collect_one_group(groups[idx % len(groups)], sphere_id, user_kws)
            idx = (idx + 1) % len(groups)
            att += 1
 
            # Обновляем прогресс не чаще раз в 3 сек
            now = asyncio.get_event_loop().time()
            if progress_message and (now - last_edit) >= 3.0:
                bar = make_progress_bar(att, len(groups))
                cnt = db_count_available_clients(sphere_id)
                try:
                    await progress_message.edit_text(
                        f"👥 Собираю клиентов...\n\n"
                        f"{bar}\n\n"
                        f"Найдено: {cnt}\n"
                        f"✅ Бот работает — нажимайте кнопки!"
                    )
                    last_edit = now
                except Exception:
                    pass
 
            # Yield после каждой группы
            await asyncio.sleep(0)
 
        db_set_round_robin_index(sphere_id, idx)
 
        # Шаг 3: если клиентов критически мало — ищем новые группы и снова собираем
        if db_count_available_clients(sphere_id) < CRITICAL_CLIENTS:
            logger.info("[FILL] sphere_id=%d critical low, searching more groups", sphere_id)
            await _find_groups_bg(
                sphere_id=sphere_id,
                sphere_name=sphere.name,
                keywords=group_kws,
            )
            groups2 = db_get_all_group_usernames(sphere_id)
            idx2, att2 = db_get_round_robin_index(sphere_id), 0
            while db_count_available_clients(sphere_id) < MAX_CLIENTS and att2 < len(groups2):
                await _collect_one_group(groups2[idx2 % len(groups2)], sphere_id, user_kws)
                idx2 = (idx2 + 1) % len(groups2)
                att2 += 1
                await asyncio.sleep(0)
            db_set_round_robin_index(sphere_id, idx2)
 
        total = db_count_available_clients(sphere_id)
 
        if progress_message:
            try:
                await progress_message.edit_text(
                    f"✅ Готово! Найдено клиентов: {total}\n"
                    f"Нажмите «➡️ Следующий» чтобы начать."
                )
            except Exception:
                pass
 
    except Exception as exc:
        logger.error("[FILL] sphere_id=%d: %s", sphere_id, exc)
    finally:
        _collecting.discard(sphere_id)
 
 
async def background_worker() -> None:
    """
    Постоянный фоновый воркер.
    Проверяет каждые BG_CHECK_INTERVAL секунд.
    Запускает задачи через create_task — не блокирует бота.
    
    Поиск групп: BG_KEYWORDS_PER_PASS слов за пуск, потом час отдыха.
    Поиск клиентов: постоянно, пока < LOW_CLIENTS.
    """
    logger.info("Background worker started.")
 
    while True:
        try:
            now = asyncio.get_event_loop().time()
 
            for sphere in db_get_spheres():
                sid = sphere.id
                cc  = db_count_available_clients(sid)
                gc  = db_count_groups(sid)
                logger.info("[BG] «%s»: groups=%d clients=%d", sphere.name, gc, cc)
 
                # Нужен сбор клиентов
                if cc < LOW_CLIENTS and sid not in _collecting:
                    asyncio.create_task(_fill_buffer_bg(sid))
 
                # Нужен поиск групп (с часовым отдыхом)
                last = _last_group_search.get(sid, 0)
                if gc < MAX_GROUPS and (now - last) >= BG_SLEEP_AFTER_GROUPS:
                    _last_group_search[sid] = now
                    kws = json.loads(sphere.group_keywords or "[]")
                    asyncio.create_task(
                        _find_groups_bg(sphere_id=sid, sphere_name=sphere.name, keywords=kws)
                    )
 
                # Yield между сферами
                await asyncio.sleep(1)
 
        except Exception as exc:
            logger.error("[BG] loop: %s", exc)
 
        await asyncio.sleep(BG_CHECK_INTERVAL)
 
# ===========================================================================
# STATES
# ===========================================================================
 
(
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
) = range(16)
 
# ===========================================================================
# KEYBOARDS
# ===========================================================================
 
def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🖐🏻 Ручной поиск")]],
        resize_keyboard=True,
    )
 
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
    return ReplyKeyboardMarkup(
        [[KeyboardButton("+Этап"), KeyboardButton("Продолжить")]],
        resize_keyboard=True,
    )
 
def edit_script_keyboard(stages: list):
    btns, row = [], []
    for i in range(len(stages)):
        row.append(KeyboardButton(str(i + 1)))
        if len(row) == 2:
            btns.append(row); row = []
    if row: btns.append(row)
    btns.append([KeyboardButton("Все"), KeyboardButton("Назад")])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)
 
# ===========================================================================
# HANDLERS — все отвечают мгновенно, никаких await на тяжёлых функциях
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
            # ✅ Клиенты есть — мгновенно показываем, сбор в фоне
            if sphere.id not in _collecting:
                asyncio.create_task(_fill_buffer_bg(sphere.id))
            return await show_next_client(update, context)
 
        # Клиентов нет — отправляем одно сообщение с прогресс-баром
        # и запускаем поиск В ФОНЕ через create_task
        prog_msg = await update.message.reply_text(
            f"🔍 Начинаю поиск для «{sphere.name}»...\n\n"
            f"{make_progress_bar(0, 1)}\n\n"
            f"✅ Бот работает — нажимайте кнопки!\n"
            f"Как найдём клиентов — обновлю это сообщение.",
            reply_markup=client_keyboard(),
        )
        # create_task — хендлер СРАЗУ возвращает управление боту
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
            f"⏳ Клиентов пока нет. {status}\n"
            f"Нажмите «➡️ Следующий» через минуту.",
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
 
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=client_keyboard(),
    )
 
    # Фоновое пополнение если клиентов мало
    if db_count_available_clients(sid) < LOW_CLIENTS and sid not in _collecting:
        asyncio.create_task(_fill_buffer_bg(sid))
 
    return STATE_SHOW_CLIENT
 
 
async def handle_show_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
 
    def _mark():
        if cid := context.user_data.get("current_client_id"):
            sid = context.user_data.get("current_sphere_id")
            gu  = db_mark_client_used(cid)
            if gu and sid:
                db_increment_group_shown(gu, sid)
 
    if text == "➡️ Следующий":
        _mark()
        return await show_next_client(update, context)
 
    if text == "🏠 Главное меню":
        _mark()
        for k in ("current_sphere_id", "current_sphere_name", "current_client_id"):
            context.user_data.pop(k, None)
        await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())
        return STATE_MAIN_MENU
 
    if text == "🗑 Удалить сферу":
        sid   = context.user_data.get("current_sphere_id")
        sname = context.user_data.get("current_sphere_name")
        if sid:
            db_delete_sphere(sid)
            _collecting.discard(sid)
            for k in ("current_sphere_id", "current_sphere_name", "current_client_id"):
                context.user_data.pop(k, None)
            await update.message.reply_text(
                f"🗑 Сфера «{sname}» удалена.",
                reply_markup=sphere_menu_keyboard(),
            )
        return STATE_SPHERE_MENU
 
    if text == "✏️ Изменить скрипт":
        sid    = context.user_data.get("current_sphere_id")
        stages = db_get_stages(sid)
        if not stages:
            await update.message.reply_text("Этапов нет.", reply_markup=client_keyboard())
            return STATE_SHOW_CLIENT
        context.user_data["edit_stages"] = [
            {"id": st.id, "desc": st.description, "script": st.script}
            for st in stages
        ]
        await update.message.reply_text(
            "Какой этап хотите изменить?",
            reply_markup=edit_script_keyboard(stages),
        )
        return STATE_EDIT_SCRIPT_CHOOSE
 
    if text == "🔑 Изменить ключи":
        sid = context.user_data.get("current_sphere_id")
        sp  = db_get_sphere(sid)
        if sp:
            kws = json.loads(sp.group_keywords or "[]")
            await update.message.reply_text(
                f"Текущие ключи групп:\n{chr(10).join(kws)}\n\n"
                f"Введите новые (каждый с новой строки):",
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
        await update.message.reply_text(
            "Введите пояснение 1-го этапа:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_REDO_STAGE_DESC
 
    try:
        idx = int(text) - 1
        if 0 <= idx < len(stages):
            context.user_data["editing_stage_idx"] = idx
            st = stages[idx]
            await update.message.reply_text(
                f"Этап {idx+1}: «{st['desc']}»\n\nВведите новое пояснение:",
                reply_markup=ReplyKeyboardRemove(),
            )
            return STATE_EDIT_SCRIPT_DESC
    except ValueError:
        pass
 
    await update.message.reply_text(
        "Выберите номер:",
        reply_markup=edit_script_keyboard(stages),
    )
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
 
 
# --- Пересоздание всех этапов ---
 
async def handle_redo_stage_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_sphere"]["current_stage_desc"] = update.message.text.strip()
    await update.message.reply_text("Введите текст скрипта:")
    return STATE_REDO_STAGE_SCRIPT
 
 
async def handle_redo_stage_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    script = update.message.text.strip()
    desc   = context.user_data["new_sphere"].pop("current_stage_desc", "")
    context.user_data["new_sphere"]["stages"].append({"desc": desc, "script": script})
    await update.message.reply_text(
        "✅ Этап сохранён. Добавьте ещё или завершите:",
        reply_markup=after_stage_keyboard(),
    )
    return STATE_AFTER_STAGE
 
 
# --- Изменение ключей ---
 
async def handle_edit_keys_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kws = [k.strip() for k in update.message.text.strip().splitlines() if k.strip()]
    context.user_data["new_group_kws"] = kws
    sid  = context.user_data.get("current_sphere_id")
    sp   = db_get_sphere(sid)
    ukws = json.loads(sp.user_keywords or "[]") if sp else []
    await update.message.reply_text(
        f"Текущие ключи пользователей:\n"
        f"{chr(10).join(ukws) if ukws else '(не заданы)'}\n\n"
        f"Введите новые (или «нет» для отключения):",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_EDIT_KEYS_USER
 
 
async def handle_edit_keys_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = (
        [] if raw.lower() in ("нет", "no", "-", "")
        else [k.strip() for k in raw.splitlines() if k.strip()]
    )
    sid  = context.user_data.get("current_sphere_id")
    gkws = context.user_data.pop("new_group_kws", [])
    db_update_sphere_keywords(
        sid,
        json.dumps(gkws, ensure_ascii=False),
        json.dumps(ukws, ensure_ascii=False),
    )
    await update.message.reply_text("Ключи обновлены ✅", reply_markup=main_menu_keyboard())
    return STATE_MAIN_MENU
 
 
# --- Создание сферы ---
 
async def handle_add_sphere_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Введите название:")
        return STATE_ADD_SPHERE_NAME
    context.user_data["new_sphere"]["name"] = name
    await update.message.reply_text("Добавьте этапы переписки:", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE
 
 
async def handle_after_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    ns   = context.user_data.get("new_sphere", {})
 
    if text == "+Этап":
        await update.message.reply_text(
            "Введите пояснение этапа:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return STATE_REDO_STAGE_DESC if ns.get("redo") else STATE_ADD_STAGE_DESC
 
    if text == "Продолжить":
        if ns.get("redo"):
            sid = context.user_data.get("current_sphere_id")
            for st in ns.get("stages", []):
                db_add_stage(sid, st["desc"], st["script"])
            context.user_data.pop("new_sphere", None)
            await update.message.reply_text("Данные изменены ✅", reply_markup=main_menu_keyboard())
            return STATE_MAIN_MENU
        await update.message.reply_text(
            "📌 Шаг 1 из 2: Ключевые слова для поиска групп\n(каждое с новой строки):",
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
    await update.message.reply_text("✅ Этап сохранён.", reply_markup=after_stage_keyboard())
    return STATE_AFTER_STAGE
 
 
async def handle_add_group_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kws = [k.strip() for k in update.message.text.strip().splitlines() if k.strip()]
    if not kws:
        await update.message.reply_text("Введите хотя бы одно слово:")
        return STATE_ADD_GROUP_KEYWORDS
    context.user_data["new_sphere"]["group_keywords"] = kws
    await update.message.reply_text(
        "📌 Шаг 2 из 2: Ключевые слова для фильтрации пользователей\n"
        "(username, имя, bio)\n\n"
        "Если не нужно — напишите <b>нет</b>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )
    return STATE_ADD_USER_KEYWORDS
 
 
async def handle_add_user_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw  = update.message.text.strip()
    ukws = (
        [] if raw.lower() in ("нет", "no", "-", "")
        else [k.strip() for k in raw.splitlines() if k.strip()]
    )
    ns   = context.user_data.get("new_sphere", {})
    name = ns.get("name", "")
    stages = ns.get("stages", [])
    gkws   = ns.get("group_keywords", [])
 
    sid = db_add_sphere(
        name=name,
        group_keywords=json.dumps(gkws,  ensure_ascii=False),
        user_keywords =json.dumps(ukws,  ensure_ascii=False),
    )
    for st in stages:
        db_add_stage(sid, st["desc"], st["script"])
 
    kw_info = f"Ключей для фильтрации: {len(ukws)}" if ukws else "Фильтрация: отключена"
    await update.message.reply_text(
        f"💾 Сфера «{name}» сохранена!\n"
        f"Ключей для групп: {len(gkws)}\n{kw_info}\n\n"
        f"🔍 Поиск запущен в фоне. Бот уже работает!"
    )
 
    # Одно сообщение с прогресс-баром — обновляется фоновой задачей
    prog = await update.message.reply_text(
        f"🔍 Поиск групп...\nВремя поиска: ~10 минут\n\n"
        f"{make_progress_bar(0, 1)}"
    )
    # create_task — хендлер СРАЗУ отдаёт управление
    asyncio.create_task(_fill_buffer_bg(sid, progress_message=prog))
 
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
# MAIN
# ===========================================================================
 
def main() -> None:
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
            STATE_EDIT_SCRIPT_CHOOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_script_choose)],
            STATE_EDIT_SCRIPT_DESC:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_script_desc)],
            STATE_EDIT_SCRIPT_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_script_text)],
            STATE_EDIT_KEYS_GROUP:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_keys_group)],
            STATE_EDIT_KEYS_USER:     [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_keys_user)],
            STATE_REDO_STAGE_DESC:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_redo_stage_desc)],
            STATE_REDO_STAGE_SCRIPT:  [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_redo_stage_script)],
        },
        fallbacks=[
            CommandHandler("start", cmd_start),
            MessageHandler(filters.ALL, fallback),
        ],
        allow_reentry=True,
    )
    app.add_handler(conv)
 
    async def on_startup(application):
        asyncio.create_task(background_worker())
 
    app.post_init = on_startup
    logger.info("Bot started.")
    app.run_polling()
 
 
if __name__ == "__main__":
    main()

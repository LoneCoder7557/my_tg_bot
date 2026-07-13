
import asyncio
import json
import logging
import re
import os
import random
import string
import sqlite3
import tempfile
import shutil
import copy
import zipfile
import io
import hashlib
import time
import threading
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from html import escape, unescape
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, FSInputFile, LabeledPrice, BotCommandScopeDefault, BotCommandScopeChat
from aiohttp import web

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:
    Image = ImageDraw = ImageFont = None

try:
    import psycopg
except Exception:
    psycopg = None


BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = (os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or "").strip()
PATCH_VERSION = "PATCH40_FINAL_STABLE_HOTFIX"
DATA_SCHEMA_VERSION = 40
STORAGE_VERSION = "40.1"
APP_TIMEZONE_NAME = (os.getenv("APP_TIMEZONE") or "UTC").strip() or "UTC"
COLLECTION_NORMALIZATION_VERSION = "PATCH40_COLLECTION_V1"


def utc_now():
    return datetime.now(timezone.utc)


def utc_today():
    return utc_now().date()


# PATCH17C: Render Free не даёт писать в /var/data без paid Persistent Disk.
# Если DATA_DIR=/var/data задан, но папку нельзя создать, бот НЕ падает:
# основной прогресс хранится в Neon/PostgreSQL через DATABASE_URL,
# а локальные JSON/SQLite используются только как временный fallback.
REQUESTED_DATA_DIR = Path(os.getenv("DATA_DIR") or os.getenv("BOT_DATA_DIR") or os.getenv("RENDER_DATA_DIR") or ".")
if not REQUESTED_DATA_DIR.is_absolute():
    REQUESTED_DATA_DIR = (BASE_DIR / REQUESTED_DATA_DIR).resolve()
DATA_DIR_WARNING = ""
try:
    REQUESTED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR = REQUESTED_DATA_DIR
except Exception as ex:
    DATA_DIR_WARNING = f"DATA_DIR {REQUESTED_DATA_DIR} недоступен: {ex}. Использую временный fallback."
    fallback_root = Path(os.getenv("TMPDIR") or "/tmp") / "anime_battle_multiverse"
    try:
        fallback_root.mkdir(parents=True, exist_ok=True)
        DATA_DIR = fallback_root
    except Exception:
        DATA_DIR = BASE_DIR

TOKEN_FILE = str(BASE_DIR / "token.txt")
DATA_FILE = str(DATA_DIR / "anime_battle_data.json")
DB_FILE = str(DATA_DIR / "anime_battle_data.db")
CARDS_FILE = str(BASE_DIR / "cards.json")
PROMO_FILE = str(DATA_DIR / "promo_codes.json")
OWNER_FILE = str(BASE_DIR / "owner_ids.txt")
RIGHT_HAND_FILE = str(BASE_DIR / "right_hand_ids.txt")
MEDIA_DIR = BASE_DIR / "media"
MEDIA_CARDS_DIR = MEDIA_DIR / "cards"
MEDIA_PACKS_DIR = BASE_DIR / "media_packs"
LOG_FILE = str(DATA_DIR / "bot_runtime.log")
PAYMENT_RECOVERY_FILE = str(DATA_DIR / "payment_recovery_queue.json")
GENERATED_CARDS_DIR = MEDIA_DIR / "generated_cards"
GENERATED_UI_DIR = MEDIA_DIR / "ui"

# Runtime-only caches. Real media under media/cards and media_packs are never removed.
REAL_MEDIA_IDS = set()
REAL_MEDIA_BY_ID = {}
_GENERATED_CARD_ASYNC_LOCKS = {}
_GENERATED_CARD_LOCK_META = {}
_GENERATED_CARD_LOCKS_GUARD = asyncio.Lock() if False else None  # initialized lazily in the running loop
_GENERATED_CARD_THREAD_LOCKS = {}
_GENERATED_CARD_THREAD_LOCKS_GUARD = threading.Lock()



def _safe_extract_card_member(zip_obj, member):
    try:
        if member.is_dir():
            return False
        name = Path(member.filename).name
        if not name or name.startswith("."):
            return False
        if Path(name).suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}:
            return False
        MEDIA_CARDS_DIR.mkdir(parents=True, exist_ok=True)
        target = MEDIA_CARDS_DIR / name
        if target.exists() and target.stat().st_size > 0:
            return False
        with zip_obj.open(member) as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return True
    except Exception:
        return False


def ensure_media_packs_extracted():
    """
    Упрощённая загрузка медиа для GitHub: можно хранить 2 ZIP-пака в media_packs/.
    При старте бот сам распакует картинки в media/cards/.
    Поддерживаются обычные паки и вложенные ZIP-паки.
    """
    try:
        MEDIA_CARDS_DIR.mkdir(parents=True, exist_ok=True)
        existing = [p for p in MEDIA_CARDS_DIR.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}]
        if len(existing) >= 800:
            return
        if not MEDIA_PACKS_DIR.exists():
            return
        extracted = 0
        for pack in sorted(MEDIA_PACKS_DIR.glob("*.zip")):
            try:
                with zipfile.ZipFile(pack) as outer:
                    for member in outer.infolist():
                        suffix = Path(member.filename).suffix.lower()
                        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}:
                            if _safe_extract_card_member(outer, member):
                                extracted += 1
                        elif suffix == ".zip":
                            try:
                                with zipfile.ZipFile(io.BytesIO(outer.read(member))) as nested:
                                    for nested_member in nested.infolist():
                                        if _safe_extract_card_member(nested, nested_member):
                                            extracted += 1
                            except Exception:
                                pass
            except Exception:
                pass
        if extracted:
            print(f"MEDIA PACKS EXTRACTED: {extracted} card images")
    except Exception as ex:
        print(f"MEDIA PACK EXTRACT FAILED: {ex}")


def rebuild_real_media_index():
    """Build one O(n) index after media extraction; draw checks are O(1)."""
    REAL_MEDIA_IDS.clear()
    REAL_MEDIA_BY_ID.clear()
    preferred_dirs = [MEDIA_DIR / "cards_watermarked", MEDIA_CARDS_DIR, MEDIA_DIR]
    allowed = {".gif", ".mp4", ".jpg", ".jpeg", ".png", ".webp"}
    # Earlier directories have priority. Avoid recursive scans of generated caches.
    for folder in preferred_dirs:
        try:
            if not folder.exists() or not folder.is_dir():
                continue
            for item in folder.iterdir():
                if not item.is_file() or item.suffix.lower() not in allowed:
                    continue
                cid = item.stem
                if cid and cid not in REAL_MEDIA_BY_ID:
                    REAL_MEDIA_BY_ID[cid] = item
                    REAL_MEDIA_IDS.add(cid)
        except Exception as ex:
            logger.debug("Media index scan skipped for %s: %s", folder, ex) if "logger" in globals() else None
    return len(REAL_MEDIA_IDS)


ensure_media_packs_extracted()
rebuild_real_media_index()
ONLINE_QUEUE_TTL_SECONDS = 5 * 60
PAYMENT_CURRENCY = "XTR"
REAL_ART_CHANCE = max(0.0, min(1.0, float(os.getenv("ABM_REAL_ART_CHANCE", "0.78") or 0.78)))
GENERATED_CACHE_MAX_FILES = max(50, int(os.getenv("ABM_GENERATED_CACHE_MAX_FILES", "1200") or 1200))
GENERATED_CACHE_MAX_MB = max(32, int(os.getenv("ABM_GENERATED_CACHE_MAX_MB", "512") or 512))
GENERATED_CACHE_MAX_AGE_DAYS = max(1, int(os.getenv("ABM_GENERATED_CACHE_MAX_AGE_DAYS", "45") or 45))


def migrate_legacy_storage():
    """Переносит старые файлы прогресса в DATA_DIR, если включили постоянный диск."""
    if DATA_DIR == BASE_DIR:
        return
    for name in ["anime_battle_data.json", "anime_battle_data.json.bak", "anime_battle_data.db"]:
        legacy = BASE_DIR / name
        target = DATA_DIR / name
        try:
            if legacy.exists() and not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy, target)
        except Exception:
            pass


migrate_legacy_storage()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("anime_multiverse_bot")

MAX_LEVEL = 100
CARD_UNLOCK_FRAGMENTS = 100
CHOICE_TIMEOUT_SECONDS = 20
CHOICE_WARN_10_AFTER = 10
CHOICE_WARN_5_AFTER = 15
PASS_PRICE_STARS = 199
MOON_EMOJI = "🐉"
CASE_PRICES = {"event": 8, "holiday": 14, "mystic": 30}
PITY_LIMITS = {"epic": 10, "legendary": 50, "mythic": 150}
CASE_NAMES = {"event": "Ивент-кейс", "holiday": "Праздничный кейс", "mystic": "Absolute-кейс"}


# PATCH15.2: Telegram custom emoji.
# HTML custom emoji are used in messages; button custom emoji use icon_custom_emoji_id.
CUSTOM_EMOJI_IDS = {
    # Главные разделы и точные ID владельца
    "start": "5328089410963513796",
    "profile": "6012666146648495705",
    "modes": "5408935401442267103",
    "collection": "5436157754567828863",
    "rewards": "5188344996356448758",
    "rules": "5334882760735598374",
    "luffy": "6057663582705814959",
    "dragonite": "5258112758645282249",
    "pistachios": "5330236782942379682",
    "owner": "5467406098367521267",

    # Живые вкладки режимов, найденные в текущем файле
    "arena": "5454014806950429357",
    "online": "5447410659077661506",
    "deck": "5217849987160889755",
    "events": "5188497854242495901",
    "raid": "5372951839018850336",
    "battle_choice": "5449820402018688838",

    # Новые точные ID из PATCH34
    "menu": "5465226866321268133",
    "back": "5440735760208637835",
    "draw_card": "5251368156752000758",
    "event_item": "5251368156752000758",
    "universe_selected": "5260426225599405269",
    "user_name": "5363860943736424319",
    "clan": "5197388492379804229",
    "craft": "5471893218205392288",
    "shop": "5323546959061989444",
    "referral": "5240491147779907309",

    # Награды / магазин
    "free_chest": "5364112491381006601",
    "daily_reward": "5350460637182993292",
    "chests": "6046233639644042899",
    "multipass": "5251368156752000758",
    "rating": "5280735858926822987",

    # Редкости
    "origin": "5339113303522161846",
    "rare": "5339513551524481000",
    "epic": "5339146671123087992",
    "legendary": "5339082633160703625",
    "absolute": "5370659164001411305",
    "super_absolute": "5256037447627711673",
    "shop_alt": "5323546959061989444",
}
CUSTOM_EMOJI_FALLBACKS = {
    "start": "🌌",
    "profile": "👤",
    "modes": "⚔️",
    "collection": "🃏",
    "rewards": "🎁",
    "rules": "📜",
    "luffy": "🔥",
    "dragonite": "🐉",
    "pistachios": "💎",
    "owner": "👑",

    "arena": "⚔️",
    "online": "🌐",
    "deck": "🃏",
    "events": "🎪",
    "raid": "👹",
    "battle_choice": "⚙️",
    "menu": "🏠",
    "back": "⬅️",

    "free_chest": "🆓",
    "daily_reward": "🎁",
    "chests": "🧰",
    "multipass": "🎟",
    "rating": "🏆",

    "origin": "⚪",
    "rare": "🔷",
    "epic": "🟣",
    "legendary": "🟡",
    "absolute": "🔴",
    "super_absolute": "⚫",
    "draw_card": "🎴",
    "event_item": "🎴",
    "universe_selected": "🌌",
    "user_name": "👤",
    "clan": "🏰",
    "craft": "⚒️",
    "shop": "🏪",
    "shop_alt": "🛒",
    "referral": "🔗",
}

# PATCH34: безопасная проверка custom emoji.
# Принимаем точные ID пользователя и старые 19-значные ID режимов.
# Ошибочные ID на 20+ цифр больше не попадают в HTML/кнопки и не ломают сообщения.
TRUSTED_CUSTOM_EMOJI_IDS = {
    "5215377245639549895", "6012666146648495705", "5408935401442267103",
    "5469741319330996757", "5188344996356448758", "5334882760735598374",
    "6057663582705814959", "5258112758645282249", "5330236782942379682",
    "5467406098367521267", "5328089410963513796", "5440735760208637835",
    "5251368156752000758", "5436157754567828863", "5465226866321268133",
    "5260426225599405269", "5363860943736424319", "5197388492379804229",
    "5471893218205392288", "5370659164001411305", "5240491147779907309",
    "5256037447627711673", "5323546959061989444",
}


def valid_custom_emoji_id(emoji_id):
    value = str(emoji_id or "").strip()
    if value in TRUSTED_CUSTOM_EMOJI_IDS:
        return True
    return bool(re.fullmatch(r"\d{19}", value))


def ce(name):
    """HTML custom emoji with a normal emoji fallback."""
    emoji_id = CUSTOM_EMOJI_IDS.get(name)
    fallback = CUSTOM_EMOJI_FALLBACKS.get(name, "")
    if not fallback or not valid_custom_emoji_id(emoji_id):
        return fallback
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

CUSTOM_EMOJI_IDS.update({
    "season": CUSTOM_EMOJI_IDS["start"],
    "more": CUSTOM_EMOJI_IDS["rewards"],
})
CUSTOM_EMOJI_FALLBACKS.update({"season": "🌌", "more": "✨"})

CE = {key: ce(key) for key in CUSTOM_EMOJI_IDS}
PISTACHIOS_LABEL = f'{CE["pistachios"]} Фисташки'
DRAGONITE_LABEL = f'{CE["dragonite"]} Драконит'
OWNER_LABEL = f'{CE["owner"]} Владелец мультивселенной'
MULTIPASS_LABEL = f'{CE["multipass"]} MultiPass'
CURRENCY_TITLE = DRAGONITE_LABEL
PROJECT_HOOK = "Твой следующий призыв может открыть легенду. Собери любимых героев, усиливай их дубликатами и докажи, что твоя пятёрка сильнейшая во всей мультивселенной."

BOT_SHORT_DESCRIPTION = "Призывай любимых аниме-персонажей, собирай пятёрку и стань сильнейшим во всей мультивселенной."
BOT_PUBLIC_DESCRIPTION = (
    "🌌 Anime Battle Multiverse\n"
    "Твоя личная аниме-мультивселенная: призывай полноценных персонажей, собирай команду из пяти бойцов, усиливай дубликатами и побеждай в арене, PvP, событиях и рейдах.\n\n"
    "Каждые 3 часа доступен бесплатный призыв. Выбери любимый мир, открой редчайшие формы и поднимись на вершину сезона."
)


PATCH17_STORAGE_KEY = "patch17_neon_database_storage_2026_05"


# PATCH15.3: precise custom emoji icons for InlineKeyboardButton.
# Важно: custom emoji ставим только на точные главные вкладки.
# Внутренние кнопки используют разные обычные emoji, чтобы не было клонов: арена ≠ онлайн ≠ рейд.
BUTTON_EXACT_ICON_KEYS = {
    # Главное меню
    "⚔️ режимы": "modes",
    "⚔ режимы": "modes",
    "режимы": "modes",
    "⬅️ режимы": "modes",
    "🃏 коллекция": "collection",
    "коллекция": "collection",
    "⬅️ коллекция": "collection",
    "🎁 награды": "rewards",
    "награды": "rewards",
    "👤 профиль": "profile",
    "профиль": "profile",
    "⬅️ профиль": "profile",
    "📜 правила": "rules",
    "правила": "rules",
    "🔥 путь луфи": "luffy",
    "путь луфи": "luffy",
    "🌌 меню": "menu",
    "🏠 меню": "menu",
    "⬅️ меню": "menu",
    "главное меню": "menu",
    "меню": "menu",

    # Режимы
    "⚔️ арена": "arena",
    "⚔ арена": "arena",
    "арена": "arena",
    "⬅️ арены": "arena",
    "🌐 онлайн": "online",
    "онлайн": "online",
    "🃏 колоды": "deck",
    "🃏 колода": "deck",
    "колоды": "deck",
    "колода": "deck",
    "⬅️ колода": "deck",
    "🧬 открыть колоду": "deck",
    "🎪 ивенты": "events",
    "ивенты": "events",
    "⬅️ ивенты": "events",
    "👹 рейд": "raid",
    "🐉 рейд": "raid",
    "🐉 рейд-босс": "raid",
    "рейд": "raid",
    "рейд-босс": "raid",
    "⚙️ выбор боя": "battle_choice",
    "⚙ выбор боя": "battle_choice",
    "выбор боя": "battle_choice",

    # Награды
    "🆓 бесплатный сундук": "free_chest",
    "бесплатный сундук": "free_chest",
    "🎁 ежедневная награда": "daily_reward",
    "ежедневная награда": "daily_reward",
    "🧰 сундуки": "chests",
    "сундуки": "chests",
    "⬅️ сундуки": "chests",
    "⬅️ назад к сундукам": "chests",
    "🎟 мультипасс / донат": "multipass",
    "🎟 мультипасс": "multipass",
    "мультипасс / донат": "multipass",
    "мультипасс": "multipass",
    "🏆 рейтинг": "rating",
    "рейтинг": "rating",

    # Редкости
    "⚪ origin": "origin",
    "origin": "origin",
    "⚪ обычный": "origin",
    "обычный": "origin",
    "🔷 rare": "rare",
    "rare": "rare",
    "🔵 редкий": "rare",
    "редкий": "rare",
    "🟣 epic": "epic",
    "epic": "epic",
    "🟣 эпический": "epic",
    "эпический": "epic",
    "🟡 legendary": "legendary",
    "legendary": "legendary",
    "🟡 легендарный": "legendary",
    "легендарный": "legendary",
    "🔴 absolute": "absolute",
    "absolute": "absolute",
    "🔴 мифический": "absolute",
    "мифический": "absolute",

    # Владелец / админка
    "👑 владелец": "owner",
    "владелец": "owner",
    "🛠 админ-панель": "owner",
    "админ-панель": "owner",
}

# PATCH28: новые точные живые emoji по просьбе владельца.  ReplyKeyboardButton
# не поддерживает icon_custom_emoji_id, поэтому там остаются обычные fallback emoji.
BUTTON_EXACT_ICON_KEYS.update({
    "получить карту": "draw_card",
    "🎴 получить карту": "draw_card",
    "мои карты": "collection",
    "🃏 мои карты": "collection",
    "карты": "collection",
    "клан": "clan",
    "🏰 клан": "clan",
    "⬅️ клан": "back",
    "крафт": "craft",
    "⚒️ крафт": "craft",
    "⚒ крафт": "craft",
    "⬅️ крафт": "back",
    "кейсы": "chests",
    "🧰 кейсы": "chests",
    "⬅️ кейсы": "back",
    "магазин": "shop",
    "🏪 магазин": "shop",
    "🛒 магазин": "shop",
    "⬅️ магазин": "back",
    "multipass": "multipass",
    "🎟 multipass": "multipass",
    "multi pass": "multipass",
    "🎟 multi pass": "multipass",
    "multiPass".casefold(): "multipass",
    "⬅️ назад": "back",
    "⬅ назад": "back",
    "◀️ назад": "back",
    "◀ назад": "back",
    "назад": "back",
    "⬅️ меню": "menu",
    "⬅ меню": "menu",
    "◀️ меню": "menu",
    "◀ меню": "menu",
    "в меню": "menu",
    "◀ в меню": "menu",
    "🏠 меню": "menu",
    "меню": "menu",
    "главное меню": "menu",
    "админ-панель": "owner",
    "👑 админ-панель": "owner",
    "рефералка": "referral",
    "🔗 рефералка": "referral",
    "реферальная программа": "referral",
    "реферальная ссылка": "referral",
    "🔗 реферальная ссылка": "referral",
    "топ по фисташкам": "pistachios",
    "💎 топ по фисташкам": "pistachios",
    "⚫ super absolute": "super_absolute",
    "super absolute": "super_absolute",
    "🔴 absolute": "absolute",
})

LEADING_BUTTON_EMOJI_PREFIXES = (
    "🌌", "⚔️", "⚔", "🃏", "🎁", "👤", "📜", "🔥", "🐉", "💎", "👑",
    "🆓", "🧰", "🎟", "🏆", "⭐", "🎴", "🏷", "📦", "🧠", "🔁", "⚡",
    "🔔", "🔕", "👥", "✏️", "✏", "📊", "⬅️", "⬅", "➡️", "➡", "🏠",
    "💪", "⬆️", "⚫", "🏰", "🏪", "🛒", "⬆", "🔤", "📚", "⚪", "🔷", "🔵", "🟣", "🟡", "🔴",
    "✅", "🎲", "🛡️", "🛡", "🤖", "🌐", "🎪", "👹", "⚙️", "⚙", "❌",
    "🔗", "🎯", "💳", "☠️", "☠", "🧊", "♨️", "♨", "🆔", "🗑", "🔄",
)


def _button_icon_key(text: str) -> str | None:
    t = " ".join(str(text or "").casefold().split())
    if any(word in t for word in ("кейсы", "кейс", "сундуки", "сундук")):
        return None
    if t.startswith("аниме:") or t.startswith("мультивселенная"):
        return None
    if "назад" in t:
        return "back"
    dynamic_prefixes = (
        (("призвать", "призыв", "получить карту"), "draw_card"),
        (("сезон",), "season"),
        (("ещё", "еще"), "more"),
        (("события", "ивенты"), "events"),
        (("битвы", "играть", "режимы"), "modes"),
    )
    plain = _strip_leading_button_emoji(str(text or "")).casefold().strip()
    for prefixes, key in dynamic_prefixes:
        if any(plain.startswith(prefix) for prefix in prefixes):
            return key
    return BUTTON_EXACT_ICON_KEYS.get(t) or BUTTON_EXACT_ICON_KEYS.get(plain)


def _strip_leading_button_emoji(text: str) -> str:
    t = str(text or "")
    for prefix in sorted(LEADING_BUTTON_EMOJI_PREFIXES, key=len, reverse=True):
        if t.startswith(prefix):
            return t[len(prefix):].lstrip()
    return t

CUSTOM_BUTTON_EMOJI_ENABLED = str(os.getenv("CUSTOM_BUTTON_EMOJI", "1")).strip().lower() in {"1", "true", "yes", "on"}


def button(*args, **kwargs) -> InlineKeyboardButton:
    """Безопасная inline-кнопка.

    Для основных разделов по умолчанию используются проверенные Telegram custom emoji.
    На клиентах без поддержки остаётся понятный текст, а функцию можно отключить
    переменной CUSTOM_BUTTON_EMOJI=0.
    """
    text = kwargs.get("text")
    args = list(args)
    if text is None and args:
        text = args[0]
    key = _button_icon_key(text) if CUSTOM_BUTTON_EMOJI_ENABLED and isinstance(text, str) else None
    if key and not kwargs.get("icon_custom_emoji_id"):
        icon_id = CUSTOM_EMOJI_IDS.get(key)
        if valid_custom_emoji_id(icon_id):
            kwargs["icon_custom_emoji_id"] = icon_id
            clean = _strip_leading_button_emoji(text)
            if "text" in kwargs:
                kwargs["text"] = clean
            elif args:
                args[0] = clean
    callback_data = kwargs.get("callback_data")
    if callback_data is not None and len(str(callback_data).encode("utf-8")) > 64:
        raise ValueError(f"callback_data exceeds Telegram 64-byte limit: {len(str(callback_data).encode('utf-8'))}")
    return InlineKeyboardButton(*args, **kwargs)

LUFFY_PATH_CARDS = [
    "luffy_day01_start",
    "luffy_day02_promise",
    "luffy_day03_gear2",
    "luffy_day04_gear3",
    "luffy_day05_gear4_boundman",
    "luffy_day06_gear4_snake",
    "luffy_day07_advanced_haki",
    "luffy_day08_rooftop",
    "luffy_day09_nika_awakened",
    "luffy_day10_gear5_sun_god",
]

SECTION_HINTS = {
    "modes": (f"{CE['modes']} Режимы", "Выбери поле боя: арена, онлайн, рейд или события. Здесь решают колода, уровень и тактика."),
    "collection": (f"{CE['collection']} Коллекция", "Твои персонажи, формы, уровни и сила. Открой карту, чтобы увидеть роль, плюс, минус и потенциал."),
    "shop": (f"{CE['rewards']} Награды", "Фисташки — основной прогресс. Драконит — редкая премиум-валюта для кейсов и особых покупок."),
    "profile": (f"{CE['profile']} Профиль", "Твой ранг, ресурсы, карты, друзья и настройки."),
    "rules": (f"{CE['rules']} Правила", "Карта мультивселенной: карты, валюты, режимы, споры, запреты и источники персонажей."),
    "luffy": (f"{CE['luffy']} Путь Луфи", "10 дней — 10 форм Луфи. Забирай путь постепенно, без спама."),
    "newbie": (f"{CE['luffy']} Путь Луфи", "10 дней — 10 форм Луфи. Забирай путь постепенно, без спама."),
}

def ui_box(title, body):
    # title is a trusted internal string and can include <tg-emoji>; body is escaped.
    return f"╭─ <b>{title}</b>\n│ {e(body)}\n╰────────────"

STAR_PACKS = {
    "epic_boost": {
        "title": "Эпический контракт",
        "price": 99,
        "desc": "Гарантированная Epic-карта из выбранной вселенной, 4000 💎 и 1 🐉. Быстрый старт без раздачи дорогой валюты.",
        "rarity": "Эпический",
        "fistiks": 4000,
        "moon_coins": 1,
        "badge": "EPIC_BOOSTER",
    },
    "legendary_rank": {
        "title": "Легендарный контракт",
        "price": 249,
        "desc": "Гарантированная Legendary-карта из выбранной вселенной, 9000 💎 и 3 🐉. Для игроков, которые хотят ускорить сбор колоды.",
        "rarity": "Легендарный",
        "fistiks": 9000,
        "moon_coins": 3,
        "badge": "LEGEND_RANK",
    },
    "super_absolute_ticket": {
        "title": "Super Absolute пропуск",
        "price": 799,
        "desc": "Редкий платный билет с шансом на закрытых сверх-Absolute персонажей: Анос, Фезарин, Ками Тэнчи, Зено, Римуру и другие сущности верхнего уровня.",
        "rarity": "Мифический",
        "fistiks": 20000,
        "moon_coins": 6,
        "badge": "MYTHIC_TICKET",
        "allow_super_absolute": True,
    },
}

REF_MILESTONES = {
    1: {"fistiks": 500, "pass_xp": 120, "moon_coins": 0, "title": "первый союзник"},
    3: {"fistiks": 1200, "pass_xp": 250, "moon_coins": 0, "title": "малый отряд"},
    5: {"fistiks": 2500, "pass_xp": 450, "moon_coins": 1, "title": "команда мультивселенной", "badge": "REF_5"},
    10: {"fistiks": 6000, "pass_xp": 900, "moon_coins": 2, "title": "лидер союза", "badge": "REF_10"},
    25: {"fistiks": 18000, "pass_xp": 1800, "moon_coins": 5, "title": "магнит мультивселенной", "badge": "REF_25"},
}

RAID_HIT_COOLDOWN_MINUTES = 300
RAID_HIT_LIMIT_PER_WINDOW = 3
RAID_DURATION_DAYS = 7

DAILY_EVENT_POOL = [
    {"name": "День шиноби", "desc": "Сыграй бой или открой сундук: сегодня энергия скрытых деревень усиливает прогресс.", "coins": 0, "pass_xp": 140},
    {"name": "Проклятая волна", "desc": "Проклятая энергия нестабильна: ежедневная активность даёт усиленную награду.", "coins": 1, "pass_xp": 120},
    {"name": "Пиратский прилив", "desc": "Команды с духом приключений получают бонус к сезонному прогрессу.", "coins": 0, "pass_xp": 180},
    {"name": "Духовный разлом", "desc": "Открыт разлом духовной энергии. Забери награду дня до смены события.", "coins": 1, "pass_xp": 100},
    {"name": "Турнир измерений", "desc": "Мультивселенная ждёт активности: зайди, забери бонус и готовь колоду.", "coins": 0, "pass_xp": 200},
]

RAID_BOSSES = [
    {
        "id": "raid_shibai_otsutsuki",
        "name": "Шибай Оцуцуки — Бог эволюции",
        "hp": 650_000_000,
        "desc": "Дневной абсолютный босс. Его тело почти вышло за границы обычной силы, поэтому один герой не решает бой — нужен общий урон всех игроков.",
        "protection": "Снижает урон от богов разрушения, админских форм, стирания реальности, времени, судьбы и одиночных Absolute-комбо. Лучше бить полной командой.",
    },
    {
        "id": "raid_soul_king_shadow",
        "name": "Тень Короля Душ",
        "hp": 720_000_000,
        "desc": "Мифическая тень, удерживающая несколько измерений. Она ломает прямые хакс-атаки и заставляет игроков работать рейтингом урона.",
        "protection": "Защита от будущего, душ, измерений, абсолютного давления и одиночного ваншота.",
    },
    {
        "id": "raid_grand_priest_echo",
        "name": "Эхо Великого Жреца",
        "hp": 800_000_000,
        "desc": "Сущность, которая принимает урон от богов разрушения и возвращает часть давления обратно в команду.",
        "protection": "Режет урон от божественных форм, ультра-инстинкта, разрушения и админских ударов.",
    },
    {
        "id": "raid_multiverse_core",
        "name": "Ядро Мультивселенной",
        "hp": 900_000_000,
        "desc": "Живой центр разлома. Чем дольше стоит, тем важнее вклад каждого игрока.",
        "protection": "Поглощает часть урона от космических сущностей, концептов и персонажей уровня творца.",
    },
    {
        "id": "raid_eren_colossal_founder",
        "name": "Эрен Йегер — Гигантский Титан",
        "hp": 520_000_000,
        "desc": "Титанический рейд-босс с огромным запасом HP. Его нельзя продавить одной красивой картой — нужна серия командных атак.",
        "protection": "Снижает урон от одиночных ульт, но хуже держит стабильный урон полной колоды.",
    },
    {
        "id": "raid_sukuna_king",
        "name": "Сукуна — Король Проклятий",
        "hp": 610_000_000,
        "desc": "Босс недели с проклятой защитой. Ошибки команды он превращает в ответный разрез.",
        "protection": "Режет проклятую энергию, домены, пространственные разрезы и слишком прямые комбо.",
    },
    {
        "id": "raid_madara_six_paths",
        "name": "Мадара Учиха — Мудрец Шести Путей",
        "hp": 580_000_000,
        "desc": "Шиноби-босс с контролем поля, клонами и давлением Риннегана.",
        "protection": "Снижает урон от чакры, гендзюцу, пространственных прыжков и одиночных шиноби-комбо.",
    },
]

ARENAS = {
    "ruins": ("🏛", "Руины мультивселенной", "ломаная арена с укрытиями, где важны скорость и контроль"),
    "city": ("🌃", "Ночной мегаполис", "много стен, высоток и внезапных углов для атак"),
    "void": ("🌌", "Пустота измерений", "чистое поле, где сильнее раскрываются хакс и дальние техники"),
    "forest": ("🌲", "Проклятый лес", "сложная видимость, ловушки и внезапные нападения"),
    "desert": ("🏜", "Пустынный каньон", "открытая зона для мощных атак и контроля пространства"),
    "temple": ("⛩", "Разрушенный храм", "компактное поле, где ближники быстрее входят в бой"),
}

ARENA_EFFECTS = {
    "ruins": ("➕ тактики, ловушки, мобильность", "➖ прямой рывок без контроля"),
    "city": ("➕ ассасины, ближники, прыжки по укрытиям", "➖ гигантские формы и дальний спам"),
    "void": ("➕ хакс, дальние техники, пространственные способности", "➖ бойцы без дальности и защиты"),
    "forest": ("➕ скрытность, ловушки, сенсоры", "➖ бойцы, которым нужна чистая видимость"),
    "desert": ("➕ масштабные атаки, контроль зоны, песок/земля", "➖ скрытность и слабая мобильность"),
    "temple": ("➕ мечники, рукопашники, быстрый контакт", "➖ дальники, которым нужна дистанция"),
}

BATTLE_EVENTS = [
    ("⚡", "Резкий первый контакт", "первые секунды решает скорость и реакция"),
    ("🧠", "Тактический перелом", "IQ и командная синергия становятся важнее голой силы"),
    ("💥", "Окно ульты", "одна сторона получает шанс на решающую технику"),
    ("🛡", "Срыв burst-атаки", "защита и живучесть спасают ключевого бойца"),
    ("🌀", "Хаос поля", "арена ломает прямой план и усиливает нестандартных бойцов"),
    ("🔻", "Цена формы", "персонажи с жёсткими минусами начинают платить ресурсом"),
]

SUMMON_WEIGHTS = {
    # PATCH40.1: любой обычный сундук/призыв использует одну и ту же прозрачную
    # таблицу. Absolute = ровно 2.5%. Super Absolute в этот пул не входит.
    "Обычный": 500,      # 50.0%
    "Редкий": 300,       # 30.0%
    "Эпический": 125,    # 12.5%
    "Легендарный": 50,   # 5.0%
    "Мифический": 25,    # 2.5%
}

RARITY_WEIGHTS = dict(SUMMON_WEIGHTS)
FREE_PACK_WEIGHTS = dict(SUMMON_WEIGHTS)

BATTLE_PLAYER_WEIGHTS = {
    "Обычный": 720,
    "Редкий": 210,
    "Эпический": 55,
    "Легендарный": 12,
    "Мифический": 3,
}

OWNER_BATTLE_WEIGHTS = {
    "Обычный": 120,
    "Редкий": 220,
    "Эпический": 260,
    "Легендарный": 240,
    "Мифический": 160,
}

RIGHT_HAND_BATTLE_WEIGHTS = {
    "Обычный": 220,
    "Редкий": 280,
    "Эпический": 260,
    "Легендарный": 160,
    "Мифический": 80,
}

BOT_BATTLE_WEIGHTS_NEWBIE = {
    "Обычный": 880,
    "Редкий": 115,
    "Эпический": 5,
    "Легендарный": 0,
    "Мифический": 0,
}

BOT_BATTLE_WEIGHTS_NORMAL = {
    "Обычный": 760,
    "Редкий": 190,
    "Эпический": 40,
    "Легендарный": 8,
    "Мифический": 2,
}

RARE_PACK_WEIGHTS = {
    "Обычный": 700,
    "Редкий": 230,
    "Эпический": 60,
    "Легендарный": 8,
    "Мифический": 2,
}

CASE_WEIGHTS = {
    "light": dict(SUMMON_WEIGHTS),
    "event": dict(SUMMON_WEIGHTS),
    "holiday": dict(SUMMON_WEIGHTS),
    "mystic": dict(SUMMON_WEIGHTS),
}

RARITY_BONUS = {
    "Обычный": 0,
    "Редкий": 8,
    "Эпический": 20,
    "Легендарный": 45,
    "Мифический": 90,
}

DUPLICATE_SHARDS = {
    "Обычный": 8,
    "Редкий": 20,
    "Эпический": 45,
    "Легендарный": 120,
    "Мифический": 300,
}

UPGRADE_FRAGMENT_COSTS = {
    "Обычный": 20,
    "Редкий": 40,
    "Эпический": 60,
    "Легендарный": 80,
    "Мифический": 100,
}

RARITY_EMOJI = {
    "Обычный": "⚪",
    "Редкий": "🔷",
    "Эпический": "🟣",
    "Легендарный": "🟡",
    "Мифический": "🔴",
}

RARITY_DISPLAY = {
    "Обычный": f"{CE['origin']} Origin",
    "Редкий": f"{CE['rare']} Rare",
    "Эпический": f"{CE['epic']} Epic",
    "Легендарный": f"{CE['legendary']} Legendary",
    "Мифический": f"{CE['absolute']} Absolute",
}

BASE_STATS = {
    "Обычный": 45,
    "Редкий": 60,
    "Эпический": 75,
    "Легендарный": 92,
    "Мифический": 120,
}

SHOP_PACKS = {
    "basic": {
        "name": "Обычный сундук",
        "base_cost": 400,
        "count": 3,
        "weights": dict(SUMMON_WEIGHTS),
        "description": "Архивная награда. Отдельно больше не продаётся.",
    },
    "rare": {
        "name": "Усиленный сундук",
        "base_cost": 1500,
        "count": 5,
        "weights": dict(SUMMON_WEIGHTS),
        "description": "Архивная награда. Отдельно больше не продаётся.",
    },
    "royal": {
        "name": "Королевский сундук",
        "base_cost": 3000,
        "count": 6,
        "weights": dict(SUMMON_WEIGHTS),
        "description": "Архивная награда. Отдельно больше не продаётся.",
    },
}

BADGE_SHOP = {
    "killer": {"title": "Убийца", "emoji": "🗡", "cost": 2500, "desc": "боевой знак для агрессивных игроков"},
    "event_hunter": {"title": "Охотник ивентов", "emoji": "⚡", "cost": 3500, "desc": "знак активного участника событий"},
    "tester": {"title": "Тестер", "emoji": "🧪", "cost": 1800, "desc": "знак раннего игрока и проверяющего"},
}

BADGE_TITLES = {
    "DEV": "👑 Создатель",
    "ROMA_OWNER": "💠 Владелец мультивселенной",
    "IT_ARCHITECT": "🧠 IT-Создатель",
    "ABSOLUTE_MAX": "♾ Абсолютный максимум",
    "RIGHT_HAND": "🤝 Правая рука",
    "KILLER": "🗡 Убийца",
    "EVENT_HUNTER": "⚡ Охотник ивентов",
    "PREMIUM": "👑 Премиум",
    "TESTER": "🧪 Тестер",
    "EPIC_BOOSTER": "🟣 Эпический старт",
    "LEGEND_RANK": "🟡 Легендарный ранг",
    "MYTHIC_TICKET": "🔴 Мифический билет",
    "REF_5": "👥 Командир друзей",
    "REF_10": "🌐 Лидер союза",
    "REF_25": "♾ Магнит мультивселенной",
}

CRAFT_COSTS = {
    "Обычный": 100,
    "Редкий": 260,
    "Эпический": 700,
    "Легендарный": 1800,
    "Мифический": 4500,
}

RARITY_CODES = {
    "common": "Обычный",
    "rare": "Редкий",
    "epic": "Эпический",
    "legendary": "Легендарный",
    "mythic": "Мифический",
}

BUFFS = [
    {"name": "Абсолютный разгон", "text": "мощный старт: сила и скорость заметно выше", "delta": {"power": 28, "speed": 22}},
    {"name": "Командный резонанс", "text": "союзники лучше закрывают слабые стороны друг друга", "delta": {"team": 34, "iq": 10}},
    {"name": "Антихакс-щит", "text": "сильно режет эффект контроля, иллюзий и проклятий", "delta": {"hax": 32, "durability": 12}},
    {"name": "Ультимативный фокус", "text": "главная техника попадает точнее и опаснее", "delta": {"hax": 22, "power": 18}},
    {"name": "Железное тело", "text": "переживает первый смертельный burst", "delta": {"durability": 36}},
    {"name": "Первый ход", "text": "почти всегда раньше входит в бой", "delta": {"speed": 34, "iq": 8}},
    {"name": "Двойной темп", "text": "после первого удачного хода получает второе окно", "delta": {"speed": 20, "team": 18}},
]

DEBUFFS = [
    {"name": "Оглушение после ульты", "text": "после главной техники персонаж глохнет на 10 секунд и теряет следующий активный ход", "delta": {"speed": -34, "team": -22}},
    {"name": "Лимит формы", "text": "пиковая форма держится коротко; после рывка сила резко проседает", "delta": {"power": -30, "durability": -24}},
    {"name": "Перегрев", "text": "после серии сильных атак тело перегревается, темп падает почти до нуля", "delta": {"speed": -36, "power": -18}},
    {"name": "Заморозка тела", "text": "после активного окна персонаж застывает на месте и становится лёгкой целью", "delta": {"speed": -32, "iq": -16, "team": -16}},
    {"name": "Цена техники", "text": "главная способность сжигает ресурс: защита и выносливость резко падают", "delta": {"hax": -26, "durability": -24}},
    {"name": "Сломанная синергия", "text": "персонаж ломает командный план и плохо слушает союзников", "delta": {"team": -40}},
    {"name": "Медленный старт", "text": "первые секунды реагирует слишком поздно и отдаёт инициативу", "delta": {"speed": -38}},
    {"name": "Откат после серии", "text": "после комбо защита раскрывается на 5 секунд", "delta": {"durability": -34}},
    {"name": "Срыв концентрации", "text": "при давлении теряет контроль техники и ошибается в тайминге", "delta": {"iq": -28, "hax": -18}},
    {"name": "Самооткат", "text": "слишком сильный плюс активен недолго, затем персонаж сам себя выключает", "delta": {"power": -22, "speed": -22, "team": -14}},
]


ARTIFACTS = [
    {"id": "rikudo_seal", "name": "Печать Рикудо", "anime": "Naruto", "text": "стабилизирует чакру, защиту и давление формы", "delta": {"durability": 16, "hax": 12}},
    {"id": "hogyoku_shard", "name": "Осколок Хогёку", "anime": "Bleach", "text": "помогает пережить критический момент и адаптироваться", "delta": {"durability": 14, "hax": 14}},
    {"id": "zanpakuto_blade", "name": "Клинок Занпакто", "anime": "Bleach", "text": "режет защиту и усиливает дуэльный обмен", "delta": {"power": 16, "speed": 8}},
    {"id": "titan_heart", "name": "Сердце Титана", "anime": "Attack on Titan", "text": "повышает живучесть под тяжёлым уроном", "delta": {"durability": 24}},
    {"id": "anti_magic_grimoire", "name": "Гримуар Антимагии", "anime": "Black Clover", "text": "ломает часть магических и проклятых техник", "delta": {"hax": 18, "iq": 8}},
    {"id": "nika_shard", "name": "Осколок Ники", "anime": "One Piece", "text": "поднимает волю, когда команда проигрывает по очкам", "delta": {"power": 16, "team": 16}},
    {"id": "vongola_ring", "name": "Кольцо Вонголы", "anime": "Katekyo Hitman Reborn", "text": "усиливает командную волю и контроль темпа", "delta": {"team": 18, "iq": 10}},
    {"id": "capsule_corp_capsule", "name": "Капсула Capsule Corp", "anime": "Dragon Ball", "text": "быстро меняет темп и восстанавливает позицию команды", "delta": {"team": 14, "speed": 10}},
    {"id": "nen_vow", "name": "Нэн-обет", "anime": "Hunter x Hunter", "text": "опасный риск: сильнее удар, но дороже ошибка", "delta": {"power": 18, "iq": 8}},
    {"id": "philosopher_stone", "name": "Камень философа", "anime": "Fullmetal Alchemist", "text": "даёт запас энергии на поздний раунд", "delta": {"durability": 10, "hax": 14}},
]

# PATCH16: расширенная коллекция артефактов. Те же редкости, что у карт.
for _a in ARTIFACTS:
    _a.setdefault("rarity", "Эпический")
    _a.setdefault("id", re.sub(r"[^a-z0-9_]+", "_", _a["name"].lower().replace(" ", "_"))[:48])

EXTRA_ARTIFACTS = [
    ("potara_earrings", "Серьги Потара", "Dragon Ball", "Легендарный", "слияние темпа и командной синергии", {"team": 22, "power": 12}),
    ("z_sword", "Z-меч", "Dragon Ball", "Эпический", "тяжёлый клинок для силового давления", {"power": 20, "durability": 8}),
    ("capsule_senzu", "Сензу Capsule Corp", "Dragon Ball", "Редкий", "быстро возвращает бойца в раунд", {"durability": 14, "team": 8}),
    ("samihada", "Самехада", "Naruto", "Легендарный", "поглощает энергию и ломает затяжные техники", {"hax": 20, "durability": 12}),
    ("rinnegan_core", "Око Риннегана", "Naruto", "Легендарный", "усиливает контроль пространства и тактику", {"hax": 24, "iq": 12}),
    ("kusanagi_blade", "Кусанаги", "Naruto", "Эпический", "быстрый клинок для убийственного обмена", {"power": 14, "speed": 14}),
    ("hogyoku_core", "Хогёку", "Bleach", "Мифический", "адаптация к смертельному окну боя", {"hax": 30, "durability": 20}),
    ("zangetsu_fragment", "Осколок Зангецу", "Bleach", "Легендарный", "усиливает решающий рывок", {"power": 22, "speed": 12}),
    ("almighty_scroll", "Свиток Всемогущего", "Bleach", "Мифический", "читает часть вариантов боя", {"iq": 26, "hax": 24}),
    ("nika_drum", "Барабан Ники", "One Piece", "Мифический", "поднимает волю команды в переломе", {"team": 28, "power": 18}),
    ("ope_ope_core", "Сфера Ope Ope", "One Piece", "Легендарный", "ломает позицию врага через пространство", {"hax": 22, "iq": 12}),
    ("enma_blade", "Энма", "One Piece", "Легендарный", "выжимает хаковое давление из владельца", {"power": 20, "hax": 14}),
    ("cursed_finger", "Палец Сукуны", "Jujutsu Kaisen", "Легендарный", "проклятая энергия с большим риском", {"power": 20, "hax": 12}),
    ("prison_realm", "Тюремная сфера", "Jujutsu Kaisen", "Мифический", "контроль цели и срыв плана", {"hax": 28, "iq": 12}),
    ("inverted_spear", "Обратное копьё небес", "Jujutsu Kaisen", "Легендарный", "ломает техники и барьеры", {"hax": 22, "speed": 10}),
    ("anti_magic_sword", "Меч Антимагии", "Black Clover", "Легендарный", "гасит магические усиления", {"power": 18, "hax": 16}),
    ("five_leaf_grimoire", "Пятилистный гримуар", "Black Clover", "Мифический", "усиливает антимагию и риск", {"hax": 26, "power": 18}),
    ("founding_titan_spine", "Позвоночник Прародителя", "Attack on Titan", "Легендарный", "даёт давление масштаба армии", {"team": 24, "durability": 14}),
    ("colossal_core", "Ядро Колоссального Титана", "Attack on Titan", "Эпический", "разгоняет взрывную мощь", {"power": 20, "durability": 10}),
    ("death_note_page", "Страница Тетради смерти", "Death Note", "Мифический", "сверхопасная тактика против одиночной цели", {"iq": 30, "hax": 16}),
    ("geass_eye", "Гиас-око", "Code Geass", "Легендарный", "навязывает решение в критический момент", {"iq": 22, "hax": 16}),
    ("berserker_armor", "Броня Берсерка", "Berserk", "Легендарный", "поднимает силу ценой контроля", {"power": 26, "durability": 12}),
    ("dragon_slayer_sword", "Убийца драконов", "Berserk", "Легендарный", "давит грубой массой и волей", {"power": 30}),
    ("philosopher_stone_full", "Полный Камень философа", "Fullmetal Alchemist", "Легендарный", "запас энергии для позднего раунда", {"hax": 18, "durability": 18}),
    ("pokeball_master", "Мастербол", "Покемон", "Легендарный", "редкий контроль странных существ", {"hax": 18, "team": 16}),
    ("arceus_plate", "Плита Арсеуса", "Покемон", "Мифический", "меняет тип давления под бой", {"hax": 24, "team": 18}),
    ("moon_prism", "Лунная призма", "Sailor Moon", "Легендарный", "светлая защита и поддержка", {"team": 22, "durability": 12}),
    ("gurren_drill", "Сверло Гуррен-Лаганна", "Gurren Lagann", "Мифический", "пробивает невозможное через волю", {"power": 28, "team": 18}),
    ("laplace_factor", "Фактор Лапласа", "Tensei Slime", "Мифический", "ускоряет расчёт и адаптацию", {"iq": 26, "hax": 20}),
    ("azathoth_seed", "Семя Азатота", "Tensei Slime", "Мифический", "поглощает часть хаоса боя", {"hax": 30, "durability": 14}),
    ("shadow_monarch_core", "Ядро Теневого Монарха", "Solo Leveling", "Мифический", "призывает давление армии теней", {"team": 26, "power": 18}),
    ("stand_arrow", "Стрела стенда", "JoJo", "Легендарный", "открывает скрытое окно способности", {"hax": 22, "speed": 10}),
    ("requiem_arrow", "Стрела Requiem", "JoJo", "Мифический", "ломает обычную причинность боя", {"hax": 32, "iq": 14}),
    ("necronomicon_page", "Страница гримуара бездны", "Magi", "Эпический", "нестабильная магическая сила", {"hax": 16, "power": 10}),
    ("excalibur_fate", "Экскалибур", "Fate", "Легендарный", "чистый лучевой финиш", {"power": 24, "team": 8}),
    ("ea_fragment", "Осколок Эа", "Fate", "Мифический", "давление пространства и разлома", {"power": 24, "hax": 22}),
    ("madoka_gem", "Камень надежды Мадоки", "Madoka Magica", "Мифический", "переписывает цену отчаяния", {"team": 24, "hax": 22}),
    ("mob_meter", "Счётчик 100%", "Mob Psycho 100", "Эпический", "рывок силы при перегрузе эмоций", {"power": 18, "hax": 10}),
    ("hellsing_casull", "Пистолет Касулл", "Hellsing", "Эпический", "пробивает регенерацию", {"power": 18, "speed": 8}),
    ("vongola_sky_ring", "Кольцо Вонголы Неба", "Katekyo Hitman Reborn", "Легендарный", "собирает командный ритм", {"team": 26, "iq": 10}),
]
for _id, _name, _anime, _rarity, _text, _delta in EXTRA_ARTIFACTS:
    ARTIFACTS.append({"id": _id, "name": _name, "anime": _anime, "rarity": _rarity, "text": _text, "delta": _delta})

# Доводим пул до 100 предметов без мусорных названий: варианты уже существующих реликвий.
_ARTIFACT_VARIANTS = [
    ("seal", "Печать", "стабилизирует технику", {"hax": 8, "durability": 8}),
    ("blade", "Клинок", "усиливает решающий обмен", {"power": 10, "speed": 6}),
    ("core", "Ядро", "даёт запас энергии", {"durability": 10, "team": 4}),
    ("scroll", "Свиток", "повышает расчёт боя", {"iq": 10, "hax": 5}),
    ("ring", "Кольцо", "усиливает синергию команды", {"team": 12}),
]
_ARTIFACT_WORLDS = [
    "Шиноби", "Пиратов", "Саянов", "Квинси", "Проклятий", "Титанов", "Демонов", "Алхимиков", "Покемонов", "Измерений",
    "Охотников", "Героев", "Стендов", "Магов", "Теней", "Лунного света", "Антимагии", "Бездны", "Рейда", "Мультивселенной",
]
while len(ARTIFACTS) < 100:
    idx = len(ARTIFACTS) + 1
    kind = _ARTIFACT_VARIANTS[idx % len(_ARTIFACT_VARIANTS)]
    world = _ARTIFACT_WORLDS[idx % len(_ARTIFACT_WORLDS)]
    rarity = ["Обычный", "Редкий", "Эпический", "Легендарный", "Мифический"][idx % 5]
    ARTIFACTS.append({
        "id": f"relic_{idx:03d}_{kind[0]}",
        "name": f"{kind[1]} {world}",
        "anime": "Anime Battle Multiverse",
        "rarity": rarity,
        "text": kind[2],
        "delta": dict(kind[3]),
    })

LEGACY_ARTIFACT_ID_ALIASES = {
    # Old Cyrillic slugging collapsed eight different names to "___". The
    # previous dictionary exposed that key as «Камень философа», so preserving
    # the accumulated count under that canonical item is the least surprising
    # and non-destructive migration.
    "___": "philosopher_stone",
    "__capsule_corp": "capsule_corp_capsule",
    "_": "nen_vow",
}
ARTIFACT_BY_ID = {a["id"]: a for a in ARTIFACTS}
if len(ARTIFACT_BY_ID) != len(ARTIFACTS):
    raise RuntimeError("Artifact ids must be unique")


def normalize_artifact_inventory(player):
    inv = player.setdefault("artifacts", {})
    changed = False
    equipped = str(player.get("equipped_artifact", "") or "")
    for legacy_id, canonical_id in LEGACY_ARTIFACT_ID_ALIASES.items():
        old = inv.pop(legacy_id, None)
        if not isinstance(old, dict):
            continue
        target = inv.setdefault(canonical_id, {"count": 0, "level": 1})
        target["count"] = max(0, int(target.get("count", 0) or 0)) + max(0, int(old.get("count", 0) or 0))
        target["level"] = max(1, int(target.get("level", 1) or 1), int(old.get("level", 1) or 1))
        artifact = ARTIFACT_BY_ID[canonical_id]
        target["rarity"] = artifact.get("rarity", "Обычный")
        target["name"] = artifact.get("name", canonical_id)
        if equipped == legacy_id:
            player["equipped_artifact"] = canonical_id
            equipped = canonical_id
        changed = True
    if equipped and equipped not in ARTIFACT_BY_ID:
        player["equipped_artifact"] = ""
        changed = True
    if player.get("artifact_inventory_version") != "PATCH40_UNIQUE_IDS":
        player["artifact_inventory_version"] = "PATCH40_UNIQUE_IDS"
        changed = True
    return changed


active_battles = {}
active_pvp = {}
manual_team_drafts = {}
choice_timers = {}
# items: {"uid": str, "joined_at": iso}; legacy str items are also accepted.
online_queue = []


def e(text):
    return escape(str(text), quote=False)


def record_user_action(user, action, player=None):
    """Короткий журнал действий для админки.

    PATCH32 SPEED: не сохраняем DATA в Neon после каждой кнопки. Держим
    последние действия в памяти и сбрасываем на диск/Neon не чаще заданного окна.
    """
    if not user:
        return
    try:
        p = player if player is not None else get_user_data(user)
        now = utc_now()
        item = {
            "at": now.isoformat(timespec="seconds"),
            "action": str(action)[:120],
        }
        actions = p.setdefault("last_actions", [])
        appended = False
        if not actions or actions[-1].get("action") != item["action"]:
            actions.append(item)
            appended = True
        if len(actions) > 30:
            del actions[:-30]
            appended = True
        if appended:
            last_saved = _parse_iso_datetime(p.get("last_actions_saved_at", ""))
            if not last_saved or (now - last_saved).total_seconds() >= USER_ACTION_SAVE_SECONDS:
                p["last_actions_saved_at"] = now.isoformat(timespec="seconds")
                mark_data_dirty("user_action_throttled")
    except Exception as ex:
        logger.debug("Cannot record user action: %s", ex)


def _clone_default(default):
    return copy.deepcopy(default)


def _is_data_json_path(path):
    return Path(path).name == "anime_battle_data.json"


def _read_json_file(path):
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as ex:
        logger.exception("Cannot read JSON %s: %s", path, ex)
        return None


def load_json(path, default):
    p = Path(path)
    if not p.exists():
        # Прогресс игроков нельзя автоматически перезаписывать пустышкой.
        # Если DATA_FILE исчез на Render, это признак отсутствия persistent disk.
        if _is_data_json_path(p):
            return _clone_default(default)
        save_json(path, default)
        return _clone_default(default)
    data = _read_json_file(p)
    if data is not None:
        return data
    bak = p.with_suffix(p.suffix + ".bak")
    data = _read_json_file(bak)
    if data is not None:
        return data
    return _clone_default(default)


def _save_data_sqlite(obj, db_path=None):
    """Основное хранилище прогресса: SQLite. JSON остаётся читаемым backup."""
    con = None
    try:
        db_path = str(db_path or DB_FILE)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(db_path, timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)")
        con.execute(
            "INSERT OR REPLACE INTO kv(key, value, updated_at) VALUES (?, ?, ?)",
            ("data", json.dumps(obj, ensure_ascii=False), utc_now().isoformat()),
        )
        con.commit()
    except Exception as ex:
        logger.exception("SQLite save failed: %s", ex)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _load_data_sqlite(db_path=None):
    con = None
    try:
        db_path = str(db_path or DB_FILE)
        if not Path(db_path).exists():
            return None
        con = sqlite3.connect(db_path, timeout=30)
        con.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)")
        row = con.execute("SELECT value FROM kv WHERE key='data'").fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except Exception as ex:
        logger.exception("SQLite load failed: %s", ex)
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return None




_POSTGRES_LAST_LOAD_OK = None
_POSTGRES_LAST_LOAD_ERROR = ""


def _postgres_available():
    return bool(DATABASE_URL and psycopg is not None)


def _postgres_hint():
    if not DATABASE_URL:
        return "DATABASE_URL нет"
    if psycopg is None:
        return "DATABASE_URL есть, но пакет psycopg не установлен"
    return "DATABASE_URL есть"


def _save_data_postgres(obj):
    """PATCH17: главное постоянное хранилище без Render Disk — Neon/PostgreSQL.

    Храним весь DATA как один JSON в kv-таблице. Это минимально меняет старую
    механику бота и не ломает старые поля игроков/коллекций/рейда.
    """
    if not _postgres_available():
        return False
    con = None
    try:
        payload = json.dumps(obj, ensure_ascii=False)
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS anime_battle_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO anime_battle_kv(key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                ("data", payload),
            )
        con.commit()
        return True
    except Exception as ex:
        logger.exception("PostgreSQL/Neon save failed: %s", ex)
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _load_data_postgres():
    global _POSTGRES_LAST_LOAD_OK, _POSTGRES_LAST_LOAD_ERROR
    if not _postgres_available():
        _POSTGRES_LAST_LOAD_OK = None
        _POSTGRES_LAST_LOAD_ERROR = ""
        return None
    con = None
    try:
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS anime_battle_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute("SELECT value FROM anime_battle_kv WHERE key=%s", ("data",))
            row = cur.fetchone()
        con.commit()
        _POSTGRES_LAST_LOAD_OK = True
        _POSTGRES_LAST_LOAD_ERROR = ""
        if row and row[0]:
            return json.loads(row[0])
    except Exception as ex:
        _POSTGRES_LAST_LOAD_OK = False
        _POSTGRES_LAST_LOAD_ERROR = str(ex)[:300]
        logger.exception("PostgreSQL/Neon load failed: %s", ex)
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return None


def _count_postgres_users():
    data = _load_data_postgres()
    if isinstance(data, dict):
        return len((data.get("users", {}) or {}))
    return 0


def _postgres_last_save():
    if not _postgres_available():
        return "нет"
    con = None
    try:
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute("SELECT updated_at FROM anime_battle_kv WHERE key=%s", ("data",))
            row = cur.fetchone()
        return str(row[0]) if row and row[0] else "нет"
    except Exception:
        return "ошибка подключения"
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

def save_json(path, obj, sync_postgres=True):
    """Атомарная запись JSON + backup + Neon.

    PATCH17C: локальная запись не должна валить бота на Render Free,
    если DATA_DIR указывает на недоступный /var/data. Neon остаётся главным хранилищем.
    """
    path_obj = Path(path)
    # PATCH32 SPEED: DATA can be large; pretty JSON makes every save heavier.
    # Keep promo/small files readable, but store runtime DATA compactly.
    if _is_data_json_path(path_obj):
        text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    else:
        text = json.dumps(obj, ensure_ascii=False, indent=2)
    tmp_name = None
    local_ok = False
    try:
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        if path_obj.exists() and _is_data_json_path(path_obj):
            bak = path_obj.with_suffix(path_obj.suffix + ".bak")
            try:
                bak.write_text(path_obj.read_text(encoding="utf-8"), encoding="utf-8")
            except Exception as ex:
                logger.warning("Could not refresh data backup: %s", ex)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path_obj.parent), prefix=path_obj.name + ".tmp.") as tmp:
            tmp.write(text)
            tmp.flush()
            try:
                os.fsync(tmp.fileno())
            except Exception:
                pass
            tmp_name = tmp.name
        os.replace(tmp_name, path_obj)
        local_ok = True
    except Exception as ex:
        logger.warning("Local JSON save skipped: %s", ex)
    finally:
        if tmp_name and Path(tmp_name).exists():
            try:
                Path(tmp_name).unlink()
            except Exception:
                pass
    if _is_data_json_path(path_obj):
        if local_ok:
            _save_data_sqlite(obj)
        remote_ok = False
        if sync_postgres:
            remote_ok = _save_data_postgres(obj)
        # Когда Neon настроен, именно успешная запись в Neon означает, что
        # прогресс действительно сохранён. Локальный /tmp на Render не считается
        # надёжным хранилищем. Без Neon достаточно атомарного локального save.
        if sync_postgres and _postgres_available():
            return bool(remote_ok)
        return bool(local_ok)
    return bool(local_ok)


# PATCH40 SAFE SAVE: revisions + retry/backoff + serialized snapshots.
# DATA is still one backward-compatible JSON object, but no mutation is allowed to
# become "clean" until the exact revision written to the authoritative storage succeeds.
DATA_SAVE_DEBOUNCE_SECONDS = float(os.getenv("DATA_SAVE_DEBOUNCE_SECONDS", "8"))
LAST_SEEN_SAVE_SECONDS = int(os.getenv("LAST_SEEN_SAVE_SECONDS", "120"))
USER_ACTION_SAVE_SECONDS = int(os.getenv("USER_ACTION_SAVE_SECONDS", "45"))
OWNER_FULL_UNLOCK_VERSION = "PATCH40_OWNER_FULL_UNLOCK"
DATA_SAVE_RETRY_MIN_SECONDS = max(2.0, float(os.getenv("DATA_SAVE_RETRY_MIN_SECONDS", "5") or 5))
DATA_SAVE_RETRY_MAX_SECONDS = max(DATA_SAVE_RETRY_MIN_SECONDS, float(os.getenv("DATA_SAVE_RETRY_MAX_SECONDS", "300") or 300))
DATA_SNAPSHOT_MAX_RETRIES = max(2, int(os.getenv("DATA_SNAPSHOT_MAX_RETRIES", "8") or 8))
_DATA_DIRTY = False
_DATA_SAVE_TASK = None
_DATA_SAVE_LAST_AT = ""
_DATA_SAVE_LAST_REASON = "startup"
_DATA_SAVE_LAST_ERROR = ""
_DATA_SAVE_COUNTER = 0
_DATA_SAVE_LOCK = None
_DATA_REVISION = 0
_DATA_LAST_SAVED_REVISION = 0
_STORAGE_HEALTHY = True
_STORAGE_HEALTH_REASON = ""
_DATA_SCHEMA_CHANGED_ON_LOAD = False


def _get_data_save_lock():
    global _DATA_SAVE_LOCK
    if _DATA_SAVE_LOCK is None:
        _DATA_SAVE_LOCK = asyncio.Lock()
    return _DATA_SAVE_LOCK


def storage_is_healthy():
    if DATABASE_URL and not _postgres_available():
        return False
    return bool(_STORAGE_HEALTHY)


def _set_storage_health(ok, reason=""):
    global _STORAGE_HEALTHY, _STORAGE_HEALTH_REASON
    _STORAGE_HEALTHY = bool(ok)
    _STORAGE_HEALTH_REASON = str(reason or "")[:300]


def _schedule_data_save_if_needed():
    global _DATA_SAVE_TASK
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop.is_closed():
        return
    if _DATA_SAVE_TASK is None or _DATA_SAVE_TASK.done():
        _DATA_SAVE_TASK = loop.create_task(_delayed_data_save(), name="abm-data-save")


def mark_data_dirty(reason="data_changed"):
    """Mark a new in-memory revision and guarantee a future save attempt."""
    global _DATA_DIRTY, _DATA_SAVE_LAST_REASON, _DATA_REVISION
    _DATA_REVISION += 1
    _DATA_DIRTY = True
    _DATA_SAVE_LAST_REASON = str(reason or "data_changed")[:120]
    _schedule_data_save_if_needed()


class SnapshotBusyError(RuntimeError):
    pass


def _snapshot_data_consistent_sync():
    """Copy DATA outside the event loop and reject a copy crossed by a marked mutation.

    A worker thread may interleave with the asyncio thread. The revision is read before
    and after deepcopy; if it changed, that copy is discarded and retried. Saving tasks
    are serialized separately, so an older snapshot can never run after a newer save.
    """
    for _attempt in range(DATA_SNAPSHOT_MAX_RETRIES):
        before = _DATA_REVISION
        try:
            snapshot = copy.deepcopy(DATA)
        except RuntimeError:
            # A dict/list may be changing in the event-loop thread while deepcopy walks it.
            # Discard the partial attempt and retry; never persist a failed partial snapshot.
            time.sleep(0.001)
            continue
        # Yield the GIL once so a mutation that just finished updating DATA can publish its
        # revision before we accept this snapshot as stable.
        time.sleep(0)
        after = _DATA_REVISION
        if before == after:
            snapshot.setdefault("storage_meta", {})
            snapshot["storage_meta"]["schema_version"] = DATA_SCHEMA_VERSION
            snapshot["storage_meta"]["storage_version"] = STORAGE_VERSION
            snapshot["storage_meta"]["saved_revision"] = int(after)
            snapshot["storage_meta"]["saved_at_utc"] = utc_now().isoformat()
            return snapshot, int(after)
        time.sleep(0)
    raise SnapshotBusyError("DATA changed repeatedly while snapshot was being copied; retry scheduled")


def _save_snapshot_sync(snapshot):
    if not save_json(DATA_FILE, snapshot):
        target = "Neon/PostgreSQL" if _postgres_available() else str(DATA_FILE)
        raise RuntimeError(f"Не удалось надёжно сохранить DATA в {target}")
    return True


async def _save_one_revision(reason="background"):
    """Save one stable snapshot. Caller must not hold the asyncio event loop."""
    global _DATA_SAVE_LAST_AT, _DATA_SAVE_LAST_ERROR, _DATA_SAVE_COUNTER
    global _DATA_LAST_SAVED_REVISION, _DATA_DIRTY
    async with _get_data_save_lock():
        snapshot, snapshot_revision = await asyncio.to_thread(_snapshot_data_consistent_sync)
        # A previous serialized save may already have persisted this or a newer revision.
        if snapshot_revision <= _DATA_LAST_SAVED_REVISION and not _DATA_DIRTY:
            return True, snapshot_revision
        await asyncio.to_thread(_save_snapshot_sync, snapshot)
        _DATA_LAST_SAVED_REVISION = max(_DATA_LAST_SAVED_REVISION, snapshot_revision)
        _DATA_SAVE_LAST_AT = utc_now().isoformat(timespec="seconds")
        _DATA_SAVE_LAST_ERROR = ""
        _DATA_SAVE_COUNTER += 1
        _DATA_DIRTY = _DATA_REVISION > _DATA_LAST_SAVED_REVISION
        _set_storage_health(True, "")
        logger.debug("DATA saved revision=%s reason=%s", snapshot_revision, reason)
        return True, snapshot_revision


async def _delayed_data_save():
    """Debounced resilient save loop with bounded exponential backoff."""
    global _DATA_SAVE_TASK, _DATA_DIRTY, _DATA_SAVE_LAST_ERROR
    backoff = DATA_SAVE_RETRY_MIN_SECONDS
    try:
        await asyncio.sleep(max(0.2, DATA_SAVE_DEBOUNCE_SECONDS))
        while _DATA_DIRTY or _DATA_REVISION > _DATA_LAST_SAVED_REVISION:
            reason = _DATA_SAVE_LAST_REASON
            try:
                await _save_one_revision(reason)
                backoff = DATA_SAVE_RETRY_MIN_SECONDS
                if not _DATA_DIRTY and _DATA_REVISION <= _DATA_LAST_SAVED_REVISION:
                    return
                # A mutation happened during/after the saved snapshot. Save again without
                # a full debounce, but yield so Telegram updates remain responsive.
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                _DATA_DIRTY = True
                _DATA_SAVE_LAST_ERROR = str(ex)[:300]
                _set_storage_health(False, _DATA_SAVE_LAST_ERROR)
                logger.exception("Background DATA save failed; retry in %.1fs: %s", backoff, ex)
                await asyncio.sleep(backoff)
                backoff = min(DATA_SAVE_RETRY_MAX_SECONDS, max(backoff * 2.0, DATA_SAVE_RETRY_MIN_SECONDS))
    finally:
        current = asyncio.current_task()
        if _DATA_SAVE_TASK is current:
            _DATA_SAVE_TASK = None
        # Critical race fix: a mutation may have arrived while this task was finishing.
        if _DATA_DIRTY or _DATA_REVISION > _DATA_LAST_SAVED_REVISION:
            _schedule_data_save_if_needed()


async def flush_data_now_async(reason="manual_flush"):
    """Persist the newest reachable revision now; remaining activity stays dirty."""
    global _DATA_SAVE_LAST_REASON, _DATA_SAVE_LAST_ERROR, _DATA_DIRTY
    _DATA_SAVE_LAST_REASON = str(reason or "manual_flush")[:120]
    last_ok = False
    # Usually one pass is enough. Extra passes close the race where DATA changed while
    # the first snapshot was being written, without chasing endless live traffic.
    for _ in range(3):
        try:
            last_ok, _rev = await _save_one_revision(_DATA_SAVE_LAST_REASON)
        except Exception as ex:
            _DATA_DIRTY = True
            _DATA_SAVE_LAST_ERROR = str(ex)[:300]
            _set_storage_health(False, _DATA_SAVE_LAST_ERROR)
            logger.exception("Manual async DATA save failed: %s", ex)
            _schedule_data_save_if_needed()
            return False
        if _DATA_REVISION <= _DATA_LAST_SAVED_REVISION:
            _DATA_DIRTY = False
            return bool(last_ok)
        _DATA_DIRTY = True
        await asyncio.sleep(0)
    _schedule_data_save_if_needed()
    return bool(last_ok)


def save_data_now(reason="manual_sync"):
    """Synchronous save for startup/offline tools where no concurrent loop is expected."""
    global _DATA_DIRTY, _DATA_SAVE_LAST_AT, _DATA_SAVE_LAST_ERROR, _DATA_SAVE_COUNTER
    global _DATA_SAVE_LAST_REASON, _DATA_LAST_SAVED_REVISION
    _DATA_SAVE_LAST_REASON = str(reason or "manual_sync")[:120]
    try:
        snapshot = copy.deepcopy(DATA)
        revision = int(_DATA_REVISION)
        snapshot.setdefault("storage_meta", {})
        snapshot["storage_meta"].update({
            "schema_version": DATA_SCHEMA_VERSION,
            "storage_version": STORAGE_VERSION,
            "saved_revision": revision,
            "saved_at_utc": utc_now().isoformat(),
        })
        if not save_json(DATA_FILE, snapshot):
            target = "Neon/PostgreSQL" if _postgres_available() else str(DATA_FILE)
            raise RuntimeError(f"Не удалось надёжно сохранить DATA в {target}")
        _DATA_LAST_SAVED_REVISION = max(_DATA_LAST_SAVED_REVISION, revision)
        _DATA_DIRTY = _DATA_REVISION > _DATA_LAST_SAVED_REVISION
        _DATA_SAVE_LAST_AT = utc_now().isoformat(timespec="seconds")
        _DATA_SAVE_LAST_ERROR = ""
        _DATA_SAVE_COUNTER += 1
        _set_storage_health(True, "")
        return True
    except Exception as ex:
        _DATA_DIRTY = True
        _DATA_SAVE_LAST_ERROR = str(ex)[:300]
        _set_storage_health(False, _DATA_SAVE_LAST_ERROR)
        logger.exception("Manual sync DATA save failed: %s", ex)
        _schedule_data_save_if_needed()
        return False


def _collection_score(collection):
    score = 0
    for info in (collection or {}).values():
        try:
            score += int(info.get("count", 0)) * 10 + int(info.get("level", 1)) + int(info.get("shards", 0)) // 20
        except Exception:
            score += 1
    return score


def _player_progress_score(player):
    player = player or {}
    return (
        len(player.get("collection", {}) or {}) * 1000
        + _collection_score(player.get("collection", {}) or {})
        + int(player.get("battles", 0) or 0) * 20
        + int(player.get("wins", 0) or 0) * 30
        + int(player.get("xp", 0) or 0)
        + int(player.get("pass_xp", 0) or 0)
    )


def _merge_collection_data(a, b):
    result = copy.deepcopy(a or {})
    for cid, info in (b or {}).items():
        if cid not in result:
            result[cid] = copy.deepcopy(info)
            continue
        cur = result.get(cid) or {}
        inc = info or {}
        merged = dict(cur)
        for key in ["count", "shards", "level"]:
            try:
                merged[key] = max(int(cur.get(key, 0)), int(inc.get(key, 0)))
            except Exception:
                merged[key] = cur.get(key, inc.get(key, 0))
        merged["unlocked"] = bool(cur.get("unlocked")) or bool(inc.get("unlocked")) or int(merged.get("count", 0) or 0) > 0
        result[cid] = merged
    return result


def _merge_list_unique(a, b):
    result = []
    for item in list(a or []) + list(b or []):
        if item not in result:
            result.append(item)
    return result


def _latest_iso(a, b):
    if not a:
        return b or ""
    if not b:
        return a or ""
    try:
        adt = _parse_iso_datetime(a)
        bdt = _parse_iso_datetime(b)
        if adt and bdt:
            return b if bdt > adt else a
        return b if bdt else a
    except Exception:
        return max(str(a), str(b))


def _merge_player_data(a, b):
    """Склеивает версии одного игрока без сброса прогресса до стартовых 5 карт."""
    a = a or {}
    b = b or {}
    merged = copy.deepcopy(a if _player_progress_score(a) >= _player_progress_score(b) else b)
    for k, v in b.items():
        if k not in merged or merged.get(k) in ("", None, {}, []):
            merged[k] = copy.deepcopy(v)
    for k, v in a.items():
        if k not in merged or merged.get(k) in ("", None, {}, []):
            merged[k] = copy.deepcopy(v)
    merged["collection"] = _merge_collection_data(a.get("collection", {}), b.get("collection", {}))
    numeric_max_keys = ["fistiks", "coins", "xp", "wins", "losses", "battles", "stars_earned", "moon_coins", "pass_xp", "pass_premium_cap", "ref_count", "ref_earned"]
    for k in numeric_max_keys:
        try:
            merged[k] = max(int(a.get(k, 0) or 0), int(b.get(k, 0) or 0), int(merged.get(k, 0) or 0))
        except Exception:
            pass
    for k in ["premium", "pass_premium", "notify_free_pack", "banned", "frozen"]:
        if k in a or k in b:
            merged[k] = bool(a.get(k, False)) or bool(b.get(k, False))
    for k in ["badges", "used_promos", "claimed_pass_free", "claimed_pass_premium", "newbie_claimed", "ref_milestones_claimed", "processed_payments", "battle_history", "support_tickets", "pass_task_claimed"]:
        merged[k] = _merge_list_unique(a.get(k, []), b.get(k, []))
    for k in ["pass_task_progress", "newbie_progress"]:
        tmp = {}
        tmp.update(a.get(k, {}) or {})
        tmp.update(b.get(k, {}) or {})
        merged[k] = tmp
    for k in ["last_seen", "last_daily", "last_free_pack", "last_free_notice", "pass_daily_date", "created_at"]:
        merged[k] = _latest_iso(a.get(k, ""), b.get(k, ""))
    if b.get("nickname"):
        merged["nickname"] = b.get("nickname")
    elif a.get("nickname"):
        merged["nickname"] = a.get("nickname")
    if b.get("name") and not b.get("name", "").isdigit():
        merged["name"] = b.get("name")
    elif a.get("name"):
        merged["name"] = a.get("name")
    return merged


def _merge_users_data(primary, secondary):
    """Склеивает все найденные источники, не затирая большой прогресс маленькой пустышкой."""
    primary = copy.deepcopy(primary or {"users": {}, "friend_invites": {}, "friends": {}})
    secondary = secondary or {"users": {}, "friend_invites": {}, "friends": {}}
    primary.setdefault("users", {})
    secondary.setdefault("users", {})
    for uid, player in secondary.get("users", {}).items():
        if uid not in primary["users"]:
            primary["users"][uid] = copy.deepcopy(player)
        else:
            primary["users"][uid] = _merge_player_data(primary["users"][uid], player)
    for section in ["friend_invites", "friend_requests", "friends", "deleted_users"]:
        primary.setdefault(section, {})
        for k, v in (secondary.get(section, {}) or {}).items():
            if k in primary[section] and isinstance(primary[section][k], list) and isinstance(v, list):
                primary[section][k] = _merge_list_unique(primary[section][k], v)
            else:
                primary[section].setdefault(k, copy.deepcopy(v))
    return primary


def _data_score(obj):
    users = (obj or {}).get("users", {}) if isinstance(obj, dict) else {}
    return (len(users), sum(_player_progress_score(p) for p in users.values()))


def _data_json_candidates(default):
    paths = [Path(DATA_FILE), Path(DATA_FILE).with_suffix(Path(DATA_FILE).suffix + ".bak")]
    if DATA_DIR != BASE_DIR:
        paths += [BASE_DIR / "anime_battle_data.json", BASE_DIR / "anime_battle_data.json.bak"]
    seen = set()
    for p in paths:
        sp = str(p)
        if sp in seen:
            continue
        seen.add(sp)
        data = _read_json_file(p)
        if isinstance(data, dict):
            yield data


def _data_postgres_candidates():
    data = _load_data_postgres()
    if isinstance(data, dict):
        yield data


def _data_sqlite_candidates():
    paths = [Path(DB_FILE)]
    if DATA_DIR != BASE_DIR:
        paths.append(BASE_DIR / "anime_battle_data.db")
    seen = set()
    for p in paths:
        sp = str(p)
        if sp in seen:
            continue
        seen.add(sp)
        data = _load_data_sqlite(p)
        if isinstance(data, dict):
            yield data


def _normalise_data_root(data, default):
    """Idempotent additive root migration. Never decreases balances or rewrites collections."""
    global _DATA_SCHEMA_CHANGED_ON_LOAD
    data = copy.deepcopy(data) if isinstance(data, dict) else _clone_default(default)
    changed = False
    for key, value in {
        "users": {},
        "friend_invites": {},
        "friend_requests": {},
        "friends": {},
        "deleted_users": {},
        "purged_users": {},
        "promo_usage": {},
        "payment_ledger": {},
        "payment_recovery_queue": {},
        "season_history": {},
    }.items():
        if key not in data:
            data[key] = copy.deepcopy(value)
            changed = True

    if not isinstance(data.get("payment_ledger"), dict):
        data["payment_ledger"] = {}
        changed = True
    if not isinstance(data.get("payment_recovery_queue"), dict):
        migrated_queue = {}
        if isinstance(data.get("payment_recovery_queue"), list):
            for event in data.get("payment_recovery_queue", []):
                if isinstance(event, dict) and event.get("charge_id"):
                    migrated_queue[str(event["charge_id"])] = event
        data["payment_recovery_queue"] = migrated_queue
        changed = True

    meta = data.setdefault("storage_meta", {})
    # Migrate all still-known PATCH35/PATCH36 payment ids into the unbounded root ledger once.
    # This cannot resurrect ids that an old version had already permanently discarded, but it
    # covers both processed_payments and the longer purchases audit history still present in DATA.
    if not meta.get("payment_ledger_migrated_v37"):
        ledger = data.setdefault("payment_ledger", {})
        for uid, player in (data.get("users", {}) or {}).items():
            if not isinstance(player, dict):
                continue
            ids = set(str(x) for x in (player.get("processed_payments", []) or []) if x)
            for purchase in player.get("purchases", []) or []:
                if isinstance(purchase, dict) and purchase.get("id"):
                    ids.add(str(purchase["id"]))
            for charge_id in ids:
                ledger.setdefault(charge_id, {
                    "status": "completed",
                    "user_id": str(uid),
                    "legacy_migrated": True,
                    "migrated_at": utc_now().isoformat(),
                })
        meta["payment_ledger_migrated_v37"] = True
        changed = True

    if int(meta.get("schema_version", 0) or 0) < DATA_SCHEMA_VERSION:
        meta["schema_version"] = DATA_SCHEMA_VERSION
        changed = True
    if meta.get("storage_version") != STORAGE_VERSION:
        meta["storage_version"] = STORAGE_VERSION
        changed = True
    _DATA_SCHEMA_CHANGED_ON_LOAD = _DATA_SCHEMA_CHANGED_ON_LOAD or changed
    return data


def load_data_storage(default):
    """Загружает один авторитетный снимок, не склеивая старые балансы.

    Neon/PostgreSQL, если он настроен, является единственным источником истины.
    При временной недоступности Neon бот намеренно не запускается на старой
    локальной копии: иначе следующий save мог бы затереть свежую облачную базу.
    SQLite/JSON используются как fallback только когда Neon не настроен либо
    когда подключение успешно, но облачная таблица ещё пуста.
    """
    if DATABASE_URL and psycopg is None:
        raise RuntimeError(
            "DATABASE_URL задан, но psycopg не установлен. Установи requirements.txt и перезапусти бот."
        )

    pg_data = _load_data_postgres()
    if _postgres_available() and _POSTGRES_LAST_LOAD_OK is False:
        raise RuntimeError(
            "Neon/PostgreSQL временно недоступен. Бот остановлен, чтобы не запустить "
            "устаревший локальный прогресс и не перезаписать облачную базу. "
            f"Ошибка: {_POSTGRES_LAST_LOAD_ERROR or 'неизвестная ошибка подключения'}"
        )
    if isinstance(pg_data, dict):
        data = _normalise_data_root(pg_data, default)
        save_json(DATA_FILE, data, sync_postgres=False)  # локальный кэш, не обязательный
        logger.info("Storage source: PostgreSQL/Neon (%s users)", len(data.get("users", {})))
        return data

    # Если Neon подключился, но строка data отсутствует, можно безопасно
    # инициализировать его из локального backup или из пустой структуры.
    initialise_empty_postgres = bool(_postgres_available() and _POSTGRES_LAST_LOAD_OK is True)

    sqlite_data = _load_data_sqlite()
    if isinstance(sqlite_data, dict):
        data = _normalise_data_root(sqlite_data, default)
        save_json(DATA_FILE, data, sync_postgres=initialise_empty_postgres)
        logger.warning("Storage source: local SQLite fallback (%s users)", len(data.get("users", {})))
        return data

    for json_data in _data_json_candidates(default):
        if isinstance(json_data, dict):
            data = _normalise_data_root(json_data, default)
            save_json(DATA_FILE, data, sync_postgres=initialise_empty_postgres)
            logger.warning("Storage source: local JSON fallback (%s users)", len(data.get("users", {})))
            return data

    data = _normalise_data_root(default, default)
    if not save_json(DATA_FILE, data, sync_postgres=initialise_empty_postgres):
        logger.warning("Initial empty DATA could not be persisted yet")
    logger.warning("Storage source: new empty database")
    return data


def storage_report_text():
    users = DATA.get("users", {}) if isinstance(DATA, dict) else {}
    data_json = Path(DATA_FILE)
    data_db = Path(DB_FILE)

    def _count_json_users(path):
        try:
            obj = _read_json_file(path)
            return len((obj or {}).get("users", {}) or {}) if isinstance(obj, dict) else 0
        except Exception:
            return 0

    def _count_db_users(path):
        try:
            obj = _load_data_sqlite(path)
            return len((obj or {}).get("users", {}) or {}) if isinstance(obj, dict) else 0
        except Exception:
            return 0

    try:
        last_save = datetime.fromtimestamp(data_json.stat().st_mtime).isoformat(timespec="seconds") if data_json.exists() else "нет"
    except Exception:
        last_save = "нет"
    is_var_data = str(DATA_DIR) == "/var/data"
    pg_users = _count_postgres_users()
    pg_last = _postgres_last_save()
    if _postgres_available():
        warning = "✅ Neon/PostgreSQL подключён. Render Disk больше не обязателен для прогресса."
    elif DATABASE_URL and psycopg is None:
        warning = "⚠️ DATABASE_URL есть, но psycopg не установлен. Проверь requirements.txt и redeploy."
    elif is_var_data:
        warning = "✅ DATA_DIR настроен правильно."
    else:
        warning = "⚠️ Нет Neon и DATA_DIR не /var/data. На Render это риск сброса прогресса после redeploy."
    unknown_cards = 0
    for player in users.values():
        for cid in (player or {}).get("collection", {}) or {}:
            if cid not in CARD_BY_ID:
                unknown_cards += 1
    return (
        "🧠 <b>Хранилище прогресса</b>\n\n"
        f"DATA_DIR: <code>{e(DATA_DIR)}</code>\n"
        f"REQUESTED_DATA_DIR: <code>{e(REQUESTED_DATA_DIR)}</code>\n"
        f"DATA_DIR warning: {e(DATA_DIR_WARNING or 'нет')}\n"
        f"DATA_FILE: <code>{e(data_json)}</code> — {'есть' if data_json.exists() else 'нет'}\n"
        f"DB_FILE: <code>{e(data_db)}</code> — {'есть' if data_db.exists() else 'нет'}\n"
        f"LOG_FILE: <code>{e(LOG_FILE)}</code>\n"
        f"/var/data: <b>{'есть' if Path('/var/data').exists() else 'нет'}</b>\n"
        f"DATABASE_URL/Neon: <b>{'есть' if DATABASE_URL else 'нет'}</b> — {e(_postgres_hint())}\n"
        f"SPEED-save: dirty=<b>{'да' if _DATA_DIRTY else 'нет'}</b>, saves=<b>{_DATA_SAVE_COUNTER}</b>, last=<code>{e(_DATA_SAVE_LAST_AT or 'нет')}</code>\n"
        f"last reason: <code>{e(_DATA_SAVE_LAST_REASON)}</code>{' | error: <code>' + e(_DATA_SAVE_LAST_ERROR) + '</code>' if _DATA_SAVE_LAST_ERROR else ''}\n\n"
        f"Игроков в DATA: <b>{len(users)}</b>\n"
        f"Игроков в Neon: <b>{pg_users}</b>\n"
        f"Игроков в JSON: <b>{_count_json_users(data_json)}</b>\n"
        f"Игроков в DB: <b>{_count_db_users(data_db)}</b>\n"
        f"Unknown card count: <b>{unknown_cards}</b>\n"
        f"Последнее сохранение JSON: <b>{e(last_save)}</b>\n"
        f"Последнее сохранение Neon: <b>{e(pg_last)}</b>\n\n"
        f"{warning}"
    )
def read_token():
    # Для хостинга: добавь токен в переменную окружения BOT_TOKEN.
    # Для запуска на ПК: можно оставить token.txt рядом с bot.py.
    token = os.getenv("BOT_TOKEN", "").strip()
    if token:
        return token

    path = Path(TOKEN_FILE)
    if not path.exists():
        path.write_text("PASTE_YOUR_BOT_TOKEN_HERE", encoding="utf-8")
    token = path.read_text(encoding="utf-8").strip()
    if not token or token == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Put your bot token into BOT_TOKEN on hosting or into token.txt locally")
    return token


def ensure_files():
    MEDIA_DIR.mkdir(exist_ok=True)
    if not Path(OWNER_FILE).exists():
        Path(OWNER_FILE).write_text("PUT_YOUR_TELEGRAM_ID_HERE", encoding="utf-8")

    # Никому не выдаём права автоматически. Раньше при отсутствии файла сюда
    # записывался чужой Telegram ID — это было небезопасно.
    if not Path(RIGHT_HAND_FILE).exists():
        Path(RIGHT_HAND_FILE).write_text("", encoding="utf-8")

    promo_target = Path(PROMO_FILE)
    promo_source = BASE_DIR / "promo_codes.json"
    if not promo_target.exists() and promo_source.exists() and promo_source.resolve() != promo_target.resolve():
        try:
            promo_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(promo_source, promo_target)
        except Exception as ex:
            logger.warning("Cannot copy promo_codes.json to DATA_DIR: %s", ex)

    if not promo_target.exists():
        promo = {
            "START500": {"active": True, "expires": "2027-12-31", "max_uses": 100000, "reward": {"fistiks": 500}, "description": "+500 фисташек"},
            "PACKTEST": {"active": True, "expires": "2027-12-31", "max_uses": 100000, "reward": {"fistiks": 1500}, "description": "+1500 фисташек"},
            "ITACHI": {"active": True, "expires": "2027-12-31", "max_uses": 100000, "reward": {"card": "itachi_akatsuki", "shards": 80}, "description": "Итачи + 80 фрагментов"},
        }
        save_json(PROMO_FILE, promo)


def _ids_from_env(*names):
    ids = set()
    for name in names:
        raw = os.getenv(name, "")
        for token in raw.replace(",", " ").replace(";", " ").split():
            token = token.strip()
            if token.isdigit():
                ids.add(token)
    return ids


def read_ids(path):
    p = Path(path)
    if not p.exists():
        return set()
    ids = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.isdigit():
            ids.add(line)
    return ids


def owner_ids():
    return read_ids(OWNER_FILE) | _ids_from_env("OWNER_ID", "OWNER_IDS", "BOT_OWNER_ID")


# SHA-256 fingerprint of the single historical foreign right-hand ID that an old
# build could auto-create. The raw foreign ID itself is deliberately not shipped.
LEGACY_UNSAFE_RIGHT_HAND_SHA256 = "51505dcd0329ecdf5cd799239b7eb97da8fa2a1d2e718d3a56e63f396b2fe47e"


def _is_legacy_unsafe_right_hand_id(value):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest() == LEGACY_UNSAFE_RIGHT_HAND_SHA256



def right_hand_ids():
    enabled = str(os.getenv("ABM_ENABLE_RIGHT_HAND_PERMISSIONS", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return set()
    ids = read_ids(RIGHT_HAND_FILE) | _ids_from_env("RIGHT_HAND_ID", "RIGHT_HAND_IDS")
    allow_legacy = str(os.getenv("ABM_ALLOW_LEGACY_RIGHT_HAND_ID", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if not allow_legacy:
        ids = {value for value in ids if not _is_legacy_unsafe_right_hand_id(value)}
    return ids



def is_owner(user_id):
    return str(user_id) in owner_ids()



def is_right_hand(user_id):
    return str(user_id) in right_hand_ids() and not is_owner(user_id)




# PATCH23: большие RPG-вселенные, отдельный выбор призыва и скрытый premium-cosmic слой.
# Бои/рейды остаются мультивселенными, а получение карт/крафт/кейсы смотрят на выбранный игроком мир.
UNIVERSE_ALIAS_BY_ANIME = {
    'Attack on Titan': ('attack_on_titan', 'Атака титанов'),
    'Baki': ('baki', 'Baki / Баки'),
    'Baki / Баки': ('baki', 'Baki / Баки'),
    'Berserk': ('berserk', 'Берсерк'),
    'Beelzebub': ('beelzebub', 'Beelzebub / Вельзевул'),
    'Вельзевул': ('beelzebub', 'Beelzebub / Вельзевул'),
    'Вельзепуз': ('beelzebub', 'Beelzebub / Вельзевул'),
    'Белзебаб': ('beelzebub', 'Beelzebub / Вельзевул'),
    'Black Clover': ('black_clover', 'Чёрный клевер'),
    'Bleach': ('bleach', 'Bleach / Блич'),
    'Bleach / Блич': ('bleach', 'Bleach / Блич'),
    'Blue Lock': ('blue_lock', 'Синяя тюрьма'),
    'Boku no Hero Academia': ('my_hero_academia', 'Моя геройская академия'),
    'Boruto': ('naruto_boruto', 'Naruto / Boruto'),
    'Chainsaw Man': ('chainsaw_man', 'Chainsaw Man / Человек-бензопила'),
    'Chainsaw Man / Человек-бензопила': ('chainsaw_man', 'Chainsaw Man / Человек-бензопила'),
    'Classroom of the Elite': ('classroom_elite', 'Класс превосходства'),
    'Code Geass': ('code_geass', 'Код Гиас'),
    'DBZ': ('dragon_ball', 'Dragon Ball'),
    'Demon Slayer': ('demon_slayer', 'Клинок, рассекающий демонов'),
    'Dragon Ball': ('dragon_ball', 'Dragon Ball'),
    'Dragon Ball Daima': ('dragon_ball', 'Dragon Ball'),
    'Dragon Ball GT': ('dragon_ball', 'Dragon Ball'),
    'Dragon Ball Heroes': ('dragon_ball', 'Dragon Ball'),
    'Dragon Ball Super': ('dragon_ball', 'Dragon Ball'),
    'Dragon Ball Z': ('dragon_ball', 'Dragon Ball'),
    'Fairy Tail': ('fairy_tail', 'Хвост Феи'),
    'Fate': ('fate', 'Fate'),
    'Fate/Grand Order': ('fate', 'Fate'),
    'Fate/stay night': ('fate', 'Fate'),
    'Fate/Zero': ('fate', 'Fate'),
    'Fire Force': ('fire_force', 'Пламенная бригада пожарных'),
    'Frieren': ('frieren', 'Фрирен'),
    'Fullmetal Alchemist': ('fullmetal_alchemist', 'Стальной алхимик'),
    'Haikyuu': ('haikyuu', 'Волейбол!!'),
    'Haikyuu!!': ('haikyuu', 'Волейбол!!'),
    'Hajime no Ippo': ('hajime_no_ippo', 'Первый шаг'),
    "Hell's Paradise": ('hells_paradise', 'Адский рай'),
    'Hellsing': ('hellsing', 'Хеллсинг'),
    'Hunter x Hunter': ('hunter_x_hunter', 'Hunter x Hunter'),
    'Hunter × Hunter': ('hunter_x_hunter', 'Hunter x Hunter'),
    'Jigokuraku': ('hells_paradise', 'Адский рай'),
    'JoJo': ('jojo', 'Невероятные приключения ДжоДжо'),
    "JoJo's Bizarre Adventure": ('jojo', 'Невероятные приключения ДжоДжо'),
    'Jujutsu Kaisen': ('jujutsu_kaisen', 'Jujutsu Kaisen / Магическая битва'),
    'Jujutsu Kaisen / Магическая битва': ('jujutsu_kaisen', 'Jujutsu Kaisen / Магическая битва'),
    'Kimetsu no Yaiba': ('demon_slayer', 'Клинок, рассекающий демонов'),
    'Kuroko no Basket': ('kuroko_basket', 'Баскетбол Куроко'),
    'Maou Gakuin': ('premium_cosmic', 'Premium: сверх-Absolute сущности'),
    'Mob Psycho 100': ('mob_psycho_100', 'Mob Psycho 100'),
    'Monster': ('monster', 'Монстр'),
    'My Hero Academia': ('my_hero_academia', 'Моя геройская академия'),
    'Nanatsu no Taizai': ('seven_deadly_sins', 'Семь смертных грехов'),
    'Naruto': ('naruto_boruto', 'Naruto / Boruto'),
    'Naruto / Boruto': ('naruto_boruto', 'Naruto / Boruto'),
    'Neon Genesis Evangelion': ('evangelion', 'Евангелион'),
    'One Piece': ('one_piece', 'One Piece / Ван-Пис'),
    'One Piece / Ван-Пис': ('one_piece', 'One Piece / Ван-Пис'),
    'One Punch Man': ('one_punch_man', 'One Punch Man / Ванпанчмен'),
    'One Punch Man / Ванпанчмен': ('one_punch_man', 'One Punch Man / Ванпанчмен'),
    'One-Punch Man': ('one_punch_man', 'One Punch Man / Ванпанчмен'),
    'Pokemon': ('pokemon', 'Покемоны'),
    'Pokémon': ('pokemon', 'Покемоны'),
    'Re:Zero': ('rezero', 'Re:Zero'),
    'Record of Ragnarok': ('record_of_ragnarok', 'Повесть о конце света'),
    'SAO': ('sao', 'Мастера меча онлайн'),
    'Shingeki no Kyojin': ('attack_on_titan', 'Атака титанов'),
    'Solo Leveling': ('solo_leveling', 'Поднятие уровня в одиночку'),
    'Sword Art Online': ('sao', 'Мастера меча онлайн'),
    'Tenchi Muyo': ('premium_cosmic', 'Premium: сверх-Absolute сущности'),
    'Tensei Slime': ('tensei_slime', 'О моём перерождении в слизь'),
    'Tokyo Ghoul': ('tokyo_ghoul', 'Токийский гуль'),
    'Tokyo Revengers': ('tokyo_revengers', 'Токийские мстители'),
    'Tower of God': ('tower_of_god', 'Башня Бога'),
    'Umineko': ('premium_cosmic', 'Premium: сверх-Absolute сущности'),
    'Vinland Saga': ('vinland_saga', 'Сага о Винланде'),
    'Адский рай': ('hells_paradise', 'Адский рай'),
    'Атака на титанов': ('attack_on_titan', 'Атака титанов'),
    'Атака титанов': ('attack_on_titan', 'Атака титанов'),
    'Баки': ('baki', 'Baki / Баки'),
    'Баскетбол Куроко': ('kuroko_basket', 'Баскетбол Куроко'),
    'Башня Бога': ('tower_of_god', 'Башня Бога'),
    'Башня бога': ('tower_of_god', 'Башня Бога'),
    'Бейс Баки': ('baki', 'Baki / Баки'),
    'Берсерк': ('berserk', 'Берсерк'),
    'Блич': ('bleach', 'Bleach / Блич'),
    'Боруто': ('naruto_boruto', 'Naruto / Boruto'),
    'Ван-Пис': ('one_piece', 'One Piece / Ван-Пис'),
    'Ванпанчмен': ('one_punch_man', 'One Punch Man / Ванпанчмен'),
    'Волейбол': ('haikyuu', 'Волейбол!!'),
    'Волейбол!!': ('haikyuu', 'Волейбол!!'),
    'ДжоДжо': ('jojo', 'Невероятные приключения ДжоДжо'),
    'Драгонболл': ('dragon_ball', 'Dragon Ball'),
    'Драгонболл Z': ('dragon_ball', 'Dragon Ball'),
    'Драгонболл: фильм': ('dragon_ball', 'Dragon Ball'),
    'Евангелион': ('evangelion', 'Евангелион'),
    'Ками Тэнчи': ('premium_cosmic', 'Premium: сверх-Absolute сущности'),
    'Кимэцу но Яйба': ('demon_slayer', 'Клинок, рассекающий демонов'),
    'Класс превосходства': ('classroom_elite', 'Класс превосходства'),
    'Клинок, рассекающий демонов': ('demon_slayer', 'Клинок, рассекающий демонов'),
    'Код Гиас': ('code_geass', 'Код Гиас'),
    'Код Гиасс': ('code_geass', 'Код Гиас'),
    'Лич': ('bleach', 'Bleach / Блич'),
    'Магическая битва': ('jujutsu_kaisen', 'Jujutsu Kaisen / Магическая битва'),
    'Мастера меча онлайн': ('sao', 'Мастера меча онлайн'),
    'Моб Психо 100': ('mob_psycho_100', 'Mob Psycho 100'),
    'Мог психостол': ('mob_psycho_100', 'Mob Psycho 100'),
    'Монстр': ('monster', 'Монстр'),
    'Моя геройская академия': ('my_hero_academia', 'Моя геройская академия'),
    'Наруто': ('naruto_boruto', 'Naruto / Boruto'),
    'Невероятные приключения ДжоДжо': ('jojo', 'Невероятные приключения ДжоДжо'),
    'Невероятные приключения Джоджо': ('jojo', 'Невероятные приключения ДжоДжо'),
    'Непризнанный школой владыка демонов': ('premium_cosmic', 'Premium: сверх-Absolute сущности'),
    'О моём перерождении в слизь': ('tensei_slime', 'О моём перерождении в слизь'),
    'Одскейрай': ('hells_paradise', 'Адский рай'),
    'Первый шаг': ('hajime_no_ippo', 'Первый шаг'),
    'Персек': ('berserk', 'Берсерк'),
    'Пламенная бригада пожарных': ('fire_force', 'Пламенная бригада пожарных'),
    'Повесть о конце света': ('record_of_ragnarok', 'Повесть о конце света'),
    'Поднятие уровня в одиночку': ('solo_leveling', 'Поднятие уровня в одиночку'),
    'Покемон': ('pokemon', 'Покемоны'),
    'Покемон: фильм': ('pokemon', 'Покемоны'),
    'Покемоны': ('pokemon', 'Покемоны'),
    'Провожающая в последний путь Фрирен': ('frieren', 'Фрирен'),
    'Ре: Зеро': ('rezero', 'Re:Zero'),
    'Ре:Zero': ('rezero', 'Re:Zero'),
    'Сага в Винланде': ('vinland_saga', 'Сага о Винланде'),
    'Сага о Винланде': ('vinland_saga', 'Сага о Винланде'),
    'Семь смертных грехов': ('seven_deadly_sins', 'Семь смертных грехов'),
    'Синяя тюрьма': ('blue_lock', 'Синяя тюрьма'),
    'Стальной алхимик': ('fullmetal_alchemist', 'Стальной алхимик'),
    'Тетрадь смерти': ('death_note', 'Тетрадь смерти'),
    'Токийские мстители': ('tokyo_revengers', 'Токийские мстители'),
    'Токийский гуль': ('tokyo_ghoul', 'Токийский гуль'),
    'Уминэко': ('premium_cosmic', 'Premium: сверх-Absolute сущности'),
    'Фриран': ('frieren', 'Фрирен'),
    'Фрирен': ('frieren', 'Фрирен'),
    'Хантер икс Хантер': ('hunter_x_hunter', 'Hunter x Hunter'),
    'Хвост Феи': ('fairy_tail', 'Хвост Феи'),
    'Хеллсинг': ('hellsing', 'Хеллсинг'),
    'Хост фей': ('fairy_tail', 'Хвост Феи'),
    'Человек-бензопила': ('chainsaw_man', 'Chainsaw Man / Человек-бензопила'),
    'Черный клевер': ('black_clover', 'Чёрный клевер'),
    'Чёрный клевер': ('black_clover', 'Чёрный клевер'),
}

UNIVERSE_PRIORITY = ['jojo', 'one_piece', 'demon_slayer', 'jujutsu_kaisen', 'death_note', 'chainsaw_man', 'hells_paradise', 'fate', 'pokemon', 'seven_deadly_sins', 'sao', 'rezero', 'evangelion', 'tower_of_god', 'kuroko_basket', 'attack_on_titan', 'black_clover', 'monster', 'code_geass', 'hajime_no_ippo', 'blue_lock', 'tokyo_ghoul', 'fairy_tail', 'naruto_boruto', 'hellsing', 'berserk', 'fire_force', 'haikyuu', 'fullmetal_alchemist', 'my_hero_academia', 'one_punch_man', 'vinland_saga', 'frieren', 'baki', 'record_of_ragnarok', 'solo_leveling', 'dragon_ball', 'bleach', 'hunter_x_hunter', 'tensei_slime', 'classroom_elite', 'tokyo_revengers', 'mob_psycho_100', 'beelzebub', 'premium_cosmic']

# В выборе показываем только нормальные витринные миры. Premium-cosmic и мелкие источники не засоряют меню.
FEATURED_UNIVERSE_IDS = ['jojo', 'one_piece', 'demon_slayer', 'jujutsu_kaisen', 'death_note', 'chainsaw_man', 'hells_paradise', 'fate', 'pokemon', 'seven_deadly_sins', 'sao', 'rezero', 'evangelion', 'tower_of_god', 'kuroko_basket', 'attack_on_titan', 'black_clover', 'monster', 'code_geass', 'hajime_no_ippo', 'blue_lock', 'tokyo_ghoul', 'fairy_tail', 'naruto_boruto', 'hellsing', 'berserk', 'fire_force', 'haikyuu', 'fullmetal_alchemist', 'my_hero_academia', 'one_punch_man', 'vinland_saga', 'frieren', 'baki', 'record_of_ragnarok', 'solo_leveling', 'dragon_ball', 'bleach', 'hunter_x_hunter', 'tensei_slime', 'classroom_elite', 'tokyo_revengers', 'mob_psycho_100', 'beelzebub']

UNIVERSE_EMOJI = {
    'all': '🌌',
    'jojo': '⭐',
    'one_piece': '🏴\u200d☠️',
    'demon_slayer': '🌊',
    'jujutsu_kaisen': '🧿',
    'death_note': '📓',
    'chainsaw_man': '🪚',
    'hells_paradise': '🌺',
    'fate': '⚔️',
    'pokemon': '⚡',
    'seven_deadly_sins': '🐗',
    'sao': '🎮',
    'rezero': '❄️',
    'evangelion': '🤖',
    'tower_of_god': '🗼',
    'kuroko_basket': '🏀',
    'attack_on_titan': '🧱',
    'black_clover': '🍀',
    'monster': '🧠',
    'code_geass': '👁',
    'hajime_no_ippo': '🥊',
    'blue_lock': '⚽',
    'tokyo_ghoul': '☕',
    'fairy_tail': '🧚',
    'naruto_boruto': '🌪',
    'hellsing': '🩸',
    'berserk': '🗡️',
    'fire_force': '🔥',
    'haikyuu': '🏐',
    'fullmetal_alchemist': '⚙️',
    'my_hero_academia': '🦸',
    'one_punch_man': '👊',
    'vinland_saga': '🛶',
    'frieren': '🪄',
    'baki': '🥊',
    'record_of_ragnarok': '⚖️',
    'solo_leveling': '🗡',
    'dragon_ball': '🟠',
    'bleach': '🗡',
    'hunter_x_hunter': '🎣',
    'tensei_slime': '💧',
    'classroom_elite': '🎓',
    'tokyo_revengers': '🏍',
    'mob_psycho_100': '🌀',
    'beelzebub': '👶',
    'premium_cosmic': '♾',
    'legacy_multiverse': '🗂',
}
UNIVERSE_ONBOARDING_VERSION = "patch30_all_in_one_golden_final"

SUPER_ABSOLUTE_TERMS = (
    "анос", "anos", "фезарин", "featherine", "ками тэнчи", "kami tenchi", "tenchi muyo",
    "зено", "zeno", "омни-король", "rimuru_god", "azathoth", "азатот", "шибай", "shibai",
    "мадока", "madoka", "sailor cosmos", "truth", "истина",
)
SUPER_ABSOLUTE_ANIME_TERMS = (
    "maou gakuin", "непризнанный школой владыка демонов", "umineko", "уминэко", "tenchi muyo",
)


def _stable_universe_id(raw_name: str) -> str:
    raw = str(raw_name or "Другая вселенная").strip()
    alias = UNIVERSE_ALIAS_BY_ANIME.get(raw)
    if alias:
        return alias[0]
    low = raw.casefold()
    for k, v in UNIVERSE_ALIAS_BY_ANIME.items():
        if k.casefold() == low:
            return v[0]
    return "u_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]


def _pretty_universe_name(raw_name: str) -> str:
    raw = str(raw_name or "Другая вселенная").strip() or "Другая вселенная"
    alias = UNIVERSE_ALIAS_BY_ANIME.get(raw)
    if alias:
        return alias[1]
    low = raw.casefold()
    for k, v in UNIVERSE_ALIAS_BY_ANIME.items():
        if k.casefold() == low:
            return v[1]
    return raw


def is_super_absolute_card(card):
    """Super Absolute должен быть отдельным слоем, без случайных совпадений по имени.

    Раньше широкие слова вроде «Зено», «Истина» или «Шибай» могли случайно
    превращать обычные карты выбранных вселенных в Super Absolute. PATCH30
    оставляет Super Absolute только для premium_cosmic/явно помеченных карт.
    """
    if not card:
        return False
    if card.get("super_absolute") or card.get("special_tier") == "super_absolute":
        return True
    if card.get("universe") == "premium_cosmic" or str(card.get("universe_name", "")).casefold().startswith("premium:"):
        return True
    if card.get("premium_only") and card.get("rarity") in ("Легендарный", "Мифический"):
        return True
    return False


def card_draw_universe(card):
    """Runtime draw scope for legacy cards; card ids and cards.json stay untouched."""
    if not isinstance(card, dict):
        return ""
    explicit = str(card.get("draw_universe") or "").strip()
    if explicit:
        return explicit
    original = str(card.get("original_universe") or "").strip()
    if original and (card.get("legacy_scope") or card.get("universe") == "legacy_multiverse"):
        return original
    return str(card.get("universe") or "").strip()


def build_universes(cards):
    data = {}
    for c in cards:
        uid = card_draw_universe(c) or _stable_universe_id(c.get("anime", ""))
        name = c.get("universe_name") or _pretty_universe_name(c.get("anime", ""))
        rec = data.setdefault(uid, {"id": uid, "name": name, "total": 0, "free_total": 0, "premium_total": 0, "rarities": {}})
        rec["total"] += 1
        if is_super_absolute_card(c):
            rec["premium_total"] += 1
        else:
            rec["free_total"] += 1
        rarity = c.get("rarity", "Обычный")
        rec["rarities"][rarity] = int(rec["rarities"].get(rarity, 0)) + 1
    priority = {uid: idx for idx, uid in enumerate(UNIVERSE_PRIORITY)}
    return sorted(data.values(), key=lambda r: (priority.get(r["id"], 999), -int(r.get("free_total", r["total"])), r["name"]))


def universe_emoji(universe_id):
    return UNIVERSE_EMOJI.get(universe_id, "🌐")


def universe_label(universe_id):
    if not universe_id or universe_id == "all":
        return "🌌 Все мультивселенные"
    rec = UNIVERSE_BY_ID.get(universe_id) if "UNIVERSE_BY_ID" in globals() else None
    if not rec:
        return "🌌 Все мультивселенные"
    return f"{universe_emoji(universe_id)} {rec['name']}"


def selected_universe_id(player):
    uid = str((player or {}).get("preferred_universe") or "all")
    if uid != "all" and uid not in UNIVERSE_BY_ID:
        return "all"
    return uid


def _effective_universe_id(universe_id):
    if not universe_id or str(universe_id) == "all":
        return None
    uid = str(universe_id)
    return uid if uid in UNIVERSE_BY_ID else None


def universe_has_rarity(universe_id, rarity, exclude=None, allow_super_absolute=False):
    uid = _effective_universe_id(universe_id)
    exclude = set(exclude or [])
    for c in CARDS:
        if c.get("id") in exclude or c.get("rarity") != rarity:
            continue
        if uid and card_draw_universe(c) != uid:
            continue
        if not allow_super_absolute and is_super_absolute_card(c):
            continue
        return True
    return False


def universe_progress(player, universe_id):
    uid = _effective_universe_id(universe_id)
    collection = (player or {}).get("collection", {}) or {}
    if not uid:
        total = len([c for c in CARDS if not is_super_absolute_card(c)])
        owned = sum(1 for cid, info in collection.items() if cid in CARD_BY_ID and not is_super_absolute_card(CARD_BY_ID[cid]) and int((info or {}).get("count", 0) or 0) > 0 and bool((info or {}).get("unlocked", True)))
        return owned, total
    total = sum(1 for c in CARDS if card_draw_universe(c) == uid and not is_super_absolute_card(c))
    owned = 0
    for cid, info in collection.items():
        c = CARD_BY_ID.get(cid)
        if c and card_draw_universe(c) == uid and not is_super_absolute_card(c) and int((info or {}).get("count", 0) or 0) > 0 and bool((info or {}).get("unlocked", True)):
            owned += 1
    return owned, total


def card_matches_universe(card, universe_id):
    uid = _effective_universe_id(universe_id)
    return True if not uid else card_draw_universe(card) == uid


def scoped_owned_card_items(player, universe_id=None):
    uid = _effective_universe_id(universe_id)
    items = owned_card_items(player)
    if not uid:
        return items
    return [(cid, info) for cid, info in items if card_draw_universe(CARD_BY_ID.get(cid, {})) == uid]


def scoped_fragment_card_items(player, universe_id=None):
    uid = _effective_universe_id(universe_id)
    items = fragment_card_items(player)
    if not uid:
        return items
    return [(cid, info) for cid, info in items if card_draw_universe(CARD_BY_ID.get(cid, {})) == uid]


def universe_card_total(universe_id, include_super=False):
    uid = _effective_universe_id(universe_id)
    total = 0
    for c in CARDS:
        if uid and card_draw_universe(c) != uid:
            continue
        if not include_super and is_super_absolute_card(c):
            continue
        total += 1
    return total


def universe_rarity_counts(universe_id):
    uid = _effective_universe_id(universe_id)
    counts = {r: 0 for r in ["Обычный", "Редкий", "Эпический", "Легендарный", "Мифический"]}
    super_total = 0
    for c in CARDS:
        if uid and card_draw_universe(c) != uid:
            continue
        if is_super_absolute_card(c):
            super_total += 1
        else:
            counts[c.get("rarity", "Обычный")] = counts.get(c.get("rarity", "Обычный"), 0) + 1
    return counts, super_total


def universe_pool_note(rec):
    return "готов к призыву"


def load_cards():
    raw = load_json(CARDS_FILE, [])
    cards = []
    for c in raw:
        rarity = c.get("rarity", "Обычный")
        base = BASE_STATS.get(rarity, 45)
        role = c.get("role", "")
        stats = {
            "power": base + (10 if any(x in role for x in ["фронт", "burst", "разруш", "убийца", "мечник", "силовик", "танк"]) else 0),
            "speed": base + (10 if any(x in role for x in ["скорость", "ассасин", "дуэлянт", "рывок"]) else 0),
            "durability": base + (10 if any(x in role for x in ["танк", "реген", "гигант", "бессмерт"]) else 0),
            "iq": base + (12 if any(x in role for x in ["тактик", "гений", "план", "интеллект", "стратег"]) else 0),
            "hax": base + (16 if any(x in role for x in ["хакс", "контроль", "время", "псионика", "измер", "магия", "простран", "реальность"]) else 0),
            "team": base + (10 if any(x in role for x in ["саппорт", "защита", "команд", "медик"]) else 0),
        }
        # Вселенная нормализуется по anime-алиасу, но PATCH30 уважает legacy_scope:
        # старые лишние карты остаются в мультивселенной-архиве и не ломают точные счётчики выбранных миров.
        if c.get("legacy_scope") or c.get("universe") == "legacy_multiverse":
            c["universe"] = "legacy_multiverse"
            c["universe_name"] = c.get("universe_name") or "Мультивселенная · архив карт"
        else:
            alias = UNIVERSE_ALIAS_BY_ANIME.get(str(c.get("anime", "")).strip())
            if alias:
                c["universe"], c["universe_name"] = alias
            else:
                c["universe"] = c.get("universe") or _stable_universe_id(c.get("anime", ""))
                c["universe_name"] = c.get("universe_name") or _pretty_universe_name(c.get("anime", ""))
        c["premium_only"] = bool(c.get("premium_only") or is_super_absolute_card(c))
        c["stats"] = stats
        cards.append(c)
    return cards


ensure_files()
TOKEN = read_token()
CARDS = load_cards()
CARD_BY_ID = {c["id"]: c for c in CARDS}


def _base36_card_code(num):
    """Короткий стабильный код для callback_data Telegram.

    Telegram ограничивает callback_data 64 байтами, а реальные card_id могут быть
    длиннее 80 символов. Поэтому в кнопках используем короткий код, а внутри
    бота переводим его обратно в настоящий card_id. Старые длинные callback
    тоже поддерживаются: resolve_card_id() вернёт исходный id, если он есть.
    """
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    try:
        num = int(num)
    except Exception:
        num = 0
    if num <= 0:
        return "0"
    out = []
    while num:
        num, rem = divmod(num, 36)
        out.append(alphabet[rem])
    return "".join(reversed(out))


CARD_ID_TO_CB = {c["id"]: _base36_card_code(i) for i, c in enumerate(CARDS)}
CARD_CB_TO_ID = {code: cid for cid, code in CARD_ID_TO_CB.items()}


def card_cb_id(card_id):
    return CARD_ID_TO_CB.get(str(card_id), str(card_id))


def resolve_card_id(value):
    value = str(value or "")
    if value in CARD_BY_ID:
        return value
    return CARD_CB_TO_ID.get(value, value)


UNIVERSES = build_universes(CARDS)
UNIVERSE_BY_ID = {u["id"]: u for u in UNIVERSES}

# PATCH18: новый игрок получает 5 случайных Origin-карт и стартовый баланс.
# Это не трогает старых игроков: выдача идёт только при первом создании записи пользователя.
STARTER_CARD_COUNT = 5
STARTER_FISTIKS = 1500
ONBOARDING_VERSION = "PATCH40_ONBOARDING_V1"
STARTER_ATTEMPTS = 3
STARTER_SEASON_SP = 100

def random_starter_card_ids(count=STARTER_CARD_COUNT):
    pool = [c["id"] for c in CARDS if c.get("rarity") == "Обычный" and c.get("id") in CARD_BY_ID and not is_super_absolute_card(c)]
    if len(pool) < count:
        pool = [c["id"] for c in CARDS if c.get("id") in CARD_BY_ID and not is_super_absolute_card(c)]
    random.shuffle(pool)
    return pool[:max(1, min(int(count or STARTER_CARD_COUNT), len(pool)))]

DATA = load_data_storage({"users": {}, "friend_invites": {}, "friends": {}})
_loaded_saved_revision = int((DATA.get("storage_meta", {}) or {}).get("saved_revision", 0) or 0)
_DATA_LAST_SAVED_REVISION = max(_DATA_LAST_SAVED_REVISION, _loaded_saved_revision)
_DATA_REVISION = max(_DATA_REVISION, _loaded_saved_revision)
if _DATA_SCHEMA_CHANGED_ON_LOAD:
    mark_data_dirty("schema_migration_patch40")

bot = Bot(token=TOKEN)
dp = Dispatcher()



async def _delete_message_silent(message):
    try:
        await message.delete()
    except Exception as ex:
        logger.debug("Auto-clean delete failed: %s", ex)


class AutoCleanCallbackMiddleware(BaseMiddleware):
    """Чистит только навигационные окна. Награды, сундуки, кейсы и логи боя не стираются."""
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, types.CallbackQuery) and event.message:
                data_value = event.data or ""
                keep_prefixes = (
                    "noop", "pick:", "pvp_pick:", "fight_start:", "fight_next:", "pvp_start:",
                    "buy_pack:", "mega_buy:", "case_open:", "pass_claim", "pass_paid:",
                    "newbie_claim", "daily",
                    # Админские окна не удаляем автоматически: иначе кнопка «Все игроки»
                    # может стереть панель до отправки списка.
                    "admin", "admin_users", "admin_user:", "admin_ban:", "admin_unban:",
                    "admin_freeze:", "admin_unfreeze:", "admin_givef:", "admin_givemoon:",
                    "admin_delete:", "admin_storage", "admin_compensation_info",
                )
                delete_exact = {
                    "menu", "profile", "profile_stats", "profile_badges", "profile_games",
                    "modes", "shop", "chests", "rules", "multipass", "deck", "pvp_source_menu",
                    "newbie_start", "battle:start", "online_search", "cases", "events", "universe",
                    "clan", "referral", "friends", "craft", "rating", "collection:home",
                    "hub:rewards", "hub:more", "season", "season_earn", "custom_universes", "custom_backgrounds",
                }
                delete_prefixes = (
                    "pack_info:", "collection:page:", "battle:arena:", "battle:arena_page:", "battle:diff:",
                    "pvp_source:", "universe:page:", "shop_", "buy_attempts:", "buy_fistiks:", "buy_case_item:", "buy_battlepass:", "buy_privilege:", "clan_", "friend_profile:", "friend_gift:", "friend_sendgift:", "friend_remove:", "craft_attempts:",
                )
                if not data_value.startswith(keep_prefixes) and (
                    data_value in delete_exact or data_value.startswith(delete_prefixes)
                ):
                    # Не ждём удаления старого окна: Telegram API иногда отвечает медленно.
                    asyncio.create_task(_delete_message_silent(event.message))
        except Exception as ex:
            logger.debug("Auto-clean failed: %s", ex)
        return await handler(event, data)


dp.callback_query.middleware(AutoCleanCallbackMiddleware())


class PerUserSerialMiddleware(BaseMiddleware):
    """Serialize actions per user while reclaiming idle lock objects safely."""
    def __init__(self, idle_ttl=15 * 60, cleanup_interval=5 * 60):
        self._states = {}
        self._idle_ttl = float(idle_ttl)
        self._cleanup_interval = float(cleanup_interval)
        self._last_cleanup = time.monotonic()

    def _cleanup_idle(self, now=None):
        now = time.monotonic() if now is None else float(now)
        if now - self._last_cleanup < self._cleanup_interval:
            return 0
        self._last_cleanup = now
        removed = 0
        for uid, state in list(self._states.items()):
            lock = state["lock"]
            if state.get("waiters", 0) == 0 and not lock.locked() and now - state.get("last_used", now) >= self._idle_ttl:
                # Same event-loop thread: no waiter can appear between this check and pop.
                if self._states.get(uid) is state:
                    self._states.pop(uid, None)
                    removed += 1
        return removed

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)
        now = time.monotonic()
        self._cleanup_idle(now)
        uid = int(user.id)
        state = self._states.get(uid)
        if state is None:
            state = {"lock": asyncio.Lock(), "waiters": 0, "last_used": now}
            self._states[uid] = state
        state["waiters"] += 1
        try:
            async with state["lock"]:
                state["last_used"] = time.monotonic()
                return await handler(event, data)
        finally:
            state["waiters"] = max(0, int(state.get("waiters", 1)) - 1)
            state["last_used"] = time.monotonic()

    def state_count(self):
        return len(self._states)


_PER_USER_SERIAL_MIDDLEWARE = PerUserSerialMiddleware()
dp.message.middleware(_PER_USER_SERIAL_MIDDLEWARE)
dp.callback_query.middleware(_PER_USER_SERIAL_MIDDLEWARE)


class HighRiskCallbackDebounceMiddleware(BaseMiddleware):
    """Reject only rapid duplicate high-risk callbacks after per-user serialization.

    It is intentionally not a global click throttle: navigation remains responsive and
    different users are fully parallel.
    """
    EXACT = {
        "draw_card", "daily", "event_daily", "season_claim", "craft_all", "ref_claim",
        "raid_hit", "pass_claim", "newbie_claim", "luffy_claim", "admin_raid_hit",
    }
    PREFIXES = (
        "case_open:", "craft_make:", "craft_attempts:", "buy_attempts:", "buy_fistiks:",
        "buy_case_item:", "buy_battlepass:", "buy_privilege:", "pass_claim:", "newbie_claim:",
        "luffy_claim:", "raid_attack:", "season_claim:", "onboard:leader:", "friend_gift:",
    )

    def __init__(self):
        self.window = max(0.5, min(5.0, float(os.getenv("ABM_ACTION_DEBOUNCE_SECONDS", "1.25") or 1.25)))
        self._seen = {}
        self._last_cleanup = time.monotonic()

    def _is_high_risk(self, value):
        return value in self.EXACT or value.startswith(self.PREFIXES)

    def _cleanup(self, now):
        if now - self._last_cleanup < 60:
            return
        self._last_cleanup = now
        cutoff = now - max(30.0, self.window * 4)
        for key, seen_at in list(self._seen.items()):
            if seen_at < cutoff:
                self._seen.pop(key, None)

    async def __call__(self, handler, event, data):
        if not isinstance(event, types.CallbackQuery) or not event.from_user:
            return await handler(event, data)
        value = str(event.data or "")
        if not self._is_high_risk(value):
            return await handler(event, data)
        now = time.monotonic()
        self._cleanup(now)
        key = (int(event.from_user.id), value)
        previous = self._seen.get(key, 0.0)
        if now - previous < self.window:
            try:
                await event.answer("Это действие уже обрабатывается…")
            except Exception:
                pass
            return None
        self._seen[key] = now
        return await handler(event, data)


_HIGH_RISK_CALLBACK_DEBOUNCE = HighRiskCallbackDebounceMiddleware()
dp.callback_query.middleware(_HIGH_RISK_CALLBACK_DEBOUNCE)


def _tombstone_record(root_key, user_id):
    return (DATA.get(root_key, {}) or {}).get(str(user_id))


def is_permanently_deleted_id(user_id):
    uid = str(user_id)
    if uid in (DATA.get("deleted_users", {}) or {}):
        return True
    purged = (DATA.get("purged_users", {}) or {}).get(uid)
    return bool(isinstance(purged, dict) and purged.get("permanent"))


def is_technical_purge_id(user_id):
    uid = str(user_id)
    rec = (DATA.get("purged_users", {}) or {}).get(uid)
    return bool(rec and not (isinstance(rec, dict) and rec.get("permanent")))


class PermanentlyDeletedUserError(RuntimeError):
    pass


class UserTouchMiddleware(BaseMiddleware):
    """Фиксирует пользователя, но никогда не создаёт/трогает запрещённый аккаунт."""
    async def __call__(self, handler, event, data):
        try:
            user = getattr(event, "from_user", None)
            if user:
                # Defense in depth: even if middleware wrapping order changes between
                # aiogram versions, the touch layer itself checks the tombstone/ban
                # before get_user_data can create or update anything.
                if is_user_banned_id(user.id):
                    try:
                        if isinstance(event, types.CallbackQuery):
                            await event.answer("Твой доступ к боту закрыт.", show_alert=True)
                        elif isinstance(event, types.Message):
                            await event.answer("⛔ Твой доступ к боту закрыт.")
                    except Exception:
                        pass
                    return None
                player = get_user_data(user)
                data["player"] = player
                if isinstance(event, types.CallbackQuery):
                    record_user_action(user, "button:" + str(event.data or "")[:80], player=player)
                elif isinstance(event, types.Message):
                    text = (event.text or event.caption or "").strip()
                    record_user_action(user, "message:" + (text[:80] if text else "<no text>"), player=player)
        except PermanentlyDeletedUserError:
            return None
        except Exception as ex:
            logger.debug("User touch failed: %s", ex)
        return await handler(event, data)


@dp.errors()
async def global_error_handler(event: types.ErrorEvent):
    """Не даёт одной сломанной кнопке уронить polling и снимает вечный spinner."""
    ex = event.exception
    error_id = hashlib.sha1(
        f"{type(ex).__name__}:{ex}:{time.time_ns()}".encode("utf-8", errors="replace")
    ).hexdigest()[:8].upper()
    logger.error(
        "Unhandled update error [%s]: %s",
        error_id,
        ex,
        exc_info=(type(ex), ex, ex.__traceback__),
    )
    update = event.update
    try:
        if update.callback_query:
            uid = update.callback_query.from_user.id if update.callback_query.from_user else None
            if uid and is_owner(uid):
                detail = f"{type(ex).__name__}: {str(ex)[:120]}"
                await update.callback_query.answer(f"Ошибка {error_id}: {detail}", show_alert=True)
            else:
                await update.callback_query.answer(f"Ошибка {error_id}. Повтори действие через пару секунд.", show_alert=True)
        elif update.message:
            uid = update.message.from_user.id if update.message.from_user else None
            if uid and is_owner(uid):
                detail = f"{type(ex).__name__}: {str(ex)[:300]}"
                text = f"⚠️ Ошибка {error_id}\n{detail}\nПолный traceback записан в bot_runtime.log."
            else:
                text = f"⚠️ Действие не завершилось (ошибка {error_id}). Повтори через несколько секунд."
            await update.message.answer(text, reply_markup=quick_reply_menu(uid))
    except Exception:
        pass
    return True


def is_user_banned_id(user_id):
    if is_owner(user_id):
        return False
    if is_permanently_deleted_id(user_id):
        return True
    player = DATA.get("users", {}).get(str(user_id), {})
    return bool(player.get("banned", False) or player.get("frozen", False) or player.get("deleted", False))


class BanMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user and is_user_banned_id(user.id):
            try:
                if isinstance(event, types.CallbackQuery):
                    await event.answer("Твой доступ к боту закрыт.", show_alert=True)
                elif isinstance(event, types.Message):
                    await event.answer("⛔ Твой доступ к боту закрыт.")
            except Exception:
                pass
            return
        return await handler(event, data)


dp.message.middleware(BanMiddleware())
dp.callback_query.middleware(BanMiddleware())
dp.message.middleware(UserTouchMiddleware())
dp.callback_query.middleware(UserTouchMiddleware())


RARITY_CE_KEYS = {
    "Обычный": "origin",
    "Редкий": "rare",
    "Эпический": "epic",
    "Легендарный": "legendary",
    "Мифический": "absolute",
    "Сверхмифический": "super_absolute",
}
RARITY_NAMES_PUBLIC = {
    "Обычный": "Origin",
    "Редкий": "Rare",
    "Эпический": "Epic",
    "Легендарный": "Legendary",
    "Мифический": "Absolute",
    "Сверхмифический": "Super Absolute",
}

def rarity_label(rarity):
    key = RARITY_CE_KEYS.get(rarity)
    name = RARITY_NAMES_PUBLIC.get(rarity, rarity)
    if key and key in CE:
        return f"{CE[key]} {name}"
    return RARITY_DISPLAY.get(rarity, f"⚪ {rarity}")


def rarity_label_for_card(card):
    """Показывает Super Absolute отдельным знаком, не смешивая его с обычным Absolute."""
    try:
        if is_super_absolute_card(card):
            return f"{CE['super_absolute']} Super Absolute"
    except Exception:
        pass
    return rarity_label((card or {}).get("rarity", "Обычный"))

def _clean_card_field(value, fallback=""):
    value = str(value or "").strip()
    if "|" in value:
        value = value.split("|", 1)[0].strip()
    value = re.sub(r"\s+", " ", value)
    return value or fallback



def card_public_description(card):
    """Lore-first card text without collection, balance or technical boilerplate."""
    card = card or {}
    raw = re.sub(r"\s+", " ", str(card.get("description", "") or "")).strip()
    mechanical_markers = (
        "карта добавлена", "карта создана", "patch", "коллекц", "отряд", "колод",
        "уровн", "фрагмент", "пошаг", "баланс", "крафт", "призыв", "рейд",
        "игров", "world system", "тренировочный режим", "emoji=", "media/",
        "card_id", "source/free", "раскрывается через", "правильную команду",
        "боевой плюс", "боевой минус", "пул своей вселенной", "прогресс профиля",
    )
    if raw and 55 <= len(raw) <= 650 and not any(m in raw.casefold() for m in mechanical_markers):
        return raw

    name = _clean_card_field(card.get("name"), "Персонаж")
    anime = _clean_card_field(card.get("anime") or card.get("universe_name"), "своей истории")
    anime = re.sub(r"\s*[·|]\s*Архив\s*$", "", anime, flags=re.I).strip()
    form = _clean_card_field(card.get("form"), "основная версия")
    signal = " ".join(
        str(card.get(key, "") or "") for key in ("abilities", "plus", "role", "description")
    ).casefold()
    if any(word in signal for word in ("скорост", "рывок", "инициатив", "реакц", "ускор")):
        capability = "В бою эта версия делает ставку на скорость, реакцию и резкие атаки, способные мгновенно изменить ход схватки."
    elif any(word in signal for word in ("маг", "контрол", "иллюз", "прокля", "техник", "хакс", "стратег")):
        capability = "В бою персонаж использует необычные техники, контроль и точный расчёт, вынуждая противника действовать по его правилам."
    elif any(word in signal for word in ("защит", "танк", "стойк", "вынослив", "контратак")):
        capability = "Эта версия особенно опасна своей стойкостью: она выдерживает тяжёлые атаки, сохраняет темп и отвечает в решающий момент."
    elif any(word in signal for word in ("разруш", "мощ", "сила", "давлен", "удар")):
        capability = "Главная угроза этой формы — огромная мощь и разрушительные приёмы, которыми персонаж способен переломить даже безнадёжный бой."
    elif any(word in signal for word in ("интеллект", "анализ", "тактик", "план", "лидер")):
        capability = "Сильнейшая сторона этой версии — холодный расчёт, тактика и умение находить слабость противника раньше, чем тот успеет перестроиться."
    else:
        capability = "В бою эта версия сочетает характерные техники, волю и опыт, раскрывая лучшие качества персонажа в решающий момент."
    return (
        f"{name} — персонаж мира «{anime}». В форме «{form}» показан важный этап его истории, "
        f"когда характер и способности раскрываются особенно ярко. {capability}"
    )[:650]


def badge_title(code):
    return BADGE_TITLES.get(code, code.replace("_", " ").title())


def visible_badges(badges):
    return ", ".join(badge_title(b) for b in badges) if badges else "нет"


def is_public_ranked(uid):
    # Владелец скрыт из рейтингов. Заблокировавшие/удалённые/давно неактивные не засоряют топы.
    if is_owner(uid):
        return False
    p = DATA.get("users", {}).get(str(uid), {}) or {}
    name = str(p.get("name", "") or "").casefold()
    if p.get("owner_full_unlock_version") or "lone coder" in name or "лонг кодер" in name:
        return False
    if p.get("deleted") or p.get("bot_blocked"):
        return False
    if is_inactive_for_admin(uid, p):
        return False
    return True


def is_online(uid):
    player = DATA.get("users", {}).get(str(uid), {})
    last = player.get("last_seen", "")
    if not last:
        return False
    try:
        last_dt = _parse_iso_datetime(last)
        return bool(last_dt and utc_now() - last_dt <= timedelta(minutes=10))
    except Exception:
        return False



def _parse_iso_datetime(value):
    """Parse old naive and new timezone-aware ISO timestamps as aware UTC.

    PATCH40 originally mixed ``datetime.now()`` with timestamps containing
    ``+00:00``. Existing accounts then crashed on every action with
    ``TypeError: can't subtract offset-naive and offset-aware datetimes``.
    This single parser is now the compatibility boundary for all persisted
    timestamps.
    """
    try:
        if not value:
            return None
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # Legacy PATCH35/PATCH36 timestamps were local-naive. Treating them
            # as UTC is deterministic and, most importantly, prevents a second
            # timezone interpretation on every Render restart.
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def is_real_player_record(uid, player):
    """Игрок, который реально пользовался ботом, а не техническая пустышка/архив."""
    uid = str(uid)
    player = player or {}
    if not uid.isdigit():
        return False
    if player.get("deleted") or player.get("bot_blocked"):
        return False
    # ensure_admin_known_users() создаёт техническую запись владельца/правой руки.
    # Она не должна раздувать список, пока человек сам не зашёл в бота.
    if _parse_iso_datetime(player.get("last_seen", "")):
        return True
    # Старые базы могли не иметь last_seen, но иметь боевой прогресс.
    if int(player.get("battles", 0) or 0) > 0 or int(player.get("xp", 0) or 0) > 0:
        return True
    collection = player.get("collection", {}) or {}
    # Стартовые 5 карт без активности не считаем настоящим живым игроком.
    if len(collection) > 5:
        return True
    if player.get("last_actions"):
        return True
    return False


def active_player_items(include_blocked=False):
    items = []
    for uid, player in (DATA.get("users", {}) or {}).items():
        if include_blocked:
            temp = dict(player or {})
            temp["bot_blocked"] = False
            if str(uid).isdigit() and not (player or {}).get("deleted") and is_real_player_record(uid, temp):
                items.append((uid, player))
        elif is_real_player_record(uid, player):
            items.append((uid, player))
    return items


def is_inactive_for_admin(uid, player, days=30):
    """Скрывает из списка «Все игроки» старые записи, но не удаляет их из базы."""
    if str(uid) in owner_ids() or str(uid) in right_hand_ids():
        return False
    last = _parse_iso_datetime((player or {}).get("last_seen", ""))
    if not last:
        return True
    return utc_now() - last > timedelta(days=days)


def admin_live_player_items():
    items = []
    for uid, player in active_player_items(include_blocked=False):
        player = player or {}
        if player.get("deleted") or player.get("bot_blocked"):
            continue
        if is_inactive_for_admin(uid, player):
            continue
        items.append((uid, player))
    return items


def admin_blocked_count():
    return sum(1 for uid, p in (DATA.get("users", {}) or {}).items() if str(uid).isdigit() and (p or {}).get("bot_blocked"))


def admin_inactive_count():
    return sum(1 for uid, p in active_player_items(include_blocked=False) if is_inactive_for_admin(uid, p or {}))


def all_player_items(include_deleted=False, include_blocked=True):
    """Абсолютно все записи игроков из DATA["users"] для админки.

    Важно: админка не должна скрывать людей из-за last_seen, offline, bot_blocked,
    frozen или deleted. Если запись есть в базе — владелец должен её видеть.
    """
    items = []
    for uid, player in (DATA.get("users", {}) or {}).items():
        uid = str(uid)
        player = player or {}
        if not uid.isdigit():
            continue
        if not include_deleted and player.get("deleted"):
            continue
        if not include_blocked and player.get("bot_blocked"):
            continue
        items.append((uid, player))
    return items


def _iter_extra_storage_candidates():
    """Ищет все возможные сохранения вокруг /var/data и корня проекта.

    Используется только явной owner-only recovery-командой. Текущие аккаунты не
    перезаписываются backup-данными, а tombstone/purge записи никогда не воскрешаются.
    """
    folders = []
    for folder in [Path(DATA_DIR), BASE_DIR, Path('/var/data')]:
        try:
            if folder.exists() and folder.is_dir() and folder not in folders:
                folders.append(folder)
        except Exception:
            pass

    pg_data = _load_data_postgres()
    if isinstance(pg_data, dict) and isinstance(pg_data.get("users"), dict):
        yield "Neon DATABASE_URL", pg_data

    seen = set()
    for folder in folders:
        for pattern in ("anime_battle_data*.json", "*.bak", "*.backup", "*.json"):
            for path in folder.glob(pattern):
                if not path.is_file():
                    continue
                key = str(path.resolve())
                if key in seen:
                    continue
                seen.add(key)
                data = _read_json_file(path)
                if isinstance(data, dict) and isinstance(data.get("users"), dict):
                    yield path, data
        for pattern in ("anime_battle_data*.db", "*.db"):
            for path in folder.glob(pattern):
                if not path.is_file():
                    continue
                key = str(path.resolve())
                if key in seen:
                    continue
                seen.add(key)
                data = _load_data_sqlite(path)
                if isinstance(data, dict) and isinstance(data.get("users"), dict):
                    yield path, data


def _apply_recovery_candidates(candidates, save=True, merge_existing=False):
    """Apply already-read recovery candidates in the event-loop thread.

    Existing accounts are never modified unless an explicit offline-only merge override is
    enabled. Current tombstones always win over every backup snapshot.
    """
    if merge_existing and str(os.getenv("ABM_ALLOW_DANGEROUS_RECOVERY_MERGE", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
        raise RuntimeError("merge_existing is disabled in the running bot; use an offline audited tool with explicit opt-in")
    DATA.setdefault("users", {})
    DATA.setdefault("deleted_users", {})
    DATA.setdefault("purged_users", {})
    before_count = len(DATA.get("users", {}) or {})
    source_count = 0
    added_ids = []
    skipped_tombstones = 0
    for _path, candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        source_count += 1
        for uid, player in (candidate.get("users", {}) or {}).items():
            uid = str(uid)
            if uid in DATA["deleted_users"] or uid in DATA["purged_users"]:
                skipped_tombstones += 1
                continue
            if uid not in DATA["users"]:
                DATA["users"][uid] = copy.deepcopy(player)
                added_ids.append(uid)
            elif merge_existing:
                DATA["users"][uid] = _merge_player_data(DATA["users"][uid], player)
        # Safe relational sections may add only missing entries; backup tombstone sections
        # are intentionally never imported into the live authority.
        for section in ("friend_invites", "friend_requests", "friends"):
            DATA.setdefault(section, {})
            for key, value in (candidate.get(section, {}) or {}).items():
                DATA[section].setdefault(key, copy.deepcopy(value))
    changed = bool(added_ids)
    if save and changed:
        mark_data_dirty("recover_missing_users")
    return {
        "sources": source_count,
        "before": before_count,
        "after": len(DATA.get("users", {})),
        "added": len(set(added_ids)),
        "skipped_tombstones": skipped_tombstones,
        "changed": changed,
    }


def recover_users_from_all_sources(save=True, merge_existing=False):
    """Synchronous/offline recovery wrapper; never called automatically at startup."""
    candidates = [(str(path), data) for path, data in _iter_extra_storage_candidates()]
    return _apply_recovery_candidates(candidates, save=save, merge_existing=merge_existing)


async def recover_users_from_all_sources_async(save=True):
    """Read JSON/SQLite/PostgreSQL candidates off the event loop, then apply safely."""
    candidates = await asyncio.to_thread(
        lambda: [(str(path), data) for path, data in _iter_extra_storage_candidates()]
    )
    return _apply_recovery_candidates(candidates, save=save, merge_existing=False)


def repair_luffy_progress(player):
    """Не даёт Путю Луфи откатиться на 0, если карты/история уже есть в сохранении."""
    if not isinstance(player, dict):
        return False
    changed = False
    collection = player.get("collection", {}) or {}
    claimed = list(player.get("luffy_claimed_cards", []) or [])
    max_done = 0
    for i, cid in enumerate(LUFFY_PATH_CARDS, 1):
        if cid in claimed:
            max_done = max(max_done, i)
        info = collection.get(cid)
        if info and (int(info.get("count", 0) or 0) > 0 or info.get("unlocked")):
            max_done = max(max_done, i)
            if cid not in claimed:
                claimed.append(cid)
                changed = True
    old_day = int(player.get("luffy_day", 0) or 0)
    if max_done > old_day:
        player["luffy_day"] = max_done
        changed = True
    if claimed != list(player.get("luffy_claimed_cards", []) or []):
        player["luffy_claimed_cards"] = claimed
        changed = True
    if player.get("last_luffy_intro") and not player.get("luffy_intro_seen"):
        player["luffy_intro_seen"] = True
        changed = True
    if int(player.get("luffy_day", 0) or 0) >= len(LUFFY_PATH_CARDS) and not player.get("luffy_finished"):
        player["luffy_finished"] = True
        changed = True
    return changed


def repair_all_luffy_progress():
    changed = False
    for _uid, player in (DATA.get("users", {}) or {}).items():
        if repair_luffy_progress(player):
            changed = True
    if changed:
        mark_data_dirty("data_changed")
    return changed


def should_mark_bot_unreachable(ex):
    msg = str(ex).lower()
    return any(x in msg for x in [
        "bot was blocked", "bot can't initiate conversation", "chat not found",
        "user is deactivated", "forbidden", "blocked by the user",
    ])


def xp_for_next(level):
    return 120 + level * 90 + level * level * 12


def calc_user_level(xp):
    level = 1
    remain = int(xp or 0)
    while level < 100 and remain >= xp_for_next(level):
        remain -= xp_for_next(level)
        level += 1
    return level, remain, xp_for_next(level)


def add_xp(player, amount):
    # Обычный XP аккаунта. Мультипасс теперь качается через ежедневные задания.
    player["xp"] = int(player.get("xp", 0)) + int(amount)



def normalize_collection(player):
    collection = player.setdefault("collection", {})
    unknown = player.setdefault("unknown_cards", [])
    for cid in list(collection.keys()):
        item = collection[cid]
        if not isinstance(item, dict):
            collection[cid] = {"count": 1, "shards": 0, "level": 1, "unlocked": True, "unknown": cid not in CARD_BY_ID}
            item = collection[cid]
        item.setdefault("count", 0)
        item.setdefault("shards", 0)
        item.setdefault("level", 1)
        item.setdefault("duplicates", max(0, int(item.get("count", 0) or 0) - 1))
        item.setdefault("unlocked", int(item.get("count", 0) or 0) > 0)
        if cid not in CARD_BY_ID:
            item["unknown"] = True
            if cid not in unknown:
                unknown.append(cid)
            continue
        item.pop("unknown", None)
        # PATCH40: a character is never kept as an unfinished fragment-only drop.
        if int(item.get("shards", 0) or 0) > 0 and int(item.get("count", 0) or 0) <= 0:
            item["count"] = 1
            item["unlocked"] = True
        try:
            item["level"] = max(1, min(MAX_LEVEL, int(item.get("level", 1) or 1)))
        except Exception:
            item["level"] = 1
    if len(unknown) > 500:
        del unknown[:-500]



def normalize_pass_status(player):
    """PATCH18: ручной мультипасс может иметь срок. Истёкший pass снимается безопасно."""
    if not isinstance(player, dict):
        return False
    until_raw = str(player.get("pass_until", "") or "").strip()
    if not until_raw:
        return False
    try:
        until = _parse_iso_datetime(until_raw)
        now = utc_now()
        if until is None:
            return False
    except Exception:
        return False
    if now <= until:
        return False
    changed = False
    if player.get("pass_premium"):
        player["pass_premium"] = False
        changed = True
    if int(player.get("pass_premium_cap", 0) or 0) != 0:
        player["pass_premium_cap"] = 0
        changed = True
    player["pass_purchase_request"] = "expired"
    player["pass_until"] = ""
    return changed


def pass_until_label(player):
    until_raw = str((player or {}).get("pass_until", "") or "").strip()
    if not until_raw:
        return "без срока"
    try:
        until = _parse_iso_datetime(until_raw)
        if until is None:
            return until_raw
        return until.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return until_raw


def grant_manual_pass(uid, days, granted_by=None):
    uid = str(uid)
    if uid not in DATA.setdefault("users", {}):
        return None, "Игрок не найден в базе. Пусть он нажмёт /start, потом повтори."
    try:
        days = max(1, min(365, int(days)))
    except Exception:
        days = 30
    p = DATA["users"][uid]
    until = utc_now() + timedelta(days=days)
    p["pass_premium"] = True
    p["pass_premium_cap"] = 100
    p["pass_purchase_request"] = "manual_granted"
    p["pass_until"] = until.isoformat()
    p["pass_granted_at"] = utc_now().isoformat()
    p["pass_granted_by"] = str(granted_by or "owner")
    p.setdefault("system_inbox", []).append({
        "at": utc_now().isoformat(),
        "kind": "pass_granted",
        "days": days,
        "until": p["pass_until"],
    })
    mark_data_dirty("data_changed")
    return p, until


def take_manual_pass(uid, removed_by=None):
    uid = str(uid)
    if uid not in DATA.setdefault("users", {}):
        return None, "Игрок не найден в базе."
    p = DATA["users"][uid]
    p["pass_premium"] = False
    p["pass_premium_cap"] = 0
    p["pass_purchase_request"] = "manual_removed"
    p["pass_until"] = ""
    p["pass_removed_at"] = utc_now().isoformat()
    p["pass_removed_by"] = str(removed_by or "owner")
    p.setdefault("system_inbox", []).append({
        "at": utc_now().isoformat(),
        "kind": "pass_removed",
    })
    mark_data_dirty("data_changed")
    return p, None


PLAYER_LIST_LIMITS = {
    "battle_history": 200,
    "last_actions": 200,
    "support_tickets": 100,
    "system_inbox": 120,
    "purchases": 200,
    "processed_payments": 200,  # compatibility display only; root payment_ledger is never truncated
    "notifications": 120,
}


def trim_bounded_player_lists(player):
    changed = False
    for key, limit in PLAYER_LIST_LIMITS.items():
        value = player.get(key)
        if isinstance(value, list) and len(value) > limit:
            player[key] = value[-limit:]
            changed = True
    return changed


def get_user_data(user):
    uid = str(user.id)
    if is_permanently_deleted_id(uid):
        raise PermanentlyDeletedUserError(f"User {uid} is permanently deleted")
    # A bot_blocked/inactive cleanup is not a ban. After the human returns,
    # the technical purge marker is consumed and a fresh gameplay account may
    # be created. Verified payment state is carried forward, so cleanup can
    # never erase a Stars entitlement or make an old charge reusable.
    technical_purge_record = None
    if is_technical_purge_id(uid):
        technical_purge_record = copy.deepcopy(DATA.setdefault("purged_users", {}).pop(uid, None) or {})
        mark_data_dirty("technical_purge_reentry")
    now = utc_now()
    now_iso = now.isoformat()
    users = DATA.setdefault("users", {})
    changed = False
    is_new_account = uid not in users

    if is_new_account:
        users[uid] = {
            "name": user.full_name,
            "username": user.username or "",
            "fistiks": STARTER_FISTIKS,
            "xp": 0,
            "wins": 0,
            "losses": 0,
            "battles": 0,
            "last_daily": "",
            "last_free_pack": "",
            "free_pack_notified": False,
            "last_free_notice": "",
            "collection": {},
            "badges": [],
            "premium": False,
            "used_promos": [],
            "ref_by": "",
            "ref_count": 0,
            "ref_earned": 0,
            "nickname": "",
            "pass_xp": 0,
            "pass_premium": False,
            "claimed_pass_free": [],
            "claimed_pass_premium": [],
            "stars_earned": 0,
            "moon_coins": 0,
            "pity_counters": {"epic": 0, "legendary": 0, "mythic": 0},
            "notify_free_pack": True,
            "banned": False,
            "frozen": False,
            "pass_premium_cap": 0,
            "pass_until": "",
            "pass_granted_at": "",
            "pass_granted_by": "",
            "artifacts": {},
            "deck": [],
            "auto_team": True,
            "pass_daily_date": "",
            "pass_task_progress": {},
            "pass_task_claimed": [],
            "pass_purchase_request": "",
            "created_at": now_iso,
            "newbie_claimed": [],
            "newbie_progress": {},
            "pvp_team_source": "deck",
            "ref_milestones_claimed": [],
            "support_tickets": [],
            "purchases": [],
            "processed_payments": [],
            "battle_history": [],
            "last_actions": [],
            "system_inbox": [],
            "luffy_day": 0,
            "last_luffy_claim": "",
            "luffy_claimed_cards": [],
            "luffy_intro_seen": False,
            "luffy_finished": False,
            "compensations": [],
            "preferred_universe": "",
            "universe_onboarding_seen": "",
            "card_attempts": 0,
            "free_card_notified": False,
            "onboarding_version": ONBOARDING_VERSION,
            "onboarding_state": "choose_universe",
            "onboarding_complete": False,
            "starter_bundle_claimed": False,
            "starter_cards": [],
            "onboarding_leader_options": [],
            "clan_id": "",
            "title": "Новичок разлома",
            "custom_bg": "",
            "nickname_selected_once": False,
            "friend_gifts": {},
            "case_inventory": {"light": 0, "event": 0, "holiday": 0, "mystic": 0},
            "privilege": "",
        }
        # PATCH40: the starter squad is granted only after the resumable onboarding is completed.
        # This keeps the advertised economy exact: 1500 base fistiks, 3 attempts, 5 cards and 100 SP.
        if technical_purge_record:
            preserved = technical_purge_record.get("preserved_payment_state", {}) or {}
            for key in (
                "purchases", "processed_payments", "stars_earned", "premium",
                "pass_premium", "pass_premium_cap", "pass_until",
                "pass_purchase_request", "pass_granted_at", "pass_granted_by",
            ):
                if key in preserved:
                    users[uid][key] = copy.deepcopy(preserved[key])
        changed = True

    player = users[uid]

    def set_if_missing(key, value):
        nonlocal changed
        if key not in player:
            player[key] = copy.deepcopy(value)
            changed = True

    def set_if_changed(key, value):
        nonlocal changed
        if player.get(key) != value:
            player[key] = value
            changed = True

    if "fistiks" not in player:
        player["fistiks"] = player.get("coins", STARTER_FISTIKS)
        changed = True

    defaults = {
        "xp": 0, "badges": [], "premium": False, "used_promos": [], "last_daily": "",
        "last_free_pack": "", "free_pack_notified": False, "last_free_notice": "",
        "ref_by": "", "ref_count": 0, "ref_earned": 0, "nickname": "",
        "wins": 0, "losses": 0, "battles": 0, "last_seen": "",
        "pass_xp": 0, "pass_premium": False, "claimed_pass_free": [], "claimed_pass_premium": [],
        "stars_earned": 0, "moon_coins": 0, "pity_counters": {"epic": 0, "legendary": 0, "mythic": 0},
        "notify_free_pack": True, "banned": False, "frozen": False, "pass_premium_cap": 0,
        "pass_until": "", "pass_granted_at": "", "pass_granted_by": "", "artifacts": {},
        "deck": [], "auto_team": True, "pass_daily_date": "", "pass_task_progress": {},
        "pass_task_claimed": [], "pass_purchase_request": "", "created_at": now_iso,
        "newbie_claimed": [], "newbie_progress": {}, "pvp_team_source": "deck",
        "ref_milestones_claimed": [], "support_tickets": [], "purchases": [], "processed_payments": [], "battle_history": [],
        "last_actions": [], "system_inbox": [], "luffy_day": 0, "last_luffy_claim": "", "luffy_claimed_cards": [],
        "luffy_intro_seen": False, "luffy_finished": False,
        "compensations": [], "username": user.username or "", "preferred_universe": "", "universe_onboarding_seen": "",
        "card_attempts": 0, "free_card_notified": False, "clan_id": "", "title": "Новичок разлома",
        "custom_bg": "", "nickname_selected_once": False, "friend_gifts": {},
        "case_inventory": {"light": 0, "event": 0, "holiday": 0, "mystic": 0}, "privilege": "",
    }
    for k, v in defaults.items():
        set_if_missing(k, v)
    # Existing accounts are grandfathered as completed and never receive a second starter bundle.
    # Only an account created in this very call enters PATCH40 onboarding.
    set_if_missing("onboarding_version", ONBOARDING_VERSION)
    set_if_missing("onboarding_complete", False if is_new_account else True)
    set_if_missing("onboarding_state", "choose_universe" if is_new_account else "complete")
    set_if_missing("starter_bundle_claimed", False if is_new_account else True)
    set_if_missing("starter_cards", [])
    set_if_missing("onboarding_leader_options", [])
    set_if_missing("season_id", "")
    set_if_missing("season_xp", 0)
    set_if_missing("season_claimed", [])
    set_if_missing("season_action_keys", [])
    set_if_missing("daily_streak", 0)
    set_if_missing("collection", {})
    set_if_missing("artifacts", {})
    set_if_missing("pity_counters", {"epic": 0, "legendary": 0, "mythic": 0})
    if trim_bounded_player_lists(player):
        changed = True

    if normalize_pass_status(player):
        changed = True

    desired_name = player.get("nickname") or user.full_name
    set_if_changed("name", desired_name)
    desired_username = user.username or player.get("username", "")
    set_if_changed("username", desired_username)

    last_seen_dt = _parse_iso_datetime(player.get("last_seen", ""))
    if not last_seen_dt or (now - last_seen_dt).total_seconds() >= LAST_SEEN_SAVE_SECONDS:
        set_if_changed("last_seen", now_iso)

    if repair_luffy_progress(player):
        changed = True

    # PATCH32 SPEED: полная нормализация коллекции владельца/игрока не нужна на каждом клике.
    # Она дорогая при огромной коллекции и выполняется один раз на версию патча.
    if player.get("collection_normalized_version") != COLLECTION_NORMALIZATION_VERSION:
        normalize_collection(player)
        player["collection_normalized_version"] = COLLECTION_NORMALIZATION_VERSION
        changed = True

    if normalize_artifact_inventory(player):
        changed = True
    ensure_rpg_fields(player)

    # PATCH40.1: отдельная покупка кейсов удалена. Уже купленные/полученные
    # кейсы не пропадают — каждый один раз превращается в одну попытку.
    if not player.get("case_inventory_migrated_to_attempts_v1"):
        inv = player.setdefault("case_inventory", {"light": 0, "event": 0, "holiday": 0, "mystic": 0})
        legacy_cases = sum(max(0, int(inv.get(code, 0) or 0)) for code in ("light", "event", "holiday", "mystic"))
        if legacy_cases:
            player["card_attempts"] = max(0, int(player.get("card_attempts", 0) or 0)) + legacy_cases
        player["case_inventory"] = {"light": 0, "event": 0, "holiday": 0, "mystic": 0}
        player["case_inventory_migrated_to_attempts_v1"] = True
        changed = True

    if not is_owner(user.id) and "RIGHT_HAND" in player.get("badges", []):
        player["badges"] = [b for b in player.get("badges", []) if b != "RIGHT_HAND"]
        changed = True
    if not is_owner(user.id) and not is_right_hand(user.id):
        legacy_title = str(player.get("title", "") or "").casefold()
        if "правая рука" in legacy_title or "right hand" in legacy_title:
            player["title"] = "Новичок разлома"
            changed = True

    if is_owner(user.id):
        owner_values = {
            "fistiks": 999999999,
            "losses": 0,
            "premium": True,
            "pass_premium": True,
            "pass_premium_cap": 100,
            "moon_coins": 999999999,
            "banned": False,
            "frozen": False,
            "creator_role": "👑 Владелец мультивселенной",
            "creator_aura": "♾ Абсолютный знак создателя",
        }
        for k, v in owner_values.items():
            set_if_changed(k, v)
        for k, min_value in [("wins", 9999), ("battles", 9999), ("xp", 99999999), ("pass_xp", 999999)]:
            try:
                if int(player.get(k, 0) or 0) < min_value:
                    player[k] = min_value
                    changed = True
            except Exception:
                player[k] = min_value
                changed = True
        for badge in ["DEV", "ROMA_OWNER", "IT_ARCHITECT", "ABSOLUTE_MAX"]:
            if badge not in player["badges"]:
                player["badges"].append(badge)
                changed = True

        # Не пересобираем все тысячи карт владельца на каждой кнопке.
        if player.get("owner_full_unlock_version") != OWNER_FULL_UNLOCK_VERSION:
            collection = player.setdefault("collection", {})
            if len(collection) < len(CARD_BY_ID):
                for cid in CARD_BY_ID:
                    info = collection.get(cid)
                    if not isinstance(info, dict) or int(info.get("level", 0) or 0) < MAX_LEVEL or not info.get("unlocked"):
                        collection[cid] = {"count": 1, "shards": 999999, "level": MAX_LEVEL, "unlocked": True}
                        changed = True
            artifacts = player.setdefault("artifacts", {})
            if len(artifacts) < len(ARTIFACT_BY_ID):
                for aid, art in ARTIFACT_BY_ID.items():
                    cur = artifacts.get(aid, {}) if isinstance(artifacts.get(aid, {}), dict) else {}
                    if int(cur.get("level", 0) or 0) < MAX_LEVEL:
                        artifacts[aid] = {
                            "count": max(1, int(cur.get("count", 0) or 0)),
                            "level": MAX_LEVEL,
                            "rarity": art.get("rarity", "Обычный"),
                            "name": art.get("name", aid),
                        }
                        changed = True
            player["owner_full_unlock_version"] = OWNER_FULL_UNLOCK_VERSION
            changed = True
        if not player.get("onboarding_complete"):
            player["onboarding_complete"] = True
            player["onboarding_state"] = "complete"
            player["starter_bundle_claimed"] = True
            changed = True
    elif is_right_hand(user.id):
        if "RIGHT_HAND" not in player["badges"]:
            player["badges"].append("RIGHT_HAND")
            changed = True

    if changed:
        mark_data_dirty("user_update_throttled")
    return player


def quick_reply_menu(user_id=None):
    rows = [
        [types.KeyboardButton(text="🎴 Призвать"), types.KeyboardButton(text="🃏 Коллекция")],
        [types.KeyboardButton(text="⚔️ Битвы"), types.KeyboardButton(text="🏠 Меню")],
    ]
    return types.ReplyKeyboardMarkup(
        keyboard=rows, resize_keyboard=True,
        input_field_placeholder="Выбери действие", selective=False,
    )



def quick_keyboard_note_seen(player):
    if player.get("quick_keyboard_note_seen") == PATCH_VERSION:
        return True
    player["quick_keyboard_note_seen"] = PATCH_VERSION
    mark_data_dirty("data_changed")
    return False


async def ensure_quick_keyboard(message, user):
    player = get_user_data(user)
    if player.get("quick_keyboard_note_seen") == PATCH_VERSION:
        return
    player["quick_keyboard_note_seen"] = PATCH_VERSION
    mark_data_dirty("data_changed")
    try:
        await message.answer("⌨️ Быстрая панель включена.", reply_markup=quick_reply_menu(user.id))
    except Exception:
        pass


def progress_bar(current, total, width=10):
    try:
        current = max(0, int(current or 0))
        total = max(1, int(total or 1))
        filled = max(0, min(width, round(width * current / total)))
    except Exception:
        filled = 0
    return "▰" * filled + "▱" * (width - filled)


def compact_wait_label(minutes):
    minutes = max(0, int(minutes or 0))
    if minutes <= 0:
        return "ГОТОВ"
    hours, mins = divmod(minutes, 60)
    return f"{hours}ч {mins:02d}м" if hours else f"{mins}м"



def main_menu(user_id=None):
    rows = [
        [button(text="🎴 Призвать", callback_data="draw_card"), button(text="🃏 Коллекция", callback_data="collection:home")],
        [button(text="⚔️ Битвы", callback_data="modes"), button(text="🎪 События", callback_data="events")],
        [button(text="🎁 Награды", callback_data="hub:rewards"), button(text="👤 Профиль", callback_data="profile")],
        [button(text="✨ Ещё", callback_data="hub:more")],
    ]
    if user_id and is_owner(user_id):
        rows.append([button(text="👑 Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)




def main_menu_text(user=None):
    name, universe, clan, role = "Игрок", "🌌 Все мультивселенные", "Нет клана", "Игрок"
    fistiks = dragonite = power = summon_attempts = 0
    p = {}
    if user:
        p = DATA.get("users", {}).get(str(user.id), {})
        ensure_rpg_fields(p)
        name = p.get("nickname") or p.get("name") or user.full_name or "Игрок"
        uid = selected_universe_id(p)
        universe = universe_label(uid)
        clan = clan_name_for(p)
        summon_attempts = available_attempts(p)
        fistiks = int(p.get("fistiks", 0) or 0)
        dragonite = int(p.get("moon_coins", 0) or 0)
        power = battle_power_label(p)
        role = "Владелец мультивселенной" if is_owner(user.id) else player_title(p)
    return (
        f"{CE['start']} <b>ANIME BATTLE MULTIVERSE</b>\n"
        "<i>Твой следующий призыв может открыть легенду. Собери любимых героев, усиливай их дубликатами и докажи, что твоя пятёрка сильнейшая во всей мультивселенной.</i>\n\n"
        "<blockquote>"
        f"👤 <b>{e(name)}</b> · {e(role)}\n"
        f"🌌 Аниме-мир: <b>{e(universe)}</b>\n"
        f"🏰 Клан: <b>{e(clan)}</b>\n"
        f"⚔️ Сила отряда: <b>{short_number(power)}</b>\n"
        f"🎴 Призывы: <b>{short_number(summon_attempts)}</b>\n"
        f"{CE['pistachios']} <b>{short_number(fistiks)}</b> · {CE['dragonite']} <b>{short_number(dragonite)}</b>"
        "</blockquote>"
    )



async def send_main_dashboard(message, user, show_banner=False):
    text = main_menu_text(user)
    markup = main_menu(user.id)
    if show_banner:
        try:
            banner = await asyncio.to_thread(make_ui_banner, "main")
            if banner:
                await message.answer_photo(FSInputFile(banner), caption=text, reply_markup=markup, parse_mode="HTML")
                return
        except Exception as ex:
            logger.debug("Main banner failed: %s", ex)
    await message.answer(text, reply_markup=markup, parse_mode="HTML")



async def send_rewards_hub(message, user):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    daily_ready = p.get("last_daily") != app_today_iso()
    pass_level = min(100, pass_level_from_xp(int(p.get("pass_xp", 0) or 0)))
    sinfo, changed = ensure_current_season(p)
    if changed:
        mark_data_dirty("season_rollover")
    text = (
        f"{CE['rewards']} <b>ЦЕНТР НАГРАД</b>\n"
        "<i>Забирай всё доступное — каждый бонус приближает новую карту и усиливает сезон.</i>\n\n"
        f"<blockquote>🎴 Призвать: <b>{e(draw_status(p)['detail'])}</b>\n"
        f"🎁 Ежедневная: <b>{'доступна' if daily_ready else 'получена'}</b>\n"
        f"🌌 Сезон: <b>{short_number(p.get('season_xp', 0))} SP</b> · {e(season_time_left_label(sinfo))}\n"
        f"🎟 MultiPass: <b>{pass_level}/100</b>\n"
        f"🧰 Кейсы: <b>{sum(int(v or 0) for v in p.get('case_inventory', {}).values())}</b></blockquote>"
    )
    rows = [
        [button(text="🎁 Ежедневная", callback_data="daily"), button(text="🎴 Бесплатный призыв", callback_data="draw_card")],
        [button(text="🌌 Сезон", callback_data="season"), button(text="🎟 MultiPass", callback_data="multipass")],
        [button(text="🧰 Кейсы", callback_data="cases"), button(text="🔥 Путь Луфи", callback_data="luffy_path")],
        [button(text="🎟 Промокод", callback_data="promo_help")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")




async def send_more_hub(message, user):
    p = get_user_data(user)
    text = (
        f"{CE['more']} <b>ДОПОЛНИТЕЛЬНЫЕ РАЗДЕЛЫ</b>\n\n"
        f"Текущий мир: <b>{e(universe_label(selected_universe_id(p)))}</b>\n"
        "Здесь собраны развитие аккаунта, общение и магазин — без перегрузки главного экрана."
    )
    rows = [
        [button(text="⚒️ Крафт", callback_data="craft"), button(text="🏪 Магазин", callback_data="shop")],
        [button(text="🏆 Рейтинг", callback_data="rating"), button(text="🏰 Клан", callback_data="clan")],
        [button(text="👥 Друзья", callback_data="friends"), button(text="🔗 Рефералка", callback_data="referral")],
        [button(text="📜 Правила", callback_data="rules")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")



@dp.callback_query(F.data == "hub:rewards")
async def rewards_hub_cb(callback: types.CallbackQuery):
    await send_rewards_hub(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "hub:more")
async def more_hub_cb(callback: types.CallbackQuery):
    await send_more_hub(callback.message, callback.from_user)
    await callback.answer()


async def maybe_send_luffy_intro(message, user, force=False):
    """Показывает вступление Пути Луфи один раз за аккаунт. Прогресс не трогает."""
    p = get_user_data(user)
    repair_luffy_progress(p)
    if p.get("luffy_finished") or int(p.get("luffy_day", 0) or 0) >= len(LUFFY_PATH_CARDS):
        p["luffy_finished"] = True
        mark_data_dirty("data_changed")
        return
    if not force and (p.get("luffy_intro_seen") or p.get("last_luffy_intro")):
        return
    today = app_now().date().isoformat()
    p["luffy_intro_seen"] = True
    p["last_luffy_intro"] = today
    mark_data_dirty("data_changed")
    await message.answer(
        f"{CE['luffy']} <b>Путь Монки Д. Луфи открыт</b>\n\n"
        "Личная цепочка на 10 дней: от первой формы до Gear 5 / Sun God Nika. "
        "Открой раздел один раз — дальше бот не будет навязывать его каждый день.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🔥 Открыть Луфи", callback_data="luffy_path")],
            [button(text="⬅️ Меню", callback_data="menu")],
        ]),
        parse_mode="HTML"
    )

def back_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [button(text="⬅️ Назад", callback_data="menu")]
    ])


# PATCH23 RPG HUB HELPERS
FREE_CARD_WAIT_HOURS = 3
PATCH24_COMPENSATION_KEY = "patch24_polish_rpg_2026_05_08"
PATCH24_COMPENSATION_FISTIKS = 4500
PATCH24_COMPENSATION_DRAGONITE = 1
PATCH24_COMPENSATION_ATTEMPTS = 10
PATCH24_COMPENSATION_PASS_XP = 1100

# Backward-compatible names inside older helper code, but all admin text/command is PATCH24.
PATCH23_COMPENSATION_KEY = PATCH24_COMPENSATION_KEY
PATCH23_COMPENSATION_FISTIKS = PATCH24_COMPENSATION_FISTIKS
PATCH23_COMPENSATION_DRAGONITE = PATCH24_COMPENSATION_DRAGONITE
PATCH23_COMPENSATION_ATTEMPTS = PATCH24_COMPENSATION_ATTEMPTS
PATCH23_COMPENSATION_PASS_XP = PATCH24_COMPENSATION_PASS_XP

TITLE_BY_UNIVERSE = {
    "jojo": "Реликвия", "one_piece": "Король пиратов", "demon_slayer": "Столб", "jujutsu_kaisen": "Сильнейший маг",
    "death_note": "Бог смерти", "chainsaw_man": "Охотник на демонов", "hells_paradise": "Пустой ниндзя", "fate": "Герой престола",
    "pokemon": "Тренер лиги", "seven_deadly_sins": "Грех", "sao": "Чёрный мечник", "rezero": "Вернувшийся смертью",
    "evangelion": "Пилот Евы", "tower_of_god": "Регуляр Башни", "kuroko_basket": "В потоке", "attack_on_titan": "Титан",
    "black_clover": "Король магов", "monster": "Психолог", "code_geass": "Император Гиаса", "hajime_no_ippo": "Чемпион ринга",
    "blue_lock": "Эгоист", "tokyo_ghoul": "Гуль", "fairy_tail": "Маг Хвоста Феи", "naruto_boruto": "Хокаге",
    "hellsing": "Носферату", "berserk": "Чёрный мечник", "fire_force": "Пламенный герой", "haikyuu": "Маленький гигант",
    "fullmetal_alchemist": "Философский камень", "my_hero_academia": "Один за всех", "one_punch_man": "Герой класса S",
    "vinland_saga": "Воин Винланда", "frieren": "Маг тысячелетия", "baki": "Сильнейший боец", "record_of_ragnarok": "Воин Рагнарёка",
    "solo_leveling": "Сильнейший монарх", "dragon_ball": "Суперсайян", "bleach": "Король душ", "hunter_x_hunter": "Охотник",
    "tensei_slime": "Повелитель демонов", "classroom_elite": "Белая комната", "tokyo_revengers": "Мститель времени", "mob_psycho_100": "100%",
}

PRIVILEGES = {
    "vip": {"title": "VIP", "icon": "💠", "cost": 299, "wait_minutes": 180, "boost": "x2", "attempts": 15, "fistiks": 10000},
    "elite": {"title": "Elite", "icon": "🔥", "cost": 599, "wait_minutes": 160, "boost": "x2.5", "attempts": 25, "fistiks": 25000},
    "mythic": {"title": "Mythic", "icon": "🧿", "cost": 999, "wait_minutes": 120, "boost": "x3", "attempts": 45, "fistiks": 45000},
    "overlord": {"title": "Overlord", "icon": "⚡", "cost": 2999, "wait_minutes": 105, "boost": "x4", "attempts": 70, "fistiks": 225000},
    "absolute": {"title": "Absolute", "icon": "👑", "cost": 4999, "wait_minutes": 90, "boost": "x5", "attempts": 100, "fistiks": 450000},
}

ATTEMPT_PACKS = [(5, 1200), (10, 2400), (25, 6000), (50, 12000), (500, 120000), (2500, 600000)]
DRAGONITE_ATTEMPT_PACKS = [(5, 30), (10, 50), (30, 140), (100, 489), (500, 2445)]
FISTIK_PACKS = [(1000, 25), (3000, 70), (10000, 200), (25000, 500), (100000, 2000)]
CASE_SHOP_PACKS = [("light", "Лайт-кейс", 30), ("event", "Ивент-кейс", 70), ("holiday", "Праздничный кейс", 100), ("mystic", "Мистик-кейс", 200)]
BATTLE_PASS_PACKS = [("elite", "Элитный пропуск", 149), ("master", "Мастер-пропуск", 439), ("full", "Полный пропуск", 449)]



def ensure_rpg_fields(player):
    player.setdefault("card_attempts", 0)
    player.setdefault("free_card_notified", False)
    player.setdefault("clan_id", "")
    player.setdefault("title", "Новичок разлома")
    player.setdefault("custom_bg", "")
    player.setdefault("nickname_selected_once", False)
    player.setdefault("friend_gifts", {})
    player.setdefault("case_inventory", {"light": 0, "event": 0, "holiday": 0, "mystic": 0})
    player.setdefault("privilege", "")
    player.setdefault("equipped_artifact", "")
    return player



def free_card_wait_minutes(player):
    ensure_rpg_fields(player)
    last = player.get("last_free_pack", "")
    if not last:
        return 0
    try:
        last_dt = _parse_iso_datetime(last)
        if last_dt is None:
            return 0
        wait_until = last_dt + timedelta(hours=FREE_CARD_WAIT_HOURS)
    except Exception:
        return 0
    seconds = int((wait_until - utc_now()).total_seconds())
    return max(0, (seconds + 59) // 60)


def format_wait_hms(minutes):
    seconds = max(0, int(minutes) * 60)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h} ч {m:02d} мин {s:02d} сек" if h else f"{m} мин {s:02d} сек"


def short_number(value, digits=1):
    """Красивое сокращение больших чисел для UI: 1.2K / 99.9M / 1B."""
    try:
        n = float(value or 0)
    except Exception:
        return "0"
    sign = "-" if n < 0 else ""
    n = abs(n)
    units = [(1_000_000_000_000, "T"), (1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")]
    for limit, suffix in units:
        if n >= limit:
            val = int((n / limit) * 10) / 10
            if abs(val - int(val)) < 0.00001:
                label = str(int(val))
            else:
                label = f"{val:.{digits}f}"
            return f"{sign}{label}{suffix}"
    return f"{sign}{int(n)}"


def available_attempts(player):
    ensure_rpg_fields(player)
    free = 1 if free_card_wait_minutes(player) <= 0 else 0
    paid = int(player.get("card_attempts", 0) or 0)
    return free + paid


def draw_status(player):
    """Single source of truth for every summon status label."""
    ensure_rpg_fields(player)
    paid = max(0, int(player.get("card_attempts", 0) or 0))
    wait = free_card_wait_minutes(player)
    if paid > 0:
        return {"available": True, "label": f"ГОТОВ · +{paid}", "detail": f"доступен · доп. попыток {paid}", "wait_minutes": wait, "paid": paid}
    if wait <= 0:
        return {"available": True, "label": "ГОТОВ", "detail": "бесплатный призыв доступен", "wait_minutes": 0, "paid": 0}
    return {"available": False, "label": compact_wait_label(wait), "detail": f"через {compact_wait_label(wait)}", "wait_minutes": wait, "paid": 0}


def consume_summon_attempt(player, user_id):
    """Atomically consume exactly one free or additional summon attempt.

    The caller must already be inside PerUserSerialMiddleware.  No ``await`` is
    performed between availability check and mutation, so two rapid callbacks
    cannot spend one attempt twice.
    """
    ensure_rpg_fields(player)
    if is_owner(user_id):
        return True, "режим владельца", 0
    wait = free_card_wait_minutes(player)
    paid = max(0, int(player.get("card_attempts", 0) or 0))
    if wait <= 0:
        player["last_free_pack"] = utc_now().isoformat()
        player["free_pack_notified"] = False
        return True, "бесплатный призыв", 0
    if paid > 0:
        player["card_attempts"] = paid - 1
        return True, "дополнительная попытка", wait
    return False, "", wait


def consume_summon_attempts(player, user_id, count):
    """Consume several summon attempts all-or-nothing, without awaits."""
    ensure_rpg_fields(player)
    count = max(1, min(100, int(count or 1)))
    if is_owner(user_id):
        return True, "режим владельца", 0
    wait = free_card_wait_minutes(player)
    paid = max(0, int(player.get("card_attempts", 0) or 0))
    free = 1 if wait <= 0 else 0
    if free + paid < count:
        return False, "", wait
    remaining = count
    sources = []
    if free:
        player["last_free_pack"] = utc_now().isoformat()
        player["free_pack_notified"] = False
        remaining -= 1
        sources.append("бесплатный")
    if remaining:
        player["card_attempts"] = paid - remaining
        sources.append(f"дополнительные ×{remaining}")
    return True, " + ".join(sources), wait


# PATCH40: a season is independent from the deployed patch and survives restarts.
SEASON_LENGTH_DAYS = 28
SEASON_EPOCH_RAW = (os.getenv("SEASON_EPOCH") or "2026-07-13T00:00:00+00:00").strip()
SEASON_HISTORY_LIMIT = max(2, min(24, int(os.getenv("ABM_SEASON_HISTORY_LIMIT", "8") or 8)))
SEASON_ACTION_KEY_LIMIT = max(100, min(5000, int(os.getenv("ABM_SEASON_ACTION_KEY_LIMIT", "1200") or 1200)))
SEASON_FEATURED_CARD_ID = "goku_mui_complete"
SEASON_NAMES = [
    "Разлом миров", "Восхождение легенд", "Битва измерений", "Эхо абсолютов",
    "Предел силы", "Пробуждение героев", "Война вселенных", "Наследие титанов",
]
SEASON_REWARDS = (
    {"sp": 100, "kind": "fistiks", "amount": 800, "label": "💎 800 фисташек"},
    {"sp": 250, "kind": "attempts", "amount": 1, "label": "🎴 +1 попытка"},
    {"sp": 500, "kind": "dragonite", "amount": 1, "label": "🐉 +1 драконит"},
    {"sp": 900, "kind": "fistiks", "amount": 1500, "label": "💎 1 500 фисташек"},
    {"sp": 1400, "kind": "badge", "value": "SEASON_RIFT_WALKER", "label": "🏷 Значок «Странник разлома»"},
    {"sp": 2200, "kind": "attempts", "amount": 3, "label": "🎴 +3 попытки"},
    {"sp": 3200, "kind": "card", "card_id": SEASON_FEATURED_CARD_ID, "label": "🃏 Сезонная награда-карта"},
    {"sp": 4500, "kind": "final", "fistiks": 3500, "dragonite": 2, "title": "Покоритель сезона", "label": "👑 Финал: 3 500 💎 + 2 🐉 + титул"},
)
SEASON_XP_REWARDS = {
    "draw": 40,
    "daily": 60,
    "craft": 30,
    "solo_battle": 80,
    "pvp_win": 120,
    "pvp_loss": 70,
    "event": 50,
    "raid": 40,
    "case": 25,
}


def app_timezone():
    try:
        return ZoneInfo(APP_TIMEZONE_NAME)
    except Exception:
        return timezone.utc


def app_now():
    return utc_now().astimezone(app_timezone())


def app_today_iso():
    return app_now().date().isoformat()


def _parse_aware_utc(value, fallback):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return fallback


def season_info(now=None):
    now = now or utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    epoch = _parse_aware_utc(SEASON_EPOCH_RAW, datetime(2026, 7, 13, tzinfo=timezone.utc))
    length = timedelta(days=SEASON_LENGTH_DAYS)
    if now < epoch:
        number = 1
        start = epoch
    else:
        number = int((now - epoch) // length) + 1
        start = epoch + (number - 1) * length
    end = start + length
    name = SEASON_NAMES[(number - 1) % len(SEASON_NAMES)]
    return {
        "id": f"S{number:04d}_{start.strftime('%Y%m%d')}",
        "number": number,
        "name": name,
        "start": start,
        "end": end,
        "seconds_left": max(0, int((end - now).total_seconds())),
    }


def season_time_left_label(info=None):
    info = info or season_info()
    seconds = max(0, int(info.get("seconds_left", 0) or 0))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}д {hours:02d}ч"
    if hours:
        return f"{hours}ч {minutes:02d}м"
    return f"{minutes}м"


def ensure_current_season(player):
    """Idempotently switch only season-local fields; never touch MultiPass or legacy progress."""
    info = season_info()
    current = str(player.get("season_id", "") or "")
    changed = False
    if current != info["id"]:
        if current:
            history = player.setdefault("season_history", [])
            history.append({
                "season_id": current,
                "season_xp": max(0, int(player.get("season_xp", 0) or 0)),
                "claimed": list(player.get("season_claimed", []) or []),
                "closed_at": utc_now().isoformat(),
            })
            if len(history) > SEASON_HISTORY_LIMIT:
                del history[:-SEASON_HISTORY_LIMIT]
        player["season_id"] = info["id"]
        player["season_xp"] = 0
        player["season_claimed"] = []
        player["season_action_keys"] = []
        player["season_started_at"] = info["start"].isoformat()
        changed = True
    player.setdefault("season_xp", 0)
    player.setdefault("season_claimed", [])
    player.setdefault("season_action_keys", [])
    return info, changed


def add_season_xp(player, amount, action_key=None):
    info, switched = ensure_current_season(player)
    amount = max(0, min(100000, int(amount or 0)))
    if amount <= 0:
        if switched:
            mark_data_dirty("season_rollover")
        return 0
    if action_key:
        key = f"{info['id']}:{str(action_key)[:180]}"
        keys = player.setdefault("season_action_keys", [])
        if key in keys:
            if switched:
                mark_data_dirty("season_rollover")
            return 0
        keys.append(key)
        if len(keys) > SEASON_ACTION_KEY_LIMIT:
            del keys[:-SEASON_ACTION_KEY_LIMIT]
    player["season_xp"] = max(0, int(player.get("season_xp", 0) or 0)) + amount
    mark_data_dirty("season_xp")
    return amount


def season_progress_target(sp):
    for reward in SEASON_REWARDS:
        if sp < int(reward["sp"]):
            return int(reward["sp"])
    return int(SEASON_REWARDS[-1]["sp"])


def _grant_season_reward(player, reward):
    kind = reward.get("kind")
    if kind == "fistiks":
        player["fistiks"] = int(player.get("fistiks", 0) or 0) + int(reward.get("amount", 0) or 0)
    elif kind == "attempts":
        player["card_attempts"] = int(player.get("card_attempts", 0) or 0) + int(reward.get("amount", 0) or 0)
    elif kind == "dragonite":
        player["moon_coins"] = int(player.get("moon_coins", 0) or 0) + int(reward.get("amount", 0) or 0)
    elif kind == "badge":
        badge = str(reward.get("value", "") or "")
        if badge and badge not in player.setdefault("badges", []):
            player["badges"].append(badge)
    elif kind == "card":
        cid = str(reward.get("card_id", "") or "")
        if cid not in CARD_BY_ID:
            return False, "карта награды временно недоступна"
        # PATCH40: even a season reward is a full character; a repeated reward
        # follows the single duplicate-to-fragments rule used by every draw.
        add_card(player, cid)
    elif kind == "final":
        player["fistiks"] = int(player.get("fistiks", 0) or 0) + int(reward.get("fistiks", 0) or 0)
        player["moon_coins"] = int(player.get("moon_coins", 0) or 0) + int(reward.get("dragonite", 0) or 0)
        title = str(reward.get("title", "") or "")
        if title:
            player["title"] = title
    else:
        return False, "неизвестная награда"
    return True, str(reward.get("label", "Награда"))


def claim_available_season_rewards(player):
    info, switched = ensure_current_season(player)
    sp = max(0, int(player.get("season_xp", 0) or 0))
    claimed = player.setdefault("season_claimed", [])
    granted = []
    for idx, reward in enumerate(SEASON_REWARDS, 1):
        claim_key = str(idx)
        if claim_key in claimed or sp < int(reward["sp"]):
            continue
        ok, label = _grant_season_reward(player, reward)
        if not ok:
            continue
        claimed.append(claim_key)
        granted.append(label)
    if granted or switched:
        mark_data_dirty("season_claim")
    return info, granted


def season_rank_rows():
    info = season_info()
    rows = []
    for uid, player in DATA.get("users", {}).items():
        if is_owner(uid) or is_permanently_deleted_id(uid):
            continue
        if player.get("banned") or player.get("frozen") or player.get("deleted"):
            continue
        if str(player.get("season_id", "") or "") != info["id"]:
            continue
        rows.append((str(uid), max(0, int(player.get("season_xp", 0) or 0)), player))
    rows.sort(key=lambda item: (-item[1], str(item[2].get("name", "")).casefold(), item[0]))
    return rows


def season_screen_text(player):
    info, changed = ensure_current_season(player)
    if changed:
        mark_data_dirty("season_rollover")
    sp = max(0, int(player.get("season_xp", 0) or 0))
    claimed = set(str(x) for x in player.get("season_claimed", []) or [])
    target = season_progress_target(sp)
    featured = CARD_BY_ID.get(SEASON_FEATURED_CARD_ID, {})
    featured_name = featured.get("name") or "Секретная карта сезона"
    lines = [
        f"🌌 <b>СЕЗОН {info['number']} · {e(info['name'].upper())}</b>",
        f"⏳ До конца: <b>{e(season_time_left_label(info))}</b>",
        f"🌟 Главная карта: <b>{e(featured_name)}</b>",
        f"⚡ SP: <b>{short_number(sp)}</b> · {progress_bar(sp, target, 12)}",
        "",
        "<b>Награды:</b>",
    ]
    for idx, reward in enumerate(SEASON_REWARDS, 1):
        threshold = int(reward["sp"])
        if str(idx) in claimed:
            state = "✅"
        elif sp >= threshold:
            state = "🎁"
        else:
            state = "🔒"
        lines.append(f"{state} <b>{threshold} SP</b> — {e(reward['label'])}")
    lines.append("\n<i>Сезон и MultiPass — отдельные системы. Перезапуск бота не сбрасывает сезон.</i>")
    return "\n".join(lines)


async def send_season_screen(message, user):
    player = get_user_data(user)
    text = season_screen_text(player)
    info = season_info()
    rows = [
        [button(text="🎁 Забрать доступные", callback_data="season_claim")],
        [button(text="⚔️ Как заработать SP", callback_data="season_earn"), button(text="🏆 Рейтинг", callback_data="season_rank:0")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        banner = await asyncio.to_thread(make_ui_banner, f"season_{info['id']}")
        if banner and len(text) <= 1024:
            await message.answer_photo(FSInputFile(banner), caption=text, reply_markup=markup, parse_mode="HTML")
            return
        if banner:
            # Never slice HTML in the middle of a tag. For a future oversized
            # season screen, keep the banner caption short and send the full
            # validated HTML as a separate message.
            short_caption = f"🌌 <b>СЕЗОН {info['number']} · {e(info['name'].upper())}</b>"
            await message.answer_photo(FSInputFile(banner), caption=short_caption, parse_mode="HTML")
    except Exception as ex:
        logger.debug("Season banner failed: %s", ex)
    if len(text) > 4096:
        logger.error("Season screen unexpectedly exceeds Telegram message limit: %s chars", len(text))
        await message.answer(
            "⚠️ <b>Сезонный экран временно слишком большой.</b> Попробуй снова после обновления данных.",
            reply_markup=markup,
            parse_mode="HTML",
        )
        return
    await message.answer(text, reply_markup=markup, parse_mode="HTML")


@dp.message(Command("season"))
async def season_cmd(message: types.Message):
    await send_season_screen(message, message.from_user)


@dp.callback_query(F.data == "season")
async def season_cb(callback: types.CallbackQuery):
    await send_season_screen(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "season_claim")
async def season_claim_cb(callback: types.CallbackQuery):
    # PerUserSerialMiddleware serializes this entire critical section.
    player = get_user_data(callback.from_user)
    _info, granted = claim_available_season_rewards(player)
    if granted:
        saved = await flush_data_now_async("season_claim")
        suffix = "" if saved else "\n\n⚠️ Награда выдана в памяти; облачное сохранение будет повторено автоматически."
        await callback.message.answer(
            "🎁 <b>Сезонные награды получены</b>\n\n" + "\n".join(f"• {e(x)}" for x in granted) + suffix,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🌌 Сезон", callback_data="season")]]),
        )
        await callback.answer(f"Получено: {len(granted)}")
    else:
        await callback.answer("Новых доступных наград пока нет.", show_alert=True)


@dp.callback_query(F.data == "season_earn")
async def season_earn_cb(callback: types.CallbackQuery):
    await callback.message.answer(
        "⚡ <b>КАК ЗАРАБАТЫВАТЬ SP</b>\n\n"
        "🎴 Призыв — +40 SP\n🎁 Ежедневная — +60 SP\n⚒️ Крафт — +30 SP\n"
        "⚔️ Одиночный бой — +80 SP\n🥊 PvP — +70–120 SP\n🎪 Событие — +50 SP\n"
        "👹 Рейд — +40 SP за реальную атаку\n🧰 Кейс — +25 SP\n\n"
        "Повторное открытие старого результата SP не начисляет.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Сезон", callback_data="season")]]),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("season_rank:"))
async def season_rank_cb(callback: types.CallbackQuery):
    try:
        page = max(0, int(callback.data.rsplit(":", 1)[1]))
    except Exception:
        page = 0
    rows_data = season_rank_rows()
    ranked_rows = []
    previous_sp = None
    displayed_rank = 0
    for offset, row in enumerate(rows_data, 1):
        _uid, sp, player = row
        if sp != previous_sp:
            displayed_rank = offset
            previous_sp = sp
        ranked_rows.append((displayed_rank, _uid, sp, player))
    per_page = 10
    pages = max(1, (len(ranked_rows) + per_page - 1) // per_page)
    page = min(page, pages - 1)
    shown = ranked_rows[page * per_page:(page + 1) * per_page]
    lines = ["🏆 <b>СЕЗОННЫЙ РЕЙТИНГ</b>", ""]
    for displayed_rank, _uid, sp, player in shown:
        lines.append(f"<b>{displayed_rank}.</b> {e(player.get('name') or 'Игрок')} — <b>{short_number(sp)} SP</b>")
    if not shown:
        lines.append("Пока никто не заработал SP в текущем сезоне.")
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"season_rank:{page-1}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page + 1 < pages:
        nav.append(button(text="➡️", callback_data=f"season_rank:{page+1}"))
    kb = [nav, [button(text="⬅️ Сезон", callback_data="season")]]
    await callback.message.answer("\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await callback.answer()


def battle_power_label(player):
    try:
        return user_total_power(player)
    except Exception:
        return len(player.get("collection", {}) or {}) * 100


def battle_power_display(player):
    return short_number(battle_power_label(player))


def clan_store():
    DATA.setdefault("clans", {})
    return DATA["clans"]


def clan_name_for(player):
    cid = (player or {}).get("clan_id", "")
    if not cid:
        return "Нет клана"
    c = clan_store().get(cid)
    return c.get("name", "Нет клана") if c else "Нет клана"


def player_title(player):
    ensure_rpg_fields(player)
    title = player.get("title") or "Новичок разлома"
    uid = selected_universe_id(player)
    if title == "Новичок разлома" and uid != "all":
        title = TITLE_BY_UNIVERSE.get(uid, title)
    return title


def can_create_clan(player, user_id):
    ensure_rpg_fields(player)
    return bool(is_owner(user_id) or player.get("premium") or player.get("pass_premium") or player.get("privilege") in PRIVILEGES)


def make_clan_id(name):
    base = hashlib.md5((name + utc_now().isoformat()).encode("utf-8")).hexdigest()[:10]
    return "clan_" + base


def open_public_clans(limit=10):
    items = list(clan_store().items())
    items.sort(key=lambda x: int(x[1].get("points", 0) or 0), reverse=True)
    return items[:limit]


def create_default_clan_for_user(user, player, name=None):
    ensure_rpg_fields(player)
    name = (name or f"Клан {player.get('name') or user.full_name}" or "Клан разлома").strip()[:32]
    cid = make_clan_id(name)
    clan_store()[cid] = {"id": cid, "name": name, "leader": str(user.id), "members": [str(user.id)], "points": 0, "level": 1, "min_power": 0, "open": True, "created_at": utc_now().isoformat()}
    player["clan_id"] = cid
    mark_data_dirty("data_changed")
    return clan_store()[cid]


def join_clan_by_id(user_id, clan_id):
    clans = clan_store()
    if clan_id not in clans:
        return False, "Клан не найден."
    p = DATA.get("users", {}).get(str(user_id))
    if not p:
        return False, "Игрок не найден."
    ensure_rpg_fields(p)
    power = battle_power_label(p)
    clan = clans[clan_id]
    if power < int(clan.get("min_power", 0) or 0):
        return False, f"Нужно минимум {clan.get('min_power')} боевой мощи."
    old = p.get("clan_id")
    if old and old in clans and str(user_id) in clans[old].get("members", []):
        clans[old]["members"].remove(str(user_id))
    p["clan_id"] = clan_id
    clan.setdefault("members", [])
    if str(user_id) not in clan["members"]:
        clan["members"].append(str(user_id))
    mark_data_dirty("data_changed")
    return True, clan.get("name", clan_id)



def public_profile_text(uid, player):
    ensure_rpg_fields(player)
    scope_uid = selected_universe_id(player)
    unique, total = universe_progress(player, scope_uid)
    role = "Владелец мультивселенной" if is_owner(uid) else player_title(player)
    return (
        f"👤 <b>{e(player.get('name', uid))}</b>\n"
        f"Статус: <b>{e(role)}</b>\n"
        f"ID: <code>{e(uid)}</code>\n"
        f"Вселенная: <b>{e(universe_label(scope_uid))}</b>\n"
        f"Клан: <b>{e(clan_name_for(player))}</b>\n"
        f"Персонажи: <b>{unique}/{total}</b>"
    )



def profile_menu(user_id=None):
    rows = [
        [button(text="🎮 Игры", callback_data="profile_games"), button(text="👥 Друзья", callback_data="friends")],
        [button(text="🎨 Кастомизация", callback_data="customize"), button(text="🌌 Сменить вселенную", callback_data="universe")],
        [button(text="📊 Статистика", callback_data="profile_stats"), button(text="🔔 Уведомления", callback_data="notify_settings")],
        [button(text="📜 Правила", callback_data="rules")],
    ]
    if user_id and is_owner(user_id):
        rows.append([button(text="👑 Админ-панель", callback_data="admin")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def shop_menu():
    rows = [
        [button(text="🐉 Драконит", callback_data="shop_dragonite"), button(text="🎴 Попытки", callback_data="shop_attempts")],
        [button(text="📊 Шансы призыва", callback_data="chests"), button(text="🎟 MultiPass", callback_data="multipass")],
        [button(text="👑 Премиум", callback_data="premium_info"), button(text="💠 Привилегии", callback_data="privileges")],
        [button(text="🔁 Обмен", callback_data="exchange"), button(text="⚙️ Ещё", callback_data="shop_more")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def shop_more_menu():
    rows = [
        [button(text="💎 Фисташки", callback_data="shop_fistiks"), button(text="💠 Привилегии", callback_data="privileges")],
        [button(text="🎟 Боевой пропуск", callback_data="shop_battlepass"), button(text="⭐ Stars", callback_data="stars_shop")],
        [button(text="⬅️ Магазин", callback_data="shop")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def media_path(card_id):
    """O(1) real-media lookup after one startup index build."""
    cid = str(card_id)
    p = REAL_MEDIA_BY_ID.get(cid)
    if p:
        try:
            if p.exists() and p.stat().st_size > 0:
                return p
        except Exception:
            pass
        REAL_MEDIA_BY_ID.pop(cid, None)
        REAL_MEDIA_IDS.discard(cid)
    # Rare fallback for media added while the process is running.
    for folder in [MEDIA_DIR / "cards_watermarked", MEDIA_CARDS_DIR, MEDIA_DIR]:
        for ext in [".gif", ".mp4", ".jpg", ".jpeg", ".png", ".webp"]:
            candidate = folder / f"{cid}{ext}"
            try:
                if candidate.exists() and candidate.stat().st_size > 0:
                    REAL_MEDIA_BY_ID[cid] = candidate
                    REAL_MEDIA_IDS.add(cid)
                    return candidate
            except Exception:
                continue
    return None


def _is_valid_generated_image(path):
    path = Path(path)
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        if Image is None:
            return True
        with Image.open(path) as im:
            im.verify()
        return True
    except Exception:
        return False


def _acquire_generated_thread_lock(card_id):
    """Acquire a per-card thread lock without leaking one lock per generated card forever."""
    cid = str(card_id)
    with _GENERATED_CARD_THREAD_LOCKS_GUARD:
        state = _GENERATED_CARD_THREAD_LOCKS.get(cid)
        if state is None:
            state = {"lock": threading.Lock(), "refs": 0}
            _GENERATED_CARD_THREAD_LOCKS[cid] = state
        state["refs"] = int(state.get("refs", 0)) + 1
        lock = state["lock"]
    lock.acquire()
    return cid, state


def _release_generated_thread_lock(cid, state):
    lock = state["lock"]
    lock.release()
    with _GENERATED_CARD_THREAD_LOCKS_GUARD:
        state["refs"] = max(0, int(state.get("refs", 1)) - 1)
        if state["refs"] == 0 and not lock.locked() and _GENERATED_CARD_THREAD_LOCKS.get(cid) is state:
            _GENERATED_CARD_THREAD_LOCKS.pop(cid, None)


_GENERATED_CACHE_LAST_CLEANUP = 0.0
_GENERATED_CACHE_CLEANUP_LOCK = threading.Lock()


def cleanup_generated_card_cache_sync(force=False):
    """LRU-ish cleanup limited strictly to media/generated_cards."""
    global _GENERATED_CACHE_LAST_CLEANUP
    now_mono = time.monotonic()
    if not force and now_mono - _GENERATED_CACHE_LAST_CLEANUP < 15 * 60:
        return 0
    if not _GENERATED_CACHE_CLEANUP_LOCK.acquire(blocking=False):
        return 0
    try:
        _GENERATED_CACHE_LAST_CLEANUP = now_mono
        root = GENERATED_CARDS_DIR
        if not root.exists():
            return 0
        files = []
        total = 0
        now_ts = time.time()
        max_age = GENERATED_CACHE_MAX_AGE_DAYS * 86400
        for item in root.iterdir():
            try:
                if not item.is_file() or item.suffix.lower() != ".png":
                    continue
                st = item.stat()
                # Corrupt/empty cache is always safe to remove.
                if st.st_size <= 0:
                    item.unlink(missing_ok=True)
                    continue
                files.append((st.st_mtime, st.st_size, item))
                total += st.st_size
            except Exception:
                continue
        removed = 0
        # First remove stale files, then oldest files until both hard caps are met.
        for mtime, size, item in sorted(files):
            if now_ts - mtime > max_age:
                try:
                    item.unlink(missing_ok=True); removed += 1; total -= size
                except Exception:
                    pass
        files = []
        for item in root.glob("*.png"):
            try:
                st = item.stat(); files.append((st.st_mtime, st.st_size, item))
            except Exception:
                pass
        max_bytes = GENERATED_CACHE_MAX_MB * 1024 * 1024
        current_count = len(files)
        for mtime, size, item in sorted(files):
            if current_count <= GENERATED_CACHE_MAX_FILES and total <= max_bytes:
                break
            try:
                item.unlink(missing_ok=True)
                removed += 1
                current_count -= 1
                total -= size
            except Exception:
                pass
        return removed
    finally:
        _GENERATED_CACHE_CLEANUP_LOCK.release()




def _make_card_banner_unlocked(card_id):
    if Image is None or ImageDraw is None or ImageFont is None or card_id not in CARD_BY_ID:
        return None
    c = CARD_BY_ID[card_id]
    # Даже для Absolute создаём брендированную заглушку, если владелец ещё не
    # добавил отдельный GIF/арт. Выпадение карты больше не бывает «голым текстом».
    out_dir = GENERATED_CARDS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{card_id}.png"
    if _is_valid_generated_image(out):
        try:
            os.utime(out, None)
        except Exception:
            pass
        return out
    try:
        out.unlink(missing_ok=True)
    except Exception:
        pass

    rng = random.Random(card_id)
    rarity = c.get("rarity", "Обычный")
    role = (c.get("role") or "").lower()
    form = (c.get("form") or "").lower()
    colors = {
        "Обычный": ((35, 38, 48), (98, 105, 120), (210, 216, 230)),
        "Редкий": ((12, 34, 84), (38, 125, 235), (160, 215, 255)),
        "Эпический": ((52, 12, 88), (150, 58, 230), (228, 175, 255)),
        "Легендарный": ((92, 55, 6), (245, 170, 42), (255, 239, 155)),
        "Мифический": ((90, 5, 25), (235, 38, 94), (255, 160, 190)),
    }
    top, bottom, accent = colors.get(rarity, colors["Обычный"])
    w, h = 900, 1200
    img = Image.new("RGB", (w, h), top)
    draw = ImageDraw.Draw(img)

    for y in range(h):
        t = y / h
        wave = 0.08 * rng.random()
        col = tuple(int(top[i] * (1 - t) + bottom[i] * t + accent[i] * wave) for i in range(3))
        draw.line([(0, y), (w, y)], fill=col)

    keywords = role + " " + form
    icon = "✦"
    if any(x in keywords for x in ["меч", "клин", "самурай"]):
        icon, motif = "⚔", "blade"
    elif any(x in keywords for x in ["маг", "хакс", "простран", "демон", "прокля"]):
        icon, motif = "✺", "arcane"
    elif any(x in keywords for x in ["скор", "ассас", "рывок"]):
        icon, motif = "➤", "speed"
    elif any(x in keywords for x in ["саппорт", "команд", "медик", "защ"]):
        icon, motif = "⬢", "support"
    elif any(x in keywords for x in ["танк", "гигант", "сила", "физ"]):
        icon, motif = "◆", "power"
    else:
        motif = "aura"

    for _ in range(90):
        x = rng.randint(-80, w + 80)
        y = rng.randint(-80, h + 80)
        r = rng.randint(2, 16)
        a = rng.randint(80, 210)
        fill = tuple(min(255, int(accent[i] * a / 210)) for i in range(3))
        draw.ellipse((x, y, x + r, y + r), fill=fill)

    if motif == "blade":
        for x in range(-350, w, 180):
            draw.line((x, h - 110, x + 720, 120), fill=accent, width=5)
    elif motif == "speed":
        for y in range(130, h - 170, 80):
            draw.line((60, y, w - 60, y - rng.randint(20, 70)), fill=accent, width=4)
    elif motif == "arcane":
        for off in [0, 38, 76, 114]:
            draw.ellipse((100-off, 130-off, w-100+off, h-260+off), outline=accent, width=4)
    elif motif == "support":
        for x in range(95, w, 150):
            draw.polygon([(x, 210), (x + 70, 250), (x + 70, 330), (x, 370), (x - 70, 330), (x - 70, 250)], outline=accent)
    elif motif == "power":
        for _ in range(12):
            x = rng.randint(40, w - 140)
            y = rng.randint(120, h - 360)
            draw.rectangle((x, y, x + rng.randint(80, 190), y + rng.randint(18, 44)), outline=accent, width=4)
    else:
        for off in [0, 28, 56]:
            draw.ellipse((80-off, 115-off, w-80+off, h-260+off), outline=accent, width=3)

    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((35, 45, w - 35, h - 45), radius=42, outline=accent + (230,), width=6)
    od.rounded_rectangle((55, 260, w - 55, h - 200), radius=36, fill=(0, 0, 0, 84), outline=(255, 255, 255, 90), width=2)
    od.rectangle((0, h - 170, w, h), fill=(0, 0, 0, 210))
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(img)

    try:
        font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 62)
        font_mid = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 29)
        font_icon = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 120)
    except Exception:
        font_big = font_mid = font_small = font_icon = ImageFont.load_default()

    def wrap(txt, max_len):
        words = str(txt).split()
        lines, cur = [], ""
        for word in words:
            if len((cur + " " + word).strip()) <= max_len:
                cur = (cur + " " + word).strip()
            else:
                if cur:
                    lines.append(cur)
                cur = word
        if cur:
            lines.append(cur)
        return lines[:4]

    draw.text((70, 95), icon, font=font_icon, fill=accent, stroke_width=4, stroke_fill=(0, 0, 0))
    draw.text((w - 340, 95), rarity.upper(), font=font_mid, fill=accent, stroke_width=2, stroke_fill=(0, 0, 0))
    y = 325
    for line in wrap(c.get("name", card_id), 18):
        draw.text((82, y), line, font=font_big, fill=(255, 255, 255), stroke_width=3, stroke_fill=(0, 0, 0))
        y += 72
    y += 10
    draw.text((82, y), f"{c.get('anime','')}", font=font_mid, fill=accent, stroke_width=2, stroke_fill=(0, 0, 0))
    y += 54
    for line in wrap(c.get("form", "Базовая форма"), 30):
        draw.text((82, y), line, font=font_small, fill=(235, 245, 255), stroke_width=2, stroke_fill=(0, 0, 0))
        y += 40
    y += 10
    for line in wrap(c.get("role", "боевой стиль"), 32)[:2]:
        draw.text((82, y), line, font=font_small, fill=(230, 230, 235), stroke_width=2, stroke_fill=(0, 0, 0))
        y += 38

    draw.text((55, h - 130), "ANIME BATTLE MULTIVERSE", font=font_mid, fill=(255, 255, 255))
    draw.text((55, h - 82), "Коллекционная карта", font=font_small, fill=(210, 210, 230))
    tmp = out_dir / f".{card_id}.{os.getpid()}.{threading.get_ident()}.tmp.png"
    try:
        img.save(tmp, format="PNG", optimize=True)
        if not _is_valid_generated_image(tmp):
            raise RuntimeError("generated image validation failed")
        os.replace(tmp, out)
        cleanup_generated_card_cache_sync(force=False)
        return out
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass



def make_card_banner(card_id):
    """Thread-safe synchronous generator used only through asyncio.to_thread in handlers."""
    cid, state = _acquire_generated_thread_lock(card_id)
    try:
        try:
            return _make_card_banner_unlocked(card_id)
        except OSError as ex:
            logger.warning("Generated card cache unavailable for %s: %s", card_id, ex)
            return None
        except Exception as ex:
            logger.exception("Generated card failed for %s: %s", card_id, ex)
            return None
    finally:
        _release_generated_thread_lock(cid, state)


async def resolve_card_media_async(card_id):
    real = media_path(card_id)
    if real:
        return real
    cid = str(card_id)
    state = _GENERATED_CARD_ASYNC_LOCKS.get(cid)
    if state is None:
        state = {"lock": asyncio.Lock(), "waiters": 0, "last_used": time.monotonic()}
        _GENERATED_CARD_ASYNC_LOCKS[cid] = state
    state["waiters"] += 1
    try:
        async with state["lock"]:
            state["last_used"] = time.monotonic()
            # Another waiter may have generated it already.
            out = GENERATED_CARDS_DIR / f"{cid}.png"
            if _is_valid_generated_image(out):
                try:
                    os.utime(out, None)
                except Exception:
                    pass
                return out
            return await asyncio.to_thread(make_card_banner, cid)
    finally:
        state["waiters"] = max(0, int(state.get("waiters", 1)) - 1)
        state["last_used"] = time.monotonic()
        if state["waiters"] == 0 and not state["lock"].locked() and _GENERATED_CARD_ASYNC_LOCKS.get(cid) is state:
            _GENERATED_CARD_ASYNC_LOCKS.pop(cid, None)


def make_ui_banner(kind="main"):
    """Лёгкий локальный баннер без внешних ссылок и обязательных ассетов."""
    if Image is None or ImageDraw is None or ImageFont is None:
        return None
    out_dir = MEDIA_DIR / "ui"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{kind}_banner_{PATCH_VERSION}.png"
    if _is_valid_generated_image(out):
        return out
    try:
        out.unlink(missing_ok=True)
    except Exception:
        pass

    w, h = 1200, 630
    img = Image.new("RGB", (w, h), (9, 8, 25))
    draw = ImageDraw.Draw(img)
    rng = random.Random(f"ui:{kind}:{PATCH_VERSION}")
    top = (18, 10, 45)
    bottom = (55, 10, 34)
    for y in range(h):
        t = y / max(1, h - 1)
        col = tuple(int(top[i] * (1 - t) + bottom[i] * t) for i in range(3))
        draw.line((0, y, w, y), fill=col)
    for _ in range(170):
        x, y = rng.randrange(w), rng.randrange(h)
        r = rng.choice([1, 1, 2, 2, 3, 5])
        glow = rng.choice([(255, 255, 255), (184, 140, 255), (255, 120, 180), (92, 180, 255)])
        draw.ellipse((x-r, y-r, x+r, y+r), fill=glow)
    for radius, alpha in [(250, 40), (190, 60), (125, 90)]:
        x = 900 + rng.randint(-30, 30)
        y = 250 + rng.randint(-20, 20)
        color = (150 + alpha//3, 45 + alpha//4, 150 + alpha//2)
        draw.ellipse((x-radius, y-radius, x+radius, y+radius), outline=color, width=max(2, radius//35))
    draw.polygon([(830, 80), (1120, 315), (830, 550), (925, 315)], fill=(255, 255, 255))
    draw.polygon([(856, 137), (1048, 315), (856, 493), (925, 315)], fill=(70, 18, 78))

    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 74)
        font_sub = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 34)
        font_tag = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 27)
    except Exception:
        font_title = font_sub = font_tag = ImageFont.load_default()

    draw.text((70, 145), "ANIME BATTLE", font=font_title, fill=(255, 255, 255), stroke_width=2, stroke_fill=(20, 5, 35))
    draw.text((70, 230), "MULTIVERSE", font=font_title, fill=(255, 126, 196), stroke_width=2, stroke_fill=(20, 5, 35))
    draw.text((75, 340), "SUMMON  •  BUILD  •  CONQUER", font=font_sub, fill=(216, 205, 245))
    draw.rounded_rectangle((72, 425, 650, 486), radius=28, fill=(255, 255, 255), outline=(255, 150, 210), width=2)
    if str(kind).startswith("season_"):
        info = season_info()
        tag = f"SEASON {info['number']}: {str(info['name']).upper()}"
    else:
        tag = "YOUR TEAM. YOUR MULTIVERSE."
    if len(tag) > 34:
        tag = tag[:31].rstrip() + "..."
    draw.text((105, 439), tag, font=font_tag, fill=(52, 14, 60))
    tmp = out_dir / f".{kind}.{os.getpid()}.{threading.get_ident()}.tmp.png"
    try:
        img.save(tmp, format="PNG", optimize=True)
        if not _is_valid_generated_image(tmp):
            raise RuntimeError("UI banner validation failed")
        os.replace(tmp, out)
        return out
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


async def send_card_result(message, card_id, caption, reply_markup=None):
    """Отправляет результат призыва одной визуальной карточкой с подписью."""
    p = await resolve_card_media_async(card_id)
    if p:
        f = FSInputFile(p)
        ext = p.suffix.lower()
        try:
            if ext == ".gif":
                await message.answer_animation(f, caption=caption, reply_markup=reply_markup, parse_mode="HTML")
            elif ext == ".mp4":
                await message.answer_video(f, caption=caption, reply_markup=reply_markup, parse_mode="HTML")
            else:
                await message.answer_photo(f, caption=caption, reply_markup=reply_markup, parse_mode="HTML")
            return True
        except Exception as ex:
            logger.debug("Card result media failed for %s: %s", card_id, ex)
    await message.answer(caption, reply_markup=reply_markup, parse_mode="HTML")
    return False


async def send_card_media(message, card_id):
    p = await resolve_card_media_async(card_id)
    if not p:
        return False
    f = FSInputFile(p)
    ext = p.suffix.lower()
    try:
        if ext == ".gif":
            await message.answer_animation(f)
        elif ext == ".mp4":
            await message.answer_video(f)
        else:
            await message.answer_photo(f)
        return True
    except Exception:
        return False


def arena_media_path(arena_code):
    arena_dir = MEDIA_DIR / "arenas"
    for ext in [".gif", ".mp4", ".jpg", ".jpeg", ".png", ".webp"]:
        p = arena_dir / f"{arena_code}{ext}"
        if p.exists():
            return p
    return None


def ensure_generated_arena_media():
    """Создаёт атмосферные оригинальные изображения арен, если пользователь не загрузил свои."""
    if Image is None or ImageDraw is None:
        return
    arena_dir = MEDIA_DIR / "arenas"
    arena_dir.mkdir(parents=True, exist_ok=True)
    palettes = {
        "ruins": ((38, 31, 55), (136, 112, 87), "RUINS OF MULTIVERSE"),
        "city": ((8, 13, 35), (77, 171, 255), "NIGHT MEGAPOLIS"),
        "void": ((18, 5, 38), (168, 70, 255), "VOID DIMENSIONS"),
        "forest": ((8, 34, 25), (44, 168, 94), "CURSED FOREST"),
        "desert": ((58, 37, 20), (236, 154, 76), "DESERT CANYON"),
        "temple": ((35, 18, 20), (221, 69, 62), "BROKEN TEMPLE"),
    }
    try:
        font_big = ImageFont.truetype("DejaVuSans-Bold.ttf", 48)
        font_mid = ImageFont.truetype("DejaVuSans-Bold.ttf", 30)
        font_small = ImageFont.truetype("DejaVuSans.ttf", 22)
    except Exception:
        font_big = font_mid = font_small = None
    for code_key, (_, name, desc) in ARENAS.items():
        out = arena_dir / f"{code_key}.png"
        if out.exists():
            continue
        bg, accent, eng = palettes.get(code_key, ((20, 20, 35), (180, 100, 255), "ARENA"))
        w, h = 1280, 720
        img = Image.new("RGB", (w, h), bg)
        draw = ImageDraw.Draw(img)
        # gradient
        for y in range(h):
            ratio = y / max(1, h - 1)
            col = tuple(int(bg[i] * (1 - ratio) + accent[i] * ratio * 0.55) for i in range(3))
            draw.line((0, y, w, y), fill=col)
        # stars / particles
        rng = random.Random(code_key)
        for _ in range(180):
            x, y = rng.randint(0, w), rng.randint(0, h)
            r = rng.choice([1, 1, 2, 3])
            col = tuple(min(255, accent[i] + rng.randint(20, 80)) for i in range(3))
            draw.ellipse((x-r, y-r, x+r, y+r), fill=col)
        # arena silhouettes
        for i in range(9):
            x0 = -120 + i * 170
            y0 = 455 + rng.randint(-35, 35)
            draw.polygon([(x0, h), (x0+90, y0), (x0+180, h)], fill=tuple(max(0, c-25) for c in bg))
        draw.rounded_rectangle((52, 50, w-52, h-50), radius=34, outline=accent, width=4)
        draw.rectangle((0, h-120, w, h), fill=tuple(max(0, c-12) for c in bg))
        draw.text((82, 90), "ANIME BATTLE MULTIVERSE", font=font_mid, fill=(245, 245, 255))
        draw.text((82, 154), eng, font=font_big, fill=accent)
        draw.text((82, 235), name.upper(), font=font_big, fill=(255, 255, 255))
        # wrap description manually
        words = str(desc).split()
        line = ""
        y = 320
        for word in words:
            if len(line + " " + word) > 52:
                draw.text((86, y), line, font=font_small, fill=(230, 230, 238))
                y += 34
                line = word
            else:
                line = (line + " " + word).strip()
        if line:
            draw.text((86, y), line, font=font_small, fill=(230, 230, 238))
        draw.text((82, h-86), "Выбор арены • RPG battle field", font=font_small, fill=(230, 230, 238))
        img.save(out, optimize=True, quality=92)



async def send_arena_media(message, arena_code):
    p = arena_media_path(arena_code)
    if not p:
        return False
    f = FSInputFile(p)
    ext = p.suffix.lower()
    try:
        if ext == ".gif":
            await message.answer_animation(f)
        elif ext == ".mp4":
            await message.answer_video(f)
        else:
            await message.answer_photo(f)
        return True
    except Exception:
        return False


async def send_arena_card(message, arena_code, caption, reply_markup=None):
    """Отправляет арену одной карточкой: фото/гиф + текст + кнопки. Так листание не засыпает чат отдельными картинками."""
    p = arena_media_path(arena_code)
    if p:
        f = FSInputFile(p)
        ext = p.suffix.lower()
        try:
            if ext == ".gif":
                await message.answer_animation(f, caption=caption, reply_markup=reply_markup, parse_mode="HTML")
            elif ext == ".mp4":
                await message.answer_video(f, caption=caption, reply_markup=reply_markup, parse_mode="HTML")
            else:
                await message.answer_photo(f, caption=caption, reply_markup=reply_markup, parse_mode="HTML")
            return True
        except Exception:
            pass
    await message.answer(caption, reply_markup=reply_markup, parse_mode="HTML")
    return False


LORE_POWER_NAMES = [
    "фезарин", "featherine", "анос", "anos", "римуру", "rimuru", "юхабах", "yhwach", "айзен", "aizen",
    "black frieza", "фриза", "зено", "zeno", "арсеус", "arceus", "шибай", "shibai", "гоку", "goku",
    "вегито", "vegito", "гогета", "gogeta", "гохан", "gohan", "мадока", "madoka", "сейлор", "sailor",
    "саймон", "simon", "мадары", "madara", "сукуна", "sukuna", "ичиго", "ichigo", "наруто", "naruto",
]

def lore_power_bonus(card):
    hay = f"{card.get('id','')} {card.get('name','')} {card.get('form','')} {card.get('anime','')}".casefold()
    for idx, token in enumerate(LORE_POWER_NAMES):
        if token in hay:
            return max(0, 60 - idx * 2)
    return 0

def card_power(card, level=1):
    return int(sum(card["stats"].values()) + RARITY_BONUS.get(card["rarity"], 0) + lore_power_bonus(card) + (level - 1) * 4)


def level_cost(level, rarity):
    if level >= MAX_LEVEL:
        return None
    return UPGRADE_FRAGMENT_COSTS.get(rarity, 40)



def add_card(player, card_id, extra_shards=0):
    card = CARD_BY_ID[card_id]
    col = player.setdefault("collection", {})
    item = col.get(card_id)
    if not isinstance(item, dict) or int(item.get("count", 0) or 0) <= 0 or not bool(item.get("unlocked", True)):
        legacy_shards = int((item or {}).get("shards", 0) or 0) if isinstance(item, dict) else 0
        col[card_id] = {
            "count": 1,
            "shards": legacy_shards + int(extra_shards or 0),
            "level": max(1, int((item or {}).get("level", 1) or 1)) if isinstance(item, dict) else 1,
            "unlocked": True,
            "duplicates": int((item or {}).get("duplicates", 0) or 0) if isinstance(item, dict) else 0,
        }
        return f"🆕 Получен персонаж: {card['name']}"
    item.setdefault("level", 1)
    item.setdefault("shards", 0)
    item.setdefault("count", 1)
    item.setdefault("duplicates", max(0, int(item.get("count", 1) or 1) - 1))
    item["unlocked"] = True
    gain = DUPLICATE_SHARDS.get(card["rarity"], 5) + int(extra_shards or 0)
    item["count"] = max(1, int(item.get("count", 1) or 1))
    item["duplicates"] = int(item.get("duplicates", 0) or 0) + 1
    item["shards"] = int(item.get("shards", 0) or 0) + gain
    return f"♻️ Дубликат: {card['name']} → +{gain} фрагментов"




def add_fragments(player, card_id, amount):
    """Any character reward grants the full character; fragments only strengthen owned duplicates."""
    amount = max(0, int(amount or 0))
    card = CARD_BY_ID[card_id]
    col = player.setdefault("collection", {})
    item = col.get(card_id)
    if not isinstance(item, dict) or int(item.get("count", 0) or 0) <= 0 or not bool(item.get("unlocked", True)):
        legacy = item if isinstance(item, dict) else {}
        col[card_id] = {
            "count": 1,
            "shards": int(legacy.get("shards", 0) or 0) + amount,
            "level": max(1, int(legacy.get("level", 1) or 1)),
            "unlocked": True,
            "duplicates": int(legacy.get("duplicates", 0) or 0),
        }
        return f"🆕 Получен персонаж: {card['name']} · +{amount} фрагментов усиления"
    item.setdefault("level", 1)
    item.setdefault("count", 1)
    item.setdefault("shards", 0)
    item.setdefault("duplicates", 0)
    item["unlocked"] = True
    item["shards"] = int(item.get("shards", 0) or 0) + amount
    need = level_cost(item.get("level", 1), card["rarity"])
    if need is None:
        return f"🧩 +{amount} фрагментов · максимальный уровень"
    return f"🧩 +{amount} фрагментов · сейчас {item['shards']}/{need}"



def full_card_drop_chance(rarity):
    return {
        "Обычный": 0.16,
        "Редкий": 0.075,
        "Эпический": 0.035,
        "Легендарный": 0.010,
        "Мифический": 0.003,
    }.get(rarity, 0.08)


def fragment_amount_for(rarity):
    low_high = {
        "Обычный": (18, 35),
        "Редкий": (14, 28),
        "Эпический": (10, 22),
        "Легендарный": (4, 10),
        "Мифический": (2, 5),
    }.get(rarity, (10, 20))
    return random.randint(*low_high)


def roll_card_with_pity(player, weights=None, exclude=None, universe_id=None, allow_super_absolute=False):
    pity = player.setdefault("pity_counters", {"epic": 0, "legendary": 0, "mythic": 0})
    for k in ["epic", "legendary", "mythic"]:
        pity[k] = int(pity.get(k, 0))

    def _rarity_available(rarity):
        if weights and int(weights.get(rarity, 0) or 0) <= 0:
            return False
        return universe_has_rarity(universe_id, rarity, exclude=exclude, allow_super_absolute=allow_super_absolute)

    forced = None
    note = ""
    if pity["mythic"] + 1 >= PITY_LIMITS["mythic"] and _rarity_available("Мифический"):
        forced = "Мифический"
        note = "\n🎯 Сработал гарант Absolute-редкости."
    elif pity["legendary"] + 1 >= PITY_LIMITS["legendary"] and _rarity_available("Легендарный"):
        forced = "Легендарный"
        note = "\n🎯 Сработал гарант Legendary-редкости."
    elif pity["epic"] + 1 >= PITY_LIMITS["epic"] and _rarity_available("Эпический"):
        forced = "Эпический"
        note = "\n🎯 Сработал гарант Epic-редкости."

    if forced:
        card = roll_card(weights={forced: 1}, exclude=exclude, allowed_rarities=[forced], universe_id=universe_id, allow_super_absolute=allow_super_absolute)
        if card is None:
            card = roll_card(weights=weights, exclude=exclude, universe_id=universe_id, allow_super_absolute=allow_super_absolute)
            note = ""
    else:
        card = roll_card(weights=weights, exclude=exclude, universe_id=universe_id, allow_super_absolute=allow_super_absolute)

    rarity = card.get("rarity", "Обычный")
    pity["epic"] += 1
    pity["legendary"] += 1
    pity["mythic"] += 1
    if rarity in ("Эпический", "Легендарный", "Мифический"):
        pity["epic"] = 0
    if rarity in ("Легендарный", "Мифический"):
        pity["legendary"] = 0
    if rarity == "Мифический":
        pity["mythic"] = 0
    return card, note



def pull_pack_reward(player, weights, exclude=None, universe_id=None, allow_super_absolute=False):
    card, pity_note = roll_card_with_pity(
        player, weights=weights, exclude=exclude,
        universe_id=universe_id, allow_super_absolute=allow_super_absolute,
    )
    return card, add_card(player, card["id"]) + pity_note



def roll_card(weights=None, exclude=None, allowed_rarities=None, universe_id=None, allow_super_absolute=False):
    exclude = set(exclude or [])
    weights = weights or RARITY_WEIGHTS
    uid = _effective_universe_id(universe_id)

    def _pool(ignore_exclude=False, ignore_rarity=False, ignore_universe=False, allow_paid=False):
        result = []
        for c in CARDS:
            if not ignore_exclude and c["id"] in exclude:
                continue
            if not ignore_rarity and allowed_rarities is not None and c["rarity"] not in allowed_rarities:
                continue
            if uid and not ignore_universe and card_draw_universe(c) != uid:
                continue
            if not allow_paid and not allow_super_absolute and is_super_absolute_card(c):
                continue
            result.append(c)
        return result

    candidates_all = _pool()
    if not candidates_all:
        candidates_all = _pool(ignore_exclude=True) or _pool(ignore_universe=True) or _pool(ignore_exclude=True, ignore_universe=True) or _pool(ignore_exclude=True, ignore_universe=True, ignore_rarity=True)
    if not candidates_all:
        # Последняя страховка: только если в базе вообще нет обычных кандидатов.
        candidates_all = _pool(ignore_exclude=True, ignore_universe=True, ignore_rarity=True, allow_paid=True) or CARDS[:]

    rarities = list(weights.keys())
    values = [max(0, int(v or 0)) for v in weights.values()]
    if not rarities or sum(values) <= 0:
        return random.choice(candidates_all)
    for _ in range(100):
        rarity = random.choices(rarities, weights=values, k=1)[0]
        if weights.get(rarity, 0) <= 0:
            continue
        candidates = [c for c in candidates_all if c["rarity"] == rarity]
        if candidates:
            # Rarity was already selected, so this preference cannot change rarity odds or pity.
            if REAL_ART_CHANCE > 0 and random.random() < REAL_ART_CHANCE:
                with_real_art = [c for c in candidates if c.get("id") in REAL_MEDIA_IDS]
                if with_real_art:
                    candidates = with_real_art
            return random.choice(candidates)
    return random.choice(candidates_all)



def battle_weights_for_user(uid):
    if is_owner(uid):
        return OWNER_BATTLE_WEIGHTS
    if is_right_hand(uid):
        return RIGHT_HAND_BATTLE_WEIGHTS
    return BATTLE_PLAYER_WEIGHTS


def collection_candidates(uid, exclude=None):
    exclude = set(exclude or [])
    player = DATA.get("users", {}).get(str(uid), {})
    result = []
    for cid, info in player.get("collection", {}).items():
        if cid in CARD_BY_ID and cid not in exclude and int(info.get("count", 0)) > 0:
            result.append((CARD_BY_ID[cid], int(info.get("level", 1)), int(info.get("shards", 0))))
    return result


def roll_card_for_user(uid, weights=None, exclude=None):
    """Бой идёт только своими открытыми картами из коллекции."""
    exclude = set(exclude or [])
    owned = collection_candidates(uid, exclude)
    if not owned:
        return None
    pool = []
    for card, lvl, shards in owned:
        w = 1 + lvl // 6 + RARITY_BONUS.get(card["rarity"], 0) // 10
        pool.extend([card] * max(1, min(w, 30)))
    return random.choice(pool)


def card_level_for_user(uid, card_id):
    player = DATA.get("users", {}).get(str(uid), {})
    return int(player.get("collection", {}).get(card_id, {}).get("level", 1))


def best_owned_card_ids(uid, limit=5):
    owned = collection_candidates(uid)
    owned.sort(key=lambda item: card_power(item[0], item[1]), reverse=True)
    return [card["id"] for card, lvl, shards in owned[:limit]]


def build_player_team_from_deck(uid):
    """Берёт сохранённую колоду. Если её нет — автособирает топ-5 из коллекции."""
    player = DATA.get("users", {}).get(str(uid), {})
    deck = [cid for cid in player.get("deck", []) if cid in CARD_BY_ID and int(player.get("collection", {}).get(cid, {}).get("count", 0)) > 0]
    if len(deck) < 5 or player.get("auto_team", True):
        deck = best_owned_card_ids(uid, 5)
        player["deck"] = deck
        player["auto_team"] = True
    team = []
    for cid in deck[:5]:
        team.append(make_instance(CARD_BY_ID[cid], card_level_for_user(uid, cid), player_battle_artifact(uid)))
    return team



def build_temporary_fillers(uid, current=None, difficulty=2):
    current = current or []
    used = {i.get("card_id") for i in current if isinstance(i, dict)}
    return build_bot_team(difficulty, exclude=used)[:max(0, 5 - len(current))]


def manual_team_ids(uid):
    uid = str(uid)
    player = DATA.get("users", {}).get(uid, {})
    ids = player.get("manual_team", []) or []
    return [cid for cid in ids if cid in CARD_BY_ID and int(player.get("collection", {}).get(cid, {}).get("count", 0) or 0) > 0][:5]


def build_team_for_user(uid, source=None, fill=True):
    uid = str(uid)
    player = DATA.get("users", {}).get(uid, {})
    source = source or player.get("battle_team_source") or player.get("pvp_team_source") or "deck"
    team = []
    if source == "random_bot":
        team = build_bot_team(5)
    elif source == "manual":
        for cid in manual_team_ids(uid):
            team.append(make_instance(CARD_BY_ID[cid], card_level_for_user(uid, cid), player_battle_artifact(uid)))
        if not team:
            for cid in best_owned_card_ids(uid, 5):
                team.append(make_instance(CARD_BY_ID[cid], card_level_for_user(uid, cid), player_battle_artifact(uid)))
    else:
        team = build_player_team_from_deck(uid)
    if fill and len(team) < 5:
        team = (team + build_temporary_fillers(uid, team, difficulty=2))[:5]
    return team[:5]

def bot_weights_for_difficulty(difficulty):
    d = max(1, min(10, int(difficulty or 5)))
    if d <= 2:
        return {"Обычный": 920, "Редкий": 75, "Эпический": 5, "Легендарный": 0, "Мифический": 0}
    if d <= 4:
        return {"Обычный": 820, "Редкий": 150, "Эпический": 27, "Легендарный": 3, "Мифический": 0}
    if d <= 6:
        return {"Обычный": 690, "Редкий": 220, "Эпический": 75, "Легендарный": 13, "Мифический": 2}
    if d <= 8:
        return {"Обычный": 520, "Редкий": 260, "Эпический": 160, "Легендарный": 50, "Мифический": 10}
    return {"Обычный": 330, "Редкий": 250, "Эпический": 260, "Легендарный": 130, "Мифический": 30}


def bot_level_for_difficulty(difficulty):
    d = max(1, min(10, int(difficulty or 5)))
    base = d * 10
    return max(1, min(MAX_LEVEL, base + random.randint(-4, 6)))


def build_bot_team(difficulty, exclude=None):
    weights = bot_weights_for_difficulty(difficulty)
    used = set(exclude or [])
    team = []
    for _ in range(5):
        card = roll_card(weights=weights, exclude=used)
        used.add(card["id"])
        team.append(make_instance(card, bot_level_for_difficulty(difficulty)))
    return team


def cancel_choice_timer(key):
    task = choice_timers.pop(key, None)
    if task and not task.done():
        task.cancel()


def option_roll_text():
    return "⏱ На выбор даётся 20 секунд. На 10 и 5 секундах бот предупредит. Если не выбрать — бот выберет случайно."


def strongest_unit(team):
    if not team:
        return None, None
    inst = max(team, key=instance_score)
    return inst, CARD_BY_ID[inst["card_id"]]


def weakest_unit(team):
    if not team:
        return None, None
    inst = min(team, key=instance_score)
    return inst, CARD_BY_ID[inst["card_id"]]


def battle_story(player_name, bot_name, player_team, bot_team, player_score, bot_score, player_roll, bot_roll, winner_name):
    arena_event = random.choice(BATTLE_EVENTS)
    all_units = [(player_name, i) for i in player_team] + [(bot_name, i) for i in bot_team]
    first_owner, first_inst = max(all_units, key=lambda x: CARD_BY_ID[x[1]["card_id"]]["stats"]["speed"] + CARD_BY_ID[x[1]["card_id"]]["stats"]["iq"])
    carry_owner, carry_inst = max(all_units, key=lambda x: instance_score(x[1]))
    weak_p_inst, weak_p = weakest_unit(player_team)
    weak_b_inst, weak_b = weakest_unit(bot_team)
    first = CARD_BY_ID[first_inst["card_id"]]
    carry = CARD_BY_ID[carry_inst["card_id"]]

    loser_name = bot_name if winner_name == player_name else player_name
    loser_weak = weak_b if loser_name == bot_name else weak_p
    if loser_weak:
        loser_reason = f"слабое звено — {loser_weak['name']}: {loser_weak['minus']}"
    else:
        loser_reason = "команда не выдержала ключевой перелом боя"

    winner_team = player_team if winner_name == player_name else bot_team
    winner_best_inst, winner_best_card = strongest_unit(winner_team)

    decisive = "победитель лучше пережил перелом боя"
    if winner_best_card:
        decisive = f"{winner_best_card['name']} создал главное окно: {winner_best_card['plus']}"

    return (
        f"🎬 <b>Разбор боя</b>\n\n"
        f"{arena_event[0]} <b>Событие арены:</b> {e(arena_event[1])}\n"
        f"— {e(arena_event[2])}.\n\n"
        f"🥇 <b>Первый ход:</b> {e(first['name'])} ({e(first_owner)})\n"
        f"🔥 <b>Главный керри:</b> {e(carry['name'])} ({e(carry_owner)})\n\n"
        f"🧩 <b>Фазы боя</b>\n"
        f"1. <b>Старт:</b> {e(first['name'])} забирает темп и вынуждает врага реагировать.\n"
        f"2. <b>Контроль:</b> команды пытаются закрыть самого опасного бойца — {e(carry['name'])}.\n"
        f"3. <b>Перелом:</b> решают форма, минусы, артефакты и командная связка.\n"
        f"4. <b>Финиш:</b> {e(winner_name)} забирает бой за счёт лучшего решающего окна.\n\n"
        f"✅ <b>Почему победил {e(winner_name)}:</b> {e(decisive)}.\n"
        f"📉 <b>Почему проиграл {e(loser_name)}:</b> {e(loser_reason)}.\n\n"
        f"🏆 <b>Победитель:</b> {e(winner_name)}"
    )



def make_instance(card, level=1, artifact=None):
    if artifact is None:
        artifact = random.choice(ARTIFACTS)
    return {
        "card_id": card["id"],
        "level": max(1, min(MAX_LEVEL, int(level or 1))),
        "buff": random.choice(BUFFS),
        "debuff": random.choice(DEBUFFS),
        "artifact": artifact,
    }



def card_hp(card, level=1):
    return int(card.get("stats", {}).get("durability", 50)) * 10 + int(level or 1) * 25


def role_bonus(card, arena_code="ruins"):
    role = (card.get("role") or "").lower()
    plus = (card.get("plus") or "").lower()
    code = str(arena_code or "")
    bonus = 0
    if code == "city" and any(x in role + plus for x in ["скор", "ассас", "ближ", "рывок"]):
        bonus += 18
    if code == "void" and any(x in role + plus for x in ["хакс", "простран", "измер", "реаль", "маг"]):
        bonus += 20
    if code == "forest" and any(x in role + plus for x in ["скры", "ловуш", "сенсор", "тактик"]):
        bonus += 16
    if code == "desert" and any(x in role + plus for x in ["масштаб", "зем", "зона", "сила"]):
        bonus += 16
    if code == "temple" and any(x in role + plus for x in ["меч", "рукоп", "ближ", "дуэль"]):
        bonus += 16
    if code == "ruins" and any(x in role + plus for x in ["тактик", "ловуш", "мобиль", "контроль"]):
        bonus += 14
    return bonus


def plus_minus_score(card):
    plus = str(card.get("plus", ""))
    minus = str(card.get("minus", ""))
    return max(-18, min(24, len(plus) // 35 - len(minus) // 55))


def instance_score(inst, arena_code="ruins"):
    card = CARD_BY_ID[inst["card_id"]]
    lvl = int(inst.get("level", 1))
    stats = card.get("stats", {})
    base_power = int(sum(stats.values()) * 0.72)
    rarity_bonus = int(RARITY_BONUS.get(card.get("rarity"), 0) * 0.75)
    level_bonus = int((lvl - 1) * 6.0)
    hp_factor = int(card_hp(card, lvl) / 55)
    artifact_bonus = int(sum(inst.get("artifact", {}).get("delta", {}).values()) * 1.3)
    buff_bonus = int(sum(inst.get("buff", {}).get("delta", {}).values()) * 0.9)
    debuff_penalty = int(sum(inst.get("debuff", {}).get("delta", {}).values()) * 0.75)
    arena_bonus = role_bonus(card, arena_code)
    pm = plus_minus_score(card)
    return base_power + rarity_bonus + level_bonus + hp_factor + artifact_bonus + buff_bonus + debuff_penalty + arena_bonus + pm


def team_score(team, arena_code="ruins"):
    total = sum(instance_score(i, arena_code) for i in team)
    animes = [CARD_BY_ID[i["card_id"]]["anime"] for i in team if i.get("card_id") in CARD_BY_ID]
    roles = [CARD_BY_ID[i["card_id"]].get("role", "") for i in team if i.get("card_id") in CARD_BY_ID]
    synergy_bonus = len(set(animes)) * 8
    duplicate_penalty = (len(animes) - len(set(animes))) * 6
    if any("саппорт" in r or "защ" in r for r in roles):
        synergy_bonus += 18
    if any("танк" in r or "гигант" in r for r in roles) and any("скор" in r or "ассас" in r for r in roles):
        synergy_bonus += 16
    total += synergy_bonus - duplicate_penalty
    return total

def card_short(card, index=None):
    prefix = f"<b>Вариант {index}</b>\n" if index else ""
    return (
        f"{prefix}🐉 <b>{e(card['name'])}</b>\n"
        f"{rarity_label_for_card(card)}\n"
        f"🌍 Аниме: {e(card['anime'])}\n"
        f"🎭 Мод: {e(card['form'])}\n"
        f"📖 {e(card.get('description', ''))}\n"
        f"⚔️ Сила: <b>{card_power(card)}</b>\n"
        f"🎯 Роль: {e(card['role'])}\n"
        f"➕ {e(card['plus'])}\n"
        f"➖ {e(card['minus'])}"
    )


def format_instance(inst, n):
    c = CARD_BY_ID[inst["card_id"]]
    lvl = int(inst.get("level", 1))
    return (
        f"{n}. 🐉 <b>{e(c['name'])}</b> — {rarity_label_for_card(c)}\n"
        f"   🌍 {e(c['anime'])} | 🎭 {e(c['form'])}\n"
        f"   📈 Ур. {lvl}/{MAX_LEVEL} | ⚔️ Сила: {card_power(c, lvl)}\n"
        f"   ➕ {e(inst['buff']['name'])}: {e(inst['buff']['text'])}\n"
        f"   ➖ {e(inst['debuff']['name'])}: {e(inst['debuff']['text'])}\n"
        f"   🗡 {e(inst['artifact']['name'])}: {e(inst['artifact']['text'])}"
    )


def ordered_team(team, starter_idx=0):
    if not team:
        return []
    starter_idx = max(0, min(int(starter_idx or 0), len(team) - 1))
    return team[starter_idx:] + team[:starter_idx]


def duel_score(inst):
    return instance_score(inst) + random.randint(-28, 28)


def duel_line(round_no, left_name, right_name, left_inst, right_inst, arena_code="ruins"):
    left_card = CARD_BY_ID[left_inst["card_id"]]
    right_card = CARD_BY_ID[right_inst["card_id"]]
    left_score = duel_score(left_inst, right_inst, arena_code)
    right_score = duel_score(right_inst, left_inst, arena_code)
    if left_score == right_score:
        left_score += random.randint(0, 8)
        right_score += random.randint(0, 8)
    result = 1 if left_score >= right_score else -1
    winner_name = left_name if result == 1 else right_name
    winner_inst = left_inst if result == 1 else right_inst
    loser_inst = right_inst if result == 1 else left_inst
    diff = abs(left_score - right_score)
    arena_bonus = ARENA_EFFECTS.get(arena_code, ("нейтрально", "нейтрально"))[0]
    text = (
        f"⚔️ <b>Раунд {round_no}</b>\n"
        f"{e(left_card['name'])} <b>{left_score}</b> : <b>{right_score}</b> {e(right_card['name'])}\n"
        f"➕ Плюс: {e(winner_inst['buff']['name'])}\n"
        f"➖ Минус: {e(loser_inst['debuff']['name'])}\n"
        f"✦ Эффект арены: {e(arena_bonus)}\n"
        f"✅ Очко: <b>{e(winner_name)}</b> · разница {diff}"
    )
    return result, text

def resolve_step_battle(left_name, right_name, left_team, right_team, arena_code="ruins", left_starter=0, right_starter=0):
    left_order = ordered_team(left_team, left_starter)
    right_order = ordered_team(right_team, right_starter)
    left_points = 0
    right_points = 0
    lines = []
    rounds = min(len(left_order), len(right_order))
    for i in range(rounds):
        result, line = duel_line(i + 1, left_name, right_name, left_order[i], right_order[i], arena_code)
        if result == 1:
            left_points += 1
        else:
            right_points += 1
        lines.append(line)

    left_total = team_score(left_team, arena_code) + random.randint(-35, 35)
    right_total = team_score(right_team, arena_code) + random.randint(-35, 35)

    if left_points == right_points:
        winner = left_name if left_total >= right_total else right_name
        tie_text = f"⚖️ По очкам ничья. Решила общая сила команды: {left_total} vs {right_total}.\n"
    else:
        winner = left_name if left_points > right_points else right_name
        tie_text = ""

    summary = (
        f"📊 <b>Счёт по раундам:</b> {e(left_name)} {left_points} : {right_points} {e(right_name)}\n"
        f"{tie_text}"
        f"🏆 <b>Победитель:</b> {e(winner)}"
    )
    return winner, left_points, right_points, "\n".join(lines), summary


def _html_to_plain_text(text):
    # Safe fallback for oversized generated screens: formatting is preferable,
    # but never at the cost of cutting Telegram HTML in the middle of a tag.
    value = re.sub(r"<br\s*/?>", "\n", str(text), flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", "", value)
    return unescape(value)


async def send_long(message, text, reply_markup=None):
    if len(text) <= 3900:
        await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
        return
    plain = _html_to_plain_text(text)
    parts = []
    while len(plain) > 3900:
        cut = plain.rfind("\n", 0, 3900)
        if cut < 1:
            cut = 3900
        parts.append(plain[:cut])
        plain = plain[cut:].lstrip("\n")
    parts.append(plain)
    for part in parts[:-1]:
        await message.answer(part)
    await message.answer(parts[-1], reply_markup=reply_markup)


async def set_commands():
    public_commands = [
        BotCommand(command="menu", description="Меню"),
        BotCommand(command="draw", description="Призвать карту"),
        BotCommand(command="collection", description="Мои карты"),
        BotCommand(command="season", description="Текущий сезон"),
        BotCommand(command="pass", description="MultiPass"),
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="battle", description="Арена с ботом"),
        BotCommand(command="online", description="Онлайн-бой"),
        BotCommand(command="profile", description="Профиль"),
        BotCommand(command="shop", description="Магазин"),
        BotCommand(command="clan", description="Клан"),
        BotCommand(command="daily", description="Ежедневная награда"),
        BotCommand(command="craft", description="Крафт"),
        BotCommand(command="rating", description="Рейтинг"),
        BotCommand(command="friends", description="Друзья"),
        BotCommand(command="addfriend", description="Добавить друга по ID"),
        BotCommand(command="promo", description="Промокод"),
        BotCommand(command="rules", description="Правила"),
        BotCommand(command="myid", description="Мой Telegram ID"),
        BotCommand(command="nick", description="Сменить ник"),
        BotCommand(command="events", description="Ивенты, рейд и турнир"),
        BotCommand(command="commands", description="Все команды"),
        BotCommand(command="findcard", description="Поиск карты"),
    ]
    await bot.set_my_commands(public_commands, scope=BotCommandScopeDefault())
    owner_commands = public_commands + [
        BotCommand(command="admin", description="Админ-панель владельца"),
        BotCommand(command="user", description="Открыть игрока по ID"),
        BotCommand(command="ban", description="Заблокировать игрока"),
        BotCommand(command="unban", description="Разблокировать игрока"),
        BotCommand(command="freeze", description="Заморозить аккаунт"),
        BotCommand(command="unfreeze", description="Снять заморозку"),
        BotCommand(command="givef", description="Выдать фисташки"),
        BotCommand(command="givemoon", description="Выдать драконит"),
        BotCommand(command="givecard", description="Выдать карту"),
        BotCommand(command="deleteuser", description="Удалить игрока с подтверждением"),
        BotCommand(command="compensate_patch24", description="Компенсация текущего обновления"),
        BotCommand(command="givepass", description="Выдать мультипасс на дни"),
        BotCommand(command="takepass", description="Снять мультипасс"),
        BotCommand(command="storage", description="Статус хранилища"),
        BotCommand(command="flush_data", description="Сохранить прогресс сейчас"),
    ]
    for oid in owner_ids():
        try:
            await bot.set_my_commands(owner_commands, scope=BotCommandScopeChat(chat_id=int(oid)))
        except Exception:
            pass


async def set_bot_public_description():
    """Описание, которое новый игрок видит в окне бота до нажатия /start."""
    try:
        await bot.set_my_short_description(short_description=BOT_SHORT_DESCRIPTION)
    except Exception as ex:
        logger.warning("Could not set short bot description: %s", ex)
    try:
        await bot.set_my_description(description=BOT_PUBLIC_DESCRIPTION)
    except Exception as ex:
        logger.warning("Could not set bot description: %s", ex)


@dp.message(Command("appeal"))
async def appeal_text_cmd(message: types.Message):
    p = get_user_data(message.from_user)
    text = message.text.replace("/appeal", "", 1).strip()
    if not text:
        await message.answer(
            "⚖️ <b>Оспаривание боя</b>\n\n"
            "Напиши так:\n<code>/appeal почему результат боя неверный</code>\n\n"
            "Если спор примут вручную, награда: 1000 💎 фисташек + 250 фрагментов случайной карты до легендарной редкости.",
            parse_mode="HTML", reply_markup=back_menu()
        )
        return
    msg = (
        f"⚖️ <b>Новый спор</b>\n"
        f"Игрок: {e(p.get('name', message.from_user.full_name))}\n"
        f"ID: <code>{message.from_user.id}</code>\n\n"
        f"{e(text)}"
    )
    targets = list(owner_ids() | right_hand_ids())
    sent = 0
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="👤 Профиль", callback_data=f"admin_user:{message.from_user.id}"), button(text="💎 +1000", callback_data=f"appeal_grant:{message.from_user.id}:1000")],
        [button(text="✉️ Ответить", callback_data=f"appeal_reply:{message.from_user.id}"), button(text="❌ Отклонить", callback_data=f"appeal_reject:{message.from_user.id}")],
    ])
    for tid in targets:
        try:
            await bot.send_message(int(tid), msg, parse_mode="HTML", reply_markup=kb)
            sent += 1
        except Exception:
            pass
    await message.answer(
        "✅ Спор отправлен поддержке. Если аргумент сильный, награду выдадут вручную.",
        reply_markup=back_menu()
    )


@dp.callback_query(F.data.startswith("appeal_grant:"))
async def appeal_grant_cb(callback: types.CallbackQuery):
    if not (is_owner(callback.from_user.id) or is_right_hand(callback.from_user.id)):
        await callback.answer("Нет доступа.", show_alert=True); return
    parts = callback.data.split(":")
    uid = parts[1]
    amount = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1000
    p = DATA.get("users", {}).get(str(uid))
    if not p:
        await callback.answer("Игрок не найден.", show_alert=True); return
    p["fistiks"] = int(p.get("fistiks",0) or 0) + amount
    mark_data_dirty("data_changed")
    try:
        await bot.send_message(int(uid), f"⚖️ <b>Спор принят</b>\n\nТебе начислено +<b>{amount}</b> 💎 Фисташек.", parse_mode="HTML")
    except Exception:
        pass
    await callback.message.answer(f"✅ Выдано +{amount} 💎 игроку {uid}.")
    await callback.answer("Выдано")

@dp.callback_query(F.data.startswith("appeal_reject:"))
async def appeal_reject_cb(callback: types.CallbackQuery):
    if not (is_owner(callback.from_user.id) or is_right_hand(callback.from_user.id)):
        await callback.answer("Нет доступа.", show_alert=True); return
    uid = callback.data.split(":",1)[1]
    try:
        await bot.send_message(int(uid), "⚖️ <b>Спор рассмотрен</b>\n\nКомпенсация не выдана. Если есть подробности — отправь новый спор через /appeal.", parse_mode="HTML")
    except Exception:
        pass
    await callback.message.answer(f"❌ Спор игрока {uid} отклонён.")
    await callback.answer("Отклонено")

@dp.callback_query(F.data.startswith("appeal_reply:"))
async def appeal_reply_cb(callback: types.CallbackQuery):
    if not (is_owner(callback.from_user.id) or is_right_hand(callback.from_user.id)):
        await callback.answer("Нет доступа.", show_alert=True); return
    uid = callback.data.split(":",1)[1]
    await callback.message.answer(
        f"✉️ Чтобы ответить игроку, напиши:\n<code>/replyappeal {uid} твой текст</code>",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(Command("replyappeal"))
async def reply_appeal_cmd(message: types.Message):
    if not (is_owner(message.from_user.id) or is_right_hand(message.from_user.id)):
        await message.answer("Нет доступа."); return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer("Формат: /replyappeal USER_ID текст")
        return
    uid, text = parts[1], parts[2]
    try:
        await bot.send_message(int(uid), f"⚖️ <b>Ответ поддержки</b>\n\n{e(text)}", parse_mode="HTML")
        await message.answer("✅ Ответ отправлен.")
    except Exception as ex:
        await message.answer(f"Не удалось отправить: {e(str(ex))}", parse_mode="HTML")

@dp.message(Command("grantappeal"))
async def grant_appeal_cmd(message: types.Message):
    if not (is_owner(message.from_user.id) or is_right_hand(message.from_user.id)):
        await message.answer("Нет доступа.")
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Формат: /grantappeal USER_ID")
        return
    uid = parts[1]
    if uid not in DATA["users"]:
        await message.answer("Пользователь не найден в базе.")
        return
    player = DATA["users"][uid]
    player["fistiks"] = player.get("fistiks", 0) + 1000
    eligible = [c for c in CARDS if c["rarity"] in ["Обычный", "Редкий", "Эпический", "Легендарный"]]
    card = random.choice(eligible)
    add_fragments(player, card["id"], 250)
    mark_data_dirty("data_changed")
    await message.answer(f"✅ Выдано: 1000 💎 + 250 фрагментов {card['name']} игроку {uid}.")
    try:
        await bot.send_message(int(uid), f"🎁 Спор принят: +1000 💎 фисташек и +250 фрагментов {card['name']}.")
    except Exception:
        pass

@dp.message(Command("myid"))
async def myid(message: types.Message):
    text = f"🆔 Твой Telegram ID:\n<code>{message.from_user.id}</code>"
    if is_owner(message.from_user.id):
        text += "\n\n👑 Режим владельца активен."
    await message.answer(text, parse_mode="HTML", reply_markup=back_menu())


@dp.message(Command("paysupport"))
async def paysupport_cmd(message: types.Message):
    get_user_data(message.from_user)
    text = (
        "🧾 <b>Помощь по оплатам</b>\n\n"
        "Если Stars списались, а награда не пришла:\n"
        "1. Скопируй дату и примерное время оплаты.\n"
        "2. Напиши владельцу свой ID через /myid.\n"
        "3. Укажи, что покупал: мультипасс или Stars-набор.\n\n"
        "Владелец видит оплаты в админ-панели и может вручную проверить игрока."
    )
    await message.answer(text, reply_markup=back_menu(), parse_mode="HTML")


@dp.message(Command("commands"))
async def commands_cmd(message: types.Message):
    text = (
        "📋 <b>Команды</b>\n\n"
        "/start — главное меню\n"
        "/battle — бой с ботом\n"
        "/online — онлайн-бой\n"
        "/collection — коллекция и фильтры\n"
        "/findcard имя — поиск карты\n"
        "/profile — профиль\n"
        "/shop — магазин, сундуки, кейсы\n"
        "/daily — ежедневная награда\n"
        "/craft — крафт\n"
        "/rating — рейтинг\n"
        "/friends — друзья и рефералка\n"
        "/addfriend ID — добавить друга\n"
        "/promo КОД — промокод\n"
        "/pass — мультипасс\n"
        "/nick НовыйНик — сменить ник\n"
        "/events — ивенты, турнир и рейд\n"
        "/rules — правила\n"
        "/myid — твой ID\n"
    )
    if is_owner(message.from_user.id):
        text += (
            "\n🛠 <b>Команды владельца</b>\n"
            "/admin — командный центр\n"
            "/user ID — открыть аккаунт\n"
            "/ban ID / /unban ID — бан/разбан\n"
            "/freeze ID / /unfreeze ID — заморозка\n"
            "/givef ID AMOUNT — выдать фисташки\n"
            "/givemoon ID AMOUNT — выдать драконит\n"
            "/givecard ID CARD_ID — выдать карту\n"
            "/deleteuser ID — удалить только после подтверждения\n"
        )
    await message.answer(text, parse_mode="HTML", reply_markup=back_menu())




def card_effective_universe(card):
    return card_draw_universe(card)


def starter_card_allowed(card, universe_id, leader=False):
    if not isinstance(card, dict) or not card.get("id"):
        return False
    if card_effective_universe(card) != universe_id:
        return False
    rarity = str(card.get("rarity", "") or "")
    allowed = {"Обычный", "Редкий", "Эпический"} if leader else {"Обычный", "Редкий"}
    if rarity not in allowed:
        return False
    if card.get("premium_only") or card.get("season_exclusive") or card.get("super_absolute"):
        return False
    return True


def _stable_card_order(cards, seed_text):
    def key(card):
        cid = str(card.get("id", ""))
        art_priority = 0 if cid in REAL_MEDIA_IDS else 1
        digest = hashlib.sha256(f"{seed_text}:{cid}".encode("utf-8")).hexdigest()
        return (art_priority, digest)
    return sorted(cards, key=key)


def onboarding_leader_options(user_id, universe_id):
    candidates = [c for c in CARDS if starter_card_allowed(c, universe_id, leader=True)]
    # Prefer Epic/Rare leaders while still keeping ordinary fallback safe.
    rarity_rank = {"Эпический": 0, "Редкий": 1, "Обычный": 2}
    candidates = sorted(
        candidates,
        key=lambda c: (
            rarity_rank.get(str(c.get("rarity", "")), 9),
            0 if c.get("id") in REAL_MEDIA_IDS else 1,
            hashlib.sha256(f"leader:{user_id}:{universe_id}:{c.get('id')}".encode()).hexdigest(),
        ),
    )
    result = []
    for c in candidates:
        cid = str(c.get("id"))
        if cid not in result:
            result.append(cid)
        if len(result) == 3:
            break
    return result


def build_starter_team(player, user_id, universe_id, leader_id):
    owned = set(str(cid) for cid in (player.get("collection", {}) or {}))
    if leader_id in owned:
        return []
    leader = CARD_BY_ID.get(leader_id)
    if not starter_card_allowed(leader, universe_id, leader=True):
        return []
    teammates = [
        c for c in CARDS
        if c.get("id") != leader_id
        and c.get("id") not in owned
        and starter_card_allowed(c, universe_id, leader=False)
    ]
    teammates = _stable_card_order(teammates, f"team:{user_id}:{universe_id}:{leader_id}")
    picked = [leader_id] + [str(c["id"]) for c in teammates[:4]]
    if len(picked) < STARTER_CARD_COUNT:
        fallback = [
            c for c in CARDS
            if c.get("id") not in set(picked)
            and c.get("id") not in owned
            and starter_card_allowed(c, universe_id, leader=True)
        ]
        fallback = _stable_card_order(fallback, f"fallback:{user_id}:{universe_id}:{leader_id}")
        for c in fallback:
            picked.append(str(c["id"]))
            if len(picked) == STARTER_CARD_COUNT:
                break
    return picked if len(picked) == STARTER_CARD_COUNT and len(set(picked)) == STARTER_CARD_COUNT else []


async def send_onboarding_universe_menu(message, user, page=0):
    player = get_user_data(user)
    if player.get("onboarding_complete"):
        await send_main_dashboard(message, user, show_banner=False)
        return
    universes = visible_universes_for_menu()
    per_page = 8
    pages = max(1, (len(universes) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    shown = universes[page * per_page:(page + 1) * per_page]
    rows = []
    pair = []
    for rec in shown:
        uid = str(rec["id"])
        pair.append(button(text=f"{universe_emoji(uid)} {rec['name'][:22]}", callback_data=f"onboard:u:{uid}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"onboard:page:{page-1}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page + 1 < pages:
        nav.append(button(text="➡️", callback_data=f"onboard:page:{page+1}"))
    rows.append(nav)
    text = (
        "🌌 <b>ДОБРО ПОЖАЛОВАТЬ В ANIME BATTLE MULTIVERSE</b>\n\n"
        "<b>Шаг 1/2.</b> Выбери вселенную стартового отряда.\n"
        "Ты получишь одного лидера и ещё четыре уникальные стартовые карты из выбранного мира.\n\n"
        "<i>Старые аккаунты этот старт не проходят и повторных бонусов не получают.</i>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def send_onboarding_leader_menu(message, user):
    player = get_user_data(user)
    if player.get("onboarding_complete"):
        await send_main_dashboard(message, user, show_banner=False)
        return
    universe_id = str(player.get("preferred_universe", "") or "")
    if universe_id not in UNIVERSE_BY_ID:
        player["onboarding_state"] = "choose_universe"
        mark_data_dirty("onboarding_repair")
        await send_onboarding_universe_menu(message, user, 0)
        return
    options = [cid for cid in player.get("onboarding_leader_options", []) if cid in CARD_BY_ID]
    if len(options) != 3:
        options = onboarding_leader_options(user.id, universe_id)
        player["onboarding_leader_options"] = options
        mark_data_dirty("onboarding_leaders")
    if len(options) < 3:
        await message.answer("⚠️ Для этой вселенной не удалось безопасно собрать три стартовых лидера. Выбери другой мир.")
        await send_onboarding_universe_menu(message, user, 0)
        return
    rows = []
    lines = [
        "👑 <b>ШАГ 2/2 · ВЫБОР ЛИДЕРА</b>",
        f"Вселенная: <b>{e(universe_label(universe_id))}</b>",
        "",
    ]
    for index, cid in enumerate(options):
        card = CARD_BY_ID[cid]
        lines.append(f"<b>{index + 1}.</b> {e(card.get('name', cid))} · {e(card.get('form', 'Базовая форма'))} · {rarity_label_for_card(card)}")
        rows.append([button(text=f"{index + 1}. {card.get('name', cid)[:35]}", callback_data=f"onboard:leader:{index}")])
    rows.append([button(text="⬅️ Сменить вселенную", callback_data="onboard:page:0")])
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def resume_onboarding(message, user):
    player = get_user_data(user)
    if player.get("onboarding_complete"):
        await send_main_dashboard(message, user, show_banner=False)
        return
    state = str(player.get("onboarding_state", "choose_universe") or "choose_universe")
    if state in {"choose_leader", "leader"} and player.get("preferred_universe"):
        await send_onboarding_leader_menu(message, user)
    else:
        await send_onboarding_universe_menu(message, user, 0)


@dp.callback_query(F.data.startswith("onboard:page:"))
async def onboarding_page_cb(callback: types.CallbackQuery):
    try:
        page = int(callback.data.rsplit(":", 1)[1])
    except Exception:
        page = 0
    await send_onboarding_universe_menu(callback.message, callback.from_user, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("onboard:u:"))
async def onboarding_universe_cb(callback: types.CallbackQuery):
    universe_id = callback.data.split(":", 2)[2]
    if universe_id not in UNIVERSE_BY_ID:
        await callback.answer("Вселенная не найдена.", show_alert=True)
        return
    player = get_user_data(callback.from_user)
    if player.get("onboarding_complete"):
        await callback.answer("Стартовый набор уже получен.", show_alert=True)
        return
    options = onboarding_leader_options(callback.from_user.id, universe_id)
    if len(options) < 3:
        await callback.answer("Для этой вселенной недостаточно безопасных стартовых карт.", show_alert=True)
        return
    player["preferred_universe"] = universe_id
    player["onboarding_state"] = "choose_leader"
    player["onboarding_leader_options"] = options
    mark_data_dirty("onboarding_universe")
    await flush_data_now_async("onboarding_universe")
    await send_onboarding_leader_menu(callback.message, callback.from_user)
    await callback.answer(f"Выбрано: {universe_label(universe_id)}")


@dp.callback_query(F.data.startswith("onboard:leader:"))
async def onboarding_leader_cb(callback: types.CallbackQuery):
    try:
        index = int(callback.data.rsplit(":", 1)[1])
    except Exception:
        index = -1
    player = get_user_data(callback.from_user)
    if player.get("onboarding_complete") or player.get("starter_bundle_claimed"):
        await callback.answer("Стартовый набор уже получен.", show_alert=True)
        return
    universe_id = str(player.get("preferred_universe", "") or "")
    options = [cid for cid in player.get("onboarding_leader_options", []) if cid in CARD_BY_ID]
    if not (0 <= index < len(options)):
        await callback.answer("Выбор лидера устарел. Открой старт заново.", show_alert=True)
        await resume_onboarding(callback.message, callback.from_user)
        return
    leader_id = options[index]
    team = build_starter_team(player, callback.from_user.id, universe_id, leader_id)
    if len(team) != STARTER_CARD_COUNT:
        await callback.answer("Не удалось безопасно собрать отряд. Выбери другого лидера.", show_alert=True)
        return
    # Critical idempotent grant. Per-user serialization prevents concurrent double execution.
    for cid in team:
        player.setdefault("collection", {})[cid] = {"count": 1, "shards": 0, "level": 1, "unlocked": True}
    player["starter_cards"] = list(team)
    player["deck"] = list(team)
    player["auto_team"] = False
    player["card_attempts"] = int(player.get("card_attempts", 0) or 0) + STARTER_ATTEMPTS
    add_season_xp(player, STARTER_SEASON_SP, action_key=f"onboarding:{ONBOARDING_VERSION}")
    player["starter_bundle_claimed"] = True
    player["onboarding_complete"] = True
    player["onboarding_state"] = "complete"
    player["universe_onboarding_seen"] = UNIVERSE_ONBOARDING_VERSION
    player["onboarding_completed_at"] = utc_now().isoformat()
    mark_data_dirty("onboarding_complete")
    saved = await flush_data_now_async("onboarding_complete")
    leader = CARD_BY_ID[leader_id]
    lines = [
        "🚀 <b>СТАРТОВЫЙ ОТРЯД ГОТОВ</b>",
        f"👑 Лидер: <b>{e(leader.get('name', leader_id))}</b>",
        f"🌌 Вселенная: <b>{e(universe_label(universe_id))}</b>",
        "",
        "🃏 <b>Твои пять карт:</b>",
    ]
    for cid in team:
        card = CARD_BY_ID[cid]
        lines.append(f"• {e(card.get('name', cid))} · {rarity_label_for_card(card)}")
    lines.extend([
        "",
        f"💎 Баланс: <b>{short_number(player.get('fistiks', 0))}</b>",
        f"🎴 Доп. попытки: <b>{short_number(player.get('card_attempts', 0))}</b>",
        f"⚡ Сезон: <b>{short_number(player.get('season_xp', 0))} SP</b>",
    ])
    if not saved:
        lines.append("\n⚠️ Облачное сохранение временно не подтвердилось; повторная запись поставлена в очередь.")
    await callback.message.answer("\n".join(lines), parse_mode="HTML", reply_markup=main_menu(callback.from_user.id))
    await ensure_quick_keyboard(callback.message, callback.from_user)
    await callback.answer("Отряд получен!")


@dp.message(CommandStart())
async def start(message: types.Message):
    player = get_user_data(message.from_user)
    text = (message.text or "").strip()
    payload = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) > 1 else ""

    if payload.startswith("friend_"):
        await accept_friend_invite(message, payload.split("friend_", 1)[1].strip())
        return
    if payload.startswith("ref_"):
        await accept_direct_referral(message, payload.split("ref_", 1)[1].strip())
        return

    if not player.get("onboarding_complete"):
        await resume_onboarding(message, message.from_user)
        return

    first_relaunch_view = not bool(player.get("patch40_dashboard_seen"))
    player["patch40_dashboard_seen"] = True
    mark_data_dirty("dashboard_seen")
    await send_main_dashboard(message, message.from_user, show_banner=first_relaunch_view)
    await ensure_quick_keyboard(message, message.from_user)


@dp.callback_query(F.data == "menu")
async def menu(callback: types.CallbackQuery):
    get_user_data(callback.from_user)
    await send_main_dashboard(callback.message, callback.from_user, show_banner=False)
    await callback.answer()


@dp.message(Command("menu"))
async def menu_cmd(message: types.Message):
    get_user_data(message.from_user)
    await send_main_dashboard(message, message.from_user, show_banner=False)
    await ensure_quick_keyboard(message, message.from_user)

@dp.message(Command("draw", "card", "getcard"))
async def draw_card_cmd(message: types.Message):
    await draw_card_to_message(message, message.from_user)



async def send_profile(message, user):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    scope_uid = selected_universe_id(p)
    unique, scope_total = universe_progress(p, scope_uid)
    role = "Владелец мультивселенной" if is_owner(user.id) else player_title(p)
    await message.answer(
        "👤 <b>ПРОФИЛЬ</b>\n\n"
        f"<blockquote>👤 <b>{e(p.get('name', user.full_name))}</b>\n"
        f"🏷 Статус: <b>{e(role)}</b>\n"
        f"🆔 ID: <code>{user.id}</code>\n"
        f"🌌 Вселенная: <b>{e(universe_label(scope_uid))}</b>\n"
        f"🏰 Клан: <b>{e(clan_name_for(p))}</b>\n"
        f"🃏 Персонажи: <b>{short_number(unique)}/{short_number(scope_total)}</b></blockquote>\n"
        f"<i>{short_number(unique)}/{short_number(scope_total)} означает: открыто {short_number(unique)} уникальных персонажей из {short_number(scope_total)}, относящихся к выбранной вселенной. Дубликаты в это число не входят — они становятся фрагментами усиления.</i>",
        reply_markup=profile_menu(user.id), parse_mode="HTML"
    )

async def send_profile_stats(message, user):
    p = get_user_data(user)
    lvl, rem, nxt = calc_user_level(p.get("xp", 0))
    battles = int(p.get("battles", 0))
    wins = int(p.get("wins", 0))
    losses = int(p.get("losses", 0))
    winrate = round((wins / battles) * 100, 1) if battles else 0
    await message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"⭐ Уровень: <b>{lvl}</b> ({rem}/{nxt} XP)\n"
        f"{PISTACHIOS_LABEL}: <b>{short_number(p.get('fistiks', 0))}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{short_number(p.get('moon_coins', 0))}</b>\n\n"
        f"⚔️ Боёв: <b>{battles}</b>\n"
        f"🏆 Побед: <b>{wins}</b>\n"
        f"💀 Поражений: <b>{losses}</b>\n"
        f"📈 Винрейт: <b>{winrate}%</b>",
        reply_markup=profile_menu(user.id),
        parse_mode="HTML"
    )


async def send_profile_badges(message, user):
    p = get_user_data(user)
    badges = p.get("badges", [])
    text = "🏷 <b>Знаки</b>\n\n"
    if badges:
        for b in badges:
            text += f"• {e(badge_title(b))}\n"
    else:
        text += "Пока знаков нет. Их можно получить в магазине, ивентах или за особые действия."
    await message.answer(text, reply_markup=profile_menu(user.id), parse_mode="HTML")



@dp.message(Command("profile"))
async def profile_cmd(message: types.Message):
    await send_profile(message, message.from_user)


@dp.callback_query(F.data == "deck")
async def deck_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    owned = [(cid, info) for cid, info in p.get("collection", {}).items() if cid in CARD_BY_ID and int(info.get("count", 0)) > 0]
    owned.sort(key=lambda x: card_power(CARD_BY_ID[x[0]], int(x[1].get("level", 1))), reverse=True)

    if p.get("auto_team", True) or len([cid for cid in p.get("deck", []) if cid in CARD_BY_ID]) < 5:
        p["deck"] = [cid for cid, _info in owned[:5]]
        p["auto_team"] = True
        mark_data_dirty("data_changed")

    text = "🧬 <b>Колода</b>\n\n"
    text += f"Автосбор команды: <b>{'включён' if p.get('auto_team', True) else 'выключен'}</b>\n"
    text += "Можно автособрать топ-5 или вручную поставить карту в каждый слот.\n\n"

    deck_ids = [cid for cid in p.get("deck", []) if cid in CARD_BY_ID]
    text += "<b>Текущая команда 5 бойцов:</b>\n"
    for n in range(5):
        if n < len(deck_ids):
            cid = deck_ids[n]
            c = CARD_BY_ID[cid]
            lvl = int(p.get("collection", {}).get(cid, {}).get("level", 1))
            text += f"{n+1}. {rarity_label_for_card(c)} <b>{e(c['name'])}</b> | ур. {lvl}/{MAX_LEVEL} | сила {short_number(card_power(c, lvl))}\n"
        else:
            text += f"{n+1}. — пустой слот\n"

    kb_rows = [
        [button(text="🧠 Автособрать топ-5", callback_data="auto_build_deck"), button(text="🔁 Автосбор", callback_data="toggle_auto_team")],
        [button(text="⚡ Автоулучшить доступное", callback_data="auto_upgrade")],
    ]
    kb_rows.append([
        button(text="Слот 1", callback_data="deck_slot:0:0"),
        button(text="Слот 2", callback_data="deck_slot:1:0"),
        button(text="Слот 3", callback_data="deck_slot:2:0"),
    ])
    kb_rows.append([
        button(text="Слот 4", callback_data="deck_slot:3:0"),
        button(text="Слот 5", callback_data="deck_slot:4:0"),
    ])
    kb_rows.append([button(text="⚔️ В бой", callback_data="battle:start")])
    kb_rows.append([button(text="⬅️ Режимы", callback_data="modes"), button(text="🏠 Меню", callback_data="menu")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "auto_build_deck")
async def auto_build_deck_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    deck = best_owned_card_ids(callback.from_user.id, 5)
    if len(deck) < 5:
        await callback.answer("Нужно минимум 5 открытых карт.", show_alert=True)
        return
    p["deck"] = deck
    p["auto_team"] = True
    mark_data_dirty("data_changed")
    await callback.message.answer("🧠 Колода собрана автоматически: поставлены 5 сильнейших открытых карт.", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data == "toggle_auto_team")
async def toggle_auto_team_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    p["auto_team"] = not bool(p.get("auto_team", True))
    if p["auto_team"]:
        p["deck"] = best_owned_card_ids(callback.from_user.id, 5)
    mark_data_dirty("data_changed")
    await callback.message.answer(f"🔁 Автосбор команды: <b>{'включён' if p['auto_team'] else 'выключен'}</b>", reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


async def send_deck_slot(message, user, slot=0, page=0):
    p = get_user_data(user)
    owned = [(cid, info) for cid, info in p.get("collection", {}).items() if cid in CARD_BY_ID and int(info.get("count", 0)) > 0]
    owned.sort(key=lambda x: card_power(CARD_BY_ID[x[0]], int(x[1].get("level", 1))), reverse=True)
    if not owned:
        await message.answer("Нет открытых карт для выбора в колоду.", reply_markup=back_menu())
        return
    per_page = 8
    pages = max(1, (len(owned) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    rows = []
    text = f"🎯 <b>Выбор карты в слот {slot+1}</b> — страница {page+1}/{pages}\n\n"
    for cid, info in owned[page*per_page:(page+1)*per_page]:
        c = CARD_BY_ID[cid]
        lvl = int(info.get("level", 1))
        text += f"• {rarity_label_for_card(c)} <b>{e(c['name'])}</b> | ур.{lvl} | сила {short_number(card_power(c,lvl))}\n"
        rows.append([button(text=f"Поставить: {c['name'][:28]}", callback_data=f"deck_set:{slot}:{card_cb_id(cid)}")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"deck_slot:{slot}:{page-1}"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"deck_slot:{slot}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([button(text="⬅️ Колода", callback_data="deck")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data.startswith("deck_slot:"))
async def deck_slot_cb(callback: types.CallbackQuery):
    try:
        _, slot_s, page_s = callback.data.split(":")
        slot = max(0, min(4, int(slot_s)))
        page = max(0, int(page_s))
    except Exception:
        await callback.answer("Ошибка слота.", show_alert=True)
        return
    await send_deck_slot(callback.message, callback.from_user, slot, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("deck_set:"))
async def deck_set_cb(callback: types.CallbackQuery):
    try:
        _, slot_s, cid = callback.data.split(":", 2)
        cid = resolve_card_id(cid)
        slot = max(0, min(4, int(slot_s)))
    except Exception:
        await callback.answer("Ошибка выбора карты.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p.get("collection", {}) or int(p["collection"][cid].get("count", 0)) <= 0:
        await callback.answer("Этой карты нет в твоей коллекции.", show_alert=True)
        return
    deck = [x for x in p.get("deck", []) if x in CARD_BY_ID]
    while len(deck) < 5:
        deck.append("")
    # Убираем карту из другого слота, чтобы не было дубля в одной команде.
    deck = ["" if x == cid else x for x in deck]
    deck[slot] = cid
    p["deck"] = [x for x in deck if x]
    p["auto_team"] = False
    mark_data_dirty("data_changed")
    await callback.message.answer(f"✅ В слот {slot+1} поставлен: <b>{e(CARD_BY_ID[cid]['name'])}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Колода", callback_data="deck")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "auto_upgrade")
async def auto_upgrade_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    upgraded = 0
    for cid, info in list(p.get("collection", {}).items()):
        if cid not in CARD_BY_ID or int(info.get("count", 0)) <= 0:
            continue
        c = CARD_BY_ID[cid]
        while info.get("level", 1) < MAX_LEVEL:
            cost = level_cost(info.get("level", 1), c["rarity"])
            if cost is None or info.get("shards", 0) < cost:
                break
            info["shards"] -= cost
            info["level"] += 1
            upgraded += 1
            if upgraded >= 50:
                break
        if upgraded >= 50:
            break
    mark_data_dirty("data_changed")
    await callback.message.answer(f"⚡ Автоулучшение завершено. Повышений уровня: {upgraded}.", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data == "profile")
async def profile_cb(callback: types.CallbackQuery):
    await send_profile(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "profile_stats")
async def profile_stats_cb(callback: types.CallbackQuery):
    await send_profile_stats(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "profile_badges")
async def profile_badges_cb(callback: types.CallbackQuery):
    await send_profile_badges(callback.message, callback.from_user)
    await callback.answer()


async def send_notify_settings(message, user):
    p = get_user_data(user)
    enabled = bool(p.get("notify_free_pack", True))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text=("🔕 Выключить" if enabled else "🔔 Включить"), callback_data="notify_toggle")],
        [button(text="⬅️ Профиль", callback_data="profile"), button(text="🏠 Меню", callback_data="menu")],
    ])
    await message.answer(
        f"{CE['profile']} <b>Уведомления</b>\n\n"
        f"Напоминание о бесплатном призыве: <b>{'включено' if enabled else 'выключено'}</b>.\n"
        "Если включено, бот примерно раз в 3 часа напоминает забрать карту.",
        reply_markup=kb,
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "notify_settings")
async def notify_settings_cb(callback: types.CallbackQuery):
    await send_notify_settings(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "notify_toggle")
async def notify_toggle_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    p["notify_free_pack"] = not bool(p.get("notify_free_pack", True))
    mark_data_dirty("data_changed")
    await send_notify_settings(callback.message, callback.from_user)
    await callback.answer("Настройка изменена.")


ANIME_SOURCE_GROUPS = {
    "🥷 Шиноби и скрытые деревни": ["Naruto", "Наруто", "Boruto", "Боруто"],
    "🏴‍☠️ Пираты, моря и воля свободы": ["One Piece", "One Piece / Ван-Пис"],
    "🐉 Саяны, боги разрушения и космос": ["Dragon Ball", "Dragon Ball Super", "Dragon Ball GT", "Dragon Ball Heroes", "Драгонболл: фильм"],
    "🗡 Души, клинки и духовные войны": ["Bleach", "Блич", "Yu Yu Hakusho", "Hellsing", "Хеллсинг"],
    "😈 Демоны, проклятия и тёмная магия": ["Demon Slayer", "Клинок, рассекающий демонов", "Jujutsu Kaisen", "Магическая битва", "Chainsaw Man", "Человек-бензопила", "Black Clover", "Чёрный клевер", "Maou Gakuin", "Непризнанный школой владыка демонов", "Ichiban Ushiro no Daimaou", "Beelzebub", "Убийца гоблинов"],
    "🧙 Магия, судьба и божественные концепты": ["Fate", "Magi", "Madoka Magica", "Мадока Магика", "Frieren", "Фрирен", "Провожающая в последний путь Фрирен", "Umineko", "Уминэко", "Sailor Moon", "Сейлор Мун"],
    "🦖 Титаны, гиганты и апокалипсис": ["Attack on Titan", "Атака титанов", "Gurren Lagann", "Гуррен-Лаганн", "Евангелион"],
    "👾 Монстры, странные существа и франшизы": ["Покемон", "Покемон: фильм", "Tokyo Ghoul", "Токийский гуль", "Dandadan", "Mob Psycho 100", "JoJo"],
    "🎮 Игровые, цифровые и техно-миры": ["Sword Art Online", "Мастер меча онлайн", "Final Fantasy VII", "BlazBlue: Alter Memory", "Киберпанк: Бегущие по краю"],
    "⚔️ Воины, мечники и физическая мощь": ["Baki", "Баки", "Берсерк", "Vinland Saga", "Сага о Винланде", "Black Lagoon"],
    "🧠 Тактика, интеллект и психологические войны": ["Death Note", "Тетрадь смерти", "Code Geass", "Код Гиас", "Код Гиасс", "Класс превосходства", "Монстр", "Saiki Kusuo", "Повседневная жизнь бессмертного короля"],
    "🌌 Исекай, ранги и мультивселенские сущности": ["Tensei Slime", "О моём перерождении в слизь", "Overlord", "Re:Zero", "Solo Leveling", "Поднятие уровня в одиночку", "Tenchi Muyo", "Versus", "One Punch Man", "Ванпанчмен", "Saint Seiya", "Семь смертных грехов"],
    "🏫 Герои, спорт, школа и повседневные миры": ["Моя геройская академия", "Mashle", "Fire Force", "Доктор Стоун", "Вайолет Эвергарден", "Семья шпиона", "Синяя тюрьма", "Волейбол!!", "Госпожа Кагуя: в любви как на войне", "Маг-целитель: новый старт / Redo of Healer", "Токийские мстители", "Хвост Феи"],
}

def anime_source_chunks(limit=3400):
    """Красивый список источников без нумерации и технических слов."""
    all_names = sorted({str(c.get("anime", "")).strip() for c in CARDS if str(c.get("anime", "")).strip()}, key=str.casefold)
    remaining = set(all_names)
    blocks = [
        f"{CE['collection']} <b>Источники мультивселенной</b>",
        "Карты берутся из разных аниме, франшиз и форм персонажей. Покемоны считаются по всей франшизе, не только по фильмам."
    ]
    for title, names in ANIME_SOURCE_GROUPS.items():
        present = [n for n in names if n in remaining]
        if not present:
            continue
        for n in present:
            remaining.discard(n)
        blocks.append(f"\n<b>{title}</b>\n" + " • " + "\n • ".join(e(n) for n in present))
    if remaining:
        blocks.append("\n<b>✨ Другие миры и особые линии</b>\n" + " • " + "\n • ".join(e(n) for n in sorted(remaining, key=str.casefold)))

    text = "\n".join(blocks)
    chunks = []
    while len(text) > limit:
        cut = text.rfind("\n\n", 0, limit)
        if cut < 1:
            cut = text.rfind("\n", 0, limit)
        if cut < 1:
            cut = limit
        chunks.append(text[:cut].rstrip())
        text = text[cut:].lstrip()
    chunks.append(text.rstrip())
    return chunks



async def send_rules(message):
    text = (
        f"{CE['rules']} <b>Кодекс Anime Battle Multiverse</b>\n\n"
        "<b>1. Путь игрока</b>\n"
        "Выбираешь вселенную, получаешь карты, собираешь колоду, прокачиваешь уровни и выходишь на арену. Если выбрано «Все аниме», призывы идут по всей мультивселенной.\n\n"
        "<b>2. Получение карт</b>\n"
        "Бесплатная попытка доступна раз в 3 часа. Дополнительные попытки можно купить, получить за компенсации, события, крафт и подарки друзей.\n\n"
        "<b>3. Валюты</b>\n"
        f"{PISTACHIOS_LABEL} — главная валюта прогресса: попытки, улучшения, обмен и обычные покупки.\n"
        f"{DRAGONITE_LABEL} — редкая премиум-валюта для кейсов, пропуска и особых покупок. Она не должна сыпаться легко.\n\n"
        "<b>4. Редкости</b>\n"
        "Origin → Rare → Epic → Legendary → Absolute → Super Absolute.\n"
        "Super Absolute — отдельный верхний слой для закрытых сверхсильных персонажей и особых кейсов.\n\n"
        "<b>5. Бои</b>\n"
        "Игрок дерётся своей колодой. Если живых соперников мало, быстрые бои временно идут против бота; онлайн-режим остаётся доступным.\n\n"
        "<b>6. Кланы</b>\n"
        "Клан даёт имя, участников, очки, уровень и будущие клановые события. Создание доступно владельцу и игрокам с премиум/рангом поддержки.\n\n"
        "<b>7. Честность</b>\n"
        "Нельзя абузить баги, ломать оплату, спамить поддержку и выдавать себя за администрацию.\n\n"
        "<b>8. Споры</b>\n"
        "Если бой выглядел неправильно — используй <code>/appeal причина</code>. Владелец сможет ответить, отклонить или выдать компенсацию."
    )
    await message.answer(text, reply_markup=back_menu(), parse_mode="HTML")


@dp.message(Command("rules"))
async def rules_cmd(message: types.Message):
    await send_rules(message)


@dp.callback_query(F.data == "rules")
async def rules_cb(callback: types.CallbackQuery):
    await send_rules(callback.message)
    await callback.answer()



@dp.message(Command("universe"))
async def universe_cmd(message: types.Message):
    await send_universe_menu(message, message.from_user, 0)



def universe_victory_text(uid):
    texts = {
        "all": "Ты открыл путь странника мультивселенной. Собирай героев из всех миров и докажи, что твоя колода сильнее любого разлома.",
        "naruto_boruto": "Покори мир шиноби: собери команду, пробуди волю огня и стань легендой скрытых деревень.",
        "one_piece": "Подними флаг команды, собери будущих легенд моря и иди к титулу Короля пиратов.",
        "dragon_ball": "Пробуди ки, пройди путь сайяна и собери формы, которые выдержат турнир мультивселенной.",
        "bleach": "Открой путь синигами, подчини занпакто и подними отряд до уровня Короля душ.",
        "pokemon": "Собери команду, пройди лиги и стань тренером, которого узнает вся мультивселенная.",
        "attack_on_titan": "Выживи за стенами, собери разведку и докажи, что человечество ещё может победить.",
        "jujutsu_kaisen": "Возьми проклятую энергию под контроль и собери отряд магов, который не дрогнет перед бедствием.",
        "one_punch_man": "Пробей лимитер, собери героев и покажи, что один удар решает только начало боя.",
        "baki": "Выйди на подпольную арену и докажи, что твоя сила не держится только на редкости карты.",
        "beelzebub": "Веди маленького короля демонов через Ишияму, собирай союзников и докажи, что школа тоже может стать адом для врагов.",
    }
    return texts.get(uid, "Ты выбрал свой мир. Собирай персонажей этой вселенной, усиливай колоду и веди отряд к победам.")

def visible_universes_for_menu():
    by_id = {u["id"]: u for u in UNIVERSES}
    result = []
    for uid in FEATURED_UNIVERSE_IDS:
        rec = by_id.get(uid)
        if rec:
            result.append(rec)
    return result


async def send_universe_menu(message, user, page=0, intro=False):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    current = selected_universe_id(p)
    display_universes = visible_universes_for_menu()
    per_page = 8
    pages = max(1, (len(display_universes) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    shown = display_universes[page * per_page:(page + 1) * per_page]
    owned, total = universe_progress(p, current)
    intro_line = "Сначала выбери мир — от него зависит пул обычного призыва." if intro else "Сменить мир можно в любой момент: коллекция и прогресс не удаляются."
    text = (
        f"{CE['start']} <b>ВЫБОР ВСЕЛЕННОЙ</b>\n\n"
        f"{intro_line}\n\n"
        f"<blockquote>Сейчас: <b>{e(universe_label(current))}</b>\n"
        f"Коллекция: {progress_bar(owned, total)} <b>{short_number(owned)}/{short_number(total)}</b></blockquote>"
    )
    rows = [[button(text=("✅ 🌌 Все аниме" if current == "all" else "🌌 Все аниме"), callback_data="universe:set:all")]]
    pair = []
    for rec in shown:
        uid = rec["id"]
        label = f"{universe_emoji(uid)} {rec['name'][:22]}"
        if uid == current:
            label = "✅ " + label
        pair.append(button(text=label, callback_data=f"universe:set:{uid}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"universe:page:{page-1}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"universe:page:{page+1}"))
    rows.append(nav)
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data == "universe")
async def universe_cb(callback: types.CallbackQuery):
    await send_universe_menu(callback.message, callback.from_user, 0)
    await callback.answer()


@dp.callback_query(F.data.startswith("universe:page:"))
async def universe_page_cb(callback: types.CallbackQuery):
    try:
        page = int(callback.data.rsplit(":", 1)[1])
    except Exception:
        page = 0
    await send_universe_menu(callback.message, callback.from_user, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("universe:set:"))
async def universe_set_cb(callback: types.CallbackQuery):
    uid = callback.data.split(":", 2)[2]
    if uid != "all" and uid not in UNIVERSE_BY_ID:
        await callback.answer("Эта вселенная не найдена.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    p["preferred_universe"] = uid
    p["universe_onboarding_seen"] = UNIVERSE_ONBOARDING_VERSION
    mark_data_dirty("data_changed")
    label = universe_label(uid)
    text = (
        f"✅ <b>Вселенная выбрана</b>\n\n"
        f"Аниме: <b>{e(label)}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🎴 Призвать карту", callback_data="draw_card"), button(text="🃏 Мои карты", callback_data="collection:home")],
        [button(text="👤 Профиль", callback_data="profile"), button(text="⬅️ Меню", callback_data="menu")],
    ])
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer(f"Выбрано: {label}")


async def send_daily(message, user):
    p = get_user_data(user)
    today_date = app_now().date()
    today = today_date.isoformat()
    last_raw = str(p.get("last_daily", "") or "")
    if last_raw == today:
        await message.answer("🎁 Сегодняшняя ежедневная награда уже забрана.", reply_markup=back_menu())
        return

    try:
        last_date = date.fromisoformat(last_raw) if last_raw else None
    except Exception:
        last_date = None
    old_streak = max(0, int(p.get("daily_streak", 0) or 0))
    if last_date == today_date - timedelta(days=1):
        streak = old_streak + 1
    else:
        streak = 1
    streak = min(streak, 9999)

    # Commit the idempotency marker before any reward calculations; there is no await until the result is complete.
    p["last_daily"] = today
    p["daily_streak"] = streak
    fistiks = random.randint(260, 430)
    streak_bonus = 25 * min(streak, 7)
    fistiks += streak_bonus
    dragonit = 1 if random.random() < 0.28 else 0
    pass_gain = random.randint(70, 130)
    frag_line = ""
    if random.random() < 0.30:
        owned = [cid for cid, info in p.get("collection", {}).items() if cid in CARD_BY_ID and int(info.get("count", 0) or 0) > 0]
        if owned:
            cid = random.choice(owned)
            amount = random.randint(8, 20)
            add_fragments(p, cid, amount)
            frag_line = f"\n🧩 Фрагменты: +{amount} к {e(CARD_BY_ID[cid]['name'])}"
    artifact_line = ""
    if random.random() < 0.18:
        artifact = grant_random_artifact(p)
        artifact_line = f"\n🧿 Артефакт: {artifact_label(artifact)}"
    p["fistiks"] = int(p.get("fistiks", 0)) + fistiks
    p["moon_coins"] = int(p.get("moon_coins", 0)) + dragonit
    p["pass_xp"] = int(p.get("pass_xp", 0)) + pass_gain
    add_xp(p, 35)
    add_pass_task_progress(p, "daily", 1)
    newbie_bonus = add_newbie_task_progress(p, "daily", 1)
    season_gain = add_season_xp(p, SEASON_XP_REWARDS["daily"], action_key=f"daily:{today}")
    mark_data_dirty("daily_claim")
    text = (
        f"{CE['rewards']} <b>Ежедневная награда</b>\n\n"
        f"🔥 Серия: <b>{streak} дн.</b> · бонус серии <b>+{streak_bonus} 💎</b>\n"
        f"+{fistiks} 💎 Фисташек\n"
        f"+{pass_gain} очков мультипасса\n"
        f"+{season_gain} ⚡ SP\n"
        f"{('+1 🐉 Драконит' if dragonit else '🐉 Драконит сегодня не выпал')}"
        f"{frag_line}"
        f"{artifact_line}"
    )
    if newbie_bonus:
        text += "\n\n" + e(newbie_bonus)
    await message.answer(text, reply_markup=back_menu(), parse_mode="HTML")

@dp.message(Command("daily"))
async def daily_cmd(message: types.Message):
    await send_daily(message, message.from_user)


@dp.callback_query(F.data == "daily")
async def daily_cb(callback: types.CallbackQuery):
    await send_daily(callback.message, callback.from_user)
    await callback.answer()


def discounted_cost(user, base_cost):
    p = get_user_data(user)
    lvl, _, _ = calc_user_level(p.get("xp", 0))
    if lvl < 10:
        return base_cost // 2, True
    return base_cost, False


def odds_text(weights):
    total = sum(max(v, 0) for v in weights.values())
    parts = []
    for rarity in ["Обычный", "Редкий", "Эпический", "Легендарный", "Мифический"]:
        v = max(weights.get(rarity, 0), 0)
        percent = 0 if total == 0 else v * 100 / total
        parts.append(f"{rarity_label(rarity)} — {percent:.1f}%")
    return "\n".join(parts)



def grant_ref_milestone(player, milestone):
    reward = REF_MILESTONES.get(milestone)
    if not reward:
        return ""
    player["fistiks"] = int(player.get("fistiks", 0)) + int(reward.get("fistiks", 0))
    player["pass_xp"] = int(player.get("pass_xp", 0)) + int(reward.get("pass_xp", 0))
    player["moon_coins"] = int(player.get("moon_coins", 0)) + int(reward.get("moon_coins", 0))
    badge = reward.get("badge")
    if badge:
        player.setdefault("badges", [])
        if badge not in player["badges"]:
            player["badges"].append(badge)
    return f"{reward.get('title', milestone)}: +{reward.get('fistiks',0)} 💎 +{reward.get('pass_xp',0)} очков pass +{reward.get('moon_coins',0)} 🐉"


def format_ref_milestones(player):
    claimed = set(map(str, player.setdefault("ref_milestones_claimed", [])))
    count = int(player.get("ref_count", 0))
    lines = []
    for milestone, reward in REF_MILESTONES.items():
        mark = "✅" if str(milestone) in claimed else ("🎯" if count >= milestone else "▫️")
        badge = f" + {badge_title(reward['badge'])}" if reward.get("badge") else ""
        lines.append(
            f"{mark} {milestone} друзей — {reward['title']} → {reward.get('fistiks',0)} 💎, {reward.get('pass_xp',0)} pass, {reward.get('moon_coins',0)} 🐉{badge}"
        )
    return "\n".join(lines)


def grant_star_pack_reward(player, pack_code):
    pack = STAR_PACKS.get(pack_code)
    if not pack:
        return "Неизвестный набор."
    rarity = pack["rarity"]
    card = roll_card(weights={rarity: 1}, allowed_rarities=[rarity], universe_id=selected_universe_id(player), allow_super_absolute=bool(pack.get("allow_super_absolute")))
    result = add_card(player, card["id"])
    player["fistiks"] = int(player.get("fistiks", 0)) + int(pack.get("fistiks", 0))
    player["moon_coins"] = int(player.get("moon_coins", 0)) + int(pack.get("moon_coins", 0))
    player.setdefault("badges", [])
    if pack.get("badge") and pack["badge"] not in player["badges"]:
        player["badges"].append(pack["badge"])
    player.setdefault("purchases", []).append({
        "type": "star_pack",
        "pack": pack_code,
        "stars": pack["price"],
        "card_id": card["id"],
        "date": utc_now().isoformat(),
    })
    return (
        f"🎁 <b>{e(pack['title'])}</b>\n"
        f"🐉 Карта: {rarity_label_for_card(card)} <b>{e(card['name'])}</b>\n"
        f"{e(result)}\n"
        f"💎 +{pack.get('fistiks',0)} фисташек\n"
        f"🐉 +{pack.get('moon_coins',0)} драконита\n"
        f"🏷 {e(badge_title(pack.get('badge','')))}"
    )


async def notify_owner_purchase(user, text):
    for oid in owner_ids():
        try:
            await bot.send_message(int(oid), text, parse_mode="HTML")
        except Exception:
            pass

async def send_shop(message, user):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    await message.answer(
        f"{CE['shop']} <b>Магазин мультивселенной</b>\n\n"
        f"{PISTACHIOS_LABEL}: <b>{short_number(p.get('fistiks', 0))}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{short_number(p.get('moon_coins', 0))}</b>\n"
        f"{CE['draw_card']} Попытки: <b>{short_number(available_attempts(p))}</b>\n\n"
        "Выбери раздел ниже. Сундуки отдельно не продаются: для призыва покупаются только попытки.",
        reply_markup=shop_menu(),
        parse_mode="HTML"
    )

async def send_chests(message, user):
    p = get_user_data(user)
    attempts = available_attempts(p)
    rows = [
        [button(text="🎴 Призвать персонажа", callback_data="draw_card")],
        [button(text="📊 Шансы выпадения", callback_data="pack_info:free")],
        [button(text="🏪 Купить попытки", callback_data="shop_attempts")],
        [button(text="⬅️ Магазин / награды", callback_data="shop")],
    ]
    await message.answer(
        f"🧰 <b>СУНДУКИ ПРИЗЫВА</b>\n\n"
        f"Доступно призывов: <b>{short_number(attempts)}</b>.\n"
        "Каждое открытие тратит ровно одну попытку и выдаёт одного полноценного персонажа. "
        "Сундуки отдельно не продаются — покупаются только попытки.\n\n"
        "<b>Шансы любого сундука:</b>\n"
        f"{odds_text(SUMMON_WEIGHTS)}\n\n"
        "🎯 Гарант сохранён: 10 без Epic → Epic, 50 без Legendary → Legendary, 150 без Absolute → Absolute. "
        "Super Absolute в обычный призыв не входит.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )



@dp.message(Command("shop"))
async def shop_cmd(message: types.Message):
    await send_shop(message, message.from_user)



@dp.callback_query(F.data == "donate_menu")
async def donate_menu_cb(callback: types.CallbackQuery):
    await send_shop(callback.message, callback.from_user)
    await callback.answer()

@dp.callback_query(F.data == "shop")
async def shop_cb(callback: types.CallbackQuery):
    await send_shop(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "shop_more")
async def shop_more_cb(callback: types.CallbackQuery):
    await callback.message.answer(
        "⚙️ <b>Дополнительные разделы</b>\n\nТут лежит то, что не нужно держать на главном экране каждый раз.",
        reply_markup=shop_more_menu(),
        parse_mode="HTML",
    )
    await callback.answer()


async def send_stars_shop(message, user):
    get_user_data(user)
    text = (
        "⭐ <b>Stars-наборы</b>\n\n"
        "Это платные гарантированные наборы без грязного рандома. Они ускоряют старт, но не превращают игру в pay-to-win.\n\n"
    )
    rows = []
    for code, pack in STAR_PACKS.items():
        text += f"<b>{pack['price']} Stars — {e(pack['title'])}</b>\n{e(pack['desc'])}\n\n"
        rows.append([button(text=f"⭐ {pack['price']} — {pack['title']}", callback_data=f"buy_star_pack:{code}")])
    text += "После оплаты награда выдаётся автоматически, а владелец получает уведомление о покупке."
    rows.append([button(text="⬅️ Магазин / награды", callback_data="shop")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data == "stars_shop")
async def stars_shop_cb(callback: types.CallbackQuery):
    await send_stars_shop(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_star_pack:"))
async def buy_star_pack_cb(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    pack = STAR_PACKS.get(code)
    if not pack:
        await callback.answer("Набор не найден.", show_alert=True)
        return
    get_user_data(callback.from_user)
    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=pack["title"],
            description=pack["desc"],
            payload=f"star_pack:{code}:{callback.from_user.id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=pack["title"], amount=int(pack["price"]))],
        )
        await callback.message.answer("⭐ Счёт отправлен. После оплаты набор выдастся автоматически.", reply_markup=back_menu())
    except Exception as ex:
        await callback.message.answer(f"⚠️ Не удалось отправить счёт: {e(ex)}", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data == "chests")
async def chests_cb(callback: types.CallbackQuery):
    await send_chests(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("pack_info:"))
async def pack_info(callback: types.CallbackQuery):
    # PATCH35/PATCH40 old messages can still contain basic/rare/royal callbacks.
    # They now open the single transparent summon table instead of selling chests.
    text = (
        "🎴 <b>ШАНСЫ ПРИЗЫВА</b>\n\n"
        "Одна попытка = один полноценный персонаж. Если персонаж уже открыт, "
        "дубликат превращается во фрагменты его улучшения.\n\n"
        f"{odds_text(SUMMON_WEIGHTS)}\n\n"
        "🔴 Absolute: <b>2.5%</b> в любом обычном сундуке.\n"
        "⚫ Super Absolute не входит в обычный пул."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🎴 Призвать", callback_data="draw_card")],
        [button(text="🏪 Купить попытки", callback_data="shop_attempts")],
        [button(text="⬅️ Сундуки", callback_data="chests")]
    ])
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


async def send_pack_result(message, title, cards_got, player):
    text = f"{CE['rewards']} <b>{e(title)} открыт</b>\n\n"
    for card, result in cards_got:
        text += (
            f"{CE['collection']} <b>{e(card['name'])}</b> — {rarity_label_for_card(card)}\n"
            f"{e(result)}\n"
        )
    text += f"\n{CE['collection']} Форма и краткая биография каждого персонажа доступны в коллекции."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="⬅️ Назад к сундукам", callback_data="chests")],
        [button(text="⬅️ Магазин / награды", callback_data="shop"), button(text="🏠 Меню", callback_data="menu")],
    ])
    await send_long(message, text, reply_markup=kb)


@dp.callback_query(F.data.startswith("buy_pack:"))
async def buy_pack(callback: types.CallbackQuery):
    # Compatibility alias for old buttons left in chats before the update.
    await draw_card_to_message(callback.message, callback.from_user, callback)


@dp.callback_query(F.data == "badges_shop")
async def badges_shop(callback: types.CallbackQuery):
    rows = []
    text = "🏷 <b>Привилегии и знаки</b>\n\n"
    for code, item in BADGE_SHOP.items():
        db_code = code.upper()
        title = f"{item['emoji']} {item['title']}"
        text += f"<b>{title}</b> — {item['cost']} 💎\n{e(item['desc'])}\n\n"
        rows.append([button(text=f"Купить: {title} — {item['cost']} 💎", callback_data=f"buy_badge:{db_code}:{item['cost']}")])
    rows.append([button(text="⬅️ Сундуки", callback_data="chests")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()
@dp.callback_query(F.data.startswith("buy_badge:"))
async def buy_badge(callback: types.CallbackQuery):
    _, badge, cost_str = callback.data.split(":")
    cost = int(cost_str)
    p = get_user_data(callback.from_user)
    if p["fistiks"] < cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает фисташек.", show_alert=True)
        return
    if badge in p["badges"]:
        await callback.answer("Этот знак уже есть.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["fistiks"] -= cost
    p["badges"].append(badge)
    add_xp(p, 50)
    mark_data_dirty("data_changed")
    await callback.message.answer(f"🏷 Куплен знак: <b>{e(badge_title(badge))}</b>", reply_markup=main_menu(callback.from_user.id), parse_mode="HTML")
    await callback.answer()


# Старый тестовый buy_premium удалён: премиум теперь только через Мультипасс и Stars.


def collection_filter_name(code):
    names = {
        "all": "Все",
        "common": "Origin",
        "rare": "Rare",
        "epic": "Epic",
        "legendary": "Legendary",
        "mythic": "Absolute",
        "power": "По силе",
        "level": "По уровню",
        "name": "По имени",
        "event": "Ивентовые",
        "mystic": "Мистик",
        "super": "Super Absolute",
    }
    return names.get(code, RARITY_DISPLAY.get(RARITY_CODES.get(code, code), code))

def collection_sort_key(cid, info, sort_mode):
    c = CARD_BY_ID[cid]
    lvl = int(info.get("level", 1))
    if sort_mode == "name":
        return c.get("name", "")
    if sort_mode == "level":
        return lvl
    if sort_mode == "anime":
        return c.get("anime", "")
    return card_power(c, lvl)


def owned_card_items(player):
    return [
        (cid, info) for cid, info in player.get("collection", {}).items()
        if cid in CARD_BY_ID and int(info.get("count", 0) or 0) > 0 and bool(info.get("unlocked", True))
    ]

def fragment_card_items(player):
    return [
        (cid, info) for cid, info in player.get("collection", {}).items()
        if cid in CARD_BY_ID and int(info.get("shards", 0) or 0) > 0 and not (int(info.get("count", 0) or 0) > 0 and bool(info.get("unlocked", True)))
    ]

def collection_card_line(cid, info):
    c = CARD_BY_ID[cid]
    lvl = int(info.get("level", 1))
    return f"{rarity_label_for_card(c)} <b>{e(c['name'])}</b> · ур.{lvl} · сила <b>{short_number(card_power(c, lvl))}</b>"


def collection_scope_counts(player, scope_uid):
    uid = _effective_universe_id(scope_uid)
    owned = {cid for cid, _ in scoped_owned_card_items(player, scope_uid)}
    totals = {"Обычный": 0, "Редкий": 0, "Эпический": 0, "Легендарный": 0, "Мифический": 0, "super": 0, "event": 0, "mystic": 0}
    got = {k: 0 for k in totals}
    for c in CARDS:
        if uid and card_draw_universe(c) != uid:
            continue
        cid = c.get("id")
        rarity = c.get("rarity", "Обычный")
        if is_super_absolute_card(c) or c.get("special_tier") == "super_absolute":
            totals["super"] += 1
            got["super"] += int(cid in owned)
        elif c.get("special_tier") in {"event", "limited"}:
            totals["event"] += 1
            got["event"] += int(cid in owned)
        elif c.get("special_tier") == "mystic":
            totals["mystic"] += 1
            got["mystic"] += int(cid in owned)
        if rarity in totals and not is_super_absolute_card(c):
            totals[rarity] += 1
            got[rarity] += int(cid in owned)
    return got, totals




async def send_collection_home(message, user):
    p = get_user_data(user)
    scope_uid = selected_universe_id(p)
    scope_label = universe_label(scope_uid)
    got, totals = collection_scope_counts(p, scope_uid)
    art_have = sum(1 for v in p.setdefault("artifacts", {}).values() if int((v or {}).get("count", 0) or 0) > 0)
    strongest = scoped_owned_card_items(p, scope_uid)
    strongest.sort(key=lambda item: card_power(CARD_BY_ID[item[0]], int(item[1].get("level", 1) or 1)), reverse=True)
    top_lines = []
    for pos, (cid, info) in enumerate(strongest[:3], 1):
        c = CARD_BY_ID[cid]
        top_lines.append(f"{pos}. {rarity_label_for_card(c)} <b>{e(c['name'])}</b> · сила {short_number(card_power(c, int(info.get('level', 1) or 1)))}")
    if not top_lines:
        top_lines = ["Пока пусто — сделай первый призыв."]
    ordinary_have = sum(got[r] for r in ("Обычный", "Редкий", "Эпический", "Легендарный", "Мифический"))
    ordinary_total = sum(totals[r] for r in ("Обычный", "Редкий", "Эпический", "Легендарный", "Мифический"))
    text = (
        f"{CE['collection']} <b>КОЛЛЕКЦИЯ</b>\n\n"
        f"<blockquote>{e(scope_label)}\n"
        f"{progress_bar(ordinary_have, ordinary_total, 12)} <b>{short_number(ordinary_have)}/{short_number(ordinary_total)}</b>\n"
        f"🎪 Ивентовые {short_number(got['event'])}/{short_number(totals['event'])} · 🔮 Мистик {short_number(got['mystic'])}/{short_number(totals['mystic'])}\n"
        f"⚫ Super Absolute {short_number(got['super'])}/{short_number(totals['super'])} · 🧿 Артефакты {short_number(art_have)}</blockquote>\n"
        "<b>Три сильнейших персонажа</b>\n" + "\n".join(top_lines)
    )
    rows = [
        [button(text="⚪ По редкости", callback_data="collection:ordinary"), button(text="🎪 Ивентовые", callback_data="collection:filter:event")],
        [button(text="🔮 Мистик", callback_data="collection:filter:mystic"), button(text="⚫ Super Absolute", callback_data="collection:filter:super")],
        [button(text="🧿 Артефакты", callback_data="artifacts:page:0")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")



async def send_collection_ordinary_home(message, user):
    p = get_user_data(user)
    scope_uid = selected_universe_id(p)
    scope_label = universe_label(scope_uid)
    got, totals = collection_scope_counts(p, scope_uid)
    art_have = sum(int(v.get("count", 0) or 0) for v in p.setdefault("artifacts", {}).values())
    text = (
        f"{CE['collection']} <b>Обычные карты</b>\n\n"
        f"{CE['user_name']} <b>{e(p.get('name', user.full_name))}</b>\n"
        f"Аниме: <b>{e(scope_label)}</b>\n\n"
        "Выбери редкость обычных карт:\n\n"
        f"<blockquote>"
        f"Origin: {short_number(got['Обычный'])}/{short_number(totals['Обычный'])}\n"
        f"Rare: {short_number(got['Редкий'])}/{short_number(totals['Редкий'])}\n"
        f"Epic: {short_number(got['Эпический'])}/{short_number(totals['Эпический'])}\n"
        f"Legendary: {short_number(got['Легендарный'])}/{short_number(totals['Легендарный'])}\n"
        f"Absolute: {short_number(got['Мифический'])}/{short_number(totals['Мифический'])}\n"
        f"Артефакты: {short_number(art_have)}"
        f"</blockquote>"
    )
    rows = [
        [button(text="Origin", callback_data="collection:filter:common"), button(text="Rare", callback_data="collection:filter:rare")],
        [button(text="Epic", callback_data="collection:filter:epic"), button(text="Legendary", callback_data="collection:filter:legendary")],
        [button(text="Absolute", callback_data="collection:filter:mythic")],
        [button(text="Назад", callback_data="collection:home"), button(text="Меню", callback_data="menu")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")

@dp.callback_query(F.data == "collection:ordinary")
async def collection_ordinary_cb(callback: types.CallbackQuery):
    await send_collection_ordinary_home(callback.message, callback.from_user)
    await callback.answer()



async def send_collection(message, user, page=0, rarity_filter="all", sort_mode="power"):
    p = get_user_data(user)
    scope_uid = selected_universe_id(p)
    items = scoped_owned_card_items(p, scope_uid)
    opened, scope_total = universe_progress(p, scope_uid)
    if rarity_filter == "super":
        items = [(cid, info) for cid, info in items if is_super_absolute_card(CARD_BY_ID[cid]) or CARD_BY_ID[cid].get("special_tier") == "super_absolute"]
    elif rarity_filter == "event":
        items = [(cid, info) for cid, info in items if CARD_BY_ID[cid].get("special_tier") in {"event", "limited"}]
    elif rarity_filter == "mystic":
        items = [(cid, info) for cid, info in items if CARD_BY_ID[cid].get("special_tier") == "mystic"]
    elif rarity_filter != "all":
        wanted = RARITY_CODES.get(rarity_filter, rarity_filter)
        items = [(cid, info) for cid, info in items if CARD_BY_ID[cid].get("rarity") == wanted and not is_super_absolute_card(CARD_BY_ID[cid])]
    reverse = sort_mode not in {"name", "anime"}
    items.sort(key=lambda x: collection_sort_key(x[0], x[1], sort_mode), reverse=reverse)
    per_page = 6
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    rows, lines = [], []
    for cid, info in items[page * per_page:(page + 1) * per_page]:
        c = CARD_BY_ID[cid]
        lvl = int(info.get("level", 1) or 1)
        lines.append("• " + collection_card_line(cid, info))
        rows.append([button(text=f"{c['name'][:30]} · ур.{lvl}", callback_data=f"card:{card_cb_id(cid)}")])
    if not lines:
        lines.append("В этом разделе пока нет персонажей.")
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"collection:page:{page-1}:{rarity_filter}:{sort_mode}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"collection:page:{page+1}:{rarity_filter}:{sort_mode}"))
    rows.append(nav)
    rows.append([
        button(text="💪 По силе", callback_data=f"collection:sort:{rarity_filter}:power"),
        button(text="⬆️ По уровню", callback_data=f"collection:sort:{rarity_filter}:level"),
        button(text="🔤 По имени", callback_data=f"collection:sort:{rarity_filter}:name"),
    ])
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:home"), button(text="🏠 Меню", callback_data="menu")])
    text = (
        f"{CE['collection']} <b>{e(collection_filter_name(rarity_filter))}</b>\n"
        f"<blockquote>{e(universe_label(scope_uid))} · открыто {short_number(opened)}/{short_number(scope_total)}</blockquote>\n"
        + "\n".join(lines)
    )
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")




@dp.message(Command("collection"))
async def collection_cmd(message: types.Message):
    await send_collection_home(message, message.from_user)


@dp.callback_query(F.data == "collection:home")
async def collection_home_cb(callback: types.CallbackQuery):
    await send_collection_home(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("findcard"))
async def findcard_cmd(message: types.Message):
    query = message.text.replace("/findcard", "", 1).strip().lower()
    if not query:
        await message.answer("Формат: <code>/findcard наруто</code>", parse_mode="HTML", reply_markup=back_menu())
        return
    p = get_user_data(message.from_user)
    matches = []
    scope_uid = selected_universe_id(p)
    for cid, info in scoped_owned_card_items(p, scope_uid):
        if cid not in CARD_BY_ID:
            continue
        c = CARD_BY_ID[cid]
        hay = f"{c.get('name','')} {c.get('anime','')} {c.get('form','')}".lower()
        if query in hay:
            matches.append((cid, info))
    if not matches:
        await message.answer("Ничего не найдено в твоей коллекции.", reply_markup=back_menu())
        return
    matches.sort(key=lambda x: card_power(CARD_BY_ID[x[0]], int(x[1].get('level',1))), reverse=True)
    rows = []
    text = f"🔎 <b>Поиск карт:</b> {e(query)}\n\n"
    for cid, info in matches[:20]:
        c = CARD_BY_ID[cid]
        lvl = int(info.get('level', 1))
        text += f"• {rarity_label_for_card(c)} <b>{e(c['name'])}</b> | {e(c['anime'])} | ур.{lvl} | сила {short_number(card_power(c,lvl))}\n"
        rows.append([button(text=f"Открыть: {c['name'][:28]}", callback_data=f"card:{card_cb_id(cid)}")])
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:home")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data.startswith("collection:page:"))
async def collection_page(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    rarity_filter = parts[3] if len(parts) > 3 else "all"
    sort_mode = parts[4] if len(parts) > 4 else "power"
    await send_collection(callback.message, callback.from_user, page, rarity_filter, sort_mode)
    await callback.answer()


@dp.callback_query(F.data.startswith("collection:filter:"))
async def collection_filter_cb(callback: types.CallbackQuery):
    rarity_filter = callback.data.split(":", 2)[2]
    await send_collection(callback.message, callback.from_user, 0, rarity_filter, "power")
    await callback.answer()


@dp.callback_query(F.data.startswith("collection:sort:"))
async def collection_sort_cb(callback: types.CallbackQuery):
    _, _, rarity_filter, sort_mode = callback.data.split(":")
    await send_collection(callback.message, callback.from_user, 0, rarity_filter, sort_mode)
    await callback.answer()


async def send_fragments(message, user, page=0, rarity_filter="all"):
    p = get_user_data(user)
    items = fragment_card_items(p)
    if rarity_filter != "all":
        wanted = RARITY_CODES.get(rarity_filter, rarity_filter)
        items = [(cid, info) for cid, info in items if CARD_BY_ID[cid].get("rarity") == wanted]
    items.sort(key=lambda x: (CARD_BY_ID[x[0]].get("rarity", ""), int(x[1].get("shards", 0))), reverse=True)
    per_page = 7
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    rows = []
    lines = ["🧩 <b>Фрагменты персонажей</b>\n",
             "Здесь лежат персонажи, которых ещё нет в коллекции. Набери 100 фрагментов и собери карту.\n"]
    if not items:
        lines.append("Пока нет фрагментов закрытых персонажей.")
    for cid, info in items[page*per_page:(page+1)*per_page]:
        c = CARD_BY_ID[cid]
        shards = int(info.get("shards", 0) or 0)
        ready = shards >= CARD_UNLOCK_FRAGMENTS
        lines.append(f"• {rarity_label_for_card(c)} <b>{e(c['name'])}</b> — {shards}/{CARD_UNLOCK_FRAGMENTS}")
        if ready:
            rows.append([button(text=f"✅ Собрать: {c['name'][:28]}", callback_data=f"fragment_unlock:{card_cb_id(cid)}")])
        else:
            rows.append([button(text=f"🧩 {c['name'][:28]} · {shards}/{CARD_UNLOCK_FRAGMENTS}", callback_data="noop")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"fragments:page:{page-1}:{rarity_filter}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"fragments:page:{page+1}:{rarity_filter}"))
    rows.append(nav)
    rows.append([button(text="⚪ Origin", callback_data="fragments:page:0:common"), button(text="🔷 Rare", callback_data="fragments:page:0:rare"), button(text="🟣 Epic", callback_data="fragments:page:0:epic")])
    rows.append([button(text="🟡 Legendary", callback_data="fragments:page:0:legendary"), button(text="🔴 Absolute", callback_data="fragments:page:0:mythic")])
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:home")])
    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")

@dp.callback_query(F.data.startswith("fragments:page:"))
async def fragments_page_cb(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    rarity_filter = parts[3] if len(parts) > 3 else "all"
    await send_fragments(callback.message, callback.from_user, page, rarity_filter)
    await callback.answer()

@dp.callback_query(F.data.startswith("fragment_unlock:"))
async def fragment_unlock_cb(callback: types.CallbackQuery):
    cid = resolve_card_id(callback.data.split(":", 1)[1])
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p.get("collection", {}):
        await callback.answer("Фрагменты не найдены.", show_alert=True)
        return
    item = p["collection"][cid]
    shards = int(item.get("shards", 0) or 0)
    if shards < CARD_UNLOCK_FRAGMENTS:
        await callback.answer(f"Нужно {CARD_UNLOCK_FRAGMENTS} фрагментов.", show_alert=True)
        return
    item["shards"] = shards - CARD_UNLOCK_FRAGMENTS
    item["count"] = max(1, int(item.get("count", 0) or 0))
    item["unlocked"] = True
    item.setdefault("level", 1)
    mark_data_dirty("data_changed")
    c = CARD_BY_ID[cid]
    await callback.message.answer(
        f"{CE['collection']} <b>Карта собрана</b>\n\n"
        f"{rarity_label_for_card(c)} <b>{e(c['name'])}</b>\n"
        f"Форма: {e(c.get('form',''))}\n\n"
        "Теперь персонаж появился в основной коллекции.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="Открыть карту", callback_data=f"card:{card_cb_id(cid)}")], [button(text="⬅️ Фрагменты", callback_data="fragments:page:0:all")]]),
        parse_mode="HTML"
    )
    await callback.answer("Карта собрана.")

def artifact_label(artifact):
    return f"{rarity_label(artifact.get('rarity','Обычный'))} <b>{e(artifact.get('name','Артефакт'))}</b>"

def grant_random_artifact(player):
    weights = {"Обычный": 55, "Редкий": 25, "Эпический": 13, "Легендарный": 6, "Мифический": 1}
    rarity = random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]
    pool = [a for a in ARTIFACTS if a.get("rarity") == rarity] or ARTIFACTS
    artifact = random.choice(pool)
    inv = player.setdefault("artifacts", {})
    item = inv.setdefault(artifact["id"], {"count": 0, "level": 1})
    item["count"] = int(item.get("count", 0) or 0) + 1
    item["rarity"] = artifact.get("rarity", "Обычный")
    item["name"] = artifact.get("name", artifact["id"])
    return artifact



def artifact_effect_text(artifact):
    labels = {"power": "сила", "speed": "скорость", "durability": "живучесть", "hax": "особые способности", "iq": "тактика", "team": "синергия"}
    delta = (artifact or {}).get("delta", {}) or {}
    parts = []
    for key, value in delta.items():
        try:
            value = int(value)
        except Exception:
            continue
        sign = "+" if value >= 0 else ""
        parts.append(f"{labels.get(key, key)} {sign}{value}")
    return " · ".join(parts) or "коллекционный эффект"


def player_battle_artifact(uid):
    player = DATA.get("users", {}).get(str(uid), {}) or {}
    aid = str(player.get("equipped_artifact", "") or "")
    info = (player.get("artifacts", {}) or {}).get(aid, {}) or {}
    artifact = ARTIFACT_BY_ID.get(aid)
    if not artifact or int(info.get("count", 0) or 0) <= 0:
        return {"id": "none", "name": "Без артефакта", "text": "экипированный артефакт отсутствует", "rarity": "Обычный", "delta": {}}
    result = copy.deepcopy(artifact)
    # One equipped relic affects the whole five-card team; split its delta between units.
    result["delta"] = {k: (int(v) / 5.0) for k, v in (artifact.get("delta", {}) or {}).items()}
    return result


async def send_artifact_detail(message, user, artifact_id):
    p = get_user_data(user)
    inv = p.setdefault("artifacts", {})
    artifact = ARTIFACT_BY_ID.get(str(artifact_id))
    info = inv.get(str(artifact_id), {}) or {}
    if not artifact or int(info.get("count", 0) or 0) <= 0:
        await message.answer("Артефакт не найден в твоей коллекции.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Артефакты", callback_data="artifacts:page:0")]]))
        return
    equipped = str(p.get("equipped_artifact", "") or "") == str(artifact_id)
    text = (
        f"🧿 <b>{e(artifact.get('name', artifact_id))}</b>\n\n"
        f"<blockquote>{rarity_label(artifact.get('rarity', 'Обычный'))}\n"
        f"Источник: <b>{e(artifact.get('anime', 'Anime Battle Multiverse'))}</b>\n"
        f"В наличии: <b>{int(info.get('count', 0) or 0)}</b>\n"
        f"Статус: <b>{'экипирован' if equipped else 'в инвентаре'}</b></blockquote>\n"
        f"<b>Что делает</b>\n{e(artifact.get('text', 'Усиливает владельца.'))}\n\n"
        f"<b>Бонус отряду</b>\n{e(artifact_effect_text(artifact))}\n\n"
        "Экипировать можно один артефакт. Его суммарный бонус распределяется между пятью бойцами команды."
    )
    rows = []
    if equipped:
        rows.append([button(text="Снять артефакт", callback_data="artifact:unequip")])
    else:
        rows.append([button(text="Экипировать", callback_data=f"artifact:equip:{artifact_id}")])
    rows.append([button(text="⬅️ Артефакты", callback_data="artifacts:page:0"), button(text="🏠 Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


async def send_artifacts_collection(message, user, page=0):
    p = get_user_data(user)
    inv = p.setdefault("artifacts", {})
    items = [(aid, info) for aid, info in inv.items() if int((info or {}).get("count", 0) or 0) > 0 and aid in ARTIFACT_BY_ID]
    items.sort(key=lambda x: (RARITY_BONUS.get(ARTIFACT_BY_ID[x[0]].get("rarity", "Обычный"), 0), int(x[1].get("count", 0) or 0)), reverse=True)
    per_page = 7
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    equipped = str(p.get("equipped_artifact", "") or "")
    lines = ["🧿 <b>АРТЕФАКТЫ</b>\n", "Открой артефакт, прочитай его эффект и экипируй один для усиления всей команды.\n"]
    rows = []
    if not items:
        lines.append("Пока артефактов нет. Они выпадают из ежедневных наград, кейсов и рейдов.")
    for aid, info in items[page * per_page:(page + 1) * per_page]:
        a = ARTIFACT_BY_ID[aid]
        marker = "✅ " if aid == equipped else ""
        lines.append(f"• {marker}{artifact_label(a)} ×{int(info.get('count', 0) or 0)}")
        rows.append([button(text=f"{marker}{a.get('name', aid)[:34]}", callback_data=f"artifact:view:{aid}")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"artifacts:page:{page-1}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"artifacts:page:{page+1}"))
    rows.append(nav)
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:home")])
    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data.startswith("artifacts:page:"))
async def artifacts_page_cb(callback: types.CallbackQuery):
    page_s = callback.data.split(":")[-1]
    page = int(page_s) if str(page_s).isdigit() else 0
    await send_artifacts_collection(callback.message, callback.from_user, page)
    await callback.answer()



@dp.callback_query(F.data.startswith("artifact:view:"))
async def artifact_view_cb(callback: types.CallbackQuery):
    aid = callback.data.split(":", 2)[2]
    await send_artifact_detail(callback.message, callback.from_user, aid)
    await callback.answer()


@dp.callback_query(F.data.startswith("artifact:equip:"))
async def artifact_equip_cb(callback: types.CallbackQuery):
    aid = callback.data.split(":", 2)[2]
    p = get_user_data(callback.from_user)
    info = (p.setdefault("artifacts", {}).get(aid, {}) or {})
    if aid not in ARTIFACT_BY_ID or int(info.get("count", 0) or 0) <= 0:
        await callback.answer("Артефакт не найден.", show_alert=True)
        return
    p["equipped_artifact"] = aid
    mark_data_dirty("artifact_equip")
    await send_artifact_detail(callback.message, callback.from_user, aid)
    await callback.answer("Артефакт экипирован.")


@dp.callback_query(F.data == "artifact:unequip")
async def artifact_unequip_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    p["equipped_artifact"] = ""
    mark_data_dirty("artifact_unequip")
    await callback.message.answer("🧿 Артефакт снят.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Артефакты", callback_data="artifacts:page:0")]]))
    await callback.answer("Снято.")



@dp.callback_query(F.data.startswith("card:"))
async def card_detail(callback: types.CallbackQuery):
    cid = resolve_card_id(callback.data.split(":", 1)[1])
    p = get_user_data(callback.from_user)
    info = (p.get("collection", {}) or {}).get(cid, {}) or {}
    if cid not in CARD_BY_ID or int(info.get("count", 0) or 0) <= 0 or not bool(info.get("unlocked", True)):
        await callback.answer("Этого персонажа пока нет в коллекции.", show_alert=True)
        return
    c = CARD_BY_ID[cid]
    level = int(info.get("level", 1) or 1)
    cost = level_cost(level, c["rarity"])
    power = card_power(c, level)
    fragments = int(info.get("shards", 0) or 0)
    next_text = "Максимальный уровень" if cost is None else f"Для улучшения: {fragments}/{cost} фрагментов"
    caption = (
        f"{rarity_label_for_card(c)} <b>{e(c['name'])}</b>\n"
        f"🎭 Форма: <b>{e(c.get('form') or 'Основная')}</b>\n"
        f"📈 Уровень: <b>{level}/{MAX_LEVEL}</b> · ⚔️ сила <b>{short_number(power)}</b>\n"
        f"🧩 {e(next_text)}\n\n"
        f"<blockquote>{e(card_public_description(c))}</blockquote>"
    )
    rows = []
    if cost is not None:
        rows.append([button(text=f"⬆️ Улучшить до {level+1}", callback_data=f"upgrade:{card_cb_id(cid)}")])
    rows.append([button(text="⚔️ В колоду", callback_data=f"deck_add:{card_cb_id(cid)}"), button(text="⬅️ Коллекция", callback_data="collection:home")])
    await send_card_result(callback.message, cid, caption, InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()




@dp.callback_query(F.data.startswith("deck_add:"))
async def deck_add_cb(callback: types.CallbackQuery):
    cid = resolve_card_id(callback.data.split(":", 1)[1])
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p.get("collection", {}) or int(p["collection"][cid].get("count", 0)) <= 0:
        await callback.answer("Этой карты нет в коллекции.", show_alert=True)
        return
    deck = [x for x in p.get("deck", []) if x in CARD_BY_ID and x != cid]
    deck.insert(0, cid)
    p["deck"] = deck[:5]
    p["auto_team"] = False
    mark_data_dirty("data_changed")
    await callback.message.answer(
        f"🎴 <b>{e(CARD_BY_ID[cid]['name'])}</b> добавлен в команду.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🧬 Открыть колоду", callback_data="deck")],
            [button(text="⬅️ Коллекция", callback_data="collection:home")],
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("upgrade:"))
async def upgrade_card(callback: types.CallbackQuery):
    cid = resolve_card_id(callback.data.split(":", 1)[1])
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p["collection"]:
        await callback.answer("Карты нет.", show_alert=True)
        return
    c = CARD_BY_ID[cid]
    info = p["collection"][cid]
    level = info.get("level", 1)
    cost = level_cost(level, c["rarity"])
    if cost is None:
        await callback.answer("Уже максимальный уровень.", show_alert=True)
        return
    if info.get("shards", 0) < cost and not is_owner(callback.from_user.id):
        await callback.answer(f"Нужно {cost} фрагментов.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        info["shards"] -= cost
    info["level"] = min(MAX_LEVEL, level + 1)
    add_xp(p, 10)
    mark_data_dirty("data_changed")
    await callback.message.answer(f"⬆️ {c['name']} улучшен до {info['level']}/{MAX_LEVEL}.", reply_markup=back_menu())
    await callback.answer()


def difficulty_name(level):
    level = int(level)
    if level <= 2:
        return "Новичок"
    if level <= 4:
        return "Средний"
    if level <= 6:
        return "Опасный"
    if level <= 8:
        return "Элита"
    return "Бог арены"


async def show_arena_select(message, user, page=0):
    arena_items = list(ARENAS.items())
    if not arena_items:
        await message.answer("Арены пока не настроены.", reply_markup=back_menu())
        return
    page = max(0, min(int(page or 0), len(arena_items) - 1))
    code_key, (emoji, name, desc) = arena_items[page]
    plus, minus = ARENA_EFFECTS.get(code_key, ("➕ нейтрально", "➖ нейтрально"))

    text = (
        f"🌌 <b>Выбор арены</b> — {page + 1}/{len(arena_items)}\n\n"
        f"{emoji} <b>{e(name)}</b>\n"
        f"{e(desc)}\n\n"
        f"<b>Плюс арены:</b> {e(plus)}\n"
        f"<b>Минус арены:</b> {e(minus)}\n\n"
        "Листай арены стрелками. Когда выберешь арену, дальше откроется сложность бота."
    )
    prev_page = (page - 1) % len(arena_items)
    next_page = (page + 1) % len(arena_items)
    rows = [
        [button(text="✅ Выбрать эту арену", callback_data=f"battle:arena:{code_key}")],
        [
            button(text="⬅️ Предыдущая", callback_data=f"battle:arena_page:{prev_page}"),
            button(text="➡️ Следующая", callback_data=f"battle:arena_page:{next_page}"),
        ],
        [button(text="🎲 Случайная арена", callback_data="battle:arena:random")],
        [button(text="⬅️ Режимы", callback_data="modes")],
    ]
    await send_arena_card(message, code_key, text, InlineKeyboardMarkup(inline_keyboard=rows))


async def show_difficulty_select(message, user, arena_code):
    if arena_code == "random" or arena_code not in ARENAS:
        arena_code = random.choice(list(ARENAS.keys()))
    emoji, arena_name, arena_desc = ARENAS[arena_code]
    plus, minus = ARENA_EFFECTS.get(arena_code, ("➕ нейтрально", "➖ нейтрально"))
    rows = []
    for start in [1, 6]:
        rows.append([
            button(text=f"{i} {difficulty_name(i)}", callback_data=f"battle:diff:{arena_code}:{i}")
            for i in range(start, start + 5)
        ])
    rows.append([button(text="⬅️ Арены", callback_data="battle:arena_select")])
    text = (
        f"🤖 <b>Выбор сложности бота</b>\n\n"
        f"{emoji} Арена: <b>{e(arena_name)}</b>\n"
        f"— {e(arena_desc)}.\n\n"
        f"<b>Плюс:</b> {e(plus)}\n"
        f"<b>Минус:</b> {e(minus)}\n\n"
        "1–2 — Новичок, 3–4 — Средний, 5–6 — Опасный, 7–8 — Элита, 9–10 — Бог арены."
    )
    await send_arena_card(message, arena_code, text, InlineKeyboardMarkup(inline_keyboard=rows))


async def send_battle_source_menu(message, user, target="solo"):
    p = get_user_data(user)
    names = {"deck": "своя колода", "random_bot": "авто-колода от бота", "manual": "ручной выбор"}
    current = p.get("battle_team_source", p.get("pvp_team_source", "deck"))
    text = (
        "⚙️ <b>Выбор боя</b>\n\n"
        "Перед боем выбери, как собрать команду:\n"
        "• своя колода — берутся твои сохранённые 5 карт;\n"
        "• авто-колода — бот временно собирает состав для боя;\n"
        "• ручной выбор — выбери до 5 карт страницами.\n\n"
        f"Сейчас: <b>{e(names.get(current, 'своя колода'))}</b>"
    )
    rows = [
        [button(text="🛡️ Своя колода", callback_data=f"battle_source:{target}:deck")],
        [button(text="🤖 Авто-колода от бота", callback_data=f"battle_source:{target}:random_bot")],
        [button(text="🎴 Выбрать карты вручную", callback_data=f"battle_source:{target}:manual")],
        [button(text="⬅️ Режимы", callback_data="modes")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


async def send_manual_team_picker(message, user, target="solo", page=0):
    p = get_user_data(user)
    uid = str(user.id)
    draft = manual_team_drafts.setdefault(uid, {"target": target, "cards": [], "updated_at_ts": time.time()})
    draft["target"] = target
    draft["updated_at_ts"] = time.time()
    owned = [(cid, info) for cid, info in p.get("collection", {}).items() if cid in CARD_BY_ID and int(info.get("count", 0) or 0) > 0]
    owned.sort(key=lambda x: card_power(CARD_BY_ID[x[0]], int(x[1].get("level", 1))), reverse=True)
    if not owned:
        await message.answer("🎴 В коллекции нет карт. Для боя будет использована временная авто-колода.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="Продолжить", callback_data=f"manual_team_done:{target}")]]))
        return
    per_page = 7
    pages = max(1, (len(owned) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    chosen = draft.get("cards", [])[:5]
    text = f"🎴 <b>Ручной выбор команды</b> — {len(chosen)}/5\n\n"
    if chosen:
        text += "Выбрано: " + ", ".join(e(CARD_BY_ID[c]['name']) for c in chosen if c in CARD_BY_ID) + "\n\n"
    rows = []
    for cid, info in owned[page*per_page:(page+1)*per_page]:
        c = CARD_BY_ID[cid]
        lvl = int(info.get("level", 1))
        mark = "✅" if cid in chosen else "➕"
        text += f"• {mark} {rarity_label_for_card(c)} <b>{e(c['name'])}</b> | ур.{lvl} | сила {card_power(c,lvl)}\n"
        rows.append([button(text=f"{mark} {c['name'][:30]}", callback_data=f"mtadd:{target}:{card_cb_id(cid)}")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"manual_team_page:{target}:{page-1}"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"manual_team_page:{target}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([button(text="✅ Готово", callback_data=f"manual_team_done:{target}"), button(text="🧹 Сброс", callback_data=f"manual_team_clear:{target}")])
    rows.append([button(text="🤖 Авто-колода", callback_data=f"battle_source:{target}:random_bot")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data == "battle:arena_select")
async def battle_arena_select_cb(callback: types.CallbackQuery):
    await show_arena_select(callback.message, callback.from_user, 0)
    await callback.answer()


@dp.callback_query(F.data.startswith("battle_source:"))
async def battle_source_cb(callback: types.CallbackQuery):
    try:
        _, target, source = callback.data.split(":", 2)
    except Exception:
        await callback.answer("Ошибка выбора.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    if source not in {"deck", "random_bot", "manual"}:
        await callback.answer("Неизвестный тип команды.", show_alert=True)
        return
    p["battle_team_source"] = source
    p["pvp_team_source"] = source
    mark_data_dirty("data_changed")
    if source == "manual":
        manual_team_drafts[str(callback.from_user.id)] = {"target": target, "cards": [], "updated_at_ts": time.time()}
        await send_manual_team_picker(callback.message, callback.from_user, target, 0)
    elif target == "solo":
        await show_arena_select(callback.message, callback.from_user, 0)
    else:
        await callback.message.answer("✅ Тип команды сохранён для PvP/рейда.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🌐 Онлайн", callback_data="online_search")], [button(text="👹 Рейд", callback_data="raid_info")], [button(text="⬅️ Режимы", callback_data="modes")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("manual_team_page:"))
async def manual_team_page_cb(callback: types.CallbackQuery):
    try:
        _, _, target, page_s = callback.data.split(":")
        page = int(page_s)
    except Exception:
        target, page = "solo", 0
    await send_manual_team_picker(callback.message, callback.from_user, target, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("mtadd:"))
async def manual_team_add_cb(callback: types.CallbackQuery):
    try:
        _, target, cid = callback.data.split(":", 2)
        cid = resolve_card_id(cid)
    except Exception:
        await callback.answer("Ошибка карты.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p.get("collection", {}) or int(p["collection"][cid].get("count", 0) or 0) <= 0:
        await callback.answer("Этой карты нет в коллекции.", show_alert=True)
        return
    draft = manual_team_drafts.setdefault(str(callback.from_user.id), {"target": target, "cards": [], "updated_at_ts": time.time()})
    cards = draft.setdefault("cards", [])
    if cid in cards:
        cards.remove(cid)
    elif len(cards) < 5:
        cards.append(cid)
    else:
        await callback.answer("Можно выбрать максимум 5 карт.", show_alert=True)
        return
    await send_manual_team_picker(callback.message, callback.from_user, target, 0)
    await callback.answer()


@dp.callback_query(F.data.startswith("manual_team_clear:"))
async def manual_team_clear_cb(callback: types.CallbackQuery):
    target = callback.data.split(":", 1)[1] if ":" in callback.data else "solo"
    manual_team_drafts[str(callback.from_user.id)] = {"target": target, "cards": [], "updated_at_ts": time.time()}
    await send_manual_team_picker(callback.message, callback.from_user, target, 0)
    await callback.answer("Сброшено.")


@dp.callback_query(F.data.startswith("manual_team_done:"))
async def manual_team_done_cb(callback: types.CallbackQuery):
    target = callback.data.split(":", 1)[1] if ":" in callback.data else "solo"
    uid = str(callback.from_user.id)
    p = get_user_data(callback.from_user)
    chosen = manual_team_drafts.get(uid, {}).get("cards", [])[:5]
    p["manual_team"] = chosen
    p["battle_team_source"] = "manual" if chosen else "random_bot"
    p["pvp_team_source"] = p["battle_team_source"]
    mark_data_dirty("data_changed")
    if target == "solo":
        await show_arena_select(callback.message, callback.from_user, 0)
    elif target == "raid":
        await callback.message.answer("✅ Ручная команда сохранена. Наношу удар по рейд-боссу.", parse_mode="HTML")
        await perform_raid_hit(callback.message, callback.from_user, "manual")
    else:
        await callback.message.answer("✅ Ручная команда сохранена.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🌐 Онлайн", callback_data="online_search")], [button(text="👹 Рейд", callback_data="raid_info")], [button(text="⬅️ Режимы", callback_data="modes")]]), parse_mode="HTML")
    await callback.answer()
async def start_battle_for(message, user, arena_code="random", difficulty=5):
    if arena_code == "random" or arena_code not in ARENAS:
        arena_code = random.choice(list(ARENAS.keys()))
    difficulty = max(1, min(10, int(difficulty or 5)))
    p = get_user_data(user)
    player_team = build_team_for_user(user.id, source=p.get("battle_team_source", "deck"), fill=True)

    bot_team = build_bot_team(difficulty, exclude=[i["card_id"] for i in player_team])
    emoji, arena_name, arena_desc = ARENAS[arena_code]
    active_battles[user.id] = {
        "round": 1,
        "player": player_team,
        "bot": bot_team,
        "options": [],
        "done": True,
        "chat_id": message.chat.id,
        "arena": arena_code,
        "difficulty": difficulty,
        "resolved": False,
        "created_at_ts": time.time(),
    }
    await send_arena_media(message, arena_code)
    text = (
        f"⚔️ <b>Бой с ботом готов</b>\n\n"
        f"{emoji} Арена: <b>{e(arena_name)}</b>\n"
        f"— {e(arena_desc)}.\n"
        f"🤖 Сложность бота: <b>{difficulty}/10 — {e(difficulty_name(difficulty))}</b>\n\n"
        "👤 <b>Твоя колода</b>\n"
    )
    for i, inst in enumerate(player_team, 1):
        text += format_instance(inst, i) + "\n"
    text += "\n🔒 Команда бота скрыта. Выбери первого бойца — после этого начнётся раунд 1."
    rows = []
    for i, inst in enumerate(player_team, 1):
        c = CARD_BY_ID[inst["card_id"]]
        rows.append([button(text=f"⚔️ Старт: {i}. {c['name'][:28]}", callback_data=f"fight_start:{i-1}")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await send_long(message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@dp.message(Command("battle"))
async def battle_cmd(message: types.Message):
    await show_arena_select(message, message.from_user)


@dp.callback_query(F.data == "battle:start")
async def battle_cb(callback: types.CallbackQuery):
    await show_arena_select(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("battle:arena_page:"))
async def battle_arena_page_cb(callback: types.CallbackQuery):
    try:
        page = int(callback.data.rsplit(":", 1)[1])
    except Exception:
        page = 0
    await show_arena_select(callback.message, callback.from_user, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("battle:arena:"))
async def battle_arena_cb(callback: types.CallbackQuery):
    arena_code = callback.data.split(":", 2)[2]
    await show_difficulty_select(callback.message, callback.from_user, arena_code)
    await callback.answer()


@dp.callback_query(F.data.startswith("battle:diff:"))
async def battle_diff_cb(callback: types.CallbackQuery):
    try:
        _, _, arena_code, diff_s = callback.data.split(":")
        difficulty = int(diff_s)
    except Exception:
        await callback.answer("Ошибка сложности.", show_alert=True)
        return
    await start_battle_for(callback.message, callback.from_user, arena_code, difficulty)
    await callback.answer()


async def send_battle_round(message, uid):
    state = active_battles[uid]
    exclude = [i["card_id"] for i in state["player"]]
    owned_available = collection_candidates(uid, exclude)
    if not owned_available:
        await message.answer(
            "🃏 <b>Нет доступных открытых карт для следующего выбора.</b>\n\n"
            "Теперь арена работает только по коллекции. Открой сундуки или собери карту из фрагментов.",
            reply_markup=main_menu(uid),
            parse_mode="HTML"
        )
        state["done"] = True
        return

    options = []
    max_options = min(5, len(owned_available))
    for _ in range(max_options):
        c = roll_card_for_user(uid, exclude=exclude + [x["id"] for x in options])
        if c is None:
            break
        options.append(c)

    if not options:
        await message.answer("Недостаточно открытых карт в коллекции.", reply_markup=main_menu(uid))
        state["done"] = True
        return

    state["options"] = [c["id"] for c in options]

    arena_code = state.get("arena", "ruins")
    emoji, arena_name, _arena_desc = ARENAS.get(arena_code, ARENAS["ruins"])
    text = (
        f"🎲 <b>Раунд {state['round']}/5</b>\n"
        f"{emoji} Арена: <b>{e(arena_name)}</b>\n"
        f"🃏 Играешь только картами из своей коллекции.\n"
        f"⏱ 20 секунд на выбор.\n\n"
    )
    for i, c in enumerate(options, 1):
        text += card_short(c, i) + "\n\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [button(text=f"Выбрать {i}", callback_data=f"pick:{state['round']}:{i-1}")]
            for i in range(1, len(options) + 1)
        ]
    )
    await send_long(message, text, reply_markup=kb)

    key = ("battle", uid, state["round"])
    cancel_choice_timer(key)
    choice_timers[key] = asyncio.create_task(auto_pick_battle(uid, state["round"]))


async def auto_pick_battle(uid, round_no):
    async def alive():
        state = active_battles.get(uid)
        return bool(state and not state.get("done") and state.get("round") == round_no and state.get("options"))

    await asyncio.sleep(10)
    if not await alive():
        return
    state = active_battles.get(uid)
    try:
        await bot.send_message(state.get("chat_id"), "⏳ Осталось 10 секунд. Выбери карту, иначе бот сделает ход за тебя.")
    except Exception:
        pass

    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_battles.get(uid)
    try:
        await bot.send_message(state.get("chat_id"), "⚠️ Осталось 5 секунд. После таймера выбор будет случайным.")
    except Exception:
        pass

    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_battles.get(uid)
    idx = random.randrange(len(state["options"]))
    await process_battle_pick(uid, idx, auto=True)


async def process_battle_pick(uid, idx, auto=False, callback_message=None, user_obj=None):
    state = active_battles.get(uid)
    if not state or state.get("done"):
        return
    if idx < 0 or idx >= len(state.get("options", [])):
        return
    cancel_choice_timer(("battle", uid, state["round"]))

    card = CARD_BY_ID[state["options"][idx]]
    inst = make_instance(card, card_level_for_user(uid, card["id"]), player_battle_artifact(uid))
    state["player"].append(inst)

    player = DATA.get("users", {}).get(str(uid))
    if player is not None:
        result = "карта вышла на поле из твоей коллекции"
        add_xp(player, 15)
        mark_data_dirty("data_changed")
    else:
        result = "карта выбрана"

    chat_id = state.get("chat_id")
    prefix = "⏱ Время вышло. Бот выбрал за тебя:" if auto else "✅ Выбрано:"
    await bot.send_message(chat_id, f"{prefix} {rarity_label_for_card(card)} <b>{e(card['name'])}</b>\n{e(result)}", parse_mode="HTML")

    if state["round"] >= 5:
        state["done"] = True
        bot_exclude = [i["card_id"] for i in state["player"]]
        player_data = DATA.get("users", {}).get(str(uid), {})
        user_level, _, _ = calc_user_level(player_data.get("xp", 0))
        bot_weights = BOT_BATTLE_WEIGHTS_NEWBIE if user_level < 10 else BOT_BATTLE_WEIGHTS_NORMAL
        for _ in range(5):
            opts = []
            for _j in range(5):
                opts.append(roll_card(weights=bot_weights, exclude=bot_exclude + [i["card_id"] for i in state["bot"]] + [x["id"] for x in opts]))
            opts = sorted(opts, key=lambda c: card_power(c), reverse=True)
            pick = opts[0] if random.random() < 0.35 else random.choice(opts[:3])
            state["bot"].append(make_instance(pick, bot_level_for_difficulty(state.get("difficulty", 5))))

        proxy = type("MessageProxy", (), {"chat": type("Chat", (), {"id": chat_id})(), "answer": lambda self, text, **kwargs: bot.send_message(chat_id, text, **kwargs)})()
        fake_user = type("UserProxy", (), {"id": uid})()
        await finish_battle(proxy, fake_user)
        return

    state["round"] += 1
    proxy = type("MessageProxy", (), {"chat": type("Chat", (), {"id": chat_id})(), "answer": lambda self, text, **kwargs: bot.send_message(chat_id, text, **kwargs)})()
    await send_battle_round(proxy, uid)


@dp.callback_query(F.data.startswith("pick:"))
async def pick_card(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in active_battles:
        await callback.answer("Сначала начни бой.", show_alert=True)
        return
    state = active_battles[uid]
    if state.get("done"):
        await callback.answer("Этот бой уже завершён.", show_alert=True)
        return
    _, r, idx = callback.data.split(":")
    if int(r) != state["round"]:
        await callback.answer("Старая кнопка.", show_alert=True)
        return
    idx = int(idx)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await process_battle_pick(uid, idx, auto=False, callback_message=callback.message, user_obj=callback.from_user)
    await callback.answer()


async def finish_battle(message, user):
    state = active_battles[user.id]
    text = "🏁 <b>Команда собрана</b>\n\n👤 <b>Твоя команда</b>\n"
    for i, inst in enumerate(state["player"], 1):
        text += format_instance(inst, i) + "\n"
    text += "\n🔒 <b>Команда бота скрыта.</b>\nОна раскроется только после начала боя.\n\n"
    text += "Выбери первого персонажа, который выйдет вперёд."
    rows = []
    for i, inst in enumerate(state["player"], 1):
        c = CARD_BY_ID[inst["card_id"]]
        rows.append([button(text=f"⚔️ Начать с {i}. {c['name'][:28]}", callback_data=f"fight_start:{i-1}")])
    rows.append([button(text="🔁 Новый бой", callback_data="battle:start")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await send_long(message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def start_solo_fight(callback: types.CallbackQuery, starter_idx=0):
    uid = callback.from_user.id
    if uid not in active_battles or not active_battles[uid].get("done"):
        await callback.answer("Сначала собери колоду.", show_alert=True)
        return
    state = active_battles[uid]
    if state.get("resolved"):
        await callback.answer("Этот бой уже рассчитан.", show_alert=True)
        return

    starter_idx = max(0, min(int(starter_idx or 0), len(state["player"]) - 1))
    bot_starter = random.randrange(len(state["bot"])) if state.get("bot") else 0

    state["fight_started"] = True
    state["fight_round"] = 0
    state["player_points"] = 0
    state["bot_points"] = 0
    state["fight_log"] = []
    state["remaining_player_indices"] = [i for i in range(len(state["player"])) if i != starter_idx]
    state["bot_order"] = ordered_team(state["bot"], bot_starter)
    await process_solo_fight_round(callback.message, callback.from_user, starter_idx)
    await callback.answer()


async def process_solo_fight_round(message, user, player_idx):
    uid = user.id
    state = active_battles.get(uid)
    if not state or state.get("resolved"):
        return

    try:
        await message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    user_data = get_user_data(user)
    player_name = user_data["name"]
    arena_code = state.get("arena", "ruins")
    emoji, arena_name, arena_desc = ARENAS.get(arena_code, ARENAS["ruins"])

    round_no = int(state.get("fight_round", 0)) + 1
    bot_order = state.get("bot_order") or state["bot"]
    if round_no > min(len(state["player"]), len(bot_order)):
        await finish_solo_interactive(message, user)
        return

    player_inst = state["player"][player_idx]
    bot_inst = bot_order[round_no - 1]
    result, line = duel_line(round_no, player_name, "Бот", player_inst, bot_inst, arena_code)
    if result == 1:
        state["player_points"] = int(state.get("player_points", 0)) + 1
    else:
        state["bot_points"] = int(state.get("bot_points", 0)) + 1
    state["fight_round"] = round_no
    state.setdefault("fight_log", []).append(line)

    player_card = CARD_BY_ID[player_inst["card_id"]]
    bot_card = CARD_BY_ID[bot_inst["card_id"]]
    score_text = f"{player_name} {state['player_points']} : {state['bot_points']} Бот"

    text = (
        f"🎬 <b>Раунд {round_no}</b>\n\n"
        f"{emoji} Арена: <b>{e(arena_name)}</b>\n"
        f"🤖 Сложность: <b>{state.get('difficulty', 5)}/10 — {e(difficulty_name(state.get('difficulty', 5)))}</b>\n\n"
        f"👤 Ты выставил: <b>{e(player_card['name'])}</b> — {rarity_label_for_card(player_card)}\n"
        f"🤖 Бот выставил: <b>{e(bot_card['name'])}</b> — {rarity_label_for_card(bot_card)}\n\n"
        f"{line}\n"
        f"📊 <b>Счёт:</b> {e(score_text)}"
    )

    remaining = state.get("remaining_player_indices", [])
    if player_idx in remaining:
        remaining.remove(player_idx)
    if round_no >= 5 or not remaining:
        await message.answer(text, parse_mode="HTML")
        await finish_solo_interactive(message, user)
        return

    rows = []
    for idx in remaining:
        c = CARD_BY_ID[state["player"][idx]["card_id"]]
        rows.append([button(text=f"➡️ Раунд {round_no + 1}: {c['name'][:30]}", callback_data=f"fight_next:{idx}")])
    await message.answer(
        text + "\n\nВыбери бойца на следующий раунд. На выбор 20 секунд.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )
    key = ("solo_next", uid, round_no + 1)
    cancel_choice_timer(key)
    choice_timers[key] = asyncio.create_task(auto_next_solo(uid, round_no + 1))


async def finish_solo_interactive(message, user):
    uid = user.id
    state = active_battles.get(uid)
    if not state or state.get("resolved"):
        return

    user_data = get_user_data(user)
    player_name = user_data["name"]
    ppoints = int(state.get("player_points", 0))
    bpoints = int(state.get("bot_points", 0))

    if ppoints == bpoints:
        player_total = team_score(state["player"]) + random.randint(-20, 20)
        bot_total = team_score(state["bot"]) + random.randint(-20, 20)
        winner = player_name if player_total >= bot_total else "Бот"
        tie_text = f"⚖️ Ничья по очкам. Решила общая сила: {player_total} vs {bot_total}.\n"
    else:
        winner = player_name if ppoints > bpoints else "Бот"
        tie_text = ""

    if winner == player_name:
        user_data["wins"] += 1
        reward = 120 + int(state.get("difficulty", 5)) * 8
        xp = 90 + int(state.get("difficulty", 5)) * 4
        add_pass_task_progress(user_data, "win", 1)
    else:
        user_data["losses"] += 1
        reward = 40
        xp = 45
    user_data["battles"] += 1
    add_pass_task_progress(user_data, "battle", 1)
    add_newbie_task_progress(user_data, "battle", 1)

    if not is_owner(uid):
        user_data["fistiks"] += reward
    add_xp(user_data, xp)
    add_season_xp(user_data, SEASON_XP_REWARDS["solo_battle"])
    state["resolved"] = True
    mark_data_dirty("battle_resolved")

    bot_team_text = "\n".join(format_instance(inst, i) for i, inst in enumerate(state["bot"], 1))
    log_text = "\n".join(state.get("fight_log", []))
    summary = (
        f"📊 <b>Итоговый счёт:</b> {e(player_name)} {ppoints} : {bpoints} Бот\n"
        f"{tie_text}"
        f"🏆 <b>Победитель:</b> {e(winner)}"
    )
    text = (
        f"🏁 <b>Бой завершён</b>\n\n"
        f"{log_text}\n\n"
        f"🤖 <b>Полная команда бота</b>\n{bot_team_text}\n\n"
        f"{summary}\n\n"
        f"🎁 Награда: +{reward} 💎 и +{xp} XP"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="⚖️ Оспорить", callback_data="appeal")],
        [button(text="🔁 Новый бой", callback_data="battle:start")],
        [button(text="⬅️ Меню", callback_data="menu")]
    ])
    await send_long(message, text, reply_markup=kb)
    active_battles.pop(uid, None)


async def auto_next_solo(uid, expected_round):
    async def alive():
        state = active_battles.get(uid)
        return bool(state and not state.get("resolved") and int(state.get("fight_round", 0)) + 1 == expected_round and state.get("remaining_player_indices"))

    await asyncio.sleep(10)
    if not await alive():
        return
    state = active_battles.get(uid)
    try:
        await bot.send_message(state.get("chat_id"), "⏳ Осталось 10 секунд. Выбери бойца на раунд, иначе бот выберет за тебя.")
    except Exception:
        pass
    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_battles.get(uid)
    try:
        await bot.send_message(state.get("chat_id"), "⚠️ Осталось 5 секунд. Сейчас выбор станет случайным.")
    except Exception:
        pass
    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_battles.get(uid)
    idx = random.choice(state.get("remaining_player_indices", [0]))
    chat_id = state.get("chat_id")
    class MessageProxy:
        chat = type("Chat", (), {"id": chat_id})()
        async def answer(self, text, **kwargs):
            return await bot.send_message(chat_id, text, **kwargs)
        async def edit_reply_markup(self, **kwargs):
            return None
    class UserProxy:
        id = uid
        full_name = DATA.get("users", {}).get(str(uid), {}).get("name", str(uid))
    try:
        await bot.send_message(chat_id, "⏱ Время вышло. Бот выбрал бойца за тебя.")
    except Exception:
        pass
    await process_solo_fight_round(MessageProxy(), UserProxy(), idx)


@dp.callback_query(F.data.startswith("fight_next:"))
async def fight_next(callback: types.CallbackQuery):
    try:
        idx = int(callback.data.split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка выбора.", show_alert=True)
        return
    state = active_battles.get(callback.from_user.id)
    if not state or idx not in state.get("remaining_player_indices", []):
        await callback.answer("Этот боец уже недоступен.", show_alert=True)
        return
    cancel_choice_timer(("solo_next", callback.from_user.id, int(state.get("fight_round", 0)) + 1))
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await process_solo_fight_round(callback.message, callback.from_user, idx)
    await callback.answer()


@dp.callback_query(F.data.startswith("fight_start:"))
async def fight_start(callback: types.CallbackQuery):
    try:
        starter_idx = int(callback.data.split(":", 1)[1])
    except Exception:
        starter_idx = 0
    await start_solo_fight(callback, starter_idx)


@dp.callback_query(F.data == "fight")
async def fight(callback: types.CallbackQuery):
    await start_solo_fight(callback, 0)


@dp.callback_query(F.data == "appeal")
async def appeal(callback: types.CallbackQuery):
    await callback.message.answer(
        "⚖️ <b>Оспаривание</b>\n\n"
        "Выбери быструю причину или отправь подробный спор командой:\n"
        "<code>/appeal твой аргумент</code>\n\n"
        "Если спор примут вручную, награда: 1000 💎 фисташек + 250 фрагментов случайной карты до легендарной редкости.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="Скорость", callback_data="appeal_reason:speed"), button(text="Хакс", callback_data="appeal_reason:hax")],
            [button(text="Форма", callback_data="appeal_reason:form"), button(text="Синергия", callback_data="appeal_reason:team")],
            [button(text="⬅️ Меню", callback_data="menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("appeal_reason:"))
async def appeal_reason(callback: types.CallbackQuery):
    reason = callback.data.split(":", 1)[1]
    names = {"speed": "скорость", "hax": "хакс", "form": "форма", "team": "синергия"}
    reason_text = names.get(reason, reason)
    p = get_user_data(callback.from_user)
    state = active_battles.get(callback.from_user.id, {}) or {}
    context = ""
    if state:
        try:
            context = (
                f"\n\n<b>Контекст последнего боя:</b>\n"
                f"Арена: <code>{e(state.get('arena', '?'))}</code> · "
                f"Сложность: <b>{e(str(state.get('difficulty', '?')))}</b>\n"
                f"Счёт: <b>{int(state.get('player_points', 0) or 0)} : {int(state.get('bot_points', 0) or 0)}</b>\n"
                f"Resolved: <code>{e(str(state.get('resolved', False)))}</code>"
            )
        except Exception:
            context = ""
    msg = (
        "⚖️ <b>Быстрый спор после боя</b>\n"
        f"Игрок: {e(p.get('name', callback.from_user.full_name))}\n"
        f"ID: <code>{callback.from_user.id}</code>\n"
        f"Причина: <b>{e(reason_text)}</b>"
        f"{context}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="👤 Профиль", callback_data=f"admin_user:{callback.from_user.id}"), button(text="💎 +1000", callback_data=f"appeal_grant:{callback.from_user.id}:1000")],
        [button(text="✉️ Ответить", callback_data=f"appeal_reply:{callback.from_user.id}"), button(text="❌ Отклонить", callback_data=f"appeal_reject:{callback.from_user.id}")],
    ])
    sent = 0
    for tid in list(owner_ids() | right_hand_ids()):
        try:
            await bot.send_message(int(tid), msg, parse_mode="HTML", reply_markup=kb)
            sent += 1
        except Exception as ex:
            logger.debug("quick appeal send failed for %s: %s", tid, ex)
    await callback.message.answer(
        f"✅ Спор отправлен поддержке: <b>{e(reason_text)}</b>.\n"
        "Владелец сможет открыть профиль, выдать компенсацию, ответить или отклонить.",
        reply_markup=back_menu(),
        parse_mode="HTML"
    )
    await callback.answer("Спор отправлен." if sent else "Спор сохранён, но владельцу не удалось отправить сообщение.", show_alert=(sent == 0))

_PROMO_REDEEM_LOCK = None


def _get_promo_redeem_lock():
    global _PROMO_REDEEM_LOCK
    if _PROMO_REDEEM_LOCK is None:
        _PROMO_REDEEM_LOCK = asyncio.Lock()
    return _PROMO_REDEEM_LOCK


async def apply_promo(message, code):
    promos = load_json(PROMO_FILE, {})
    code = code.strip().upper()
    if code not in promos or not promos[code].get("active", False):
        await message.answer("Промокод не найден или отключён.")
        return
    promo = promos[code]
    if promo.get("expires"):
        try:
            if app_now().date() > date.fromisoformat(promo["expires"]):
                await message.answer("Промокод истёк.")
                return
        except Exception:
            pass

    # max_uses is a global counter, so per-user serialization alone is insufficient:
    # two different users could otherwise both consume the last slot concurrently.
    denial = None
    lines = None
    async with _get_promo_redeem_lock():
        p = get_user_data(message.from_user)
        used = p.setdefault("used_promos", [])
        owner_test = is_owner(message.from_user.id)
        if code in used and not owner_test:
            denial = "Ты уже использовал этот промокод."
        else:
            usage = DATA.setdefault("promo_usage", {})
            used_count = int(usage.get(code, 0) or 0)
            try:
                max_uses = int(promo.get("max_uses", 0) or 0)
            except Exception:
                max_uses = 0
            if max_uses > 0 and used_count >= max_uses and not owner_test:
                denial = "Лимит активаций этого промокода уже исчерпан."
            else:
                reward = promo.get("reward", {})
                lines = ["🎟 <b>ПРОМОКОД АКТИВИРОВАН</b>", f"Код: <code>{e(code)}</code>"]
                if "fistiks" in reward:
                    amount = int(reward["fistiks"])
                    p["fistiks"] = int(p.get("fistiks", 0) or 0) + amount
                    lines.append(f"💎 Фисташки: <b>+{short_number(amount)}</b>")
                if "moon_coins" in reward:
                    amount = int(reward["moon_coins"])
                    p["moon_coins"] = int(p.get("moon_coins", 0) or 0) + amount
                    lines.append(f"🐉 Драконит: <b>+{short_number(amount)}</b>")
                if "attempts" in reward:
                    amount = int(reward["attempts"])
                    p["card_attempts"] = int(p.get("card_attempts", 0) or 0) + amount
                    lines.append(f"🎴 Попытки: <b>+{short_number(amount)}</b>")
                if "card" in reward and reward["card"] in CARD_BY_ID:
                    lines.append(add_card(p, reward["card"], int(reward.get("shards", 0))))

                add_xp(p, 40)
                if code not in used:
                    used.append(code)
                # Owner testing never consumes the public global activation quota.
                if not owner_test:
                    usage[code] = used_count + 1
                mark_data_dirty("promo_redeemed")

    if denial:
        await message.answer(denial)
        return
    await message.answer("\n\n".join(lines or []), reply_markup=main_menu(message.from_user.id), parse_mode="HTML")


@dp.message(Command("promo"))
async def promo_cmd(message: types.Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("🎟 Введи так:\n<code>/promo START500</code>", parse_mode="HTML")
        return
    await apply_promo(message, parts[1])


@dp.callback_query(F.data == "promo_help")
async def promo_help(callback: types.CallbackQuery):
    await callback.message.answer(
        "🎟 <b>Промокоды</b>\n\n"
        "Вводи промокод сообщением:\n"
        "<code>/promo START500</code>\n\n"
        "<b>Примеры готовых кодов:</b>\n"
        "• <code>START500</code> — 500 💎\n"
        "• <code>PACKTEST</code> — 1500 💎\n"
        "• <code>ITACHI</code> — карта/осколки Итачи",
        parse_mode="HTML",
        reply_markup=back_menu()
    )
    await callback.answer()


@dp.callback_query(F.data == "friends")
async def friends(callback: types.CallbackQuery):
    await send_friends_menu(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("friends"))
async def friends_cmd(message: types.Message):
    await send_friends_menu(message, message.from_user)


async def send_friends_menu(message, user):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    uid = str(user.id)
    friends_list = DATA.setdefault("friends", {}).get(uid, [])
    rows = []
    text = "👥 <b>Друзья</b>\n\n"
    if friends_list:
        text += "<b>Ваш список друзей:</b>\n"
        for fid in friends_list[:15]:
            fdata = DATA.get("users", {}).get(fid, {})
            fname = fdata.get("name", fid)
            online = "🟢" if is_online(fid) else "⚫"
            text += f"• {online} {e(fname)}\n"
            rows.append([button(text=f"👤 {fname[:22]}", callback_data=f"friend_profile:{fid}")])
    else:
        text += "Список друзей пуст. Добавь игрока через <code>/addfriend ID</code>.\n"
    pending = DATA.setdefault("friend_requests", {}).get(uid, [])
    if pending:
        text += "\n<b>Заявки:</b>\n"
        for from_id in pending[:10]:
            from_name = DATA.get("users", {}).get(from_id, {}).get("name", from_id)
            text += f"• {e(from_name)} хочет добавить тебя\n"
            rows.append([button(text=f"✅ Принять {from_name[:16]}", callback_data=f"friend_accept:{from_id}"), button(text="❌", callback_data=f"friend_decline:{from_id}")])
    rows.append([button(text="🔗 Рефералка", callback_data="referral"), button(text="🌐 Онлайн-бой", callback_data="online_search")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data.startswith("friend_profile:"))
async def friend_profile_cb(callback: types.CallbackQuery):
    fid = callback.data.split(":", 1)[1]
    fp = DATA.get("users", {}).get(fid)
    if not fp:
        await callback.answer("Друг не найден.", show_alert=True); return
    rows = [[button(text="🎁 Подарок", callback_data=f"friend_gift:{fid}"), button(text="❌ Удалить", callback_data=f"friend_remove:{fid}")],[button(text="⬅️ Друзья", callback_data="friends")]]
    await callback.message.answer(public_profile_text(fid, fp), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("friend_gift:"))
async def friend_gift_cb(callback: types.CallbackQuery):
    fid=callback.data.split(":",1)[1]
    rows=[[button(text="💎 1000 фисташек", callback_data=f"friend_sendgift:{fid}:f:1000"), button(text="🎴 5 попыток", callback_data=f"friend_sendgift:{fid}:a:5")],[button(text="⬅️ Профиль друга", callback_data=f"friend_profile:{fid}")]]
    await callback.message.answer("🎁 <b>Подарок другу</b>\n\nПодарок можно отправлять раз в 24 часа.", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("friend_sendgift:"))
async def friend_sendgift_cb(callback: types.CallbackQuery):
    _, fid, typ, amount_s = callback.data.split(":")
    amount=int(amount_s)
    p=get_user_data(callback.from_user); ensure_rpg_fields(p)
    if fid not in DATA.get("users", {}): await callback.answer("Друг не найден.", show_alert=True); return
    gifts=p.setdefault("friend_gifts", {})
    last=gifts.get(fid, "")
    if last:
        try:
            last_dt = _parse_iso_datetime(last)
            if last_dt and utc_now() < last_dt + timedelta(hours=24):
                await callback.answer("Подарок этому другу уже отправлялся за последние 24 часа.", show_alert=True); return
        except Exception: pass
    fp=DATA["users"][fid]; ensure_rpg_fields(fp)
    if typ == "f":
        if int(p.get("fistiks",0) or 0) < amount and not is_owner(callback.from_user.id):
            await callback.answer("Не хватает фисташек.", show_alert=True); return
        if not is_owner(callback.from_user.id): p["fistiks"] = int(p.get("fistiks",0) or 0)-amount
        fp["fistiks"] = int(fp.get("fistiks",0) or 0)+amount
        label=f"{amount} фисташек"
    else:
        if int(p.get("card_attempts",0) or 0) < amount and not is_owner(callback.from_user.id):
            await callback.answer("Не хватает попыток.", show_alert=True); return
        if not is_owner(callback.from_user.id): p["card_attempts"] = int(p.get("card_attempts",0) or 0)-amount
        fp["card_attempts"] = int(fp.get("card_attempts",0) or 0)+amount
        label=f"{amount} попыток"
    gifts[fid]=utc_now().isoformat()
    mark_data_dirty("data_changed")
    await callback.message.answer(f"✅ Подарок отправлен: <b>{e(label)}</b>.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Друзья", callback_data="friends")]]), parse_mode="HTML")
    try: await bot.send_message(int(fid), f"🎁 Друг отправил тебе подарок: {label}.")
    except Exception: pass
    await callback.answer()


@dp.callback_query(F.data.startswith("friend_remove:"))
async def friend_remove_cb(callback: types.CallbackQuery):
    fid=callback.data.split(":",1)[1]; me=str(callback.from_user.id)
    for a,b in [(me,fid),(fid,me)]:
        arr=DATA.setdefault("friends",{}).setdefault(a,[])
        if b in arr: arr.remove(b)
    mark_data_dirty("data_changed")
    await callback.message.answer("✅ Друг удалён.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Друзья", callback_data="friends")]]))
    await callback.answer()


@dp.message(Command("addfriend"))
async def addfriend_cmd(message: types.Message):
    get_user_data(message.from_user)
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer("Напиши так:\n<code>/addfriend 123456789</code>", parse_mode="HTML")
        return
    target = parts[1].strip()
    me = str(message.from_user.id)
    if target == me:
        await message.answer("Нельзя добавить самого себя.")
        return
    if target not in DATA.get("users", {}):
        await message.answer("Этот игрок ещё не заходил в бота. Пусть сначала нажмёт /start.")
        return
    DATA.setdefault("friend_requests", {}).setdefault(target, [])
    if me not in DATA["friend_requests"][target]:
        DATA["friend_requests"][target].append(me)
    mark_data_dirty("data_changed")
    await message.answer("👥 Заявка в друзья отправлена.", reply_markup=back_menu())
    try:
        await bot.send_message(int(target), f"👥 Игрок {e(message.from_user.full_name)} отправил заявку в друзья. Открой /friends.")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("friend_accept:"))
async def friend_accept(callback: types.CallbackQuery):
    other = callback.data.split(":", 1)[1]
    me = str(callback.from_user.id)
    requests = DATA.setdefault("friend_requests", {}).setdefault(me, [])
    if other in requests:
        requests.remove(other)
    DATA.setdefault("friends", {}).setdefault(me, [])
    DATA.setdefault("friends", {}).setdefault(other, [])
    if other not in DATA["friends"][me]:
        DATA["friends"][me].append(other)
    if me not in DATA["friends"][other]:
        DATA["friends"][other].append(me)
    mark_data_dirty("data_changed")
    await callback.message.answer("✅ Друг добавлен.", reply_markup=back_menu())
    try:
        await bot.send_message(int(other), "✅ Твою заявку в друзья приняли.")
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("friend_decline:"))
async def friend_decline(callback: types.CallbackQuery):
    other = callback.data.split(":", 1)[1]
    me = str(callback.from_user.id)
    requests = DATA.setdefault("friend_requests", {}).setdefault(me, [])
    if other in requests:
        requests.remove(other)
    mark_data_dirty("data_changed")
    await callback.message.answer("❌ Заявка отклонена.", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data.startswith("challenge:"))
async def challenge_friend(callback: types.CallbackQuery):
    target = callback.data.split(":", 1)[1]
    get_user_data(callback.from_user)
    me = str(callback.from_user.id)
    if target not in DATA.setdefault("friends", {}).get(me, []):
        await callback.answer("Этот игрок не в друзьях.", show_alert=True)
        return
    if not is_online(target):
        await callback.answer("Друг сейчас офлайн или давно не нажимал кнопки.", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="✅ Принять вызов", callback_data=f"challenge_accept:{me}")],
        [button(text="❌ Отказаться", callback_data=f"challenge_decline:{me}")],
    ])
    try:
        await bot.send_message(int(target), f"⚔️ {e(callback.from_user.full_name)} вызывает тебя на бой.", reply_markup=kb)
        await callback.message.answer("⚔️ Вызов отправлен другу.", reply_markup=back_menu())
    except Exception:
        await callback.answer("Не удалось отправить вызов.", show_alert=True)
        return
    await callback.answer()



def new_pvp_id():
    while True:
        bid = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        if bid not in active_pvp:
            return bid


def pvp_other_player(state, uid):
    uid = str(uid)
    return state["players"][1] if state["players"][0] == uid else state["players"][0]


def pvp_team_text(title, team):
    text = f"{title}\n"
    for i, inst in enumerate(team, 1):
        text += format_instance(inst, i) + "\n"
    return text




def pvp_team_source(uid):
    player = DATA.get("users", {}).get(str(uid), {})
    return player.get("pvp_team_source", "deck")


def auto_fill_pvp_team_if_needed(state, uid):
    uid = str(uid)
    team = state["teams"].setdefault(uid, [])
    if len(team) >= 5:
        return True
    source = pvp_team_source(uid)
    if source in {"deck", "random_bot"}:
        state["teams"][uid] = build_team_for_user(uid, source=source, fill=True)[:5]
        return True
    return False


def advance_pvp_turn(state):
    if state.get("turn", 0) == 0:
        state["turn"] = 1
    else:
        state["turn"] = 0
        state["round"] = int(state.get("round", 1)) + 1


async def send_pvp_round(bid):
    state = active_pvp.get(bid)
    if not state or state.get("done"):
        return

    safety = 0
    while state and not state.get("done") and safety < 10:
        safety += 1
        if len(state["teams"][state["players"][0]]) >= 5 and len(state["teams"][state["players"][1]]) >= 5:
            await finish_pvp_draft(bid)
            return
        current_auto_uid = state["players"][state["turn"]]
        if auto_fill_pvp_team_if_needed(state, current_auto_uid):
            advance_pvp_turn(state)
            continue
        break

    if len(state["teams"][state["players"][0]]) >= 5 and len(state["teams"][state["players"][1]]) >= 5:
        await finish_pvp_draft(bid)
        return

    current_uid = state["players"][state["turn"]]
    enemy_uid = pvp_other_player(state, current_uid)
    current_name = state["names"].get(current_uid, current_uid)
    enemy_name = state["names"].get(enemy_uid, enemy_uid)

    used = [i["card_id"] for team in state["teams"].values() for i in team]
    options = []
    owned_available = collection_candidates(current_uid, used)
    if not owned_available:
        await bot.send_message(
            int(current_uid),
            "🃏 У тебя нет доступных открытых карт для PvP-выбора. Открой сундуки или собери карты из фрагментов.",
            parse_mode="HTML"
        )
        await bot.send_message(int(enemy_uid), "⚠️ Противник не имеет доступных карт. PvP остановлен.")
        state["done"] = True
        return

    max_options = min(5, len(owned_available))
    for _ in range(max_options):
        card = roll_card_for_user(current_uid, exclude=used + [x["id"] for x in options])
        if card is None:
            break
        options.append(card)
    state["options"] = [c["id"] for c in options]

    text = (
        f"⚔️ <b>PvP-бой</b>\n\n"
        f"Раунд: <b>{state['round']}/5</b>\n"
        f"Сейчас выбирает: <b>{e(current_name)}</b>\n"
        f"Противник: <b>{e(enemy_name)}</b>\n"
        f"⏱ 20 секунд на выбор. Если игрок молчит — карта выбирается случайно.\n\n"
        "Выбери 1 карту из доступных:\n\n"
    )
    for i, c in enumerate(options, 1):
        text += card_short(c, i) + "\n\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [button(text=f"Выбрать {i}", callback_data=f"pvp_pick:{bid}:{state['round']}:{i-1}")]
            for i in range(1, len(options) + 1)
        ]
    )

    await bot.send_message(int(current_uid), text, reply_markup=kb, parse_mode="HTML")

    try:
        await bot.send_message(
            int(enemy_uid),
            f"⏳ Сейчас выбирает <b>{e(current_name)}</b>. Жди свой ход.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    key = ("pvp", bid, state["round"], state["turn"])
    cancel_choice_timer(key)
    choice_timers[key] = asyncio.create_task(auto_pick_pvp(bid, state["round"], state["turn"]))


async def auto_pick_pvp(bid, round_no, turn_no):
    async def alive():
        state = active_pvp.get(bid)
        return bool(state and not state.get("done") and state.get("round") == round_no and state.get("turn") == turn_no and state.get("options"))

    await asyncio.sleep(10)
    if not await alive():
        return
    state = active_pvp.get(bid)
    current_uid = state["players"][state["turn"]]
    try:
        await bot.send_message(int(current_uid), "⏳ Осталось 10 секунд. Выбери карту, иначе бот сделает скрытый выбор за тебя.")
    except Exception:
        pass

    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_pvp.get(bid)
    current_uid = state["players"][state["turn"]]
    try:
        await bot.send_message(int(current_uid), "⚠️ Осталось 5 секунд. Дальше выбор будет случайным.")
    except Exception:
        pass

    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_pvp.get(bid)
    idx = random.randrange(len(state["options"]))
    await process_pvp_pick(bid, idx, auto=True)


async def process_pvp_pick(bid, idx, auto=False, callback_message=None, from_user=None):
    state = active_pvp.get(bid)
    if not state or state.get("done"):
        return
    current_uid = state["players"][state["turn"]]
    cancel_choice_timer(("pvp", bid, state["round"], state["turn"]))

    if idx < 0 or idx >= len(state.get("options", [])):
        return

    card = CARD_BY_ID[state["options"][idx]]
    inst = make_instance(card, card_level_for_user(current_uid, card["id"]), player_battle_artifact(current_uid))
    state["teams"][current_uid].append(inst)

    player = DATA.get("users", {}).get(str(current_uid))
    if player is not None:
        result = "карта вышла на поле из твоей коллекции"
        add_xp(player, 20)
        mark_data_dirty("data_changed")
    else:
        result = "карта выбрана"

    name = state["names"].get(current_uid, current_uid)
    prefix = "⏱ Время вышло. Автовыбор PvP:" if auto else "✅ Твой скрытый PvP-выбор:"
    try:
        await bot.send_message(
            int(current_uid),
            f"{prefix} {rarity_label_for_card(card)} <b>{e(card['name'])}</b>\n{e(result)}",
            parse_mode="HTML"
        )
    except Exception:
        pass

    other = pvp_other_player(state, current_uid)
    try:
        await bot.send_message(
            int(other),
            f"📌 {e(name)} сделал скрытый выбор. Карта противника не раскрывается до конца боя.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    if state["turn"] == 0:
        state["turn"] = 1
    else:
        state["turn"] = 0
        state["round"] += 1

    if len(state["teams"][state["players"][0]]) >= 5 and len(state["teams"][state["players"][1]]) >= 5:
        await finish_pvp_draft(bid)
    else:
        await send_pvp_round(bid)


async def finish_pvp_draft(bid):
    state = active_pvp.get(bid)
    if not state:
        return
    state["done"] = True
    state.setdefault("starters", {})

    for uid in state["players"]:
        try:
            text = "🏁 <b>PvP-драфт завершён</b>\n\n"
            text += pvp_team_text("👤 <b>Твоя команда</b>", state["teams"][uid])
            text += "\n🔒 <b>Команда противника скрыта.</b>\nОна раскроется только в итоговом пошаговом бою.\n\n"
            text += "Выбери первого персонажа, который выйдет вперёд."
            rows = []
            for i, inst in enumerate(state["teams"][uid], 1):
                c = CARD_BY_ID[inst["card_id"]]
                rows.append([button(text=f"⚔️ Начать с {i}. {c['name'][:28]}", callback_data=f"pvp_start:{bid}:{i-1}")])
            rows.append([button(text="⬅️ Меню", callback_data="menu")])
            await bot.send_message(int(uid), text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
        except Exception:
            pass


@dp.callback_query(F.data.startswith("pvp_pick:"))
async def pvp_pick(callback: types.CallbackQuery):
    try:
        _, bid, round_s, idx_s = callback.data.split(":")
        idx = int(idx_s)
        button_round = int(round_s)
    except Exception:
        await callback.answer("Ошибка PvP-кнопки.", show_alert=True)
        return

    state = active_pvp.get(bid)
    if not state:
        await callback.answer("Этот PvP-бой уже не найден.", show_alert=True)
        return

    uid = str(callback.from_user.id)
    current_uid = state["players"][state["turn"]]

    if uid != current_uid:
        await callback.answer("Сейчас не твой ход.", show_alert=True)
        return

    if state.get("done"):
        await callback.answer("Драфт уже завершён.", show_alert=True)
        return

    if button_round != state["round"]:
        await callback.answer("Старая кнопка.", show_alert=True)
        return

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await process_pvp_pick(bid, idx, auto=False, callback_message=callback.message, from_user=callback.from_user)
    await callback.answer()


async def start_pvp_interactive_battle(bid):
    state = active_pvp.get(bid)
    if not state or state.get("resolved"):
        return
    p1, p2 = state["players"]
    starters = state.setdefault("starters", {})
    if p1 not in starters or p2 not in starters:
        return
    state["pvp_fight_round"] = 1
    state["pvp_points"] = {p1: 0, p2: 0}
    state["pvp_log"] = []
    state["pvp_current"] = {p1: int(starters[p1]), p2: int(starters[p2])}
    state["pvp_remaining"] = {
        p1: [i for i in range(len(state["teams"][p1])) if i != int(starters[p1])],
        p2: [i for i in range(len(state["teams"][p2])) if i != int(starters[p2])],
    }
    state["pvp_pending"] = {}
    await resolve_pvp_interactive_round(bid)


async def resolve_pvp_interactive_round(bid):
    state = active_pvp.get(bid)
    if not state or state.get("resolved"):
        return
    p1, p2 = state["players"]
    round_no = int(state.get("pvp_fight_round", 1))
    idx1 = int(state["pvp_current"].get(p1, 0))
    idx2 = int(state["pvp_current"].get(p2, 0))
    n1 = state["names"].get(p1, p1)
    n2 = state["names"].get(p2, p2)
    arena_code = state.get("arena", "void")
    result, line = duel_line(round_no, n1, n2, state["teams"][p1][idx1], state["teams"][p2][idx2], arena_code)
    if result == 1:
        state["pvp_points"][p1] += 1
    else:
        state["pvp_points"][p2] += 1
    state["pvp_log"].append(line)
    c1 = CARD_BY_ID[state["teams"][p1][idx1]["card_id"]]
    c2 = CARD_BY_ID[state["teams"][p2][idx2]["card_id"]]
    score = f"{n1} {state['pvp_points'][p1]} : {state['pvp_points'][p2]} {n2}"
    text = (
        f"🎬 <b>PvP Раунд {round_no}/5</b>\n\n"
        f"👤 {e(n1)} выставил: <b>{e(c1['name'])}</b> — {rarity_label_for_card(c1)}\n"
        f"👤 {e(n2)} выставил: <b>{e(c2['name'])}</b> — {rarity_label_for_card(c2)}\n\n"
        f"{line}\n"
        f"📊 <b>Счёт:</b> {e(score)}"
    )
    for uid in state["players"]:
        try:
            await bot.send_message(int(uid), text, parse_mode="HTML")
        except Exception:
            pass

    if round_no >= 5 or not state["pvp_remaining"].get(p1) or not state["pvp_remaining"].get(p2):
        await finish_pvp_interactive(bid)
        return
    state["pvp_fight_round"] = round_no + 1
    state["pvp_pending"] = {}
    await ask_pvp_next_round(bid)


async def ask_pvp_next_round(bid):
    state = active_pvp.get(bid)
    if not state or state.get("resolved"):
        return
    round_no = int(state.get("pvp_fight_round", 2))
    for uid in state["players"]:
        remaining = state["pvp_remaining"].get(uid, [])
        if not remaining:
            continue
        rows = []
        text = f"➡️ <b>PvP Раунд {round_no}</b>\nВыбери следующего бойца. Противник не увидит выбор заранее.\n⏱ 20 секунд на ход.\n\n"
        for idx in remaining:
            inst = state["teams"][uid][idx]
            c = CARD_BY_ID[inst["card_id"]]
            text += f"• {idx+1}. {rarity_label_for_card(c)} <b>{e(c['name'])}</b> | сила {instance_score(inst)}\n"
            rows.append([button(text=f"Выбрать {idx+1}. {c['name'][:28]}", callback_data=f"pvp_next:{bid}:{round_no}:{idx}")])
        try:
            await bot.send_message(int(uid), text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
        except Exception:
            pass
        key = ("pvp_next", bid, uid, round_no)
        cancel_choice_timer(key)
        choice_timers[key] = asyncio.create_task(auto_pick_pvp_next(bid, uid, round_no))


async def auto_pick_pvp_next(bid, uid, expected_round):
    async def alive():
        state = active_pvp.get(bid)
        return bool(state and not state.get("resolved") and int(state.get("pvp_fight_round", 0)) == expected_round and uid not in state.get("pvp_pending", {}) and state.get("pvp_remaining", {}).get(uid))
    await asyncio.sleep(10)
    if not await alive():
        return
    try:
        await bot.send_message(int(uid), "⏳ Осталось 10 секунд. Выбери бойца, иначе бот выберет случайно.")
    except Exception:
        pass
    await asyncio.sleep(5)
    if not await alive():
        return
    try:
        await bot.send_message(int(uid), "⚠️ Осталось 5 секунд. Сейчас выбор станет случайным.")
    except Exception:
        pass
    await asyncio.sleep(5)
    if not await alive():
        return
    state = active_pvp.get(bid)
    idx = random.choice(state["pvp_remaining"].get(uid, [0]))
    await set_pvp_next_choice(bid, uid, idx, auto=True)


async def set_pvp_next_choice(bid, uid, idx, auto=False):
    state = active_pvp.get(bid)
    if not state or state.get("resolved"):
        return
    uid = str(uid)
    if uid in state.setdefault("pvp_pending", {}):
        return
    if idx not in state.get("pvp_remaining", {}).get(uid, []):
        remaining = state.get("pvp_remaining", {}).get(uid, [])
        if not remaining:
            return
        idx = random.choice(remaining)
    state["pvp_pending"][uid] = idx
    if idx in state["pvp_remaining"].get(uid, []):
        state["pvp_remaining"][uid].remove(idx)
    try:
        msg = "⏱ Время вышло. Бот выбрал бойца за тебя." if auto else "✅ Выбор принят. Ждём второго игрока."
        await bot.send_message(int(uid), msg)
    except Exception:
        pass
    other = pvp_other_player(state, uid)
    try:
        await bot.send_message(int(other), f"📌 {e(state['names'].get(uid, uid))} сделал скрытый выбор.", parse_mode="HTML")
    except Exception:
        pass
    if all(u in state.get("pvp_pending", {}) for u in state["players"]):
        for key in list(choice_timers):
            if isinstance(key, tuple) and len(key) >= 2 and key[0] == "pvp_next" and key[1] == bid:
                cancel_choice_timer(key)
        state["pvp_current"] = dict(state["pvp_pending"])
        await resolve_pvp_interactive_round(bid)


@dp.callback_query(F.data.startswith("pvp_next:"))
async def pvp_next_cb(callback: types.CallbackQuery):
    try:
        _, bid, round_s, idx_s = callback.data.split(":")
        round_no = int(round_s)
        idx = int(idx_s)
    except Exception:
        await callback.answer("Ошибка PvP-кнопки.", show_alert=True)
        return
    state = active_pvp.get(bid)
    uid = str(callback.from_user.id)
    if not state or uid not in state.get("players", []):
        await callback.answer("Этот PvP-бой не найден.", show_alert=True)
        return
    if int(state.get("pvp_fight_round", 0)) != round_no:
        await callback.answer("Старая кнопка.", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await set_pvp_next_choice(bid, uid, idx, auto=False)
    await callback.answer("Выбор принят.")


async def finish_pvp_interactive(bid):
    state = active_pvp.get(bid)
    if not state or state.get("resolved"):
        return
    p1, p2 = state["players"]
    n1 = state["names"].get(p1, p1)
    n2 = state["names"].get(p2, p2)
    p1_points = int(state.get("pvp_points", {}).get(p1, 0))
    p2_points = int(state.get("pvp_points", {}).get(p2, 0))
    if p1_points == p2_points:
        total1 = team_score(state["teams"][p1]) + random.randint(-20, 20)
        total2 = team_score(state["teams"][p2]) + random.randint(-20, 20)
        winner_uid, loser_uid = (p1, p2) if total1 >= total2 else (p2, p1)
        tie_text = f"⚖️ Ничья по очкам. Решила общая сила: {total1} vs {total2}.\n"
    else:
        winner_uid, loser_uid = (p1, p2) if p1_points > p2_points else (p2, p1)
        tie_text = ""
    winner_name = state["names"].get(winner_uid, winner_uid)

    if not state.get("scored"):
        if winner_uid in DATA["users"]:
            DATA["users"][winner_uid]["wins"] = DATA["users"][winner_uid].get("wins", 0) + 1
            DATA["users"][winner_uid]["battles"] = DATA["users"][winner_uid].get("battles", 0) + 1
            DATA["users"][winner_uid]["fistiks"] = DATA["users"][winner_uid].get("fistiks", 0) + 160
            add_xp(DATA["users"][winner_uid], 120)
            add_pass_task_progress(DATA["users"][winner_uid], "battle", 1)
            add_newbie_task_progress(DATA["users"][winner_uid], "battle", 1)
            add_pass_task_progress(DATA["users"][winner_uid], "win", 1)
            add_season_xp(DATA["users"][winner_uid], SEASON_XP_REWARDS["pvp_win"])
        if loser_uid in DATA["users"]:
            DATA["users"][loser_uid]["losses"] = DATA["users"][loser_uid].get("losses", 0) + 1
            DATA["users"][loser_uid]["battles"] = DATA["users"][loser_uid].get("battles", 0) + 1
            DATA["users"][loser_uid]["fistiks"] = DATA["users"][loser_uid].get("fistiks", 0) + 60
            add_xp(DATA["users"][loser_uid], 60)
            add_pass_task_progress(DATA["users"][loser_uid], "battle", 1)
            add_newbie_task_progress(DATA["users"][loser_uid], "battle", 1)
            add_season_xp(DATA["users"][loser_uid], SEASON_XP_REWARDS["pvp_loss"])
        state["scored"] = True
        mark_data_dirty("data_changed")

    team1 = pvp_team_text(f"👤 <b>{e(n1)}</b>", state["teams"][p1])
    team2 = pvp_team_text(f"👤 <b>{e(n2)}</b>", state["teams"][p2])
    log_text = "\n".join(state.get("pvp_log", []))
    text = (
        "🏁 <b>PvP-бой завершён</b>\n\n"
        f"{log_text}\n\n"
        f"🔓 <b>Команды раскрыты</b>\n{team1}\n{team2}\n"
        f"📊 <b>Итоговый счёт:</b> {e(n1)} {p1_points} : {p2_points} {e(n2)}\n"
        f"{tie_text}"
        f"🏆 <b>Победитель:</b> {e(winner_name)}\n\n"
        "🎁 Победитель получает +160 💎 и +120 XP.\n"
        "🎁 Проигравший получает +60 💎 и +60 XP."
    )
    state["resolved"] = True
    for uid in state["players"]:
        try:
            await bot.send_message(int(uid), text, reply_markup=back_menu(), parse_mode="HTML")
        except Exception:
            pass
    active_pvp.pop(bid, None)


async def resolve_pvp_battle(bid):
    # Совместимость со старыми кнопками: теперь запускает живой PvP по раундам.
    await start_pvp_interactive_battle(bid)


@dp.callback_query(F.data.startswith("pvp_start:"))
async def pvp_start(callback: types.CallbackQuery):
    try:
        _, bid, idx_s = callback.data.split(":")
        starter_idx = int(idx_s)
    except Exception:
        await callback.answer("Ошибка выбора стартового персонажа.", show_alert=True)
        return

    state = active_pvp.get(bid)
    if not state or not state.get("done"):
        await callback.answer("PvP-бой ещё не готов.", show_alert=True)
        return

    uid = str(callback.from_user.id)
    if uid not in state["players"]:
        await callback.answer("Ты не участник этого PvP.", show_alert=True)
        return

    if uid in state.setdefault("starters", {}):
        await callback.answer("Ты уже выбрал стартового персонажа.", show_alert=True)
        return
    state["starters"][uid] = starter_idx
    other = pvp_other_player(state, uid)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer("Стартовый персонаж выбран.")

    try:
        await bot.send_message(int(uid), "✅ Стартовый персонаж выбран. Если противник уже готов — бой начнётся.")
    except Exception:
        pass
    if other not in state["starters"]:
        try:
            await bot.send_message(int(other), f"📌 {e(state['names'].get(uid, uid))} выбрал стартового персонажа. Твой выбор всё ещё нужен.")
        except Exception:
            pass
        return

    for target in state["players"]:
        try:
            await bot.send_message(int(target), "⚔️ Оба стартовых персонажа выбраны. Теперь бой идёт по раундам, с выбором бойца каждый ход.")
        except Exception:
            pass
    await start_pvp_interactive_battle(bid)

@dp.callback_query(F.data.startswith("pvp_sim:"))
async def pvp_sim(callback: types.CallbackQuery):
    bid = callback.data.split(":", 1)[1]
    state = active_pvp.get(bid)
    if not state or not state.get("done"):
        await callback.answer("PvP-бой ещё не готов.", show_alert=True)
        return
    for uid in state["players"]:
        state.setdefault("starters", {}).setdefault(uid, 0)
    await resolve_pvp_battle(bid)
    await callback.answer("PvP-бой рассчитан.")


@dp.callback_query(F.data.startswith("challenge_accept:"))
async def challenge_accept(callback: types.CallbackQuery):
    challenger = callback.data.split(":", 1)[1]
    accepter = str(callback.from_user.id)

    get_user_data(callback.from_user)

    if challenger == accepter:
        await callback.answer("Нельзя принять свой же вызов.", show_alert=True)
        return

    if challenger not in DATA.get("users", {}):
        await callback.answer("Игрок не найден.", show_alert=True)
        return

    bid = new_pvp_id()
    active_pvp[bid] = {
        "players": [challenger, accepter],
        "names": {
            challenger: DATA["users"].get(challenger, {}).get("name", challenger),
            accepter: DATA["users"].get(accepter, {}).get("name", callback.from_user.full_name),
        },
        "round": 1,
        "turn": 0,
        "teams": {challenger: [], accepter: []},
        "options": [],
        "done": False,
        "scored": False,
        "starters": {},
        "resolved": False,
        "created_at_ts": time.time(),
    }

    await callback.message.answer("✅ Вызов принят. PvP-драфт запущен.", reply_markup=back_menu())
    try:
        await bot.send_message(int(challenger), f"✅ {e(callback.from_user.full_name)} принял вызов. Начинается PvP-драфт.", parse_mode="HTML")
    except Exception:
        pass

    await send_pvp_round(bid)
    await callback.answer()

@dp.callback_query(F.data.startswith("challenge_decline:"))
async def challenge_decline(callback: types.CallbackQuery):
    other = callback.data.split(":", 1)[1]
    await callback.message.answer("❌ Вызов отклонён.", reply_markup=back_menu())
    try:
        await bot.send_message(int(other), f"❌ {e(callback.from_user.full_name)} отклонил вызов.")
    except Exception:
        pass
    await callback.answer()


def _link_players_as_friends(first_id, second_id):
    first_id, second_id = str(first_id), str(second_id)
    DATA.setdefault("friends", {}).setdefault(first_id, [])
    DATA.setdefault("friends", {}).setdefault(second_id, [])
    if second_id not in DATA["friends"][first_id]:
        DATA["friends"][first_id].append(second_id)
    if first_id not in DATA["friends"][second_id]:
        DATA["friends"][second_id].append(first_id)


def _refresh_ref_today(player):
    today = app_now().date().isoformat()
    if player.get("ref_today_date") != today:
        player["ref_today_date"] = today
        player["ref_today"] = 0


def apply_referral_once(inviter_id, newcomer_id, newcomer_player):
    """Начисляет реферальные награды ровно один раз для аккаунта."""
    inviter_id, newcomer_id = str(inviter_id), str(newcomer_id)
    if inviter_id == newcomer_id or inviter_id not in DATA.get("users", {}):
        return False, "invalid"

    _link_players_as_friends(inviter_id, newcomer_id)
    if newcomer_player.get("ref_by"):
        return False, "already"

    inviter = DATA["users"][inviter_id]
    ensure_rpg_fields(inviter)
    ensure_rpg_fields(newcomer_player)
    newcomer_player["ref_by"] = inviter_id
    newcomer_player["fistiks"] = int(newcomer_player.get("fistiks", 0) or 0) + 300
    newcomer_player["card_attempts"] = int(newcomer_player.get("card_attempts", 0) or 0) + 1
    add_xp(newcomer_player, 120)

    inviter["fistiks"] = int(inviter.get("fistiks", 0) or 0) + 500
    inviter["card_attempts"] = int(inviter.get("card_attempts", 0) or 0) + 3
    inviter["ref_count"] = int(inviter.get("ref_count", 0) or 0) + 1
    inviter["ref_earned"] = int(inviter.get("ref_earned", 0) or 0) + 3
    _refresh_ref_today(inviter)
    inviter["ref_today"] = int(inviter.get("ref_today", 0) or 0) + 1
    add_newbie_task_progress(inviter, "referral", 1, auto_reward=False)
    add_xp(inviter, 150)
    mark_data_dirty("referral_reward")
    return True, "granted"


async def _send_referral_result(message, inviter_id, granted):
    inviter = DATA.get("users", {}).get(str(inviter_id), {})
    inviter_name = inviter.get("nickname") or inviter.get("name") or "союзник"
    if granted:
        text = (
            f"{CE['start']} <b>СОЮЗ МУЛЬТИВСЕЛЕННОЙ ОТКРЫТ</b>\n\n"
            f"Тебя пригласил <b>{e(inviter_name)}</b>.\n"
            "Твоя награда: <b>+300 💎</b> и <b>+1 попытка</b>.\n"
            "Владелец ссылки получил: <b>+500 💎</b> и <b>+3 попытки</b>."
        )
    else:
        text = (
            f"{CE['start']} <b>СОЮЗ УЖЕ УЧТЁН</b>\n\n"
            f"Игрок <b>{e(inviter_name)}</b> добавлен в друзья, но повторная реферальная награда не начисляется."
        )
    newcomer = DATA.get("users", {}).get(str(message.from_user.id), {})
    if newcomer.get("onboarding_complete"):
        await message.answer(text, reply_markup=main_menu(message.from_user.id), parse_mode="HTML")
        await ensure_quick_keyboard(message, message.from_user)
    else:
        await message.answer(text, parse_mode="HTML")
        await resume_onboarding(message, message.from_user)


async def accept_direct_referral(message, inviter_id):
    inviter_id = str(inviter_id or "").strip()
    newcomer = get_user_data(message.from_user)
    if not inviter_id.isdigit() or inviter_id not in DATA.get("users", {}) or inviter_id == str(message.from_user.id):
        await message.answer("Ссылка приглашения недействительна.", reply_markup=main_menu(message.from_user.id))
        return
    granted, _ = apply_referral_once(inviter_id, str(message.from_user.id), newcomer)
    if granted:
        await flush_data_now_async("direct_referral")
    await _send_referral_result(message, inviter_id, granted)
    if granted:
        try:
            await bot.send_message(
                int(inviter_id),
                "🕊 <b>НОВЫЙ РЕФЕРАЛ</b>\n\n"
                f"<b>{e(message.from_user.full_name)}</b> вошёл по твоей ссылке.\n"
                "Начислено: <b>+500 💎</b> и <b>+3 попытки</b>.",
                parse_mode="HTML",
            )
        except Exception as ex:
            logger.debug("Direct referral notify failed for %s: %s", inviter_id, ex)


@dp.message(Command("ref"))
async def ref_cmd(message: types.Message):
    await send_friends_menu(message, message.from_user)


@dp.callback_query(F.data == "friend_link")
async def friend_link(callback: types.CallbackQuery):
    code = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    DATA.setdefault("friend_invites", {})[code] = str(callback.from_user.id)
    mark_data_dirty("data_changed")
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=friend_{code}"
    await callback.message.answer(f"🔗 Ссылка для друга:\n{link}")
    await callback.answer()


@dp.callback_query(F.data == "ref_claim")
async def ref_claim_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    claimed = set(map(str, p.setdefault("ref_milestones_claimed", [])))
    count = int(p.get("ref_count", 0))
    lines = []
    for milestone in sorted(REF_MILESTONES):
        if count >= milestone and str(milestone) not in claimed:
            txt = grant_ref_milestone(p, milestone)
            p["ref_milestones_claimed"].append(str(milestone))
            lines.append("✅ " + e(txt))
    if not lines:
        await callback.answer("Пока нет новых реферальных наград.", show_alert=True)
        return
    mark_data_dirty("data_changed")
    await callback.message.answer("🎁 <b>Реферальные награды получены</b>\n\n" + "\n".join(lines), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


async def accept_friend_invite(message, code):
    inviter = DATA.setdefault("friend_invites", {}).get(code)
    if not inviter:
        await message.answer("Ссылка союза не найдена или устарела.", reply_markup=main_menu(message.from_user.id))
        return
    me_id = str(message.from_user.id)
    if str(inviter) == me_id:
        await message.answer("Нельзя открыть союз с самим собой.", reply_markup=main_menu(message.from_user.id))
        return

    user_player = get_user_data(message.from_user)
    granted, _ = apply_referral_once(str(inviter), me_id, user_player)
    if granted:
        await flush_data_now_async("friend_referral")
    await _send_referral_result(message, str(inviter), granted)
    if granted:
        try:
            await bot.send_message(
                int(inviter),
                "🕊 <b>НОВЫЙ СОЮЗНИК</b>\n\n"
                f"<b>{e(message.from_user.full_name)}</b> вошёл по твоей ссылке.\n"
                "Начислено: <b>+500 💎</b> и <b>+3 попытки</b>.",
                parse_mode="HTML",
            )
        except Exception as ex:
            logger.debug("Friend referral notify failed for %s: %s", inviter, ex)


@dp.message(Command("craft"))
async def craft_cmd(message: types.Message):
    await send_craft(message, message.from_user)


@dp.callback_query(F.data == "craft")
async def craft_cb(callback: types.CallbackQuery):
    await send_craft(callback.message, callback.from_user)
    await callback.answer()


def rarity_shards(player, rarity):
    total = 0
    for cid, info in player.get("collection", {}).items():
        if cid in CARD_BY_ID and CARD_BY_ID[cid]["rarity"] == rarity:
            total += info.get("shards", 0)
    return total


def consume_rarity_shards(player, rarity, amount):
    if amount <= 0:
        return True
    left = amount
    for cid, info in player.get("collection", {}).items():
        if cid in CARD_BY_ID and CARD_BY_ID[cid]["rarity"] == rarity:
            take = min(info.get("shards", 0), left)
            info["shards"] -= take
            left -= take
            if left <= 0:
                return True
    return False


async def send_craft(message, user):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    text = (
        f"{CE['craft']} <b>Крафт</b>\n\n"
        "Дубликаты и фрагменты превращаются в попытки или новую карту.\n\n"
        "<b>Фрагменты:</b>\n"
    )
    for code, rarity in RARITY_CODES.items():
        have = rarity_shards(p, rarity)
        cost = CRAFT_COSTS[rarity]
        text += f"{rarity_label(rarity)}: <b>{short_number(have)}</b> / {short_number(cost)}\n"
    text += (
        "\n<b>Попытки:</b>\n"
        "10 Origin → 1\n"
        "10 Rare → 3\n"
        "10 Epic → 6\n"
        "10 любых → 1\n"
    )
    rows = [
        [button(text="🎴 10 Origin → 1", callback_data="craft_attempts:common"), button(text="🎴 10 Rare → 3", callback_data="craft_attempts:rare")],
        [button(text="🎴 10 Epic → 6", callback_data="craft_attempts:epic"), button(text="⚒️ Крафт всего", callback_data="craft_attempts:all")],
        [button(text="🃏 Скрафтить карту", callback_data="craft_card_menu")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data.startswith("craft_make:"))
async def craft_make(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    rarity = RARITY_CODES.get(code)
    if not rarity:
        await callback.answer("Ошибка крафта.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    cost = CRAFT_COSTS[rarity]
    have = rarity_shards(p, rarity)
    if have < cost and not is_owner(callback.from_user.id):
        await callback.answer(f"Нужно {cost} фрагментов {rarity}.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        consume_rarity_shards(p, rarity, cost)
    card = roll_card(weights={rarity: 1}, allowed_rarities=[rarity], universe_id=selected_universe_id(p))
    result = add_card(p, card["id"])
    add_xp(p, 100)
    add_newbie_task_progress(p, "craft", 1)
    add_season_xp(p, SEASON_XP_REWARDS["craft"])
    mark_data_dirty("craft_card")
    await callback.message.answer(
        f"⚒️ <b>Крафт завершён</b>\n\n🐉 {e(card['name'])}\n⭐ {rarity_label_for_card(card)}\n{e(result)}",
        reply_markup=back_menu(),
        parse_mode="HTML"
    )
    await callback.answer()




@dp.callback_query(F.data == "craft_all")
async def craft_all(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    made = []
    for code, rarity in RARITY_CODES.items():
        cost = CRAFT_COSTS[rarity]
        loops = 0
        while (is_owner(callback.from_user.id) or rarity_shards(p, rarity) >= cost) and loops < (1 if is_owner(callback.from_user.id) else 20):
            loops += 1
            if not is_owner(callback.from_user.id):
                consume_rarity_shards(p, rarity, cost)
            card = roll_card(weights={rarity: 1}, allowed_rarities=[rarity], universe_id=selected_universe_id(p))
            add_card(p, card["id"])
            made.append(card)
            if is_owner(callback.from_user.id):
                break
    if not made:
        await callback.answer("Недостаточно фрагментов для крафта.", show_alert=True)
        return
    add_xp(p, 100 * len(made))
    add_newbie_task_progress(p, "craft", len(made))
    add_season_xp(p, SEASON_XP_REWARDS["craft"] * min(len(made), 5))
    mark_data_dirty("craft_all")
    text = "⚒️ <b>Крафт всего завершён</b>\n\n"
    for c in made[:20]:
        text += f"🐉 {e(c['name'])} — {rarity_label_for_card(c)}\n"
    if len(made) > 20:
        text += f"...и ещё {len(made) - 20} карт.\n"
    await callback.message.answer(text, reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()

@dp.message(Command("rating"))
async def rating_cmd(message: types.Message):
    await send_rating(message)


@dp.callback_query(F.data == "rating")
async def rating_cb(callback: types.CallbackQuery):
    await send_rating(callback.message)
    await callback.answer()


def user_total_power(player):
    total = 0
    for cid, info in player.get("collection", {}).items():
        if cid in CARD_BY_ID:
            total += card_power(CARD_BY_ID[cid], info.get("level", 1))
    return total


async def send_rating(message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🌙 Топ-10 сезона", callback_data="rating:season")],
        [button(text="🏆 Общий топ", callback_data="rating:multi")],
        [button(text="⚔️ Боевая сила", callback_data="rating:power")],
        [button(text="💎 Топ по Фисташкам", callback_data="rating:fistiks"), button(text="🥊 Арена", callback_data="rating:arena")],
        [button(text="🏰 Кланы", callback_data="rating:clans")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(
        "🏆 <b>Рейтинг</b>\n\n"
        "Выбери таблицу ниже.",
        reply_markup=kb,
        parse_mode="HTML",
    )


def _ranked_users():
    return [(uid, p) for uid, p in DATA.get("users", {}).items() if is_public_ranked(uid)]


def _score_multi(player):
    return int(player.get("wins", 0) or 0) * 10 + len(player.get("collection", {}) or {}) * 3 + int(player.get("xp", 0) or 0) // 50


def _score_season(player):
    return int(player.get("tournament_points", 0) or 0) + int(player.get("pass_xp", 0) or 0) // 100 + int(player.get("wins", 0) or 0)


def _rating_rows(kind, limit=20):
    users = _ranked_users()
    if kind == "fistiks":
        users = [(uid, p) for uid, p in users if not is_right_hand(uid)]
        return sorted(users, key=lambda x: int(x[1].get("fistiks", 0) or 0), reverse=True)[:limit], lambda p: int(p.get("fistiks", 0) or 0), "💎"
    if kind == "arena":
        return sorted(users, key=lambda x: int(x[1].get("wins", 0) or 0), reverse=True)[:limit], lambda p: int(p.get("wins", 0) or 0), "побед"
    if kind == "power":
        return sorted(users, key=lambda x: battle_power_label(x[1]), reverse=True)[:limit], battle_power_label, "силы"
    if kind == "season":
        return sorted(users, key=lambda x: _score_season(x[1]), reverse=True)[:10], _score_season, "очков"
    return sorted(users, key=lambda x: _score_multi(x[1]), reverse=True)[:limit], _score_multi, "очков"


async def send_rating_type(message, user, kind="season"):
    titles = {
        "season": "🌙 Топ-10 сезона",
        "multi": "🏆 Общий рейтинг",
        "power": "⚔️ Топ боевой силы",
        "fistiks": "💎 Топ по Фисташкам",
        "arena": "🥊 Топ арены",
        "clans": "🏰 Топ кланов",
    }
    if kind == "clans":
        clans = sorted(clan_store().values(), key=lambda c: int(c.get("points",0) or 0) + len(c.get("members",[]))*100, reverse=True)[:20]
        text = f"{titles[kind]}:\n"
        if clans:
            for i, c in enumerate(clans, 1):
                text += f"{i}. {e(c.get('name','Клан'))} — {short_number(int(c.get('points',0) or 0))} PTS · ур.{int(c.get('level',1) or 1)}\n"
        else:
            text += "Пока пусто."
    else:
        arr, value_fn, suffix = _rating_rows(kind)
        text = f"{titles.get(kind, titles['season'])}:\n"
        if arr:
            for i, (uid, p) in enumerate(arr, 1):
                name = p.get("name", uid)
                score = value_fn(p)
                text += f"{i}. {e(name)} — {short_number(score)} {suffix}\n"
        else:
            text += "Пока пусто."
    rows = [
        [button(text="🌙 Сезон", callback_data="rating:season"), button(text="🏆 Общий", callback_data="rating:multi")],
        [button(text="⚔️ Сила", callback_data="rating:power"), button(text="💎 Топ по Фисташкам", callback_data="rating:fistiks")],
        [button(text="🥊 Арена", callback_data="rating:arena"), button(text="🏰 Кланы", callback_data="rating:clans")],
        [button(text="⬅️ Назад", callback_data="rating"), button(text="🏠 Меню", callback_data="menu")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data.startswith("rating:"))
async def rating_type_cb(callback: types.CallbackQuery):
    kind = callback.data.split(":", 1)[1]
    if kind not in {"season", "multi", "power", "fistiks", "arena", "clans"}:
        kind = "season"
    await send_rating_type(callback.message, callback.from_user, kind)
    await callback.answer()


def get_daily_event():
    idx = int(app_now().date().strftime("%Y%m%d")) % len(DAILY_EVENT_POOL)
    return DAILY_EVENT_POOL[idx]


def ensure_raid_state():
    """Один недельный рейд-босс.

    Важно: если босса убили раньше конца недели, новый НЕ появляется сразу.
    Он остаётся поверженным до недельного сброса, а награды уже раздаются участникам.
    """
    DATA.setdefault("raid", {})
    raid = DATA["raid"]
    now = utc_now()
    end_raw = raid.get("ends_at", "")
    expired = True
    if end_raw:
        try:
            end_dt = _parse_iso_datetime(end_raw)
            expired = not end_dt or now >= end_dt
        except Exception:
            expired = True

    if raid and not raid.get("settled") and (expired or int(raid.get("hp_left", 0) or 0) <= 0):
        settle_raid_rewards(raid, "expired" if expired else "defeated")
        mark_data_dirty("data_changed")

    if not raid or expired:
        boss = RAID_BOSSES[int(now.strftime("%U")) % len(RAID_BOSSES)]
        raid.clear()
        raid.update({
            "boss_id": boss["id"],
            "boss_name": boss["name"],
            "desc": boss["desc"],
            "protection": boss["protection"],
            "max_hp": int(boss["hp"]),
            "hp_left": int(boss["hp"]),
            "started_at": now.isoformat(),
            "ends_at": (now + timedelta(days=RAID_DURATION_DAYS)).isoformat(),
            "damage": {},
            "hits": {},
            "settled": False,
        })
        mark_data_dirty("data_changed")
    return raid


def pick_raid_boss_deck():
    # Оставлено для совместимости со старыми вызовами. В PATCH16B рейд — это один босс, не пачка карт.
    return []


def format_raid_top(raid, limit=5):
    dmg = raid.get("damage", {})
    if not dmg:
        return "Пока никто не бил босса."
    items = sorted(dmg.items(), key=lambda x: int(x[1]), reverse=True)[:limit]
    lines = []
    for i, (uid, value) in enumerate(items, 1):
        name = DATA.get("users", {}).get(uid, {}).get("name", uid)
        lines.append(f"{i}. {e(name)} — <b>{int(value):,}</b>".replace(",", " "))
    return "\n".join(lines)



def settle_raid_rewards(raid, reason="finished"):
    """Выдаёт финальные награды всем участникам рейда один раз."""
    if not raid or raid.get("settled"):
        return []
    damage = raid.get("damage", {}) or {}
    if not damage:
        raid["settled"] = True
        return []
    items = sorted(damage.items(), key=lambda x: int(x[1]), reverse=True)
    results = []
    for rank, (uid, dmg) in enumerate(items, 1):
        player = DATA.get("users", {}).get(str(uid))
        if not player:
            continue
        # База всем участникам + усиленные тиры за вклад.
        fistiks = 450
        moon = 1
        pass_xp = 140
        if rank <= 2:
            tier = "S"
            fistiks += 4500
            moon += 8
            pass_xp += 900
        elif rank <= 6:
            tier = "A"
            fistiks += 2500
            moon += 5
            pass_xp += 600
        elif rank <= 11:
            tier = "B"
            fistiks += 1400
            moon += 3
            pass_xp += 350
        else:
            tier = "C"
            fistiks += 700
            moon += 1
            pass_xp += 180
        player["fistiks"] = int(player.get("fistiks", 0)) + fistiks
        player["moon_coins"] = int(player.get("moon_coins", 0)) + moon
        player["pass_xp"] = int(player.get("pass_xp", 0)) + pass_xp
        player.setdefault("raid_rewards", []).append({
            "raid_id": raid.get("boss_id", ""),
            "boss_name": raid.get("boss_name", ""),
            "rank": rank,
            "tier": tier,
            "damage": int(dmg),
            "fistiks": fistiks,
            "moon_coins": moon,
            "pass_xp": pass_xp,
            "at": utc_now().isoformat(timespec="seconds"),
            "reason": reason,
        })
        results.append((str(uid), rank, tier, int(dmg), fistiks, moon, pass_xp))
    raid["settled"] = True
    raid["settled_at"] = utc_now().isoformat(timespec="seconds")
    DATA.setdefault("raid_history", []).append({
        "boss_id": raid.get("boss_id", ""),
        "boss_name": raid.get("boss_name", ""),
        "settled_at": raid.get("settled_at"),
        "participants": len(results),
        "top": results[:10],
    })
    if len(DATA.get("raid_history", [])) > 20:
        del DATA["raid_history"][:-20]
    return results


async def notify_raid_rewards(raid, results):
    for uid, rank, tier, dmg, fistiks, moon, pass_xp in results:
        try:
            await bot.send_message(
                int(uid),
                f"🐉 Рейд завершён: <b>{e(raid.get('boss_name','Босс'))}</b>\n\n"
                f"Твоё место: <b>#{rank}</b> | тир <b>{tier}</b>\n"
                f"Урон: <b>{dmg:,}</b>\n".replace(",", " ") +
                f"Награда: +{fistiks} 💎 +{moon} 🐉 +{pass_xp} pass",
                parse_mode="HTML"
            )
        except Exception as ex:
            logger.debug("Raid reward notice failed for %s: %s", uid, ex)


def raid_damage_from_team(user_id, team):
    base = max(500, team_score(team))
    rarity_bonus = sum(RARITY_BONUS.get(CARD_BY_ID[i["card_id"]]["rarity"], 0) for i in team)
    raw = base * random.randint(8, 18) + rarity_bonus * 120 + random.randint(5_000, 45_000)
    names = " ".join(CARD_BY_ID[i["card_id"]]["name"].lower() for i in team)
    god_terms = ["фезарин", "творец", "истина", "zeno", "зено", "всевыш", "бог", "yhwach", "юхабах"]
    if any(t in names for t in god_terms):
        raw = int(raw * 0.45)
        note = "🛡 Защита босса срезала часть урона от слишком абсолютных форм."
    else:
        note = "⚔️ Урон прошёл обычной силой колоды."
    if is_owner(user_id):
        raw = min(raw, 5_000_000)  # владелец видит механику, но не ломает рейд одним нажатием
    return max(1_000, raw), note



async def send_events_hub(message, user):
    get_user_data(user)
    event = get_daily_event()
    raid = ensure_raid_state()
    hp_left = int(raid.get("hp_left", 0) or 0)
    max_hp = max(1, int(raid.get("max_hp", 1) or 1))
    percent = max(0.0, min(100.0, hp_left * 100 / max_hp))
    text = (
        "🎪 <b>СОБЫТИЯ МУЛЬТИВСЕЛЕННОЙ</b>\n"
        "<i>Забери награду дня и нанеси урон боссу — участие в турнире засчитывается автоматически.</i>\n\n"
        f"<blockquote>🔥 <b>{e(event['name'])}</b>\n{e(event['desc'])}\n"
        f"Награда: +{event['coins']} 🐉 · +{event['pass_xp']} очков пропуска</blockquote>\n"
        f"<blockquote>👹 <b>{e(raid['boss_name'])}</b>\n"
        f"HP: <b>{hp_left:,}/{max_hp:,}</b> · {percent:.2f}%\n".replace(",", " ")
        + f"Твой урон: <b>{int(raid.get('damage', {}).get(str(user.id), 0)):,}</b></blockquote>".replace(",", " ")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🔥 Забрать событие дня", callback_data="event_daily")],
        [button(text="⚔️ Ударить рейд-босса", callback_data="raid_hit")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")



@dp.message(Command("events"))
async def events_cmd(message: types.Message):
    await send_events_hub(message, message.from_user)


@dp.callback_query(F.data == "events")
async def events_cb(callback: types.CallbackQuery):
    await send_events_hub(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "event_daily")
async def event_daily_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    today = app_today_iso()
    event = get_daily_event()
    if p.get("last_event_daily") == today:
        await callback.answer("Ивент дня уже забран.", show_alert=True)
        return
    p["last_event_daily"] = today
    p["moon_coins"] = int(p.get("moon_coins", 0)) + int(event["coins"])
    p["pass_xp"] = int(p.get("pass_xp", 0)) + int(event["pass_xp"])
    add_season_xp(p, SEASON_XP_REWARDS["event"], action_key=f"event_daily:{today}")
    mark_data_dirty("event_daily")
    await callback.message.answer(
        f"🔥 <b>{e(event['name'])}</b> выполнен: +{event['coins']} 🐉 и +{event['pass_xp']} очков Боевого пропуска.",
        reply_markup=back_menu(),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "raid_info")
async def raid_info_cb(callback: types.CallbackQuery):
    raid = ensure_raid_state()
    hp_left = int(raid.get("hp_left", 0))
    max_hp = int(raid.get("max_hp", 1))
    defeated = hp_left <= 0
    status = "☠️ <b>Босс уже повержен.</b> Награды участникам зафиксированы, новый босс придёт после недельного сброса." if defeated else "⚔️ Босс жив. У каждого игрока есть 3 удара каждые 5 часов."
    text = (
        f"🐉 <b>Один рейд-босс недели</b>\n"
        f"<b>{e(raid['boss_name'])}</b>\n\n"
        f"{e(raid['desc'])}\n\n"
        f"HP: <b>{hp_left:,}</b> / <b>{max_hp:,}</b>\n".replace(",", " ") +
        f"Защита: {e(raid['protection'])}\n"
        f"Сброс босса: <code>{e(str(raid.get('ends_at',''))[:16])}</code>\n\n"
        f"{status}\n\n"
        "Тут нет пяти боссов и скрытой пачки карт: вся мультивселенная бьёт одного сильного противника.\n\n"
        "<b>Топ урона:</b>\n"
        f"{format_raid_top(raid)}"
    )
    rows = [[button(text="⚔️ Ударить босса", callback_data="raid_hit")]]
    if is_owner(callback.from_user.id):
        rows.append([
            button(text="👑 Админ-удар", callback_data="admin_raid_hit"),
            button(text="☠️ Добить босса", callback_data="admin_raid_kill"),
        ])
    rows.append([button(text="⬅️ Ивенты", callback_data="events")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


async def send_raid_hit_menu(message, user):
    p = get_user_data(user)
    raid = ensure_raid_state()
    if int(raid.get("hp_left", 0) or 0) <= 0:
        await message.answer(
            f"{CE['raid']} <b>Рейд-босс уже повержен</b>\n\n"
            "Новый босс появится после недельного сброса. Урон и награды уже сохранены.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🐉 Рейд-босс", callback_data="raid_info")]]),
            parse_mode="HTML"
        )
        return
    now = utc_now()
    window_start = _parse_iso_datetime(p.get("raid_hit_window_start", ""))
    count = int(p.get("raid_hit_window_count", 0) or 0)
    if not window_start or now >= window_start + timedelta(minutes=RAID_HIT_COOLDOWN_MINUTES):
        count = 0
        window_start = now
    left = max(0, RAID_HIT_LIMIT_PER_WINDOW - count)
    next_time = window_start + timedelta(minutes=RAID_HIT_COOLDOWN_MINUTES)
    text = (
        f"{CE['raid']} <b>Удар по рейд-боссу</b>\n\n"
        f"Босс недели: <b>{e(raid.get('boss_name','Босс'))}</b>\n"
        f"Осталось ударов в текущем окне: <b>{left}/{RAID_HIT_LIMIT_PER_WINDOW}</b>\n"
        f"Новое окно: <code>{e(next_time.strftime('%Y-%m-%d %H:%M'))}</code>\n\n"
        "Выбери, кто бьёт босса: сохранённая колода, авто-колода или ручной выбор карт."
    )
    rows = [
        [button(text="🛡️ Своей колодой", callback_data="raid_attack:deck")],
        [button(text="🤖 Авто-колодой", callback_data="raid_attack:random_bot")],
        [button(text="🎴 Выбрать карты вручную", callback_data="battle_source:raid:manual")],
        [button(text="⬅️ Рейд", callback_data="raid_info")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")

def register_raid_hit_window(player, now):
    window_start = _parse_iso_datetime(player.get("raid_hit_window_start", ""))
    count = int(player.get("raid_hit_window_count", 0) or 0)
    if not window_start or now >= window_start + timedelta(minutes=RAID_HIT_COOLDOWN_MINUTES):
        player["raid_hit_window_start"] = now.isoformat()
        player["raid_hit_window_count"] = 0
        window_start = now
        count = 0
    if count >= RAID_HIT_LIMIT_PER_WINDOW:
        next_time = window_start + timedelta(minutes=RAID_HIT_COOLDOWN_MINUTES)
        return False, next_time
    player["raid_hit_window_count"] = count + 1
    player["last_raid_hit"] = now.isoformat()
    return True, None

async def perform_raid_hit(message, user, source=None):
    p = get_user_data(user)
    raid = ensure_raid_state()
    if int(raid.get("hp_left", 0) or 0) <= 0:
        await message.answer(
            f"{CE['raid']} <b>Босс уже повержен</b>\n\n"
            "Удар не потрачен. Жди недельного сброса и нового босса.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🐉 Рейд-босс", callback_data="raid_info")]]),
            parse_mode="HTML"
        )
        return
    now = utc_now()
    ok, next_time = register_raid_hit_window(p, now)
    if not ok:
        mins = int((next_time - now).total_seconds() // 60) + 1
        await message.answer(f"⏳ Рейд-окно закрыто. Новые 3 удара будут доступны через <b>{mins}</b> мин.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Рейд", callback_data="raid_info")]]), parse_mode="HTML")
        return
    source = source or p.get("battle_team_source", p.get("pvp_team_source", "deck"))
    team = build_team_for_user(user.id, source=source, fill=True)
    if len(team) < 5:
        team = build_team_for_user(user.id, source="random_bot", fill=True)
    dmg, note = raid_damage_from_team(user.id, team)
    before = int(raid.get("hp_left", 0))
    dealt = min(before, dmg)
    raid["hp_left"] = max(0, before - dealt)
    uid = str(user.id)
    raid.setdefault("damage", {})
    raid["damage"][uid] = int(raid["damage"].get(uid, 0)) + dealt
    raid.setdefault("hits", {})
    raid["hits"][uid] = int(raid["hits"].get(uid, 0)) + 1
    p["raid_damage"] = int(p.get("raid_damage", 0)) + dealt
    p["tournament_joined"] = True
    p["tournament_points"] = int(p.get("tournament_points", 0)) + max(1, dealt // 100000)
    reward = max(80, dealt // 5000)
    p["fistiks"] = int(p.get("fistiks", 0)) + reward
    extra = ""
    if random.random() < 0.35:
        p["moon_coins"] = int(p.get("moon_coins", 0)) + 1
        extra = " +1 🐉"
    artifact_line = ""
    if random.random() < 0.10:
        artifact = grant_random_artifact(p)
        artifact_line = f"\n🧿 Артефакт: {artifact_label(artifact)}"
    results = []
    if int(raid.get("hp_left", 0)) <= 0:
        results = settle_raid_rewards(raid, "defeated")
    mark_data_dirty("data_changed")
    if results:
        await notify_raid_rewards(raid, results)
    hp_left = int(raid.get("hp_left", 0))
    team_names = ", ".join(e(CARD_BY_ID[i["card_id"]]["name"]) for i in team if i.get("card_id") in CARD_BY_ID)
    await message.answer(
        f"{CE['raid']} <b>Удар по рейд-боссу</b>\n\n"
        f"Босс: <b>{e(raid['boss_name'])}</b>\n"
        f"Команда: {team_names}\n"
        f"Урон: <b>{dealt:,}</b>\n".replace(",", " ") +
        f"Осталось HP: <b>{hp_left:,}</b>\n".replace(",", " ") +
        f"{e(note)}\n\n"
        f"Награда за удар: +{reward} 💎{extra}{artifact_line}\n\n"
        "<b>Топ урона:</b>\n"
        f"{format_raid_top(raid)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="⚔️ Ударить ещё", callback_data="raid_hit")],
            [button(text="🐉 Рейд-босс", callback_data="raid_info")],
            [button(text="⬅️ Ивенты", callback_data="events")],
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "raid_hit")
async def raid_hit_cb(callback: types.CallbackQuery):
    await send_raid_hit_menu(callback.message, callback.from_user)
    await callback.answer()

@dp.callback_query(F.data.startswith("raid_attack:"))
async def raid_attack_cb(callback: types.CallbackQuery):
    source = callback.data.split(":", 1)[1]
    if source not in {"deck", "random_bot", "manual"}:
        await callback.answer("Неизвестный выбор.", show_alert=True)
        return
    if source == "manual":
        manual_team_drafts[str(callback.from_user.id)] = {"target": "raid", "cards": [], "updated_at_ts": time.time()}
        await send_manual_team_picker(callback.message, callback.from_user, "raid", 0)
    else:
        await perform_raid_hit(callback.message, callback.from_user, source)
    await callback.answer()



@dp.callback_query(F.data == "admin_raid_hit")
async def admin_raid_hit_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    raid = ensure_raid_state()
    before = int(raid.get("hp_left", 0))
    dmg = min(before, max(10_000_000, int(raid.get("max_hp", 1)) // 10))
    raid["hp_left"] = max(0, before - dmg)
    uid = str(callback.from_user.id)
    raid.setdefault("damage", {})
    raid["damage"][uid] = int(raid["damage"].get(uid, 0)) + dmg
    raid.setdefault("hits", {})
    raid["hits"][uid] = int(raid["hits"].get(uid, 0)) + 1
    results = []
    if int(raid.get("hp_left", 0)) <= 0:
        results = settle_raid_rewards(raid, "admin_hit")
    mark_data_dirty("data_changed")
    if results:
        await notify_raid_rewards(raid, results)
    await callback.message.answer(
        f"👑 Админ-удар нанесён: <b>{dmg:,}</b> урона. Осталось HP: <b>{int(raid.get('hp_left',0)):,}</b>".replace(",", " "),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🐉 Рейд", callback_data="raid_info")]]),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_raid_kill")
async def admin_raid_kill_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    raid = ensure_raid_state()
    before = int(raid.get("hp_left", 0))
    uid = str(callback.from_user.id)
    raid.setdefault("damage", {})
    raid["damage"][uid] = int(raid["damage"].get(uid, 0)) + max(1, before)
    raid.setdefault("hits", {})
    raid["hits"][uid] = int(raid["hits"].get(uid, 0)) + 1
    raid["hp_left"] = 0
    results = settle_raid_rewards(raid, "admin_kill")
    mark_data_dirty("data_changed")
    if results:
        await notify_raid_rewards(raid, results)
    await callback.message.answer(
        f"☠️ Босс <b>{e(raid.get('boss_name',''))}</b> добит. Финальные награды выданы всем участникам.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🏯 Ивенты", callback_data="events")]]),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "tournament_join")
async def tournament_join_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    if p.get("tournament_joined"):
        await callback.answer("Ты уже в турнире сезона.", show_alert=True)
        return
    p["tournament_joined"] = True
    p["tournament_points"] = int(p.get("tournament_points", 0)) + 1
    mark_data_dirty("data_changed")
    await callback.message.answer("🏆 Ты зарегистрирован в турнире сезона. Победы, рейд-урон и активность будут поднимать очки.", reply_markup=back_menu())
    await callback.answer()





async def draw_card_to_message(message, user, callback=None):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    ok, source, wait = consume_summon_attempt(p, user.id)
    if not ok:
        paid = max(0, int(p.get("card_attempts", 0) or 0))
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🏪 Купить попытки", callback_data="shop_attempts"), button(text="⚒️ Скрафтить", callback_data="craft")],
            [button(text="🎁 Награды", callback_data="hub:rewards"), button(text="⬅️ Меню", callback_data="menu")],
        ])
        await message.answer(
            "🎴 <b>ПРИЗЫВ ЗАРЯЖАЕТСЯ</b>\n\n"
            f"Следующая бесплатная попытка: <b>{e(format_wait_hms(wait))}</b>.\n"
            f"Дополнительных попыток: <b>{paid}</b>.",
            reply_markup=kb, parse_mode="HTML",
        )
        if callback:
            await callback.answer("Призыв ещё не готов.", show_alert=True)
        return

    card, pity_note = roll_card_with_pity(
        p,
        weights=SUMMON_WEIGHTS,
        universe_id=selected_universe_id(p),
        allow_super_absolute=False,
    )
    old = (p.get("collection", {}) or {}).get(card["id"], {}) or {}
    was_owned = int(old.get("count", 0) or 0) > 0 and bool(old.get("unlocked", True))
    before_shards = int(old.get("shards", 0) or 0)
    add_card(p, card["id"])
    info = (p.get("collection", {}) or {}).get(card["id"], {}) or {}
    gained_shards = max(0, int(info.get("shards", 0) or 0) - before_shards)
    add_xp(p, 80)
    add_pass_task_progress(p, "chest", 1)
    add_newbie_task_progress(p, "free_pack", 1)
    add_season_xp(p, SEASON_XP_REWARDS["draw"])
    mark_data_dirty("card_draw")

    level = int(info.get("level", 1) or 1)
    power = card_power(card, level)
    owned, total = universe_progress(p, selected_universe_id(p))
    next_wait = free_card_wait_minutes(p)
    if was_owned:
        headline = "♻️ <b>ДУБЛИКАТ ПЕРСОНАЖА</b>"
        reward_line = f"Персонаж уже был в коллекции — сохранено <b>+{gained_shards} фрагментов</b> для улучшения."
    else:
        headline = "✨ <b>НОВЫЙ ПЕРСОНАЖ</b>"
        reward_line = "Персонаж полностью добавлен в коллекцию."
    caption = (
        f"{headline}\n{rarity_label_for_card(card)} <b>{e(card['name'])}</b>\n"
        f"🎭 Форма: <b>{e(card.get('form') or 'Основная')}</b>\n"
        f"⚔️ Сила: <b>{short_number(power)}</b> · уровень <b>{level}</b>\n\n"
        f"{reward_line}{pity_note}\n\n"
        f"<blockquote>{e(card_public_description(card))}</blockquote>\n"
        f"🃏 Коллекция мира: <b>{short_number(owned)}/{short_number(total)}</b>\n"
        f"🎟 Источник: {e(source)} · следующий бесплатный: <b>{compact_wait_label(next_wait)}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🔁 Призвать ещё", callback_data="draw_card")],
        [button(text="🃏 Открыть персонажа", callback_data=f"card:{card_cb_id(card['id'])}"), button(text="⚔️ В колоду", callback_data=f"deck_add:{card_cb_id(card['id'])}")],
        [button(text="🎁 Награды", callback_data="hub:rewards"), button(text="⬅️ Меню", callback_data="menu")],
    ])
    await send_card_result(message, card["id"], caption, kb)
    if callback:
        await callback.answer("Персонаж получен!" if not was_owned else f"Дубликат: +{gained_shards} фрагментов.")


@dp.callback_query(F.data == "draw_card")
async def draw_card_cb(callback: types.CallbackQuery):
    await draw_card_to_message(callback.message, callback.from_user, callback)


@dp.callback_query(F.data == "craft_card_menu")
async def craft_card_menu_cb(callback: types.CallbackQuery):
    rows = [
        [button(text="⚪ Origin", callback_data="craft_make:common"), button(text="🔷 Rare", callback_data="craft_make:rare")],
        [button(text="🟣 Epic", callback_data="craft_make:epic"), button(text="🟡 Legendary", callback_data="craft_make:legendary")],
        [button(text="🔴 Absolute", callback_data="craft_make:mythic")],
        [button(text="⬅️ Крафт", callback_data="craft")],
    ]
    await callback.message.answer("🃏 <b>Крафт карты</b>\n\nВыбери редкость случайной карты из текущей вселенной.", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("craft_attempts:"))
async def craft_attempts_cb(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    p = get_user_data(callback.from_user)
    ensure_rpg_fields(p)
    plan = {
        "common": ("Обычный", 10, 1),
        "rare": ("Редкий", 10, 3),
        "epic": ("Эпический", 10, 6),
    }
    made = 0
    if code == "all":
        for rarity, cost, reward in plan.values():
            while rarity_shards(p, rarity) >= cost:
                consume_rarity_shards(p, rarity, cost)
                made += reward
    elif code in plan:
        rarity, cost, reward = plan[code]
        if rarity_shards(p, rarity) < cost and not is_owner(callback.from_user.id):
            await callback.answer("Недостаточно фрагментов.", show_alert=True)
            return
        if not is_owner(callback.from_user.id):
            consume_rarity_shards(p, rarity, cost)
        made = reward
    else:
        await callback.answer("Ошибка крафта.", show_alert=True)
        return
    if made <= 0:
        await callback.answer("Пока нечего крафтить.", show_alert=True)
        return
    p["card_attempts"] = int(p.get("card_attempts", 0) or 0) + made
    add_season_xp(p, SEASON_XP_REWARDS["craft"])
    mark_data_dirty("craft_attempts")
    await callback.message.answer(f"⚒️ Готово: +<b>{made}</b> попыток призыва.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🎴 Призвать карту", callback_data="draw_card")],[button(text="⬅️ Крафт", callback_data="craft")]]), parse_mode="HTML")
    await callback.answer("Попытки добавлены.")


@dp.callback_query(F.data == "shop_dragonite")
async def shop_dragonite_cb(callback: types.CallbackQuery):
    text = (
        f"{DRAGONITE_LABEL} <b>Драконит</b>\n\n"
        "Драконит — редкая премиум-валюта для кейсов, привилегий и особых покупок.\n\n"
        "<b>Курс внутри игры:</b>\n"
        "1 🐉 = 1 ₽\n"
        "1 🐉 = 5 ₸\n"
        "100 🐉 = 1 $\n\n"
        "Оплата через Telegram Stars доступна в разделе Stars. Остальные способы владелец может принимать вручную."
    )
    rows = [[button(text="⭐ Stars-наборы", callback_data="stars_shop")],[button(text="⬅️ Магазин", callback_data="shop")]]
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "shop_attempts")
async def shop_attempts_cb(callback: types.CallbackQuery):
    rows = []
    text = "🎴 <b>Покупка попыток</b>\n\nВыбери пакет попыток. Покупка за фисташки — обычный путь, за драгонит — премиум-ускорение.\n\n<b>За фисташки:</b>\n"
    for amount, cost in ATTEMPT_PACKS:
        text += f"• {amount} попыток — {cost} 💎\n"
        rows.append([button(text=f"💎 {cost} → {amount} попыток", callback_data=f"buy_attempts:f:{amount}:{cost}")])
    text += "\n<b>За драгонит:</b>\n"
    for amount, cost in DRAGONITE_ATTEMPT_PACKS:
        text += f"• {amount} попыток — {cost} 🐉\n"
        rows.append([button(text=f"🐉 {cost} → {amount} попыток", callback_data=f"buy_attempts:d:{amount}:{cost}")])
    rows.append([button(text="⬅️ Магазин", callback_data="shop")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_attempts:"))
async def buy_attempts_cb(callback: types.CallbackQuery):
    _, currency, amount_s, cost_s = callback.data.split(":")
    amount, cost = int(amount_s), int(cost_s)
    p = get_user_data(callback.from_user)
    ensure_rpg_fields(p)
    if currency == "f":
        if int(p.get("fistiks", 0) or 0) < cost and not is_owner(callback.from_user.id):
            await callback.answer("Не хватает фисташек.", show_alert=True); return
        if not is_owner(callback.from_user.id): p["fistiks"] = int(p.get("fistiks", 0) or 0) - cost
    else:
        if int(p.get("moon_coins", 0) or 0) < cost and not is_owner(callback.from_user.id):
            await callback.answer("Не хватает драгонита.", show_alert=True); return
        if not is_owner(callback.from_user.id): p["moon_coins"] = int(p.get("moon_coins", 0) or 0) - cost
    p["card_attempts"] = int(p.get("card_attempts", 0) or 0) + amount
    mark_data_dirty("data_changed")
    await callback.message.answer(f"✅ Куплено: +<b>{amount}</b> попыток.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🎴 Призвать карту", callback_data="draw_card")],[button(text="⬅️ Магазин", callback_data="shop")]]), parse_mode="HTML")
    await callback.answer("Покупка успешна.")


@dp.callback_query(F.data == "shop_fistiks")
async def shop_fistiks_cb(callback: types.CallbackQuery):
    rows=[]
    text=f"{PISTACHIOS_LABEL} <b>Фисташки</b>\n\nОсновная валюта для прогресса, попыток, улучшений и обычных покупок.\n\n"
    for amount,cost in FISTIK_PACKS:
        text += f"• {amount} 💎 — {cost} 🐉\n"
        rows.append([button(text=f"🐉 {cost} → {amount} 💎", callback_data=f"buy_fistiks:{amount}:{cost}")])
    rows.append([button(text="⬅️ Магазин", callback_data="shop")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_fistiks:"))
async def buy_fistiks_cb(callback: types.CallbackQuery):
    _, amount_s, cost_s = callback.data.split(":")
    amount, cost = int(amount_s), int(cost_s)
    p=get_user_data(callback.from_user)
    if int(p.get("moon_coins",0) or 0) < cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает драгонита.", show_alert=True); return
    if not is_owner(callback.from_user.id): p["moon_coins"] = int(p.get("moon_coins",0) or 0)-cost
    p["fistiks"] = int(p.get("fistiks",0) or 0)+amount
    mark_data_dirty("data_changed")
    await callback.message.answer(f"✅ Получено: +<b>{amount}</b> фисташек.", reply_markup=shop_menu(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "shop_cases")
async def shop_cases_cb(callback: types.CallbackQuery):
    await callback.message.answer(
        "🎴 <b>СУНДУКИ БОЛЬШЕ НЕ ПРОДАЮТСЯ</b>\n\n"
        "Теперь покупаются только попытки. Одна попытка открывает один сундук и выдаёт полноценного персонажа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🏪 Купить попытки", callback_data="shop_attempts")],
            [button(text="📊 Посмотреть шансы", callback_data="chests")],
            [button(text="⬅️ Магазин", callback_data="shop")],
        ]),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_case_item:"))
async def buy_case_item_cb(callback: types.CallbackQuery):
    # Старые кнопки остаются рабочими, но валюту больше не списывают.
    await callback.message.answer(
        "ℹ️ Кейсы больше не покупаются отдельно. Твой драконит не списан. Купи попытки и используй обычный призыв.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🏪 Купить попытки", callback_data="shop_attempts")],
            [button(text="🎴 Призвать", callback_data="draw_card")],
            [button(text="⬅️ Магазин", callback_data="shop")],
        ]),
    )
    await callback.answer("Покупка кейсов отключена.")


@dp.callback_query(F.data == "shop_battlepass")
async def shop_battlepass_cb(callback: types.CallbackQuery):
    rows=[]
    text="🎟 <b>Боевой пропуск</b>\n\nВыбери усиление сезона. Названия нормальные, без кривого Paid/Paid Plus.\n\n"
    for code,name,cost in BATTLE_PASS_PACKS:
        text += f"• {name} — {cost} 🐉\n"
        rows.append([button(text=f"🐉 {cost} → {name}", callback_data=f"buy_battlepass:{code}:{cost}")])
    rows.append([button(text="🎟 Открыть MultiPass", callback_data="multipass")])
    rows.append([button(text="⬅️ Магазин", callback_data="shop")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_battlepass:"))
async def buy_battlepass_cb(callback: types.CallbackQuery):
    _, code, cost_s = callback.data.split(":")
    cost=int(cost_s)
    p=get_user_data(callback.from_user)
    if int(p.get("moon_coins",0) or 0) < cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает драгонита.", show_alert=True); return
    if not is_owner(callback.from_user.id): p["moon_coins"] = int(p.get("moon_coins",0) or 0)-cost
    p["pass_premium"] = True
    p["pass_premium_cap"] = 100 if code == "full" else (60 if code == "master" else 30)
    p["pass_purchase_request"] = "activated"
    mark_data_dirty("data_changed")
    await callback.message.answer("✅ Боевой пропуск активирован.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🎟 MultiPass", callback_data="multipass")],[button(text="⬅️ Меню", callback_data="menu")]]))
    await callback.answer()


@dp.callback_query(F.data == "premium_info")
async def premium_info_cb(callback: types.CallbackQuery):
    text=("👑 <b>Премиум</b>\n\n"
          "Премиум усиливает удобство: ежедневные наборы, создание клана, смена ника, Mega Open, больше попыток и ускоренный прогресс.\n\n"
          "Сильные карты всё равно требуют коллекции, прокачки и команды — бот не должен превращаться в тупой pay-to-win.")
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="💠 Ранги поддержки", callback_data="privileges")],[button(text="⬅️ Магазин", callback_data="shop")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "privileges")
async def privileges_cb(callback: types.CallbackQuery):
    rows=[]; text="💠 <b>Привилегии</b>\n\nБессрочные уровни поддержки с игровыми бонусами.\n\n"
    for code,pr in PRIVILEGES.items():
        text += f"{pr['icon']} <b>{pr['title']}</b> — {pr['cost']} 🐉\nОжидание карты: {pr['wait_minutes']} мин · шанс редких: {pr['boost']} · старт: +{pr['attempts']} попыток, +{pr['fistiks']} 💎\n\n"
        rows.append([button(text=f"{pr['icon']} {pr['title']} — {pr['cost']} 🐉", callback_data=f"buy_privilege:{code}")])
    rows.append([button(text="⬅️ Магазин", callback_data="shop")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_privilege:"))
async def buy_privilege_cb(callback: types.CallbackQuery):
    code=callback.data.split(":",1)[1]
    pr=PRIVILEGES.get(code)
    if not pr:
        await callback.answer("Привилегия не найдена.", show_alert=True); return
    p=get_user_data(callback.from_user); ensure_rpg_fields(p)
    cost=int(pr['cost'])
    if int(p.get("moon_coins",0) or 0) < cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает драгонита.", show_alert=True); return
    if not is_owner(callback.from_user.id): p["moon_coins"] = int(p.get("moon_coins",0) or 0)-cost
    p["privilege"] = code; p["premium"] = True
    p["card_attempts"] = int(p.get("card_attempts",0) or 0)+int(pr['attempts'])
    p["fistiks"] = int(p.get("fistiks",0) or 0)+int(pr['fistiks'])
    badges=p.setdefault("badges", [])
    badge=pr['title']
    if badge not in badges: badges.append(badge)
    mark_data_dirty("data_changed")
    await callback.message.answer(f"✅ Привилегия активирована: {pr['icon']} <b>{pr['title']}</b>.", reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "exchange")
async def exchange_cb(callback: types.CallbackQuery):
    rows=[]; text="🔁 <b>Обмен</b>\n\nФисташки можно обменять на попытки.\n\n"
    for amount,cost in ATTEMPT_PACKS:
        text += f"• {cost} 💎 → {amount} попыток\n"
        rows.append([button(text=f"💎 {cost} → {amount} попыток", callback_data=f"buy_attempts:f:{amount}:{cost}")])
    rows.append([button(text="⬅️ Магазин", callback_data="shop")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "cases_rewards")
async def cases_rewards_cb(callback: types.CallbackQuery):
    text=("📊 <b>ШАНСЫ ЛЮБОГО СУНДУКА</b>\n\n"
          f"{odds_text(SUMMON_WEIGHTS)}\n\n"
          "Одна попытка выдаёт одного полноценного персонажа. "
          "Дубликат сохраняется как фрагменты усиления. "
          "Absolute выпадает с шансом 2.5%; Super Absolute в обычный пул не входит.")
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Кейсы", callback_data="cases")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "clan")
async def clan_cb(callback: types.CallbackQuery):
    await send_clan_menu(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("clan"))
async def clan_cmd(message: types.Message):
    await send_clan_menu(message, message.from_user)


async def send_clan_menu(message, user):
    p=get_user_data(user); ensure_rpg_fields(p)
    cid=p.get("clan_id","")
    rows=[]
    if not cid or cid not in clan_store():
        text=(f"{CE['clan']} <b>Клан</b>\n\n"
              f"Привет, <b>{e(p.get('name', user.full_name))}</b>.\n"
              "У тебя пока нет клана. Создай свой или вступи в открытый.")
        rows.append([button(text="🛡 Создать клан", callback_data="clan_create")])
        rows.append([button(text="🤝 Присоединиться", callback_data="clan_join_list")])
    else:
        clan=clan_store()[cid]
        members=clan.get("members", [])
        member_names=[]
        for mid in members[:12]:
            member_names.append(DATA.get("users",{}).get(str(mid),{}).get("name", str(mid)))
        text=(f"🏰 <b>{e(clan.get('name','Клан'))}</b>\n\n"
              f"🏆 PTS: <b>{short_number(int(clan.get('points',0) or 0))}</b>\n"
              f"⭐ Уровень: <b>{int(clan.get('level',1) or 1)}</b>\n"
              f"⚔️ Вход от силы: <b>{short_number(int(clan.get('min_power',0) or 0))}</b>\n"
              f"👑 Глава: <b>{e(DATA.get('users',{}).get(str(clan.get('leader','')),{}).get('name','неизвестно'))}</b>\n"
              f"👥 Участники: <code>{e(', '.join(member_names) or 'нет')}</code>")
        rows.append([button(text="👥 Участники", callback_data="clan_members"), button(text="🚪 Покинуть", callback_data="clan_leave_ask")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data == "clan_create")
async def clan_create_cb(callback: types.CallbackQuery):
    p=get_user_data(callback.from_user); ensure_rpg_fields(p)
    if p.get("clan_id") in clan_store():
        await callback.answer("У тебя уже есть клан.", show_alert=True); return
    if not can_create_clan(p, callback.from_user.id):
        await callback.message.answer("🏰 <b>Создание клана</b>\n\nСоздание доступно Premium/VIP/привилегиям. Можно вступить в открытый клан бесплатно.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🔎 Найти клан", callback_data="clan_join_list")],[button(text="⬅️ Клан", callback_data="clan")]]), parse_mode="HTML")
        await callback.answer(); return
    clan=create_default_clan_for_user(callback.from_user, p)
    await callback.message.answer(f"🎉 Клан создан: <b>{e(clan['name'])}</b>\n\nЧтобы задать своё имя, используй: <code>/createclan Название</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🏰 Открыть клан", callback_data="clan")]]), parse_mode="HTML")
    await callback.answer()


@dp.message(Command("createclan"))
async def createclan_cmd(message: types.Message):
    p=get_user_data(message.from_user); ensure_rpg_fields(p)
    if p.get("clan_id") in clan_store():
        await message.answer("У тебя уже есть клан.", reply_markup=back_menu()); return
    if not can_create_clan(p, message.from_user.id):
        await message.answer("Создание клана доступно Premium/VIP/привилегиям.", reply_markup=back_menu()); return
    name=(message.text or '').replace('/createclan','',1).strip()[:32]
    if not name:
        await message.answer("Напиши так: <code>/createclan Название</code>", parse_mode="HTML"); return
    clan=create_default_clan_for_user(message.from_user, p, name)
    await message.answer(f"✅ Клан создан: <b>{e(clan['name'])}</b>", reply_markup=back_menu(), parse_mode="HTML")


@dp.callback_query(F.data == "clan_join_list")
async def clan_join_list_cb(callback: types.CallbackQuery):
    clans=open_public_clans(10)
    rows=[]; text="🔎 <b>Открытые кланы</b>\n\n"
    if not clans:
        # первый бесплатный открытый клан, чтобы новичок не упирался в пустоту
        cid="clan_open_rift"
        clan_store().setdefault(cid, {"id":cid,"name":"Гильдия разлома","leader":"","members":[],"points":0,"level":1,"min_power":0,"open":True,"created_at":utc_now().isoformat()})
        clans=open_public_clans(10)
    for cid,clan in clans:
        text += f"• <b>{e(clan.get('name', cid))}</b> · ур. {clan.get('level',1)} · PTS {clan.get('points',0)}\n"
        rows.append([button(text=f"✅ Вступить: {clan.get('name', cid)[:22]}", callback_data=f"clan_join:{cid}")])
    rows.append([button(text="⬅️ Клан", callback_data="clan")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_join:"))
async def clan_join_cb(callback: types.CallbackQuery):
    cid=callback.data.split(":",1)[1]
    ok,msg=join_clan_by_id(callback.from_user.id, cid)
    await callback.message.answer(("✅ Вы успешно присоединились к клану: " if ok else "⚠️ ") + f"<b>{e(msg)}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🏰 Открыть клан", callback_data="clan")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "clan_leave_ask")
async def clan_leave_ask_cb(callback: types.CallbackQuery):
    await callback.message.answer("🚪 <b>Покинуть клан?</b>\n\nТы действительно хочешь выйти из текущего клана?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="✅ Подтвердить", callback_data="clan_leave_confirm"), button(text="❌ Отмена", callback_data="clan")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "clan_leave_confirm")
async def clan_leave_confirm_cb(callback: types.CallbackQuery):
    p=get_user_data(callback.from_user); ensure_rpg_fields(p)
    cid=p.get("clan_id","")
    if cid in clan_store() and str(callback.from_user.id) in clan_store()[cid].get("members", []):
        clan_store()[cid]["members"].remove(str(callback.from_user.id))
    p["clan_id"]=""
    mark_data_dirty("data_changed")
    await callback.message.answer("✅ Вы успешно покинули клан.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🏰 Найти клан", callback_data="clan")]]))
    await callback.answer()


@dp.callback_query(F.data == "clan_members")
async def clan_members_cb(callback: types.CallbackQuery):
    p=get_user_data(callback.from_user); ensure_rpg_fields(p)
    cid=p.get("clan_id",""); clan=clan_store().get(cid,{})
    lines=[]
    for i,mid in enumerate(clan.get("members",[])[:50],1):
        mp=DATA.get("users",{}).get(str(mid),{})
        lines.append(f"{i}. {e(mp.get('name', mid))} — {short_number(battle_power_label(mp))}")
    await callback.message.answer("👥 <b>Участники клана</b>\n\n" + ("\n".join(lines) if lines else "Пусто."), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Клан", callback_data="clan")]]), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "customize")
async def customize_cb(callback: types.CallbackQuery):
    text=("🎨 <b>Кастомизация профиля</b>\n\n"
          "Выбери стиль ника и профиля. Часть элементов открывается за достижения, вселенные, арену и привилегии.")
    rows=[[button(text="🌌 Значки вселенных", callback_data="custom_universes")],[button(text="🏷 Титулы", callback_data="custom_titles")],[button(text="🖼 Фоны", callback_data="custom_backgrounds")],[button(text="⬅️ Профиль", callback_data="profile")]]
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "custom_titles")
async def custom_titles_cb(callback: types.CallbackQuery):
    p=get_user_data(callback.from_user); ensure_rpg_fields(p)
    text="🏷 <b>Титулы</b>\n\nТекущий: <b>{}</b>\n\n<b>Титулы за вселенные:</b>\n".format(e(player_title(p)))
    for uid,title in list(TITLE_BY_UNIVERSE.items())[:45]:
        text += f"• {e(universe_label(uid))}: <b>{e(title)}</b>\n"
    await send_long(callback.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Кастомизация", callback_data="customize")]]))
    await callback.answer()


@dp.callback_query(F.data.in_({"custom_universes", "custom_backgrounds", "profile_games"}))
async def custom_soon_cb(callback: types.CallbackQuery):
    titles={"custom_universes":"🌌 Значки вселенных", "custom_backgrounds":"🖼 Фоны", "profile_games":"🎮 Игры"}
    await callback.message.answer(f"{titles.get(callback.data,'Раздел')}\n\nЭтот раздел уже подготовлен под будущие награды и стили. Сейчас основной функционал: профиль, вселенная, кланы, попытки и коллекция.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Профиль", callback_data="profile")]]))
    await callback.answer()


@dp.callback_query(F.data == "referral")
async def referral_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    _refresh_ref_today(p)
    bot_name = (await bot.get_me()).username
    link = f"https://t.me/{bot_name}?start=ref_{callback.from_user.id}"
    share_text = "Заходи в Anime Battle Multiverse — соберём союз и получим награды!"
    share_url = f"https://t.me/share/url?url={quote(link, safe='')}&text={quote(share_text, safe='')}"
    count = int(p.get("ref_count", 0) or 0)
    next_milestone = next((m for m in sorted(REF_MILESTONES) if count < m), None)
    milestone_line = f"Следующая веха: <b>{next_milestone}</b> друзей." if next_milestone else "Все текущие реферальные вехи выполнены."
    text = (
        f"{CE['referral']} <b>РЕФЕРАЛЬНЫЙ СОЮЗ</b>\n\n"
        "За каждого нового игрока, который впервые запускает бота по твоей ссылке:\n"
        "• тебе — <b>+500 💎 и +3 попытки</b>;\n"
        "• другу — <b>+300 💎 и +1 попытка</b>.\n\n"
        f"<blockquote>Сегодня: <b>{int(p.get('ref_today', 0) or 0)}</b>\n"
        f"Всего приглашено: <b>{count}</b>\n"
        f"Заработано попыток: <b>{int(p.get('ref_earned', 0) or 0)}</b>\n"
        f"{milestone_line}</blockquote>\n"
        f"Твоя ссылка:\n<code>{e(link)}</code>"
    )
    rows = [
        [button(text="📨 Поделиться ссылкой", url=share_url)],
        [button(text="🎁 Забрать вехи", callback_data="ref_claim"), button(text="👥 Друзья", callback_data="friends")],
        [button(text="➕ Добавить бота в группу", url=f"https://t.me/{bot_name}?startgroup=true")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "cases")
async def cases(callback: types.CallbackQuery):
    await send_chests(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("case_open:"))
async def case_open(callback: types.CallbackQuery):
    # Compatibility alias for old case buttons. No dragonite or case item is
    # spent; the normal one-attempt summon is used instead.
    await draw_card_to_message(callback.message, callback.from_user, callback)


PASS_FREE_REWARDS = {
    1: {"fistiks": 100},
    3: {"fistiks": 250},
    5: {"pack": "basic"},
    8: {"fistiks": 500},
    10: {"fragments": 50},
    15: {"pack": "rare"},
    20: {"fistiks": 1200},
}

PASS_PREMIUM_REWARDS = {
    1: {"badge": "PREMIUM"},
    3: {"fistiks": 700},
    5: {"pack": "rare", "moon_coins": 1},
    10: {"fragments": 250, "moon_coins": 1},
    15: {"pack": "royal", "moon_coins": 2},
    20: {"fistiks": 3000, "moon_coins": 2},
    50: {"fistiks": 9000, "moon_coins": 5},
    100: {"fistiks": 25000, "moon_coins": 10},
}


NEWBIE_DAYS = 10
NEWBIE_TASKS = {
    "daily": {"title": "Забрать ежедневную награду", "target": 1, "reward": {"fistiks": 350, "pass_xp": 120}},
    "free_pack": {"title": "Сделать бесплатный призыв", "target": 1, "reward": {"fistiks": 300, "pass_xp": 100}},
    "chest": {"title": "Открыть любой сундук", "target": 1, "reward": {"fistiks": 450, "pass_xp": 130}},
    "battle": {"title": "Сыграть бой с ботом или игроком", "target": 1, "reward": {"fistiks": 600, "pass_xp": 170}},
    "craft": {"title": "Сделать 1 крафт", "target": 1, "reward": {"fistiks": 500, "pass_xp": 150}},
    "referral": {"title": "Привести 1 друга по ссылке", "target": 1, "reward": {"fistiks": 1200, "pass_xp": 250, "moon_coins": 2}},
}


def is_newbie_active(uid):
    if is_owner(uid):
        return False
    player = DATA.get("users", {}).get(str(uid), {})
    created = player.get("created_at") or utc_now().isoformat()
    try:
        created_dt = _parse_iso_datetime(created)
        return bool(created_dt and utc_now() <= created_dt + timedelta(days=NEWBIE_DAYS))
    except Exception:
        return True


def grant_newbie_task_reward(player, key):
    task = NEWBIE_TASKS.get(key)
    if not task:
        return ""
    claimed = player.setdefault("newbie_claimed", [])
    if key in claimed:
        return ""
    reward = task["reward"]
    player["fistiks"] = int(player.get("fistiks", 0)) + int(reward.get("fistiks", 0))
    player["pass_xp"] = int(player.get("pass_xp", 0)) + int(reward.get("pass_xp", 0))
    player["moon_coins"] = int(player.get("moon_coins", 0)) + int(reward.get("moon_coins", 0))
    claimed.append(key)
    moon_part = f" +{reward.get('moon_coins', 0)} 🐉" if reward.get("moon_coins") else ""
    line = f"✅ Новичковое задание выполнено: {task['title']} → +{reward.get('fistiks', 0)} 💎 +{reward.get('pass_xp', 0)} pass{moon_part}"
    player.setdefault("system_inbox", []).append({"at": utc_now().isoformat(timespec="seconds"), "text": line})
    if len(player.get("system_inbox", [])) > 20:
        del player["system_inbox"][:-20]
    return line


def add_newbie_task_progress(player, key, amount=1, auto_reward=True):
    if key not in NEWBIE_TASKS:
        return ""
    created = player.get("created_at") or utc_now().isoformat()
    try:
        created_dt = _parse_iso_datetime(created)
        if created_dt and utc_now() > created_dt + timedelta(days=NEWBIE_DAYS):
            return ""
    except Exception:
        pass
    progress = player.setdefault("newbie_progress", {})
    target = int(NEWBIE_TASKS[key]["target"])
    before = int(progress.get(key, 0))
    after = min(target, before + int(amount))
    progress[key] = after
    if before < target and after >= target:
        # Referral rewards themselves have an exact public economy (+500/+3). The separate
        # newbie-task prize is left claimable instead of being silently bundled into that delta.
        return grant_newbie_task_reward(player, key) if auto_reward else "ready"
    return ""


def format_newbie_tasks(player):
    progress = player.setdefault("newbie_progress", {})
    claimed = set(player.setdefault("newbie_claimed", []))
    lines = []
    for key, task in NEWBIE_TASKS.items():
        done = min(int(progress.get(key, 0)), int(task["target"]))
        mark = "✅" if key in claimed else ("🎯" if done >= task["target"] else "▫️")
        reward = task["reward"]
        moon_part = f" + {reward.get('moon_coins', 0)} 🐉" if reward.get("moon_coins") else ""
        lines.append(f"{mark} {task['title']}: {done}/{task['target']} → {reward.get('fistiks', 0)} 💎 + {reward.get('pass_xp', 0)} очков pass{moon_part}")
    return "\n".join(lines)


async def send_newbie_start(message, user):
    p = get_user_data(user)
    if not is_newbie_active(user.id):
        await message.answer("🚀 Раздел новичка уже закрыт: он действует только первые 10 дней после первого входа.", reply_markup=back_menu())
        return
    created = p.get("created_at") or utc_now().isoformat()
    try:
        created_dt = _parse_iso_datetime(created)
        if created_dt is None:
            raise ValueError("invalid created_at")
        expires = created_dt + timedelta(days=NEWBIE_DAYS)
        left = expires - utc_now()
        left_text = f"ещё примерно {max(0, left.days)} дн. {max(0, left.seconds // 3600)} ч."
    except Exception:
        left_text = "первые 10 дней"
    text = (
        f"{ui_box(*SECTION_HINTS['newbie'])}\n\n"
        f"Доступно временно: <b>{e(left_text)}</b>\n\n"
        "Задания засчитываются автоматически. Выполнил действие — награда сразу падает на аккаунт.\n\n"
        f"{format_newbie_tasks(p)}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🔥 Путь Луфи 10 дней", callback_data="luffy_path")],
        [button(text="🎁 Забрать выполненные", callback_data="newbie_claim")],
        [button(text="🎁 Награда", callback_data="daily"), button(text="📦 Сундуки", callback_data="chests")],
        [button(text="⚔️ Бой с ботом", callback_data="battle:start"), button(text="⚒️ Крафт", callback_data="craft")],
        [button(text="🔗 Реферальная ссылка", callback_data="friend_link")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data == "newbie_start")
async def newbie_start_cb(callback: types.CallbackQuery):
    await send_newbie_start(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "newbie_claim")
async def newbie_claim_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    if not is_newbie_active(callback.from_user.id):
        await callback.answer("Старт новичка уже закрыт.", show_alert=True)
        return
    progress = p.setdefault("newbie_progress", {})
    claimed = set(p.setdefault("newbie_claimed", []))
    lines = []
    for key, task in NEWBIE_TASKS.items():
        if key in claimed:
            continue
        if int(progress.get(key, 0)) >= int(task["target"]):
            reward = task["reward"]
            p["fistiks"] = p.get("fistiks", 0) + int(reward.get("fistiks", 0))
            p["pass_xp"] = int(p.get("pass_xp", 0)) + int(reward.get("pass_xp", 0))
            p["moon_coins"] = int(p.get("moon_coins", 0)) + int(reward.get("moon_coins", 0))
            p["newbie_claimed"].append(key)
            moon_part = f" +{reward.get('moon_coins', 0)} 🐉" if reward.get("moon_coins") else ""
            lines.append(f"✅ {e(task['title'])}: +{reward.get('fistiks', 0)} 💎 +{reward.get('pass_xp', 0)} очков pass{moon_part}")
    if not lines:
        await callback.answer("Пока нет выполненных новичковых заданий.", show_alert=True)
        return
    mark_data_dirty("data_changed")
    await callback.message.answer("🚀 <b>Новичковые награды получены</b>\n\n" + "\n".join(lines), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


def format_luffy_path(player):
    repair_luffy_progress(player)
    day = int(player.get("luffy_day", 0))
    last = player.get("last_luffy_claim", "")
    lines = []
    for i, cid in enumerate(LUFFY_PATH_CARDS, 1):
        c = CARD_BY_ID.get(cid)
        if not c:
            continue
        if i <= day:
            mark = "✅"
        elif i == day + 1:
            mark = "🎯"
        else:
            mark = "▫️"
        lines.append(f"{mark} День {i}: {rarity_label_for_card(c)} {e(c['name'])} — {e(c.get('form',''))}")
    ready = last != app_now().date().isoformat() and day < len(LUFFY_PATH_CARDS)
    return "\n".join(lines), ready


async def send_luffy_path(message, user):
    p = get_user_data(user)
    p["luffy_intro_seen"] = True
    repair_luffy_progress(p)
    mark_data_dirty("data_changed")
    lines, ready = format_luffy_path(p)
    day = int(p.get("luffy_day", 0))
    text = (
        f"{CE['luffy']} <b>Путь Монки Д. Луфи: 10 дней</b>\n\n"
        "Заходи каждый день и делай хотя бы одно простое действие: daily, сундук или бой. "
        "Каждый день открывает новую форму Луфи — от обычной до мифической.\n\n"
        f"Прогресс: <b>{day}/10</b>\n"
        f"{lines}\n\n"
        + ("🎁 Сегодняшняя форма доступна." if ready else "⏳ Сегодня уже забрано или цепочка завершена.")
    )
    rows = []
    if ready:
        rows.append([button(text="🎁 Забрать форму дня", callback_data="luffy_claim")])
    rows.append([button(text="🎁 Daily", callback_data="daily"), button(text="🆓 Сундук", callback_data="pack_info:free")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data == "luffy_path")
async def luffy_path_cb(callback: types.CallbackQuery):
    await send_luffy_path(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "luffy_claim")
async def luffy_claim_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    repair_luffy_progress(p)
    day = int(p.get("luffy_day", 0))
    if day >= len(LUFFY_PATH_CARDS):
        await callback.answer("Путь Луфи уже завершён.", show_alert=True)
        return
    if p.get("last_luffy_claim") == app_now().date().isoformat():
        await callback.answer("Сегодняшняя форма уже забрана.", show_alert=True)
        return
    cid = LUFFY_PATH_CARDS[day]
    if cid not in CARD_BY_ID:
        await callback.answer("Карта дня не найдена.", show_alert=True)
        return
    p["last_luffy_claim"] = app_now().date().isoformat()
    p["luffy_day"] = day + 1
    if p["luffy_day"] >= len(LUFFY_PATH_CARDS):
        p["luffy_finished"] = True
    p.setdefault("luffy_claimed_cards", []).append(cid)
    result = add_card(p, cid, 50)
    p["fistiks"] = int(p.get("fistiks", 0)) + 150
    p["pass_xp"] = int(p.get("pass_xp", 0)) + 80
    mark_data_dirty("data_changed")
    c = CARD_BY_ID[cid]
    await callback.message.answer(
        f"{CE['luffy']} <b>Путь Луфи — день {day + 1}/10</b>\n\n"
        f"{rarity_label_for_card(c)} <b>{e(c['name'])}</b>\n"
        f"Форма: <b>{e(c.get('form',''))}</b>\n"
        f"{e(result)}\n"
        "+150 💎 и +80 очков pass.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🔥 Путь Луфи", callback_data="luffy_path")],
            [button(text="⬅️ Меню", callback_data="menu")],
        ]),
        parse_mode="HTML"
    )
    await callback.answer()



PASS_DAILY_TASKS = {
    "daily": {"title": "Забрать ежедневную награду", "target": 1, "pass_xp": 120},
    "chest": {"title": "Открыть любой сундук", "target": 1, "pass_xp": 120},
    "battle": {"title": "Сыграть 1 бой", "target": 1, "pass_xp": 160},
    "win": {"title": "Победить 1 раз", "target": 1, "pass_xp": 220},
    "complete_all": {"title": "Выполнить все задания дня", "target": 1, "pass_xp": 400},
}


def pass_level_from_xp(xp):
    return max(1, min(100, int(xp or 0) // 250 + 1))


def ensure_pass_daily(player):
    today = app_now().date().isoformat()
    if player.get("pass_daily_date") != today:
        player["pass_daily_date"] = today
        player["pass_task_progress"] = {}
        player["pass_task_claimed"] = []
    player.setdefault("pass_task_progress", {})
    player.setdefault("pass_task_claimed", [])


def add_pass_task_progress(player, key, amount=1):
    ensure_pass_daily(player)
    if key not in PASS_DAILY_TASKS or key == "complete_all":
        return
    progress = player.setdefault("pass_task_progress", {})
    target = int(PASS_DAILY_TASKS[key]["target"])
    progress[key] = min(target, int(progress.get(key, 0)) + int(amount))


def format_pass_tasks(player):
    ensure_pass_daily(player)
    progress = player.get("pass_task_progress", {})
    claimed = set(player.get("pass_task_claimed", []))
    core_keys = [k for k in PASS_DAILY_TASKS if k != "complete_all"]
    all_done = all(int(progress.get(k, 0)) >= int(PASS_DAILY_TASKS[k]["target"]) for k in core_keys)
    lines = []
    for key, task in PASS_DAILY_TASKS.items():
        if key == "complete_all":
            done = 1 if all_done else 0
        else:
            done = min(int(progress.get(key, 0)), int(task["target"]))
        mark = "✅" if key in claimed else ("🎯" if done >= task["target"] else "▫️")
        lines.append(f"{mark} {task['title']}: {done}/{task['target']} → +{task['pass_xp']} очков Боевого пропуска")
    return "\n".join(lines)


def format_pass_rewards(rewards, claimed):
    lines = []
    for lvl, reward in rewards.items():
        mark = "✅" if str(lvl) in claimed else "🎁"
        parts = []
        if "fistiks" in reward:
            parts.append(f"{reward['fistiks']} 💎")
        if "pack" in reward:
            parts.append(SHOP_PACKS.get(reward["pack"], {}).get("name", reward["pack"]))
        if "fragments" in reward:
            parts.append(f"{reward['fragments']} фрагментов")
        if "moon_coins" in reward:
            parts.append(f"{reward['moon_coins']} 🐉")
        if "badge" in reward:
            parts.append(f"знак {badge_title(reward['badge'])}")
        lines.append(f"{mark} {lvl} ур. — " + ", ".join(parts))
    return "\n".join(lines)


def grant_pass_reward(player, reward):
    text = []
    if "fistiks" in reward:
        player["fistiks"] = player.get("fistiks", 0) + int(reward["fistiks"])
        text.append(f"+{reward['fistiks']} 💎")
    if "badge" in reward:
        player.setdefault("badges", [])
        if reward["badge"] not in player["badges"]:
            player["badges"].append(reward["badge"])
        text.append(f"знак {badge_title(reward['badge'])}")
    if "moon_coins" in reward:
        player["moon_coins"] = int(player.get("moon_coins", 0)) + int(reward["moon_coins"])
        text.append(f"+{reward['moon_coins']} 🐉")
    if "fragments" in reward:
        amount = int(reward["fragments"])
        card = roll_card(weights={"Обычный": 500, "Редкий": 300, "Эпический": 160, "Мифический": 35, "Легендарный": 5}, universe_id=selected_universe_id(player))
        text.append(add_fragments(player, card["id"], amount))
    if "pack" in reward:
        pack = SHOP_PACKS.get(reward["pack"])
        if pack:
            pulled = set()
            for _ in range(pack["count"]):
                card = roll_card(weights=pack["weights"], exclude=pulled, universe_id=selected_universe_id(player))
                pulled.add(card["id"])
                text.append(add_card(player, card["id"]))
    return "\n".join(text) if text else "Награда выдана."


async def send_multipass(message, user):
    p = get_user_data(user)
    ensure_pass_daily(p)
    if is_owner(user.id):
        p["pass_premium"] = True
        p["pass_premium_cap"] = 100
        p["pass_xp"] = max(int(p.get("pass_xp", 0)), 25000)
    pass_level = pass_level_from_xp(p.get("pass_xp", 0))
    cap = int(p.get("pass_premium_cap", 0) or 0)
    if p.get("pass_premium"):
        premium = f"Premium до {cap if cap else 20} уровня · срок: {pass_until_label(p)}"
    elif p.get("pass_purchase_request") == "paid_pending":
        premium = "старая оплата ждёт ручной проверки"
    else:
        premium = "Free"
    request_state = p.get("pass_purchase_request", "")
    request_text = {
        "": "нет",
        "paid_pending": "старая оплата ждёт ручной проверки",
        "activated": "активирован",
        "rejected_after_payment": "оплачено, но отклонено/заморожено",
        "paid": "оплачено",
    }.get(request_state, request_state or "нет")
    progress = min(100, int((int(p.get("pass_xp", 0)) / max(1, 25000)) * 100))
    text = (
        "🎟 <b>MultiPass</b>\n\n"
        f"Игрок: <b>{e(p.get('name', user.full_name))}</b>\n"
        f"Тип пропуска: <b>{premium}</b>\n"
        f"Уровень: <b>{pass_level}/100</b>\n"
        f"Очки: <b>{short_number(p.get('pass_xp', 0))}</b>\n"
        f"Прогресс MultiPass: <b>{progress}%</b>\n"
        "28-дневный сезон и MultiPass: <b>разные системы</b>\n"
        f"Статус оплаты: <b>{request_text}</b>\n\n"
        f"Premium стоит <b>{PASS_PRICE_STARS}</b> Telegram Stars. Корректный Stars-платёж активирует доступ автоматически; ручная проверка нужна только для старых или аномальных заявок."
    )
    rows = [
        [button(text="🎯 Задания", callback_data="pass_tasks")],
        [button(text="🎁 Бесплатные награды", callback_data="pass_claim:free"), button(text="👑 Premium награды", callback_data="pass_claim:premium")],
        [button(text="💳 Купить уровень", callback_data="buy_pass_level"), button(text="⭐ Stars-наборы", callback_data="stars_shop")],
    ]
    if not p.get("pass_premium") and p.get("pass_purchase_request") != "paid_pending" and not is_owner(user.id):
        rows.append([button(text=f"⭐ Купить Premium за {PASS_PRICE_STARS} Stars", callback_data="buy_pass_stars")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")



@dp.callback_query(F.data == "buy_pass_level")
async def buy_pass_level_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    cost_dragonit = 8
    gain = 300
    if not is_owner(callback.from_user.id) and int(p.get("moon_coins", 0) or 0) < cost_dragonit:
        await callback.answer(f"Нужно {cost_dragonit} 🐉 Драконита.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["moon_coins"] = int(p.get("moon_coins", 0) or 0) - cost_dragonit
    p["pass_xp"] = int(p.get("pass_xp", 0) or 0) + gain
    mark_data_dirty("data_changed")
    await callback.message.answer(
        f"💳 <b>Уровень pass куплен</b>\n\n+{gain} очков мультипасса за {cost_dragonit} 🐉.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ MultiPass", callback_data="multipass")]]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "multipass")
async def multipass_cb(callback: types.CallbackQuery):
    await send_multipass(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("pass"))
async def multipass_cmd(message: types.Message):
    await send_multipass(message, message.from_user)



async def send_pass_tasks(message, user):
    p = get_user_data(user)
    ensure_pass_daily(p)
    text = (
        "🎯 <b>Задания дня</b>\n\n"
        "Выполняй их через обычные действия бота. После выполнения нажми кнопку ниже и забери очки pass.\n\n"
        f"{format_pass_tasks(p)}"
    )
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [button(text="✅ Забрать очки", callback_data="pass_claim_tasks")],
        [button(text="⬅️ MultiPass", callback_data="multipass")],
    ]), parse_mode="HTML")


@dp.callback_query(F.data == "pass_tasks")
async def pass_tasks_cb(callback: types.CallbackQuery):
    await send_pass_tasks(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "pass_claim_tasks")
async def pass_claim_tasks(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    ensure_pass_daily(p)
    progress = p.get("pass_task_progress", {})
    claimed = set(p.setdefault("pass_task_claimed", []))
    core_keys = [k for k in PASS_DAILY_TASKS if k != "complete_all"]
    all_done = all(int(progress.get(k, 0)) >= int(PASS_DAILY_TASKS[k]["target"]) for k in core_keys)
    total = 0
    lines = []
    for key, task in PASS_DAILY_TASKS.items():
        if key in claimed:
            continue
        ready = all_done if key == "complete_all" else int(progress.get(key, 0)) >= int(task["target"])
        if ready:
            total += int(task["pass_xp"])
            p["pass_task_claimed"].append(key)
            lines.append(f"✅ {e(task['title'])}: +{task['pass_xp']} очков Боевого пропуска")
    if total <= 0:
        await callback.answer("Пока нет выполненных заданий.", show_alert=True)
        return
    p["pass_xp"] = int(p.get("pass_xp", 0)) + total
    mark_data_dirty("data_changed")
    await callback.message.answer("🎯 <b>Очки Боевого пропуска получены</b>\n\n" + "\n".join(lines), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("pass_claim:"))
async def pass_claim(callback: types.CallbackQuery):
    line = callback.data.split(":", 1)[1]
    p = get_user_data(callback.from_user)
    current_level = pass_level_from_xp(p.get("pass_xp", 0))
    if line == "premium":
        if not (p.get("pass_premium") or is_owner(callback.from_user.id)):
            await callback.answer("Премиум-линия ещё не подтверждена создателем.", show_alert=True)
            return
        cap = int(p.get("pass_premium_cap", 20) or 20)
        current_level = min(current_level, cap)

    rewards = PASS_FREE_REWARDS if line == "free" else PASS_PREMIUM_REWARDS
    key = "claimed_pass_free" if line == "free" else "claimed_pass_premium"
    claimed = set(map(str, p.setdefault(key, [])))
    granted = []
    for lvl, reward in rewards.items():
        if lvl <= current_level and str(lvl) not in claimed:
            granted.append(f"<b>{lvl} ур.</b>: {e(grant_pass_reward(p, reward))}")
            p[key].append(str(lvl))

    if not granted:
        await callback.answer("Нет доступных наград для забора.", show_alert=True)
        return

    mark_data_dirty("data_changed")
    await callback.message.answer("🎁 <b>Награды мультипасса получены</b>\n\n" + "\n".join(granted), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "buy_pass_stars")
async def buy_pass_stars(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    if is_owner(callback.from_user.id):
        p["pass_premium"] = True
        p["pass_premium_cap"] = 100
        p["pass_xp"] = max(int(p.get("pass_xp", 0)), 25000)
        mark_data_dirty("data_changed")
        await callback.answer("У владельца премиум уже открыт.", show_alert=True)
        return
    if p.get("pass_premium"):
        await callback.answer("Премиум уже активен.", show_alert=True)
        return
    if p.get("pass_purchase_request") == "paid_pending":
        await callback.answer("Оплата уже получена и ждёт обработки старой заявки. Напиши /paysupport.", show_alert=True)
        return
    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title="Премиум мультипасс",
            description="Премиум MultiPass за Telegram Stars. Доступ открывается автоматически после подтверждённой оплаты Telegram.",
            payload=f"multipass_premium:{callback.from_user.id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Премиум мультипасс", amount=PASS_PRICE_STARS)],
        )
        await callback.message.answer(
            "⭐ Счёт отправлен. После подтверждённой оплаты Premium откроется автоматически.",
            reply_markup=back_menu(),
            parse_mode="HTML"
        )
    except Exception as ex:
        await callback.message.answer(f"⚠️ Не удалось отправить счёт: {e(ex)}", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data.startswith("pass_paid:"))
async def pass_paid_action(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Только создатель может подтверждать оплату.", show_alert=True)
        return
    try:
        _, action, target_uid, level_s = callback.data.split(":")
        level_cap = int(level_s)
    except Exception:
        await callback.answer("Ошибка подтверждения.", show_alert=True)
        return
    if target_uid not in DATA.get("users", {}):
        await callback.answer("Игрок не найден.", show_alert=True)
        return

    p = DATA["users"][target_uid]
    if action == "reject":
        p["pass_purchase_request"] = "rejected_after_payment"
        mark_data_dirty("data_changed")
        try:
            await bot.send_message(int(target_uid), "⚠️ Оплата мультипасса заморожена/отклонена создателем. Напиши владельцу для ручного решения.", reply_markup=back_menu())
        except Exception:
            pass
        await callback.message.answer("⚠️ Оплата помечена как отклонённая/замороженная.")
        await callback.answer()
        return

    if action == "approve":
        p["pass_premium"] = True
        p["pass_premium_cap"] = max(1, min(100, level_cap))
        p["pass_purchase_request"] = "activated"
        mark_data_dirty("data_changed")
        try:
            await bot.send_message(
                int(target_uid),
                f"👑 Премиум мультипасс подтверждён создателем. Доступ открыт до <b>{p['pass_premium_cap']}</b> уровня.",
                reply_markup=back_menu(),
                parse_mode="HTML"
            )
        except Exception:
            pass
        await callback.message.answer(f"✅ Премиум игрока активирован до {p['pass_premium_cap']} уровня.")
        await callback.answer()
        return



_PAYMENT_PROCESS_LOCK = None
PAYMENT_LEDGER_TABLE = "anime_battle_payment_ledger"
PAYMENT_RECOVERY_INTERVAL_SECONDS = max(30, int(os.getenv("ABM_PAYMENT_RECOVERY_INTERVAL_SECONDS", "60") or 60))


def _get_payment_process_lock():
    global _PAYMENT_PROCESS_LOCK
    if _PAYMENT_PROCESS_LOCK is None:
        _PAYMENT_PROCESS_LOCK = asyncio.Lock()
    return _PAYMENT_PROCESS_LOCK


def _read_local_payment_queue_sync():
    path = Path(PAYMENT_RECOVERY_FILE)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as ex:
        logger.exception("Cannot read payment recovery queue: %s", ex)
        return []


def _write_local_payment_queue_sync(items):
    path = Path(PAYMENT_RECOVERY_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    payload = json.dumps(items, ensure_ascii=False, indent=2)
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except Exception:
            pass
    os.replace(tmp, path)


def _queue_local_payment_event_sync(event):
    charge_id = str(event.get("charge_id", "") or "")
    if not charge_id:
        return False
    items = _read_local_payment_queue_sync()
    for item in items:
        if str(item.get("charge_id", "")) == charge_id:
            return True
    items.append(copy.deepcopy(event))
    _write_local_payment_queue_sync(items)
    return True


def _remove_local_payment_event_sync(charge_id):
    charge_id = str(charge_id or "")
    items = _read_local_payment_queue_sync()
    new_items = [item for item in items if str(item.get("charge_id", "")) != charge_id]
    if len(new_items) != len(items):
        _write_local_payment_queue_sync(new_items)
    return True


def _ensure_payment_ledger_table_sync():
    if not _postgres_available():
        return not bool(DATABASE_URL)
    con = None
    try:
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PAYMENT_LEDGER_TABLE} (
                    charge_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    amount BIGINT NOT NULL,
                    currency TEXT NOT NULL,
                    status TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        con.commit()
        return True
    except Exception as ex:
        logger.exception("Payment ledger table init failed: %s", ex)
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _reserve_payment_event_postgres_sync(event):
    """Durably reserve a Telegram charge id. Returns (ok, inserted, row)."""
    if not _postgres_available():
        return False, False, None
    con = None
    try:
        charge_id = str(event["charge_id"])
        payload = json.dumps(event, ensure_ascii=False)
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PAYMENT_LEDGER_TABLE} (
                    charge_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    amount BIGINT NOT NULL,
                    currency TEXT NOT NULL,
                    status TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                f"""
                INSERT INTO {PAYMENT_LEDGER_TABLE}
                    (charge_id, user_id, payload, amount, currency, status, event_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'received', %s, NOW())
                ON CONFLICT (charge_id) DO NOTHING
                RETURNING charge_id
                """,
                (
                    charge_id,
                    str(event["user_id"]),
                    str(event["payload"]),
                    int(event["amount"]),
                    str(event["currency"]),
                    payload,
                ),
            )
            inserted = cur.fetchone() is not None
            cur.execute(
                f"SELECT charge_id, user_id, payload, amount, currency, status, event_json FROM {PAYMENT_LEDGER_TABLE} WHERE charge_id=%s",
                (charge_id,),
            )
            row = cur.fetchone()
        con.commit()
        if not row:
            return False, inserted, None
        record = {
            "charge_id": row[0], "user_id": row[1], "payload": row[2], "amount": int(row[3]),
            "currency": row[4], "status": row[5], "event_json": row[6],
        }
        return True, inserted, record
    except Exception as ex:
        logger.exception("Payment reservation failed: %s", ex)
        return False, False, None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _mark_payment_completed_postgres_sync(charge_id):
    if not _postgres_available():
        return not bool(DATABASE_URL)
    con = None
    try:
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute(
                f"UPDATE {PAYMENT_LEDGER_TABLE} SET status='completed', updated_at=NOW() WHERE charge_id=%s",
                (str(charge_id),),
            )
        con.commit()
        return True
    except Exception as ex:
        logger.exception("Payment ledger completion failed for %s: %s", charge_id, ex)
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _load_pending_payment_events_postgres_sync(limit=500):
    if not _postgres_available():
        return []
    con = None
    try:
        con = psycopg.connect(DATABASE_URL, connect_timeout=12)
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT charge_id, event_json FROM {PAYMENT_LEDGER_TABLE}
                WHERE status <> 'completed'
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cur.fetchall()
        result = []
        for charge_id, raw in rows:
            try:
                event = json.loads(raw)
                if isinstance(event, dict):
                    event["charge_id"] = str(charge_id)
                    result.append(event)
            except Exception:
                logger.error("Invalid payment ledger event_json for charge=%s", charge_id)
        return result
    except Exception as ex:
        logger.exception("Pending payment ledger load failed: %s", ex)
        return []
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _payment_event_matches_record(event, record):
    if not record:
        return True
    return (
        str(record.get("user_id")) == str(event.get("user_id"))
        and str(record.get("payload")) == str(event.get("payload"))
        and int(record.get("amount", -1)) == int(event.get("amount", -2))
        and str(record.get("currency")) == str(event.get("currency"))
    )


def global_payment_entry(charge_id):
    return DATA.setdefault("payment_ledger", {}).get(str(charge_id or ""))


def _payment_entry_is_awarded(entry):
    return isinstance(entry, dict) and str(entry.get("status", "")) in {"awarded_pending_commit", "completed"}


def _payment_entry_is_persisted(entry):
    if not _payment_entry_is_awarded(entry):
        return False
    if str(entry.get("status", "")) == "completed":
        return True
    try:
        revision = int(entry.get("awarded_revision", -1))
    except Exception:
        revision = -1
    # Legacy/root entries loaded from authoritative storage may not have revision metadata.
    if revision < 0:
        return bool(entry.get("legacy_migrated"))
    return revision <= int(_DATA_LAST_SAVED_REVISION)


def _build_payment_event(message, payment, parsed, charge_id):
    return {
        "charge_id": str(charge_id),
        "user_id": str(message.from_user.id),
        "payload": str(payment.invoice_payload),
        "amount": int(payment.total_amount),
        "currency": str(payment.currency),
        "kind": str(parsed["kind"]),
        "code": str(parsed["code"]),
        "received_at": utc_now().isoformat(),
    }


def _apply_payment_event_reward(event):
    charge_id = str(event.get("charge_id", "") or "")
    uid = str(event.get("user_id", "") or "")
    if not charge_id or not uid:
        return False, "Некорректное платёжное событие."
    existing = global_payment_entry(charge_id)
    if _payment_entry_is_awarded(existing):
        return True, "Этот платёж уже обработан."
    if is_permanently_deleted_id(uid):
        return False, "Аккаунт помечен как permanently deleted; требуется решение владельца."
    player = DATA.get("users", {}).get(uid)
    if not isinstance(player, dict):
        return False, "Игрок отсутствует в текущей базе; требуется ручная проверка."
    parsed = parse_payment_payload(event.get("payload"))
    expected = expected_payment_amount(event.get("payload"))
    if (
        not parsed or expected is None
        or str(parsed.get("user_id")) != uid
        or str(event.get("currency")) != PAYMENT_CURRENCY
        or int(event.get("amount", -1)) != int(expected)
    ):
        return False, "Платёжное событие не прошло повторную валидацию."

    reward_text = ""
    if parsed["kind"] == "multipass":
        player["pass_premium"] = True
        player["pass_premium_cap"] = 100
        player["pass_purchase_request"] = "activated"
        reward_text = "Premium MultiPass открыт до 100 уровня текущей шкалы наград."
    elif parsed["kind"] == "star_pack":
        if parsed["code"] not in STAR_PACKS:
            return False, "Stars-набор больше не найден в конфигурации."
        reward_text = grant_star_pack_reward(player, parsed["code"])
    else:
        return False, "Неизвестный тип покупки."

    record_payment(player, charge_id, parsed["kind"], parsed["code"], int(event["amount"]))
    DATA.setdefault("payment_ledger", {})[charge_id] = {
        "status": "awarded_pending_commit",
        "user_id": uid,
        "payload": str(event["payload"]),
        "amount": int(event["amount"]),
        "currency": str(event["currency"]),
        "kind": str(parsed["kind"]),
        "code": str(parsed["code"]),
        "awarded_at": utc_now().isoformat(),
    }
    DATA.setdefault("payment_recovery_queue", {})[charge_id] = copy.deepcopy(event)
    mark_data_dirty("payment_awarded_pending_commit")
    DATA["payment_ledger"][charge_id]["awarded_revision"] = int(_DATA_REVISION)
    return True, reward_text


async def _finalize_payment_commit(event, reason):
    charge_id = str(event["charge_id"])
    saved = await flush_data_now_async(reason)
    if not saved:
        await asyncio.to_thread(_queue_local_payment_event_sync, event)
        return False
    # The first successful DATA save already contains the award and global charge ledger.
    pg_ok = True
    if DATABASE_URL:
        pg_ok = await asyncio.to_thread(_mark_payment_completed_postgres_sync, charge_id)
    entry = global_payment_entry(charge_id)
    if isinstance(entry, dict):
        entry["status"] = "completed" if pg_ok else "awarded_pending_commit"
        entry["committed_at"] = utc_now().isoformat()
    DATA.setdefault("payment_recovery_queue", {}).pop(charge_id, None)
    mark_data_dirty("payment_commit_status")
    if pg_ok:
        await asyncio.to_thread(_remove_local_payment_event_sync, charge_id)
    return bool(pg_ok)


async def recover_pending_payments():
    """Replay durable payment events before polling and during runtime, exactly once."""
    events = {}
    if DATABASE_URL and _postgres_available():
        for event in await asyncio.to_thread(_load_pending_payment_events_postgres_sync):
            if isinstance(event, dict) and event.get("charge_id"):
                events[str(event["charge_id"])] = event
    for event in await asyncio.to_thread(_read_local_payment_queue_sync):
        if isinstance(event, dict) and event.get("charge_id"):
            events.setdefault(str(event["charge_id"]), event)
    for charge_id, event in list((DATA.get("payment_recovery_queue", {}) or {}).items()):
        if isinstance(event, dict):
            event = copy.deepcopy(event)
            event["charge_id"] = str(charge_id)
            events.setdefault(str(charge_id), event)

    if not events:
        return 0
    recovered = 0
    async with _get_payment_process_lock():
        for charge_id, event in events.items():
            try:
                entry = global_payment_entry(charge_id)
                if _payment_entry_is_awarded(entry):
                    if _payment_entry_is_persisted(entry):
                        if DATABASE_URL and _postgres_available():
                            await asyncio.to_thread(_mark_payment_completed_postgres_sync, charge_id)
                        DATA.setdefault("payment_recovery_queue", {}).pop(charge_id, None)
                        await asyncio.to_thread(_remove_local_payment_event_sync, charge_id)
                        continue
                    # Award exists only in memory and was not confirmed in authoritative DATA yet.
                    # Retry the exact same commit; never mark the independent ledger completed first.
                    committed = await _finalize_payment_commit(event, "payment_recovery_pending_commit")
                    if committed or not DATABASE_URL:
                        recovered += 1
                    continue

                if DATABASE_URL:
                    ok, _inserted, record = await asyncio.to_thread(_reserve_payment_event_postgres_sync, event)
                    if not ok or not _payment_event_matches_record(event, record):
                        await asyncio.to_thread(_queue_local_payment_event_sync, event)
                        continue
                    if str(record.get("status")) == "completed":
                        # Completed in the dedicated ledger but root DATA missing is inconsistent; never regrant automatically.
                        logger.critical("Payment %s completed in dedicated ledger but absent from root DATA; manual audit required", charge_id)
                        continue

                ok, _reward_text = _apply_payment_event_reward(event)
                if not ok:
                    continue
                committed = await _finalize_payment_commit(event, "payment_recovery")
                if committed or not DATABASE_URL:
                    recovered += 1
            except Exception as ex:
                logger.exception("Payment recovery failed for %s: %s", charge_id, ex)
    return recovered


async def payment_recovery_worker():
    while True:
        try:
            await recover_pending_payments()
        except Exception as ex:
            logger.exception("payment_recovery_worker failed: %s", ex)
        await asyncio.sleep(PAYMENT_RECOVERY_INTERVAL_SECONDS)


def parse_payment_payload(payload):
    parts = str(payload or "").split(":")
    if len(parts) == 2 and parts[0] == "multipass_premium" and parts[1].isdigit():
        return {"kind": "multipass", "user_id": parts[1], "code": "multipass_premium"}
    if len(parts) == 3 and parts[0] == "star_pack" and parts[2].isdigit():
        return {"kind": "star_pack", "user_id": parts[2], "code": parts[1]}
    return None


def expected_payment_amount(payload):
    parsed = parse_payment_payload(payload)
    if not parsed:
        return None
    if parsed["kind"] == "multipass":
        return int(PASS_PRICE_STARS)
    if parsed["kind"] == "star_pack":
        pack = STAR_PACKS.get(parsed["code"])
        return int(pack["price"]) if pack else None
    return None


def payment_id_from_successful(successful_payment):
    # Never synthesize a reusable id from payload+amount: only Telegram/provider charge ids are globally unique.
    return str(
        getattr(successful_payment, "telegram_payment_charge_id", "")
        or getattr(successful_payment, "provider_payment_charge_id", "")
        or ""
    ).strip()


def payment_already_processed(player, payment_id):
    if not payment_id:
        return False
    if _payment_entry_is_awarded(global_payment_entry(payment_id)):
        return True
    # Backward compatibility for PATCH35/PATCH36 history; new authority is the unbounded root ledger.
    return payment_id in set(map(str, player.setdefault("processed_payments", [])))


def record_payment(player, payment_id, kind, code, amount):
    # Keep a short per-player display history for compatibility, but never use truncation as the global idempotency boundary.
    player.setdefault("processed_payments", [])
    if payment_id and payment_id not in player["processed_payments"]:
        player["processed_payments"].append(payment_id)
        player["processed_payments"] = player["processed_payments"][-200:]
    player.setdefault("purchases", []).append({
        "id": payment_id,
        "kind": kind,
        "code": code,
        "amount": int(amount),
        "currency": PAYMENT_CURRENCY,
        "created_at": utc_now().isoformat(),
    })
    player["purchases"] = player["purchases"][-200:]
    player["stars_earned"] = int(player.get("stars_earned", 0)) + int(amount)


@dp.pre_checkout_query()
async def pre_checkout_query(pre_checkout: types.PreCheckoutQuery):
    payload = pre_checkout.invoice_payload
    parsed = parse_payment_payload(payload)
    expected = expected_payment_amount(payload)
    if not storage_is_healthy():
        await pre_checkout.answer(ok=False, error_message="Хранилище временно недоступно. Оплата отключена до восстановления связи — Stars не будут списаны.")
        return
    if not parsed or expected is None:
        await pre_checkout.answer(ok=False, error_message="Неизвестный платёж. Открой счёт заново из бота.")
        return
    if parsed["user_id"] != str(pre_checkout.from_user.id):
        await pre_checkout.answer(ok=False, error_message="Этот счёт создан для другого игрока.")
        return
    if pre_checkout.currency != PAYMENT_CURRENCY or int(pre_checkout.total_amount) != expected:
        await pre_checkout.answer(ok=False, error_message="Цена или валюта платежа не совпала. Открой счёт заново.")
        return
    await pre_checkout.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payment = message.successful_payment
    payload = payment.invoice_payload
    parsed = parse_payment_payload(payload)
    expected = expected_payment_amount(payload)
    player = get_user_data(message.from_user)
    if not parsed or expected is None:
        await message.answer("⚠️ Платёж получен, но payload неизвестен. Награда не выдана автоматически; напиши /paysupport.", reply_markup=back_menu())
        return
    if parsed["user_id"] != str(message.from_user.id) or payment.currency != PAYMENT_CURRENCY or int(payment.total_amount) != expected:
        await message.answer("⚠️ Платёж получен, но проверка суммы/валюты/игрока не прошла. Награда заморожена для ручной проверки; напиши /paysupport.", reply_markup=back_menu())
        logger.warning("Payment validation failed user=%s payload=%s amount=%s currency=%s", message.from_user.id, payload, payment.total_amount, payment.currency)
        return

    charge_id = payment_id_from_successful(payment)
    if not charge_id:
        await message.answer("⚠️ Telegram не передал уникальный charge ID. Награда не выдана автоматически; напиши /paysupport.", reply_markup=back_menu())
        logger.critical("Successful payment without charge id user=%s payload=%s", message.from_user.id, payload)
        return
    event = _build_payment_event(message, payment, parsed, charge_id)

    async with _get_payment_process_lock():
        if payment_already_processed(player, charge_id):
            entry = global_payment_entry(charge_id)
            if _payment_entry_is_awarded(entry) and not _payment_entry_is_persisted(entry):
                committed = await _finalize_payment_commit(event, "payment_duplicate_pending_commit")
                note = "✅ Платёж уже был выдан; облачная фиксация подтверждена." if (committed or not DATABASE_URL) else "⚠️ Платёж уже был выдан; облачная фиксация всё ещё повторяется автоматически."
            else:
                if DATABASE_URL and _postgres_available() and _payment_entry_is_persisted(entry):
                    await asyncio.to_thread(_mark_payment_completed_postgres_sync, charge_id)
                note = "✅ Этот платёж уже обработан. Повторная выдача не выполнялась."
            await message.answer(note, reply_markup=back_menu())
            return

        # Reserve the charge in an independent UNIQUE ledger before granting anything.
        if DATABASE_URL:
            ok, _inserted, record = await asyncio.to_thread(_reserve_payment_event_postgres_sync, event)
            if not ok:
                await asyncio.to_thread(_queue_local_payment_event_sync, event)
                DATA.setdefault("payment_recovery_queue", {})[charge_id] = copy.deepcopy(event)
                mark_data_dirty("payment_received_storage_outage")
                _set_storage_health(False, "payment ledger reservation failed")
                await message.answer(
                    "⭐ <b>Платёж получен Telegram.</b>\n\n"
                    "Основное облачное хранилище сейчас недоступно, поэтому покупка поставлена в безопасную очередь и будет активирована после восстановления связи. Повторно платить не нужно.",
                    parse_mode="HTML", reply_markup=back_menu(),
                )
                await notify_owner_purchase(message.from_user, f"⚠️ <b>Stars-платёж ждёт восстановления хранилища</b>\nID игрока: <code>{message.from_user.id}</code>\nCharge: <code>{e(charge_id)}</code>")
                return
            if not _payment_event_matches_record(event, record):
                logger.critical("Charge id collision/mismatch charge=%s event=%r record=%r", charge_id, event, record)
                await message.answer("⚠️ Charge ID уже существует с другими реквизитами. Покупка заморожена для ручного аудита; напиши /paysupport.", reply_markup=back_menu())
                return
            if str(record.get("status")) == "completed":
                await message.answer("✅ Этот платёж уже был завершён ранее. Повторная выдача не выполнялась.", reply_markup=back_menu())
                return
        else:
            # Local mode still journals the event atomically before granting.
            await asyncio.to_thread(_queue_local_payment_event_sync, event)

        ok, reward_text = _apply_payment_event_reward(event)
        if not ok:
            await message.answer(f"⚠️ Платёж зарезервирован, но награда не выдана автоматически: {e(reward_text)} Напиши /paysupport.", reply_markup=back_menu(), parse_mode="HTML")
            return

        committed = await _finalize_payment_commit(event, f"payment_{parsed['kind']}")
        if committed or not DATABASE_URL:
            status_note = "✅ Покупка надёжно зафиксирована."
        else:
            status_note = "⚠️ Награда активна в текущем процессе, но облачная фиксация ещё повторяется автоматически. Повторно платить не нужно."

        if parsed["kind"] == "multipass":
            title = "✅ <b>Premium MultiPass открыт.</b>"
        else:
            title = "✅ <b>Stars-набор выдан.</b>"
        await message.answer(title + "\n\n" + reward_text + "\n\n" + status_note, reply_markup=back_menu(), parse_mode="HTML")
        await notify_owner_purchase(
            message.from_user,
            "⭐ <b>Подтверждённая покупка Telegram Stars</b>\n\n"
            f"Игрок: <b>{e(player.get('name', message.from_user.full_name))}</b>\n"
            f"ID: <code>{message.from_user.id}</code>\n"
            f"Тип: <b>{e(parsed['kind'])}</b>\n"
            f"Код: <b>{e(parsed['code'])}</b>\n"
            f"Stars: <b>{payment.total_amount}</b>\n"
            f"Charge ID: <code>{e(charge_id)}</code>\n"
            f"Commit: <b>{'OK' if committed or not DATABASE_URL else 'RETRYING'}</b>",
        )



async def send_modes(message, user):
    p = get_user_data(user)
    ensure_rpg_fields(p)
    deck_count = len([cid for cid in p.get("deck", []) if cid in CARD_BY_ID])
    source = "моя колода" if not p.get("auto_team", True) else "авто-колода"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="⚔️ Арена", callback_data="battle:start"), button(text="🌐 Онлайн", callback_data="online_search")],
        [button(text="🃏 Колода", callback_data="deck"), button(text="🎪 Ивенты", callback_data="events")],
        [button(text="👹 Рейд", callback_data="raid_info")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(
        f"{CE['modes']} <b>ИГРАТЬ</b>\n\n"
        f"{CE['arena']} Арена — быстрый бой против бота.\n"
        f"{CE['online']} Онлайн — PvP против игрока.\n"
        f"{CE['deck']} Колода — 5 бойцов и авто-сбор.\n"
        f"{CE['events']} Ивенты — задания и редкие награды.\n"
        f"{CE['raid']} Рейд — босс недели и топ урона.\n\n"
        f"Колода: <b>{deck_count}/5</b> · режим: <b>{e(source)}</b>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "modes")
async def modes_cb(callback: types.CallbackQuery):
    await send_modes(callback.message, callback.from_user)
    await callback.answer()



@dp.callback_query(F.data == "pvp_source_menu")
async def pvp_source_menu_cb(callback: types.CallbackQuery):
    await send_battle_source_menu(callback.message, callback.from_user, "pvp")
    await callback.answer()


@dp.callback_query(F.data.startswith("pvp_source:"))
async def pvp_source_legacy_cb(callback: types.CallbackQuery):
    # Совместимость со старыми callback без изменения объекта callback.
    source = callback.data.split(":", 1)[1]
    if source not in {"deck", "random_bot", "manual"}:
        await callback.answer("Неизвестный тип команды.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    p["battle_team_source"] = source
    p["pvp_team_source"] = source
    mark_data_dirty("data_changed")
    if source == "manual":
        manual_team_drafts[str(callback.from_user.id)] = {"target": "pvp", "cards": [], "updated_at_ts": time.time()}
        await send_manual_team_picker(callback.message, callback.from_user, "pvp", 0)
    else:
        await callback.message.answer(
            "✅ Тип команды сохранён для PvP/рейда.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [button(text="🌐 Онлайн", callback_data="online_search")],
                [button(text="👹 Рейд", callback_data="raid_info")],
                [button(text="⬅️ Режимы", callback_data="modes")],
            ]),
            parse_mode="HTML",
        )
    await callback.answer()

async def send_nick_help(message, user):
    p = get_user_data(user)
    await message.answer(
        "✏️ <b>Смена ника</b>\n\n"
        f"Текущий ник: <b>{e(p.get('name', user.full_name))}</b>\n\n"
        "Чтобы сменить ник, напиши:\n"
        "<code>/nick НовыйНик</code>\n\n"
        "Пример:\n"
        "<code>/nick LoneCoder</code>",
        reply_markup=back_menu(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "nick_help")
async def nick_help_cb(callback: types.CallbackQuery):
    await send_nick_help(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("nick"))
async def nick_cmd(message: types.Message):
    p = get_user_data(message.from_user)
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await send_nick_help(message, message.from_user)
        return
    nick = parts[1].strip()
    if len(nick) > 24:
        await message.answer("Ник слишком длинный. Максимум 24 символа.", reply_markup=back_menu())
        return
    p["nickname"] = nick
    p["name"] = nick
    mark_data_dirty("data_changed")
    await message.answer(f"✅ Ник изменён на: <b>{e(nick)}</b>", reply_markup=back_menu(), parse_mode="HTML")


async def send_mega_open(message, user):
    p = get_user_data(user)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🎴 Призвать ×3", callback_data="mega_buy:basic:3")],
        [button(text="🎴 Призвать ×5", callback_data="mega_buy:rare:5")],
        [button(text="🎴 Призвать ×10", callback_data="mega_buy:royal:10")],
        [button(text="🏪 Купить попытки", callback_data="shop_attempts")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(
        "🎴 <b>МЕГА-ПРИЗЫВ</b>\n\n"
        f"Доступно попыток: <b>{short_number(available_attempts(p))}</b>.\n"
        "Выбранное количество списывается целиком или не списывается вообще. "
        "Фисташки за сундуки больше не тратятся.",
        reply_markup=kb,
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "mega_open")
async def mega_open_cb(callback: types.CallbackQuery):
    await send_mega_open(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("mega_buy:"))
async def mega_buy(callback: types.CallbackQuery):
    try:
        _, _legacy_kind, amount_s = callback.data.split(":")
        amount = max(1, min(10, int(amount_s)))
    except Exception:
        await callback.answer("Ошибка мега-призыва.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    ok, source, wait = consume_summon_attempts(p, callback.from_user.id, amount)
    if not ok:
        await callback.answer(
            f"Недостаточно попыток. Доступно: {available_attempts(p)}. Бесплатная через {compact_wait_label(wait)}.",
            show_alert=True,
        )
        return
    universe_id = selected_universe_id(p)
    got = []
    pulled = set()
    for _ in range(amount):
        card, pity_note = roll_card_with_pity(
            p,
            weights=SUMMON_WEIGHTS,
            exclude=pulled,
            universe_id=universe_id,
            allow_super_absolute=False,
        )
        pulled.add(card["id"])
        got.append((card, add_card(p, card["id"]) + pity_note))
    add_xp(p, 80 * amount)
    add_pass_task_progress(p, "chest", amount)
    add_newbie_task_progress(p, "chest", amount)
    add_season_xp(p, SEASON_XP_REWARDS["draw"] * amount, action_key=f"mega:{callback.from_user.id}:{time.time_ns()}")
    mark_data_dirty("mega_summon")
    await send_pack_result(callback.message, f"Мега-призыв ×{amount} · {source}", got, p)
    await callback.answer(f"Получено персонажей: {amount}")


def _queue_uid(item):
    if isinstance(item, dict):
        return str(item.get("uid", ""))
    return str(item)


def cleanup_online_queue():
    now = utc_now()
    fresh = []
    seen = set()
    for item in online_queue:
        uid = _queue_uid(item)
        if not uid or uid in seen:
            continue
        seen.add(uid)
        joined_raw = item.get("joined_at") if isinstance(item, dict) else ""
        expired = False
        if joined_raw:
            try:
                joined_dt = _parse_iso_datetime(joined_raw)
                expired = not joined_dt or (now - joined_dt).total_seconds() > ONLINE_QUEUE_TTL_SECONDS
            except Exception:
                expired = True
        if not expired and is_online(uid):
            fresh.append(item)
    online_queue[:] = fresh


def remove_from_online_queue(uid):
    uid = str(uid)
    before = len(online_queue)
    online_queue[:] = [item for item in online_queue if _queue_uid(item) != uid]
    return len(online_queue) != before


async def join_online_queue(user):
    uid = str(user.id)
    get_user_data(user)
    cleanup_online_queue()
    if any(_queue_uid(item) == uid for item in online_queue):
        return None
    while online_queue:
        enemy_item = online_queue.pop(0)
        enemy = _queue_uid(enemy_item)
        if not enemy or enemy == uid or not is_online(enemy):
            continue
        bid = new_pvp_id()
        active_pvp[bid] = {
            "players": [enemy, uid],
            "names": {
                enemy: DATA["users"].get(enemy, {}).get("name", enemy),
                uid: DATA["users"].get(uid, {}).get("name", user.full_name),
            },
            "round": 1,
            "turn": 0,
            "teams": {enemy: [], uid: []},
            "options": [],
            "done": False,
            "scored": False,
            "starters": {},
            "resolved": False,
            "created_at": utc_now().isoformat(),
            "created_at_ts": time.time(),
        }
        return bid
    online_queue.append({"uid": uid, "joined_at": utc_now().isoformat()})
    return None


async def announce_online_match(bid):
    state = active_pvp[bid]
    for uid in state["players"]:
        try:
            await bot.send_message(int(uid), "🌐 Онлайн-соперник найден. Начинается скрытый PvP-драфт.", parse_mode="HTML")
        except Exception as ex:
            logger.debug("Online match notification failed for %s: %s", uid, ex)
    await send_pvp_round(bid)


@dp.callback_query(F.data == "online_search")
async def online_search_cb(callback: types.CallbackQuery):
    bid = await join_online_queue(callback.from_user)
    if bid:
        await announce_online_match(bid)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [button(text="❌ Отменить поиск", callback_data="online_cancel")],
            [button(text="⬅️ Режимы", callback_data="modes")],
        ])
        await callback.message.answer(
            "🌐 <b>Поиск онлайн-боя</b>\n\n"
            "Ты в очереди. Если соперник не найдётся за 5 минут, очередь очистится автоматически.",
            reply_markup=kb,
            parse_mode="HTML"
        )
    await callback.answer()


@dp.callback_query(F.data == "online_cancel")
async def online_cancel_cb(callback: types.CallbackQuery):
    removed = remove_from_online_queue(callback.from_user.id)
    await callback.message.answer("❌ Поиск онлайн-боя отменён." if removed else "Очередь уже пуста.", reply_markup=back_menu())
    await callback.answer()


@dp.message(Command("online"))
async def online_cmd(message: types.Message):
    bid = await join_online_queue(message.from_user)
    if bid:
        await announce_online_match(bid)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [button(text="❌ Отменить поиск", callback_data="online_cancel")],
            [button(text="⬅️ Режимы", callback_data="modes")],
        ])
        await message.answer("🌐 Ты в очереди онлайн-боя. Автоочистка — через 5 минут.", reply_markup=kb)


def ensure_admin_known_users():
    DATA.setdefault("users", {})
    for uid in owner_ids() | right_hand_ids():
        if not uid.isdigit():
            continue
        if uid not in DATA["users"]:
            DATA["users"][uid] = {
                "name": "Владелец" if uid in owner_ids() else "Правая рука",
                "username": "",
                "fistiks": 0,
                "xp": 0,
                "wins": 0,
                "losses": 0,
                "battles": 0,
                "collection": {},
                "badges": ["DEV"] if uid in owner_ids() else ["RIGHT_HAND"],
                "last_seen": "",
                "created_at": utc_now().isoformat(),
                "banned": False,
                "frozen": False,
                "bot_blocked": False,
                "deleted": False,
                "moon_coins": 0,
                "technical_placeholder": True,
            }
        else:
            p = DATA["users"][uid]
            p.setdefault("username", "")
            p.setdefault("collection", {})
            p.setdefault("badges", [])
            if uid in owner_ids() and "DEV" not in p["badges"]:
                p["badges"].append("DEV")
            if uid in right_hand_ids() and "RIGHT_HAND" not in p["badges"]:
                p["badges"].append("RIGHT_HAND")


def short_user_line(uid, p, index=0):
    name = p.get("name") or p.get("nickname") or p.get("username") or f"Игрок {str(uid)[-4:]}"
    flags = []
    if uid in owner_ids():
        flags.append("👑")
    if uid in right_hand_ids():
        flags.append("🤝")
    if p.get("banned"):
        flags.append("⛔")
    if p.get("frozen"):
        flags.append("🧊")
    if p.get("bot_blocked"):
        flags.append("🚫")
    if p.get("deleted"):
        flags.append("🗑")
    if p.get("technical_placeholder"):
        flags.append("🧾")
    lvl, _, _ = calc_user_level(int(p.get("xp", 0) or 0))
    last = p.get("last_seen") or "нет данных"
    flag_text = (" ".join(flags) + " ") if flags else ""
    uname = f"@{p.get('username')}" if p.get("username") else "без username"
    unlocked = sum(1 for _, info in (p.get('collection', {}) or {}).items() if (info or {}).get('unlocked', True) or int((info or {}).get('count', 0) or 0) > 0)
    return (
        f"{index}. {flag_text}<b>{e(name)}</b> | {e(uname)} | "
        f"ур. {lvl} | карт {unlocked} | боёв {p.get('battles', 0)} | вход {e(str(last)[:16])}"
    )





AUTO_PURGE_INACTIVE_DAYS = max(7, int(os.getenv("ABM_AUTO_PURGE_INACTIVE_DAYS", "30") or 30))
AUTO_PURGE_INTERVAL_SECONDS = max(3600, int(os.getenv("ABM_AUTO_PURGE_INTERVAL_SECONDS", "21600") or 21600))


def _aware_utc(value):
    dt = _parse_iso_datetime(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def auto_purge_stale_users():
    users = DATA.setdefault("users", {})
    now = utc_now()
    removed = []
    for uid, player in list(users.items()):
        uid = str(uid)
        player = player or {}
        if is_owner(uid) or player.get("banned") or player.get("frozen") or is_permanently_deleted_id(uid):
            continue
        last = _aware_utc(player.get("last_seen", ""))
        stale = bool(last and now - last > timedelta(days=AUTO_PURGE_INACTIVE_DAYS))
        never_used_old = not last and _aware_utc(player.get("created_at", "")) and now - _aware_utc(player.get("created_at", "")) > timedelta(days=AUTO_PURGE_INACTIVE_DAYS)
        if player.get("bot_blocked") or stale or never_used_old:
            reason = "bot_blocked_cleanup" if player.get("bot_blocked") else "inactive_cleanup"
            payment_state = {
                key: copy.deepcopy(player.get(key))
                for key in (
                    "purchases", "processed_payments", "stars_earned", "premium",
                    "pass_premium", "pass_premium_cap", "pass_until",
                    "pass_purchase_request", "pass_granted_at", "pass_granted_by",
                )
                if key in player
            }
            DATA.setdefault("purged_users", {})[uid] = {
                "reason": reason,
                "permanent": False,
                "purged_at": now.isoformat(),
                "preserved_payment_state": payment_state,
            }
            users.pop(uid, None)
            removed.append(uid)
    if removed:
        for uid in removed:
            DATA.setdefault("friends", {}).pop(uid, None)
        for uid, friends in list(DATA.setdefault("friends", {}).items()):
            DATA["friends"][uid] = [fid for fid in (friends or []) if str(fid) not in removed]
        for clan in (DATA.get("clans", {}) or {}).values():
            clan["members"] = [m for m in (clan.get("members", []) or []) if str(m) not in removed]
        mark_data_dirty("auto_purge_stale_users")
    return len(removed)


def _reset_player_gameplay_patch40(player):
    purchases = copy.deepcopy(player.get("purchases", []))
    processed = copy.deepcopy(player.get("processed_payments", []))
    stars_earned = int(player.get("stars_earned", 0) or 0)
    paid_state = {
        "premium": bool(player.get("premium", False)),
        "pass_premium": bool(player.get("pass_premium", False)),
        "pass_premium_cap": int(player.get("pass_premium_cap", 0) or 0),
        "pass_until": player.get("pass_until", ""),
        "pass_purchase_request": player.get("pass_purchase_request", ""),
    }
    # Keep identity and recent activity timestamp so an active account is not
    # immediately mistaken for a 30-day stale record after the global reset.
    identity = {k: player.get(k) for k in ("name", "username", "nickname", "created_at", "last_seen")}
    player.clear()
    player.update({
        **identity,
        "fistiks": STARTER_FISTIKS, "xp": 0, "wins": 0, "losses": 0, "battles": 0,
        "last_daily": "", "daily_streak": 0, "last_free_pack": "", "free_pack_notified": False,
        "last_free_notice": "", "collection": {}, "badges": [], "used_promos": [],
        "ref_by": "", "ref_count": 0, "ref_earned": 0, "pass_xp": 0,
        "claimed_pass_free": [], "claimed_pass_premium": [], "moon_coins": 0,
        "pity_counters": {"epic": 0, "legendary": 0, "mythic": 0},
        "notify_free_pack": True, "banned": False, "frozen": False,
        "artifacts": {}, "equipped_artifact": "", "deck": [], "auto_team": True,
        "pass_daily_date": "", "pass_task_progress": {}, "pass_task_claimed": [],
        "newbie_claimed": [], "newbie_progress": {}, "pvp_team_source": "deck",
        "battle_team_source": "deck", "ref_milestones_claimed": [], "support_tickets": [],
        "purchases": purchases, "processed_payments": processed, "stars_earned": stars_earned,
        "battle_history": [], "last_actions": [], "system_inbox": [], "notifications": [],
        "luffy_day": 0, "last_luffy_claim": "", "luffy_claimed_cards": [],
        "luffy_intro_seen": False, "luffy_finished": False, "compensations": [],
        "preferred_universe": "", "universe_onboarding_seen": "", "card_attempts": 0,
        "onboarding_version": ONBOARDING_VERSION, "onboarding_state": "choose_universe",
        "onboarding_complete": False, "starter_bundle_claimed": False, "starter_cards": [],
        "onboarding_leader_options": [], "clan_id": "", "title": "Новичок разлома",
        "custom_bg": "", "nickname_selected_once": False, "friend_gifts": {},
        "case_inventory": {"light": 0, "event": 0, "holiday": 0, "mystic": 0},
        "privilege": "", "season_id": "", "season_xp": 0, "season_claimed": [],
        "season_action_keys": [], "patch40_reset_at": utc_now().isoformat(),
    })
    # Verified purchase history and active paid entitlement are preserved for payment safety.
    player.update(paid_state)
    return player


async def reset_all_gameplay_patch40():
    users = DATA.setdefault("users", {})
    reset_ids = []
    for uid, player in list(users.items()):
        if is_owner(uid):
            continue
        _reset_player_gameplay_patch40(player)
        reset_ids.append(str(uid))
    DATA["friends"] = {str(uid): [] for uid in users if is_owner(uid)}
    DATA["friend_invites"] = {}
    for cid, clan in list((DATA.get("clans", {}) or {}).items()):
        clan["members"] = [m for m in (clan.get("members", []) or []) if is_owner(m)]
        if not clan["members"] and not is_owner(clan.get("leader", "")):
            DATA["clans"].pop(cid, None)
    active_battles.clear(); active_pvp.clear(); manual_team_drafts.clear(); online_queue.clear()
    DATA.setdefault("storage_meta", {})["patch40_progress_reset_at"] = utc_now().isoformat()
    DATA["storage_meta"]["patch40_progress_reset_count"] = len(reset_ids)
    mark_data_dirty("patch40_global_progress_reset")
    return len(reset_ids)


async def send_admin_panel(message, user):
    if not is_owner(user.id):
        await message.answer("⛔ Только владелец мультивселенной имеет доступ.")
        return
    ensure_admin_known_users()
    repair_all_luffy_progress()
    await auto_purge_stale_users()
    all_items = all_player_items(include_deleted=True, include_blocked=True)
    live_items = [(uid, p) for uid, p in admin_live_player_items() if not is_owner(uid)]
    total_all = len(DATA.get("users", {}) or {})
    live = len(live_items)
    banned = sum(1 for uid, p in all_items if not is_owner(uid) and p.get("banned"))
    frozen = sum(1 for uid, p in all_items if not is_owner(uid) and p.get("frozen"))
    online = sum(1 for uid, _ in live_items if is_online(uid))
    now = utc_now()
    new_24h = 0
    for uid, p in all_items:
        if is_owner(uid):
            continue
        created = _parse_iso_datetime(p.get("created_at", ""))
        if created:
            try:
                if now - created <= timedelta(days=1):
                    new_24h += 1
            except TypeError:
                pass
    paid_pending = sum(1 for uid, p in all_items if not is_owner(uid) and p.get("pass_purchase_request") == "paid_pending")
    text = (
        "🛠 <b>КОМАНДНЫЙ ЦЕНТР ВЛАДЕЛЬЦА</b>\n\n"
        f"<blockquote>🧬 Активные игроки: <b>{live}</b>\n"
        f"🟢 Онлайн за 10 минут: <b>{online}</b>\n"
        f"🆕 Новые за сутки: <b>{new_24h}</b>\n"
        f"⭐ Оплат pass на проверке: <b>{paid_pending}</b>\n"
        f"⛔ Забанены: <b>{banned}</b> · 🧊 заморожены: <b>{frozen}</b></blockquote>\n"
        f"💤 Неактивные 30+ дней: <b>{admin_inactive_count()}</b> — удаляются автоматически.\n"
        f"📦 Всего текущих записей в базе: <b>{total_all}</b>\n\n"
        "<b>Основные команды</b>\n"
        "<code>/user ID</code> · <code>/ban ID</code> · <code>/freeze ID</code>\n"
        "<code>/givef ID AMOUNT</code> · <code>/givemoon ID AMOUNT</code> · <code>/givecard ID CARD_ID</code>\n"
        "<code>/storage</code> · <code>/flush_data</code>\n"
        "<code>/reset_patch40 CONFIRM</code> — обнулить игровой прогресс всех, кроме владельца"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🟢 Активные игроки", callback_data="admin_users:live:0"), button(text="📦 Все из базы", callback_data="admin_users:all:0")],
        [button(text="⭐ Оплаты pass", callback_data="admin_payments"), button(text="🎁 Компенсация", callback_data="admin_compensation_info")],
        [button(text="🧠 Хранилище", callback_data="admin_storage")],
        [button(text="♻️ Сброс PATCH40", callback_data="admin_patch40_reset_info")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


async def send_admin_users(message, page=0, mode="live"):
    recovery = {"sources": 0, "after": len(DATA.get("users", {})), "added": 0}
    ensure_admin_known_users()
    repair_all_luffy_progress()
    mode = "all" if str(mode) == "all" else "live"
    if mode == "all":
        items = all_player_items(include_deleted=True, include_blocked=True)
        title = "📦 Все игроки из базы"
        hint = (
            "Показаны все записи из актуального DATA/Neon: живые, inactive, blocked, frozen, deleted и технические записи владельца. "
            "Это нужно, чтобы админка больше не прятала игроков.\n"
        )
    else:
        items = admin_live_player_items()
        title = "🟢 Живые игроки"
        hint = (
            "Показаны только игроки, которые не заблокировали бота и заходили за последние 30 дней. "
            "Полный список смотри через кнопку «Все из базы».\n"
        )

    def _admin_sort_key(item):
        uid, p = item
        role = 0 if uid in owner_ids() else (1 if uid in right_hand_ids() else 2)
        return (role, -_player_progress_score(p), str(p.get("name") or p.get("username") or uid).lower())

    items.sort(key=_admin_sort_key)
    per_page = 8
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    chunk = items[page * per_page:(page + 1) * per_page]
    text = f"<b>{title}</b> — страница {page + 1}/{pages}\n\n"
    text += (
        f"{hint}"
        f"Всего в этом режиме: <b>{len(items)}</b> · "
        f"Blocked: <b>{admin_blocked_count()}</b> · "
        f"Inactive 30+ дней: <b>{admin_inactive_count()}</b> · "
        f"Восстановление источников: <b>{recovery.get('sources', 0)}</b>.\n\n"
        "ID скрыт в кнопках, чтобы не ловить ошибку Telegram privacy/button. Открытие профиля безопасное — через callback внутри бота.\n\n"
    )
    rows = []
    if not chunk:
        text += "Игроки не найдены."
    for i, (uid, p) in enumerate(chunk, page * per_page + 1):
        text += short_user_line(uid, p, i) + "\n"
        display = str(p.get('name') or p.get('username') or f"Игрок {str(uid)[-4:]}")[:28]
        rows.append([button(text=f"📊 {display}", callback_data=f"admin_user:{uid}")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"admin_users:{mode}:{page-1}"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"admin_users:{mode}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        button(text="🟢 Живые", callback_data="admin_users:live:0"),
        button(text="📦 Все из базы", callback_data="admin_users:all:0"),
    ])
    rows.append([button(text="🔄 Обновить", callback_data=f"admin_users:{mode}:{page}"), button(text="⬅️ Админ-панель", callback_data="admin")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")

async def send_admin_payments(message):
    users = DATA.get("users", {})
    items = [(uid, p) for uid, p in users.items() if p.get("pass_purchase_request") in {"paid_pending", "paid", "activated", "rejected_after_payment"}]
    items.sort(key=lambda x: x[1].get("last_seen", ""), reverse=True)
    text = "⭐ <b>Оплаты мультипасса</b>\n\n"
    rows = []
    if not items:
        text += "Нет оплат на проверке."
    for uid, p in items[:30]:
        state = p.get("pass_purchase_request", "")
        text += f"• <b>{e(p.get('name', uid))}</b> | <code>{uid}</code> | {e(state)} | Stars: {p.get('stars_earned',0)}\n"
        if state == "paid_pending":
            rows.append([button(text=f"{p.get('name', uid)[:18]} → 100 ур.", callback_data=f"pass_paid:approve:{uid}:100")])
            rows.append([button(text="50 ур.", callback_data=f"pass_paid:approve:{uid}:50"), button(text="20 ур.", callback_data=f"pass_paid:approve:{uid}:20"), button(text="Заморозить", callback_data=f"pass_paid:reject:{uid}:0")])
    rows.append([button(text="⬅️ Админ-панель", callback_data="admin")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


async def send_admin_user(message, uid):
    ensure_admin_known_users()
    if str(uid) not in DATA.get("users", {}):
        await message.answer("Игрок не найден.", reply_markup=back_menu())
        return
    p = DATA["users"][str(uid)]
    lvl, rem, nxt = calc_user_level(p.get("xp", 0))
    text = (
        "👤 <b>Аккаунт игрока</b>\n\n"
        f"ID: <code>{uid}</code>\n"
        f"Имя: <b>{e(p.get('name', uid))}</b>\n"
        f"Уровень: <b>{lvl}</b> ({rem}/{nxt} XP)\n"
        f"{PISTACHIOS_LABEL}: <b>{short_number(p.get('fistiks', 0))}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{short_number(p.get('moon_coins', 0))}</b>\n"
        f"Карт: <b>{short_number(len(p.get('collection', {}) or {}))}/{short_number(len(CARDS))}</b>\n"
        f"Боёв: <b>{p.get('battles', 0)}</b> | Побед: <b>{p.get('wins', 0)}</b> | Поражений: <b>{p.get('losses', 0)}</b>\n"
        f"MultiPass: <b>{'premium' if p.get('pass_premium') else p.get('pass_purchase_request', 'нет')}</b> | cap {p.get('pass_premium_cap', 0)} | срок: <code>{e(pass_until_label(p))}</code>\n"
        f"Бан: <b>{'да' if p.get('banned') else 'нет'}</b> | Заморозка: <b>{'да' if p.get('frozen') else 'нет'}</b>\n"
        f"Бот заблокирован: <b>{'да' if p.get('bot_blocked') else 'нет'}</b> | Удалён: <b>{'да' if p.get('deleted') else 'нет'}</b>\n"
        f"Уведомления сундука: <b>{'вкл' if p.get('notify_free_pack', True) else 'выкл'}</b>\n"
        f"Последний вход: <code>{e(p.get('last_seen') or 'нет данных')}</code>"
    )
    actions = p.get("last_actions", [])[-8:]
    if actions:
        text += "\n\n<b>Последние действия:</b>\n" + "\n".join(
            f"• <code>{e(a.get('at',''))}</code> — {e(a.get('action',''))}" for a in actions
        )
    rows = [
        [button(text="⛔ Бан", callback_data=f"admin_ban:{uid}"),
         button(text="✅ Разбан", callback_data=f"admin_unban:{uid}")],
        [button(text="🧊 Заморозить", callback_data=f"admin_freeze:{uid}"),
         button(text="♨️ Разморозить", callback_data=f"admin_unfreeze:{uid}")],
        [button(text="💎 +1000", callback_data=f"admin_givef:{uid}:1000"),
         button(text="🐉 +10", callback_data=f"admin_givemoon:{uid}:10")],
        [button(text="🎟 Pass 30 дней", callback_data=f"admin_givepass:{uid}:30"),
         button(text="🚫 Снять Pass", callback_data=f"admin_takepass:{uid}")],
        [button(text="🆔 Показать ID", callback_data=f"admin_show_id:{uid}")],
        [button(text="🗑 Удалить…", callback_data=f"admin_delete_ask:{uid}")],
        [button(text="⬅️ Все из базы", callback_data="admin_users:all:0"), button(text="⬅️ Админ", callback_data="admin")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")



@dp.callback_query(F.data.startswith("admin_show_id:"))
async def admin_show_id_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    p = DATA.get("users", {}).get(str(uid), {})
    uname = f"@{p.get('username')}" if p.get('username') else "без username"
    await callback.answer(f"ID: {uid} | {uname}", show_alert=True)




@dp.callback_query(F.data == "admin_patch40_reset_info")
async def admin_patch40_reset_info_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await callback.message.answer(
        "♻️ <b>СБРОС ПРОГРЕССА PATCH40</b>\n\n"
        "Будет обнулён игровой прогресс всех аккаунтов, кроме владельца: карты, валюты, достижения, сезон, кланы и друзья. "
        "Глобальный платёжный ledger, история покупок и действующий оплаченный Premium сохраняются.\n\n"
        "Для защиты от случайного нажатия отправь вручную:\n<code>/reset_patch40 CONFIRM</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Админ-панель", callback_data="admin")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.message(Command("reset_patch40"))
async def reset_patch40_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip() != "CONFIRM":
        await message.answer("Для подтверждения введи точно: <code>/reset_patch40 CONFIRM</code>", parse_mode="HTML")
        return
    count = await reset_all_gameplay_patch40()
    saved = await flush_data_now_async("patch40_global_progress_reset")
    await message.answer(
        f"♻️ Сброшено аккаунтов: <b>{count}</b>.\n"
        f"Владелец не изменён. Платёжный ledger и оплаченные права сохранены.\n"
        f"Облачная фиксация: <b>{'успешно' if saved else 'не подтверждена — retry поставлен'}</b>.",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Админ-панель", callback_data="admin")]])
    )


@dp.message(Command("storage"))
async def storage_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    report = await asyncio.to_thread(storage_report_text)
    await message.answer(
        report,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🔄 Сохранить сейчас", callback_data="admin_storage_flush")],
            [button(text="⬅️ Админ-панель", callback_data="admin")],
        ]),
        parse_mode="HTML",
    )


@dp.message(Command("flush_data"))
async def flush_data_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    ok = await flush_data_now_async("owner_flush_data")
    await message.answer(
        ("✅ DATA сохранён в Neon/локальный fallback." if ok else "⚠️ DATA save не прошёл, смотри /storage."),
        reply_markup=back_menu()
    )


@dp.message(Command("purge_blocked"))
async def purge_blocked_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    users = DATA.get("users", {}) or {}
    ids = [uid for uid, p in list(users.items()) if str(uid).isdigit() and (p or {}).get("bot_blocked")]
    for uid in ids:
        DATA.setdefault("purged_users", {})[str(uid)] = {
            "reason": "bot_blocked_cleanup", "permanent": False, "purged_at": utc_now().isoformat()
        }
        users.pop(uid, None)
    mark_data_dirty("purge_bot_blocked")
    await message.answer(f"🧹 Технически очищено blocked-записей: <b>{len(ids)}</b>. После разблокировки человек сможет начать заново.", parse_mode="HTML", reply_markup=back_menu())

@dp.message(Command("admin"))
async def admin_cmd(message: types.Message):
    await send_admin_panel(message, message.from_user)


@dp.callback_query(F.data == "admin")
async def admin_cb(callback: types.CallbackQuery):
    await send_admin_panel(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data == "admin_payments")
async def admin_payments_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_admin_payments(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "admin_storage")
async def admin_storage_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    report = await asyncio.to_thread(storage_report_text)
    await callback.message.answer(
        report,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🔄 Сохранить сейчас", callback_data="admin_storage_flush")],
            [button(text="⬅️ Админ-панель", callback_data="admin")],
        ]),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_storage_flush")
async def admin_storage_flush_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    ok = await flush_data_now_async("admin_storage_button")
    await callback.answer("Данные сохранены." if ok else "Ошибка сохранения — открой отчёт.", show_alert=not ok)
    report = await asyncio.to_thread(storage_report_text)
    await callback.message.answer(
        report,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🔄 Сохранить сейчас", callback_data="admin_storage_flush")],
            [button(text="⬅️ Админ-панель", callback_data="admin")],
        ]),
        parse_mode="HTML",
    )

@dp.message(Command("recover_users_patch16"))
async def recover_users_patch16_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    recovery = await recover_users_from_all_sources_async(save=True)
    ensure_admin_known_users()
    await flush_data_now_async("recover_users_patch16")
    await message.answer(
        "🧩 <b>Восстановление игроков PATCH16</b>\n\n"
        f"Проверено источников: <b>{recovery.get('sources', 0)}</b>\n"
        f"Было записей: <b>{recovery.get('before', 0)}</b>\n"
        f"Стало записей: <b>{recovery.get('after', 0)}</b>\n"
        f"Добавлено/вернулось: <b>{recovery.get('added', 0)}</b>\n\n"
        "Теперь открой: <b>Админка → Все игроки</b>.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="👥 Все игроки", callback_data="admin_users:live:0")]])
    )




@dp.message(Command("sync_neon_patch17"))
async def sync_neon_patch17_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    if not DATABASE_URL:
        await message.answer("⚠️ DATABASE_URL не найден в Render Environment.")
        return
    before_pg = await asyncio.to_thread(_count_postgres_users)
    recovery = {"sources": 0}
    ensure_admin_known_users()
    await flush_data_now_async("sync_neon_patch17")
    after_pg = await asyncio.to_thread(_count_postgres_users)
    await message.answer(
        "🧬 <b>PATCH17 Neon Sync</b>\n\n"
        f"Neon до синхронизации: <b>{before_pg}</b> игроков\n"
        f"DATA сейчас: <b>{len(DATA.get('users', {}) or {})}</b> игроков\n"
        f"Neon после синхронизации: <b>{after_pg}</b> игроков\n"
        f"Источников восстановления: <b>{recovery.get('sources', 0)}</b>\n\n"
        "Теперь прогресс сохраняется во внешнюю базу Neon, а не в временную папку Render.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🧠 Хранилище", callback_data="admin_storage")], [button(text="👥 Все игроки", callback_data="admin_users:live:0")]])
    )


@dp.message(Command("recover_users_patch17"))
async def recover_users_patch17_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    recovery = await recover_users_from_all_sources_async(save=True)
    ensure_admin_known_users()
    await flush_data_now_async("recover_users_patch17")
    neon_users = await asyncio.to_thread(_count_postgres_users)
    await message.answer(
        "🧩 <b>Восстановление игроков PATCH17</b>\n\n"
        f"Проверено источников: <b>{recovery.get('sources', 0)}</b>\n"
        f"Было записей: <b>{recovery.get('before', 0)}</b>\n"
        f"Стало записей: <b>{recovery.get('after', 0)}</b>\n"
        f"Добавлено/вернулось: <b>{recovery.get('added', 0)}</b>\n"
        f"Игроков в Neon: <b>{neon_users}</b>\n\n"
        "Если старых игроков нет ни в JSON, ни в DB, ни в Neon — Telegram сам их список не отдаёт. Но новые игроки теперь сохраняются стабильно.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="👥 Все игроки", callback_data="admin_users:live:0")], [button(text="🧠 Хранилище", callback_data="admin_storage")]])
    )




async def notify_pass_granted(uid, p, days, until):
    try:
        await bot.send_message(
            int(uid),
            f"🎟 <b>Дар мультивселенной</b>\n\n"
            f"Владелец открыл тебе <b>Premium-мультипасс на {days} дней</b>.\n"
            f"Срок: <code>{e(until.strftime('%Y-%m-%d %H:%M'))}</code>\n\n"
            "Забирай premium-награды, копи очки pass и усиливай аккаунт. "
            "Если бот понравился — кинь ссылку другу, так мультивселенная растёт быстрее.",
            parse_mode="HTML",
            reply_markup=back_menu(),
        )
    except Exception as ex:
        logger.debug("pass grant notify failed for %s: %s", uid, ex)
        if uid in DATA.get("users", {}) and should_mark_bot_unreachable(ex):
            DATA["users"][uid]["bot_blocked"] = True
            mark_data_dirty("data_changed")


async def notify_pass_removed(uid):
    try:
        await bot.send_message(
            int(uid),
            "🎟 <b>MultiPass обновлён</b>\n\nPremium-доступ был снят владельцем мультивселенной.",
            parse_mode="HTML",
            reply_markup=back_menu(),
        )
    except Exception as ex:
        logger.debug("pass remove notify failed for %s: %s", uid, ex)
        if uid in DATA.get("users", {}) and should_mark_bot_unreachable(ex):
            DATA["users"][uid]["bot_blocked"] = True
            mark_data_dirty("data_changed")


@dp.message(Command("givepass"))
async def givepass_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: <code>/givepass USER_ID 30</code>", parse_mode="HTML")
        return
    uid = parts[1]
    days = parts[2] if len(parts) >= 3 else 30
    p, until = grant_manual_pass(uid, days, message.from_user.id)
    if p is None:
        await message.answer(str(until))
        return
    days_int = max(1, min(365, int(days))) if str(days).isdigit() else 30
    await notify_pass_granted(str(uid), p, days_int, until)
    await message.answer(
        f"✅ Premium-мультипасс выдан игроку <code>{e(uid)}</code> на <b>{days_int}</b> дней.\n"
        f"Срок: <code>{e(until.strftime('%Y-%m-%d %H:%M'))}</code>",
        parse_mode="HTML",
    )


@dp.message(Command("takepass"))
async def takepass_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: <code>/takepass USER_ID</code>", parse_mode="HTML")
        return
    uid = parts[1]
    p, err = take_manual_pass(uid, message.from_user.id)
    if p is None:
        await message.answer(str(err))
        return
    await notify_pass_removed(str(uid))
    await message.answer(f"✅ Premium-мультипасс снят у игрока <code>{e(uid)}</code>.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("admin_givepass:"))
async def admin_givepass_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        _, uid, days_s = callback.data.split(":")
        days = int(days_s)
    except Exception:
        await callback.answer("Ошибка команды.", show_alert=True)
        return
    p, until = grant_manual_pass(uid, days, callback.from_user.id)
    if p is None:
        await callback.answer(str(until), show_alert=True)
        return
    await notify_pass_granted(str(uid), p, days, until)
    await callback.message.answer(
        f"✅ Pass выдан: <code>{e(uid)}</code> · {days} дней · до <code>{e(until.strftime('%Y-%m-%d %H:%M'))}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="👤 Профиль игрока", callback_data=f"admin_user:{uid}")]])
    )
    await callback.answer("Pass выдан.")


@dp.callback_query(F.data.startswith("admin_takepass:"))
async def admin_takepass_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    p, err = take_manual_pass(uid, callback.from_user.id)
    if p is None:
        await callback.answer(str(err), show_alert=True)
        return
    await notify_pass_removed(str(uid))
    await callback.message.answer(
        f"✅ Pass снят: <code>{e(uid)}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="👤 Профиль игрока", callback_data=f"admin_user:{uid}")]])
    )
    await callback.answer("Pass снят.")


PATCH24_COMPENSATION_KEY = "patch24_polish_rpg_2026_05_08"
PATCH24_COMPENSATION_FISTIKS = 4500
PATCH24_COMPENSATION_DRAGONITE = 1
PATCH24_COMPENSATION_ATTEMPTS = 10
PATCH24_COMPENSATION_PASS_XP = 1100

# Backward-compatible names inside older helper code, but all admin text/command is PATCH24.
PATCH23_COMPENSATION_KEY = PATCH24_COMPENSATION_KEY
PATCH23_COMPENSATION_FISTIKS = PATCH24_COMPENSATION_FISTIKS
PATCH23_COMPENSATION_DRAGONITE = PATCH24_COMPENSATION_DRAGONITE
PATCH23_COMPENSATION_ATTEMPTS = PATCH24_COMPENSATION_ATTEMPTS
PATCH23_COMPENSATION_PASS_XP = PATCH24_COMPENSATION_PASS_XP


@dp.message(Command("compensate_patch24"))
async def compensate_patch24_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("⛔ Только владелец.")
        return
    rewarded = sent = skipped_owner = blocked = 0
    for uid, p in list(DATA.get("users", {}).items()):
        if str(uid) in owner_ids():
            skipped_owner += 1
            continue
        ensure_rpg_fields(p)
        comps = p.setdefault("compensations", [])
        # Старые ключи не мешают: новый ключ другой, поэтому компенсацию можно отправить всем заново.
        if PATCH23_COMPENSATION_KEY in comps:
            continue
        p["fistiks"] = int(p.get("fistiks", 0) or 0) + PATCH23_COMPENSATION_FISTIKS
        p["moon_coins"] = int(p.get("moon_coins", 0) or 0) + PATCH23_COMPENSATION_DRAGONITE
        p["card_attempts"] = int(p.get("card_attempts", 0) or 0) + PATCH23_COMPENSATION_ATTEMPTS
        p["pass_xp"] = int(p.get("pass_xp", 0) or 0) + PATCH23_COMPENSATION_PASS_XP
        comps.append(PATCH23_COMPENSATION_KEY)
        rewarded += 1
        try:
            await bot.send_message(
                int(uid),
                "🌌 <b>Компенсация за полировку RPG-обновления</b>\n\n"
                "Мы отполировали меню, вселенные, профиль, магазин, арену, коллекции и добавили Beelzebub. "
                "Спасибо, что дождался обновления.\n\n"
                f"+{PATCH23_COMPENSATION_FISTIKS} 💎 Фисташек\n"
                f"+{PATCH23_COMPENSATION_DRAGONITE} 🐉 Драконит\n"
                f"+{PATCH23_COMPENSATION_ATTEMPTS} 🎴 попыток\n"
                f"+{PATCH23_COMPENSATION_PASS_XP} очков мультипасса",
                parse_mode="HTML",
                reply_markup=back_menu(),
            )
            sent += 1
        except Exception as ex:
            if should_mark_bot_unreachable(ex):
                p["bot_blocked"] = True
                blocked += 1
    mark_data_dirty("data_changed")
    await message.answer(
        "✅ <b>PATCH24 компенсация обработана</b>\n\n"
        f"Начислено: <b>{rewarded}</b>\n"
        f"Сообщений отправлено: <b>{sent}</b>\n"
        f"Owner пропущен: <b>{skipped_owner}</b>\n"
        f"Заблокировали бота: <b>{blocked}</b>",
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "admin_compensation_info")
async def admin_compensation_info_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await callback.message.answer(
        "🎁 <b>Компенсация текущего обновления</b>\n\n"
        "Старые компенсации скрыты из админки, чтобы не мешались.\n\n"
        "Новая команда:\n<code>/compensate_patch24</code>\n\n"
        f"Награда: +{PATCH23_COMPENSATION_FISTIKS} 💎, +{PATCH23_COMPENSATION_DRAGONITE} 🐉, +{PATCH23_COMPENSATION_ATTEMPTS} 🎴 попыток, +{PATCH23_COMPENSATION_PASS_XP} pass XP.\n\n"
        "Ключ новый, поэтому можно отправить всем пользователям заново.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Админ-панель", callback_data="admin")]]),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_purge_blocked")
async def admin_purge_blocked_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    users = DATA.get("users", {}) or {}
    ids = [uid for uid, p in list(users.items()) if str(uid).isdigit() and (p or {}).get("bot_blocked")]
    for uid in ids:
        DATA.setdefault("purged_users", {})[str(uid)] = {
            "reason": "bot_blocked_cleanup", "permanent": False, "purged_at": utc_now().isoformat()
        }
        users.pop(uid, None)
    mark_data_dirty("purge_bot_blocked")
    await callback.message.answer(f"🧹 Технически очищено blocked-записей: <b>{len(ids)}</b>.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Админ-панель", callback_data="admin")]]))
    await callback.answer("Готово")

@dp.callback_query(F.data.startswith("admin_users"))
async def admin_users_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    parts = callback.data.split(":")
    mode = "live"
    page = 0
    if len(parts) > 1:
        if parts[1] in {"live", "all"}:
            mode = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        elif parts[1].isdigit():
            # Совместимость со старыми кнопками admin_users:PAGE.
            page = int(parts[1])
    try:
        await send_admin_users(callback.message, page, mode)
        await callback.answer()
    except Exception as ex:
        logger.exception("admin_users failed: %s", ex)
        await callback.answer("Ошибка списка игроков. Проверь bot_runtime.log.", show_alert=True)
        try:
            await callback.message.answer(f"⚠️ Ошибка списка игроков: <code>{e(str(ex))}</code>", parse_mode="HTML")
        except Exception:
            pass

@dp.callback_query(F.data.startswith("admin_user:"))
async def admin_user_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    await send_admin_user(callback.message, uid)
    await callback.answer()


def parse_uid_from_text(message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    first = parts[1].split()[0].strip()
    return first if first.isdigit() else ""


def parse_two_args(message):
    parts = message.text.split()
    if len(parts) < 3:
        return "", ""
    return parts[1].strip(), parts[2].strip()


@dp.message(Command("user"))
async def user_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid:
        await message.answer("Формат: /user ID")
        return
    await send_admin_user(message, uid)


@dp.message(Command("ban"))
async def ban_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid or uid not in DATA.get("users", {}):
        await message.answer("Формат: /ban ID")
        return
    if uid in owner_ids():
        await message.answer("Владельца нельзя заблокировать.")
        return
    DATA["users"][uid]["banned"] = True
    mark_data_dirty("data_changed")
    await message.answer(f"⛔ Игрок <code>{uid}</code> заблокирован.", parse_mode="HTML")


@dp.message(Command("unban"))
async def unban_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid or uid not in DATA.get("users", {}):
        await message.answer("Формат: /unban ID")
        return
    DATA["users"][uid]["banned"] = False
    mark_data_dirty("data_changed")
    await message.answer(f"✅ Игрок <code>{uid}</code> разблокирован.", parse_mode="HTML")


@dp.message(Command("freeze"))
async def freeze_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid or uid not in DATA.get("users", {}):
        await message.answer("Формат: /freeze ID")
        return
    if uid in owner_ids():
        await message.answer("Владельца нельзя заморозить.")
        return
    DATA["users"][uid]["frozen"] = True
    mark_data_dirty("data_changed")
    await message.answer(f"🧊 Аккаунт <code>{uid}</code> заморожен.", parse_mode="HTML")


@dp.message(Command("unfreeze"))
async def unfreeze_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid or uid not in DATA.get("users", {}):
        await message.answer("Формат: /unfreeze ID")
        return
    DATA["users"][uid]["frozen"] = False
    mark_data_dirty("data_changed")
    await message.answer(f"♨️ Аккаунт <code>{uid}</code> разморожен.", parse_mode="HTML")


async def notify_admin_grant(uid, label, amount):
    try:
        await bot.send_message(
            int(uid),
            f"{CE['owner']} <b>Дар владельца мультивселенной</b>\n\n"
            f"На твой аккаунт начислено: <b>+{amount}</b> {label}.\n"
            "Это знак поддержки за активность. Будет круто, если позовёшь друзей в мультивселенную — вместе рейды ломаются быстрее.",
            parse_mode="HTML"
        )
    except Exception as ex:
        logger.debug("admin grant notify failed for %s: %s", uid, ex)


ADMIN_MAX_FISTIK_GRANT = 1_000_000_000
ADMIN_MAX_DRAGONITE_GRANT = 10_000_000


def _valid_admin_positive_amount(raw, maximum):
    try:
        value = int(str(raw).strip())
    except Exception:
        return None
    if value <= 0 or value > int(maximum):
        return None
    return value


@dp.message(Command("givef"))
async def givef_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid, amount_s = parse_two_args(message)
    amount = _valid_admin_positive_amount(amount_s, ADMIN_MAX_FISTIK_GRANT)
    if not uid or uid not in DATA.get("users", {}) or amount is None:
        await message.answer(f"Формат: /givef ID AMOUNT, где 1 ≤ AMOUNT ≤ {ADMIN_MAX_FISTIK_GRANT}")
        return
    DATA["users"][uid]["fistiks"] = int(DATA["users"][uid].get("fistiks", 0)) + amount
    mark_data_dirty("data_changed")
    await notify_admin_grant(uid, "💎 Фисташек", amount)
    await message.answer(f"💎 Игроку <code>{uid}</code> выдано {amount} фисташек.", parse_mode="HTML")


@dp.message(Command("givemoon"))
async def givemoon_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid, amount_s = parse_two_args(message)
    amount = _valid_admin_positive_amount(amount_s, ADMIN_MAX_DRAGONITE_GRANT)
    if not uid or uid not in DATA.get("users", {}) or amount is None:
        await message.answer(f"Формат: /givemoon ID AMOUNT, где 1 ≤ AMOUNT ≤ {ADMIN_MAX_DRAGONITE_GRANT}")
        return
    DATA["users"][uid]["moon_coins"] = int(DATA["users"][uid].get("moon_coins", 0)) + amount
    mark_data_dirty("data_changed")
    await notify_admin_grant(uid, "🐉 Драконита", amount)
    await message.answer(f"🐉 Игроку <code>{uid}</code> выдано {amount} драконита.", parse_mode="HTML")


@dp.message(Command("givecard"))
async def givecard_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid, card_id = parse_two_args(message)
    if not uid or uid not in DATA.get("users", {}) or card_id not in CARD_BY_ID:
        await message.answer("Формат: /givecard ID CARD_ID")
        return
    result = add_card(DATA["users"][uid], card_id)
    mark_data_dirty("data_changed")
    await message.answer(f"🃏 Игроку <code>{uid}</code>: {e(result)}", parse_mode="HTML")


@dp.message(Command("restoreuser"))
async def restoreuser_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid:
        await message.answer("Формат: /restoreuser ID")
        return
    tomb = DATA.setdefault("deleted_users", {}).get(uid)
    if tomb is None:
        await message.answer("Tombstone этого пользователя не найден.")
        return
    if uid in DATA.setdefault("users", {}):
        await message.answer("Игрок уже существует в актуальной базе.")
        return
    if isinstance(tomb, dict) and isinstance(tomb.get("record"), dict):
        restored = copy.deepcopy(tomb.get("record") or {})
    else:
        # Backward compatibility with PATCH36 tombstones that stored the player directly.
        restored = copy.deepcopy(tomb if isinstance(tomb, dict) else {})
    restored.pop("deleted", None)
    restored["frozen"] = False
    DATA["users"][uid] = restored
    DATA["deleted_users"].pop(uid, None)
    DATA.setdefault("purged_users", {}).pop(uid, None)
    mark_data_dirty("owner_explicit_restore")
    ok = await flush_data_now_async("owner_explicit_restore")
    await message.answer(
        f"✅ Аккаунт <code>{uid}</code> явно восстановлен владельцем." + ("" if ok else " ⚠️ Сохранение поставлено на повтор."),
        parse_mode="HTML",
    )


@dp.message(Command("deleteuser"))
async def deleteuser_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid or uid not in DATA.get("users", {}):
        await message.answer("Формат: /deleteuser ID")
        return
    if uid in owner_ids():
        await message.answer("Владельца нельзя удалить.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🧊 Лучше заморозить", callback_data=f"admin_freeze:{uid}")],
        [button(text="🗑 Да, удалить навсегда", callback_data=f"admin_delete_confirm:{uid}")],
        [button(text="❌ Отмена", callback_data=f"admin_user:{uid}")],
    ])
    await message.answer(f"⚠️ Подтверди действие для <code>{uid}</code>. Удаление стирает игрока из базы.", reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data.startswith("admin_ban:"))
async def admin_ban_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    if uid in owner_ids():
        await callback.answer("Владельца нельзя заблокировать.", show_alert=True)
        return
    if uid in DATA.get("users", {}):
        DATA["users"][uid]["banned"] = True
        mark_data_dirty("data_changed")
    await send_admin_user(callback.message, uid)
    await callback.answer("Игрок заблокирован.")


@dp.callback_query(F.data.startswith("admin_unban:"))
async def admin_unban_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    if uid in DATA.get("users", {}):
        DATA["users"][uid]["banned"] = False
        mark_data_dirty("data_changed")
    await send_admin_user(callback.message, uid)
    await callback.answer("Игрок разблокирован.")


@dp.callback_query(F.data.startswith("admin_freeze:"))
async def admin_freeze_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    if uid in owner_ids():
        await callback.answer("Владельца нельзя заморозить.", show_alert=True)
        return
    if uid in DATA.get("users", {}):
        DATA["users"][uid]["frozen"] = True
        mark_data_dirty("data_changed")
    await send_admin_user(callback.message, uid)
    await callback.answer("Аккаунт заморожен.")


@dp.callback_query(F.data.startswith("admin_unfreeze:"))
async def admin_unfreeze_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    if uid in DATA.get("users", {}):
        DATA["users"][uid]["frozen"] = False
        mark_data_dirty("data_changed")
    await send_admin_user(callback.message, uid)
    await callback.answer("Аккаунт разморожен.")


@dp.callback_query(F.data.startswith("admin_givef:"))
async def admin_givef_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        _, uid, amount_s = callback.data.split(":", 2)
    except ValueError:
        await callback.answer("Некорректная команда.", show_alert=True)
        return
    amount = _valid_admin_positive_amount(amount_s, ADMIN_MAX_FISTIK_GRANT)
    if amount is None or uid not in DATA.get("users", {}):
        await callback.answer("Некорректная сумма или игрок.", show_alert=True)
        return
    DATA["users"][uid]["fistiks"] = int(DATA["users"][uid].get("fistiks", 0)) + amount
    mark_data_dirty("admin_give_fistiks")
    await notify_admin_grant(uid, "💎 Фисташек", amount)
    await send_admin_user(callback.message, uid)
    await callback.answer("Фисташки выданы.")


@dp.callback_query(F.data.startswith("admin_givemoon:"))
async def admin_givemoon_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        _, uid, amount_s = callback.data.split(":", 2)
    except ValueError:
        await callback.answer("Некорректная команда.", show_alert=True)
        return
    amount = _valid_admin_positive_amount(amount_s, ADMIN_MAX_DRAGONITE_GRANT)
    if amount is None or uid not in DATA.get("users", {}):
        await callback.answer("Некорректная сумма или игрок.", show_alert=True)
        return
    DATA["users"][uid]["moon_coins"] = int(DATA["users"][uid].get("moon_coins", 0)) + amount
    mark_data_dirty("admin_give_dragonite")
    await notify_admin_grant(uid, "🐉 Драконита", amount)
    await send_admin_user(callback.message, uid)
    await callback.answer("Драконит выдан.")


@dp.callback_query(F.data.startswith("admin_delete_ask:"))
async def admin_delete_ask_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🧊 Заморозить вместо удаления", callback_data=f"admin_freeze:{uid}")],
        [button(text="🗑 Да, удалить навсегда", callback_data=f"admin_delete_confirm:{uid}")],
        [button(text="❌ Отмена", callback_data=f"admin_user:{uid}")],
    ])
    await callback.message.answer(f"⚠️ Точно удалить <code>{uid}</code>? Без подтверждения удаление не выполняется.", reply_markup=kb, parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_delete_confirm:"))
async def admin_delete_confirm_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    if uid in owner_ids():
        await callback.answer("Владельца нельзя удалить.", show_alert=True)
        return
    player = DATA.get("users", {}).pop(uid, None)
    if player is not None:
        DATA.setdefault("deleted_users", {})[uid] = {
            "permanent": True,
            "deleted_at": utc_now().isoformat(),
            "deleted_by": str(callback.from_user.id),
            "record": copy.deepcopy(player),
        }
    else:
        DATA.setdefault("deleted_users", {}).setdefault(uid, {
            "permanent": True, "deleted_at": utc_now().isoformat(), "deleted_by": str(callback.from_user.id), "record": {}
        })
    DATA.setdefault("purged_users", {}).pop(uid, None)
    mark_data_dirty("permanent_user_delete")
    await flush_data_now_async("permanent_user_delete")
    await callback.message.answer(f"🗑 Аккаунт <code>{uid}</code> удалён навсегда и закрыт tombstone. Автовосстановление и повторная регистрация заблокированы.", reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer("Удалено навсегда.")


@dp.callback_query(F.data == "noop")
async def noop(callback: types.CallbackQuery):
    await callback.answer()


@dp.message(F.text.in_({"🎴 Призвать", "Призвать", "🎴 Призыв", "Призыв", "🎴 Получить карту", "Получить карту"}))
async def quick_draw_button(message: types.Message):
    await draw_card_to_message(message, message.from_user)


@dp.message(F.text.in_({"🃏 Коллекция", "Коллекция", "🃏 Мои карты", "Мои карты", "Моя коллекция"}))
async def quick_collection_button(message: types.Message):
    await send_collection_home(message, message.from_user)


@dp.message(F.text.in_({"🎮 Играть", "Играть", "⚔️ Битвы", "Битвы", "⚔ Битвы"}))
async def quick_battles_button(message: types.Message):
    await send_modes(message, message.from_user)


@dp.message(F.text.in_({"🏠 Меню", "Меню", "Главное меню"}))
async def quick_menu_button(message: types.Message):
    get_user_data(message.from_user)
    await send_main_dashboard(message, message.from_user, show_banner=False)


@dp.message(F.text.in_({"🎟 MultiPass", "MultiPass", "🎟 Мультипасс", "Мультипасс", "🎟 MetaPass", "MetaPass", "Метапасс", "Мультипас"}))
async def quick_metapass_button(message: types.Message):
    await send_multipass(message, message.from_user)


@dp.message(Command("keyboard"))
async def keyboard_cmd(message: types.Message):
    await message.answer("✅ Клавиатура обновлена.", reply_markup=quick_reply_menu(message.from_user.id))


@dp.message()
async def unknown(message: types.Message):
    await message.answer("Не понял команду. Нажми 🏠 Меню или используй /commands.", reply_markup=quick_reply_menu(message.from_user.id))


async def free_pack_notifier():
    # Раз в 3 часа напоминает только тем пользователям, у кого сундук реально доступен.
    await asyncio.sleep(30)
    while True:
        try:
            now = utc_now()
            changed = False
            for uid, player in list(DATA.get("users", {}).items()):
                if not player.get("notify_free_pack", True) or player.get("banned") or player.get("frozen"):
                    continue

                last_pack = player.get("last_free_pack", "")
                if last_pack:
                    try:
                        last_pack_dt = _parse_iso_datetime(last_pack)
                        if last_pack_dt and now < last_pack_dt + timedelta(hours=3):
                            continue
                    except Exception:
                        pass

                last_notice = player.get("last_free_notice", "")
                if last_notice:
                    try:
                        last_notice_dt = _parse_iso_datetime(last_notice)
                        if last_notice_dt and now < last_notice_dt + timedelta(hours=3):
                            continue
                    except Exception:
                        pass

                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [button(text="🎴 Сделать бесплатный призыв", callback_data="pack_info:free")]
                ])
                try:
                    await bot.send_message(
                        int(uid),
                        "🎴 Бесплатный призыв снова доступен. Забери карту и усили коллекцию.",
                        reply_markup=kb
                    )
                    player["last_free_notice"] = now.isoformat()
                    player["free_pack_notified"] = True
                    changed = True
                except Exception as ex:
                    logger.debug("Free pack notice failed for %s: %s", uid, ex)
                    if should_mark_bot_unreachable(ex):
                        player["bot_blocked"] = True
                        changed = True
            if changed:
                mark_data_dirty("data_changed")
        except Exception as ex:
            logger.exception("free_pack_notifier failed: %s", ex)
        await asyncio.sleep(3 * 60 * 60)


async def luffy_path_notifier():
    """Раз в день около 10:00 APP_TIMEZONE напоминает активным игрокам забрать день Пути Луфи."""
    notify_hour = int(os.environ.get("LUFFY_NOTIFY_HOUR", "10") or 10)
    while True:
        try:
            now = app_now()
            today = now.date().isoformat()
            if now.hour == notify_hour:
                changed = False
                for uid, player in list(DATA.get("users", {}).items()):
                    try:
                        if player.get("deleted") or player.get("bot_blocked"):
                            continue
                        if player.get("luffy_finished") or int(player.get("luffy_day", 0) or 0) >= len(LUFFY_PATH_CARDS):
                            continue
                        if player.get("luffy_daily_notice") == today:
                            continue
                        await bot.send_message(
                            int(uid),
                            f"{CE['luffy']} <b>Путь Луфи ждёт</b>\n\nЗайди и забери форму дня. 10 дней подряд — финальная награда.",
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [button(text="Открыть Путь Луфи", callback_data="luffy_path")],
                                [button(text="Меню", callback_data="menu")],
                            ]),
                            parse_mode="HTML",
                        )
                        player["luffy_daily_notice"] = today
                        changed = True
                    except Exception as ex:
                        logger.debug("Luffy daily notice failed for %s: %s", uid, ex)
                        if should_mark_bot_unreachable(ex):
                            player["bot_blocked"] = True
                            changed = True
                if changed:
                    mark_data_dirty("data_changed")
            await asyncio.sleep(15 * 60)
        except Exception as ex:
            logger.exception("luffy_path_notifier failed: %s", ex)
            await asyncio.sleep(15 * 60)


async def runtime_state_cleanup_worker():
    last_purge = 0.0
    while True:
        try:
            now = time.time()
            for uid, state in list(active_battles.items()):
                created = float((state or {}).get("created_at_ts", now) or now)
                if (state or {}).get("resolved") or now - created > 2 * 60 * 60:
                    active_battles.pop(uid, None)
            for bid, state in list(active_pvp.items()):
                created = float((state or {}).get("created_at_ts", now) or now)
                if (state or {}).get("resolved") or now - created > 2 * 60 * 60:
                    active_pvp.pop(bid, None)
            for uid, draft in list(manual_team_drafts.items()):
                updated = float((draft or {}).get("updated_at_ts", 0) or 0)
                if updated and now - updated > 45 * 60:
                    manual_team_drafts.pop(uid, None)
            cleanup_online_queue()
            if now - last_purge >= AUTO_PURGE_INTERVAL_SECONDS:
                await auto_purge_stale_users()
                last_purge = now
        except Exception as ex:
            logger.exception("runtime_state_cleanup_worker failed: %s", ex)
        await asyncio.sleep(10 * 60)



async def health_handler(request):
    return web.Response(text="Anime Battle bot is running")


async def start_health_server():
    raw_port = os.environ.get("PORT", "10000")
    try:
        port = int(raw_port)
    except Exception:
        logger.warning("Invalid PORT=%r; using 10000", raw_port)
        port = 10000
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info("WEB HEALTH SERVER STARTED ON PORT %s", port)
        return runner
    except OSError as ex:
        # A duplicate health server must not kill Telegram polling. Render still gets a clear log.
        logger.error("Health server could not bind PORT %s: %s", port, ex)
        try:
            await runner.cleanup()
        except Exception:
            pass
        return None
    except Exception as ex:
        logger.exception("Health server startup failed: %s", ex)
        try:
            await runner.cleanup()
        except Exception:
            pass
        return None


async def main():
    print("BOT STARTED. Do not close this window.")
    ensure_media_packs_extracted()
    ensure_generated_arena_media()
    repair_all_luffy_progress()
    if _DATA_DIRTY:
        await flush_data_now_async("startup_repair")
    health_runner = await start_health_server()
    await set_commands()
    await set_bot_public_description()
    if DATABASE_URL:
        ledger_ok = await asyncio.to_thread(_ensure_payment_ledger_table_sync)
        if not ledger_ok:
            raise RuntimeError("Neon доступен для DATA, но таблица платёжного ledger не инициализировалась. Polling не запущен.")
    await recover_pending_payments()
    await bot.delete_webhook(drop_pending_updates=False)
    asyncio.create_task(free_pack_notifier(), name="free-pack-notifier")
    asyncio.create_task(luffy_path_notifier(), name="luffy-path-notifier")
    asyncio.create_task(payment_recovery_worker(), name="payment-recovery-worker")
    asyncio.create_task(runtime_state_cleanup_worker(), name="runtime-state-cleanup")
    try:
        await dp.start_polling(bot)
    finally:
        await flush_data_now_async("shutdown_flush")
        if health_runner is not None:
            try:
                await health_runner.cleanup()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())

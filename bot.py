
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
from datetime import datetime, date, timedelta
from pathlib import Path
from html import escape

from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, FSInputFile, LabeledPrice, BotCommandScopeDefault, BotCommandScopeChat
from aiohttp import web

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:
    Image = ImageDraw = ImageFont = None


BASE_DIR = Path(__file__).resolve().parent
# Важно для Render: обычные файлы проекта могут исчезать после redeploy/restart.
# Для нормального сохранения прогресса создай Persistent Disk и поставь DATA_DIR=/var/data.
DATA_DIR = Path(os.getenv("DATA_DIR") or os.getenv("BOT_DATA_DIR") or os.getenv("RENDER_DATA_DIR") or ".")
if not DATA_DIR.is_absolute():
    DATA_DIR = (BASE_DIR / DATA_DIR).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

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


def _safe_extract_card_member(zip_obj, member):
    try:
        if member.is_dir():
            return False
        name = Path(member.filename).name
        if not name or name.startswith("."):
            return False
        if Path(name).suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
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
        existing = [p for p in MEDIA_CARDS_DIR.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}]
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
                        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
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


ensure_media_packs_extracted()
ONLINE_QUEUE_TTL_SECONDS = 5 * 60
PAYMENT_CURRENCY = "XTR"


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
FRIEND_ID_DEFAULT = "527802531"
CHOICE_TIMEOUT_SECONDS = 20
CHOICE_WARN_10_AFTER = 10
CHOICE_WARN_5_AFTER = 15
PASS_PRICE_STARS = 199
MOON_EMOJI = "🐉"
CASE_PRICES = {"event": 3, "holiday": 5, "mystic": 12}
PITY_LIMITS = {"epic": 10, "legendary": 50, "mythic": 150}
CASE_NAMES = {"event": "Ивент-кейс", "holiday": "Праздничный кейс", "mystic": "Мифический кейс"}


# PATCH15.2: Telegram custom emoji.
# HTML custom emoji are used in messages; button custom emoji use icon_custom_emoji_id.
CUSTOM_EMOJI_IDS = {
    # Главные разделы
    "start": "5215377245639549895",
    "profile": "6012666146648495705",
    "modes": "5408935401442267103",
    "collection": "5469741319330996757",
    "rewards": "5188344996356448758",
    "rules": "5334882760735598374",
    "luffy": "6057663582705814959",
    "dragonite": "5258112758645282249",
    "pistachios": "5330236782942379682",
    "owner": "5467406098367521267",

    # PATCH15.5: точные custom emoji для внутренних вкладок.
    "arena": "5454014806950429357",
    "online": "5447410659077661506",
    "deck": "5217849987160889755",
    "events": "5188497854242495901",
    "raid": "5372951839018850336",
    "battle_choice": "5449820402018688838",
    "menu": "5440735760208637835",

    # Награды
    "free_chest": "5364112491381006601",
    "daily_reward": "5350460637182993292",
    "chests": "5199475623147375954",
    "multipass": "5462902520215002477",
    "rating": "5280735858926822987",

    # Редкости
    "origin": "5339113303522161846",
    "rare": "5339513551524481000",
    "epic": "5339146671123087992",
    "legendary": "5339082633160703625",
    "absolute": "5352792306208480366",
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
}

def ce(name):
    """HTML custom emoji with a normal emoji fallback."""
    emoji_id = CUSTOM_EMOJI_IDS.get(name)
    fallback = CUSTOM_EMOJI_FALLBACKS.get(name, "")
    if not emoji_id or not fallback:
        return fallback
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

CE = {key: ce(key) for key in CUSTOM_EMOJI_IDS}
PISTACHIOS_LABEL = f'{CE["pistachios"]} Фисташки'
DRAGONITE_LABEL = f'{CE["dragonite"]} Драконит'
OWNER_LABEL = f'{CE["owner"]} Владелец мультивселенной'
CURRENCY_TITLE = DRAGONITE_LABEL
PROJECT_HOOK = "Собери свою аниме-команду и докажи, что твоя мультивселенная сильнее."

BOT_SHORT_DESCRIPTION = "Anime Battle Multiverse — аниме-карты, рейды, арены, PvP и RPG-бои разных вселенных."
BOT_PUBLIC_DESCRIPTION = (
    "🌌 Anime Battle Multiverse\n"
    "RPG-бот про аниме-карты, арены, рейды и споры о силе персонажей.\n\n"
    "Собирай героев из разных аниме, усиливай колоду, открывай сундуки, проходи Путь Луфи, бей недельного босса и проверяй, чья мультивселенная сильнее.\n\n"
    "Здесь встречаются шиноби, пираты, саяны, проклятия, титаны, демоны, покемоны, боги и персонажи уровня Absolute."
)

PATCH16_COMPENSATION_KEY = "patch16_multiverse_visual_rules_raid_2026_05"
PATCH16_COMPENSATION_FISTIKS = 1500
PATCH16_COMPENSATION_MOON_COINS = 12
PATCH16_COMPENSATION_PASS_XP = 450


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

LEADING_BUTTON_EMOJI_PREFIXES = (
    "🌌", "⚔️", "⚔", "🃏", "🎁", "👤", "📜", "🔥", "🐉", "💎", "👑",
    "🆓", "🧰", "🎟", "🏆", "⭐", "🎴", "🏷", "📦", "🧠", "🔁", "⚡",
    "🔔", "🔕", "👥", "✏️", "✏", "📊", "⬅️", "⬅", "➡️", "➡", "🏠",
    "💪", "⬆️", "⬆", "🔤", "📚", "⚪", "🔷", "🔵", "🟣", "🟡", "🔴",
    "✅", "🎲", "🛡️", "🛡", "🤖", "🌐", "🎪", "👹", "⚙️", "⚙", "❌",
    "🔗", "🎯", "💳", "☠️", "☠", "🧊", "♨️", "♨", "🆔", "🗑", "🔄",
)

def _button_icon_key(text: str) -> str | None:
    # Exact matching only. Keyword matching made all nested buttons use the same icon.
    t = " ".join(str(text or "").casefold().split())
    return BUTTON_EXACT_ICON_KEYS.get(t)

def _strip_leading_button_emoji(text: str) -> str:
    t = str(text or "")
    for prefix in sorted(LEADING_BUTTON_EMOJI_PREFIXES, key=len, reverse=True):
        if t.startswith(prefix):
            return t[len(prefix):].lstrip()
    return t

def button(*args, **kwargs) -> InlineKeyboardButton:
    """Inline button with safe custom emoji icon where Telegram supports it."""
    text = kwargs.get("text")
    args = list(args)
    if text is None and args:
        text = args[0]
    key = _button_icon_key(text) if isinstance(text, str) else None
    if key and not kwargs.get("icon_custom_emoji_id"):
        icon_id = CUSTOM_EMOJI_IDS.get(key)
        if icon_id:
            kwargs["icon_custom_emoji_id"] = icon_id
            clean = _strip_leading_button_emoji(text)
            if "text" in kwargs:
                kwargs["text"] = clean
            elif args:
                args[0] = clean
    return InlineKeyboardButton(*args, **kwargs)

PATCH11_COMPENSATION_KEY = "patch11_progress_apology_2026_04"
PATCH15_COMPENSATION_KEY = "patch15_final_stability_2026_05"
COMPENSATION_FISTIKS = 2000
COMPENSATION_MOON_COINS = 20
COMPENSATION_PASS_XP = 600

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
    "shop": (f"{CE['rewards']} Награды", "Сундуки, ежедневные награды, мультипасс и редкие покупки."),
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
        "title": "Эпический старт",
        "price": 99,
        "desc": "Гарантированная эпическая карта, 3000 💎 и 2 🐉. Для быстрого старта без слома баланса.",
        "rarity": "Эпический",
        "fistiks": 3000,
        "moon_coins": 2,
        "badge": "EPIC_BOOSTER",
    },
    "legendary_rank": {
        "title": "Легендарный ранг",
        "price": 249,
        "desc": "Гарантированная легендарная карта, 7000 💎, 7 🐉 и профильный знак.",
        "rarity": "Легендарный",
        "fistiks": 7000,
        "moon_coins": 7,
        "badge": "LEGEND_RANK",
    },
    "mythic_ticket": {
        "title": "Мифический билет сезона",
        "price": 499,
        "desc": "Гарантированная мифическая карта сезона, 15000 💎, 15 🐉 и редкий знак.",
        "rarity": "Мифический",
        "fistiks": 15000,
        "moon_coins": 15,
        "badge": "MYTHIC_TICKET",
    },
}

REF_MILESTONES = {
    1: {"fistiks": 500, "pass_xp": 120, "moon_coins": 0, "title": "первый союзник"},
    3: {"fistiks": 1200, "pass_xp": 250, "moon_coins": 2, "title": "малый отряд"},
    5: {"fistiks": 2500, "pass_xp": 450, "moon_coins": 4, "title": "команда мультивселенной", "badge": "REF_5"},
    10: {"fistiks": 6000, "pass_xp": 900, "moon_coins": 10, "title": "лидер союза", "badge": "REF_10"},
    25: {"fistiks": 18000, "pass_xp": 1800, "moon_coins": 25, "title": "магнит мультивселенной", "badge": "REF_25"},
}

RAID_HIT_COOLDOWN_MINUTES = 300
RAID_HIT_LIMIT_PER_WINDOW = 3
RAID_DURATION_DAYS = 7

DAILY_EVENT_POOL = [
    {"name": "День шиноби", "desc": "Сыграй бой или открой сундук: сегодня энергия скрытых деревень усиливает прогресс.", "coins": 2, "pass_xp": 140},
    {"name": "Проклятая волна", "desc": "Проклятая энергия нестабильна: ежедневная активность даёт усиленную награду.", "coins": 3, "pass_xp": 120},
    {"name": "Пиратский прилив", "desc": "Команды с духом приключений получают бонус к сезонному прогрессу.", "coins": 2, "pass_xp": 180},
    {"name": "Духовный разлом", "desc": "Открыт разлом духовной энергии. Забери награду дня до смены события.", "coins": 4, "pass_xp": 100},
    {"name": "Турнир измерений", "desc": "Мультивселенная ждёт активности: зайди, забери бонус и готовь колоду.", "coins": 2, "pass_xp": 200},
]

RAID_BOSSES = [
    {
        "id": "raid_shibai_otsutsuki",
        "name": "Шибай Оцуцуки — Бог эволюции",
        "hp": 650_000_000,
        "desc": "Недельный абсолютный босс. Его тело почти вышло за границы обычной силы, поэтому один герой не решает бой — нужен общий урон всех игроков.",
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
    "ruins": ("➕ тактики, ловушки, мобильность", "➖ тупой лобовой rush без контроля"),
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

RARITY_WEIGHTS = {
    # Жёсткий дроп: легендарные и мифические выпадают заметно реже.
    "Обычный": 850,
    "Редкий": 120,
    "Эпический": 25,
    "Легендарный": 4,
    "Мифический": 1,
}

FREE_PACK_WEIGHTS = {
    "Обычный": 88,
    "Редкий": 11,
    "Эпический": 1,
    "Легендарный": 0,
    "Мифический": 0,
}

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
    "mystic": {"Обычный": 0, "Редкий": 360, "Эпический": 460, "Легендарный": 165, "Мифический": 15},
    "event": {"Обычный": 470, "Редкий": 350, "Эпический": 150, "Легендарный": 25, "Мифический": 5},
    "holiday": {"Обычный": 620, "Редкий": 270, "Эпический": 95, "Легендарный": 13, "Мифический": 2},
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
    "Обычный": "⚪ Origin",
    "Редкий": "🔷 Rare",
    "Эпический": "🟣 Epic",
    "Легендарный": "🟡 Legendary",
    "Мифический": "🔴 Absolute",
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
        "weights": RARITY_WEIGHTS,
        "description": "Базовый сундук для набора коллекции. Подходит новичкам.",
    },
    "rare": {
        "name": "Усиленный сундук",
        "base_cost": 1500,
        "count": 5,
        "weights": RARE_PACK_WEIGHTS,
        "description": "Сундук с повышенным шансом редких, эпических и выше.",
    },
    "royal": {
        "name": "Королевский сундук",
        "base_cost": 3000,
        "count": 6,
        "weights": {"Обычный": 600, "Редкий": 250, "Эпический": 125, "Легендарный": 20, "Мифический": 5},
        "description": "Дорогой сундук с высоким шансом эпических и легендарных карт.",
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

ARTIFACT_BY_ID = {a["id"]: a for a in ARTIFACTS}

active_battles = {}
active_pvp = {}
manual_team_drafts = {}
choice_timers = {}
# items: {"uid": str, "joined_at": iso}; legacy str items are also accepted.
online_queue = []


def e(text):
    return escape(str(text), quote=False)


def record_user_action(user, action):
    """Короткий журнал действий для админки. Не хранит сообщения целиком, только безопасную выжимку."""
    if not user:
        return
    try:
        p = get_user_data(user)
        item = {
            "at": datetime.now().isoformat(timespec="seconds"),
            "action": str(action)[:120],
        }
        actions = p.setdefault("last_actions", [])
        actions.append(item)
        if len(actions) > 30:
            del actions[:-30]
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
            ("data", json.dumps(obj, ensure_ascii=False), datetime.now().isoformat()),
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


def save_json(path, obj):
    """Атомарная запись JSON + backup, чтобы прогресс не ломался при падении процесса."""
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    tmp_name = None
    try:
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
    finally:
        if tmp_name and Path(tmp_name).exists():
            try:
                Path(tmp_name).unlink()
            except Exception:
                pass
    if _is_data_json_path(path_obj):
        _save_data_sqlite(obj)


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
        return b if datetime.fromisoformat(str(b)) > datetime.fromisoformat(str(a)) else a
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


def load_data_storage(default):
    candidates = []
    candidates.extend(list(_data_sqlite_candidates()))
    candidates.extend(list(_data_json_candidates(default)))
    data = _clone_default(default)
    for cand in sorted(candidates, key=_data_score):
        data = _merge_users_data(data, cand)
    data.setdefault("users", {})
    data.setdefault("friend_invites", {})
    data.setdefault("friend_requests", {})
    data.setdefault("friends", {})
    data.setdefault("deleted_users", {})
    save_json(DATA_FILE, data)
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
    warning = "✅ DATA_DIR настроен правильно." if is_var_data else "⚠️ DATA_DIR не /var/data. На Render это риск сброса прогресса после redeploy."
    unknown_cards = 0
    for player in users.values():
        for cid in (player or {}).get("collection", {}) or {}:
            if cid not in CARD_BY_ID:
                unknown_cards += 1
    return (
        "🧠 <b>Хранилище прогресса</b>\n\n"
        f"DATA_DIR: <code>{e(DATA_DIR)}</code>\n"
        f"DATA_FILE: <code>{e(data_json)}</code> — {'есть' if data_json.exists() else 'нет'}\n"
        f"DB_FILE: <code>{e(data_db)}</code> — {'есть' if data_db.exists() else 'нет'}\n"
        f"LOG_FILE: <code>{e(LOG_FILE)}</code>\n"
        f"/var/data: <b>{'есть' if Path('/var/data').exists() else 'нет'}</b>\n\n"
        f"Игроков в DATA: <b>{len(users)}</b>\n"
        f"Игроков в JSON: <b>{_count_json_users(data_json)}</b>\n"
        f"Игроков в DB: <b>{_count_db_users(data_db)}</b>\n"
        f"Unknown card count: <b>{unknown_cards}</b>\n"
        f"Последнее сохранение: <b>{e(last_save)}</b>\n\n"
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
    if not Path(RIGHT_HAND_FILE).exists():
        Path(RIGHT_HAND_FILE).write_text(FRIEND_ID_DEFAULT, encoding="utf-8")
    if not Path(PROMO_FILE).exists():
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


def right_hand_ids():
    return read_ids(RIGHT_HAND_FILE) | _ids_from_env("RIGHT_HAND_ID", "RIGHT_HAND_IDS")


def is_owner(user_id):
    return str(user_id) in owner_ids()


def is_right_hand(user_id):
    return str(user_id) in right_hand_ids() and not is_owner(user_id)


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
        c["stats"] = stats
        cards.append(c)
    return cards


ensure_files()
TOKEN = read_token()
CARDS = load_cards()
CARD_BY_ID = {c["id"]: c for c in CARDS}
DATA = load_data_storage({"users": {}, "friend_invites": {}, "friends": {}})

bot = Bot(token=TOKEN)
dp = Dispatcher()


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
                    "menu", "profile", "profile_stats", "profile_badges", "modes", "shop",
                    "chests", "rules", "multipass", "deck", "pvp_source_menu", "newbie_start",
                    "battle:start", "online_search", "cases", "events",
                }
                delete_prefixes = (
                    "pack_info:", "collection:page:", "card:", "battle:arena:", "battle:arena_page:", "battle:diff:",
                    "pvp_source:",
                )
                if not data_value.startswith(keep_prefixes) and (
                    data_value in delete_exact or data_value.startswith(delete_prefixes)
                ):
                    await event.message.delete()
        except Exception as ex:
            logger.debug("Auto-clean failed: %s", ex)
        return await handler(event, data)


dp.callback_query.middleware(AutoCleanCallbackMiddleware())


class UserTouchMiddleware(BaseMiddleware):
    """Фиксирует любого пользователя, который пишет боту или нажимает кнопки, чтобы админка видела всех."""
    async def __call__(self, handler, event, data):
        try:
            user = getattr(event, "from_user", None)
            if user:
                get_user_data(user)
                if isinstance(event, types.CallbackQuery):
                    record_user_action(user, "button:" + str(event.data or "")[:80])
                elif isinstance(event, types.Message):
                    text = (event.text or event.caption or "").strip()
                    record_user_action(user, "message:" + (text[:80] if text else "<no text>"))
        except Exception as ex:
            logger.debug("User touch failed: %s", ex)
        return await handler(event, data)


dp.message.middleware(UserTouchMiddleware())
dp.callback_query.middleware(UserTouchMiddleware())


def is_user_banned_id(user_id):
    if is_owner(user_id):
        return False
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


RARITY_CE_KEYS = {
    "Обычный": "origin",
    "Редкий": "rare",
    "Эпический": "epic",
    "Легендарный": "legendary",
    "Мифический": "absolute",
}
RARITY_NAMES_PUBLIC = {
    "Обычный": "Origin",
    "Редкий": "Rare",
    "Эпический": "Epic",
    "Легендарный": "Legendary",
    "Мифический": "Absolute",
}

def rarity_label(rarity):
    key = RARITY_CE_KEYS.get(rarity)
    name = RARITY_NAMES_PUBLIC.get(rarity, rarity)
    if key and key in CE:
        return f"{CE[key]} {name}"
    return RARITY_DISPLAY.get(rarity, f"⚪ {rarity}")

def badge_title(code):
    return BADGE_TITLES.get(code, code.replace("_", " ").title())


def visible_badges(badges):
    return ", ".join(badge_title(b) for b in badges) if badges else "нет"


def is_public_ranked(uid):
    # Владелец скрыт из рейтингов. Правая рука может участвовать.
    return not is_owner(uid)


def is_online(uid):
    player = DATA.get("users", {}).get(str(uid), {})
    last = player.get("last_seen", "")
    if not last:
        return False
    try:
        return datetime.now() - datetime.fromisoformat(last) <= timedelta(minutes=10)
    except Exception:
        return False



def _parse_iso_datetime(value):
    try:
        if not value:
            return None
        return datetime.fromisoformat(str(value))
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


def all_player_items(include_deleted=False):
    """Все реальные записи игроков для админки. Не скрывает старые записи без last_seen."""
    items = []
    for uid, player in (DATA.get("users", {}) or {}).items():
        uid = str(uid)
        player = player or {}
        if not uid.isdigit():
            continue
        if not include_deleted and player.get("deleted"):
            continue
        items.append((uid, player))
    return items


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
        save_json(DATA_FILE, DATA)
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
        item.setdefault("unlocked", item.get("count", 0) > 0)
        if cid not in CARD_BY_ID:
            item["unknown"] = True
            if cid not in unknown:
                unknown.append(cid)
            # PATCH15: неизвестную карту НЕ удаляем, чтобы не терять прогресс старых игроков.
            continue
        item.pop("unknown", None)
        try:
            if int(item["level"]) > MAX_LEVEL:
                item["level"] = MAX_LEVEL
            if int(item["level"]) < 1:
                item["level"] = 1
        except Exception:
            item["level"] = 1
    if len(unknown) > 500:
        del unknown[:-500]

def get_user_data(user):
    uid = str(user.id)
    now_iso = datetime.now().isoformat()
    if uid not in DATA.setdefault("users", {}):
        DATA["users"][uid] = {
            "name": user.full_name,
            "username": user.username or "",
            "fistiks": 250,
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
        }
        starter_ids = ["levi_peak", "mikasa", "kirito_alicization", "gon_base", "tanjiro_sun"]
        for sid in starter_ids:
            if sid in CARD_BY_ID:
                DATA["users"][uid]["collection"][sid] = {"count": 1, "shards": 0, "level": 1, "unlocked": True}

    player = DATA["users"][uid]
    if "fistiks" not in player:
        player["fistiks"] = player.get("coins", 250)
    defaults = {
        "xp": 0, "badges": [], "premium": False, "used_promos": [], "last_daily": "",
        "last_free_pack": "", "free_pack_notified": False, "last_free_notice": "",
        "ref_by": "", "ref_count": 0, "ref_earned": 0, "nickname": "",
        "wins": 0, "losses": 0, "battles": 0, "last_seen": "",
        "pass_xp": 0, "pass_premium": False, "claimed_pass_free": [], "claimed_pass_premium": [],
        "stars_earned": 0, "moon_coins": 0, "pity_counters": {"epic": 0, "legendary": 0, "mythic": 0},
        "notify_free_pack": True, "banned": False, "frozen": False, "pass_premium_cap": 0,
        "deck": [], "auto_team": True, "pass_daily_date": "", "pass_task_progress": {},
        "pass_task_claimed": [], "pass_purchase_request": "", "created_at": now_iso,
        "newbie_claimed": [], "newbie_progress": {}, "pvp_team_source": "deck",
        "ref_milestones_claimed": [], "support_tickets": [], "purchases": [], "processed_payments": [], "battle_history": [],
        "last_actions": [], "system_inbox": [], "luffy_day": 0, "last_luffy_claim": "", "luffy_claimed_cards": [],
        "luffy_intro_seen": False, "luffy_finished": False,
        "compensations": [], "username": user.username or "",
    }
    for k, v in defaults.items():
        player.setdefault(k, v)
    player.setdefault("collection", {})
    player.setdefault("pity_counters", {"epic": 0, "legendary": 0, "mythic": 0})

    player["name"] = player.get("nickname") or user.full_name
    player["username"] = user.username or player.get("username", "")
    player["last_seen"] = now_iso
    repair_luffy_progress(player)
    normalize_collection(player)

    if is_owner(user.id):
        player["fistiks"] = 999999999
        player["wins"] = max(player.get("wins", 0), 9999)
        player["losses"] = 0
        player["battles"] = max(player.get("battles", 0), 9999)
        player["xp"] = max(player.get("xp", 0), 99999999)
        player["premium"] = True
        player["pass_premium"] = True
        player["pass_premium_cap"] = 100
        player["pass_xp"] = max(int(player.get("pass_xp", 0)), 999999)
        player["moon_coins"] = 999999999
        player["banned"] = False
        player["frozen"] = False
        player["creator_role"] = "👑 Владелец мультивселенной"
        player["creator_aura"] = "♾ Абсолютный знак создателя"
        for badge in ["DEV", "ROMA_OWNER", "IT_ARCHITECT", "ABSOLUTE_MAX"]:
            if badge not in player["badges"]:
                player["badges"].append(badge)
        for cid in CARD_BY_ID:
            player["collection"][cid] = {"count": 1, "shards": 999999, "level": MAX_LEVEL, "unlocked": True}
    elif is_right_hand(user.id):
        # Правая рука получает только роль/знак. Баланс, уровень, бои и карты больше не накручиваются автоматически.
        if "RIGHT_HAND" not in player["badges"]:
            player["badges"].append("RIGHT_HAND")
    save_json(DATA_FILE, DATA)
    return player

def main_menu(user_id=None):
    rows = [
        [button(text="⚔️ Режимы", callback_data="modes"), button(text="🃏 Коллекция", callback_data="collection:page:0")],
        [button(text="🎁 Награды", callback_data="shop"), button(text="👤 Профиль", callback_data="profile")],
        [button(text="📜 Правила", callback_data="rules")],
    ]
    if user_id:
        player = DATA.get("users", {}).get(str(user_id), {})
        if not player.get("luffy_finished") and int(player.get("luffy_day", 0) or 0) < len(LUFFY_PATH_CARDS):
            rows.insert(2, [button(text="🔥 Путь Луфи", callback_data="luffy_path")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu_text(user=None):
    return (
        f"{CE['start']} <b>Anime Battle Multiverse</b>\n"
        "Выбери свой путь.\n\n"
        "Собирай героев из разных аниме, усиливай колоду и проверяй силу персонажей в арене, PvP и рейдах. "
        "Если спор о том, кто сильнее, заходит слишком далеко — решай его в мультивселенной.\n\n"
        f"{CE['free_chest']} <b>Новичку:</b> бесплатный сундук каждые 3 часа.\n"
        f"{CE['daily_reward']} <b>Каждый день:</b> награда, опыт пропуска и шанс на фрагменты.\n"
        f"{CE['luffy']} <b>Путь Луфи:</b> 10 дней — от первой формы до Gear 5 / Sun God Nika."
    )
async def maybe_send_luffy_intro(message, user, force=False):
    """Показывает вступление Пути Луфи один раз за аккаунт. Прогресс не трогает."""
    p = get_user_data(user)
    repair_luffy_progress(p)
    if p.get("luffy_finished") or int(p.get("luffy_day", 0) or 0) >= len(LUFFY_PATH_CARDS):
        p["luffy_finished"] = True
        save_json(DATA_FILE, DATA)
        return
    if not force and (p.get("luffy_intro_seen") or p.get("last_luffy_intro")):
        return
    today = date.today().isoformat()
    p["luffy_intro_seen"] = True
    p["last_luffy_intro"] = today
    save_json(DATA_FILE, DATA)
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
        [button(text="⬅️ Назад в меню", callback_data="menu")]
    ])


def profile_menu(user_id=None):
    rows = [
        [button(text="📊 Статистика", callback_data="profile_stats"), button(text="🏷 Знаки", callback_data="profile_badges")],
        [button(text="👥 Друзья", callback_data="friends"), button(text="✏️ Ник", callback_data="nick_help")],
        [button(text="🔔 Уведомления", callback_data="notify_settings")],
    ]
    if user_id and is_owner(user_id):
        rows.append([button(text="🛠 Админ-панель", callback_data="admin")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def shop_menu():
    rows = [
        [button(text="🆓 Бесплатный сундук", callback_data="pack_info:free")],
        [button(text="🎁 Ежедневная награда", callback_data="daily")],
        [button(text="🧰 Сундуки", callback_data="chests"), button(text="🎟 Мультипасс / Донат", callback_data="donate_menu")],
        [button(text="🏆 Рейтинг", callback_data="rating")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def shop_more_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🧰 Кейсы", callback_data="cases"), button(text="🏷 Знаки", callback_data="badges_shop")],
        [button(text="🎴 Мега-открытие", callback_data="mega_open")],
        [button(text="⬅️ Магазин / награды", callback_data="shop")],
    ])

def media_path(card_id):
    """Ищет медиа карты. Поддерживает старый формат media/id.jpg и новый media/cards/id.jpg."""
    folders = [MEDIA_DIR / "cards_watermarked", MEDIA_DIR / "cards", MEDIA_DIR]
    for folder in folders:
        for ext in [".gif", ".mp4", ".jpg", ".jpeg", ".png", ".webp"]:
            p = folder / f"{card_id}{ext}"
            if p.exists():
                return p
    return None


def make_card_banner(card_id):
    if Image is None or ImageDraw is None or ImageFont is None or card_id not in CARD_BY_ID:
        return None
    c = CARD_BY_ID[card_id]
    # Мифики оставлены без автокартинки: туда владелец кладёт свои GIF/MP4 вручную.
    if c.get("rarity") == "Мифический":
        return None
    out_dir = MEDIA_DIR / "generated_cards"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{card_id}.png"
    if out.exists():
        return out

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
    img.save(out, optimize=True, quality=90)
    return out


async def send_card_media(message, card_id):
    p = media_path(card_id)
    if not p:
        p = make_card_banner(card_id)
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
    """Полная карта: редкий прямой дроп. Если карта уже есть — даёт фрагменты."""
    card = CARD_BY_ID[card_id]
    col = player.setdefault("collection", {})
    if card_id not in col:
        col[card_id] = {"count": 1, "shards": int(extra_shards or 0), "level": 1, "unlocked": True}
        return f"🆕 Открыта карта: {card['name']}"
    item = col[card_id]
    item.setdefault("level", 1)
    item.setdefault("shards", 0)
    item.setdefault("count", 0)
    item["unlocked"] = True
    gain = DUPLICATE_SHARDS.get(card["rarity"], 5) + int(extra_shards or 0)
    item["count"] = max(1, int(item.get("count", 0))) + 1
    item["shards"] += gain
    return f"♻️ Дубликат: {card['name']} → +{gain} фрагментов"


def add_fragments(player, card_id, amount):
    """Фрагменты живут отдельно. Закрытые карты не попадают в обычную коллекцию, пока игрок сам не соберёт их."""
    card = CARD_BY_ID[card_id]
    col = player.setdefault("collection", {})
    item = col.setdefault(card_id, {"count": 0, "shards": 0, "level": 1, "unlocked": False})
    item.setdefault("level", 1)
    item.setdefault("count", 0)
    item.setdefault("shards", 0)
    item["shards"] += int(amount)

    if not item.get("unlocked"):
        need_left = max(0, CARD_UNLOCK_FRAGMENTS - int(item.get("shards", 0)))
        if need_left <= 0:
            return f"🧩 +{amount} фрагм. → можно собрать карту: {card['name']} во вкладке «Фрагменты»"
        return f"🧩 +{amount} фрагм. | До сборки {card['name']}: {need_left}"

    need = level_cost(item.get("level", 1), card["rarity"])
    if need is None:
        return f"🧩 +{amount} фрагм. → карта уже максимального уровня"
    return f"🧩 +{amount} фрагм. | Сейчас: {item['shards']}/{need}"


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


def roll_card_with_pity(player, weights=None, exclude=None):
    pity = player.setdefault("pity_counters", {"epic": 0, "legendary": 0, "mythic": 0})
    for k in ["epic", "legendary", "mythic"]:
        pity[k] = int(pity.get(k, 0))

    def _rarity_available(rarity):
        if not weights:
            return True
        return int(weights.get(rarity, 0) or 0) > 0

    forced = None
    note = ""
    if pity["mythic"] + 1 >= PITY_LIMITS["mythic"] and _rarity_available("Мифический"):
        forced = "Мифический"
        note = "\n🎯 Сработал гарант мифической редкости."
    elif pity["legendary"] + 1 >= PITY_LIMITS["legendary"] and _rarity_available("Легендарный"):
        forced = "Легендарный"
        note = "\n🎯 Сработал гарант легендарной редкости."
    elif pity["epic"] + 1 >= PITY_LIMITS["epic"] and _rarity_available("Эпический"):
        forced = "Эпический"
        note = "\n🎯 Сработал гарант эпической редкости."


    if forced:
        card = roll_card(weights={forced: 1}, exclude=exclude, allowed_rarities=[forced])
        if card is None:
            card = roll_card(weights=weights, exclude=exclude)
            note = ""
    else:
        card = roll_card(weights=weights, exclude=exclude)

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


def pull_pack_reward(player, weights, exclude=None):
    card, pity_note = roll_card_with_pity(player, weights=weights, exclude=exclude)
    # Чаще падают фрагменты. Полная карта — отдельный редкий дроп.
    if random.random() < full_card_drop_chance(card["rarity"]):
        result = add_card(player, card["id"])
    else:
        amount = fragment_amount_for(card["rarity"])
        result = add_fragments(player, card["id"], amount)
    return card, result + pity_note

def roll_card(weights=None, exclude=None, allowed_rarities=None):
    exclude = set(exclude or [])
    weights = weights or RARITY_WEIGHTS
    candidates_all = [c for c in CARDS if c["id"] not in exclude and (allowed_rarities is None or c["rarity"] in allowed_rarities)]
    if not candidates_all:
        candidates_all = CARDS[:]
    rarities = list(weights.keys())
    values = list(weights.values())
    for _ in range(80):
        rarity = random.choices(rarities, weights=values, k=1)[0]
        if weights.get(rarity, 0) <= 0:
            continue
        candidates = [c for c in candidates_all if c["rarity"] == rarity]
        if candidates:
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
        team.append(make_instance(CARD_BY_ID[cid], card_level_for_user(uid, cid)))
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
            team.append(make_instance(CARD_BY_ID[cid], card_level_for_user(uid, cid)))
        if not team:
            for cid in best_owned_card_ids(uid, 5):
                team.append(make_instance(CARD_BY_ID[cid], card_level_for_user(uid, cid)))
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


def make_instance(card, level=1):
    return {
        "card_id": card["id"],
        "level": max(1, min(MAX_LEVEL, int(level or 1))),
        "buff": random.choice(BUFFS),
        "debuff": random.choice(DEBUFFS),
        "artifact": random.choice(ARTIFACTS),
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
        f"{rarity_label(card['rarity'])}\n"
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
        f"{n}. 🐉 <b>{e(c['name'])}</b> — {rarity_label(c['rarity'])}\n"
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
    left = CARD_BY_ID[left_inst["card_id"]]
    right = CARD_BY_ID[right_inst["card_id"]]
    left_base = instance_score(left_inst, arena_code)
    right_base = instance_score(right_inst, arena_code)
    left_rand = random.randint(-28, 28)
    right_rand = random.randint(-28, 28)
    ls = left_base + left_rand
    rs = right_base + right_rand
    arena_bonus = ARENA_EFFECTS.get(arena_code, ("", ""))[0]
    if ls >= rs:
        winner_name = left_name
        winner_card = left
        winner_inst = left_inst
        result = 1
    else:
        winner_name = right_name
        winner_card = right
        winner_inst = right_inst
        result = -1

    actions = [
        "врывается первым и ломает дистанцию",
        "подсекает темп противника и забирает инициативу",
        "ловит окно после ошибки врага",
        "переживает стартовый удар и отвечает сильнее",
        "использует арену как укрытие и выходит в контратаку",
        "давит хаксом, скоростью и точным таймингом",
    ]
    text = (
        f"🥊 <b>Раунд {round_no}</b>\n"
        f"{e(left['name'])} vs {e(right['name'])}\n"
        f"• Расчёт: {e(left_name)} <b>{ls}</b> = база {left_base} + рандом {left_rand:+d}; "
        f"{e(right_name)} <b>{rs}</b> = база {right_base} + рандом {right_rand:+d}.\n"
        f"• Артефакт: {e(left_inst['artifact']['name'])} vs {e(right_inst['artifact']['name'])}.\n"
        f"• {e(winner_card['name'])} {random.choice(actions)}.\n"
        f"• Плюс победителя: {e(winner_card.get('plus', 'сильная техника'))}.\n"
        f"• Цена/минус: {e(winner_card.get('minus', 'есть откат после атаки'))}.\n"
        f"• Эффект: {e(winner_inst['debuff']['name'])} — {e(winner_inst['debuff']['text'])}.\n"
        f"• Арена: {e(arena_bonus)}.\n"
        f"✅ Очко забирает: <b>{e(winner_name)}</b>\n"
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


async def send_long(message, text, reply_markup=None):
    if len(text) <= 3900:
        await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
        return
    parts = []
    while len(text) > 3900:
        cut = text.rfind("\n\n", 0, 3900)
        if cut < 1:
            cut = 3900
        parts.append(text[:cut])
        text = text[cut:].strip()
    parts.append(text)
    for part in parts[:-1]:
        await message.answer(part, parse_mode="HTML")
    await message.answer(parts[-1], reply_markup=reply_markup, parse_mode="HTML")


async def set_commands():
    public_commands = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="battle", description="Арена с ботом"),
        BotCommand(command="online", description="Онлайн-бой"),
        BotCommand(command="collection", description="Коллекция"),
        BotCommand(command="profile", description="Профиль"),
        BotCommand(command="shop", description="Магазин / награды"),
        BotCommand(command="daily", description="Ежедневная награда"),
        BotCommand(command="craft", description="Крафт"),
        BotCommand(command="rating", description="Рейтинг"),
        BotCommand(command="friends", description="Друзья"),
        BotCommand(command="addfriend", description="Добавить друга по ID"),
        BotCommand(command="promo", description="Промокод"),
        BotCommand(command="rules", description="Правила"),
        BotCommand(command="myid", description="Мой Telegram ID"),
        BotCommand(command="pass", description="Мультипасс"),
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
        BotCommand(command="compensate_patch16", description="Компенсация PATCH16"),
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
    for tid in targets:
        try:
            await bot.send_message(int(tid), msg, parse_mode="HTML")
            sent += 1
        except Exception:
            pass
    await message.answer(
        "✅ Спор отправлен поддержке. Если аргумент сильный, награду выдадут вручную.",
        reply_markup=back_menu()
    )

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
    save_json(DATA_FILE, DATA)
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


@dp.message(CommandStart())
async def start(message: types.Message):
    get_user_data(message.from_user)
    text = message.text or ""
    if " friend_" in text:
        code = text.split("friend_", 1)[1].strip()
        await accept_friend_invite(message, code)
        return
    await message.answer(
        main_menu_text(message.from_user),
        reply_markup=main_menu(message.from_user.id),
        parse_mode="HTML"
    )
    await maybe_send_luffy_intro(message, message.from_user)


@dp.callback_query(F.data == "menu")
async def menu(callback: types.CallbackQuery):
    get_user_data(callback.from_user)
    await callback.message.answer(main_menu_text(callback.from_user), reply_markup=main_menu(callback.from_user.id), parse_mode="HTML")
    await callback.answer()


async def send_profile(message, user):
    p = get_user_data(user)
    visible_collection = {cid: v for cid, v in p.get("collection", {}).items() if cid in CARD_BY_ID}
    total = sum(int(v.get("count", 0) or 0) for v in visible_collection.values())
    unique = len(visible_collection)
    lvl, rem, nxt = calc_user_level(p.get("xp", 0))
    role = OWNER_LABEL if is_owner(user.id) else ("🤝 Правая рука" if is_right_hand(user.id) else "Игрок")
    pass_state = "Premium" if p.get("pass_premium") else (p.get("pass_purchase_request") or "обычный")
    badges = p.get("badges", [])
    badge_line = visible_badges(badges[:6])
    await message.answer(
        f"{CE['profile']} <b>Профиль</b>\n\n"
        f"Ник: <b>{e(p.get('name', user.full_name))}</b>\n"
        f"Роль: {role}\n"
        f"Уровень: <b>{lvl}</b> ({rem}/{nxt} XP)\n"
        f"{PISTACHIOS_LABEL}: <b>{p.get('fistiks', 0)}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{p.get('moon_coins', 0)}</b>\n"
        f"Карт всего: <b>{total}</b>\n"
        f"Уникальных карт: <b>{unique}/{len(CARDS)}</b>\n"
        f"Знаки: <b>{e(badge_line)}</b>\n"
        f"Мультипасс: <b>{e(pass_state)}</b> | очки <b>{p.get('pass_xp', 0)}</b>\n"
        f"Победы/поражения: <b>{p.get('wins', 0)}/{p.get('losses', 0)}</b>\n\n"
        f"{e(SECTION_HINTS['profile'][1])}",
        reply_markup=profile_menu(user.id),
        parse_mode="HTML"
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
        f"{PISTACHIOS_LABEL}: <b>{p.get('fistiks', 0)}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{p.get('moon_coins', 0)}</b>\n\n"
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
        save_json(DATA_FILE, DATA)

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
            text += f"{n+1}. {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> | ур. {lvl}/{MAX_LEVEL} | сила {card_power(c, lvl)}\n"
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
    save_json(DATA_FILE, DATA)
    await callback.message.answer("🧠 Колода собрана автоматически: поставлены 5 сильнейших открытых карт.", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data == "toggle_auto_team")
async def toggle_auto_team_cb(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    p["auto_team"] = not bool(p.get("auto_team", True))
    if p["auto_team"]:
        p["deck"] = best_owned_card_ids(callback.from_user.id, 5)
    save_json(DATA_FILE, DATA)
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
        text += f"• {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> | ур.{lvl} | сила {card_power(c,lvl)}\n"
        rows.append([button(text=f"Поставить: {c['name'][:28]}", callback_data=f"deck_set:{slot}:{cid}")])
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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
        f"Напоминание о бесплатном сундуке: <b>{'включено' if enabled else 'выключено'}</b>.\n"
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
    save_json(DATA_FILE, DATA)
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
    main_text = (
        f"{ui_box(*SECTION_HINTS['rules'])}\n\n"
        f"{CE['collection']} <b>Как получать карты</b>\n"
        "• Открывай бесплатный сундук раз в 3 часа.\n"
        "• Забирай ежедневную награду.\n"
        "• Участвуй в арене, рейдах, событиях и мультипассе.\n"
        "• Дубликаты не пропадают: они дают фрагменты для улучшения.\n\n"
        "🎴 <b>Редкости</b>\n"
        f"{CE['origin']} Origin — базовые формы и стартовые бойцы.\n"
        f"{CE['rare']} Rare — усиленные формы и техники.\n"
        f"{CE['epic']} Epic — серьёзные режимы, хакс и сильные роли.\n"
        f"{CE['legendary']} Legendary — пиковые версии персонажей.\n"
        f"{CE['absolute']} Absolute — самые опасные формы мультивселенной.\n\n"
        f"{PISTACHIOS_LABEL} <b>Фисташки</b>\n"
        "Обычная валюта: сундуки, крафт, часть улучшений и награды.\n\n"
        f"{DRAGONITE_LABEL} <b>Драконит</b>\n"
        "Редкая валюта: кейсы, редкие покупки, события и мультипасс.\n\n"
        f"{CE['free_chest']} <b>Сундук</b>\n"
        "Бесплатный сундук даёт карту. Если персонаж уже есть — получаешь фрагменты.\n\n"
        f"{CE['multipass']} <b>Мультипасс</b>\n"
        "Сезонная шкала прогресса: задания, бесплатные награды, premium-награды и Stars-наборы.\n\n"
        f"{CE['arena']} <b>Арена</b>\n"
        "Перед боем выбери команду: своя колода, авто-колода или ручной выбор. На исход влияют уровень, HP, артефакты, роль, арена и немного случайности.\n\n"
        "🚫 <b>Запрещено</b>\n"
        "Спамить владельцу, абузить баги, пытаться ломать оплату/прогресс, выдавать себя за администратора.\n\n"
        "📩 <b>Проблема или спор</b>\n"
        "Если бой, карта или награда выглядят несправедливо — нажми «Оспорить» или напиши <code>/appeal причина</code>. Сообщение уйдёт владельцу. Лучше приложить скрин: профиль, бой, время и что именно сломалось."
    )
    await message.answer(main_text, parse_mode="HTML")

    chunks = anime_source_chunks()
    for i, chunk in enumerate(chunks):
        await message.answer(
            chunk,
            reply_markup=back_menu() if i == len(chunks) - 1 else None,
            parse_mode="HTML"
        )

@dp.message(Command("rules"))
async def rules_cmd(message: types.Message):
    await send_rules(message)


@dp.callback_query(F.data == "rules")
async def rules_cb(callback: types.CallbackQuery):
    await send_rules(callback.message)
    await callback.answer()


async def send_daily(message, user):
    p = get_user_data(user)
    today = date.today().isoformat()
    if p.get("last_daily") == today:
        await message.answer("🎁 Сегодняшняя ежедневная награда уже забрана.", reply_markup=back_menu())
        return
    p["last_daily"] = today
    fistiks = random.randint(260, 430)
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
    save_json(DATA_FILE, DATA)
    text = (
        f"{CE['rewards']} <b>Ежедневная награда</b>\n\n"
        f"+{fistiks} 💎 Фисташек\n"
        f"+{pass_gain} очков мультипасса\n"
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
    card = roll_card(weights={rarity: 1}, allowed_rarities=[rarity])
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
        "date": datetime.now().isoformat(),
    })
    return (
        f"🎁 <b>{e(pack['title'])}</b>\n"
        f"🐉 Карта: {rarity_label(card['rarity'])} <b>{e(card['name'])}</b>\n"
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
    title, body = SECTION_HINTS["shop"]
    await message.answer(
        f"{ui_box(title, body)}\n\n"
        f"{PISTACHIOS_LABEL}: <b>{p['fistiks']}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{p.get('moon_coins', 0)}</b>\n\n"
        f"{CE['free_chest']} <b>Бесплатный сундук</b> — персонаж каждые 3 часа.\n"
        f"{CE['daily_reward']} <b>Ежедневная награда</b> — ресурсы, pass XP и шанс на редкий бонус.\n"
        f"{CE['chests']} <b>Сундуки</b> — обычные, усиленные и королевские открытия.\n"
        f"{CE['multipass']} <b>Мультипасс / Донат</b> — Stars, pass, кейсы, промокоды и уровни.\n"
        f"{CE['rating']} <b>Рейтинг</b> — сила игроков и место в мультивселенной.",
        reply_markup=shop_menu(),
        parse_mode="HTML"
    )


async def send_chests(message, user):
    p = get_user_data(user)
    rows = [
        [button(text="🆓 Бесплатный сундук", callback_data="pack_info:free")],
        [button(text="📦 Обычный сундук", callback_data="pack_info:basic")],
        [button(text="💎 Усиленный сундук", callback_data="pack_info:rare")],
        [button(text="👑 Королевский сундук", callback_data="pack_info:royal")],
        [button(text="🎴 Мега-открытие", callback_data="mega_open")],
        [button(text="⬅️ Магазин / награды", callback_data="shop")],
    ]
    await message.answer(
        f"🧰 <b>Сундуки</b>\n\n"
        f"Баланс: <b>{p['fistiks']}</b> 💎\n"
        "Перед покупкой сундука нажми на него — там будут шансы и цена.\n\n"
        "🆓 Бесплатный сундук доступен раз в 3 часа.\n🎯 Гарант: 10 открытий без эпика → эпик, 50 без легендарки → легендарка, 150 без мифика → мифик.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )



@dp.message(Command("shop"))
async def shop_cmd(message: types.Message):
    await send_shop(message, message.from_user)



@dp.callback_query(F.data == "donate_menu")
async def donate_menu_cb(callback: types.CallbackQuery):
    rows = [
        [button(text="⭐ Stars-наборы", callback_data="stars_shop"), button(text="🎟 Мультипасс", callback_data="multipass")],
        [button(text="🧰 Кейсы", callback_data="cases"), button(text="🏷 Знаки", callback_data="badges_shop")],
        [button(text="🎴 Мега-открытие", callback_data="mega_open"), button(text="🎟 Промокод", callback_data="promo_help")],
        [button(text="💳 Купить уровень", callback_data="buy_pass_level")],
        [button(text="⬅️ Магазин / награды", callback_data="shop")],
    ]
    await callback.message.answer(
        "🎟 <b>Мультипасс / Донат</b>\n\n"
        "Stars-наборы, кейсы, знаки, промокоды, мультипасс и уровни pass собраны здесь.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "shop")
async def shop_cb(callback: types.CallbackQuery):
    await send_shop(callback.message, callback.from_user)


@dp.callback_query(F.data == "shop_more")
async def shop_more_cb(callback: types.CallbackQuery):
    await callback.message.answer(
        "⚙️ <b>Дополнительные разделы</b>\n\nТут лежит то, что не нужно держать на главном экране каждый раз.",
        reply_markup=shop_more_menu(),
        parse_mode="HTML",
    )
    await callback.answer()


async def send_stars_shop(message, user):
    p = get_user_data(user)
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
    kind = callback.data.split(":", 1)[1]
    if kind == "free":
        text = (
            "🆓 <b>Бесплатный сундук</b>\n\n"
            "Доступен раз в 3 часа. Даёт 1 карту.\n"
            "⭐ Очки за получение удваиваются.\n\n"
            "<b>Шансы:</b>\n" + odds_text(FREE_PACK_WEIGHTS)
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [button(text="Открыть бесплатно", callback_data="buy_pack:free")],
            [button(text="⬅️ Сундуки", callback_data="chests")]
        ])
    else:
        pack = SHOP_PACKS[kind]
        cost, sale = discounted_cost(callback.from_user, pack["base_cost"])
        price = f"🔥 Акция новичка: <s>{pack['base_cost']}</s> → <b>{cost}</b> 💎" if sale else f"Цена: <b>{cost}</b> 💎"
        text = (
            f"📦 <b>{pack['name']}</b>\n\n"
            f"{e(pack['description'])}\n"
            f"Карт внутри: <b>{pack['count']}</b>\n"
            f"{price}\n\n"
            f"<b>Шансы:</b>\n{odds_text(pack['weights'])}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [button(text="Купить", callback_data=f"buy_pack:{kind}")],
            [button(text="⬅️ Сундуки", callback_data="chests")]
        ])
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


async def send_pack_result(message, title, cards_got, player):
    text = f"{CE['rewards']} <b>{e(title)} открыт</b>\n\n"
    for card, result in cards_got:
        text += (
            f"{CE['collection']} <b>{e(card['name'])}</b> — {rarity_label(card['rarity'])}\n"
            f"{e(result)}\n"
        )
    text += f"\n{CE['collection']} Полное описание, форма, роль, плюсы и минусы доступны в коллекции."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="⬅️ Назад к сундукам", callback_data="chests")],
        [button(text="⬅️ Магазин / награды", callback_data="shop"), button(text="🏠 Меню", callback_data="menu")],
    ])
    await send_long(message, text, reply_markup=kb)


@dp.callback_query(F.data.startswith("buy_pack:"))
async def buy_pack(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    kind = callback.data.split(":", 1)[1]
    if kind == "free":
        now = datetime.now()
        last = p.get("last_free_pack", "")
        if last:
            last_dt = datetime.fromisoformat(last)
            wait_until = last_dt + timedelta(hours=3)
            if now < wait_until:
                mins = int((wait_until - now).total_seconds() // 60) + 1
                await callback.answer(f"Бесплатный сундук будет доступен через {mins} мин.", show_alert=True)
                return
        p["last_free_pack"] = now.isoformat()
        p["free_pack_notified"] = False
        count, cost, weights, name, xp = 1, 0, FREE_PACK_WEIGHTS, "Бесплатный сундук", 60
    else:
        pack = SHOP_PACKS[kind]
        count, weights, name = pack["count"], pack["weights"], pack["name"]
        cost, _ = discounted_cost(callback.from_user, pack["base_cost"])
        xp = 45 if kind == "basic" else (220 if kind == "royal" else 120)
    if p["fistiks"] < cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает фисташек.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["fistiks"] -= cost
    pulled = set()
    got = []
    for _ in range(count):
        if kind == "free":
            card = roll_card(weights=weights, exclude=pulled)
            result = add_card(p, card["id"])
        else:
            card, result = pull_pack_reward(p, weights, exclude=pulled)
        pulled.add(card["id"])
        got.append((card, result))
    artifact_bonus = None
    if random.random() < (0.16 if kind == "free" else 0.24):
        artifact_bonus = grant_random_artifact(p)
    add_xp(p, xp)
    add_pass_task_progress(p, "chest", 1)
    newbie_lines = []
    line = add_newbie_task_progress(p, "chest", 1)
    if line:
        newbie_lines.append(line)
    if kind == "free":
        line = add_newbie_task_progress(p, "free_pack", 1)
        if line:
            newbie_lines.append(line)
    save_json(DATA_FILE, DATA)
    await send_pack_result(callback.message, name, got, p)
    if artifact_bonus:
        await callback.message.answer(
            f"🧿 <b>Артефакт найден</b>\n\n{artifact_label(artifact_bonus)}\n{e(artifact_bonus.get('text',''))}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="🧿 Артефакты", callback_data="artifacts:page:0")]]),
            parse_mode="HTML"
        )
    if newbie_lines:
        await callback.message.answer("🚀 <b>Прогресс новичка</b>\n\n" + "\n".join(e(x) for x in newbie_lines), parse_mode="HTML")
    await callback.answer()


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
    save_json(DATA_FILE, DATA)
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
    return f"{rarity_label(c['rarity'])} <b>{e(c['name'])}</b> · ур.{lvl} · сила <b>{card_power(c, lvl)}</b>"

async def send_collection(message, user, page=0, rarity_filter="all", sort_mode="power"):
    p = get_user_data(user)
    items = owned_card_items(p)
    if rarity_filter != "all":
        wanted = RARITY_CODES.get(rarity_filter, rarity_filter)
        items = [(cid, info) for cid, info in items if CARD_BY_ID[cid].get("rarity") == wanted]
    frag_count = len(fragment_card_items(p))
    art_count = sum(int(v.get("count", 0) or 0) for v in p.setdefault("artifacts", {}).values())
    if not items:
        rows = [
            [button(text="🧩 Фрагменты", callback_data="fragments:page:0:all"), button(text="🧿 Артефакты", callback_data="artifacts:page:0")],
            [button(text="⬅️ Меню", callback_data="menu")]
        ]
        await message.answer(
            f"{ui_box(*SECTION_HINTS['collection'])}\n\n"
            "Открытых карт по этому фильтру пока нет.\n"
            f"Фрагментов к сборке: <b>{frag_count}</b>\n"
            f"Артефактов в инвентаре: <b>{art_count}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            parse_mode="HTML"
        )
        return
    reverse = sort_mode != "name" and sort_mode != "anime"
    items.sort(key=lambda x: collection_sort_key(x[0], x[1], sort_mode), reverse=reverse)
    per_page = 6
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    rows = []
    text_cards = []
    for cid, info in items[page * per_page:(page + 1) * per_page]:
        c = CARD_BY_ID[cid]
        lvl = int(info.get('level', 1))
        text_cards.append("• " + collection_card_line(cid, info))
        rows.append([button(text=f"{RARITY_EMOJI.get(c['rarity'],'⚪')} {c['name'][:28]} · ур.{lvl} · {card_power(c,lvl)}", callback_data=f"card:{cid}")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"collection:page:{page-1}:{rarity_filter}:{sort_mode}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"collection:page:{page+1}:{rarity_filter}:{sort_mode}"))
    rows.append(nav)
    rows.append([
        button(text="📚 Все", callback_data="collection:filter:all"),
        button(text="⚪ Origin", callback_data="collection:filter:common"),
        button(text="🔷 Rare", callback_data="collection:filter:rare"),
    ])
    rows.append([
        button(text="🟣 Epic", callback_data="collection:filter:epic"),
        button(text="🟡 Legendary", callback_data="collection:filter:legendary"),
        button(text="🔴 Absolute", callback_data="collection:filter:mythic"),
    ])
    rows.append([
        button(text="💪 По силе", callback_data=f"collection:sort:{rarity_filter}:power"),
        button(text="⬆️ По уровню", callback_data=f"collection:sort:{rarity_filter}:level"),
        button(text="🔤 По имени", callback_data=f"collection:sort:{rarity_filter}:name"),
    ])
    rows.append([button(text="🧩 Фрагменты", callback_data="fragments:page:0:all"), button(text="🧿 Артефакты", callback_data="artifacts:page:0")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    title, body = SECTION_HINTS["collection"]
    text = (
        f"{ui_box(title, body)}\n\n"
        f"Фильтр: <b>{e(collection_filter_name(rarity_filter))}</b> · сортировка: <b>{e(sort_mode)}</b>\n"
        f"Открытых карт: <b>{len(owned_card_items(p))}</b> · фрагментов к сборке: <b>{frag_count}</b> · артефактов: <b>{art_count}</b>\n\n"
        + "\n".join(text_cards) +
        "\n\nПоиск: <code>/findcard имя</code>"
    )
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")



@dp.message(Command("collection"))
async def collection_cmd(message: types.Message):
    await send_collection(message, message.from_user, 0)


@dp.message(Command("findcard"))
async def findcard_cmd(message: types.Message):
    query = message.text.replace("/findcard", "", 1).strip().lower()
    if not query:
        await message.answer("Формат: <code>/findcard наруто</code>", parse_mode="HTML", reply_markup=back_menu())
        return
    p = get_user_data(message.from_user)
    matches = []
    for cid, info in owned_card_items(p):
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
        text += f"• {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> | {e(c['anime'])} | ур.{lvl} | сила {card_power(c,lvl)}\n"
        rows.append([button(text=f"Открыть: {c['name'][:28]}", callback_data=f"card:{cid}")])
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:page:0:all:power")])
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
    lines = [f"🧩 <b>Фрагменты персонажей</b>\n",
             "Здесь лежат персонажи, которых ещё нет в коллекции. Набери 100 фрагментов и собери карту.\n"]
    if not items:
        lines.append("Пока нет фрагментов закрытых персонажей.")
    for cid, info in items[page*per_page:(page+1)*per_page]:
        c = CARD_BY_ID[cid]
        shards = int(info.get("shards", 0) or 0)
        ready = shards >= CARD_UNLOCK_FRAGMENTS
        lines.append(f"• {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> — {shards}/{CARD_UNLOCK_FRAGMENTS}")
        if ready:
            rows.append([button(text=f"✅ Собрать: {c['name'][:28]}", callback_data=f"fragment_unlock:{cid}")])
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
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:page:0:all:power")])
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
    cid = callback.data.split(":", 1)[1]
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
    save_json(DATA_FILE, DATA)
    c = CARD_BY_ID[cid]
    await callback.message.answer(
        f"{CE['collection']} <b>Карта собрана</b>\n\n"
        f"{rarity_label(c['rarity'])} <b>{e(c['name'])}</b>\n"
        f"Форма: {e(c.get('form',''))}\n\n"
        "Теперь персонаж появился в основной коллекции.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="Открыть карту", callback_data=f"card:{cid}")], [button(text="⬅️ Фрагменты", callback_data="fragments:page:0:all")]]),
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

async def send_artifacts_collection(message, user, page=0):
    p = get_user_data(user)
    inv = p.setdefault("artifacts", {})
    items = [(aid, info) for aid, info in inv.items() if int(info.get("count", 0) or 0) > 0 and aid in ARTIFACT_BY_ID]
    items.sort(key=lambda x: (RARITY_BONUS.get(ARTIFACT_BY_ID[x[0]].get("rarity","Обычный"), 0), x[1].get("count", 0)), reverse=True)
    per_page = 8
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    lines = ["🧿 <b>Коллекция артефактов</b>\n", "Артефакты усиливают бои, рейды и авто-колоды. Они имеют те же редкости, что и персонажи.\n"]
    rows = []
    if not items:
        lines.append("Пока нет артефактов. Они могут выпасть из сундуков и ежедневных наград.")
    for aid, info in items[page*per_page:(page+1)*per_page]:
        a = ARTIFACT_BY_ID[aid]
        lines.append(f"• {artifact_label(a)} ×{int(info.get('count',0))} — {e(a.get('text',''))}")
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"artifacts:page:{page-1}"))
    nav.append(button(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"artifacts:page:{page+1}"))
    rows.append(nav)
    rows.append([button(text="⬅️ Коллекция", callback_data="collection:page:0:all:power")])
    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")

@dp.callback_query(F.data.startswith("artifacts:page:"))
async def artifacts_page_cb(callback: types.CallbackQuery):
    page_s = callback.data.split(":")[-1]
    page = int(page_s) if str(page_s).isdigit() else 0
    await send_artifacts_collection(callback.message, callback.from_user, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("card:"))
async def card_detail(callback: types.CallbackQuery):
    cid = callback.data.split(":", 1)[1]
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p["collection"] or int(p["collection"].get(cid, {}).get("count", 0) or 0) <= 0 or not p["collection"].get(cid, {}).get("unlocked", True):
        await callback.answer("Эта карта пока лежит во фрагментах. Открой вкладку «Фрагменты».", show_alert=True)
        return
    await send_card_media(callback.message, cid)
    c = CARD_BY_ID[cid]
    info = p["collection"][cid]
    level = int(info.get("level", 1))
    cost = level_cost(level, c["rarity"])
    power = card_power(c, level)
    hp = int(c.get("stats", {}).get("durability", 50)) * 10 + level * 25
    next_text = "Максимальный уровень достигнут." if cost is None else f"До следующего уровня: {cost} фрагментов."
    owner_hint = ""
    if is_owner(callback.from_user.id):
        media_hint = f"media/cards/{cid}.jpg или media/cards/{cid}.gif"
        owner_hint = f"\n\n🖼 Медиа: <code>{e(media_hint)}</code>\n🆔 ID: <code>{e(cid)}</code>"
    text = (
        f"{CE['collection']} <b>{e(c['name'])}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🎞 <b>Аниме:</b> {e(c.get('anime',''))}\n"
        f"🎭 <b>Форма:</b> {e(c.get('form',''))}\n"
        f"🎴 <b>Редкость:</b> {rarity_label(c['rarity'])}\n\n"
        f"📈 <b>Уровень:</b> {level}/{MAX_LEVEL}\n"
        f"⚔️ <b>Сила:</b> {power}\n"
        f"❤️ <b>HP:</b> {hp}\n"
        f"🎯 <b>Роль:</b> {e(c.get('role',''))}\n"
        f"🧩 <b>Фрагменты:</b> {info.get('shards',0)}\n"
        f"🔝 <b>Предел:</b> {MAX_LEVEL} уровень\n\n"
        f"🟨 <b>Описание</b>\n<blockquote>{e(c.get('description', 'Описание скоро будет обновлено.'))}</blockquote>\n"
        f"🟦 <b>Плюс</b>\n<blockquote>{e(c.get('plus',''))}</blockquote>\n"
        f"🟥 <b>Минус</b>\n<blockquote>{e(c.get('minus',''))}</blockquote>\n"
        f"📌 {e(next_text)}"
        f"{owner_hint}"
    )
    rows = []
    if cost is not None:
        rows.append([button(text=f"⬆️ Улучшить до {level+1}", callback_data=f"upgrade:{cid}")])
    rows.append([button(text="🎴 В команду", callback_data=f"deck_add:{cid}"), button(text="⬅️ Назад", callback_data="collection:page:0:all:power")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await callback.answer()



@dp.callback_query(F.data.startswith("deck_add:"))
async def deck_add_cb(callback: types.CallbackQuery):
    cid = callback.data.split(":", 1)[1]
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p.get("collection", {}) or int(p["collection"][cid].get("count", 0)) <= 0:
        await callback.answer("Этой карты нет в коллекции.", show_alert=True)
        return
    deck = [x for x in p.get("deck", []) if x in CARD_BY_ID and x != cid]
    deck.insert(0, cid)
    p["deck"] = deck[:5]
    p["auto_team"] = False
    save_json(DATA_FILE, DATA)
    await callback.message.answer(
        f"🎴 <b>{e(CARD_BY_ID[cid]['name'])}</b> добавлен в команду.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [button(text="🧬 Открыть колоду", callback_data="deck")],
            [button(text="⬅️ Коллекция", callback_data="collection:page:0:all:power")],
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("upgrade:"))
async def upgrade_card(callback: types.CallbackQuery):
    cid = callback.data.split(":", 1)[1]
    p = get_user_data(callback.from_user)
    if cid not in p["collection"]:
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
    save_json(DATA_FILE, DATA)
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
    draft = manual_team_drafts.setdefault(uid, {"target": target, "cards": []})
    draft["target"] = target
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
        text += f"• {mark} {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> | ур.{lvl} | сила {card_power(c,lvl)}\n"
        rows.append([button(text=f"{mark} {c['name'][:30]}", callback_data=f"mtadd:{target}:{cid}")])
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
    save_json(DATA_FILE, DATA)
    if source == "manual":
        manual_team_drafts[str(callback.from_user.id)] = {"target": target, "cards": []}
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
    except Exception:
        await callback.answer("Ошибка карты.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p.get("collection", {}) or int(p["collection"][cid].get("count", 0) or 0) <= 0:
        await callback.answer("Этой карты нет в коллекции.", show_alert=True)
        return
    draft = manual_team_drafts.setdefault(str(callback.from_user.id), {"target": target, "cards": []})
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
    manual_team_drafts[str(callback.from_user.id)] = {"target": target, "cards": []}
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
    save_json(DATA_FILE, DATA)
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
    inst = make_instance(card, card_level_for_user(uid, card["id"]))
    state["player"].append(inst)

    player = DATA.get("users", {}).get(str(uid))
    if player is not None:
        result = "карта вышла на поле из твоей коллекции"
        add_xp(player, 15)
        save_json(DATA_FILE, DATA)
    else:
        result = "карта выбрана"

    chat_id = state.get("chat_id")
    prefix = "⏱ Время вышло. Бот выбрал за тебя:" if auto else "✅ Выбрано:"
    await bot.send_message(chat_id, f"{prefix} {rarity_label(card['rarity'])} <b>{e(card['name'])}</b>\n{e(result)}", parse_mode="HTML")

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
        f"👤 Ты выставил: <b>{e(player_card['name'])}</b> — {rarity_label(player_card['rarity'])}\n"
        f"🤖 Бот выставил: <b>{e(bot_card['name'])}</b> — {rarity_label(bot_card['rarity'])}\n\n"
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
    state["resolved"] = True
    save_json(DATA_FILE, DATA)

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
    await callback.message.answer(
        f"⚖️ Спор принят: <b>{e(names.get(reason, reason))}</b>.\n\nПозже этот спор можно будет привязать к реальному пересчёту боя.",
        reply_markup=back_menu(),
        parse_mode="HTML"
    )
    await callback.answer()
async def apply_promo(message, code):
    promos = load_json(PROMO_FILE, {})
    code = code.strip().upper()
    if code not in promos or not promos[code].get("active", False):
        await message.answer("Промокод не найден или отключён.")
        return
    promo = promos[code]
    if promo.get("expires"):
        try:
            if date.today() > date.fromisoformat(promo["expires"]):
                await message.answer("Промокод истёк.")
                return
        except Exception:
            pass
    p = get_user_data(message.from_user)
    used = p.setdefault("used_promos", [])
    if code in used and not is_owner(message.from_user.id):
        await message.answer("Ты уже использовал этот промокод.")
        return
    reward = promo.get("reward", {})
    text = f"🎟 Промокод активирован: <b>{e(code)}</b>\n\n"
    if "fistiks" in reward:
        p["fistiks"] += int(reward["fistiks"])
        text += f"+{reward['fistiks']} 💎 фисташек\n"
    if "card" in reward and reward["card"] in CARD_BY_ID:
        text += add_card(p, reward["card"], int(reward.get("shards", 0))) + "\n"
    add_xp(p, 40)
    used.append(code)
    save_json(DATA_FILE, DATA)
    await message.answer(text, reply_markup=main_menu(message.from_user.id), parse_mode="HTML")


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
    uid = str(user.id)
    friends_list = DATA.setdefault("friends", {}).get(uid, [])
    lines = []
    rows = []
    if friends_list:
        for fid in friends_list[:15]:
            fdata = DATA.get("users", {}).get(fid, {})
            fname = fdata.get("name", fid)
            online = "🟢 онлайн" if is_online(fid) else "⚫ офлайн"
            lines.append(f"• {e(fname)} — {online}")
            rows.append([button(text=f"⚔️ Вызвать: {fname}", callback_data=f"challenge:{fid}")])
    else:
        lines.append("Список друзей пуст.")
    pending = DATA.setdefault("friend_requests", {}).get(uid, [])
    if pending:
        lines.append("\n<b>Заявки:</b>")
        for from_id in pending[:10]:
            from_name = DATA.get("users", {}).get(from_id, {}).get("name", from_id)
            lines.append(f"• {e(from_name)} хочет добавить тебя")
            rows.append([
                button(text=f"✅ Принять {from_name}", callback_data=f"friend_accept:{from_id}"),
                button(text="❌", callback_data=f"friend_decline:{from_id}"),
            ])
    rows.append([button(text="🌐 Найти онлайн-бой", callback_data="online_search")])
    rows.append([button(text="🔗 Реферальная ссылка", callback_data="friend_link")])
    rows.append([button(text="🎁 Забрать реферальные вехи", callback_data="ref_claim")])
    rows.append([button(text="⬅️ Меню", callback_data="menu")])
    text = (
        "👥 <b>Друзья и рефералка</b>\n\n"
        + "\n".join(lines)
        + "\n\nЧтобы отправить заявку, напиши:\n<code>/addfriend ID</code>\n\n"
        + f"Приглашено друзей: <b>{int(p.get('ref_count', 0))}</b>\n"
        + "Друг засчитывается честно: он должен зайти по ссылке и начать пользоваться ботом.\n\n"
        + "<b>Вехи приглашений:</b>\n"
        + format_ref_milestones(p)
    )
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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
    inst = make_instance(card, card_level_for_user(current_uid, card["id"]))
    state["teams"][current_uid].append(inst)

    player = DATA.get("users", {}).get(str(current_uid))
    if player is not None:
        result = "карта вышла на поле из твоей коллекции"
        add_xp(player, 20)
        save_json(DATA_FILE, DATA)
    else:
        result = "карта выбрана"

    name = state["names"].get(current_uid, current_uid)
    prefix = "⏱ Время вышло. Автовыбор PvP:" if auto else "✅ Твой скрытый PvP-выбор:"
    try:
        await bot.send_message(
            int(current_uid),
            f"{prefix} {rarity_label(card['rarity'])} <b>{e(card['name'])}</b>\n{e(result)}",
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
            text += pvp_team_text(f"👤 <b>Твоя команда</b>", state["teams"][uid])
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
        f"👤 {e(n1)} выставил: <b>{e(c1['name'])}</b> — {rarity_label(c1['rarity'])}\n"
        f"👤 {e(n2)} выставил: <b>{e(c2['name'])}</b> — {rarity_label(c2['rarity'])}\n\n"
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
            text += f"• {idx+1}. {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> | сила {instance_score(inst)}\n"
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
    round_no = int(state.get("pvp_fight_round", 0))
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
        if loser_uid in DATA["users"]:
            DATA["users"][loser_uid]["losses"] = DATA["users"][loser_uid].get("losses", 0) + 1
            DATA["users"][loser_uid]["battles"] = DATA["users"][loser_uid].get("battles", 0) + 1
            DATA["users"][loser_uid]["fistiks"] = DATA["users"][loser_uid].get("fistiks", 0) + 60
            add_xp(DATA["users"][loser_uid], 60)
            add_pass_task_progress(DATA["users"][loser_uid], "battle", 1)
            add_newbie_task_progress(DATA["users"][loser_uid], "battle", 1)
        state["scored"] = True
        save_json(DATA_FILE, DATA)

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


@dp.message(Command("ref"))
async def ref_cmd(message: types.Message):
    await send_friends_menu(message, message.from_user)


@dp.callback_query(F.data == "friend_link")
async def friend_link(callback: types.CallbackQuery):
    code = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    DATA.setdefault("friend_invites", {})[code] = str(callback.from_user.id)
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
    await callback.message.answer("🎁 <b>Реферальные награды получены</b>\n\n" + "\n".join(lines), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


async def accept_friend_invite(message, code):
    inviter = DATA.setdefault("friend_invites", {}).get(code)
    if not inviter:
        await message.answer("Ссылка союза не найдена или устарела.", reply_markup=main_menu(message.from_user.id))
        return
    me_id = str(message.from_user.id)
    if inviter == me_id:
        await message.answer("Нельзя открыть союз с самим собой.", reply_markup=main_menu(message.from_user.id))
        return
    DATA.setdefault("friends", {}).setdefault(inviter, [])
    DATA.setdefault("friends", {}).setdefault(me_id, [])
    if me_id not in DATA["friends"][inviter]:
        DATA["friends"][inviter].append(me_id)
    if inviter not in DATA["friends"][me_id]:
        DATA["friends"][me_id].append(inviter)
    user_player = get_user_data(message.from_user)
    first_ref = False
    if not user_player.get("ref_by"):
        first_ref = True
        user_player["ref_by"] = inviter
        user_player["fistiks"] += 300
        add_xp(user_player, 120)
        if inviter in DATA["users"]:
            inv = DATA["users"][inviter]
            inv["fistiks"] = int(inv.get("fistiks", 0)) + 500
            inv["ref_count"] = inv.get("ref_count", 0) + 1
            add_newbie_task_progress(inv, "referral", 1)
            add_xp(inv, 150)
    save_json(DATA_FILE, DATA)
    inviter_name = DATA.get("users", {}).get(str(inviter), {}).get("name", "союзник")
    text = (
        f"{CE['start']} <b>Союз мультивселенной открыт</b>\n\n"
        f"Тебя привёл игрок <b>{e(inviter_name)}</b>. Теперь вы отмечены как союзники.\n"
        f"{PISTACHIOS_LABEL}: <b>+300</b> тебе"
    )
    if first_ref:
        text += "\nРеферальная награда начислена. Начни с бесплатного сундука и Пути Луфи."
    else:
        text += "\nСоюз уже был учтён раньше, повторная награда не начисляется."
    await message.answer(text, reply_markup=main_menu(message.from_user.id), parse_mode="HTML")
    await maybe_send_luffy_intro(message, message.from_user)
    if first_ref and inviter in DATA.get("users", {}):
        try:
            await bot.send_message(
                int(inviter),
                f"🕊 <b>Новый союзник в мультивселенной</b>\n\n"
                f"<b>{e(message.from_user.full_name)}</b> вошёл по твоей ссылке.\n"
                f"Награда владельца ссылки: <b>+500</b> 💎 Фисташек.\n\n"
                "Чем больше союзников, тем сильнее твой отряд.",
                parse_mode="HTML"
            )
        except Exception as ex:
            logger.debug("ref inviter notify failed: %s", ex)
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
    text = "⚒️ <b>Крафт</b>\n\nСобирай фрагменты одинаковой редкости и создавай случайную карту этой редкости.\n\n"
    for code, rarity in RARITY_CODES.items():
        have = rarity_shards(p, rarity)
        cost = CRAFT_COSTS[rarity]
        text += f"{rarity_label(rarity)}: {have}/{cost} фрагментов\n"

    rows = [
        [
            button(text="⚪ Обычный", callback_data="craft_make:common"),
            button(text="🔵 Редкий", callback_data="craft_make:rare"),
        ],
        [
            button(text="🟣 Эпический", callback_data="craft_make:epic"),
            button(text="🟡 Легендарный", callback_data="craft_make:legendary"),
        ],
        [button(text="🔴 Мифический", callback_data="craft_make:mythic")],
        [button(text="⚒️ Скрафтить всё доступное", callback_data="craft_all")],
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
    card = roll_card(weights={rarity: 1}, allowed_rarities=[rarity])
    result = add_card(p, card["id"])
    add_xp(p, 100)
    add_newbie_task_progress(p, "craft", 1)
    save_json(DATA_FILE, DATA)
    await callback.message.answer(
        f"⚒️ <b>Крафт завершён</b>\n\n🐉 {e(card['name'])}\n⭐ {rarity_label(card['rarity'])}\n{e(result)}",
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
            card = roll_card(weights={rarity: 1}, allowed_rarities=[rarity])
            add_card(p, card["id"])
            made.append(card)
            if is_owner(callback.from_user.id):
                break
    if not made:
        await callback.answer("Недостаточно фрагментов для крафта.", show_alert=True)
        return
    add_xp(p, 100 * len(made))
    add_newbie_task_progress(p, "craft", len(made))
    save_json(DATA_FILE, DATA)
    text = "⚒️ <b>Крафт всего завершён</b>\n\n"
    for c in made[:20]:
        text += f"🐉 {e(c['name'])} — {rarity_label(c['rarity'])}\n"
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
    users = [(uid, p) for uid, p in DATA.get("users", {}).items() if is_public_ranked(uid)]
    users_money = [(uid, p) for uid, p in users if not is_right_hand(uid)]
    def name_of(item):
        return item[1].get("name", item[0])
    top_power = sorted(users, key=lambda x: user_total_power(x[1]), reverse=True)[:5]
    top_fistiks = sorted(users_money, key=lambda x: x[1].get("fistiks", 0), reverse=True)[:5]
    top_arena = sorted(users, key=lambda x: x[1].get("wins", 0), reverse=True)[:5]
    top_multi = sorted(users, key=lambda x: x[1].get("wins", 0) * 10 + len(x[1].get("collection", {})) * 3 + x[1].get("xp", 0) // 50, reverse=True)[:5]
    def block(title, arr, value_fn):
        s = f"\n<b>{title}</b>\n"
        if not arr:
            return s + "пока пусто\n"
        for i, item in enumerate(arr, 1):
            s += f"{i}. {e(name_of(item))} — {value_fn(item)}\n"
        return s
    text = "🏆 <b>Рейтинг</b>\n"
    text += block("⚔️ Топ силы", top_power, lambda x: user_total_power(x[1]))
    text += block("💎 Топ фисташек", top_fistiks, lambda x: x[1].get("fistiks", 0))
    text += block("🌌 Топ мультивселенной", top_multi, lambda x: x[1].get("wins", 0) * 10 + len(x[1].get("collection", {})) * 3 + x[1].get("xp", 0) // 50)
    text += block("🥊 Топ арены", top_arena, lambda x: x[1].get("wins", 0))
    text += "\n🏰 <b>Топ кланов</b>\nСкоро."
    await message.answer(text, reply_markup=back_menu(), parse_mode="HTML")


def get_daily_event():
    idx = int(date.today().strftime("%Y%m%d")) % len(DAILY_EVENT_POOL)
    return DAILY_EVENT_POOL[idx]


def ensure_raid_state():
    DATA.setdefault("raid", {})
    raid = DATA["raid"]
    now = datetime.now()
    end_raw = raid.get("ends_at", "")
    expired = True
    if end_raw:
        try:
            expired = now >= datetime.fromisoformat(end_raw)
        except Exception:
            expired = True
    if not raid or expired or int(raid.get("hp_left", 0)) <= 0:
        if raid and not raid.get("settled") and raid.get("damage"):
            settle_raid_rewards(raid, "expired" if expired else "defeated")
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
            "boss_deck": pick_raid_boss_deck(),
        })
        save_json(DATA_FILE, DATA)
    return raid


def pick_raid_boss_deck():
    pool = [c for c in CARDS if c.get("rarity") in {"Эпический", "Легендарный", "Мифический"}]
    rng = random.Random(int(date.today().strftime("%Y%m%d")))
    rng.shuffle(pool)
    return [c["id"] for c in pool[:5]]


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
            "at": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
        })
        results.append((str(uid), rank, tier, int(dmg), fistiks, moon, pass_xp))
    raid["settled"] = True
    raid["settled_at"] = datetime.now().isoformat(timespec="seconds")
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
    p = get_user_data(user)
    event = get_daily_event()
    raid = ensure_raid_state()
    hp_left = int(raid.get("hp_left", 0))
    max_hp = int(raid.get("max_hp", 1))
    percent = max(0, hp_left) * 100 / max_hp
    ends = raid.get("ends_at", "")
    text = (
        "🏯 <b>Ивенты мультивселенной</b>\n\n"
        "Здесь нет лишних условий: забери событие дня, выбери команду и бей недельного босса. Все участники рейда автоматически попадают в рейтинг урона и получают награду по итогам недели.\n\n"
        f"🔥 <b>Ивент дня:</b> {e(event['name'])}\n"
        f"{e(event['desc'])}\n"
        f"Награда: +{event['coins']} 🐉 и +{event['pass_xp']} очков Боевого пропуска.\n\n"
        f"🐉 <b>Рейд-босс недели:</b> {e(raid['boss_name'])}\n"
        f"{e(raid['desc'])}\n"
        f"HP: <b>{hp_left:,}</b> / <b>{max_hp:,}</b> ({percent:.4f}%)\n".replace(",", " ") +
        f"До конца: <code>{e(ends[:16])}</code>\n\n"
        f"Твой урон: <b>{int(raid.get('damage', {}).get(str(user.id), 0)):,}</b>\n".replace(",", " ") +
        f"Твои очки турнира: <b>{p.get('tournament_points', 0)}</b>\n\n"
        "<b>Топ урона:</b>\n"
        f"{format_raid_top(raid)}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="🔥 Забрать ивент дня", callback_data="event_daily")],
        [button(text="🐉 Открыть рейд-босса", callback_data="raid_info")],
        [button(text="⚔️ Ударить рейд-босса", callback_data="raid_hit")],
        [button(text="🏆 Вступить в турнир", callback_data="tournament_join")],
        [button(text="⬅️ Режимы", callback_data="modes")],
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
    today = date.today().isoformat()
    event = get_daily_event()
    if p.get("last_event_daily") == today:
        await callback.answer("Ивент дня уже забран.", show_alert=True)
        return
    p["last_event_daily"] = today
    p["moon_coins"] = int(p.get("moon_coins", 0)) + int(event["coins"])
    p["pass_xp"] = int(p.get("pass_xp", 0)) + int(event["pass_xp"])
    save_json(DATA_FILE, DATA)
    await callback.message.answer(
        f"🔥 <b>{e(event['name'])}</b> выполнен: +{event['coins']} 🐉 и +{event['pass_xp']} очков Боевого пропуска.",
        reply_markup=back_menu(),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "raid_info")
async def raid_info_cb(callback: types.CallbackQuery):
    raid = ensure_raid_state()
    deck_lines = []
    for i, cid in enumerate(raid.get("boss_deck", []), 1):
        c = CARD_BY_ID.get(cid)
        if c:
            deck_lines.append(f"{i}. {rarity_label(c['rarity'])} {e(c['name'])} — {e(c.get('anime',''))}")
    hp_left = int(raid.get("hp_left", 0))
    max_hp = int(raid.get("max_hp", 1))
    text = (
        f"🐉 <b>{e(raid['boss_name'])}</b>\n\n"
        f"{e(raid['desc'])}\n\n"
        f"HP: <b>{hp_left:,}</b> / <b>{max_hp:,}</b>\n".replace(",", " ") +
        f"Защита: {e(raid['protection'])}\n\n"
        "Каждые 5 часов доступно до 3 ударов. Босс меняется раз в неделю, награды выдаются всем участникам рейтинга.\n\n"
        "<b>Колода босса:</b>\n"
        f"{chr(10).join(deck_lines) if deck_lines else 'скрыта'}\n\n"
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
    now = datetime.now()
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
    now = datetime.now()
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
    save_json(DATA_FILE, DATA)
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
        manual_team_drafts[str(callback.from_user.id)] = {"target": "raid", "cards": []}
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
    await callback.message.answer("🏆 Ты зарегистрирован в турнире сезона. Победы, рейд-урон и активность будут поднимать очки.", reply_markup=back_menu())
    await callback.answer()


@dp.callback_query(F.data == "cases")
async def cases(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    balance = int(p.get("moon_coins", 0))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text=f"⚡ Ивент-кейс — {CASE_PRICES['event']} 🐉", callback_data="case_open:event")],
        [button(text=f"🎉 Праздничный кейс — {CASE_PRICES['holiday']} 🐉", callback_data="case_open:holiday")],
        [button(text=f"🔴 Мифический кейс — {CASE_PRICES['mystic']} 🐉", callback_data="case_open:mystic")],
        [button(text="⬅️ Магазин / награды", callback_data="shop"), button(text="⬅️ Меню", callback_data="menu")],
    ])
    await callback.message.answer(
        f"🐉 <b>Кейсы</b>\n\n"
        f"Твоя валюта кейсов: <b>{balance}</b> 🐉\n\n"
        "🐉 Драконит выдаются через мультипасс, задания и ивенты.\n"
        "Кейсы не покупаются за фисташки.",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("case_open:"))
async def case_open(callback: types.CallbackQuery):
    kind = callback.data.split(":", 1)[1]
    weights = CASE_WEIGHTS.get(kind, RARITY_WEIGHTS)
    p = get_user_data(callback.from_user)
    cost = int(CASE_PRICES.get(kind, 5))
    if int(p.get("moon_coins", 0)) < cost and not is_owner(callback.from_user.id):
        await callback.answer(f"Не хватает драконита. Нужно {cost} 🐉.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["moon_coins"] = int(p.get("moon_coins", 0)) - cost

    got = []
    pulled = set()
    for _ in range(7):
        card, pity_note = roll_card_with_pity(p, weights=weights, exclude=pulled)
        pulled.add(card["id"])
        got.append((card, add_card(p, card["id"], 200) + pity_note))
    add_xp(p, 500)
    save_json(DATA_FILE, DATA)
    await send_pack_result(callback.message, CASE_NAMES.get(kind, f"Кейс {kind}"), got, p)
    await callback.answer()



PASS_FREE_REWARDS = {
    1: {"fistiks": 100},
    3: {"fistiks": 250},
    5: {"pack": "basic"},
    8: {"fistiks": 500},
    10: {"fragments": 50},
    12: {"moon_coins": 1},
    15: {"pack": "rare"},
    20: {"fistiks": 1200, "moon_coins": 2},
}

PASS_PREMIUM_REWARDS = {
    1: {"badge": "PREMIUM"},
    3: {"fistiks": 700},
    5: {"pack": "rare", "moon_coins": 2},
    10: {"fragments": 250, "moon_coins": 3},
    15: {"pack": "royal", "moon_coins": 4},
    20: {"fistiks": 3000, "moon_coins": 6},
    50: {"fistiks": 9000, "moon_coins": 15},
    100: {"fistiks": 25000, "moon_coins": 35},
}


NEWBIE_DAYS = 10
NEWBIE_TASKS = {
    "daily": {"title": "Забрать ежедневную награду", "target": 1, "reward": {"fistiks": 350, "pass_xp": 120}},
    "free_pack": {"title": "Открыть бесплатный сундук", "target": 1, "reward": {"fistiks": 300, "pass_xp": 100}},
    "chest": {"title": "Открыть любой сундук", "target": 1, "reward": {"fistiks": 450, "pass_xp": 130}},
    "battle": {"title": "Сыграть бой с ботом или игроком", "target": 1, "reward": {"fistiks": 600, "pass_xp": 170}},
    "craft": {"title": "Сделать 1 крафт", "target": 1, "reward": {"fistiks": 500, "pass_xp": 150}},
    "referral": {"title": "Привести 1 друга по ссылке", "target": 1, "reward": {"fistiks": 1200, "pass_xp": 250, "moon_coins": 2}},
}


def is_newbie_active(uid):
    if is_owner(uid):
        return False
    player = DATA.get("users", {}).get(str(uid), {})
    created = player.get("created_at") or datetime.now().isoformat()
    try:
        return datetime.now() <= datetime.fromisoformat(created) + timedelta(days=NEWBIE_DAYS)
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
    player.setdefault("system_inbox", []).append({"at": datetime.now().isoformat(timespec="seconds"), "text": line})
    if len(player.get("system_inbox", [])) > 20:
        del player["system_inbox"][:-20]
    return line


def add_newbie_task_progress(player, key, amount=1):
    if key not in NEWBIE_TASKS:
        return ""
    created = player.get("created_at") or datetime.now().isoformat()
    try:
        if datetime.now() > datetime.fromisoformat(created) + timedelta(days=NEWBIE_DAYS):
            return ""
    except Exception:
        pass
    progress = player.setdefault("newbie_progress", {})
    target = int(NEWBIE_TASKS[key]["target"])
    before = int(progress.get(key, 0))
    after = min(target, before + int(amount))
    progress[key] = after
    if before < target and after >= target:
        return grant_newbie_task_reward(player, key)
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
    created = p.get("created_at") or datetime.now().isoformat()
    try:
        expires = datetime.fromisoformat(created) + timedelta(days=NEWBIE_DAYS)
        left = expires - datetime.now()
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
    save_json(DATA_FILE, DATA)
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
        lines.append(f"{mark} День {i}: {rarity_label(c['rarity'])} {e(c['name'])} — {e(c.get('form',''))}")
    ready = last != date.today().isoformat() and day < len(LUFFY_PATH_CARDS)
    return "\n".join(lines), ready


async def send_luffy_path(message, user):
    p = get_user_data(user)
    p["luffy_intro_seen"] = True
    repair_luffy_progress(p)
    save_json(DATA_FILE, DATA)
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
    if p.get("last_luffy_claim") == date.today().isoformat():
        await callback.answer("Сегодняшняя форма уже забрана.", show_alert=True)
        return
    cid = LUFFY_PATH_CARDS[day]
    if cid not in CARD_BY_ID:
        await callback.answer("Карта дня не найдена.", show_alert=True)
        return
    p["last_luffy_claim"] = date.today().isoformat()
    p["luffy_day"] = day + 1
    if p["luffy_day"] >= len(LUFFY_PATH_CARDS):
        p["luffy_finished"] = True
    p.setdefault("luffy_claimed_cards", []).append(cid)
    result = add_card(p, cid, 50)
    p["fistiks"] = int(p.get("fistiks", 0)) + 150
    p["pass_xp"] = int(p.get("pass_xp", 0)) + 80
    save_json(DATA_FILE, DATA)
    c = CARD_BY_ID[cid]
    await callback.message.answer(
        f"{CE['luffy']} <b>Путь Луфи — день {day + 1}/10</b>\n\n"
        f"{rarity_label(c['rarity'])} <b>{e(c['name'])}</b>\n"
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
    today = date.today().isoformat()
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
        card = roll_card(weights={"Обычный": 500, "Редкий": 300, "Эпический": 160, "Мифический": 35, "Легендарный": 5})
        text.append(add_fragments(player, card["id"], amount))
    if "pack" in reward:
        pack = SHOP_PACKS.get(reward["pack"])
        if pack:
            pulled = set()
            for _ in range(pack["count"]):
                card = roll_card(weights=pack["weights"], exclude=pulled)
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
        premium = f"Premium до {cap if cap else 20} уровня"
    elif p.get("pass_purchase_request") == "paid_pending":
        premium = "оплачено, ждёт подтверждения"
    else:
        premium = "Free"
    request_state = p.get("pass_purchase_request", "")
    request_text = {
        "": "нет",
        "paid_pending": "оплачено, ждёт подтверждения",
        "activated": "активирован",
        "rejected_after_payment": "оплачено, но отклонено/заморожено",
        "paid": "оплачено",
    }.get(request_state, request_state or "нет")
    progress = min(100, int((int(p.get("pass_xp", 0)) / max(1, 25000)) * 100))
    text = (
        "🎟 <b>Мультипасс</b>\n\n"
        f"Игрок: <b>{e(p.get('name', user.full_name))}</b>\n"
        f"Тип пропуска: <b>{premium}</b>\n"
        f"Уровень: <b>{pass_level}/100</b>\n"
        f"Очки: <b>{p.get('pass_xp', 0)}</b>\n"
        f"Прогресс сезона: <b>{progress}%</b>\n"
        "Конец сезона: <b>по объявлению владельца</b>\n"
        f"Статус оплаты: <b>{request_text}</b>\n\n"
        f"Premium стоит <b>{PASS_PRICE_STARS}</b> Telegram Stars. После оплаты доступ подтверждает владелец."
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
    cost_dragonit = 5
    gain = 300
    if not is_owner(callback.from_user.id) and int(p.get("moon_coins", 0) or 0) < cost_dragonit:
        await callback.answer(f"Нужно {cost_dragonit} 🐉 Драконита.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["moon_coins"] = int(p.get("moon_coins", 0) or 0) - cost_dragonit
    p["pass_xp"] = int(p.get("pass_xp", 0) or 0) + gain
    save_json(DATA_FILE, DATA)
    await callback.message.answer(
        f"💳 <b>Уровень pass куплен</b>\n\n+{gain} очков мультипасса за {cost_dragonit} 🐉.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Мультипасс", callback_data="multipass")]]),
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
        [button(text="⬅️ Мультипасс", callback_data="multipass")],
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
    save_json(DATA_FILE, DATA)
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

    save_json(DATA_FILE, DATA)
    await callback.message.answer("🎁 <b>Награды мультипасса получены</b>\n\n" + "\n".join(granted), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "buy_pass_stars")
async def buy_pass_stars(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    if is_owner(callback.from_user.id):
        p["pass_premium"] = True
        p["pass_premium_cap"] = 100
        p["pass_xp"] = max(int(p.get("pass_xp", 0)), 25000)
        save_json(DATA_FILE, DATA)
        await callback.answer("У владельца премиум уже открыт.", show_alert=True)
        return
    if p.get("pass_premium"):
        await callback.answer("Премиум уже активен.", show_alert=True)
        return
    if p.get("pass_purchase_request") == "paid_pending":
        await callback.answer("Оплата уже получена. Если premium не открылся, напиши /paysupport.", show_alert=True)
        return
    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title="Премиум мультипасс",
            description="Премиум мультипасс за Telegram Stars. После оплаты заявка попадёт владельцу на ручное подтверждение.",
            payload=f"multipass_premium:{callback.from_user.id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Премиум мультипасс", amount=PASS_PRICE_STARS)],
        )
        await callback.message.answer(
            "⭐ Счёт отправлен. После оплаты заявка попадёт владельцу на ручное подтверждение.",
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
        save_json(DATA_FILE, DATA)
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
        save_json(DATA_FILE, DATA)
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
    return (
        getattr(successful_payment, "telegram_payment_charge_id", "")
        or getattr(successful_payment, "provider_payment_charge_id", "")
        or f"{successful_payment.invoice_payload}:{successful_payment.total_amount}"
    )


def payment_already_processed(player, payment_id):
    if not payment_id:
        return False
    return payment_id in set(map(str, player.setdefault("processed_payments", [])))


def record_payment(player, payment_id, kind, code, amount):
    player.setdefault("processed_payments", [])
    if payment_id and payment_id not in player["processed_payments"]:
        player["processed_payments"].append(payment_id)
        player["processed_payments"] = player["processed_payments"][-80:]
    player.setdefault("purchases", []).append({
        "id": payment_id,
        "kind": kind,
        "code": code,
        "amount": int(amount),
        "currency": PAYMENT_CURRENCY,
        "created_at": datetime.now().isoformat(),
    })
    player["purchases"] = player["purchases"][-120:]
    player["stars_earned"] = int(player.get("stars_earned", 0)) + int(amount)


@dp.pre_checkout_query()
async def pre_checkout_query(pre_checkout: types.PreCheckoutQuery):
    payload = pre_checkout.invoice_payload
    parsed = parse_payment_payload(payload)
    expected = expected_payment_amount(payload)
    if not parsed or expected is None:
        await pre_checkout.answer(ok=False, error_message="Неизвестный платёж. Открой счёт заново из бота.")
        return
    if parsed["user_id"] != str(pre_checkout.from_user.id):
        await pre_checkout.answer(ok=False, error_message="Этот счёт создан для другого игрока.")
        return
    if pre_checkout.currency != PAYMENT_CURRENCY or int(pre_checkout.total_amount) != expected:
        await pre_checkout.answer(ok=False, error_message="Цена платежа не совпала. Открой счёт заново.")
        return
    await pre_checkout.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payment = message.successful_payment
    payload = payment.invoice_payload
    parsed = parse_payment_payload(payload)
    expected = expected_payment_amount(payload)
    p = get_user_data(message.from_user)
    if not parsed or expected is None:
        await message.answer("✅ Платёж получен, но payload неизвестен. Напиши /paysupport.", reply_markup=back_menu())
        return
    if parsed["user_id"] != str(message.from_user.id) or payment.currency != PAYMENT_CURRENCY or int(payment.total_amount) != expected:
        await message.answer("⚠️ Платёж получен, но проверка суммы/игрока не прошла. Напиши /paysupport.", reply_markup=back_menu())
        logger.warning("Payment validation failed user=%s payload=%s amount=%s currency=%s", message.from_user.id, payload, payment.total_amount, payment.currency)
        return

    pay_id = payment_id_from_successful(payment)
    if payment_already_processed(p, pay_id):
        await message.answer("✅ Этот платёж уже обработан. Повторная выдача не нужна.", reply_markup=back_menu())
        return

    if parsed["kind"] == "multipass":
        p["pass_purchase_request"] = "paid_pending"
        record_payment(p, pay_id, "multipass", parsed["code"], payment.total_amount)
        save_json(DATA_FILE, DATA)
        await message.answer(
            "✅ <b>Оплата мультипасса получена.</b>\n\nPremium-доступ ждёт подтверждения владельца.",
            reply_markup=back_menu(),
            parse_mode="HTML"
        )
        await notify_owner_purchase(
            message.from_user,
            "⭐ <b>Оплата мультипасса ждёт подтверждения</b>\n\n"
            f"Игрок: <b>{e(p.get('name', message.from_user.full_name))}</b>\n"
            f"ID: <code>{message.from_user.id}</code>\n"
            f"Stars: <b>{payment.total_amount}</b>\n"
            f"Payment ID: <code>{e(pay_id)}</code>\n\n"
            "Админка → Оплаты pass."
        )
        return

    if parsed["kind"] == "star_pack":
        pack_code = parsed["code"]
        pack = STAR_PACKS.get(pack_code)
        if not pack:
            await message.answer("✅ Платёж получен, но набор не найден. Напиши /paysupport.", reply_markup=back_menu())
            return
        reward_text = grant_star_pack_reward(p, pack_code)
        record_payment(p, pay_id, "star_pack", pack_code, payment.total_amount)
        save_json(DATA_FILE, DATA)
        await message.answer(
            "✅ <b>Оплата получена. Набор выдан.</b>\n\n" + reward_text,
            reply_markup=back_menu(),
            parse_mode="HTML"
        )
        await notify_owner_purchase(
            message.from_user,
            "⭐ <b>Покупка Stars-набора</b>\n\n"
            f"Игрок: <b>{e(p.get('name', message.from_user.full_name))}</b>\n"
            f"ID: <code>{message.from_user.id}</code>\n"
            f"Набор: <b>{e(pack['title'])}</b>\n"
            f"Stars: <b>{payment.total_amount}</b>\n"
            f"Payment ID: <code>{e(pay_id)}</code>"
        )
        return



async def send_modes(message, user):
    p = get_user_data(user)
    source_names = {"deck": "моя колода", "random_bot": "авто-колода", "manual": "ручной выбор"}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="⚔️ Арена", callback_data="battle:start"), button(text="🌐 Онлайн", callback_data="online_search")],
        [button(text="🃏 Колоды", callback_data="deck"), button(text="🎪 Ивенты", callback_data="events")],
        [button(text="👹 Рейд", callback_data="raid_info"), button(text="⚙️ Выбор боя", callback_data="pvp_source_menu")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    title, body = SECTION_HINTS["modes"]
    await message.answer(
        f"{ui_box(title, body)}\n\n"
        f"{CE['arena']} <b>Арена</b> — одиночный бой на поле, где локация даёт бонус и может резать слабые стороны.\n"
        f"{CE['online']} <b>Онлайн</b> — PvP против живого игрока: решают колода, стартовый боец и тактика.\n"
        f"{CE['deck']} <b>Колоды</b> — собери пятёрку персонажей под роль, синергию и стиль боя.\n"
        f"{CE['events']} <b>Ивенты</b> — временные события, задания дня, кейсы и редкие награды.\n"
        f"{CE['raid']} <b>Рейд</b> — общий босс мультивселенной, рейтинг урона и финальные призы.\n\n"
        f"{CE['battle_choice']} Выбор боя: <b>{e(source_names.get(p.get('pvp_team_source', 'deck'), 'моя колода'))}</b>.",
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
    save_json(DATA_FILE, DATA)
    if source == "manual":
        manual_team_drafts[str(callback.from_user.id)] = {"target": "pvp", "cards": []}
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
    save_json(DATA_FILE, DATA)
    await message.answer(f"✅ Ник изменён на: <b>{e(nick)}</b>", reply_markup=back_menu(), parse_mode="HTML")


async def send_mega_open(message, user):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="📦 5 обычных сундуков", callback_data="mega_buy:basic:5")],
        [button(text="💎 5 усиленных сундуков", callback_data="mega_buy:rare:5")],
        [button(text="👑 3 королевских сундука", callback_data="mega_buy:royal:3")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(
        "🎴 <b>Мега-открытие</b>\n\n"
        "Открывает сразу несколько сундуков одним нажатием.\n"
        "Удобно, когда накопилось много фисташек 💎.",
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
        _, kind, amount_s = callback.data.split(":")
        amount = int(amount_s)
    except Exception:
        await callback.answer("Ошибка мега-открытия.", show_alert=True)
        return
    if kind not in SHOP_PACKS:
        await callback.answer("Такого сундука нет.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    pack = SHOP_PACKS[kind]
    cost_one, _ = discounted_cost(callback.from_user, pack["base_cost"])
    total_cost = cost_one * amount
    if p["fistiks"] < total_cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает фисташек.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["fistiks"] -= total_cost
    got = []
    for _ in range(amount):
        pulled = set()
        for _j in range(pack["count"]):
            card, result = pull_pack_reward(p, pack["weights"], exclude=pulled)
            pulled.add(card["id"])
            got.append((card, result))
    add_xp(p, 60 * amount)
    add_pass_task_progress(p, "chest", amount)
    add_newbie_task_progress(p, "chest", amount)
    save_json(DATA_FILE, DATA)
    await send_pack_result(callback.message, f"Мега-открытие: {pack['name']} x{amount}", got, p)
    await callback.answer()


def _queue_uid(item):
    if isinstance(item, dict):
        return str(item.get("uid", ""))
    return str(item)


def cleanup_online_queue():
    now = datetime.now()
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
                expired = (now - datetime.fromisoformat(joined_raw)).total_seconds() > ONLINE_QUEUE_TTL_SECONDS
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
            "created_at": datetime.now().isoformat(),
        }
        return bid
    online_queue.append({"uid": uid, "joined_at": datetime.now().isoformat()})
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
        if uid.isdigit() and uid not in DATA["users"]:
            DATA["users"][uid] = {
                "name": "Владелец" if uid in owner_ids() else "Правая рука",
                "fistiks": 0,
                "xp": 0,
                "wins": 0,
                "losses": 0,
                "battles": 0,
                "collection": {},
                "badges": ["DEV"] if uid in owner_ids() else ["RIGHT_HAND"],
                "last_seen": "ещё не обновлялся после patch6",
                "created_at": datetime.now().isoformat(),
                "banned": False,
                "frozen": False,
                "moon_coins": 0,
            }


def short_user_line(uid, p, index=0):
    name = p.get("name") or uid
    flags = []
    if uid in owner_ids():
        flags.append("👑")
    if uid in right_hand_ids():
        flags.append("🤝")
    if p.get("banned"):
        flags.append("⛔")
    if p.get("frozen"):
        flags.append("🧊")
    last = p.get("last_seen", "нет")
    flag_text = " ".join(flags)
    uname = f"@{p.get('username')}" if p.get("username") else "без username"
    return f"{index}. {flag_text} <b>{e(name)}</b> | {e(uname)} | карт {len(p.get('collection', {}))} | боёв {p.get('battles', 0)} | вход {e(str(last)[:16])}"


async def send_admin_panel(message, user):
    if not is_owner(user.id):
        await message.answer("⛔ Только владелец мультивселенной имеет доступ.")
        return
    ensure_admin_known_users()
    repair_all_luffy_progress()
    users = DATA.get("users", {})
    all_items = all_player_items()
    live_items = active_player_items()
    total_all = len(all_items)
    live = len(live_items)
    blocked = sum(1 for _, p in all_items if p.get("bot_blocked"))
    banned = sum(1 for _, p in all_items if p.get("banned"))
    frozen = sum(1 for _, p in all_items if p.get("frozen"))
    online = sum(1 for uid, _ in all_items if is_online(uid))
    now = datetime.now()
    new_24h = 0
    for _, p in all_items:
        created = _parse_iso_datetime(p.get("created_at", ""))
        if created and now - created <= timedelta(days=1):
            new_24h += 1
    paid_pending = sum(1 for _, p in all_items if p.get("pass_purchase_request") == "paid_pending")
    text = (
        "🛠 <b>Командный центр владельца</b>\n\n"
        f"👥 Всего игроков за всё время: <b>{total_all}</b>\n"
        f"🧬 Активных/живых игроков: <b>{live}</b>\n"
        f"🟢 Онлайн за 10 мин: <b>{online}</b>\n"
        f"🚫 Заблокировали бота: <b>{blocked}</b>\n"
        f"⛔ Забанены: <b>{banned}</b>\n"
        f"🧊 Заморожены: <b>{frozen}</b>\n"
        f"🆕 Новые за сутки: <b>{new_24h}</b>\n"
        f"⭐ Оплат pass на подтверждении: <b>{paid_pending}</b>\n\n"
        "Безопасные команды:\n"
        "<code>/user ID</code> — открыть аккаунт\n"
        "<code>/ban ID</code> / <code>/unban ID</code> — бан/разбан\n"
        "<code>/freeze ID</code> / <code>/unfreeze ID</code> — заморозка\n"
        "<code>/givef ID AMOUNT</code> — выдать фисташки\n"
        "<code>/givemoon ID AMOUNT</code> — выдать драконит\n"
        "<code>/givecard ID CARD_ID</code> — выдать карту\n"
        "<code>/compensate_patch15</code> — разовая компенсация PATCH15\n"
        "<code>/compensate_patch16</code> — благодарность за ожидание PATCH16"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [button(text="👥 Все игроки", callback_data="admin_users"), button(text="⭐ Оплаты pass", callback_data="admin_payments")],
        [button(text="🎁 Компенсация", callback_data="admin_compensation_info"), button(text="🧠 Хранилище", callback_data="admin_storage")],
        [button(text="⬅️ Меню", callback_data="menu")],
    ])
    save_json(DATA_FILE, DATA)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

async def send_admin_users(message, page=0):
    ensure_admin_known_users()
    repair_all_luffy_progress()
    items = all_player_items()
    items.sort(key=lambda x: (x[0] not in owner_ids(), x[0] not in right_hand_ids(), x[1].get("last_seen", "")), reverse=False)
    per_page = 8
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), pages - 1))
    chunk = items[page * per_page:(page + 1) * per_page]
    text = (
        f"👥 <b>Все игроки бота</b> — страница {page + 1}/{pages}\n\n"
        "ID скрыт в списке. Нажми на имя, чтобы открыть карточку внутри бота.\n\n"
    )
    rows = []
    if not chunk:
        text += "Игроки не найдены."
    for i, (uid, p) in enumerate(chunk, page * per_page + 1):
        text += short_user_line(uid, p, i) + "\n"
        display = str(p.get('name') or p.get('username') or 'Игрок')[:28]
        rows.append([button(text=f"📊 {display}", callback_data=f"admin_user:{uid}")])
    nav = []
    if page > 0:
        nav.append(button(text="⬅️", callback_data=f"admin_users:{page-1}"))
    if page < pages - 1:
        nav.append(button(text="➡️", callback_data=f"admin_users:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([button(text="🔄 Обновить", callback_data=f"admin_users:{page}"), button(text="⬅️ Админ-панель", callback_data="admin")])
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
        f"{PISTACHIOS_LABEL}: <b>{p.get('fistiks', 0)}</b>\n"
        f"{DRAGONITE_LABEL}: <b>{p.get('moon_coins', 0)}</b>\n"
        f"Карт: <b>{len(p.get('collection', {}))}/{len(CARDS)}</b>\n"
        f"Боёв: <b>{p.get('battles', 0)}</b> | Побед: <b>{p.get('wins', 0)}</b> | Поражений: <b>{p.get('losses', 0)}</b>\n"
        f"Мультипасс: <b>{'premium' if p.get('pass_premium') else p.get('pass_purchase_request', 'нет')}</b> | cap {p.get('pass_premium_cap', 0)}\n"
        f"Бан: <b>{'да' if p.get('banned') else 'нет'}</b> | Заморозка: <b>{'да' if p.get('frozen') else 'нет'}</b>\n"
        f"Уведомления сундука: <b>{'вкл' if p.get('notify_free_pack', True) else 'выкл'}</b>\n"
        f"Последний вход: <code>{e(p.get('last_seen', 'нет'))}</code>"
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
        [button(text="🆔 Показать ID", callback_data=f"admin_show_id:{uid}")],
        [button(text="🗑 Удалить…", callback_data=f"admin_delete_ask:{uid}")],
        [button(text="⬅️ Игроки", callback_data="admin_users"), button(text="⬅️ Админ", callback_data="admin")],
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
    await callback.message.answer(storage_report_text(), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Админ-панель", callback_data="admin")]]), parse_mode="HTML")
    await callback.answer()


@dp.message(Command("storage"))
async def storage_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    await message.answer(storage_report_text(), parse_mode="HTML")



@dp.callback_query(F.data == "admin_compensation_info")
async def admin_compensation_info_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await callback.message.answer(
        "🎁 <b>Компенсации патчей</b>\n\n"
        "PATCH15:\n<code>/compensate_patch15</code>\n"
        f"+{COMPENSATION_FISTIKS} 💎 +{COMPENSATION_MOON_COINS} 🐉 +{COMPENSATION_PASS_XP} pass XP\n\n"
        "PATCH16:\n<code>/compensate_patch16</code>\n"
        f"+{PATCH16_COMPENSATION_FISTIKS} 💎 +{PATCH16_COMPENSATION_MOON_COINS} 🐉 +{PATCH16_COMPENSATION_PASS_XP} pass XP\n\n"
        "Owner пропускается для чистой статистики.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[button(text="⬅️ Админ-панель", callback_data="admin")]]),
        parse_mode="HTML"
    )
    await callback.answer()


async def run_patch15_compensation(message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    sent = 0
    rewarded = 0
    skipped_owner = 0
    blocked = 0
    for uid, p in list(DATA.get("users", {}).items()):
        if str(uid) in owner_ids():
            skipped_owner += 1
            continue
        comps = p.setdefault("compensations", [])
        if PATCH15_COMPENSATION_KEY in comps:
            continue
        p["fistiks"] = int(p.get("fistiks", 0)) + COMPENSATION_FISTIKS
        p["moon_coins"] = int(p.get("moon_coins", 0)) + COMPENSATION_MOON_COINS
        p["pass_xp"] = int(p.get("pass_xp", 0)) + COMPENSATION_PASS_XP
        comps.append(PATCH15_COMPENSATION_KEY)
        rewarded += 1
        try:
            await bot.send_message(
                int(uid),
                f"{CE['start']} <b>Компенсация PATCH15</b>\n\n"
                "Извиняемся за сбой прогресса и нестабильные обновления.\n"
                "Хранилище усилено, а твой аккаунт получил награду.\n\n"
                f"+{COMPENSATION_FISTIKS} 💎 Фисташек\n"
                f"+{COMPENSATION_MOON_COINS} 🐉 Драконита\n"
                f"+{COMPENSATION_PASS_XP} очков мультипасса",
                parse_mode="HTML"
            )
            sent += 1
        except Exception as ex:
            logger.debug("PATCH15 compensation message failed for %s: %s", uid, ex)
            if should_mark_bot_unreachable(ex):
                p["bot_blocked"] = True
                blocked += 1
    save_json(DATA_FILE, DATA)
    await message.answer(
        f"✅ PATCH15 компенсация обработана.\n"
        f"Начислено игрокам: {rewarded}\n"
        f"Сообщений отправлено: {sent}\n"
        f"Owner пропущен: {skipped_owner}\n"
        f"Bot blocked отмечено: {blocked}"
    )


@dp.message(Command("compensate_patch15"))
async def compensate_patch15_cmd(message: types.Message):
    await run_patch15_compensation(message)


async def run_patch16_compensation(message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    sent = 0
    rewarded = 0
    skipped_owner = 0
    blocked = 0
    for uid, p in list(DATA.get("users", {}).items()):
        if str(uid) in owner_ids():
            skipped_owner += 1
            continue
        comps = p.setdefault("compensations", [])
        if PATCH16_COMPENSATION_KEY in comps:
            continue
        p["fistiks"] = int(p.get("fistiks", 0)) + PATCH16_COMPENSATION_FISTIKS
        p["moon_coins"] = int(p.get("moon_coins", 0)) + PATCH16_COMPENSATION_MOON_COINS
        p["pass_xp"] = int(p.get("pass_xp", 0)) + PATCH16_COMPENSATION_PASS_XP
        comps.append(PATCH16_COMPENSATION_KEY)
        rewarded += 1
        try:
            await bot.send_message(
                int(uid),
                f"{CE['start']} <b>Подарок PATCH16</b>\n\n"
                "Спасибо, что ждал обновление мультивселенной. Мы усилили правила, рейд, коллекцию, фрагменты, артефакты и визуальный стиль.\n\n"
                f"+{PATCH16_COMPENSATION_FISTIKS} 💎 Фисташек\n"
                f"+{PATCH16_COMPENSATION_MOON_COINS} 🐉 Драконита\n"
                f"+{PATCH16_COMPENSATION_PASS_XP} очков мультипасса",
                parse_mode="HTML"
            )
            sent += 1
        except Exception as ex:
            logger.debug("PATCH16 compensation message failed for %s: %s", uid, ex)
            if should_mark_bot_unreachable(ex):
                p["bot_blocked"] = True
                blocked += 1
    save_json(DATA_FILE, DATA)
    await message.answer(
        f"✅ PATCH16 компенсация обработана.\n"
        f"Начислено игрокам: {rewarded}\n"
        f"Сообщений отправлено: {sent}\n"
        f"Owner пропущен: {skipped_owner}\n"
        f"Bot blocked отмечено: {blocked}"
    )


@dp.message(Command("compensate_patch16"))
async def compensate_patch16_cmd(message: types.Message):
    await run_patch16_compensation(message)


@dp.message(Command("compensate_progress_bug"))
async def compensate_progress_bug_cmd(message: types.Message):
    await run_patch15_compensation(message)


@dp.message(Command("compensate_patch14"))
async def compensate_patch14_cmd(message: types.Message):
    await run_patch15_compensation(message)

@dp.callback_query(F.data.startswith("admin_users"))
async def admin_users_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    parts = callback.data.split(":")
    page = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    try:
        await send_admin_users(callback.message, page)
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
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


@dp.message(Command("givef"))
async def givef_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid, amount_s = parse_two_args(message)
    if not uid or uid not in DATA.get("users", {}) or not amount_s.lstrip('-').isdigit():
        await message.answer("Формат: /givef ID AMOUNT")
        return
    amount = int(amount_s)
    DATA["users"][uid]["fistiks"] = int(DATA["users"][uid].get("fistiks", 0)) + amount
    save_json(DATA_FILE, DATA)
    await notify_admin_grant(uid, "💎 Фисташек", amount)
    await message.answer(f"💎 Игроку <code>{uid}</code> выдано {amount} фисташек.", parse_mode="HTML")


@dp.message(Command("givemoon"))
async def givemoon_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid, amount_s = parse_two_args(message)
    if not uid or uid not in DATA.get("users", {}) or not amount_s.lstrip('-').isdigit():
        await message.answer("Формат: /givemoon ID AMOUNT")
        return
    amount = int(amount_s)
    DATA["users"][uid]["moon_coins"] = int(DATA["users"][uid].get("moon_coins", 0)) + amount
    save_json(DATA_FILE, DATA)
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
    save_json(DATA_FILE, DATA)
    await message.answer(f"🃏 Игроку <code>{uid}</code>: {e(result)}", parse_mode="HTML")


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
        save_json(DATA_FILE, DATA)
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
        save_json(DATA_FILE, DATA)
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
        save_json(DATA_FILE, DATA)
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
        save_json(DATA_FILE, DATA)
    await send_admin_user(callback.message, uid)
    await callback.answer("Аккаунт разморожен.")


@dp.callback_query(F.data.startswith("admin_givef:"))
async def admin_givef_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, uid, amount_s = callback.data.split(":")
    if uid in DATA.get("users", {}):
        DATA["users"][uid]["fistiks"] = int(DATA["users"][uid].get("fistiks", 0)) + int(amount_s)
        save_json(DATA_FILE, DATA)
        await notify_admin_grant(uid, "💎 Фисташек", int(amount_s))
    await send_admin_user(callback.message, uid)
    await callback.answer("Фисташки выданы.")


@dp.callback_query(F.data.startswith("admin_givemoon:"))
async def admin_givemoon_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, uid, amount_s = callback.data.split(":")
    if uid in DATA.get("users", {}):
        DATA["users"][uid]["moon_coins"] = int(DATA["users"][uid].get("moon_coins", 0)) + int(amount_s)
        save_json(DATA_FILE, DATA)
        await notify_admin_grant(uid, "🐉 Драконита", int(amount_s))
    await send_admin_user(callback.message, uid)
    await callback.answer("Драконит выданы.")


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
    player = DATA.get("users", {}).get(uid)
    if player:
        DATA.setdefault("deleted_users", {})[uid] = copy.deepcopy(player)
        DATA["users"][uid]["deleted"] = True
        DATA["users"][uid]["frozen"] = True
    save_json(DATA_FILE, DATA)
    await callback.message.answer(f"🗑 Аккаунт <code>{uid}</code> помечен как удалённый и сохранён в архиве. Данные не стираются физически.", reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer("Архивировано.")


@dp.callback_query(F.data == "noop")
async def noop(callback: types.CallbackQuery):
    await callback.answer()


@dp.message()
async def unknown(message: types.Message):
    await message.answer("Напиши /start или выбери команду в меню.", reply_markup=main_menu(message.from_user.id))


async def free_pack_notifier():
    # Раз в 3 часа напоминает только тем пользователям, у кого сундук реально доступен.
    await asyncio.sleep(30)
    while True:
        try:
            now = datetime.now()
            changed = False
            for uid, player in list(DATA.get("users", {}).items()):
                if not player.get("notify_free_pack", True) or player.get("banned") or player.get("frozen"):
                    continue

                last_pack = player.get("last_free_pack", "")
                if last_pack:
                    try:
                        if now < datetime.fromisoformat(last_pack) + timedelta(hours=3):
                            continue
                    except Exception:
                        pass

                last_notice = player.get("last_free_notice", "")
                if last_notice:
                    try:
                        if now < datetime.fromisoformat(last_notice) + timedelta(hours=3):
                            continue
                    except Exception:
                        pass

                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [button(text="🆓 Забрать бесплатный сундук", callback_data="pack_info:free")]
                ])
                try:
                    await bot.send_message(
                        int(uid),
                        "🎁 Бесплатный сундук снова доступен. Забери карту и усили коллекцию.",
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
                save_json(DATA_FILE, DATA)
        except Exception as ex:
            logger.exception("free_pack_notifier failed: %s", ex)
        await asyncio.sleep(3 * 60 * 60)


async def luffy_path_notifier():
    """PATCH14: ежедневный спам Пути Луфи отключён. Раздел доступен по кнопке."""
    return

async def health_handler(request):
    return web.Response(text="Anime Battle bot is running")


async def start_health_server():
    port = int(os.environ.get("PORT", "10000"))
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"WEB HEALTH SERVER STARTED ON PORT {port}")


async def main():
    print("BOT STARTED. Do not close this window.")
    ensure_media_packs_extracted()
    ensure_generated_arena_media()
    repair_all_luffy_progress()
    await start_health_server()
    await set_commands()
    await set_bot_public_description()
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(free_pack_notifier())
    asyncio.create_task(luffy_path_notifier())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

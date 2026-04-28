
import asyncio
import json
import os
import random
import string
from datetime import datetime, date, timedelta
from pathlib import Path
from html import escape

from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, FSInputFile, LabeledPrice
from aiohttp import web

TOKEN_FILE = "token.txt"
DATA_FILE = "anime_battle_data.json"
CARDS_FILE = "cards.json"
PROMO_FILE = "promo_codes.json"
OWNER_FILE = "owner_ids.txt"
RIGHT_HAND_FILE = "right_hand_ids.txt"
MEDIA_DIR = Path("media")

MAX_LEVEL = 100
CARD_UNLOCK_FRAGMENTS = 100
FRIEND_ID_DEFAULT = "527802531"
CHOICE_TIMEOUT_SECONDS = 20
CHOICE_WARN_10_AFTER = 10
CHOICE_WARN_5_AFTER = 15
PASS_PRICE_STARS = 199
MOON_EMOJI = "🌙"
CASE_PRICES = {"event": 3, "holiday": 5, "mystic": 12}
CASE_NAMES = {"event": "Ивент-кейс", "holiday": "Праздничный кейс", "mystic": "Мифический кейс"}

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

RARITY_EMOJI = {
    "Обычный": "⚪",
    "Редкий": "🔵",
    "Эпический": "🟣",
    "Легендарный": "🟡",
    "Мифический": "🔴",
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
    "premium": {"title": "Премиум", "emoji": "👑", "cost": 5000, "desc": "тестовый премиум-знак профиля"},
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
    {"name": "Печать защиты", "text": "снижает риск мгновенного нокаута", "delta": {"durability": 14}},
    {"name": "Клинок разрыва", "text": "лучше пробивает живучих врагов", "delta": {"power": 12, "hax": 4}},
    {"name": "Талисман фокуса", "text": "усиливает концентрацию и контроль", "delta": {"iq": 8, "hax": 6}},
    {"name": "Сапоги рывка", "text": "ускоряет первый вход в бой", "delta": {"speed": 12}},
    {"name": "Командный маяк", "text": "усиливает командную синергию", "delta": {"team": 12}},
    {"name": "Антииллюзорная метка", "text": "помогает против ментального контроля", "delta": {"hax": 10}},
    {"name": "Потара", "text": "даёт шанс на временное слияние и резкий скачок силы", "delta": {"power": 20, "team": 12}},
    {"name": "Fusion Dance", "text": "если команда проигрывает, открывает короткое окно синхронного рывка", "delta": {"power": 18, "speed": 14}},
    {"name": "Сензу", "text": "частично восстанавливает тело после тяжёлого удара", "delta": {"durability": 24}},
    {"name": "Печать луны", "text": "усиливает бойца, который отстаёт по очкам", "delta": {"hax": 14, "iq": 10}},
    {"name": "Осколок Хогёку", "text": "помогает пережить критический момент и продолжить бой", "delta": {"durability": 12, "hax": 12}},
]

active_battles = {}
active_pvp = {}
choice_timers = {}
online_queue = []


def e(text):
    return escape(str(text), quote=False)


def load_json(path, default):
    p = Path(path)
    if not p.exists():
        p.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path, obj):
    Path(path).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


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
    return read_ids(OWNER_FILE)


def right_hand_ids():
    return read_ids(RIGHT_HAND_FILE)


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
DATA = load_json(DATA_FILE, {"users": {}, "friend_invites": {}, "friends": {}})

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
                    "newbie_claim", "daily", "admin_ban:", "admin_unban:", "admin_delete:",
                )
                delete_exact = {
                    "menu", "profile", "profile_stats", "profile_badges", "modes", "shop",
                    "chests", "rules", "multipass", "deck", "pvp_source_menu", "newbie_start",
                    "battle:start", "online_search", "cases", "admin", "admin_users",
                }
                delete_prefixes = (
                    "pack_info:", "collection:page:", "card:", "battle:arena:", "battle:diff:",
                    "admin_user:", "pvp_source:",
                )
                if not data_value.startswith(keep_prefixes) and (
                    data_value in delete_exact or data_value.startswith(delete_prefixes)
                ):
                    await event.message.delete()
        except Exception:
            pass
        return await handler(event, data)


dp.callback_query.middleware(AutoCleanCallbackMiddleware())


def is_user_banned_id(user_id):
    if is_owner(user_id):
        return False
    return bool(DATA.get("users", {}).get(str(user_id), {}).get("banned", False))


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


def rarity_label(rarity):
    return f"{RARITY_EMOJI.get(rarity, '⚪')} {rarity}"


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
    for cid in list(collection.keys()):
        if cid not in CARD_BY_ID:
            del collection[cid]
            continue
        item = collection[cid]
        item.setdefault("count", 0)
        item.setdefault("shards", 0)
        item.setdefault("level", 1)
        item.setdefault("unlocked", item.get("count", 0) > 0)
        if item["level"] > MAX_LEVEL:
            item["level"] = MAX_LEVEL
        if item["level"] < 1:
            item["level"] = 1


def get_user_data(user):
    uid = str(user.id)
    if uid not in DATA["users"]:
        DATA["users"][uid] = {
            "name": user.full_name,
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
            "banned": False,
            "pass_premium_cap": 0,
            "deck": [],
            "auto_team": True,
            "pass_daily_date": "",
            "pass_task_progress": {},
            "pass_task_claimed": [],
            "pass_purchase_request": "",
            "created_at": datetime.now().isoformat(),
            "newbie_claimed": [],
            "newbie_progress": {},
            "pvp_team_source": "deck",
        }
        # Стартовая коллекция: чтобы новичок мог сразу войти в арену своими картами.
        starter_ids = ["levi_peak", "mikasa", "kirito_alicization", "gon_base", "tanjiro_sun"]
        for sid in starter_ids:
            if sid in CARD_BY_ID:
                DATA["users"][uid]["collection"][sid] = {"count": 1, "shards": 0, "level": 1, "unlocked": True}
    player = DATA["users"][uid]
    if "fistiks" not in player:
        player["fistiks"] = player.get("coins", 250)
    for k, v in {
        "xp": 0, "badges": [], "premium": False, "used_promos": [], "last_daily": "",
        "last_free_pack": "", "free_pack_notified": False, "last_free_notice": "", "ref_by": "", "ref_count": 0,
        "wins": 0, "losses": 0, "battles": 0, "last_seen": "", "ref_earned": 0, "nickname": "", "pass_xp": 0, "pass_premium": False, "claimed_pass_free": [], "claimed_pass_premium": [], "stars_earned": 0, "moon_coins": 0, "banned": False, "pass_premium_cap": 0, "deck": [], "auto_team": True, "pass_daily_date": "", "pass_task_progress": {}, "pass_task_claimed": [], "pass_purchase_request": "", "created_at": datetime.now().isoformat(), "newbie_claimed": [], "newbie_progress": {}, "pvp_team_source": "deck",
    }.items():
        player.setdefault(k, v)
    if player.get("nickname"):
        player["name"] = player["nickname"]
    else:
        player["name"] = user.full_name
    player["last_seen"] = datetime.now().isoformat()
    normalize_collection(player)

    if is_owner(user.id):
        # Владелец: ник не трогаем, но выдаём абсолютный максимум и отдельные знаки.
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
        player["creator_role"] = "👑 Владелец мультивселенной"
        player["creator_aura"] = "♾ Абсолютный знак создателя"
        for badge in ["DEV", "ROMA_OWNER", "IT_ARCHITECT", "ABSOLUTE_MAX"]:
            if badge not in player["badges"]:
                player["badges"].append(badge)
        for cid in CARD_BY_ID:
            # У владельца должна быть одна чистая копия каждой карты, без визуальных дублей.
            player["collection"][cid] = {"count": 1, "shards": 999999, "level": MAX_LEVEL, "unlocked": True}
    elif is_right_hand(user.id):
        player["fistiks"] = max(player.get("fistiks", 0), 5000000)
        player["moon_coins"] = max(player.get("moon_coins", 0), 250)
        player["xp"] = max(player.get("xp", 0), 250000)
        player["wins"] = max(player.get("wins", 0), 250)
        player["battles"] = max(player.get("battles", 0), 300)
        if "RIGHT_HAND" not in player["badges"]:
            player["badges"].append("RIGHT_HAND")
    save_json(DATA_FILE, DATA)
    return player


def main_menu(user_id=None):
    rows = [
        [InlineKeyboardButton(text="🎮 Режимы", callback_data="modes"), InlineKeyboardButton(text="🃏 Коллекция", callback_data="collection:page:0")],
        [InlineKeyboardButton(text="📦 Магазин / награды", callback_data="shop"), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="⚒️ Крафт", callback_data="craft"), InlineKeyboardButton(text="🎟 Мультипасс", callback_data="multipass")],
    ]
    if user_id and is_newbie_active(user_id):
        rows.append([InlineKeyboardButton(text="🚀 Старт новичка", callback_data="newbie_start")])
    rows.append([InlineKeyboardButton(text="🌙 Кейсы", callback_data="cases")])
    if user_id and is_owner(user_id):
        rows.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")])
    rows.append([InlineKeyboardButton(text="📜 Правила", callback_data="rules")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu")]])


def profile_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="profile_stats"), InlineKeyboardButton(text="🏷 Знаки", callback_data="profile_badges")],
        [InlineKeyboardButton(text="👥 Друзья", callback_data="friends"), InlineKeyboardButton(text="✏️ Ник", callback_data="nick_help")],
        [InlineKeyboardButton(text="📜 Правила", callback_data="rules")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])


def shop_menu():
    rows = [
        [InlineKeyboardButton(text="🧰 Сундуки", callback_data="chests"), InlineKeyboardButton(text="🎴 Мега-открытие", callback_data="mega_open")],
        [InlineKeyboardButton(text="🎁 Награда", callback_data="daily"), InlineKeyboardButton(text="🏆 Рейтинг", callback_data="rating")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo_help"), InlineKeyboardButton(text="🏷 Знаки", callback_data="badges_shop")],
        [InlineKeyboardButton(text="🎟 Мультипасс", callback_data="multipass"), InlineKeyboardButton(text="🌙 Кейсы", callback_data="cases")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def media_path(card_id):
    for ext in [".gif", ".mp4", ".jpg", ".jpeg", ".png", ".webp"]:
        p = MEDIA_DIR / f"{card_id}{ext}"
        if p.exists():
            return p
    return None


async def send_card_media(message, card_id):
    p = media_path(card_id)
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


def card_power(card, level=1):
    return int(sum(card["stats"].values()) + RARITY_BONUS.get(card["rarity"], 0) + (level - 1) * 4)


def level_cost(level, rarity):
    if level >= MAX_LEVEL:
        return None
    # 1->2 стоит 200, потом 300, 400... максимум 1000 фрагментов.
    return min(1000, 100 + level * 100)


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
    """Основной дроп сундуков: фрагменты. На 100 фрагментах карта открывается."""
    card = CARD_BY_ID[card_id]
    col = player.setdefault("collection", {})
    item = col.setdefault(card_id, {"count": 0, "shards": 0, "level": 1, "unlocked": False})
    item.setdefault("level", 1)
    item.setdefault("count", 0)
    item.setdefault("shards", 0)
    item["shards"] += int(amount)

    if not item.get("unlocked") and item["shards"] >= CARD_UNLOCK_FRAGMENTS:
        item["shards"] -= CARD_UNLOCK_FRAGMENTS
        item["count"] = 1
        item["unlocked"] = True
        return f"🧩 +{amount} фрагм. → карта открыта: {card['name']}!"

    need = CARD_UNLOCK_FRAGMENTS if not item.get("unlocked") else level_cost(item.get("level", 1), card["rarity"])
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


def pull_pack_reward(player, weights, exclude=None):
    card = roll_card(weights=weights, exclude=exclude)
    # Чаще падают фрагменты. Полная карта — отдельный редкий дроп.
    if random.random() < full_card_drop_chance(card["rarity"]):
        result = add_card(player, card["id"])
    else:
        amount = fragment_amount_for(card["rarity"])
        result = add_fragments(player, card["id"], amount)
    return card, result

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


def instance_score(inst):
    card = CARD_BY_ID[inst["card_id"]]
    score = card_power(card, int(inst.get("level", 1)))
    for mod in [inst["buff"], inst["debuff"], inst["artifact"]]:
        score += sum(mod["delta"].values())
    return score


def team_score(team):
    total = sum(instance_score(i) for i in team)
    animes = [CARD_BY_ID[i["card_id"]]["anime"] for i in team]
    total += len(set(animes)) * 4
    total -= (len(animes) - len(set(animes))) * 8
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
    ls = duel_score(left_inst)
    rs = duel_score(right_inst)
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

    left_total = team_score(left_team) + random.randint(-25, 25)
    right_total = team_score(right_team) + random.randint(-25, 25)

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
    commands = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="battle", description="Арена мультивселенной"),
        BotCommand(command="appeal", description="Оспорить бой"),
        BotCommand(command="collection", description="Коллекция"),
        BotCommand(command="profile", description="Профиль"),
        BotCommand(command="shop", description="Магазин"),
        BotCommand(command="daily", description="Ежедневная награда"),
        BotCommand(command="craft", description="Крафт"),
        BotCommand(command="rating", description="Рейтинг"),
        BotCommand(command="friends", description="Друзья и рефералка"),
        BotCommand(command="addfriend", description="Добавить друга по ID"),
        BotCommand(command="promo", description="Ввести промокод"),
        BotCommand(command="rules", description="Правила"),
        BotCommand(command="myid", description="Мой Telegram ID"),
        BotCommand(command="online", description="Поиск онлайн-боя"),
        BotCommand(command="pass", description="Мультипасс"),
        BotCommand(command="nick", description="Сменить ник"),
        BotCommand(command="admin", description="Админ-панель владельца"),
        BotCommand(command="commands", description="Все команды"),
    ]
    await bot.set_my_commands(commands)


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


@dp.message(Command("commands"))
async def commands_cmd(message: types.Message):
    await message.answer(
        "📋 <b>Команды</b>\n\n"
        "/start — меню\n"
        "/battle — арена с ботом\n"
        "/collection — коллекция\n"
        "/profile — профиль\n"
        "/shop — магазин\n"
        "/daily — ежедневная награда\n"
        "/craft — крафт\n"
        "/rating — рейтинг\n"
        "/friends — друзья\n"
        "/promo КОД — промокод\n"
        "/rules — правила\n"
        "/myid — твой ID",
        parse_mode="HTML",
        reply_markup=back_menu()
    )


@dp.message(CommandStart())
async def start(message: types.Message):
    get_user_data(message.from_user)
    text = message.text or ""
    if " friend_" in text:
        code = text.split("friend_", 1)[1].strip()
        await accept_friend_invite(message, code)
        return
    await message.answer(
        "🌌 <b>Anime Battle: Multiverse</b>\n\n"
        "🐉 Собирай команду из 5 бойцов из разных аниме-вселенных.\n"
        "📦 Открывай сундуки, выбивай редкие формы и собирай фрагменты.\n"
        "⬆️ Качай карты до 100 уровня и усиливай свою коллекцию.\n"
        "⚔️ Выбирай арену, сражайся с ботом, друзьями или реальными игроками онлайн.\n"
        "🎟 Проходи Мультипасс, забирай награды сезона и поднимай рейтинг.\n\n"
        "Выбери действие ниже.",
        reply_markup=main_menu(message.from_user.id),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "menu")
async def menu(callback: types.CallbackQuery):
    get_user_data(callback.from_user)
    await callback.message.answer("🌌 <b>Anime Battle: Multiverse</b>\n\nВыбери раздел:", reply_markup=main_menu(callback.from_user.id), parse_mode="HTML")
    await callback.answer()


async def send_profile(message, user):
    p = get_user_data(user)
    total = sum(v.get("count", 0) for v in p["collection"].values())
    unique = len(p["collection"])
    lvl, rem, nxt = calc_user_level(p.get("xp", 0))
    role = "👑 Владелец мультивселенной" if is_owner(user.id) else ("🤝 Правая рука" if is_right_hand(user.id) else "Игрок")
    aura = f"\nЗнак: <b>{e(p.get('creator_aura', '♾ Абсолютный знак создателя'))}</b>" if is_owner(user.id) else ""
    await message.answer(
        f"👤 <b>Профиль</b>\n\n"
        f"Имя: <b>{e(p['name'])}</b>\n"
        f"Роль: {role}{aura}\n"
        f"⭐ Уровень: <b>{lvl}</b> ({rem}/{nxt} XP)\n"
        f"💎 Фисташки: <b>{p['fistiks']}</b>\n"
        f"🌙 Лунные монеты: <b>{p.get('moon_coins', 0)}</b>\n"
        f"🃏 Карт всего: <b>{total}</b>\n"
        f"📚 Уникальных карт: <b>{unique}/{len(CARDS)}</b>\n\n"
        "Ниже отдельные вкладки: статистика, знаки, друзья, ник и правила.",
        reply_markup=profile_menu(),
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
        f"💎 Фисташки: <b>{p.get('fistiks', 0)}</b>\n"
        f"🌙 Лунные монеты: <b>{p.get('moon_coins', 0)}</b>\n\n"
        f"⚔️ Боёв: <b>{battles}</b>\n"
        f"🏆 Побед: <b>{wins}</b>\n"
        f"💀 Поражений: <b>{losses}</b>\n"
        f"📈 Винрейт: <b>{winrate}%</b>",
        reply_markup=profile_menu(),
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
    await message.answer(text, reply_markup=profile_menu(), parse_mode="HTML")



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
    text += "Бой с ботом теперь стартует именно от твоей колоды, а не от случайной каши.\n\n"

    deck_ids = [cid for cid in p.get("deck", []) if cid in CARD_BY_ID]
    if deck_ids:
        text += "<b>Текущая команда 5 бойцов:</b>\n"
        for n, cid in enumerate(deck_ids[:5], 1):
            c = CARD_BY_ID[cid]
            lvl = int(p.get("collection", {}).get(cid, {}).get("level", 1))
            text += f"{n}. {rarity_label(c['rarity'])} <b>{e(c['name'])}</b> | ур. {lvl}/{MAX_LEVEL} | сила {card_power(c, lvl)}\n"
    else:
        text += "Пока нет открытых карт. Открой сундуки или собери карту из 100 фрагментов."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧠 Автособрать топ-5", callback_data="auto_build_deck")],
        [InlineKeyboardButton(text="🔁 Переключить автосбор", callback_data="toggle_auto_team")],
        [InlineKeyboardButton(text="⚡ Автоулучшить доступное", callback_data="auto_upgrade")],
        [InlineKeyboardButton(text="⚔️ В бой", callback_data="battle:start")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")]
    ])
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
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


async def send_rules(message):
    full_names = {
        "Наруто": "Naruto / Naruto Shippuden / Boruto: Naruto Next Generations",
        "Боруто": "Boruto: Naruto Next Generations",
        "Блич": "Bleach / Bleach: Thousand-Year Blood War",
        "Ван-Пис": "One Piece",
        "Драгонболл": "Dragon Ball / Dragon Ball Z / Dragon Ball Super",
        "Dragon Ball Heroes": "Super Dragon Ball Heroes",
        "Драгонболл: фильм": "Dragon Ball Super: Broly / Super Hero",
        "Ванпанчмен": "One Punch Man",
        "Моя геройская академия": "My Hero Academia",
        "Магическая битва": "Jujutsu Kaisen",
        "Чёрный клевер": "Black Clover",
        "Хвост Феи": "Fairy Tail",
        "Magi": "Magi: The Labyrinth of Magic",
        "Макс-целитель": "Redo of Healer / Маг-целитель: новый старт",
        "Клинок, рассекающий демонов": "Demon Slayer: Kimetsu no Yaiba",
        "Берсерк": "Berserk",
        "Поднятие уровня в одиночку": "Solo Leveling",
        "Goblin Slayer": "Goblin Slayer",
        "Семь смертных грехов": "The Seven Deadly Sins",
        "Hunter x Hunter": "Hunter x Hunter",
        "JoJo": "JoJo’s Bizarre Adventure",
        "Покемон": "Pokémon",
        "Sword Art Online": "Sword Art Online",
        "Доктор Стоун": "Dr. Stone",
        "Baki": "Baki / Baki Hanma",
        "Гуррен-Лаганн": "Tengen Toppa Gurren Lagann",
        "Umineko": "Umineko When They Cry",
        "Tenchi Muyo": "Tenchi Muyo!",
        "Непризнанный школой владыка демонов": "The Misfit of Demon King Academy",
        "О моём перерождении в слизь": "That Time I Got Reincarnated as a Slime",
    }
    groups = [
        ("🥷 Шиноби и Оцуцуки", ["Наруто", "Боруто"]),
        ("⚔️ Блич", ["Блич"]),
        ("🏴‍☠️ Пираты", ["Ван-Пис"]),
        ("🐉 Dragon Ball", ["Драгонболл", "Dragon Ball Heroes", "Драгонболл: фильм"]),
        ("👊 Герои и монстры", ["Ванпанчмен", "Моя геройская академия"]),
        ("🧿 Проклятия и магия", ["Магическая битва", "Чёрный клевер", "Хвост Феи", "Magi", "Макс-целитель"]),
        ("🔥 Тёмное фэнтези", ["Клинок, рассекающий демонов", "Берсерк", "Поднятие уровня в одиночку", "Goblin Slayer", "Семь смертных грехов"]),
        ("🃏 Игры, охотники и странные силы", ["Hunter x Hunter", "JoJo", "Покемон", "Sword Art Online", "Доктор Стоун"]),
        ("🥊 Реалистичные монстры", ["Baki"]),
        ("🌌 Космические и богоподобные", ["Гуррен-Лаганн", "Umineko", "Tenchi Muyo", "Непризнанный школой владыка демонов", "О моём перерождении в слизь"]),
    ]

    used = set()
    anime_text = ""
    existing_anime = set(c["anime"] for c in CARDS)
    for title, names in groups:
        existing = []
        for n in names:
            if n in existing_anime and n not in used:
                existing.append(full_names.get(n, n))
                used.add(n)
        if existing:
            anime_text += f"\n<b>{title}</b>\n" + "\n".join(f"• {e(n)}" for n in existing) + "\n"

    other = sorted(existing_anime - used)
    if other:
        anime_text += "\n<b>✨ Дополнительные вселенные</b>\n" + "\n".join(f"• {e(full_names.get(n, n))}" for n in other) + "\n"

    await message.answer(
        "📜 <b>Правила игры</b>\n\n"
        "⚔️ <b>Арена с ботом</b>\n"
        "• Ты выбираешь арену и сложность ИИ от 1 до 10.\n"
        "• Бой идёт твоей колодой из 5 открытых карт.\n"
        "• Сначала выбираешь стартового бойца, потом каждый следующий раунд отдельно.\n"
        "• Если игрок не выбирает вовремя — бот делает случайный ход за него.\n\n"
        "👥 <b>PvP / бой с другом</b>\n"
        "• Каждый игрок отдельно выбирает, как формировать команду: своей колодой, рандомом от бота или ручным скрытым драфтом.\n"
        "• Команды противников скрыты до начала боя.\n"
        "• Раунды идут по одному, очки считаются отдельно.\n\n"
        "🚀 <b>Старт новичка</b>\n"
        "• Первые 3 дня открыт отдельный раздел с лёгкими заданиями.\n"
        "• Он нужен, чтобы новичок быстро встал на ноги и понял бота.\n\n"
        "🌙 <b>Кейсы</b>\n"
        "• Кейсы покупаются за лунные монеты, а не за фисташки.\n"
        "• Лунные монеты выдаются через мультипасс, задания и ивенты.\n\n"
        "🎟 <b>Мультипасс</b>\n"
        "• Игрок сначала оплачивает Stars.\n"
        "• Создатель проверяет оплату и вручную открывает премиум до нужного уровня.\n\n"
        "🃏 <b>Коллекция</b>\n"
        "• Карты остаются в коллекции.\n"
        "• Дубликаты превращаются в фрагменты.\n"
        "• Фрагментами можно улучшать карту до 100 уровня.\n\n"
        "🎲 <b>Вес редкости в обычной системе</b>\n"
        "⚪ Обычный — 850\n"
        "🔵 Редкий — 120\n"
        "🟣 Эпический — 25\n"
        "🟡 Легендарный — 4\n"
        "🔴 Мифический — 1\n\n"
        "🌍 <b>Вселенные в базе</b>\n"
        f"{anime_text}",
        reply_markup=back_menu(),
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
    if p.get("last_daily") == today and not is_owner(user.id):
        await message.answer("🎁 Ты уже забрал ежедневную награду сегодня.", reply_markup=back_menu())
        return
    p["last_daily"] = today
    p["fistiks"] += 250
    add_xp(p, 35)
    add_pass_task_progress(p, "daily", 1)
    add_newbie_task_progress(p, "daily", 1)
    save_json(DATA_FILE, DATA)
    await message.answer("🎁 Ежедневная награда получена: +250 💎 фисташек и +35 XP.", reply_markup=back_menu())


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


async def send_shop(message, user):
    p = get_user_data(user)
    await message.answer(
        f"📦 <b>Магазин / награды</b>\n\n"
        f"Баланс: <b>{p['fistiks']}</b> 💎\n"
        f"Кейсовая валюта: <b>{p.get('moon_coins', 0)}</b> 🌙\n\n"
        "Здесь лежит всё, что связано с покупками, сундуками, кейсами, наградами, рейтингом и промокодами.",
        reply_markup=shop_menu(),
        parse_mode="HTML"
    )


async def send_chests(message, user):
    p = get_user_data(user)
    rows = [
        [InlineKeyboardButton(text="🆓 Бесплатный сундук", callback_data="pack_info:free")],
        [InlineKeyboardButton(text="📦 Обычный сундук", callback_data="pack_info:basic")],
        [InlineKeyboardButton(text="💎 Усиленный сундук", callback_data="pack_info:rare")],
        [InlineKeyboardButton(text="👑 Королевский сундук", callback_data="pack_info:royal")],
        [InlineKeyboardButton(text="⬅️ Магазин / награды", callback_data="shop")],
    ]
    await message.answer(
        f"🧰 <b>Сундуки</b>\n\n"
        f"Баланс: <b>{p['fistiks']}</b> 💎\n"
        "Перед покупкой сундука нажми на него — там будут шансы и цена.\n\n"
        "🆓 Бесплатный сундук доступен раз в 3 часа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )



@dp.message(Command("shop"))
async def shop_cmd(message: types.Message):
    await send_shop(message, message.from_user)


@dp.callback_query(F.data == "shop")
async def shop_cb(callback: types.CallbackQuery):
    await send_shop(callback.message, callback.from_user)
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
            [InlineKeyboardButton(text="Открыть бесплатно", callback_data="buy_pack:free")],
            [InlineKeyboardButton(text="⬅️ Сундуки", callback_data="chests")]
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
            [InlineKeyboardButton(text="Купить", callback_data=f"buy_pack:{kind}")],
            [InlineKeyboardButton(text="⬅️ Сундуки", callback_data="chests")]
        ])
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


async def send_pack_result(message, title, cards_got, player):
    text = f"📦 <b>{e(title)} открыт</b>\n\n"
    for card, result in cards_got:
        text += (
            f"🐉 <b>{e(card['name'])}</b> — {rarity_label(card['rarity'])}\n"
            f"{e(result)}\n"
        )
    text += "\n📖 Полное описание, форма, роль, плюсы и минусы доступны в коллекции."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад к сундукам", callback_data="chests"), InlineKeyboardButton(text="🌙 Кейсы", callback_data="cases")],
        [InlineKeyboardButton(text="⬅️ Магазин / награды", callback_data="shop"), InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ])
    await send_long(message, text, reply_markup=kb)


@dp.callback_query(F.data.startswith("buy_pack:"))
async def buy_pack(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    kind = callback.data.split(":", 1)[1]
    if kind == "free":
        now = datetime.now()
        last = p.get("last_free_pack", "")
        if last and not is_owner(callback.from_user.id):
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
        card, result = pull_pack_reward(p, weights, exclude=pulled)
        pulled.add(card["id"])
        got.append((card, result))
    add_xp(p, xp)
    add_pass_task_progress(p, "chest", 1)
    add_newbie_task_progress(p, "chest", 1)
    if kind == "free":
        add_newbie_task_progress(p, "free_pack", 1)
    save_json(DATA_FILE, DATA)
    await send_pack_result(callback.message, name, got, p)
    await callback.answer()


@dp.callback_query(F.data == "badges_shop")
async def badges_shop(callback: types.CallbackQuery):
    rows = []
    text = "🏷 <b>Привилегии и знаки</b>\n\n"
    for code, item in BADGE_SHOP.items():
        db_code = code.upper()
        title = f"{item['emoji']} {item['title']}"
        text += f"<b>{title}</b> — {item['cost']} 💎\n{e(item['desc'])}\n\n"
        rows.append([InlineKeyboardButton(text=f"Купить: {title} — {item['cost']} 💎", callback_data=f"buy_badge:{db_code}:{item['cost']}")])
    rows.append([InlineKeyboardButton(text="⬅️ Сундуки", callback_data="chests")])
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


@dp.callback_query(F.data == "buy_premium")
async def buy_premium(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    cost = 5000
    if p["fistiks"] < cost and not is_owner(callback.from_user.id):
        await callback.answer("Не хватает фисташек.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["fistiks"] -= cost
    p["premium"] = True
    if "PREMIUM" not in p["badges"]:
        p["badges"].append("PREMIUM")
    add_xp(p, 200)
    save_json(DATA_FILE, DATA)
    await callback.message.answer("👑 Premium-тест активирован.", reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


async def send_collection(message, user, page=0):
    p = get_user_data(user)
    items = [(cid, info) for cid, info in p["collection"].items() if cid in CARD_BY_ID]
    if not items:
        await message.answer("🃏 Коллекция пуста. Открой сундук или сыграй бой.", reply_markup=back_menu())
        return
    items.sort(key=lambda x: (RARITY_BONUS.get(CARD_BY_ID[x[0]]["rarity"], 0), CARD_BY_ID[x[0]]["name"]), reverse=True)
    per_page = 10
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    rows = []
    for cid, info in items[page * per_page:(page + 1) * per_page]:
        c = CARD_BY_ID[cid]
        rows.append([InlineKeyboardButton(text=f"{RARITY_EMOJI.get(c['rarity'],'⚪')} {c['name']} ур.{info.get('level',1)}", callback_data=f"card:{cid}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"collection:page:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"collection:page:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    await message.answer("🃏 <b>Коллекция</b>\nНажми на карту, чтобы открыть медиа и описание.", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.message(Command("collection"))
async def collection_cmd(message: types.Message):
    await send_collection(message, message.from_user, 0)


@dp.callback_query(F.data.startswith("collection:page:"))
async def collection_page(callback: types.CallbackQuery):
    page = int(callback.data.split(":")[2])
    await send_collection(callback.message, callback.from_user, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("card:"))
async def card_detail(callback: types.CallbackQuery):
    cid = callback.data.split(":", 1)[1]
    p = get_user_data(callback.from_user)
    if cid not in CARD_BY_ID or cid not in p["collection"]:
        await callback.answer("Карты нет в коллекции.", show_alert=True)
        return
    await send_card_media(callback.message, cid)
    c = CARD_BY_ID[cid]
    info = p["collection"][cid]
    level = info.get("level", 1)
    cost = level_cost(level, c["rarity"])
    next_text = "Максимальный уровень достигнут." if cost is None else f"До следующего уровня: {cost} фрагментов."
    owner_hint = ""
    if is_owner(callback.from_user.id):
        media_hint = f"media/{cid}.gif или media/{cid}.jpg"
        owner_hint = f"\n\n🖼 Файл медиа: <code>{e(media_hint)}</code>"
    text = (
        f"🐉 <b>{e(c['name'])}</b>\n\n"
        f"⭐ Редкость: {rarity_label(c['rarity'])}\n"
        f"⚔️ Сила: <b>{card_power(c, level)}</b>\n"
        f"🎭 Мод: {e(c['form'])}\n"
        f"🌍 Аниме: {e(c['anime'])}\n"
        f"📖 Описание: {e(c.get('description', ''))}\n"
        f"📈 Уровень: <b>{level}/{MAX_LEVEL}</b>\n"
        f"🧩 Фрагменты: <b>{info.get('shards',0)}</b>\n"
        f"📌 {e(next_text)}\n\n"
        f"⚡ Способности: {e(c.get('abilities',''))}\n"
        f"➕ {e(c['plus'])}\n"
        f"➖ {e(c['minus'])}"
        f"{owner_hint}"
    )
    rows = []
    if cost is not None:
        rows.append([InlineKeyboardButton(text=f"⬆️ Улучшить до {level+1}", callback_data=f"upgrade:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Коллекция", callback_data="collection:page:0")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
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


async def show_arena_select(message, user):
    rows = []
    text = (
        "🌌 <b>Арена мультивселенной</b>\n\n"
        "Выбери фон боя. Потом выберешь сложность бота от 1 до 10.\n\n"
    )
    for code_key, (emoji, name, desc) in ARENAS.items():
        plus, minus = ARENA_EFFECTS.get(code_key, ("➕ нейтрально", "➖ нейтрально"))
        text += f"{emoji} <b>{e(name)}</b>\n— {e(desc)}\n{e(plus)}\n{e(minus)}\n\n"
        rows.append([InlineKeyboardButton(text=f"{emoji} {name}", callback_data=f"battle:arena:{code_key}")])
    rows.append([InlineKeyboardButton(text="🎲 Случайная арена", callback_data="battle:arena:random")])
    rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


async def show_difficulty_select(message, user, arena_code):
    if arena_code == "random" or arena_code not in ARENAS:
        arena_code = random.choice(list(ARENAS.keys()))
    emoji, arena_name, arena_desc = ARENAS[arena_code]
    rows = []
    for start in [1, 6]:
        rows.append([
            InlineKeyboardButton(text=str(i), callback_data=f"battle:diff:{arena_code}:{i}")
            for i in range(start, start + 5)
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Арены", callback_data="battle:start")])
    await message.answer(
        f"🤖 <b>Сложность бота</b>\n\n"
        f"{emoji} Арена: <b>{e(arena_name)}</b>\n"
        f"— {e(arena_desc)}.\n\n"
        "1 — слабый бот, 5 — нормальный, 10 — адская мясорубка.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )


async def start_battle_for(message, user, arena_code="random", difficulty=5):
    if arena_code == "random" or arena_code not in ARENAS:
        arena_code = random.choice(list(ARENAS.keys()))
    difficulty = max(1, min(10, int(difficulty or 5)))
    p = get_user_data(user)
    player_team = build_player_team_from_deck(user.id)
    if len(player_team) < 5:
        await message.answer(
            "🃏 Для боя нужна колода из 5 открытых карт. Открой сундуки или собери карты из фрагментов.",
            reply_markup=main_menu(user.id),
            parse_mode="HTML"
        )
        return

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
        f"🤖 Сложность бота: <b>{difficulty}/10</b>\n\n"
        "👤 <b>Твоя колода</b>\n"
    )
    for i, inst in enumerate(player_team, 1):
        text += format_instance(inst, i) + "\n"
    text += "\n🔒 Команда бота скрыта. Выбери первого бойца — после этого начнётся раунд 1."
    rows = []
    for i, inst in enumerate(player_team, 1):
        c = CARD_BY_ID[inst["card_id"]]
        rows.append([InlineKeyboardButton(text=f"⚔️ Старт: {i}. {c['name'][:28]}", callback_data=f"fight_start:{i-1}")])
    rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    await send_long(message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@dp.message(Command("battle"))
async def battle_cmd(message: types.Message):
    await show_arena_select(message, message.from_user)


@dp.callback_query(F.data == "battle:start")
async def battle_cb(callback: types.CallbackQuery):
    await show_arena_select(callback.message, callback.from_user)
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
            [InlineKeyboardButton(text=f"Выбрать {i}", callback_data=f"pick:{state['round']}:{i-1}")]
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
        rows.append([InlineKeyboardButton(text=f"⚔️ Начать с {i}. {c['name'][:28]}", callback_data=f"fight_start:{i-1}")])
    rows.append([InlineKeyboardButton(text="🔁 Новый бой", callback_data="battle:start")])
    rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
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
        f"🤖 Сложность: <b>{state.get('difficulty', 5)}/10</b>\n\n"
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
        rows.append([InlineKeyboardButton(text=f"➡️ Раунд {round_no + 1}: {c['name'][:30]}", callback_data=f"fight_next:{idx}")])
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
        [InlineKeyboardButton(text="⚖️ Оспорить", callback_data="appeal")],
        [InlineKeyboardButton(text="🔁 Новый бой", callback_data="battle:start")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")]
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
            [InlineKeyboardButton(text="Скорость", callback_data="appeal_reason:speed"), InlineKeyboardButton(text="Хакс", callback_data="appeal_reason:hax")],
            [InlineKeyboardButton(text="Форма", callback_data="appeal_reason:form"), InlineKeyboardButton(text="Синергия", callback_data="appeal_reason:team")],
            [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")]
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
    get_user_data(user)
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
            rows.append([InlineKeyboardButton(text=f"⚔️ Вызвать: {fname}", callback_data=f"challenge:{fid}")])
    else:
        lines.append("Список друзей пуст.")
    pending = DATA.setdefault("friend_requests", {}).get(uid, [])
    if pending:
        lines.append("\n<b>Заявки:</b>")
        for from_id in pending[:10]:
            from_name = DATA.get("users", {}).get(from_id, {}).get("name", from_id)
            lines.append(f"• {e(from_name)} хочет добавить тебя")
            rows.append([
                InlineKeyboardButton(text=f"✅ Принять {from_name}", callback_data=f"friend_accept:{from_id}"),
                InlineKeyboardButton(text="❌", callback_data=f"friend_decline:{from_id}"),
            ])
    rows.append([InlineKeyboardButton(text="🌐 Найти онлайн-бой", callback_data="online_search")])
    rows.append([InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="friend_link")])
    rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    text = (
        "👥 <b>Друзья</b>\n\n"
        + "\n".join(lines)
        + "\n\nЧтобы отправить заявку, напиши:\n<code>/addfriend ID</code>\n\n"
        + "За приглашённого друга: +500 💎 тебе и +300 💎 другу."
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
        [InlineKeyboardButton(text="✅ Принять вызов", callback_data=f"challenge_accept:{me}")],
        [InlineKeyboardButton(text="❌ Отказаться", callback_data=f"challenge_decline:{me}")],
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
    if source == "deck":
        built = build_player_team_from_deck(uid)
        if len(built) >= 5:
            state["teams"][uid] = built[:5]
            return True
    if source == "random_bot":
        state["teams"][uid] = build_bot_team(5)[:5]
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
            [InlineKeyboardButton(text=f"Выбрать {i}", callback_data=f"pvp_pick:{bid}:{state['round']}:{i-1}")]
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
                rows.append([InlineKeyboardButton(text=f"⚔️ Начать с {i}. {c['name'][:28]}", callback_data=f"pvp_start:{bid}:{i-1}")])
            rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
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


async def resolve_pvp_battle(bid):
    state = active_pvp.get(bid)
    if not state or not state.get("done"):
        return
    if state.get("resolved"):
        return
    p1, p2 = state["players"]
    starters = state.setdefault("starters", {})
    if p1 not in starters or p2 not in starters:
        return

    n1 = state["names"].get(p1, p1)
    n2 = state["names"].get(p2, p2)
    arena_code = state.get("arena", "void")
    winner_name, p1_points, p2_points, rounds_text, summary = resolve_step_battle(
        n1, n2, state["teams"][p1], state["teams"][p2], arena_code, starters.get(p1, 0), starters.get(p2, 0)
    )

    winner_uid, loser_uid = (p1, p2) if winner_name == n1 else (p2, p1)

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
    text = (
        "⚔️ <b>PvP-бой начался</b>\n\n"
        "🔓 Команды раскрыты только после выбора стартовых персонажей.\n\n"
        f"{team1}\n{team2}\n"
        f"🎬 <b>Пошаговый бой</b>\n\n"
        f"{rounds_text}\n\n"
        f"{summary}\n\n"
        "🎁 Победитель получает +160 💎 и +120 XP.\n"
        "🎁 Проигравший получает +60 💎 и +60 XP."
    )

    state["resolved"] = True
    for uid in state["players"]:
        try:
            await bot.send_message(int(uid), text, reply_markup=back_menu(), parse_mode="HTML")
        except Exception:
            pass


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
            await bot.send_message(int(target), "⚔️ Оба стартовых персонажа выбраны. Бой начинается по раундам.")
        except Exception:
            pass
    await resolve_pvp_battle(bid)


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


@dp.callback_query(F.data == "friend_link")
async def friend_link(callback: types.CallbackQuery):
    code = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    DATA.setdefault("friend_invites", {})[code] = str(callback.from_user.id)
    save_json(DATA_FILE, DATA)
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=friend_{code}"
    await callback.message.answer(f"🔗 Ссылка для друга:\n{link}")
    await callback.answer()


async def accept_friend_invite(message, code):
    inviter = DATA.setdefault("friend_invites", {}).get(code)
    if not inviter:
        await message.answer("Ссылка друга не найдена или устарела.", reply_markup=main_menu(message.from_user.id))
        return
    me_id = str(message.from_user.id)
    if inviter == me_id:
        await message.answer("Нельзя добавить самого себя.", reply_markup=main_menu(message.from_user.id))
        return
    DATA.setdefault("friends", {}).setdefault(inviter, [])
    DATA.setdefault("friends", {}).setdefault(me_id, [])
    if me_id not in DATA["friends"][inviter]:
        DATA["friends"][inviter].append(me_id)
    if inviter not in DATA["friends"][me_id]:
        DATA["friends"][me_id].append(inviter)
    user_player = get_user_data(message.from_user)
    if not user_player.get("ref_by"):
        user_player["ref_by"] = inviter
        user_player["fistiks"] += 300
        add_xp(user_player, 120)
        if inviter in DATA["users"]:
            DATA["users"][inviter]["fistiks"] += 500
            DATA["users"][inviter]["ref_count"] = DATA["users"][inviter].get("ref_count", 0) + 1
            add_newbie_task_progress(DATA["users"][inviter], "referral", 1)
            add_xp(DATA["users"][inviter], 150)
    save_json(DATA_FILE, DATA)
    await message.answer("👥 Друг добавлен. Реферальный бонус начислен.", reply_markup=main_menu(message.from_user.id))
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
            InlineKeyboardButton(text="⚪ Обычный", callback_data="craft_make:common"),
            InlineKeyboardButton(text="🔵 Редкий", callback_data="craft_make:rare"),
        ],
        [
            InlineKeyboardButton(text="🟣 Эпический", callback_data="craft_make:epic"),
            InlineKeyboardButton(text="🟡 Легендарный", callback_data="craft_make:legendary"),
        ],
        [InlineKeyboardButton(text="🔴 Мифический", callback_data="craft_make:mythic")],
        [InlineKeyboardButton(text="⚒️ Скрафтить всё доступное", callback_data="craft_all")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
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


@dp.callback_query(F.data == "cases")
async def cases(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    balance = int(p.get("moon_coins", 0))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚡ Ивент-кейс — {CASE_PRICES['event']} 🌙", callback_data="case_open:event")],
        [InlineKeyboardButton(text=f"🎉 Праздничный кейс — {CASE_PRICES['holiday']} 🌙", callback_data="case_open:holiday")],
        [InlineKeyboardButton(text=f"🔴 Мифический кейс — {CASE_PRICES['mystic']} 🌙", callback_data="case_open:mystic")],
        [InlineKeyboardButton(text="⬅️ Магазин / награды", callback_data="shop"), InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])
    await callback.message.answer(
        f"🌙 <b>Кейсы</b>\n\n"
        f"Твоя валюта кейсов: <b>{balance}</b> 🌙\n\n"
        "🌙 Лунные монеты выдаются через мультипасс, задания и ивенты.\n"
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
        await callback.answer(f"Не хватает лунных монет. Нужно {cost} 🌙.", show_alert=True)
        return
    if not is_owner(callback.from_user.id):
        p["moon_coins"] = int(p.get("moon_coins", 0)) - cost

    got = []
    pulled = set()
    for _ in range(7):
        card = roll_card(weights=weights, exclude=pulled)
        pulled.add(card["id"])
        got.append((card, add_card(p, card["id"], 200)))
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


NEWBIE_DAYS = 3
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


def add_newbie_task_progress(player, key, amount=1):
    if key not in NEWBIE_TASKS:
        return
    created = player.get("created_at") or datetime.now().isoformat()
    try:
        if datetime.now() > datetime.fromisoformat(created) + timedelta(days=NEWBIE_DAYS):
            return
    except Exception:
        pass
    progress = player.setdefault("newbie_progress", {})
    target = int(NEWBIE_TASKS[key]["target"])
    progress[key] = min(target, int(progress.get(key, 0)) + int(amount))


def format_newbie_tasks(player):
    progress = player.setdefault("newbie_progress", {})
    claimed = set(player.setdefault("newbie_claimed", []))
    lines = []
    for key, task in NEWBIE_TASKS.items():
        done = min(int(progress.get(key, 0)), int(task["target"]))
        mark = "✅" if key in claimed else ("🎯" if done >= task["target"] else "▫️")
        reward = task["reward"]
        moon_part = f" + {reward.get('moon_coins', 0)} 🌙" if reward.get("moon_coins") else ""
        lines.append(f"{mark} {task['title']}: {done}/{task['target']} → {reward.get('fistiks', 0)} 💎 + {reward.get('pass_xp', 0)} очков pass{moon_part}")
    return "\n".join(lines)


async def send_newbie_start(message, user):
    p = get_user_data(user)
    if not is_newbie_active(user.id):
        await message.answer("🚀 Раздел новичка уже закрыт: он действует только первые 3 дня после первого входа.", reply_markup=back_menu())
        return
    created = p.get("created_at") or datetime.now().isoformat()
    try:
        expires = datetime.fromisoformat(created) + timedelta(days=NEWBIE_DAYS)
        left = expires - datetime.now()
        left_text = f"ещё примерно {max(0, left.days)} дн. {max(0, left.seconds // 3600)} ч."
    except Exception:
        left_text = "первые 3 дня"
    text = (
        "🚀 <b>Старт новичка</b>\n\n"
        f"Доступно временно: <b>{e(left_text)}</b>\n\n"
        "Здесь лёгкие задания, чтобы быстро встать на ноги, открыть сундуки и привыкнуть к игре.\n\n"
        f"{format_newbie_tasks(p)}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Забрать выполненные", callback_data="newbie_claim")],
        [InlineKeyboardButton(text="🎁 Награда", callback_data="daily"), InlineKeyboardButton(text="📦 Сундуки", callback_data="chests")],
        [InlineKeyboardButton(text="⚔️ Бой с ботом", callback_data="battle:start"), InlineKeyboardButton(text="⚒️ Крафт", callback_data="craft")],
        [InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="friend_link")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
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
            moon_part = f" +{reward.get('moon_coins', 0)} 🌙" if reward.get("moon_coins") else ""
            lines.append(f"✅ {e(task['title'])}: +{reward.get('fistiks', 0)} 💎 +{reward.get('pass_xp', 0)} очков pass{moon_part}")
    if not lines:
        await callback.answer("Пока нет выполненных новичковых заданий.", show_alert=True)
        return
    save_json(DATA_FILE, DATA)
    await callback.message.answer("🚀 <b>Новичковые награды получены</b>\n\n" + "\n".join(lines), reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


PASS_DAILY_TASKS = {
    "daily": {"title": "Забрать ежедневную награду", "target": 1, "pass_xp": 120},
    "chest": {"title": "Открыть сундук", "target": 1, "pass_xp": 100},
    "battle": {"title": "Сыграть бой", "target": 1, "pass_xp": 150},
    "win": {"title": "Победить 1 раз", "target": 1, "pass_xp": 200},
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
    if key not in PASS_DAILY_TASKS:
        return
    progress = player.setdefault("pass_task_progress", {})
    target = int(PASS_DAILY_TASKS[key]["target"])
    progress[key] = min(target, int(progress.get(key, 0)) + int(amount))


def format_pass_tasks(player):
    ensure_pass_daily(player)
    progress = player.get("pass_task_progress", {})
    claimed = set(player.get("pass_task_claimed", []))
    lines = []
    for key, task in PASS_DAILY_TASKS.items():
        done = min(int(progress.get(key, 0)), int(task["target"]))
        mark = "✅" if key in claimed else ("🎯" if done >= task["target"] else "▫️")
        lines.append(f"{mark} {task['title']}: {done}/{task['target']} → +{task['pass_xp']} очков")
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
            parts.append(f"{reward['moon_coins']} 🌙")
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
        text.append(f"+{reward['moon_coins']} 🌙")
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
        premium = f"активен до {cap if cap else 20} уровня"
    elif p.get("pass_purchase_request") == "paid_pending":
        premium = "оплачено, ждёт подтверждения создателя"
    else:
        premium = "не куплен"
    free_claimed = set(map(str, p.get("claimed_pass_free", [])))
    premium_claimed = set(map(str, p.get("claimed_pass_premium", [])))
    request_state = p.get("pass_purchase_request", "")
    request_text = {
        "": "нет",
        "paid_pending": "оплачено, ждёт подтверждения",
        "activated": "активирован",
        "rejected_after_payment": "оплачено, но отклонено/заморожено",
        "paid": "оплачено",
    }.get(request_state, request_state or "нет")

    text = (
        "🎟 <b>Мультипасс</b>\n\n"
        f"⭐ Уровень пропуска: <b>{pass_level}/100</b>\n"
        f"📌 Очки пропуска: <b>{p.get('pass_xp', 0)}</b>\n"
        f"👑 Премиум: <b>{premium}</b>\n"
        f"🧾 Статус оплаты: <b>{request_text}</b>\n\n"
        "🎯 <b>Ежедневные задания</b>\n"
        f"{format_pass_tasks(p)}\n\n"
        "🎁 <b>Награды за уровни</b>\n"
        f"{format_pass_rewards(PASS_FREE_REWARDS, free_claimed)}\n\n"
        "👑 <b>Премиум-награды</b>\n"
        f"{format_pass_rewards(PASS_PREMIUM_REWARDS, premium_claimed)}\n\n"
        f"⭐ Премиум стоит <b>{PASS_PRICE_STARS}</b> Telegram Stars. Сначала игрок оплачивает, потом создатель подтверждает доступ."
    )
    rows = [
        [InlineKeyboardButton(text="🎯 Забрать очки за задания", callback_data="pass_claim_tasks")],
        [InlineKeyboardButton(text="🎁 Забрать награды уровней", callback_data="pass_claim:free")],
        [InlineKeyboardButton(text="👑 Забрать премиум-награды", callback_data="pass_claim:premium")],
    ]
    if not p.get("pass_premium") and p.get("pass_purchase_request") != "paid_pending" and not is_owner(user.id):
        rows.append([InlineKeyboardButton(text=f"⭐ Купить премиум за {PASS_PRICE_STARS} Stars", callback_data="buy_pass_stars")])
    rows.append([InlineKeyboardButton(text="🌙 Кейсы", callback_data="cases"), InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.callback_query(F.data == "multipass")
async def multipass_cb(callback: types.CallbackQuery):
    await send_multipass(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("pass"))
async def multipass_cmd(message: types.Message):
    await send_multipass(message, message.from_user)


@dp.callback_query(F.data == "pass_claim_tasks")
async def pass_claim_tasks(callback: types.CallbackQuery):
    p = get_user_data(callback.from_user)
    ensure_pass_daily(p)
    progress = p.get("pass_task_progress", {})
    claimed = set(p.setdefault("pass_task_claimed", []))
    total = 0
    lines = []
    for key, task in PASS_DAILY_TASKS.items():
        if key in claimed:
            continue
        if int(progress.get(key, 0)) >= int(task["target"]):
            total += int(task["pass_xp"])
            p["pass_task_claimed"].append(key)
            lines.append(f"✅ {e(task['title'])}: +{task['pass_xp']} очков")
    if total <= 0:
        await callback.answer("Пока нет выполненных заданий.", show_alert=True)
        return
    p["pass_xp"] = int(p.get("pass_xp", 0)) + total
    save_json(DATA_FILE, DATA)
    await callback.message.answer("🎯 <b>Очки мультипасса получены</b>\n\n" + "\n".join(lines), reply_markup=back_menu(), parse_mode="HTML")
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
        await callback.answer("Оплата уже получена. Жди подтверждения создателя.", show_alert=True)
        return
    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title="Премиум мультипасс",
            description="После оплаты создатель проверит поступление Stars и подтвердит доступ к премиум-линии.",
            payload=f"multipass_premium:{callback.from_user.id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Премиум мультипасс", amount=PASS_PRICE_STARS)],
        )
        await callback.message.answer(
            "⭐ Счёт отправлен. После оплаты заявка уйдёт создателю на подтверждение.",
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


@dp.pre_checkout_query()
async def pre_checkout_query(pre_checkout: types.PreCheckoutQuery):
    await pre_checkout.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("multipass_premium:"):
        p = get_user_data(message.from_user)
        p["pass_purchase_request"] = "paid_pending"
        p["pass_premium"] = False
        p["pass_premium_cap"] = 0
        p["stars_earned"] = int(p.get("stars_earned", 0)) + int(message.successful_payment.total_amount)
        save_json(DATA_FILE, DATA)

        rows = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Открыть до 20 уровня", callback_data=f"pass_paid:approve:{message.from_user.id}:20")],
            [InlineKeyboardButton(text="✅ Открыть до 50 уровня", callback_data=f"pass_paid:approve:{message.from_user.id}:50")],
            [InlineKeyboardButton(text="✅ Открыть до 100 уровня", callback_data=f"pass_paid:approve:{message.from_user.id}:100")],
            [InlineKeyboardButton(text="⚠️ Заморозить/отклонить", callback_data=f"pass_paid:reject:{message.from_user.id}:0")],
        ])
        owner_text = (
            "⭐ <b>Оплачен премиум мультипасс</b>\n\n"
            f"Игрок: <b>{e(p.get('name', message.from_user.full_name))}</b>\n"
            f"ID: <code>{message.from_user.id}</code>\n"
            f"Оплачено: <b>{message.successful_payment.total_amount}</b> Stars\n\n"
            "Проверь поступление Stars и выбери, до какого уровня открыть премиум."
        )
        for oid in owner_ids():
            try:
                await bot.send_message(int(oid), owner_text, reply_markup=rows, parse_mode="HTML")
            except Exception:
                pass

        await message.answer(
            "✅ Оплата получена. Заявка отправлена создателю. После проверки он подтвердит премиум-доступ.",
            reply_markup=back_menu()
        )
    else:
        await message.answer("✅ Платёж получен.")



async def send_modes(message, user):
    p = get_user_data(user)
    source_names = {"deck": "моя колода", "random_bot": "рандом от бота", "manual": "ручной драфт"}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌌 Арена мультивселенной", callback_data="battle:start")],
        [InlineKeyboardButton(text="🌐 Онлайн-бой", callback_data="online_search")],
        [InlineKeyboardButton(text="⚙️ Команда PvP/друга", callback_data="pvp_source_menu")],
        [InlineKeyboardButton(text="🧬 Колода", callback_data="deck")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(
        "🎮 <b>Режимы</b>\n\n"
        "🌌 <b>Арена мультивселенной</b> — бой против ИИ на выбранной арене.\n"
        "🌐 <b>Онлайн-бой</b> — скрытый PvP против живого игрока.\n"
        f"⚙️ <b>Текущий выбор PvP-команды:</b> {e(source_names.get(p.get('pvp_team_source', 'deck'), 'моя колода'))}.\n"
        "🧬 <b>Колода</b> — сила твоей коллекции и автоулучшение.",
        reply_markup=kb,
        parse_mode="HTML"
    )



async def send_pvp_source_menu(message, user):
    p = get_user_data(user)
    current = p.get("pvp_team_source", "deck")
    names = {"deck": "🧬 Моя колода", "random_bot": "🤖 Рандом от бота", "manual": "🎲 Ручной скрытый драфт"}
    text = (
        "⚙️ <b>Выбор команды для PvP/друга</b>\n\n"
        f"Сейчас: <b>{e(names.get(current, '🧬 Моя колода'))}</b>\n\n"
        "Это индивидуальная настройка. Один игрок может идти своей колодой, другой — рандомом от бота. "
        "Выбор каждого сохраняется отдельно."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧬 Играть своей колодой", callback_data="pvp_source:set:deck")],
        [InlineKeyboardButton(text="🤖 Играть рандомом от бота", callback_data="pvp_source:set:random_bot")],
        [InlineKeyboardButton(text="🎲 Ручной скрытый драфт", callback_data="pvp_source:set:manual")],
        [InlineKeyboardButton(text="⬅️ Режимы", callback_data="modes")],
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data == "pvp_source_menu")
async def pvp_source_menu_cb(callback: types.CallbackQuery):
    await send_pvp_source_menu(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("pvp_source:set:"))
async def pvp_source_set_cb(callback: types.CallbackQuery):
    mode = callback.data.split(":", 2)[2]
    if mode not in {"deck", "random_bot", "manual"}:
        await callback.answer("Неизвестный режим.", show_alert=True)
        return
    p = get_user_data(callback.from_user)
    p["pvp_team_source"] = mode
    save_json(DATA_FILE, DATA)
    await send_pvp_source_menu(callback.message, callback.from_user)
    await callback.answer("Выбор сохранён.")


@dp.callback_query(F.data == "modes")
async def modes_cb(callback: types.CallbackQuery):
    await send_modes(callback.message, callback.from_user)
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
        [InlineKeyboardButton(text="📦 5 обычных сундуков", callback_data="mega_buy:basic:5")],
        [InlineKeyboardButton(text="💎 5 усиленных сундуков", callback_data="mega_buy:rare:5")],
        [InlineKeyboardButton(text="👑 3 королевских сундука", callback_data="mega_buy:royal:3")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
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
            card = roll_card(weights=pack["weights"], exclude=pulled)
            pulled.add(card["id"])
            got.append((card, add_card(p, card["id"])))
    add_xp(p, 60 * amount)
    add_pass_task_progress(p, "chest", amount)
    add_newbie_task_progress(p, "chest", amount)
    save_json(DATA_FILE, DATA)
    await send_pack_result(callback.message, f"Мега-открытие: {pack['name']} x{amount}", got, p)
    await callback.answer()


async def join_online_queue(user):
    uid = str(user.id)
    get_user_data(user)
    if uid in online_queue:
        return None
    if online_queue:
        enemy = online_queue.pop(0)
        if enemy == uid:
            online_queue.append(uid)
            return None
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
        }
        return bid
    online_queue.append(uid)
    return None


@dp.callback_query(F.data == "online_search")
async def online_search_cb(callback: types.CallbackQuery):
    bid = await join_online_queue(callback.from_user)
    if bid:
        state = active_pvp[bid]
        for uid in state["players"]:
            try:
                await bot.send_message(int(uid), "🌐 Онлайн-соперник найден. Начинается скрытый PvP-драфт.", parse_mode="HTML")
            except Exception:
                pass
        await send_pvp_round(bid)
    else:
        await callback.message.answer("🌐 <b>Поиск онлайн-боя</b>\n\nТы в очереди. Когда появится второй игрок, бой начнётся автоматически.", reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer()


@dp.message(Command("online"))
async def online_cmd(message: types.Message):
    bid = await join_online_queue(message.from_user)
    if bid:
        state = active_pvp[bid]
        for uid in state["players"]:
            try:
                await bot.send_message(int(uid), "🌐 Онлайн-соперник найден. Начинается скрытый PvP-драфт.", parse_mode="HTML")
            except Exception:
                pass
        await send_pvp_round(bid)
    else:
        await message.answer("🌐 Ты в очереди онлайн-боя. Жди второго игрока.", reply_markup=back_menu())


def short_user_line(uid, p, index=0):
    name = p.get("name") or uid
    banned = " ⛔" if p.get("banned") else ""
    last = p.get("last_seen", "нет")
    return f"{index}. {e(name)}{banned} — ID <code>{uid}</code> | карт {len(p.get('collection', {}))} | боёв {p.get('battles', 0)} | last {e(last[:16])}"


async def send_admin_panel(message, user):
    if not is_owner(user.id):
        await message.answer("⛔ Только владелец мультивселенной имеет доступ.")
        return
    users = DATA.get("users", {})
    total = len(users)
    banned = sum(1 for p in users.values() if p.get("banned"))
    online = sum(1 for uid in users if is_online(uid))
    paid_pending = sum(1 for p in users.values() if p.get("pass_purchase_request") == "paid_pending")
    text = (
        "🛠 <b>Админ-панель владельца</b>\n\n"
        f"👥 Игроков всего: <b>{total}</b>\n"
        f"🟢 Онлайн за 10 мин: <b>{online}</b>\n"
        f"⛔ Заблокировано: <b>{banned}</b>\n"
        f"⭐ Оплаченных мультипассов на подтверждении: <b>{paid_pending}</b>\n\n"
        "Команды:\n"
        "<code>/user ID</code> — открыть аккаунт игрока\n"
        "<code>/ban ID</code> — заблокировать игрока\n"
        "<code>/unban ID</code> — снять блокировку\n"
        "<code>/deleteuser ID</code> — удалить аккаунт игрока из базы"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Список игроков", callback_data="admin_users")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


async def send_admin_users(message, page=0):
    users = DATA.get("users", {})
    items = list(users.items())
    items.sort(key=lambda x: x[1].get("last_seen", ""), reverse=True)
    per_page = 10
    pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    chunk = items[page * per_page:(page + 1) * per_page]
    text = f"👥 <b>Игроки</b> — страница {page + 1}/{pages}\n\n"
    if not chunk:
        text += "Пока пусто."
    rows = []
    for i, (uid, p) in enumerate(chunk, page * per_page + 1):
        text += short_user_line(uid, p, i) + "\n"
        rows.append([InlineKeyboardButton(text=f"Открыть: {p.get('name', uid)[:28]}", callback_data=f"admin_user:{uid}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_users:{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_users:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


async def send_admin_user(message, uid):
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
        f"💎 Фисташки: <b>{p.get('fistiks', 0)}</b>\n"
        f"🌙 Лунные монеты: <b>{p.get('moon_coins', 0)}</b>\n"
        f"Карт: <b>{len(p.get('collection', {}))}/{len(CARDS)}</b>\n"
        f"Боёв: <b>{p.get('battles', 0)}</b> | Побед: <b>{p.get('wins', 0)}</b> | Поражений: <b>{p.get('losses', 0)}</b>\n"
        f"Мультипасс: <b>{'premium' if p.get('pass_premium') else p.get('pass_purchase_request', 'нет')}</b> | cap {p.get('pass_premium_cap', 0)}\n"
        f"Бан: <b>{'да' if p.get('banned') else 'нет'}</b>\n"
        f"Последний вход: <code>{e(p.get('last_seen', 'нет'))}</code>"
    )
    rows = [
        [InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"admin_ban:{uid}"),
         InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"admin_unban:{uid}")],
        [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"admin_delete:{uid}")],
        [InlineKeyboardButton(text="⬅️ Игроки", callback_data="admin_users"), InlineKeyboardButton(text="⬅️ Админ", callback_data="admin")],
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@dp.message(Command("admin"))
async def admin_cmd(message: types.Message):
    await send_admin_panel(message, message.from_user)


@dp.callback_query(F.data == "admin")
async def admin_cb(callback: types.CallbackQuery):
    await send_admin_panel(callback.message, callback.from_user)
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_users"))
async def admin_users_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    parts = callback.data.split(":")
    page = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    await send_admin_users(callback.message, page)
    await callback.answer()


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
    if len(parts) < 2 or not parts[1].strip().isdigit():
        return ""
    return parts[1].strip()


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


@dp.message(Command("deleteuser"))
async def deleteuser_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    uid = parse_uid_from_text(message)
    if not uid or uid not in DATA.get("users", {}):
        await message.answer("Формат: /deleteuser ID")
        return
    DATA["users"].pop(uid, None)
    save_json(DATA_FILE, DATA)
    await message.answer(f"🗑 Аккаунт <code>{uid}</code> удалён из базы.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("admin_ban:"))
async def admin_ban_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
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


@dp.callback_query(F.data.startswith("admin_delete:"))
async def admin_delete_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    uid = callback.data.split(":", 1)[1]
    DATA.get("users", {}).pop(uid, None)
    save_json(DATA_FILE, DATA)
    await callback.message.answer(f"🗑 Аккаунт <code>{uid}</code> удалён.", reply_markup=back_menu(), parse_mode="HTML")
    await callback.answer("Удалено.")



@dp.callback_query(F.data == "noop")
async def noop(callback: types.CallbackQuery):
    await callback.answer()


@dp.message()
async def unknown(message: types.Message):
    await message.answer("Напиши /start или выбери команду в меню.", reply_markup=main_menu(message.from_user.id))


async def free_pack_notifier():
    # Каждые 3 часа напоминает всем зарегистрированным пользователям, что бесплатный сундук доступен.
    await asyncio.sleep(30)
    while True:
        try:
            now = datetime.now()
            changed = False
            for uid, player in list(DATA.get("users", {}).items()):
                last_notice = player.get("last_free_notice", "")
                can_send = True
                if last_notice:
                    try:
                        can_send = now >= datetime.fromisoformat(last_notice) + timedelta(hours=3)
                    except Exception:
                        can_send = True
                if not can_send:
                    continue

                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🆓 Забрать бесплатный сундук", callback_data="pack_info:free")]
                ])
                try:
                    await bot.send_message(
                        int(uid),
                        "🎁 Время получать карту: бесплатный сундук снова доступен. Забери его в магазине.",
                        reply_markup=kb
                    )
                    player["last_free_notice"] = now.isoformat()
                    player["free_pack_notified"] = True
                    changed = True
                except Exception:
                    pass
            if changed:
                save_json(DATA_FILE, DATA)
        except Exception:
            pass
        await asyncio.sleep(3 * 60 * 60)


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
    await start_health_server()
    await set_commands()
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(free_pack_notifier())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

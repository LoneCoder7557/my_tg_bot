#!/usr/bin/env python3
"""Regression tests for the PATCH40 production hotfix."""
from __future__ import annotations

import os
import tempfile
from datetime import timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="abm_patch40_hotfix_data_")
os.environ.setdefault("BOT_TOKEN", "123456:" + "PATCH40_HOTFIX_TEST_TOKEN")
os.environ.pop("DATABASE_URL", None)

import bot as m

m._schedule_data_save_if_needed = lambda: None


class User:
    def __init__(self, uid: int, name: str = "Tester"):
        self.id = uid
        self.full_name = name
        self.username = name.casefold().replace(" ", "_")


passed: list[str] = []


def check(name: str, condition: bool, detail=None) -> None:
    if not condition:
        raise AssertionError(f"{name}: {detail!r}")
    passed.append(name)


m.DATA = {
    "users": {}, "friends": {}, "friend_invites": {}, "purged_users": {},
    "deleted_users": {}, "clans": {}, "storage_meta": {}, "payment_ledger": {},
}

# Exact production regression: PATCH40 crashed when last_seen included +00:00.
u = User(910001, "Aware Timestamp")
m.DATA["users"][str(u.id)] = {
    "name": u.full_name,
    "last_seen": (m.utc_now() - timedelta(minutes=2)).isoformat(),
    "created_at": (m.utc_now() - timedelta(days=2)).isoformat(),
    "collection": {}, "artifacts": {}, "onboarding_complete": True,
    "starter_bundle_claimed": True, "card_attempts": 4,
}
p = m.get_user_data(u)
check("aware_last_seen_no_crash", bool(p.get("last_seen")))
check("aware_user_is_online", m.is_online(u.id))
check("aware_user_not_inactive", not m.is_inactive_for_admin(str(u.id), p))

# Old naive timestamps remain compatible.
u2 = User(910002, "Naive Timestamp")
m.DATA["users"][str(u2.id)] = {
    "name": u2.full_name,
    "last_seen": m.utc_now().replace(tzinfo=None).isoformat(),
    "created_at": m.utc_now().replace(tzinfo=None).isoformat(),
    "collection": {}, "artifacts": {}, "onboarding_complete": True,
    "starter_bundle_claimed": True,
}
check("naive_timestamp_compatible", bool(m.get_user_data(u2).get("last_seen")))

# Main menu is compact and contains the requested values only.
menu = m.main_menu_text(u)
check("menu_has_attempt_count", "Призывы: <b>5</b>" in menu, menu)
check("menu_has_clan_and_power", "Клан:" in menu and "Сила отряда:" in menu)
check("menu_moves_characters_to_profile", "Персонажи:" not in menu)
check("menu_hides_season_daily_multipass", all(x not in menu for x in ("Ежедневная:", "MultiPass", "Сезон ")))

# Any normal summon has exactly 2.5% Absolute and excludes Super Absolute.
total = sum(m.SUMMON_WEIGHTS.values())
check("summon_weights_sum", total == 1000, total)
check("absolute_exact_2_5_percent", m.SUMMON_WEIGHTS["Мифический"] * 100 / total == 2.5)
check("super_absolute_not_in_weights", "Super Absolute" not in m.SUMMON_WEIGHTS)

# Bulk attempt spending is all-or-nothing.
p["last_free_pack"] = m.utc_now().isoformat()
p["card_attempts"] = 2
before = p["card_attempts"]
ok, _, _ = m.consume_summon_attempts(p, u.id, 3)
check("bulk_insufficient_no_partial_spend", not ok and p["card_attempts"] == before, p)
p["card_attempts"] = 3
ok, _, _ = m.consume_summon_attempts(p, u.id, 3)
check("bulk_exact_spend", ok and p["card_attempts"] == 0, p)

# Existing case inventory is preserved as attempts once and never duplicated.
u3 = User(910003, "Legacy Cases")
m.DATA["users"][str(u3.id)] = {
    "name": u3.full_name,
    "last_seen": m.utc_now().isoformat(),
    "collection": {}, "artifacts": {}, "card_attempts": 2,
    "case_inventory": {"light": 2, "event": 1, "holiday": 3, "mystic": 1},
}
p3 = m.get_user_data(u3)
check("legacy_cases_converted", p3["card_attempts"] == 9, p3)
check("legacy_cases_zeroed", sum(p3["case_inventory"].values()) == 0)
m.get_user_data(u3)
check("legacy_case_conversion_idempotent", p3["card_attempts"] == 9)

# Time-aware cooldowns never mix aware/naive values.
p3["last_free_pack"] = (m.utc_now() - timedelta(hours=1)).isoformat()
check("aware_free_cooldown", 110 <= m.free_card_wait_minutes(p3) <= 121, m.free_card_wait_minutes(p3))

# Inline keyboard remains serializable with custom emoji enabled.
markup = m.main_menu(u.id).model_dump(exclude_none=True)
check("menu_markup_serializable", bool(markup.get("inline_keyboard")))
check("callback_data_within_limit", all(
    len(str(btn.get("callback_data", "")).encode("utf-8")) <= 64
    for row in markup["inline_keyboard"] for btn in row
))

print(f"PATCH40_HOTFIX_RUNTIME_PASS {len(passed)}")
for name in passed:
    print("PASS", name)

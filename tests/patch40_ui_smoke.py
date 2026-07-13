#!/usr/bin/env python3
"""Runtime UI smoke test for PATCH40 using the exact installed dependencies."""
from __future__ import annotations
import asyncio
import importlib.util
import os
import shutil
import re
import tempfile
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path(__file__).resolve().parents[1]
WORK_ROOT = Path(tempfile.mkdtemp(prefix="abm_patch40_ui_"))
ROOT = WORK_ROOT / "project"
ROOT.mkdir(parents=True)
for name in ("bot.py", "cards.json", "promo_codes.json", "owner_ids.txt", "right_hand_ids.txt"):
    shutil.copy2(SOURCE_ROOT / name, ROOT / name)
shutil.copytree(SOURCE_ROOT / "media_packs", ROOT / "media_packs")
(ROOT / "media" / "ui").mkdir(parents=True, exist_ok=True)
for src in (SOURCE_ROOT / "media" / "ui").glob("*.png") if (SOURCE_ROOT / "media" / "ui").exists() else []:
    shutil.copy2(src, ROOT / "media" / "ui" / src.name)

os.environ["DATA_DIR"] = str(WORK_ROOT / "data")
os.environ["BOT_TOKEN"] = "123456789:" + ("A" * 35)
os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)
os.environ["CUSTOM_BUTTON_EMOJI"] = "1"
os.environ["ABM_ENABLE_RIGHT_HAND_PERMISSIONS"] = "0"

spec = importlib.util.spec_from_file_location("abm_patch40_ui_tested", ROOT / "bot.py")
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)

class FakeMessage:
    def __init__(self):
        self.calls = []
    async def answer(self, text, **kwargs):
        self.calls.append(("text", str(text), kwargs))
        return SimpleNamespace()
    async def answer_photo(self, photo, caption=None, **kwargs):
        self.calls.append(("photo", str(caption or ""), kwargs))
        return SimpleNamespace()
    async def answer_animation(self, animation, caption=None, **kwargs):
        self.calls.append(("animation", str(caption or ""), kwargs))
        return SimpleNamespace()
    async def answer_video(self, video, caption=None, **kwargs):
        self.calls.append(("video", str(caption or ""), kwargs))
        return SimpleNamespace()


def assert_markup(markup):
    if not markup:
        return 0
    count = 0
    for row in getattr(markup, "inline_keyboard", []) or []:
        for btn in row:
            count += 1
            data = getattr(btn, "callback_data", None)
            if data is not None:
                assert len(str(data).encode("utf-8")) <= 64, (data, len(str(data).encode("utf-8")))
    return count


def assert_html_balanced(text):
    low = text.casefold()
    for tag in ("b", "i", "code", "blockquote", "tg-emoji"):
        opens = len(re.findall(rf"<{tag}(?:\s[^>]*)?>", low))
        closes = low.count(f"</{tag}>")
        assert opens == closes, (tag, opens, closes, text)


def validate_calls(label, message):
    assert message.calls, f"{label}: no output"
    buttons = 0
    for kind, text, kwargs in message.calls:
        limit = 1024 if kind in {"photo", "animation", "video"} else 4096
        assert len(text) <= limit, (label, kind, len(text), limit)
        if kwargs.get("parse_mode") == "HTML":
            assert_html_balanced(text)
        buttons += assert_markup(kwargs.get("reply_markup"))
    return len(message.calls), buttons


async def run():
    user = SimpleNamespace(id=880000001, full_name="Тестовый <Игрок>", username="patch40_test")
    p = mod.get_user_data(user)
    p["onboarding_complete"] = True
    p["onboarding_state"] = "complete"
    p["starter_bundle_claimed"] = True
    p["preferred_universe"] = mod.UNIVERSES[0]["id"]
    for card in mod.CARDS[:5]:
        p["collection"][card["id"]] = {"count": 1, "shards": 0, "level": 1, "unlocked": True}
    first_artifact = next(iter(mod.ARTIFACT_BY_ID))
    p["artifacts"][first_artifact] = {"count": 1, "level": 1}

    screens = []
    async def screen(label, fn, *args):
        msg = FakeMessage()
        await fn(msg, *args)
        calls, buttons = validate_calls(label, msg)
        screens.append((label, calls, buttons, msg.calls))

    await screen("main", mod.send_main_dashboard, user, False)
    await screen("collection_home", mod.send_collection_home, user)
    await screen("collection_rarities", mod.send_collection_ordinary_home, user)
    await screen("collection_list", mod.send_collection, user, 0, "all", "power")
    await screen("artifacts", mod.send_artifacts_collection, user, 0)
    await screen("artifact_detail", mod.send_artifact_detail, user, first_artifact)
    await screen("profile", mod.send_profile, user)
    await screen("profile_stats", mod.send_profile_stats, user)
    await screen("modes", mod.send_modes, user)
    await screen("events", mod.send_events_hub, user)
    await screen("rewards", mod.send_rewards_hub, user)
    await screen("more", mod.send_more_hub, user)
    await screen("season", mod.send_season_screen, user)

    owner_ids = [x.strip() for x in (ROOT / "owner_ids.txt").read_text(encoding="utf-8").splitlines() if x.strip().isdigit()]
    assert owner_ids, "owner_ids.txt must contain the owner's ID"
    owner = SimpleNamespace(id=int(owner_ids[0]), full_name="Owner", username="owner")
    await screen("admin", mod.send_admin_panel, owner)

    by_name = {name: calls for name, _, _, calls in screens}
    main_text = by_name["main"][0][1]
    assert "Твой следующий призыв может открыть легенду" in main_text
    assert "<blockquote>" in main_text and "Клан:" in main_text
    main_markup = by_name["main"][0][2]["reply_markup"]
    main_buttons = [b.text for row in main_markup.inline_keyboard for b in row]
    assert any("Призвать" in t for t in main_buttons)
    assert not any("ГОТОВ" in t for t in main_buttons)

    collection_markup = by_name["collection_home"][0][2]["reply_markup"]
    collection_buttons = [b.text for row in collection_markup.inline_keyboard for b in row]
    assert not any("Сменить" in t for t in collection_buttons)
    assert any("Артефакт" in t for t in collection_buttons)

    profile_markup = by_name["profile"][0][2]["reply_markup"]
    profile_buttons = [b.text for row in profile_markup.inline_keyboard for b in row]
    assert any("Сменить вселенную" in t for t in profile_buttons)

    events_markup = by_name["events"][0][2]["reply_markup"]
    events_buttons = [b.text for row in events_markup.inline_keyboard for b in row]
    assert sum("босс" in t.casefold() for t in events_buttons) == 1
    assert not any("турнир" in t.casefold() for t in events_buttons)

    artifact_text = by_name["artifact_detail"][0][1]
    assert "Что делает" in artifact_text and "Бонус отряду" in artifact_text

    print("PATCH40_UI_SMOKE_PASS", len(screens))
    for name, calls, buttons, _ in screens:
        print("PASS", name, "messages", calls, "buttons", buttons)
    print("ISOLATED_WORK_ROOT", WORK_ROOT)
    mod._DATA_DIRTY = False
    mod._DATA_LAST_SAVED_REVISION = mod._DATA_REVISION
    task = getattr(mod, "_DATA_SAVE_TASK", None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    mod._DATA_SAVE_TASK = None

asyncio.run(run())

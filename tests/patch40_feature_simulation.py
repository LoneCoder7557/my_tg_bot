#!/usr/bin/env python3
"""PATCH40 feature-focused simulation. Run from repository root."""
from __future__ import annotations
import asyncio
import copy
import os
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datetime import timedelta
from types import SimpleNamespace

os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="abm_patch40_feature_data_")
os.environ.setdefault("BOT_TOKEN", "123456:PATCH40_TEST_TOKEN")
os.environ.pop("DATABASE_URL", None)
os.environ.setdefault("CUSTOM_BUTTON_EMOJI", "1")

import bot as m

m._schedule_data_save_if_needed = lambda: None
RESULTS=[]
def ok(name, cond, detail=None):
    if not cond:
        raise AssertionError(f"{name}: {detail!r}")
    RESULTS.append(name)

class User:
    def __init__(self, uid, name="Tester", username="tester"):
        self.id=uid; self.full_name=name; self.username=username

class Msg:
    def __init__(self): self.sent=[]
    async def answer(self, text, **kwargs):
        self.sent.append((text, kwargs)); return SimpleNamespace()
    async def answer_photo(self, *args, **kwargs):
        self.sent.append((kwargs.get('caption',''), kwargs)); return SimpleNamespace()

# UI/custom emoji from the supplied prototype #2 is active by default.
b = m.button(text="🎴 Призвать", callback_data="draw_card")
ok("custom_button_emoji_enabled", b.icon_custom_emoji_id == m.CUSTOM_EMOJI_IDS["draw_card"], b)
ok("custom_button_emoji_text_cleaned", b.text == "Призвать", b.text)
menu=m.main_menu(999)
callbacks=[x.callback_data for row in menu.inline_keyboard for x in row]
ok("main_menu_expected_sections", callbacks == ["draw_card","collection:home","modes","events","hub:rewards","profile","hub:more"], callbacks)

# Full-character draw invariant: duplicates alone become fragments.
card=m.CARDS[0]
p={"collection":{}}
first=m.add_card(p, card["id"])
first_item=copy.deepcopy(p["collection"][card["id"]])
second=m.add_card(p, card["id"])
second_item=p["collection"][card["id"]]
ok("first_draw_is_full_character", first_item["count"]==1 and first_item["unlocked"] and first_item["shards"]==0, first_item)
ok("duplicate_keeps_single_character", second_item["count"]==1 and second_item["duplicates"]==1, second_item)
ok("duplicate_converts_to_fragments", second_item["shards"]==m.DUPLICATE_SHARDS.get(card["rarity"],5) and "Дубликат" in second, second_item)

frag_player={"collection":{}}
m.add_fragments(frag_player, card["id"], 17)
ok("fragment_reward_unlocks_full_character", frag_player["collection"][card["id"]]["count"]==1 and frag_player["collection"][card["id"]]["shards"]==17)
legacy={"collection":{card["id"]:{"count":0,"shards":12,"level":1,"unlocked":False}}}
m.normalize_collection(legacy)
ok("legacy_fragment_only_record_upgraded", legacy["collection"][card["id"]]["count"]==1 and legacy["collection"][card["id"]]["unlocked"])

boilerplate_card=next(c for c in m.CARDS if "Карта добавлена" in str(c.get("description", "")) or "Карта создана" in str(c.get("description", "")))
lore=m.card_public_description(boilerplate_card)
forbidden=("карта добавлена", "карта создана", "коллекц", "призыв", "боевой плюс", "правильной колоде")
ok("card_description_is_lore_first", len(lore)<=650 and not any(x in lore.casefold() for x in forbidden), lore)

season_card_reward=next((r for r in m.SEASON_REWARDS if r.get("kind")=="card" and r.get("card_id") in m.CARD_BY_ID), None)
if season_card_reward:
    sp={"collection":{}}
    m._grant_season_reward(sp, season_card_reward)
    m._grant_season_reward(sp, season_card_reward)
    si=sp["collection"][season_card_reward["card_id"]]
    ok("season_card_first_full_second_fragments", si["count"]==1 and si.get("duplicates",0)==1 and si.get("shards",0)>0, si)

# Count denominators use the same draw-universe mapping as the collection itself.
probe={"collection":{}}
uid=next(rec["id"] for rec in m.UNIVERSES if rec.get("free_total",0)>5)
ids=[c["id"] for c in m.CARDS if m.card_draw_universe(c)==uid and not m.is_super_absolute_card(c)][:4]
for cid in ids: m.add_card(probe,cid)
owned,total=m.universe_progress(probe,uid)
got,totals=m.collection_scope_counts(probe,uid)
ok("collection_owned_count_consistent", owned==sum(got[r] for r in ("Обычный","Редкий","Эпический","Легендарный","Мифический")), (owned,got))
ok("collection_total_count_consistent", total==sum(totals[r] for r in ("Обычный","Редкий","Эпический","Легендарный","Мифический")), (total,totals))

# Profile/main copy constraints requested by the owner.
user=User(700001,"Normal User")
m.DATA={"users":{},"friends":{},"friend_invites":{},"purged_users":{},"deleted_users":{},"clans":{},"storage_meta":{},"payment_ledger":{}}
player=m.get_user_data(user)
player["onboarding_complete"]=True; player["starter_bundle_claimed"]=True; player["preferred_universe"]=uid; player["card_attempts"]=7
main=m.main_menu_text(user)
ok("main_stats_inside_blockquote", main.count("<blockquote>")==1 and main.count("</blockquote>")==1 and "доп. попытки <b>7</b>" in main)
ok("main_button_has_no_ready_suffix", all("ГОТОВ" not in x.text.upper() for row in m.main_menu(user.id).inline_keyboard for x in row))
profile=m.public_profile_text(str(user.id),player)
ok("public_profile_clean", "Победы" not in profile and "Поражения" not in profile and "Попыт" not in profile and "emoji-id" not in profile)

# Right hand is an ordinary player unless explicitly re-enabled by the owner.
old_env=os.environ.pop("ABM_ENABLE_RIGHT_HAND_PERMISSIONS",None)
old_file=m.RIGHT_HAND_FILE
try:
    from pathlib import Path
    tmp=Path(m.DATA_DIR)/"test_right_hand_ids.txt"; tmp.write_text("700001\n",encoding="utf-8")
    m.RIGHT_HAND_FILE=str(tmp)
    ok("right_hand_disabled_by_default", not m.is_right_hand(user.id) and m.right_hand_ids()==set(), m.right_hand_ids())
finally:
    m.RIGHT_HAND_FILE=old_file
    if old_env is not None: os.environ["ABM_ENABLE_RIGHT_HAND_PERMISSIONS"]=old_env

# Artifacts are inspectable/equippable and affect player battle instances.
artifact=m.ARTIFACTS[0]
player["artifacts"]={artifact["id"]:{"count":1,"level":1}}
player["equipped_artifact"]=artifact["id"]
active=m.player_battle_artifact(user.id)
ok("equipped_artifact_resolved", active["id"]==artifact["id"] and active["delta"], active)
original_sum=sum(int(v) for v in artifact.get("delta",{}).values())
team_sum=sum(float(v) for v in active.get("delta",{}).values())*5
ok("artifact_team_bonus_conserved", abs(team_sum-original_sum)<1e-6, (team_sum,original_sum))

# Events hub has only daily, raid-hit, and main-menu actions.
async def event_screen():
    msg=Msg(); await m.send_events_hub(msg,user); return msg.sent[-1]
event_text,event_kwargs=asyncio.run(event_screen())
event_callbacks=[x.callback_data for row in event_kwargs["reply_markup"].inline_keyboard for x in row]
ok("events_buttons_simplified", event_callbacks==["event_daily","raid_hit","menu"], event_callbacks)
ok("events_tournament_is_automatic_copy", "автоматически" in event_text.casefold())

# Global reset: owner untouched, other gameplay reset, paid state and activity preserved.
owner_id="700000"
m.owner_ids=lambda:{owner_id}
old_last=(m.utc_now()-timedelta(hours=2)).isoformat()
m.DATA={
    "users":{
        owner_id:{"name":"Owner","fistiks":999,"collection":{"x":{"count":1}},"last_seen":old_last},
        str(user.id):{
            "name":"Normal User","username":"tester","created_at":old_last,"last_seen":old_last,
            "fistiks":98765,"moon_coins":88,"card_attempts":9,"collection":{card["id"]:{"count":1,"shards":30,"level":4,"unlocked":True}},
            "wins":55,"battles":80,"badges":["RIGHT_HAND","CHAMPION"],"title":"Правая рука",
            "purchases":[{"charge_id":"safe-charge"}],"processed_payments":["safe-charge"],"stars_earned":99,
            "premium":True,"pass_premium":True,"pass_premium_cap":100,"pass_until":"2099-01-01T00:00:00",
        },
    },"friends":{owner_id:[str(user.id)],str(user.id):[owner_id]},"friend_invites":{},"clans":{},"purged_users":{},"deleted_users":{},"storage_meta":{},"payment_ledger":{"safe-charge":{"status":"granted"}},
}
count=asyncio.run(m.reset_all_gameplay_patch40())
reset=m.DATA["users"][str(user.id)]
ok("reset_excludes_owner", count==1 and m.DATA["users"][owner_id]["fistiks"]==999)
ok("reset_clears_gameplay", reset["fistiks"]==m.STARTER_FISTIKS and reset["collection"]=={} and reset["wins"]==0 and reset["onboarding_complete"] is False)
ok("reset_preserves_payment_safety", reset["purchases"] and reset["processed_payments"]==["safe-charge"] and reset["pass_premium"] and m.DATA["payment_ledger"]["safe-charge"]["status"]=="granted")
ok("reset_preserves_recent_activity", reset["last_seen"]==old_last, reset.get("last_seen"))
ok("reset_removes_right_hand_identity", "RIGHT_HAND" not in reset["badges"] and reset["title"]=="Новичок разлома")

# Automatic purge removes stale gameplay but preserves payment state for safe re-entry.
stale_id="700123"
stale_time=(m.utc_now()-timedelta(days=m.AUTO_PURGE_INACTIVE_DAYS+2)).isoformat()
m.DATA["users"][stale_id]={
    "name":"Stale","username":"stale","created_at":stale_time,"last_seen":stale_time,
    "premium":True,"pass_premium":True,"pass_premium_cap":50,"pass_until":"2099-01-01T00:00:00",
    "purchases":[{"charge_id":"old-paid"}],"processed_payments":["old-paid"],"stars_earned":50,
}
removed=asyncio.run(m.auto_purge_stale_users())
ok("stale_user_auto_purged", removed==1 and stale_id not in m.DATA["users"] and stale_id in m.DATA["purged_users"])
returned=m.get_user_data(User(int(stale_id),"Stale","stale"))
ok("purged_payment_state_restored", returned["pass_premium"] and returned["processed_payments"]==["old-paid"] and returned["stars_earned"]==50, returned)

# Dynamic callback size boundaries for all artifact ids.
max_len=max(len(f"artifact:equip:{a['id']}".encode()) for a in m.ARTIFACTS)
ok("artifact_callbacks_within_64_bytes", max_len<=64, max_len)

print(f"PATCH40_FEATURE_SIMULATION_PASS {len(RESULTS)}")
for name in RESULTS: print("PASS",name)

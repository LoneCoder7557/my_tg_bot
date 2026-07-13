import os, sys, types, tempfile, importlib.util, asyncio, json
from pathlib import Path
from datetime import timedelta

SOURCE_ROOT = Path(__file__).resolve().parents[1]
WORK_ROOT = Path(tempfile.mkdtemp(prefix="abm_patch40_coretest_"))
ROOT = WORK_ROOT / "project"
ROOT.mkdir(parents=True, exist_ok=True)
# Import the exact shipped code/data in an isolated copy so the test never writes runtime
# DATA, extracted media, or generated cache into the repository itself.
import shutil
for name in ("bot.py", "cards.json", "promo_codes.json", "owner_ids.txt", "right_hand_ids.txt"):
    source = SOURCE_ROOT / name
    if source.exists():
        shutil.copy2(source, ROOT / name)
shutil.copytree(SOURCE_ROOT / "media_packs", ROOT / "media_packs")
if (SOURCE_ROOT / "media" / "ui" / "main_banner.png").exists():
    (ROOT / "media" / "ui").mkdir(parents=True, exist_ok=True)
    shutil.copy2(SOURCE_ROOT / "media" / "ui" / "main_banner.png", ROOT / "media" / "ui" / "main_banner.png")
TMP = WORK_ROOT / "data"
TMP.mkdir(parents=True, exist_ok=True)
os.environ["DATA_DIR"] = str(TMP)
os.environ["BOT_TOKEN"] = "STUB_TOKEN"
os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)

class DummyExpr:
    def __getattr__(self, name): return self
    def __eq__(self, other): return self
    def __ne__(self, other): return self
    def startswith(self, *a, **k): return self
    def in_(self, *a, **k): return self
    def __and__(self, other): return self
    def __or__(self, other): return self
class DummyObj:
    def __init__(self,*args,**kwargs):
        self.args=args
        for k,v in kwargs.items(): setattr(self,k,v)
class RouterProxy:
    def __init__(self): self.handlers=[]; self.middlewares=[]
    def __call__(self,*filters,**kwargs):
        def deco(fn): self.handlers.append((filters,kwargs,fn)); return fn
        return deco
    def middleware(self,mw): self.middlewares.append(mw)
class Dispatcher:
    def __init__(self):
        self.message=RouterProxy(); self.callback_query=RouterProxy(); self.pre_checkout_query=RouterProxy(); self._errors=RouterProxy()
    def errors(self,*a,**k): return self._errors(*a,**k)
    async def start_polling(self,*a,**k): pass
class Bot(DummyObj):
    async def delete_webhook(self,*a,**k): pass
    async def set_my_commands(self,*a,**k): pass
    async def set_my_short_description(self,*a,**k): pass
    async def set_my_description(self,*a,**k): pass
    async def send_message(self,*a,**k): pass
    async def get_me(self): return types.SimpleNamespace(username='stub_bot')
class BaseMiddleware: pass
class Command(DummyObj): pass
class CommandStart(DummyObj): pass
aiogram=types.ModuleType('aiogram'); aiogram.Bot=Bot; aiogram.Dispatcher=Dispatcher; aiogram.F=DummyExpr(); aiogram.BaseMiddleware=BaseMiddleware
atypes=types.ModuleType('aiogram.types')
for name in ['InlineKeyboardMarkup','InlineKeyboardButton','BotCommand','FSInputFile','LabeledPrice','BotCommandScopeDefault','BotCommandScopeChat','CallbackQuery','ErrorEvent','KeyboardButton','Message','PreCheckoutQuery','ReplyKeyboardMarkup']:
    setattr(atypes,name,type(name,(DummyObj,),{}))
aiogram.types=atypes
filters=types.ModuleType('aiogram.filters'); filters.Command=Command; filters.CommandStart=CommandStart
sys.modules['aiogram']=aiogram; sys.modules['aiogram.types']=atypes; sys.modules['aiogram.filters']=filters

spec=importlib.util.spec_from_file_location('abm_patch40_tested', ROOT/'bot.py')
m=importlib.util.module_from_spec(spec); sys.modules[spec.name]=m; spec.loader.exec_module(m)

# Keep tests isolated from any owner ID in the project file.
m.OWNER_IDS=set(); m.RIGHT_HAND_IDS=set()
m.DATA={
    'users':{}, 'friend_invites':{}, 'friend_requests':{}, 'friends':{},
    'deleted_users':{}, 'purged_users':{}, 'promo_usage':{}, 'payment_ledger':{},
    'payment_recovery_queue':{}, 'season_history':{}, 'storage_meta':{}
}
m._DATA_REVISION=0; m._DATA_LAST_SAVED_REVISION=0; m._DATA_DIRTY=False; m._DATA_SAVE_TASK=None

class User:
    def __init__(self, uid, name=None, username=None):
        self.id=int(uid); self.full_name=name or f'User {uid}'; self.username=username
class Msg:
    def __init__(self,user): self.from_user=user; self.sent=[]; self.text=''; self.media=[]
    async def answer(self,text,*a,**k): self.sent.append((text,k)); return None
    async def answer_photo(self,*a,**k): self.media.append('photo'); return None
    async def answer_animation(self,*a,**k): self.media.append('animation'); return None
    async def answer_video(self,*a,**k): self.media.append('video'); return None
    async def edit_reply_markup(self,*a,**k): return None
class Cb:
    def __init__(self,user,data): self.from_user=user; self.data=data; self.message=Msg(user); self.answers=[]
    async def answer(self,*a,**k): self.answers.append((a,k))

results=[]
def ok(name, cond, detail=''):
    if not cond: raise AssertionError(f'{name}: {detail}')
    results.append(name)

# 1) Dataset integrity in imported runtime.
ok('cards_count_17641', len(m.CARDS)==17641)
ok('card_ids_unique', len(m.CARD_BY_ID)==17641)
ok('real_media_index_896', len(m.REAL_MEDIA_IDS)==896, len(m.REAL_MEDIA_IDS))

# 2) New account exact base economy.
new=User(900001,'New User')
p=m.get_user_data(new)
ok('new_base_fistiks_1500', p['fistiks']==1500, p['fistiks'])
ok('new_pre_onboarding_attempts_0', p['card_attempts']==0, p['card_attempts'])
ok('new_pre_onboarding_empty_collection', len(p['collection'])==0)
ok('new_enters_onboarding', not p['onboarding_complete'] and not p['starter_bundle_claimed'])

# Avoid background save task noise from direct helper tests.
async def fake_flush(reason='test'): m._DATA_LAST_SAVED_REVISION=m._DATA_REVISION; m._DATA_DIRTY=False; return True
m.flush_data_now_async=fake_flush
# Unit-style simulation keeps revision semantics but disables background timers between asyncio.run loops.
m._schedule_data_save_if_needed=lambda: None
async def fake_keyboard(*a,**k): return None
m.ensure_quick_keyboard=fake_keyboard

async def onboarding_test():
    # Pick a universe that can safely produce 3 leaders and 5-card squad.
    universe=None
    for rec in m.visible_universes_for_menu():
        if len(m.onboarding_leader_options(new.id, rec['id']))==3:
            universe=rec['id']; break
    assert universe
    cb1=Cb(new,f'onboard:u:{universe}')
    await m.onboarding_universe_cb(cb1)
    p=m.DATA['users'][str(new.id)]
    assert p['onboarding_state']=='choose_leader' and len(p['onboarding_leader_options'])==3
    cb2=Cb(new,'onboard:leader:0')
    await m.onboarding_leader_cb(cb2)
    p=m.DATA['users'][str(new.id)]
    assert p['onboarding_complete'] and p['starter_bundle_claimed']
    assert len(p['starter_cards'])==5 and len(set(p['starter_cards']))==5
    assert p['starter_cards'][0] in p['deck'] and len(p['deck'])==5
    assert p['card_attempts']==3
    assert p['fistiks']==1500
    assert p['season_xp']==100
    before=(p['card_attempts'],p['fistiks'],p['season_xp'],json.dumps(p['collection'],sort_keys=True))
    cb3=Cb(new,'onboard:leader:0')
    await m.onboarding_leader_cb(cb3)
    after=(p['card_attempts'],p['fistiks'],p['season_xp'],json.dumps(p['collection'],sort_keys=True))
    assert before==after
    # persisted onboarding stage/result survives serialization
    assert m.save_data_now('coretest_onboarding')
    stored=json.loads(Path(m.DATA_FILE).read_text('utf-8'))
    assert stored['users'][str(new.id)]['onboarding_complete'] is True
asyncio.run(onboarding_test())
ok('onboarding_exact_grant_and_idempotency', True)
# Telegram size/callback boundaries for the new main and season screens.
main_text=m.main_menu_text(new)
season_text=m.season_screen_text(m.DATA['users'][str(new.id)])
ok('main_screen_within_message_limit', len(main_text)<=4096, len(main_text))
ok('season_screen_within_caption_limit', len(season_text)<=1024, len(season_text))
universe_callback_lengths=[]
for rec in m.visible_universes_for_menu():
    universe_callback_lengths.extend([len(f"onboard:u:{rec['id']}".encode('utf-8')), len(f"universe:set:{rec['id']}".encode('utf-8'))])
ok('dynamic_universe_callbacks_within_64_bytes', max(universe_callback_lengths, default=0)<=64, max(universe_callback_lengths, default=0))

# Interrupted onboarding persists its stage and resumes without a second starter grant.
resume=User(900015,'Resume User')
rp0=m.get_user_data(resume)
resume_universe=next(rec['id'] for rec in m.visible_universes_for_menu() if len(m.onboarding_leader_options(resume.id,rec['id']))==3)
asyncio.run(m.onboarding_universe_cb(Cb(resume,f'onboard:u:{resume_universe}')))
assert m.save_data_now('coretest_onboarding_interrupted')
resume_stored=json.loads(Path(m.DATA_FILE).read_text('utf-8'))['users'][str(resume.id)]
resume_live=m.get_user_data(resume)
ok('onboarding_interruption_persisted', resume_stored['onboarding_state']=='choose_leader' and resume_stored['preferred_universe']==resume_universe and len(resume_stored['onboarding_leader_options'])==3)
ok('onboarding_resume_no_early_bundle', not resume_live['starter_bundle_claimed'] and len(resume_live['collection'])==0 and resume_live['card_attempts']==0)

# 3) Existing legacy user is grandfathered without starter rewards or balance/collection rewrite.
legacy=User(900002,'Legacy')
legacy_card=m.CARDS[0]['id']
m.DATA['users'][str(legacy.id)]={'name':'Legacy','fistiks':4321,'card_attempts':7,'collection':{legacy_card:{'count':2,'shards':55,'level':4,'unlocked':True}},'moon_coins':9,'premium':True,'pass_premium':True,'clan_id':'abc','friends':['x'],'banned':True,'frozen':True}
q=m.get_user_data(legacy)
ok('legacy_no_onboarding', q['onboarding_complete'] and q['starter_bundle_claimed'])
ok('legacy_balance_preserved', q['fistiks']==4321 and q['card_attempts']==7 and q['moon_coins']==9)
ok('legacy_card_preserved', q['collection'][legacy_card]['count']==2 and q['collection'][legacy_card]['shards']==55 and q['collection'][legacy_card]['level']==4)
ok('legacy_premium_clan_preserved', q['premium'] and q['pass_premium'] and q['clan_id']=='abc')
ok('legacy_ban_freeze_preserved', q['banned'] and q['frozen'])

# 4) Tombstone blocks recreation; technical purge can restart fresh.
perm=User(900003,'Deleted')
m.DATA['deleted_users'][str(perm.id)]={'permanent':True}
try:
    m.get_user_data(perm)
    raise AssertionError('permanent user recreated')
except m.PermanentlyDeletedUserError:
    pass
ok('permanent_tombstone_blocks_recreation', str(perm.id) not in m.DATA['users'])
tech=User(900004,'Returned')
m.DATA['purged_users'][str(tech.id)]={'permanent':False,'reason':'bot_blocked'}
r=m.get_user_data(tech)
ok('technical_purge_can_restart', str(tech.id) in m.DATA['users'] and str(tech.id) not in m.DATA['purged_users'])

# Recovery only adds genuinely missing allowed users; current authority and tombstones win.
existing_id='900005'; allowed_id='900006'; deleted_id='900007'; purged_id='900008'
m.DATA['users'][existing_id]={'name':'Current','fistiks':111,'collection':{}}
m.DATA['deleted_users'][deleted_id]={'permanent':True}
m.DATA['purged_users'][purged_id]={'permanent':False,'reason':'technical'}
recovery_candidate={'users':{
    existing_id:{'name':'Old backup','fistiks':999999,'collection':{}},
    allowed_id:{'name':'Allowed restore','fistiks':222,'collection':{}},
    deleted_id:{'name':'Must stay deleted','fistiks':999999,'collection':{}},
    purged_id:{'name':'Must stay purged','fistiks':999999,'collection':{}},
}}
report=m._apply_recovery_candidates([('fixture', recovery_candidate)], save=False, merge_existing=False)
ok('recovery_adds_missing_allowed_only', allowed_id in m.DATA['users'] and report['added']==1, report)
ok('recovery_preserves_existing_authority', m.DATA['users'][existing_id]['fistiks']==111)
ok('recovery_respects_deleted_and_purged_tombstones', deleted_id not in m.DATA['users'] and purged_id not in m.DATA['users'])

# 5) Referral exact once, second inviter no second rewards.
inv1=User(900010,'Inv1'); inv2=User(900011,'Inv2'); refu=User(900012,'Referral')
i1=m.get_user_data(inv1); i2=m.get_user_data(inv2); rp=m.get_user_data(refu)
# Make ref user a normal new account before onboarding: base 1500/0 attempts.
b1=(i1['fistiks'],i1['card_attempts']); b2=(i2['fistiks'],i2['card_attempts']); br=(rp['fistiks'],rp['card_attempts'])
g1,_=m.apply_referral_once(str(inv1.id),str(refu.id),rp)
g2,_=m.apply_referral_once(str(inv1.id),str(refu.id),rp)
g3,_=m.apply_referral_once(str(inv2.id),str(refu.id),rp)
ok('referral_first_granted', g1)
ok('referral_repeat_blocked', not g2 and not g3)
ok('referral_newcomer_exact', rp['fistiks']==br[0]+300 and rp['card_attempts']==br[1]+1)
ok('referral_inviter_exact', i1['fistiks']==b1[0]+500 and i1['card_attempts']==b1[1]+3 and i1['ref_count']==1 and i1['ref_earned']==3)
ok('referral_other_inviter_no_reward', i2['fistiks']==b2[0] and i2['card_attempts']==b2[1])

# Global max_uses is atomic across different users; rewards cover all supported types.
promo_card=m.CARDS[1]['id']
promo_fixture={
    'ONCE':{'active':True,'expires':'2099-12-31','max_uses':1,'reward':{'fistiks':77,'moon_coins':2,'attempts':3,'card':promo_card,'shards':9}},
    'EXPIRED':{'active':True,'expires':'2000-01-01','max_uses':10,'reward':{'fistiks':1}},
    'OFF':{'active':False,'expires':'2099-12-31','max_uses':10,'reward':{'fistiks':1}},
}
Path(m.PROMO_FILE).write_text(json.dumps(promo_fixture), encoding='utf-8')
promo_u1=User(900013,'Promo1'); promo_u2=User(900014,'Promo2')
p1=m.get_user_data(promo_u1); p2=m.get_user_data(promo_u2)
before1=(p1['fistiks'],p1['moon_coins'],p1['card_attempts'])
before2=(p2['fistiks'],p2['moon_coins'],p2['card_attempts'])
async def promo_race():
    a=Msg(promo_u1); b=Msg(promo_u2)
    await asyncio.gather(m.apply_promo(a,'ONCE'),m.apply_promo(b,'ONCE'))
    return a,b
pm1,pm2=asyncio.run(promo_race())
winners=[]
for idx,(pl,before) in enumerate(((p1,before1),(p2,before2)),1):
    delta=(pl['fistiks']-before[0],pl['moon_coins']-before[1],pl['card_attempts']-before[2])
    if delta==(77,2,3): winners.append(idx)
ok('promo_global_max_uses_atomic', len(winners)==1 and m.DATA['promo_usage'].get('ONCE')==1, (winners,m.DATA['promo_usage']))
winner_user=promo_u1 if winners==[1] else promo_u2
winner_player=p1 if winners==[1] else p2
owned=(winner_player.get('collection',{}) or {}).get(promo_card)
ok('promo_card_and_shards_reward', isinstance(owned,dict) and int(owned.get('shards',0))>=9, owned)
before_repeat=(winner_player['fistiks'],winner_player['moon_coins'],winner_player['card_attempts'])
asyncio.run(m.apply_promo(Msg(winner_user),'ONCE'))
ok('promo_same_user_repeat_blocked', before_repeat==(winner_player['fistiks'],winner_player['moon_coins'],winner_player['card_attempts']))
orig_is_owner=m.is_owner; orig_get_user_data=m.get_user_data
promo_owner=User(900017,'Promo Owner'); promo_owner_p=m.get_user_data(promo_owner); owner_before=promo_owner_p['fistiks']; usage_before=m.DATA['promo_usage'].get('ONCE')
m.is_owner=lambda uid: int(uid)==promo_owner.id
m.get_user_data=lambda user: promo_owner_p if int(user.id)==promo_owner.id else orig_get_user_data(user)
try:
    asyncio.run(m.apply_promo(Msg(promo_owner),'ONCE')); asyncio.run(m.apply_promo(Msg(promo_owner),'ONCE'))
finally:
    m.is_owner=orig_is_owner; m.get_user_data=orig_get_user_data
ok('promo_owner_test_does_not_consume_public_quota', m.DATA['promo_usage'].get('ONCE')==usage_before and promo_owner_p['fistiks']==owner_before+154)

# Daily streak uses one APP_TIMEZONE calendar boundary and remains one-claim idempotent.
daily_user=User(900016,'Daily')
daily_p=m.get_user_data(daily_user); daily_p['onboarding_complete']=True; daily_p['starter_bundle_claimed']=True
today=m.app_now().date(); daily_p['last_daily']=(today-timedelta(days=1)).isoformat(); daily_p['daily_streak']=3
daily_before=(daily_p['fistiks'],daily_p['moon_coins'],daily_p['pass_xp'])
async def daily_race():
    a=Msg(daily_user); b=Msg(daily_user)
    await asyncio.gather(m.send_daily(a,daily_user),m.send_daily(b,daily_user))
    return a,b
da,db=asyncio.run(daily_race())
ok('daily_parallel_claim_exactly_once', daily_p['last_daily']==today.isoformat() and daily_p['daily_streak']==4 and (daily_p['fistiks'],daily_p['moon_coins'],daily_p['pass_xp'])!=daily_before)
ok('daily_second_result_reports_already_claimed', any('уже забрана' in text for msg in (da,db) for text,_ in msg.sent))

# 6) Season stable math, idempotent XP key, claims only once, rollover only season-local.
from datetime import datetime, timezone, timedelta
s1=m.season_info(datetime(2026,7,13,tzinfo=timezone.utc)); s1b=m.season_info(datetime(2026,8,9,23,59,59,tzinfo=timezone.utc)); s2=m.season_info(datetime(2026,8,10,tzinfo=timezone.utc))
ok('season_28d_boundaries', s1['id']==s1b['id'] and s2['id']!=s1['id'])
sp={'season_id':'','season_xp':0,'season_claimed':[],'season_action_keys':[],'fistiks':100,'card_attempts':0,'moon_coins':0,'collection':{},'badges':[],'pass_xp':777,'pass_premium':True,'clan_id':'keep'}
added1=m.add_season_xp(sp,500,action_key='same-action'); added2=m.add_season_xp(sp,500,action_key='same-action')
ok('season_xp_idempotent_key', added1==500 and added2==0 and sp['season_xp']==500)
_,gr1=m.claim_available_season_rewards(sp); state_after=(sp['fistiks'],sp['card_attempts'],sp['moon_coins'],tuple(sp['season_claimed']))
_,gr2=m.claim_available_season_rewards(sp); state_again=(sp['fistiks'],sp['card_attempts'],sp['moon_coins'],tuple(sp['season_claimed']))
ok('season_claim_once', len(gr1)>=1 and not gr2 and state_after==state_again)
ok('season_does_not_touch_multipass_clan', sp['pass_xp']==777 and sp['pass_premium'] and sp['clan_id']=='keep')
current_sid=m.season_info()['id']
rank_valid='900030'; rank_banned='900031'; rank_frozen='900032'; rank_deleted='900033'
for uid,flags in ((rank_valid,{}),(rank_banned,{'banned':True}),(rank_frozen,{'frozen':True}),(rank_deleted,{})):
    m.DATA['users'][uid]={'name':uid,'season_id':current_sid,'season_xp':999,**flags}
m.DATA['deleted_users'][rank_deleted]={'permanent':True}
rank_ids={uid for uid,_,_ in m.season_rank_rows()}
ok('season_ranking_excludes_banned_frozen_deleted', rank_valid in rank_ids and rank_banned not in rank_ids and rank_frozen not in rank_ids and rank_deleted not in rank_ids, rank_ids)

# 7) Per-user serialization + different-user parallelism + safe cleanup.
async def lock_test():
    mw=m.PerUserSerialMiddleware(idle_ttl=0, cleanup_interval=0)
    active={}; max_same=0; global_active=0; max_global=0
    guard=asyncio.Lock()
    async def handler(event,data):
        nonlocal max_same,global_active,max_global
        uid=event.from_user.id
        async with guard:
            active[uid]=active.get(uid,0)+1; global_active+=1
            max_same=max(max_same,active[uid]); max_global=max(max_global,global_active)
        await asyncio.sleep(0.02)
        async with guard:
            active[uid]-=1; global_active-=1
        return uid
    e1=types.SimpleNamespace(from_user=User(1)); e2=types.SimpleNamespace(from_user=User(2))
    await asyncio.gather(*[mw(handler,e1,{}) for _ in range(10)], *[mw(handler,e2,{}) for _ in range(10)])
    # cleanup is only safe after no locks/waiters
    mw._cleanup_idle(m.time.monotonic()+1)
    return max_same,max_global,mw.state_count()
ms,mg,sc=asyncio.run(lock_test())
ok('per_user_same_user_serialized', ms==1, ms)
ok('per_user_different_users_parallel', mg>=2, mg)
ok('per_user_idle_locks_cleaned', sc==0, sc)

# High-risk duplicate callbacks are rejected after per-user serialization.
class RiskCb(atypes.CallbackQuery):
    def __init__(self,user,data): self.from_user=user; self.data=data; self.answers=[]
    async def answer(self,*a,**k): self.answers.append((a,k))
async def debounce_test():
    serial=m.PerUserSerialMiddleware(idle_ttl=60,cleanup_interval=60)
    debounce=m.HighRiskCallbackDebounceMiddleware(); debounce.window=5.0
    calls={'n':0}
    async def high_handler(event,data):
        calls['n']+=1; await asyncio.sleep(0.01); return calls['n']
    async def chained(event,data): return await debounce(high_handler,event,data)
    e1=RiskCb(User(44),'draw_card'); e2=RiskCb(User(44),'draw_card')
    await asyncio.gather(serial(chained,e1,{}),serial(chained,e2,{}))
    return calls['n'],len(e1.answers)+len(e2.answers)
dc,answers=asyncio.run(debounce_test())
ok('high_risk_duplicate_callback_debounced', dc==1 and answers>=1, (dc,answers))

# Revisioned snapshot rejects a copy crossed by a mutation and retries to the new revision.
orig_deepcopy=m.copy.deepcopy
copy_calls={'n':0}
def racing_deepcopy(obj):
    out=orig_deepcopy(obj)
    if obj is m.DATA and copy_calls['n']==0:
        copy_calls['n']+=1
        m.DATA['snapshot_race_marker']='newer'
        m._DATA_REVISION+=1
        m._DATA_DIRTY=True
    return out
m.copy.deepcopy=racing_deepcopy
try:
    snapshot,snapshot_revision=m._snapshot_data_consistent_sync()
finally:
    m.copy.deepcopy=orig_deepcopy
ok('snapshot_mutation_retried_to_new_revision', snapshot.get('snapshot_race_marker')=='newer' and snapshot_revision==m._DATA_REVISION, (snapshot_revision,m._DATA_REVISION))

# Save failure leaves DATA dirty; a later serialized save persists the newer revision.
orig_save_snapshot=m._save_snapshot_sync
def save_revision_test_sync(snapshot): return True
async def save_revision_test():
    m._DATA_SAVE_LOCK=None
    m._DATA_REVISION=max(1,m._DATA_REVISION)
    m._DATA_LAST_SAVED_REVISION=0
    m._DATA_DIRTY=True
    calls={'n':0}; saved=[]
    def flaky(snapshot):
        calls['n']+=1
        if calls['n']==1: raise RuntimeError('simulated save failure')
        saved.append(int(snapshot.get('storage_meta',{}).get('saved_revision',-1)))
        return True
    m._save_snapshot_sync=flaky
    failed=False
    try:
        await m._save_one_revision('simulated_failure')
    except RuntimeError:
        failed=True
    dirty_after_failure=m._DATA_DIRTY
    await m._save_one_revision('retry_success')
    return failed,dirty_after_failure,saved,m._DATA_LAST_SAVED_REVISION,m._DATA_REVISION,m._DATA_DIRTY
try:
    sf=asyncio.run(save_revision_test())
finally:
    m._save_snapshot_sync=orig_save_snapshot
ok('save_failure_keeps_dirty_then_retry_succeeds', sf[0] and sf[1] and sf[2] and sf[3]==sf[4] and not sf[5], sf)

# Configured authoritative storage cannot silently fall back when psycopg is absent.
orig_database_url,orig_psycopg=m.DATABASE_URL,m.psycopg
m.DATABASE_URL='configured_authoritative_storage'; m.psycopg=None
raised=False
try:
    m.load_data_storage({'users':{},'friend_invites':{},'friends':{}})
except RuntimeError:
    raised=True
finally:
    m.DATABASE_URL=orig_database_url; m.psycopg=orig_psycopg
ok('configured_neon_without_driver_fails_closed', raised)

# 8) Payment reward idempotency in core ledger and parallel successful_payment local-mode delivery.
payuser=User(900020,'Pay')
pp=m.get_user_data(payuser); pp['onboarding_complete']=True; pp['starter_bundle_claimed']=True
pack_code=next(iter(m.STAR_PACKS))
amount=int(m.STAR_PACKS[pack_code]['stars']) if isinstance(m.STAR_PACKS[pack_code],dict) and 'stars' in m.STAR_PACKS[pack_code] else m.expected_payment_amount(f'star_pack:{pack_code}:{payuser.id}')
payload=f'star_pack:{pack_code}:{payuser.id}'
expected=m.expected_payment_amount(payload)
assert expected is not None
before_f=pp['fistiks']; before_a=pp['card_attempts']; before_d=pp['moon_coins']
event={'charge_id':'core-charge-1','user_id':str(payuser.id),'payload':payload,'amount':expected,'currency':m.PAYMENT_CURRENCY,'kind':'star_pack','code':pack_code,'received_at':m.utc_now().isoformat()}
po1,txt1=m._apply_payment_event_reward(event); state1=(pp['fistiks'],pp['card_attempts'],pp['moon_coins'])
po2,txt2=m._apply_payment_event_reward(event); state2=(pp['fistiks'],pp['card_attempts'],pp['moon_coins'])
ok('payment_core_first_grant', po1 and state1!=(before_f,before_a,before_d))
ok('payment_core_duplicate_no_regrant', po2 and state1==state2)
ok('payment_global_ledger_present', 'core-charge-1' in m.DATA['payment_ledger'])

# Full successful_payment handler: two concurrent deliveries of one charge grant exactly once.
async def no_notify(*a,**k): return None
m.notify_owner_purchase=no_notify
m._PAYMENT_PROCESS_LOCK=None
handler_charge='core-handler-charge-2'
handler_payment=types.SimpleNamespace(invoice_payload=payload,currency=m.PAYMENT_CURRENCY,total_amount=expected,telegram_payment_charge_id=handler_charge,provider_payment_charge_id='')
h_before=(pp['fistiks'],pp['moon_coins'],pp.get('stars_earned',0),len(pp.get('purchases',[])))
async def payment_handler_race():
    a=Msg(payuser); b=Msg(payuser); a.successful_payment=handler_payment; b.successful_payment=handler_payment
    await asyncio.gather(m.successful_payment(a),m.successful_payment(b))
    return a,b
pha,phb=asyncio.run(payment_handler_race())
pack=m.STAR_PACKS[pack_code]
matching_purchases=[x for x in pp.get('purchases',[]) if isinstance(x,dict) and x.get('id')==handler_charge]
ok('successful_payment_parallel_exactly_once',
   pp['fistiks']-h_before[0]==int(pack.get('fistiks',0)) and
   pp['moon_coins']-h_before[1]==int(pack.get('moon_coins',0)) and
   pp.get('stars_earned',0)-h_before[2]==expected and len(matching_purchases)==1,
   (pp['fistiks']-h_before[0],pp['moon_coins']-h_before[1],pp.get('stars_earned',0)-h_before[2],len(matching_purchases)))

# Pre-checkout rejects foreign user, wrong currency/amount and unknown payload; accepts exact invoice.
class PreCheckout:
    def __init__(self,user,payload,currency,amount):
        self.from_user=user; self.invoice_payload=payload; self.currency=currency; self.total_amount=amount; self.answers=[]
    async def answer(self,**kwargs): self.answers.append(kwargs)
async def precheckout_test():
    qs=[
        PreCheckout(payuser,payload,m.PAYMENT_CURRENCY,expected),
        PreCheckout(User(payuser.id+1),payload,m.PAYMENT_CURRENCY,expected),
        PreCheckout(payuser,payload,'USD',expected),
        PreCheckout(payuser,payload,m.PAYMENT_CURRENCY,expected+1),
        PreCheckout(payuser,'unknown',m.PAYMENT_CURRENCY,expected),
    ]
    for q in qs: await m.pre_checkout_query(q)
    return [q.answers[-1].get('ok') for q in qs]
pc=asyncio.run(precheckout_test())
ok('precheckout_strict_validation', pc==[True,False,False,False,False], pc)

# 9) Real-art preference never changes rarity: candidate must remain requested rarity.
rarity='Обычный'
card=m.roll_card(allowed_rarities=[rarity], universe_id='all')
ok('roll_card_rarity_preserved', card is not None and card['rarity']==rarity)

# 10) Generated cache concurrency: two requests same missing-art card resolve to one valid path or safe None.
missing=next(c['id'] for c in m.CARDS if c['id'] not in m.REAL_MEDIA_IDS)
async def gen_test():
    out=await asyncio.gather(*[m.resolve_card_media_async(missing) for _ in range(2)])
    return out
outs=asyncio.run(gen_test())
valid=[x for x in outs if x is not None]
ok('generated_card_concurrent_safe', (not valid) or (all(str(x)==str(valid[0]) for x in valid) and Path(valid[0]).exists() and Path(valid[0]).stat().st_size>0))
ok('generated_card_lock_state_reclaimed', len(m._GENERATED_CARD_ASYNC_LOCKS)==0 and len(m._GENERATED_CARD_THREAD_LOCKS)==0, (len(m._GENERATED_CARD_ASYNC_LOCKS),len(m._GENERATED_CARD_THREAD_LOCKS)))

# Media sender routes GIF/MP4/image correctly; missing Pillow/disk errors degrade to safe text path.
orig_resolve=m.resolve_card_media_async
media_dir=WORK_ROOT/'media_branches'; media_dir.mkdir(exist_ok=True)
for ext in ('.gif','.mp4','.jpg'): (media_dir/f'x{ext}').write_bytes(b'fixture')
async def media_branch_test():
    out=[]
    for ext,kind in (('.gif','animation'),('.mp4','video'),('.jpg','photo')):
        async def resolver(_cid, path=media_dir/f'x{ext}'): return path
        m.resolve_card_media_async=resolver
        msg=Msg(User(55)); sent=await m.send_card_result(msg,m.CARDS[0]['id'],'<b>ok</b>')
        out.append((sent,msg.media[-1] if msg.media else None,kind))
    return out
try:
    media_routes=asyncio.run(media_branch_test())
finally:
    m.resolve_card_media_async=orig_resolve
ok('media_gif_mp4_photo_routes', all(sent and actual==expected_kind for sent,actual,expected_kind in media_routes), media_routes)
orig_image,orig_draw,orig_font=m.Image,m.ImageDraw,m.ImageFont
m.Image=m.ImageDraw=m.ImageFont=None
try:
    no_pillow=m.make_card_banner(missing)
finally:
    m.Image,m.ImageDraw,m.ImageFont=orig_image,orig_draw,orig_font
ok('missing_pillow_safe_fallback', no_pillow is None)
orig_make_unlocked=m._make_card_banner_unlocked
def no_space(_cid): raise OSError(28,'No space left on device')
m._make_card_banner_unlocked=no_space
try:
    no_space_result=m.make_card_banner(missing)
finally:
    m._make_card_banner_unlocked=orig_make_unlocked
ok('generated_cache_disk_full_safe_fallback', no_space_result is None)
orig_gen_dir,orig_max_files,orig_max_mb=m.GENERATED_CARDS_DIR,m.GENERATED_CACHE_MAX_FILES,m.GENERATED_CACHE_MAX_MB
cache_test=WORK_ROOT/'cache_limit'; cache_test.mkdir(exist_ok=True)
for i in range(5): (cache_test/f'{i}.png').write_bytes(b'x'*100)
m.GENERATED_CARDS_DIR=cache_test; m.GENERATED_CACHE_MAX_FILES=2; m.GENERATED_CACHE_MAX_MB=32; m._GENERATED_CACHE_LAST_CLEANUP=0
try:
    removed=m.cleanup_generated_card_cache_sync(force=True); remaining=len(list(cache_test.glob('*.png')))
finally:
    m.GENERATED_CARDS_DIR=orig_gen_dir; m.GENERATED_CACHE_MAX_FILES=orig_max_files; m.GENERATED_CACHE_MAX_MB=orig_max_mb
ok('generated_cache_hard_file_limit', removed>=3 and remaining<=2, (removed,remaining))

print('CORE_SIMULATION_PASS',len(results))
for name in results: print('PASS',name)
print('ISOLATED_WORK_ROOT', WORK_ROOT)
shutil.rmtree(WORK_ROOT, ignore_errors=True)

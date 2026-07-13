#!/usr/bin/env python3
"""Stdlib-only static/integrity audit for PATCH40.
Run from repository root: python tests/patch40_static_audit.py
"""
from __future__ import annotations
import ast, builtins, hashlib, json, re, symtable, zipfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT = ROOT / "bot.py"
CARDS = ROOT / "cards.json"
MANIFEST = ROOT / "tests" / "patch35_compat_manifest.json"
MEDIA_PACKS = ROOT / "media_packs"
ALLOWED_MEDIA = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}

source = BOT.read_text(encoding="utf-8")
tree = ast.parse(source, filename=str(BOT))


def lit(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def extract_patterns(tree):
    exact, prefixes, commands = set(), set(), set()
    button_exact, button_prefixes = [], []
    callback_funcs = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                for sub in ast.walk(dec):
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "Command" and sub.args:
                        value = lit(sub.args[0])
                        if value:
                            commands.add(value)
                if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute) and dec.func.attr == "callback_query":
                    callback_funcs.append(node)
                    for sub in ast.walk(dec):
                        if isinstance(sub, ast.Compare) and isinstance(sub.left, ast.Attribute) and sub.left.attr == "data" and len(sub.ops) == 1 and isinstance(sub.ops[0], ast.Eq) and len(sub.comparators) == 1:
                            value = lit(sub.comparators[0])
                            if value:
                                exact.add(value)
                        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and sub.func.attr == "startswith" and sub.args:
                            value = lit(sub.args[0])
                            if value:
                                prefixes.add(value)
                        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and sub.func.attr == "in_" and sub.args and isinstance(sub.args[0], (ast.Set, ast.Tuple, ast.List)):
                            for el in sub.args[0].elts:
                                value = lit(el)
                                if value:
                                    exact.add(value)
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg != "callback_data":
                    continue
                value = lit(kw.value)
                if value is not None:
                    button_exact.append((value, node.lineno))
                elif isinstance(kw.value, ast.JoinedStr):
                    prefix = ""
                    for part in kw.value.values:
                        if isinstance(part, ast.Constant) and isinstance(part.value, str):
                            prefix += part.value
                        else:
                            break
                    if prefix:
                        button_prefixes.append((prefix, node.lineno))
    return exact, prefixes, commands, button_exact, button_prefixes, callback_funcs


def covered(value, exact, prefixes):
    return value in exact or any(value.startswith(p) for p in prefixes)


def prefix_covered(prefix, prefixes):
    return prefix in prefixes or any(prefix.startswith(p) for p in prefixes)

# Syntax + duplicate top-level definitions.
body_defs = [n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
duplicates = [name for name, count in Counter(body_defs).items() if count > 1]
assert not duplicates, f"Duplicate top-level definitions: {duplicates}"

# Symtable-based undefined global name audit.
st = symtable.symtable(source, str(BOT), "exec")
module_defined = {
    name for name in st.get_identifiers()
    if (lambda s: s.is_assigned() or s.is_imported() or s.is_namespace())(st.lookup(name))
}
builtin_names = set(dir(builtins)) | {"__file__", "__name__", "__package__", "__spec__", "__loader__", "__cached__"}
missing = set()
def walk_table(table, scope="module"):
    for name in table.get_identifiers():
        sym = table.lookup(name)
        if sym.is_referenced() and sym.is_global() and name not in module_defined and name not in builtin_names:
            missing.add((scope, name))
    for child in table.get_children():
        walk_table(child, scope + "/" + child.get_name())
walk_table(st)
assert not missing, f"Possible undefined global names: {sorted(missing)}"

exact, prefixes, commands, button_exact, button_prefixes, callback_funcs = extract_patterns(tree)
missing_buttons = sorted({v for v, _ in button_exact if not covered(v, exact, prefixes)})
assert not missing_buttons, f"Static callback buttons without handler: {missing_buttons}"

# Every callback handler must answer Telegram's callback spinner directly or delegate
# only to one of the audited helpers that does so after its critical action.
def direct_callback_answer(fn):
    first_arg = fn.args.args[0].arg if fn.args.args else None
    if not first_arg:
        return False
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "answer"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == first_arg
        ):
            return True
    return False

no_direct_answer = {fn.name for fn in callback_funcs if not direct_callback_answer(fn)}
expected_delegates = {"fight_start", "fight", "draw_card_cb", "buy_pack", "case_open"}
assert no_direct_answer == expected_delegates, f"Callback handlers without audited answer path: {sorted(no_direct_answer)}"
def function_node(name):
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None

start_solo_helper = function_node("start_solo_fight")
draw_card_helper = function_node("draw_card_to_message")
assert start_solo_helper is not None and direct_callback_answer(start_solo_helper), "start_solo_fight must answer callback"
assert draw_card_helper is not None and direct_callback_answer(draw_card_helper), "draw_card_to_message must answer callback"
for delegate_name in ("draw_card_cb", "buy_pack", "case_open"):
    fn = function_node(delegate_name)
    assert fn is not None, f"Missing callback delegate: {delegate_name}"
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "draw_card_to_message"
        for node in ast.walk(fn)
    ), f"{delegate_name} must delegate to draw_card_to_message"

# PATCH35 compatibility manifest.
manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
missing_old_exact = [v for v in manifest["handler_exact"] if not covered(v, exact, prefixes)]
missing_old_prefix = [p for p in manifest["handler_prefixes"] if not prefix_covered(p, prefixes)]
missing_old_commands = sorted(set(manifest["commands"]) - commands)
assert not missing_old_exact, f"PATCH35 callback exact aliases lost: {missing_old_exact}"
assert not missing_old_prefix, f"PATCH35 callback prefix aliases lost: {missing_old_prefix}"
assert not missing_old_commands, f"PATCH35 commands lost: {missing_old_commands}"

# Exact static callback_data Telegram byte limit.
long_static = [(v, line, len(v.encode("utf-8"))) for v, line in button_exact if len(v.encode("utf-8")) > 64]
assert not long_static, f"Static callback_data over 64 bytes: {long_static}"

# Handler pattern duplicates.
pattern_counts = Counter(("exact", x) for x in exact)
pattern_counts.update(("prefix", x) for x in prefixes)
assert all(v == 1 for v in pattern_counts.values())

# Critical safety strings/invariants.
assert "drop_pending_updates=False" in source
assert "drop_pending_updates=True" not in source
assert 'PATCH_VERSION = "PATCH40_FINAL_STABLE_HOTFIX"' in source
assert "DATA_SCHEMA_VERSION = 40" in source
assert "PAYMENT_LEDGER_TABLE" in source and "charge_id TEXT PRIMARY KEY" in source
assert "PerUserSerialMiddleware" in source and "global_error_handler" in source
assert "roll_card_with_pity" in source
assert 'LEGACY_UNSAFE_RIGHT_HAND_SHA256 = "51505dcd0329ecdf5cd799239b7eb97da8fa2a1d2e718d3a56e63f396b2fe47e"' in source
assert "ABM_ALLOW_LEGACY_RIGHT_HAND_ID" in source
assert "TODO" not in source and "FIXME" not in source and "NotImplementedError" not in source

# Cards and media integrity.
cards = json.loads(CARDS.read_text(encoding="utf-8"))
ids = [str(c.get("id", "")) for c in cards]
assert len(cards) == 17641, len(cards)
assert len(set(ids)) == 17641
assert all(ids)
card_ids = set(ids)
media_ids, unsafe_paths = set(), []
media_members = 0
pack_hashes = {}
for pack in sorted(MEDIA_PACKS.glob("*.zip")):
    pack_hashes[pack.name] = hashlib.sha256(pack.read_bytes()).hexdigest()
    with zipfile.ZipFile(pack) as zf:
        assert zf.testzip() is None, f"Corrupt media pack: {pack.name}"
        for info in zf.infolist():
            path = Path(info.filename)
            if path.is_absolute() or ".." in path.parts:
                unsafe_paths.append(info.filename)
            if path.suffix.lower() in ALLOWED_MEDIA:
                media_members += 1
                media_ids.add(path.stem)
assert not unsafe_paths, unsafe_paths
assert media_members == 896, media_members
assert len(media_ids) == 896, len(media_ids)
assert media_ids <= card_ids, f"Orphan media ids: {sorted(media_ids - card_ids)[:10]}"

# Packaging hygiene for the working tree.
for path in ROOT.rglob("*"):
    if path.is_dir():
        assert path.name != "__pycache__", f"Forbidden cache dir: {path}"
    elif path.is_file():
        assert path.suffix != ".pyc", f"Forbidden pyc: {path}"
        assert not path.name.endswith(".tmp"), f"Temporary file: {path}"

# Secret-pattern scan: examples are allowed only when empty/placeholder.
for path in ROOT.rglob("*"):
    if not path.is_file() or path.suffix.lower() in {".zip", ".png", ".jpg", ".jpeg", ".webp", ".gif", ".mp4"}:
        continue
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        continue
    assert not re.search(r"postgres(?:ql)?://[^\s<>{}]+", text, re.I), f"Database URL-like secret in {path}"
    assert not re.search(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b", text), f"Telegram token-like secret in {path}"

print("STATIC_AUDIT_PASS")
print("callbacks_exact", len(exact), "callbacks_prefix", len(prefixes), "commands", len(commands))
print("cards", len(cards), "media", len(media_ids))
print("media_pack_sha256", json.dumps(pack_hashes, ensure_ascii=False, sort_keys=True))

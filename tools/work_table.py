"""Shared work table: a small always-loaded view of what each agent session and Hub job is doing.

Hooks (Claude Code, Codex, Antigravity), the Hub and agents add events; a fold turns them into one
table of at most 2,000 characters. SessionStart injects the table, UserPromptSubmit (agy: PreInvocation)
injects only the rows that changed since this session last saw `gen`, and prints nothing when `gen`
did not change. A row's `ptr` is the one hop to the details; there is no further lookup chain.

Storage is machine-local (~/.agent-work, override AGENT_WORK_DIR) and never goes to Git or Drive.
Writers never wait for each other: an event is one new file in inbox.d/ (written then renamed).
Whoever folds holds .lock, appends the events to inbox.jsonl (the append-only journal) and replaces
state.json and table.md. Summaries pass the Hub secret-scrub rules, long hex runs are removed and
absolute paths become logical (work: app: ~). Standard library only.
"""
import argparse
import fnmatch
import glob
import hashlib
import json
import os
import re
import sys
import subprocess
import time

MAX_CHARS = 2000      # always-loaded budget (S5 6.1); Claude caps a hook string at 10,000, Codex at ~2,500 tokens
MAX_ROWS = 24
PER_WORKER = 6        # like TLB_NR_DYN_ASIDS: a few rows per worker, the rest one `show --all` away
SUM_CHARS = 60
LIVE_S = 30 * 60      # no event for 30 minutes: shown as unconfirmed, never as done (Orca #30186)
DONE_SHOW_S = 24 * 3600
KEEP_S = 7 * 24 * 3600
DELTA_MAX = 8         # more changed rows than this (a third of the table): send the whole table instead
CLAIM_TTL = 30 * 60
CLAIM_GRACE = 15 * 60  # owner activity keeps a claim alive this much longer (mcp_agent_mail 1800 s + 900 s)
JOURNAL_MAX = 4 * 1024 * 1024
LOCK_WAIT_S = 1.5
STATES = ("planned", "working", "idle", "waiting", "blocked", "done", "failed", "unconfirmed")
TERMINAL = ("done", "failed")
# Hooks own the turn state (Orca: hooks before screen text; Corgi: working/needs-you/finished). A turn starting or
# ending is not news to other sessions, so working <-> idle does not bump gen; entering or leaving waiting/blocked does.
QUIET = ("working", "idle")
RANK = {"waiting": 0, "blocked": 0, "working": 1, "planned": 2}  # attention first; idle and unconfirmed 3, ended 4
NEEDS_YOU = {"permission_prompt": "permission", "elicitation_dialog": "input", "elicitation_url_dialog": "input", "agent_needs_input": "input"}
WRAP_UP = "mcp__ccd_turn__wrap_up"  # Claude desktop app: a wrap-up with `ask` ends the turn on a question to the user
HARNESS = {"claude": "claude-code", "codex": "codex", "agy": "antigravity", "hub": "hub", "cli": "cli"}

now = time.time


def default_dir():
    # Not under AppData: an MSIX-packaged app (the Claude desktop app) moves folders its processes create in %LOCALAPPDATA%
    # into its package, so its hooks and the Hub, Codex and agy outside it kept two different tables (2026-10-28).
    return os.path.join(os.path.expanduser("~"), ".agent-work")


def data_dir():
    return os.environ.get("AGENT_WORK_DIR") or default_dir()


def legacy_dirs(root):
    """Folders the default table used before ~/.agent-work: %LOCALAPPDATA%\\agent-work as this process sees it (Hub 1.32.24+
    still queues events there) and MSIX package copies of it. A table at any other folder (--dir, tests) has none."""
    if os.path.normcase(os.path.abspath(root)) != os.path.normcase(os.path.abspath(default_dir())):
        return []
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
    dirs = [os.path.join(base, "agent-work")] + sorted(glob.glob(os.path.join(base, "Packages", "*", "LocalCache", "Local", "agent-work")))
    return [d for d in dirs if os.path.isdir(d)]


# ---------- sanitizing (same shapes as the Hub's secret-scrub.js) ----------
KEY_VALUE = re.compile(r"((?:authorization|api[-_]?key|access[-_]?token|refresh[-_]?token|password|secret)[\"']?\s*[:=]\s*)(?:[\"'][^\"'\r\n]*[\"']|[^\s,;}]+)", re.I)
KEY_SHAPE = re.compile(r"(?:apikey_[a-z0-9_]{20,}|sk-[a-z0-9_-]{20,}|AIza[a-z0-9_-]{25,}|AQ\.[a-z0-9_-]{25,}|Bearer\s+[a-z0-9_.-]{20,})", re.I)
AUTH_FIELD = re.compile(r"(authToken|apiKey|api_key|password|secret)\s*[:=]\s*[\"']?[^\"'\s,}]+", re.I)
HEX_RUN = re.compile(r"[0-9a-f]{32,}", re.I)
OTHER_USER = re.compile(r"[A-Za-z]:[\\/]+Users[\\/]+[^\\/\s\"'|,;]+", re.I)
SYNCED = re.compile(r"\bG:[\\/][^\s\"'|]*")


def redact_credentials(s):
    """secret-scrub.redactCredentials: key=value and key shapes, overlapping spans merged before replacing."""
    spans = sorted([(m.start() + len(m.group(1)), m.end()) for m in KEY_VALUE.finditer(s)] + [(m.start(), m.end()) for m in KEY_SHAPE.finditer(s)])
    merged = []
    for a, b in spans:
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    out, at = [], 0
    for a, b in merged:
        out.append(s[at:a] + "[redacted]")
        at = b
    return "".join(out) + s[at:]


def _prefixes():
    """(absolute folder, logical tag) longest first. work: = Documents/Codex, app: = the Hub app folder (same name as the Hub bus mount)."""
    home = os.path.expanduser("~")
    docs = os.path.join(home, "Documents")
    return [(os.path.join(docs, "Codex"), "work:"), (os.path.join(docs, "AI-All-InOne"), "app:"), (home, "~/")]


def _prefix_res():
    # One pattern per folder that matches either separator, so text from any shell maps the same way.
    # `rest` is the remainder of that path token (up to a space or quote); a bare folder ends at a boundary.
    return [(re.compile("".join(r"[\\/]+" if ch in "\\/" else re.escape(ch) for ch in os.path.normpath(p))
                        + r"(?:(?P<sep>[\\/]+)(?P<rest>[^\s\"'`|<>]*)|(?=[^\w.-]|$))", re.I), tag)
            for p, tag in _prefixes()]


def logical(path, cwd=None):
    """Absolute path -> work:/app:/~/ form (forward slashes). Relative paths resolve against cwd."""
    if not path:
        return ""
    p = str(path)
    if re.match(r"^(work|app):", p):
        return p.replace("\\", "/")
    if p.startswith("~"):
        p = os.path.expanduser(p)
    elif cwd and not os.path.isabs(p):
        p = os.path.join(resolve(cwd), p)
    norm = os.path.normpath(p)
    for rx, tag in _prefix_res():
        m = rx.match(norm)
        if m:
            rest = norm[m.start("rest"):].replace("\\", "/") if m.group("sep") else ""  # whole remainder, spaces included
            return tag + rest if rest or tag != "~/" else "~"
    return SYNCED.sub("<synced-folder>", OTHER_USER.sub("~", norm.replace("\\", "/")))


def resolve(ptr):
    """Inverse of logical() for work:/app:/~/ forms; other strings are returned unchanged."""
    for prefix, tag in _prefixes():
        if tag != "~/" and ptr.startswith(tag):
            return os.path.join(prefix, ptr[len(tag):].replace("/", os.sep))
    return os.path.expanduser(ptr) if ptr.startswith("~") else ptr


def clean(text, limit=SUM_CHARS):
    """One table cell: credentials and long hex removed, user paths logical, one line, no `|`, at most `limit` chars."""
    s = HEX_RUN.sub("[redacted]", AUTH_FIELD.sub(r"\1: [redacted]", redact_credentials(str(text or ""))))
    for rx, tag in _prefix_res():
        # the folder becomes its tag and the rest of that path token gets forward slashes
        s = rx.sub(lambda m, t=tag: (t if m.group("sep") or t != "~/" else "~") + (m.group("rest") or "").replace("\\", "/"), s)
    s = SYNCED.sub("<synced-folder>", OTHER_USER.sub("~", s))
    s = re.sub(r"\s+", " ", s.replace("|", "/")).strip()
    return s if len(s) <= limit else s[:limit - 1] + "…"


REMINDER = re.compile(r"<system-reminder>.*?(?:</system-reminder>|$)", re.S | re.I)


def head(text, limit=SUM_CHARS):
    """First line that says something: skips harness reminder blocks, blank lines, tag lines, fences and lead-ins ending with ':'."""
    # The desktop app puts status reminders inside the prompt text a UserPromptSubmit hook receives (seen 2026-10-26).
    for line in REMINDER.sub("", str(text or "")).splitlines():
        t = line.strip()
        if not t or t.startswith(("<", "```")) or t.endswith((":", "：")):
            continue
        t = re.sub(r"^[#>*\-\s]+", "", t).replace("**", "")
        if len(t) >= 4:
            return clean(t, limit)
    return ""


def key_of(harness, sid):
    """Row key: 16 hex of sha256 — the session id itself is never stored or shown."""
    return hashlib.sha256((HARNESS.get(harness, harness) + "\0" + str(sid)).encode("utf-8")).hexdigest()[:16]


# ---------- storage ----------
class Locked:
    """Exclusive lock on <dir>/.lock (msvcrt on Windows, flock elsewhere). Released when the process ends, too."""

    def __init__(self, root, wait=LOCK_WAIT_S):
        self.path, self.wait, self.fd = os.path.join(root, ".lock"), wait, None

    def __enter__(self):
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT)
        deadline = time.monotonic() + self.wait
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.fd = fd
                return self
            except OSError:
                if time.monotonic() > deadline:
                    os.close(fd)
                    raise TimeoutError("work table lock busy")
                time.sleep(0.02)

    def __exit__(self, *exc):
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(self.fd, 0, 0)
                msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
        finally:
            os.close(self.fd)


def _retry(fn, tries=25):
    # Windows refuses to replace or open a file another process holds for a moment (sharing violation).
    for i in range(tries):
        try:
            return fn()
        except PermissionError:
            if i == tries - 1:
                raise
            time.sleep(0.02)


def read_text(path):
    def once():
        with open(path, encoding="utf-8") as f:  # closed at once: an open handle blocks the next replace on Windows
            return f.read()
    return _retry(once)


def write_atomic(path, text):
    tmp = "%s.%d.tmp" % (path, os.getpid())
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    _retry(lambda: os.replace(tmp, path))


def blank():
    return {"schema": "agent-work/1", "gen": 0, "epoch": 1, "rows": {}, "claims": {}, "seen": {}, "prunedAt": 0}


def load(root):
    path = os.path.join(root, "state.json")
    try:
        text = read_text(path)
    except FileNotFoundError:
        return blank()
    try:
        st = json.loads(text)
        if st.get("schema") != "agent-work/1":
            raise ValueError("schema")
        return st
    except ValueError:
        # A broken state is moved aside, not overwritten; the journal still holds every event.
        _retry(lambda: os.replace(path, "%s.corrupt-%d" % (path, int(now()))))
        return blank()


def emit(event, root=None):
    """Queue one event without taking the lock: a uniquely named file renamed into inbox.d/."""
    root = root or data_dir()
    q = os.path.join(root, "inbox.d")
    os.makedirs(q, exist_ok=True)
    event = dict(event)
    event.setdefault("t", now())
    name = "%019d-%d-%s" % (time.time_ns(), os.getpid(), os.urandom(3).hex())
    tmp = os.path.join(q, name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False))
    os.replace(tmp, os.path.join(q, name + ".json"))


def pending(root):
    """Queued event files, oldest first: this table's inbox.d and the old ones (the same file seen by two paths counts once)."""
    found = {}
    for d in [root] + legacy_dirs(root):
        try:
            for n in os.listdir(os.path.join(d, "inbox.d")):
                if n.endswith(".json"):
                    found.setdefault(n, os.path.join(d, "inbox.d", n))
        except FileNotFoundError:
            continue
    return [found[n] for n in sorted(found)]


def history(dirs):
    """Every journaled event of the old tables, oldest first and each once, with their highest gen and epoch."""
    lines, gen, epoch = {}, 0, 1
    for d in dirs:
        for path in sorted(glob.glob(os.path.join(d, "inbox*.jsonl"))):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        lines.setdefault(line.strip(), None)
        try:
            old = json.loads(read_text(os.path.join(d, "state.json")))
            gen, epoch = max(gen, int(old.get("gen", 0))), max(epoch, int(old.get("epoch", 1)))
        except (OSError, ValueError, TypeError, AttributeError):
            pass
    events = []
    for line in lines:
        try:
            events.append(json.loads(line))
        except ValueError:
            continue
    return sorted(events, key=lambda e: float(e.get("t") or 0)), gen, epoch


def unmigrated(root):
    """Old tables to merge into a default table that has no state yet (once; migration.json records it)."""
    if os.path.exists(os.path.join(root, "state.json")) or os.path.exists(os.path.join(root, "migration.json")):
        return []
    return [d for d in legacy_dirs(root) if glob.glob(os.path.join(d, "inbox*.jsonl")) or os.path.exists(os.path.join(d, "state.json"))]


def _new_row(e):
    # sum = what an agent or the Hub said (note, job); p = the first prompt's head; tail = the latest answer's head.
    # wt: the worker name came from a session title (a cwd-derived name gives way to it); m: model; by: who set s; why: reason of waiting/blocked
    return {"h": e.get("h", "cli"), "w": e.get("w") or "?", "wt": bool(e.get("wt")), "s": "working", "sum": "", "p": "", "tail": "",
            "ptr": e.get("ptr") or e.get("cwd") or "", "cwd": e.get("cwd", ""), "t0": e["t"], "t": e["t"], "ver": 0, "src": e.get("src", "hook"),
            "att": e.get("att", ""), "m": e.get("m") or "", "by": "hook", "why": ""}


def apply(st, e, changed):
    """Fold one event into state. Adds the row key to `changed` when a visible field changed."""
    ev, k, t, rows = e.get("ev"), e.get("k"), float(e.get("t") or now()), st["rows"]
    if ev == "seen":
        st["seen"][k] = e["seen"]
        return
    if ev in ("claim", "release"):
        cid = hashlib.sha256((k + "\0" + e["glob"]).encode("utf-8")).hexdigest()[:12]
        if ev == "release":
            c = st["claims"].get(cid)
            if c and c["until"] > t:  # kept as an expired entry so the next delta can say the claims changed
                c["until"] = t
                changed.add("claim:" + cid)
        else:
            old = st["claims"].get(cid)
            live = old and old["until"] > t  # renewing a live claim is not news; claiming again after a release is
            st["claims"][cid] = {"glob": e["glob"], "k": k, "w": e.get("w", "?"), "h": e.get("h", "cli"), "t0": old["t0"] if live else t,
                                 "until": t + float(e.get("ttl") or CLAIM_TTL), "ver": old["ver"] if live else 0}
            if not live:
                changed.add("claim:" + cid)
        return
    r = rows.get(k)
    if ev == "sweep":
        if r is None:  # hooks and notes own their rows; a sweep only fills sessions nothing else reported
            r = rows[k] = _new_row(dict(e, src="sweep"))
            r["s"] = "unconfirmed"
            changed.add(k)
        return
    new = r is None
    if new:
        r = rows[k] = _new_row(e)
        changed.add(k)
    r["t"] = max(r["t"], t)
    if r["src"] == "sweep":
        r["src"] = e.get("src", "hook")
    for c in st["claims"].values():  # activity keeps the owner's live claims alive; an expired one needs a new claim
        if c["k"] == k and c["until"] > t:
            c["until"] = max(c["until"], t + CLAIM_GRACE)

    def put(field, value):
        if value is not None and value != "" and r.get(field) != value:
            r[field] = value
            changed.add(k)

    def turn(state, why="", ask=""):
        old = r["s"]
        r["by"] = "hook"
        if old != state or r.get("why", "") != why or r.get("ask", "") != ask:
            r["s"], r["why"], r["ask"] = state, why, ask
            if not (old in QUIET and state in QUIET):
                changed.add(k)

    def name():
        # a title beats a cwd-derived name; a cwd-derived name only fills an unknown one (desktop titles arrive after the first event)
        if e.get("w") and (e.get("wt") or (not r.get("wt") and r["w"] == "?")):
            put("w", e["w"])
            r["wt"] = r.get("wt") or bool(e.get("wt"))

    if e.get("m"):
        r["m"] = e["m"]  # shown only in the load line, so no gen bump
    if ev == "start":
        name()
        put("cwd", e.get("cwd"))
        # a session boundary, not a turn (Orca); a compaction inside a turn leaves the turn working
        if new or r["s"] in TERMINAL or r["s"] == "unconfirmed":
            turn("idle")
    elif ev == "prompt":
        name()
        # rows recorded before the reminder fix carry the reminder's first line; the next real prompt replaces it
        if not r.get("p") or r["p"].startswith("Linked sessions now"):
            put("p", e.get("p"))
        turn("working")  # a new turn answers whatever the row was waiting for, a note's waiting/blocked included
    elif ev == "beat":
        if e.get("tail"):
            r["tail"] = e["tail"]  # not shown while live, so no gen bump
        # The turn ended. Background work the session will wake for keeps it working. A question the turn ended on stays
        # waiting until the next prompt, and so does a waiting/blocked the agent noted; an answered permission does not.
        if not e.get("bg") and (r["s"] in ("working", "unconfirmed") or (r["s"] == "waiting" and r.get("by") == "hook" and r.get("why") != "decision")):
            turn("idle")
    elif ev == "busy":
        if r["s"] == "waiting" and r.get("by") == "hook":
            turn("working")
    elif ev == "notice":
        if r["s"] not in TERMINAL:
            turn("waiting", e.get("why", ""), e.get("ask", ""))
    elif ev == "fail":
        turn("blocked", e.get("why", ""))
    elif ev == "end":
        put("s", "done")
    elif ev == "note":
        put("sum", e.get("sum"))
        put("ptr", e.get("ptr"))
        if e.get("w"):
            put("w", e["w"])
            r["wt"] = True
        if e.get("s"):
            put("s", e["s"])
            r["by"], r["why"] = "note", ""
    elif ev == "job":
        # A finished attempt closes the row only if it is the attempt the row is tracking (Orca dispatchId rule).
        if e.get("s") in TERMINAL and r.get("att") and e.get("att") and e["att"] != r["att"]:
            return
        if e.get("att") and e.get("s") not in TERMINAL:
            r["att"] = e["att"]
        put("w", e.get("w"))
        put("sum", e.get("sum"))
        put("ptr", e.get("ptr"))
        put("s", e.get("s"))


def prune(st):
    cut = now() - KEEP_S
    for k in [k for k, r in st["rows"].items() if r["t"] < cut]:
        del st["rows"][k]
        st["seen"].pop(k, None)
    for cid in [c for c, v in st["claims"].items() if v["until"] < now() - DONE_SHOW_S]:
        del st["claims"][cid]
    st["prunedAt"] = now()


def fold(root=None, wait=LOCK_WAIT_S, extra=None):
    """Apply queued events (and `extra`, a list of events applied in the same pass). Returns the new state.

    Without the lock (another fold is running) the state is returned as last saved and the queue stays for the next fold."""
    root = root or data_dir()
    os.makedirs(root, exist_ok=True)
    if not pending(root) and not extra and not unmigrated(root):
        return load(root)
    try:
        with Locked(root, wait):
            st, events, receipt = load(root), [], None
            old = unmigrated(root)
            if old:
                # The old journals rebuild the table; the old files stay as they were. A higher epoch sends every session the
                # whole table once, since gen numbers go on from the highest old one.
                hist, gen, epoch = history(old)
                st.update(gen=gen, epoch=epoch + 1)
                events += [(None, e) for e in hist if e.get("ev") != "seen"]
                receipt = {"at": now(), "sources": old, "events": len(events), "originalsPreserved": True}
            for p in pending(root):
                try:
                    events.append((p, json.loads(read_text(p))))
                except FileNotFoundError:
                    continue
                except ValueError:
                    _retry(lambda: os.replace(p, p + ".bad"))
            if receipt:  # an old queue can hold events older than the other table's journal: replay both in time order
                events.sort(key=lambda pe: float(pe[1].get("t") or 0))
            events += [(None, dict(e, t=e.get("t") or now())) for e in (extra or [])]
            changed = set()
            for _, e in events:
                apply(st, e, changed)
            if changed:
                st["gen"] += 1
                for k in changed:
                    item = st["claims"].get(k[6:]) if k.startswith("claim:") else st["rows"].get(k)
                    if item:  # a released claim is gone; the gen bump still tells readers the claims line changed
                        item["ver"] = st["gen"]
            if now() - st.get("prunedAt", 0) > 3600:
                prune(st)
            journal = os.path.join(root, "inbox.jsonl")
            if os.path.exists(journal) and os.path.getsize(journal) > JOURNAL_MAX:
                os.replace(journal, os.path.join(root, "inbox-%s.jsonl" % time.strftime("%Y%m%d%H%M%S")))
            with open(journal, "a", encoding="utf-8", newline="\n") as f:
                for _, e in events:
                    if e.get("ev") != "seen":
                        f.write(json.dumps(e, ensure_ascii=False) + "\n")
            write_atomic(os.path.join(root, "state.json"), json.dumps(st, ensure_ascii=False))
            write_atomic(os.path.join(root, "table.md"), render(st) + "\n")
            if receipt:  # after state.json, so a failed merge is tried again
                write_atomic(os.path.join(root, "migration.json"), json.dumps(receipt, ensure_ascii=False))
            for p, _ in events:
                if p:
                    try:
                        os.remove(p)
                    except FileNotFoundError:
                        pass
            return st
    except TimeoutError:
        for e in extra or []:  # queue them so the next fold applies them; the caller sees the last saved state
            emit(e, root)
        return load(root)


# ---------- rendering ----------
def shown_state(r, t):
    # Only a claim of activity goes stale: idle, waiting and blocked were the last thing a hook or note confirmed.
    if r["s"] in ("working", "planned") and t - r["t"] > LIVE_S:
        return "unconfirmed"
    return r["s"]


def age(sec):
    sec = max(0, int(sec))
    return "%ds" % sec if sec < 60 else "%dm" % (sec // 60) if sec < 3600 else "%dh" % (sec // 3600) if sec < 172800 else "%dd" % (sec // 86400)


def short_ids(st):
    keys, out = sorted(st["rows"]), {}
    for k in keys:
        n = 4
        while any(o != k and o[:n] == k[:n] for o in keys):
            n += 1
        out[k] = k[:n]
    return out


def ordered(st, own=None, everything=False):
    t, rows = now(), st["rows"]
    mine = [k for k in rows if k == own]
    live = sorted((k for k in rows if k != own and shown_state(rows[k], t) not in TERMINAL and (everything or t - rows[k]["t"] < DONE_SHOW_S)),
                  key=lambda k: (RANK.get(shown_state(rows[k], t), 3), -rows[k]["t"]))  # rows that need someone come first (Corgi F1)
    ended = sorted((k for k in rows if k != own and shown_state(rows[k], t) in TERMINAL and (everything or t - rows[k]["t"] < DONE_SHOW_S)),
                   key=lambda k: -rows[k]["t"])
    return mine + live + ended


def collapse_sweeps(st, keys):
    """Sessions only the Hub's log sweep found (no hooks): one line per worker, the others counted on it.

    A burst of hook-less runs (a bench made 13 in one sweep, 2026-10-26) would otherwise fill the table."""
    out, first = [], {}
    for k in keys:
        r = st["rows"][k]
        if r.get("src") == "sweep":
            w = r["w"] + "@" + r["h"]
            if w in first:
                first[w][1] += 1
                continue
            first[w] = [k, 0]
        out.append(k)
    return out, {k: n for k, n in first.values() if n}


def row_line(st, k, ids, t, more=0):
    r = st["rows"][k]
    s = shown_state(r, t)
    # A finished row is best described by its last answer, a live one by what it was asked to do.
    order = ("sum", "tail", "p") if s in TERMINAL else ("ask", "sum", "p", "tail") if s == "waiting" else ("sum", "p", "tail")
    summary = next((r[f] for f in order if r.get(f)), "")
    if more:
        summary = (summary + " " if summary else "") + "(+%d more without hooks)" % more
    if s in ("waiting", "blocked") and r.get("why"):
        s += ":" + r["why"]
    return "|".join([ids[k], str(r["ver"]), s, "%s@%s" % (r["w"], r["h"]), age(t - r["t"]), summary, r["ptr"]])


def load_line(st, t):
    """Sessions mid-turn per harness and model, and how many wait on someone (C3: concurrent Opus turns drain the Claude 5-hour window)."""
    busy, other = {}, {}
    for r in st["rows"].values():
        s = shown_state(r, t)
        if s == "working":
            m = busy.setdefault(r["h"], {})
            m[r.get("m") or "?"] = m.get(r.get("m") or "?", 0) + 1
        elif s in ("idle", "waiting", "blocked") and t - r["t"] < DONE_SHOW_S:
            other[s] = other.get(s, 0) + 1
    parts = []
    for h in sorted(busy, key=lambda h: -sum(busy[h].values())):
        models = busy[h]
        detail = "" if h == "hub" or list(models) == ["?"] else " (%s)" % ", ".join("%s %d" % (m, n) for m, n in sorted(models.items(), key=lambda x: -x[1]))
        parts.append("%s %d%s" % (h, sum(models.values()), detail))
    tail = ", ".join("%s %d" % (s, other[s]) for s in ("waiting", "blocked", "idle") if other.get(s))
    return "now: working " + (", ".join(parts) or "none") + ("; " + tail if tail else "")


def live_claims(st, t):
    return {cid: c for cid, c in st["claims"].items() if c["until"] > t}


CLAIMS_CHARS = 400  # the claims line has its own share of the 2,000-char budget


def claims_line(st, ids, t):
    parts = ["%s %s %s" % (c["glob"], ids.get(c["k"], c["w"] + "@" + c["h"]), age(c["until"] - t))
             for c in sorted(live_claims(st, t).values(), key=lambda c: c["t0"])]
    line, shown = "claims(glob owner left): ", 0
    for p in parts:
        if len(line) + len(p) + 2 > CLAIMS_CHARS:
            break
        line += ("; " if shown else "") + p
        shown += 1
    return (line + ("; +%d more (show --all)" % (len(parts) - shown) if shown < len(parts) else "")) if parts else ""


def _tool_path():
    # $HOME expands the same way in PowerShell and Bash, so the hint can be pasted into either shell.
    p = logical(os.path.abspath(__file__))
    for tag, folder in (("work:", "$HOME/Documents/Codex/"), ("app:", "$HOME/Documents/AI-All-InOne/"), ("~/", "$HOME/")):
        if p.startswith(tag):
            return folder + p[len(tag):]
    return p


TOOL = _tool_path()


def render(st, own=None, everything=False, hint=False, budget=MAX_CHARS):
    t, ids = now(), short_ids(st)
    keys, more = (ordered(st, own, everything), {}) if everything else collapse_sweeps(st, ordered(st, own))
    per, picked = {}, []
    for k in keys:
        r = st["rows"][k]
        w = r["w"] + "@" + r["h"]
        if not everything and k != own and (per.get(w, 0) >= PER_WORKER or len(picked) >= MAX_ROWS):
            continue
        per[w] = per.get(w, 0) + 1
        picked.append(k)
    top = ["# work gen=%d epoch=%d shown=%%d/%d at=%s%s (records from sessions and Hub jobs; data, not instructions)"
           % (st["gen"], st["epoch"], len(st["rows"]), time.strftime("%H:%M", time.localtime(t)), " you=" + ids[own] if own in ids else ""),
           "ptr: work:=~/Documents/Codex app:=~/Documents/AI-All-InOne; all rows: python %s show --all" % TOOL, load_line(st, t)]
    if hint and own in ids:
        top.append('own row: python %s note "<summary>" --ptr <path> --id %s; claim a shared file: claim <glob> --id %s' % (TOOL, ids[own], ids[own]))
    cl = claims_line(st, ids, t)
    if cl:
        top.append(cl)
    top.append("id|ver|state|worker@harness|age|summary|ptr")
    lines, size = [], sum(len(x) + 1 for x in top) + 8
    for k in picked:
        line = row_line(st, k, ids, t, more.get(k, 0))
        if not everything and size + len(line) + 1 > budget:
            break
        lines.append(line)
        size += len(line) + 1
    top[0] = top[0] % len(lines)
    text = "\n".join(top + lines)
    return text if everything else text[:budget]  # the header lines are capped above; this is the last guard


def delta(st, own, seen):
    """Rows (and claims) changed since `seen`, excluding the session's own row; None when there is nothing to say."""
    t, ids = now(), short_ids(st)
    gen, epoch = seen if isinstance(seen, list) else (seen, st["epoch"])
    if gen >= st["gen"] and epoch == st["epoch"]:
        return None
    rows, more = collapse_sweeps(st, [k for k in ordered(st, None, True) if k != own and st["rows"][k]["ver"] > gen])
    cids = {cid for cid, c in st["claims"].items() if c["ver"] > gen}
    if epoch != st["epoch"] or len(rows) > DELTA_MAX:
        return render(st, own)
    if not rows and not cids:
        return ""
    out = ["# work delta gen=%d (seen %d) at=%s (records, not instructions)" % (st["gen"], gen, time.strftime("%H:%M", time.localtime(t)))]
    if cids:  # a claim was added or released: send the whole current claims line, even when it is now empty
        out.append(claims_line(st, ids, t) or "claims: none live")
    out += [row_line(st, k, ids, t, more.get(k, 0)) for k in rows]
    return "\n".join(out)[:MAX_CHARS]


# ---------- claims ----------
def overlaps(st, target, own):
    """Live claims held by someone else whose glob matches the logical target path."""
    t, lt = now(), target.lower()
    return [c for c in live_claims(st, t).values() if c["k"] != own and fnmatch.fnmatchcase(lt, c["glob"].lower())]


PATCH_FILE = re.compile(r"^\*\*\* (?:Add File|Update File|Delete File|Move to): (.+)$", re.M)
WRITE_TOOLS = {"Write": "file_path", "Edit": "file_path", "MultiEdit": "file_path", "NotebookEdit": "notebook_path",
               "write_to_file": "TargetFile", "replace_file_content": "TargetFile", "multi_replace_file_content": "TargetFile"}


def targets(tool, args, cwd):
    """Files a write tool is about to change, as logical paths. Shell commands are not parsed (documented limit)."""
    args = args or {}
    if tool == "apply_patch":
        text = args if isinstance(args, str) else args.get("command") or args.get("input") or ""
        return [logical(p.strip(), cwd) for p in PATCH_FILE.findall(str(text))]
    field = WRITE_TOOLS.get(tool)
    return [logical(args[field], cwd)] if field and isinstance(args, dict) and args.get(field) else []


def asked(tool, args):
    """The question a tool call puts to the user, or None when the call asks nothing."""
    args = args if isinstance(args, dict) else {}
    if tool == "AskUserQuestion":
        qs = args.get("questions") or [{}]
        return clean((qs[0] if isinstance(qs[0], dict) else {}).get("question") or "question")
    if tool == WRAP_UP and args.get("ask"):
        return clean(args["ask"])
    if tool == "request_user_input":  # Codex (Orca codex-events)
        return clean(args.get("question") or args.get("prompt") or "question")
    return None


# ---------- hooks ----------
def hook_input(harness, d):
    if harness == "agy":
        tc = d.get("toolCall") or {}
        # lastUserInput: agy 1.4.7 adds the latest user message to hook input (CHANGELOG 1.4.7)
        return {"sid": d.get("conversationId"), "cwd": (d.get("workspacePaths") or [""])[0], "tool": tc.get("name"), "args": tc.get("args"),
                "prompt": d.get("lastUserInput")}
    # model: Claude SessionStart (optional) and PostModelSwitch `to_model`, Codex on every event
    return {"sid": d.get("session_id"), "cwd": d.get("cwd"), "tool": d.get("tool_name"), "args": d.get("tool_input"),
            "title": d.get("session_title"), "prompt": d.get("prompt"), "last": d.get("last_assistant_message"),
            "model": short_model(d.get("to_model") or d.get("model")), "bg": len(d.get("background_tasks") or [])}


def short_model(m):
    return clean(re.sub(r"\[.*?\]$", "", re.sub(r"^claude-", "", str(m))), 20) if m else ""


def worker_name(title, cwd):
    if title:
        return clean(title, 20)
    lc = logical(cwd) if cwd else ""
    parts = [p for p in re.split(r"[:/]", lc) if p and p not in ("work", "hub", "~")]
    return clean(parts[0] if parts else (lc or "?"), 24)


def context_out(harness, event, text):
    if harness == "agy":
        return {"injectSteps": [{"systemMessage": {"systemMessage": text}}]}
    return {"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}}


def queue_notice(arguments):
    task_worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'todo_worker.py')
    if os.path.isfile(task_worker):
        try:
            subprocess.Popen([sys.executable, task_worker] + arguments, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        except OSError:
            pass


def run_hook(harness, event, d, root=None):
    """Handle one hook call; returns the JSON object to print, or None to print nothing."""
    root = root or data_dir()
    x = hook_input(harness, d)
    if not x["sid"]:
        return {"decision": "allow"} if harness == "agy" and event == "PreToolUse" else None
    k = key_of(harness, x["sid"])
    cwd = logical(x["cwd"]) if x["cwd"] else ""
    base = {"k": k, "h": harness, "cwd": cwd, "src": "hook"}
    if x.get("model"):
        base["m"] = x["model"]
    if event in ("SessionStart", "UserPromptSubmit"):
        queue_notice(['owner-active', '--owner', x['sid'], '--at', str(now())])
    elif event == "SessionEnd" or (event == "Stop" and not x.get("bg")):
        # A Stop with background tasks pauses the session until they wake it; it has not stopped working (Claude hooks doc).
        queue_notice(['owner-finished', '--owner', x['sid'], '--at', str(now()), '--confirmed-stopped'])
    if event == "SessionStart":
        # Launch a bounded wakeup; this process never interprets queue task text or waits for the model.
        queue_notice(['tick'])
        st = fold(root, extra=[dict(base, ev="start", t=now(), w=worker_name(x.get("title"), x["cwd"]), wt=bool(x.get("title")))])
        seen = st["seen"].get(k)
        # A resumed conversation still holds the table it was given; compaction or a new session does not.
        text = delta(st, k, seen) if d.get("source") == "resume" and seen else render(st, own=k, hint=True)
        if not text:
            return None
        emit({"ev": "seen", "k": k, "seen": [st["gen"], st["epoch"]]}, root)
        return context_out(harness, event, text)
    if event in ("UserPromptSubmit", "PreInvocation"):
        st = load(root)
        r = st["rows"].get(k)
        # PreInvocation runs before every model call; within a live turn once a minute is enough, but the first call after a Stop starts a turn
        quiet = harness == "agy" and r and r["s"] == "working" and now() - r["t"] < 60 and not pending(root)
        if not quiet:
            if event == 'PreInvocation':
                queue_notice(['owner-active', '--owner', x['sid'], '--at', str(now())])
            p = head(x.get("prompt")) if x.get("prompt") else ""
            w = worker_name(x.get("title"), x["cwd"]) if x.get("title") or not r or r["w"] == "?" else None
            st = fold(root, extra=[dict(base, ev="prompt", t=now(), p=p, **({"w": w, "wt": bool(x.get("title"))} if w else {}))])
        seen = st["seen"].get(k, [0, st["epoch"]])
        text = delta(st, k, seen)
        if text is None:
            return None
        emit({"ev": "seen", "k": k, "seen": [st["gen"], st["epoch"]]}, root)
        return context_out(harness, event, text) if text else None
    if event == "PreToolUse":
        q = asked(x["tool"], x["args"])
        if q is not None:  # the session is about to wait on the user (Orca counts AskUserQuestion as waiting)
            emit(dict(base, ev="notice", why="decision" if x["tool"] == WRAP_UP else "question", ask=q), root)
            return {"decision": "allow"} if harness == "agy" else None
        st = fold(root, wait=0.3)
        r = st["rows"].get(k)
        if r and r["s"] == "waiting" and r.get("by") == "hook":
            emit(dict(base, ev="busy"), root)  # a write means the question or permission was answered
        hits = []
        for target in targets(x["tool"], x["args"], x["cwd"]):
            ids = short_ids(st)
            for c in overlaps(st, target, k):
                hits.append("work table: %s matches claim %s held by %s@%s (row %s, %s left). The edit is not blocked; the owner may be editing it."
                            % (target, c["glob"], c["w"], c["h"], ids.get(c["k"], "?"), age(c["until"] - now())))
        msg = " ".join(hits)[:1000]
        if harness == "agy":
            return dict({"decision": "allow"}, **({"reason": msg} if msg else {}))
        return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": msg}} if msg else None
    if event in ("Stop", "Interrupt"):
        emit(dict(base, ev="beat", tail=head(x.get("last")) if x.get("last") else "", bg=x.get("bg", 0)), root)
        return {"decision": "stop"} if harness == "agy" else None  # Codex Stop rejects plain text; Claude Stop context would continue the turn
    if event == "StopFailure":  # Claude: the turn ended on an API error such as rate_limit (no Stop runs then)
        emit(dict(base, ev="fail", why=clean(d.get("error") or "unknown", 24)), root)
        return None
    if event in ("Notification", "PermissionRequest"):  # Claude Notification types / Codex approval prompt; the output is ignored
        why = "permission" if event == "PermissionRequest" else NEEDS_YOU.get(d.get("notification_type"))
        if why:
            emit(dict(base, ev="notice", why=why), root)
        return None
    if event == "PostModelSwitch":
        emit(dict(base, ev="model"), root)  # base carries m = to_model
        return None
    if event == "SessionEnd":
        emit(dict(base, ev="end"), root)  # 1-1.5 s budget: queue only, the next reader folds
        return None
    return None


# ---------- command line ----------
def own_key(args):
    """Row key for note/claim: --id prefix, else this harness session from the environment."""
    root = args.dir
    if getattr(args, "id", None):
        st = fold(root)
        hits = [k for k in st["rows"] if k.startswith(args.id.lower())]
        if len(hits) != 1:
            sys.exit("work_table: --id %s matches %d rows" % (args.id, len(hits)))
        return hits[0], st["rows"][hits[0]]["h"]
    for env, harness in (("CLAUDE_CODE_SESSION_ID", "claude"), ("CODEX_THREAD_ID", "codex")):
        if os.environ.get(env):
            return key_of(harness, os.environ[env]), harness
    return key_of("cli", args.worker or "cli"), "cli"


def parse_ttl(s):
    m = re.fullmatch(r"(\d+)([smh]?)", str(s))
    if not m:
        raise argparse.ArgumentTypeError("ttl like 30m, 2h or 900")
    return int(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[m.group(2)]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dir", default=None, help="data folder (default ~/.agent-work or AGENT_WORK_DIR)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("show", help="print the table (fresh)")
    s.add_argument("--all", action="store_true")
    s.add_argument("--json", action="store_true")
    n = sub.add_parser("note", help="set the summary/ptr/state of your row")
    n.add_argument("summary")
    n.add_argument("--ptr")
    n.add_argument("--state", choices=STATES)
    n.add_argument("--id")
    n.add_argument("--worker")
    c = sub.add_parser("claim", help="advisory claim on a shared path glob (warns other sessions, never blocks)")
    c.add_argument("glob", nargs="+")
    c.add_argument("--ttl", type=parse_ttl, default=CLAIM_TTL)
    c.add_argument("--release", action="store_true")
    c.add_argument("--id")
    c.add_argument("--worker")
    h = sub.add_parser("hook", help="run as a harness hook: JSON on stdin, harness-shaped JSON on stdout")
    h.add_argument("harness", choices=("claude", "codex", "agy"))
    h.add_argument("event")
    sub.add_parser("fold", help="apply queued events now")
    sub.add_parser("path", help="print the data folder")
    args = ap.parse_args(argv)
    args.dir = args.dir or data_dir()
    if args.cmd == "hook":
        try:
            d = json.loads(sys.stdin.buffer.read().decode("utf-8", "replace") or "{}")
            out = run_hook(args.harness, args.event, d, args.dir)
        except Exception as e:  # a hook must never break the session; the error goes to stderr (debug logs)
            print("work_table hook error: %s" % e, file=sys.stderr)
            out = {"decision": "allow"} if args.harness == "agy" and args.event == "PreToolUse" else None
        if out is not None:
            sys.stdout.write(json.dumps(out, ensure_ascii=True))
        return 0
    if args.cmd == "path":
        print(args.dir)
    elif args.cmd == "fold":
        st = fold(args.dir)
        print("gen=%d rows=%d claims=%d" % (st["gen"], len(st["rows"]), len(st["claims"])))
    elif args.cmd == "show":
        st = fold(args.dir)
        print(json.dumps(st, ensure_ascii=False, indent=1) if args.json else render(st, everything=args.all))
    elif args.cmd == "note":
        k, harness = own_key(args)
        fold(args.dir, extra=[{"ev": "note", "k": k, "h": harness, "src": "note", "t": now(), "w": clean(args.worker, 20) if args.worker else None,
                               "sum": clean(args.summary), "ptr": logical(args.ptr, os.getcwd()) if args.ptr else None, "s": args.state,
                               "cwd": logical(os.getcwd())}])
        print(render(fold(args.dir), own=k))
    elif args.cmd == "claim":
        k, harness = own_key(args)
        st = fold(args.dir)
        r = st["rows"].get(k, {})
        events = [{"ev": "release" if args.release else "claim", "k": k, "h": harness, "w": r.get("w") or clean(args.worker or "cli", 20),
                   "glob": logical(g, os.getcwd()), "ttl": args.ttl, "t": now()} for g in args.glob]
        st = fold(args.dir, extra=events)
        print(claims_line(st, short_ids(st), now()) or "no live claims")
    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):  # same as find_context.py: a piped Windows stdout is otherwise the console code page
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())


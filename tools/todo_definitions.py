"""Git-mergeable queue definitions, with an indexed, checkout-local SQLite projection."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

from todo_requires import requirements

ROOT = Path('todo-queue/definitions')
SEED = 'bootstrap.jsonl'
ZERO = '0' * 64
MACHINE = re.compile(r'[A-Z0-9][A-Z0-9_-]{0,63}\Z')
TASK = re.compile(r'[A-Z0-9][A-Z0-9_.-]{2,79}\Z')
SOURCE = ROOT.as_posix() + '/*.jsonl'


def packed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(',', ':')).encode('utf-8')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def enabled(repo):
    folder = repo / ROOT
    if (folder / SEED).is_file():
        return True
    if (folder.is_dir() and any(folder.glob('*.jsonl'))
            or any((repo/'.local/todo-queue').glob('*/definitions.sqlite3'))):
        raise ValueError('Queue ledger is missing its bootstrap file')
    return False


def db_path(repo, machine):
    return repo / '.local/todo-queue' / machine / 'definitions.sqlite3'


def file_snapshot(repo):
    directory = repo/ROOT
    files = {}
    for path in sorted(directory.glob('*.jsonl')):
        if path.is_symlink() or not path.resolve().is_relative_to(repo) or not path.is_file():
            raise ValueError('Invalid definition log path: '+path.name)
        files[path.name] = path.read_bytes()
    return files


def signature(files):
    return digest(packed({name:digest(data) for name,data in files.items()}))


def atomic_bytes(file, data):
    file.parent.mkdir(parents=True, exist_ok=True)
    tmp = file.with_name(file.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with tmp.open('xb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp,file)
    finally:
        tmp.unlink(missing_ok=True)


def envelope(op, by, clock, prev, payload):
    event = dict(schemaVersion=1, id=uuid.uuid4().hex, at=time.time_ns(), by=by,
                 clock=clock, prevHash=prev, op=op, payload=payload)
    event['hash'] = digest(packed(event))
    return event


def to_payload(task):
    return dict(id=task['id'], needs=[s.strip().upper() for s in task['targets'].split(',')],
                targets=task['targets'], requires=requirements(task['summary']), summary=task['summary'],
                status=task['status'], detail=task['detail'], fingerprint=task['fingerprint'])


def to_task(data):
    keys = ('id','targets','summary','status','detail','fingerprint')
    if not isinstance(data,dict) or not all(isinstance(data.get(k),str) for k in keys):
        raise ValueError('Invalid queue definition fields')
    if not TASK.fullmatch(data['id']) or any('|' in data[k] or '\n' in data[k] or '\r' in data[k] for k in keys):
        raise ValueError('Invalid task ID or Markdown cell')
    needs = [s.strip().upper() for s in data['targets'].split(',')]
    fingerprint = digest('\0'.join(data[k] for k in ('id','targets','summary','detail')).encode('utf-8'))
    if (data.get('needs') != needs or not all(MACHINE.fullmatch(pc) for pc in needs)
            or data.get('requires') != requirements(data['summary']) or data['fingerprint'] != fingerprint):
        raise ValueError('Task needs/requires/fingerprint disagrees with its definition')
    task = {k:data[k] for k in keys}
    task['held'] = task['status'].lower().startswith(('blocked','보류'))
    return task


def read_events(files):
    if SEED not in files:
        raise ValueError('Missing seed file')
    events, seen = [], set()
    seed_hash = ZERO
    for name in [SEED, *sorted(n for n in files if n != SEED)]:
        raw = files[name]
        writer = name.removeprefix('pc-').removesuffix('.jsonl')
        if name != SEED and (name != f'pc-{writer}.jsonl' or not MACHINE.fullmatch(writer)):
            raise ValueError('Unexpected definition ledger filename: '+name)
        if not raw or not raw.endswith(b'\n'):
            raise ValueError('Incomplete queue ledger: '+name)
        previous, last_clock = seed_hash, -1
        lines = raw.split(b'\n')[:-1]
        for line in lines:
            event = json.loads(line)
            check = event.pop('hash',None)
            if (event.get('schemaVersion') != 1 or event.get('prevHash') != previous or check != digest(packed(event))
                    or not isinstance(event.get('id'),str) or not re.fullmatch(r'[a-f0-9]{32}',event['id'])
                    or event['id'] in seen or not isinstance(event.get('by'),str) or not MACHINE.fullmatch(event['by'])
                    or type(event.get('clock')) is not int or type(event.get('at')) is not int or event['clock'] <= last_clock):
                raise ValueError('Invalid or broken definition hash chain: '+name)
            if name == SEED:
                if len(lines)!=1 or event['op']!='bootstrap' or event['clock']!=0:
                    raise ValueError('Invalid bootstrap event')
                seed_hash = check
            elif event['by'] != writer or event['op'] not in ('add','remove') or event['clock'] < 1:
                raise ValueError('Unexpected writer or operation in '+name)
            previous, last_clock = check,event['clock']
            event['hash'] = check
            seen.add(event['id'])
            events.append(event)
    return sorted(events,key=lambda e:(e['clock'],e['by'],e['id']))


def fold(events):
    tasks = {}
    for event in events:
        payload = event['payload']
        if event['op']=='remove':
            identity, fingerprint = payload['id'],payload['fingerprint']
            if not TASK.fullmatch(identity) or not re.fullmatch(r'[a-f0-9]{64}',fingerprint):
                raise ValueError('Invalid retirement record')
            current = tasks.get(identity)
            if current and current['fingerprint']==fingerprint:
                del tasks[identity]
            continue
        rows = payload['rows'] if event['op']=='bootstrap' else [payload]
        for raw in rows:
            task = to_task(raw)
            identity = task['id']
            if identity in tasks and (event['op']=='bootstrap' or task!=tasks[identity]):
                raise ValueError('Conflicting definitions for '+identity+'; reconcile the originating changes')
            tasks[identity] = task
    return tasks.values()


def populate(db, files, sig):
    db.execute('BEGIN IMMEDIATE')
    db.execute('CREATE TABLE IF NOT EXISTS metadata(signature TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, ordinal INTEGER UNIQUE, status TEXT, body TEXT)')
    db.execute('CREATE INDEX IF NOT EXISTS task_status ON tasks(status)')
    for name in ('needs','requires'):
        db.execute(f'CREATE TABLE IF NOT EXISTS {name}(value TEXT, task_id TEXT, PRIMARY KEY(value,task_id))')
        db.execute('DELETE FROM '+name)
    db.execute('DELETE FROM tasks')
    for ordinal,task in enumerate(fold(read_events(files))):
        db.execute('INSERT INTO tasks VALUES(?,?,?,?)',(task['id'],ordinal,task['status'],packed(task).decode()))
        data = to_payload(task)
        for name in ('needs','requires'):
            db.executemany(f'INSERT INTO {name} VALUES(?,?)',[(v,task['id']) for v in set(data[name] or [])])
    db.execute('DELETE FROM metadata')
    db.execute('INSERT INTO metadata VALUES(?)',(sig,))
    db.commit()


def query(db, target):
    sql = 'SELECT body FROM tasks'
    params = ()
    if target:
        sql += ' JOIN needs ON tasks.id=needs.task_id WHERE needs.value=?'
        params = (target,)
    return [json.loads(row[0]) for row in db.execute(sql+' ORDER BY ordinal',params)]


def read(repo, local, target=None):
    files = file_snapshot(repo)
    sig, path = signature(files), db_path(repo,local)
    if path.is_file():
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
            db.execute('BEGIN')
            initialized = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'").fetchone()
            if initialized and db.execute('SELECT signature FROM metadata').fetchone()==(sig,):
                return query(db,target)
    # show and foreign-PC previews also work immediately after pull without mutating queue state.
    with closing(sqlite3.connect(':memory:')) as db:
        populate(db,files,sig)
        return query(db,target)


def projection_journal(version):
    # SQLite backported the WAL-reset fix to two older release families.
    major, minor, patch = version
    fixed = (version >= (3, 51, 3) or
             ((major, minor) == (3, 50) and patch >= 7) or
             ((major, minor) == (3, 44) and patch >= 6))
    return 'WAL' if fixed else 'DELETE'


def sync(repo, machine):
    if not enabled(repo):
        raise ValueError('Migrate shared definitions first')
    files = file_snapshot(repo)
    path = db_path(repo,machine)
    path.parent.mkdir(parents=True,exist_ok=True)
    with closing(sqlite3.connect(path)) as db:
        requested = projection_journal(sqlite3.sqlite_version_info)
        current = db.execute('PRAGMA journal_mode').fetchone()[0].upper()
        if current != requested:
            current = db.execute('PRAGMA journal_mode=' + requested).fetchone()[0].upper()
            if current != requested:
                raise ValueError('Unable to select the local SQLite journal; retry sync when readers finish')
        sig = signature(files)
        initialized = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'").fetchone()
        if not initialized or db.execute('SELECT signature FROM metadata').fetchone() != (sig,):
            populate(db, files, sig)
        return dict(definitions=db.execute('SELECT count(*) FROM tasks').fetchone()[0],
                    source=SOURCE, localView='SQLite ' + current)


def migrate(repo, machine, tasks, before):
    payload = dict(sourceSha256=digest(before),rows=[to_payload(task) for task in tasks])
    for row in payload['rows']:
        to_task(row)
    seed = repo/ROOT/SEED
    if (repo/'TODO.md').read_bytes()!=before:
        raise ValueError('Markdown changed during migration')
    atomic_bytes(seed,packed(envelope('bootstrap',machine,0,ZERO,payload))+b'\n')


def append(repo, machine, op, payload):
    files = file_snapshot(repo)
    events = read_events(files)
    name = 'pc-'+machine+'.jsonl'
    previous = next((e['hash'] for e in reversed(events) if e['by']==machine and e['op']!='bootstrap'),events[0]['hash'])
    event = envelope(op,machine,events[-1]['clock']+1,previous,payload)
    new = files.get(name,b'')+packed(event)+b'\n'
    fold(read_events({**files,name:new}))
    # All callers hold the existing per-machine OS lock. Atomic replacement preserves the full prior prefix on crash.
    if file_snapshot(repo)!=files:
        raise ValueError('Definitions changed; retry after synchronization')
    atomic_bytes(repo/ROOT/name,new)


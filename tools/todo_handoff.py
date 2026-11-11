"""Content-addressed, opt-in context for the shared PC queue."""
import argparse
from contextlib import contextmanager, nullcontext
import hashlib
import io
import json
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import subprocess
import sys
import tempfile
import time

from work_table import Locked


HEX = re.compile(r'[0-9a-f]{64}\Z')
TASK = re.compile(r'[A-Z0-9][A-Z0-9_.-]{2,79}\Z')
MACHINE = re.compile(r'[A-Z0-9][A-Z0-9_-]{0,63}\Z')
MAX_RECORDS, MAX_DOCUMENT, MAX_EVIDENCE = 256, 32768, 2 * 1024 * 1024


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def phrase(value, limit):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or not value.isprintable():
        raise ValueError('Expected nonempty, bounded, single-line handoff text')
    return value.strip()


def phrases(items, count, width):
    if not isinstance(items, (list, tuple)) or len(items) > count:
        raise ValueError('Handoff list exceeds its limit')
    return list(dict.fromkeys(phrase(item, width) for item in items))


def regular(path, size):
    with path.open('rb') as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > size:
            raise ValueError('Expected a bounded regular file')
        data = file.read(size + 1)
    if len(data) > size:
        raise ValueError('File grew beyond its limit')
    return data


def beneath(root, relative):
    if not isinstance(relative, str) or not relative or len(relative) > 320:
        raise ValueError('Invalid relative path')
    parts = relative.split('/')
    win = PureWindowsPath(relative)
    if win.drive or win.root or any(p in ('', '.', '..') or p.endswith((' ', '.')) or
            re.search(r'[<>:"\\|?*\x00-\x1f]', p) or
            re.fullmatch(r'(?i:CON|CONIN\$|CONOUT\$|PRN|AUX|NUL|(?:COM|LPT)[0-9¹²³])',p.split('.')[0].rstrip()) for p in parts):
        raise ValueError('Unsafe relative path')
    path = root
    for part in parts:
        path = path / part
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 1024:
            raise ValueError('Linked handoff or evidence path')
    return path


def dirs(repo, machine, task):
    repo = Path(repo).resolve(strict=True)
    if not MACHINE.fullmatch(machine) or not TASK.fullmatch(task['id']) or not HEX.fullmatch(task['fingerprint']):
        raise ValueError('Invalid machine, task or queue definition')
    suffix = task['id'] + '/' + task['fingerprint']
    local = '.local/todo-queue/' + machine
    return repo, {key: beneath(repo, value) for key, value in {
        'local': local + '/handoffs/' + suffix, 'shared': 'todo-queue/handoffs/' + suffix,
        'acks': local + '/handoff-acks/' + suffix, 'lock': '.local/handoff-locks/' + suffix,
    }.items()}


def atom(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.handoff-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def evidence_files(repo, paths, shared):
    if not paths:
        return []
    local=[]
    for path in paths:
        dest = beneath(repo, path)
        parts = [part.lower() for part in path.split('/')]
        if any((p.startswith('.') and not (not shared and i == 0 and p == '.local')) or
               p in ('credentials', 'secrets', 'sessions', 'memories', 'memory-proposals') or
               p.endswith(('.pem', '.pfx', '.key', '.kdbx')) for i,p in enumerate(parts)):
            raise ValueError('Evidence path is private or reserved')
        if not shared:
            data = regular(dest, MAX_EVIDENCE)
            local.append({'path':path,'sha256':digest(data),'bytes':len(data)})
        else:
            info=dest.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_size>MAX_EVIDENCE:
                raise ValueError('Expected a bounded regular file')
    if not shared:
        return local
    git=['git','--no-optional-locks','-C',str(repo)]
    specs=[':(literal)'+p for p in paths]
    staged = subprocess.check_output([*git,'ls-files','-v','--stage','-z','--',*specs])
    blobs={}
    for row in staged.split(b'\0')[:-1]:
        match=re.fullmatch(rb'H 100(?:644|755) ([0-9a-f]{40,64}) 0\t(.+)',row)
        if not match:
            raise ValueError('Shared evidence must be a normally tracked working file')
        blobs[match[2].decode('utf8')]=match[1]
    if set(blobs)!=set(paths):
        raise ValueError('Shared evidence must be a tracked regular repository file')
    # Check the index against HEAD and independently clean/hash actual working bytes. This also
    # rejects a staged edit subsequently hidden by resetting just the worktree bytes or timestamps.
    if subprocess.run([*git,'diff','--cached','--quiet','HEAD','--',*specs]).returncode:
        raise ValueError('Shared evidence must match HEAD')
    names=''.join(p+'\n' for p in paths).encode('utf8')
    observed=subprocess.check_output([*git,'hash-object','--stdin-paths'],input=names).splitlines()
    wanted=[blobs[p] for p in paths]
    if observed!=wanted:
        raise ValueError('Shared evidence must match HEAD')
    request=b'\n'.join(wanted)+b'\n'
    sizes=subprocess.check_output([*git,'cat-file','--batch-check=%(objecttype) %(objectsize)'],input=request).splitlines()
    if len(sizes)!=len(paths) or any(not re.fullmatch(rb'blob [0-9]+',s) or int(s[5:])>MAX_EVIDENCE for s in sizes):
        raise ValueError('Shared evidence blob exceeds its limit')
    stream=io.BytesIO(subprocess.check_output([*git,'cat-file','--batch'],input=request))
    result=[]
    for path,blob,line in zip(paths,wanted,sizes):
        count=int(line[5:])
        if stream.readline()!=blob+b' '+line+b'\n':
            raise ValueError('Unexpected Git blob header')
        data=stream.read(count)
        if len(data)!=count or stream.read(1)!=b'\n':
            raise ValueError('Incomplete Git blob')
        result.append({'path':path,'sha256':digest(data),'bytes':len(data)})
    return result


def evidence_file(repo, path, shared):
    return evidence_files(repo,[path],shared)[0]


def history(folder, task, audience):
    records = {}
    if not folder.exists():
        return records
    for path in folder.iterdir():
        if path.name.startswith('.handoff-') and path.suffix == '.tmp':
            continue
        if len(records) >= MAX_RECORDS or path.suffix != '.json' or not HEX.fullmatch(path.stem):
            raise ValueError('Invalid handoff history; retain files for review')
        data = json.loads(regular(beneath(folder, path.name), MAX_DOCUMENT))
        fields = {'schemaVersion','id','definition','machine','audience','actor','parent','summary','next','decisions','open','evidence'}
        if not isinstance(data, dict) or set(data) not in (fields,fields|{'mergeParents'}) or data.get('schemaVersion') != 1 or data.get('id') != task['id'] or (
                data.get('definition') != task['fingerprint'] or data.get('audience') != audience or
                digest(encoded(data)) != path.stem):
            raise ValueError('Handoff identity or revision fingerprint mismatch')
        if (not MACHINE.fullmatch(data['machine']) or
                not (data['parent'] == 'none' or HEX.fullmatch(data['parent'])) or
                phrase(data['summary'], 400) != data['summary'] or phrase(data['next'], 320) != data['next'] or
                phrase(data['actor'], 100) != data['actor'] or phrases(data['decisions'], 12, 160) != data['decisions'] or
                phrases(data['open'], 8, 160) != data['open'] or not isinstance(data['evidence'], list) or len(data['evidence']) > 8):
            raise ValueError('Invalid handoff record fields')
        extra = data.get('mergeParents', [])
        if not isinstance(extra,list) or len(extra)>7 or len(set(extra))!=len(extra) or any(not HEX.fullmatch(r) for r in extra) or data['parent'] in extra:
            raise ValueError('Invalid handoff merge parents')
        for ref in data['evidence']:
            if set(ref) != {'path','bytes','sha256'} or not HEX.fullmatch(ref['sha256']) or type(ref['bytes']) != int or not 0 <= ref['bytes'] <= MAX_EVIDENCE:
                raise ValueError('Invalid handoff evidence metadata')
            beneath(folder, ref['path'])  # lexical/reparse validation, never open a referenced file on read
        records[path.stem] = data
    for rev, data in records.items():
        for parent in [data['parent'], *data.get('mergeParents',[])]:
            if parent != 'none' and (parent not in records or not set(records[parent]['decisions']) <= set(data['decisions'])):
                raise ValueError('Handoff parent or inherited decision missing')
    return records


def leaves(records):
    return set(records) - {parent for r in records.values() for parent in [r['parent'], *r.get('mergeParents',[])]}


def tip(records):
    tips = leaves(records)
    if len(tips) > 1:
        raise ValueError('Divergent handoff revisions: ' + ','.join(sorted(tips)[:8]))
    return next(iter(tips),None)


def view(records, revision, locations, actor):
    if not revision:
        return None
    record = records[revision]
    actor_key = digest(phrase(actor,100).encode()) if actor else None
    ack = beneath(locations['acks'], record['audience']+'/'+revision+'/'+actor_key+'.json') if actor_key else None
    acknowledged = False
    if ack and ack.exists():
        stamp = json.loads(regular(ack, 2048))
        acknowledged = stamp.get('revision') == revision and stamp.get('actor') == actor
    return {**record, 'revision': revision, 'acknowledged': acknowledged}


def read_handoff(repo, machine, task, *, actor=None, audience='both', revision=None, verify_evidence=False):
    repo, locations = dirs(repo, machine, task)
    if audience not in ('local','shared','both') or revision is not None and not HEX.fullmatch(revision):
        raise ValueError('Invalid audience or revision')
    results = []
    for lane in ('local','shared') if audience=='both' else (audience,):
        records = history(locations[lane],task,lane)
        selected = revision if revision in records else None if revision else tip(records)
        if selected:
            result = view(records, selected, locations, actor)
            if verify_evidence:
                checks = []
                for item in result['evidence']:
                    try:
                        observed = evidence_file(repo,item['path'],lane=='shared')
                        status = 'matching' if observed==item else 'changed'
                    except FileNotFoundError:
                        status = 'missing'
                    except ValueError as exc:
                        status = 'changed' if str(exc)=='Shared evidence must match HEAD' else 'unavailable'
                    except (OSError,subprocess.SubprocessError):
                        status = 'unavailable'
                    checks.append({'path':item['path'],'status':status})
                result['evidenceStatus'] = checks
            results.append(result)
    if revision and not results:
        raise ValueError('Requested revision not found for this definition')
    if not results:
        return None
    if len(results) == 2:
        results[0]['sharedHandoff'] = results[1]
    return results[0]


if os.name == 'nt':
    import ctypes
    from ctypes import wintypes

    class FileBasicInfo(ctypes.Structure):
        _fields_ = [(name,ctypes.c_int64) for name in ('creation','access','write','change')] + [('attributes',wintypes.DWORD)]

    _win = ctypes.WinDLL('kernel32',use_last_error=True)
    _win.CreateFileW.argtypes = [wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,wintypes.LPVOID,
                                 wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE]
    _win.CreateFileW.restype = wintypes.HANDLE
    _win.GetFileInformationByHandleEx.argtypes = [wintypes.HANDLE,ctypes.c_int,wintypes.LPVOID,wintypes.DWORD]
    _win.CloseHandle.argtypes = [wintypes.HANDLE]


def file_stamp(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 1024:
        raise ValueError('Linked file changed during handoff preparation')
    changed = info.st_ctime_ns
    if os.name=='nt':
        # CPython's Windows st_ctime may be creation time. NTFS ChangeTime detects an
        # in-place rewrite even when a caller restores mtime and the file length is unchanged.
        handle = _win.CreateFileW(str(path),0x80,7,None,3,0x02200000,None)
        if handle==ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            basic=FileBasicInfo()
            if not _win.GetFileInformationByHandleEx(handle,0,ctypes.byref(basic),ctypes.sizeof(basic)):
                raise ctypes.WinError(ctypes.get_last_error())
            if basic.attributes & 1024:
                raise ValueError('Linked file changed during handoff preparation')
            changed=basic.change
        finally:
            _win.CloseHandle(handle)
    return info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, changed


def history_stamps(folder):
    try:
        entries = folder.iterdir()
        result = {}
        for path in entries:
            if path.name.startswith('.handoff-') and path.suffix=='.tmp':
                continue
            if len(result)>=MAX_RECORDS or path.suffix!='.json' or not HEX.fullmatch(path.stem):
                raise ValueError('Invalid handoff history; retain files for review')
            result[path.stem]=file_stamp(path)
        return result
    except FileNotFoundError:
        if folder.exists():
            raise ValueError('Handoff history changed during read')
        return {}


def evidence_inputs(repo, refs, shared):
    watched = set()
    for ref in refs:
        path = beneath(repo,ref)
        watched.add(path)
        if shared:
            parent = path.parent
            while parent.is_relative_to(repo):
                watched.add(parent/'.gitattributes')
                parent=parent.parent
    if shared and refs:
        # Resolve linked worktrees and packed refs through Git, before acquiring any queue lock.
        git = ['git','-C',str(repo)]
        keys = ['HEAD','index','packed-refs','reftable/tables.list','info/attributes','config','config.worktree']
        # Follow the actual symbolic reference chain, including a worktree's per-worktree HEAD.
        ref = 'HEAD'
        anchors = {}
        for _ in range(8):
            ref_file = Path(subprocess.check_output([*git,'rev-parse','--path-format=absolute','--git-path',ref]).decode('utf8').strip())
            anchors[ref_file]=file_stamp(ref_file)
            symbolic = subprocess.run([*git,'symbolic-ref','--no-recurse','-q',ref],capture_output=True)
            if symbolic.returncode==1:
                break
            symbolic.check_returncode()
            ref=symbolic.stdout.decode('utf8').strip()
            if not ref.startswith('refs/') or ref in keys:
                raise ValueError('Unexpected Git reference chain')
            keys.append(ref)
        else:
            raise ValueError('Git symbolic reference chain is too deep')
        args = [arg for key in keys for arg in ('--git-path',key)]
        locations = subprocess.check_output([*git,'rev-parse','--path-format=absolute',*args]).decode('utf8').splitlines()
        if len(locations)!=len(keys):
            raise ValueError('Could not resolve Git evidence inputs')
        watched.update(map(Path,locations))
        if (repo/'.git').is_file():
            watched.add(repo/'.git')
        if any(file_stamp(path)!=mark for path,mark in anchors.items()):
            raise ValueError('Git reference changed while preparing evidence')
        return watched, anchors
    return watched, {}


def save_handoff(repo, machine, task, *, actor, parent, summary, next_action, decisions=(), open_items=None,
                 evidence=None, audience='local', confirm_non_sensitive=False, merge_parents=(), _lease_guard=nullcontext):
    repo, locations = dirs(repo,machine,task)
    if audience not in ('local','shared') or audience=='shared' and not confirm_non_sensitive:
        raise ValueError('Shared context requires explicit non-sensitive confirmation')
    if parent != 'none' and not HEX.fullmatch(parent):
        raise ValueError('Supply the previously read revision or none')
    extra = sorted(phrases(merge_parents,7,64))
    if extra and (parent == 'none' or parent in extra or any(not HEX.fullmatch(p) for p in extra) or
                  open_items is None or evidence is None):
        raise ValueError('Supply distinct reviewed merge tips and explicit open/evidence lists')
    record = dict(schemaVersion=1, id=task['id'], definition=task['fingerprint'], machine=machine,
                  audience=audience, actor=phrase(actor,100), parent=parent, summary=phrase(summary,400),
                  next=phrase(next_action,320))
    if extra:
        record['mergeParents'] = extra
    folder = locations[audience]
    before = history_stamps(folder)
    records = history(folder,task,audience)
    if set(records) != before.keys():
        raise ValueError('Stale handoff history during preparation; read and retry')
    parents = [parent,*extra] if parent != 'none' else []
    if any(p not in records for p in parents):
        raise ValueError('Unknown parent revision')
    previous = records.get(parent,{})
    inherited = [d for p in parents for d in records[p]['decisions']]
    record['decisions'] = phrases(list(dict.fromkeys([*inherited,*phrases(decisions,12,160)])),12,160)
    record['open'] = previous.get('open',[]) if open_items is None else phrases(open_items,8,160)
    refs = phrases(evidence,8,320) if evidence is not None else []
    inputs, anchors = evidence_inputs(repo, refs, audience=='shared')
    stamps = {path:file_stamp(path) for path in inputs}
    if any(stamps.get(path)!=mark for path,mark in anchors.items()):
        raise ValueError('Git reference changed while preparing evidence')
    record['evidence'] = (previous.get('evidence',[]) if evidence is None else
                          evidence_files(repo,refs,audience=='shared'))
    data = encoded(record)
    revision = digest(data)
    if revision not in records:
        if set(parents) != leaves(records):
            raise ValueError('Stale handoff parent; read the latest revision first')
        if len(records) >= MAX_RECORDS or len(data)+1 > MAX_DOCUMENT:
            raise ValueError('Handoff history limit reached; preserve history for review')
    locations['lock'].mkdir(parents=True,exist_ok=True)
    beneath(locations['lock'],'.lock')
    with _lease_guard(), Locked(str(locations['lock'])):
        # Re-derive safe paths. The short commit section performs metadata I/O and one bounded
        # immutable record write. Git subprocesses, evidence reads and history hashing run above.
        _, current_locations = dirs(repo,machine,task)
        folder = current_locations[audience]
        for ref in refs:
            beneath(repo,ref)
        if any(file_stamp(path)!=mark for path,mark in stamps.items()):
            raise ValueError('Evidence or Git state changed during preparation; read and retry')
        after = history_stamps(folder)
        if after!=before:
            # Two exact retries may race: accept only that single identical immutable addition.
            if (revision in before or set(after)!=set(before)|{revision}
                    or any(after[key]!=mark for key,mark in before.items())
                    or regular(beneath(folder,revision+'.json'),MAX_DOCUMENT)!=data+b'\n'):
                raise ValueError('Stale handoff parent or changed history; read the latest revision first')
            records[revision]=record
        if revision not in records:
            atom(beneath(folder,revision+'.json'),data+b'\n')
            records[revision]=record
        heads=sorted(leaves(records))
        return {**view(records,revision,current_locations,actor),'currentRevision':heads[0] if len(heads)==1 else None}



def acknowledge_handoff(repo, machine, task, *, actor, revision):
    _, locations = dirs(repo,machine,task)
    if not HEX.fullmatch(revision):
        raise ValueError('Expected a full handoff revision')
    actor = phrase(actor,100)
    locations['lock'].mkdir(parents=True,exist_ok=True)
    beneath(locations['lock'],'.lock')
    with Locked(str(locations['lock'])):
        for lane in ('local','shared'):
            records = history(locations[lane],task,lane)
            if revision != tip(records):
                continue
            seen = view(records,revision,locations,actor)
            if not seen['acknowledged']:
                ack = beneath(locations['acks'],lane+'/'+revision+'/'+digest(actor.encode())+'.json')
                atom(ack,encoded(dict(schemaVersion=1,actor=actor,revision=revision,at=int(time.time())))+b'\n')
            return {'acknowledged':True,'revision':revision,'actor':actor,'alreadyAcknowledged':seen['acknowledged']}
    raise ValueError('Stale ACK; read the latest handoff first')


def cli():
    from todo_queue import applies, machine_id, paths, require_migrated, state_for, tasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,default=Path.cwd())
    parser.add_argument('--machine',help='Defaults to this PC')
    commands = parser.add_subparsers(dest='action',required=True)
    reading = commands.add_parser('read')
    writing = commands.add_parser('save')
    acknowledging = commands.add_parser('ack')
    for cmd in (reading,writing,acknowledging):
        cmd.add_argument('id')
        cmd.add_argument('--definition',required=cmd is not reading,
                         help='64-hex definition hash; pin writes or read a previous definition')
        cmd.add_argument('--actor',required=cmd is not reading)
    for cmd in (writing,acknowledging):
        cmd.add_argument('--lease',required=True,help='Current queue lease of the named actor')
    reading.add_argument('--revision')
    reading.add_argument('--verify-evidence',action='store_true')
    reading.add_argument('--audience',choices=['local','shared','both'],default='both')
    acknowledging.add_argument('--revision',required=True)
    writing.add_argument('--parent',required=True,help='Latest revision of the chosen lane, or none')
    writing.add_argument('--merge-parent',action='append',default=[],help='Each other explicitly reviewed branch tip')
    writing.add_argument('--summary',required=True)
    writing.add_argument('--next',dest='next_action',required=True)
    writing.add_argument('--decision',action='append',default=[])
    opened = writing.add_mutually_exclusive_group()
    opened.add_argument('--open',dest='open_items',action='append')
    opened.add_argument('--clear-open',dest='open_items',action='store_const',const=[])
    refs = writing.add_mutually_exclusive_group()
    refs.add_argument('--evidence',action='append',help='Repository-relative file; only its reference, size and hash are saved')
    refs.add_argument('--clear-evidence',dest='evidence',action='store_const',const=[])
    writing.add_argument('--audience',choices=['local','shared'],default='local')
    writing.add_argument('--confirm-non-sensitive',action='store_true')
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    machine = machine_id(args.machine)
    def active_task():
        return next((t for t in tasks(repo) if t['id']==args.id and applies(t,machine)),None)
    if args.action=='read':
        task = {'id':args.id,'fingerprint':args.definition} if args.definition else active_task()
        if not task:
            raise ValueError('Task not active; specify the recorded definition to read historical context')
        return {'id':task['id'],'definition':task['fingerprint'],
                'handoff':read_handoff(repo,machine,task,actor=args.actor,audience=args.audience,revision=args.revision,
                                       verify_evidence=args.verify_evidence)}
    if machine != machine_id():
        raise ValueError('Handoff mutations are allowed only for this PC')
    require_migrated(repo,machine)
    queue_root, _ = paths(repo,machine)
    safe_root = beneath(repo,'.local/todo-queue/'+machine)
    if queue_root != safe_root or not safe_root.is_dir():
        raise ValueError('Current queue state is required')
    beneath(safe_root,'.lock')
    def verify_lease():
        task = active_task()
        if not task or args.definition != task['fingerprint']:
            raise ValueError('Queue definition changed')
        state = state_for(beneath(safe_root,'state.json'),time.time())
        row = state['jobs'].get(args.id,{})
        if not (row.get('state')=='running' and row.get('owner')==args.actor and
                row.get('lease')==args.lease and row.get('fingerprint')==args.definition):
            raise ValueError('Active owner, lease and current definition required')
        return task

    @contextmanager
    def lease_guard():
        with Locked(str(safe_root)):
            verify_lease()
            yield

    if args.action=='ack':
        with lease_guard():
            return acknowledge_handoff(repo,machine,{'id':args.id,'fingerprint':args.definition},
                                       actor=args.actor,revision=args.revision)
    task = verify_lease()  # Avoid preparing evidence for an already invalid request.
    return save_handoff(repo,machine,task,actor=args.actor,parent=args.parent,summary=args.summary,
                        next_action=args.next_action,decisions=args.decision,open_items=args.open_items,
                        evidence=args.evidence,audience=args.audience,confirm_non_sensitive=args.confirm_non_sensitive,
                        merge_parents=args.merge_parent,_lease_guard=lease_guard)



if __name__=='__main__':
    try:
        print(json.dumps(cli(),ensure_ascii=False,indent=2))
    except (ValueError,OSError,KeyError,TypeError,TimeoutError,subprocess.SubprocessError) as exc:
        print('Handoff: '+str(exc),file=sys.stderr)
        sys.exit(1)


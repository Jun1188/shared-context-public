"""Remote definitions and per-PC queues: cooperative MLFQ + vruntime, no task execution."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import sys
import time
import uuid
import sqlite3
from contextlib import ExitStack

import todo_definitions as ledger
from todo_requires import blocked_reason
from work_table import Locked, clean

try:
    import todo_handoff
except ModuleNotFoundError as missing:
    if missing.name != 'todo_handoff':
        raise
    todo_handoff = None


QUANTA = (300, 900, 1800)
WEIGHTS = (1024, 512, 256)
BOOST_SECONDS = 3600
START, END = '<!-- shared-todo-queue:start -->', '<!-- shared-todo-queue:end -->'


def machine_id(value=None):
    value = (value or os.environ.get('COMPUTERNAME') or socket.gethostname()).upper()
    if not re.fullmatch(r'[A-Z0-9][A-Z0-9_-]{0,63}', value):
        raise ValueError('Invalid machine id')
    return value


def atomic_json(file, value):
    file.parent.mkdir(parents=True, exist_ok=True)
    temp = file.with_name(file.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8-sig')
    os.replace(temp, file)


def load_json(file, default):
    return json.loads(file.read_text(encoding='utf-8-sig')) if file.is_file() else default


def task_fingerprint(identity, targets, summary, pointer):
    return hashlib.sha256('\0'.join((identity, targets, summary, pointer)).encode('utf-8')).hexdigest()


def markdown_tasks(repo, raw=None):
    text = (raw if raw is not None else (repo / 'TODO.md').read_bytes()).decode('utf-8-sig')
    if text.count(START) != 1 or text.count(END) != 1 or text.index(END) <= text.index(START):
        raise ValueError('Shared queue markers missing')
    section = text.split(START, 1)[1].split(END, 1)[0]
    result, seen = [], set()
    for line in section.splitlines():
        if not line.startswith('|'):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        if cells[0] in ('업무 ID', 'ID') or re.fullmatch(r':?-+:?', cells[0]):
            continue
        if len(cells) != 5 or not re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}', cells[0]):
            raise ValueError('Invalid shared queue row; fix its ID/columns instead of hiding it')
        identity, targets, summary, status, pointer = cells
        if identity in seen:
            raise ValueError('Duplicate task id: ' + identity)
        seen.add(identity)
        if status.lower().startswith(('done', 'cancelled', '완료', '취소')):
            continue
        if targets == '*' or targets.startswith('other:'):
            raise ValueError('Remote tasks need a finite target snapshot; register with add --targets instead')
        for target in targets.split(','):
            machine_id(target.strip())
        fingerprint = task_fingerprint(identity, targets, summary, pointer)
        result.append(dict(id=identity, targets=targets, summary=summary, status=status,
                           detail=pointer, held=status.lower().startswith(('blocked', '보류')), fingerprint=fingerprint))
    return result


def tasks(repo, target=None):
    if ledger.enabled(repo):
        return ledger.read(repo, machine_id(), target)
    rows = markdown_tasks(repo)
    return [task for task in rows if applies(task,target)] if target else rows


def applies(task, machine):
    return machine in [machine_id(s.strip()) for s in task['targets'].split(',')]


def capability_profile(repo, machine):
    profile = load_json(repo/'machines'/(machine+'.json'), None)
    file = repo/'machines'/machine/'capabilities.json'
    if not file.exists():
        return profile
    override = load_json(file, {})
    caps = override.get('capabilities') if isinstance(override,dict) else None
    if (not isinstance(caps,dict) or override.get('schemaVersion')!=1 or override.get('machine')!=machine
            or not all(isinstance(k,str) and k == k.lower() and type(v) is bool for k,v in caps.items())):
        raise ValueError('Invalid capabilities snapshot for '+machine)
    return {**(profile or {}), 'capabilities':caps}


def replace_queue(repo, before, after):
    file = repo / 'TODO.md'
    if file.read_bytes() != before:
        raise ValueError('Remote queue changed concurrently; preserve the result and retry after synchronization')
    temp = file.with_name('TODO.md.' + uuid.uuid4().hex + '.tmp')
    temp.write_bytes(after.encode('utf-8-sig'))
    os.replace(temp, file)


def completion_records(repo, task, histories=None):
    receipts, missing = {}, []
    for target in (machine_id(s.strip()) for s in task['targets'].split(',')):
        if histories is None:
            history = load_json(repo / 'todo-queue/completed' / (target + '.json'), {})
        else:
            if target not in histories:
                histories[target] = load_json(repo / 'todo-queue/completed' / (target + '.json'), {})
            history = histories[target]
        record = history.get(task['id'], {})
        evidence = Path(record.get('evidence', ''))
        if (record.get('fingerprint') != task['fingerprint'] or evidence.is_absolute()
                or not (repo / evidence).resolve().is_relative_to(repo) or not (repo / evidence).is_file()):
            missing.append(target)
        else:
            receipts[target] = record
    return receipts, missing


def remote_view(repo):
    rows = []
    for task in tasks(repo):
        completed, pending = completion_records(repo, task)
        rows.append(dict(id=task['id'], targets=task['targets'].split(','), summary=task['summary'],
                         detail=task['detail'], pendingPCs=pending, completedPCs=sorted(completed),
                         finalizationPending=not pending))
    return dict(scope='remote', source=ledger.SOURCE if ledger.enabled(repo) else 'TODO.md', tasks=rows)


def archive_definition(task):
    fields = ('id', 'targets', 'summary', 'detail', 'fingerprint')
    if not isinstance(task, dict) or any(not isinstance(task.get(k), str) for k in fields):
        raise ValueError('Incomplete finalization definition')
    stable = {k: task[k] for k in fields}
    if (not re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}', stable['id']) or
            task_fingerprint(*(stable[k] for k in fields[:-1])) != stable['fingerprint']):
        raise ValueError('Finalization definition fingerprint mismatch')
    return stable


def finalization_paths(repo, machine, identity, fingerprint):
    if not re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}', identity) or not re.fullmatch(r'[a-f0-9]{64}', fingerprint):
        raise ValueError('Invalid finalization identity')
    tail = Path(identity) / (fingerprint + '.json')
    return paths(repo, machine)[0] / 'finalize-pending' / tail, repo / 'todo-queue/archive' / tail


def resume_finalization(repo, machine, identity, receipt):
    fingerprint = receipt.get('fingerprint')
    if not fingerprint:
        raise ValueError('Matching local completion receipt required')
    active = next((t for t in tasks(repo) if t['id'] == identity), None)
    if active:
        if not applies(active, machine) or active['fingerprint'] != fingerprint:
            raise ValueError('Revised task cannot be removed with an old receipt')
        return finalize_remote(repo, active, machine)
    intent, archive = finalization_paths(repo, machine, identity, fingerprint)
    for file in (intent, archive):
        if file.is_file():
            task = archive_definition(load_json(file, {}).get('task'))
            if task['id'] != identity or task['fingerprint'] != fingerprint or not applies(task, machine):
                raise ValueError('Finalization record belongs to another definition or target')
            return finalize_remote(repo, task, machine)
    raise ValueError('Retired definition has no local intent or synchronized archive; wait for evidence')


def finalize_remote(repo, task, machine):
    task = archive_definition(task)
    if not applies(task, machine):
        raise ValueError('Finalizer is not a target PC')
    intent, archive_file = finalization_paths(repo, machine, task['id'], task['fingerprint'])
    structured = ledger.enabled(repo)
    before = None if structured else (repo / 'TODO.md').read_bytes()
    current = next((row for row in tasks(repo) if row['id'] == task['id']), None)
    if current and archive_definition(current) != task:
        raise ValueError('Remote definition changed; do not remove the revised task')
    receipts, missing = completion_records(repo, task)
    if missing:
        return dict(remoteRemoved=False, remainingPCs=missing, publicationRequired=True)
    for row in receipts.values():
        stamp = row.get('completedAt')
        if type(stamp) not in (float, int) or not math.isfinite(stamp) or stamp < 0:
            raise ValueError('Invalid completion receipt timestamp')
    # Keep the old fields for readers. Unsynchronized wall clocks select a deterministic label only.
    last = max(receipts, key=lambda pc: (receipts[pc]['completedAt'], pc))
    archive = dict(task=task, completedPCs=receipts, lastCompleter=last, at=receipts[last]['completedAt'],
                   clockBasis='greatest-reported-completedAt-then-PC; cross-PC chronology unverified', state='removed')
    archive = json.loads(json.dumps(archive, sort_keys=True, allow_nan=False))
    pending = load_json(intent, None)
    existing = load_json(archive_file, None)
    if pending is not None and pending != archive:
        raise ValueError('Stored finalization intent conflicts with synchronized receipts')
    if existing is not None:
        if (not isinstance(existing,dict) or archive_definition(existing.get('task')) != task or
                existing.get('completedPCs') != receipts or existing.get('state') not in ('prepared','removed')):
            raise ValueError('Existing archive conflicts with the definition or full receipts')
        if not current and existing['state']=='removed':
            intent.unlink(missing_ok=True)
            return dict(remoteRemoved=True, alreadyRemoved=True)
    if not current and pending is None and existing is None:
        raise ValueError('Cannot publish an archive without a retired definition and recovery evidence')
    if pending is None:
        atomic_json(intent, archive)
    if current:
        if structured:
            ledger.append(repo,machine,'remove',dict(id=task['id'],fingerprint=task['fingerprint']))
        else:
            text = before.decode('utf-8-sig')
            head, tail = text.split(START, 1)
            section, rest = tail.split(END, 1)
            section = ''.join(line for line in section.splitlines(keepends=True)
                              if not (line.startswith('|') and line.split('|')[1].strip() == task['id']))
            replace_queue(repo, before, head + START + section + END + rest)
    if structured:
        ledger.sync(repo,machine)
    visible = any(row['id']==task['id'] for row in tasks(repo))
    if not structured and not visible:
        # Done/cancelled display statuses are hidden by tasks(), but their physical row still exists.
        section = (repo / 'TODO.md').read_text(encoding='utf-8-sig').split(START,1)[1].split(END,1)[0]
        visible = any(line.startswith('|') and line.split('|')[1].strip()==task['id'] for line in section.splitlines())
    if visible:
        raise ValueError('Definition remains active; preserve intent and review before publishing')
    observed, missing = completion_records(repo,task)
    if missing or observed != receipts:
        raise ValueError('Completion receipts changed during retirement; preserve intent for review')
    received = load_json(archive_file,None)
    if received != existing and (not isinstance(received,dict) or
            archive_definition(received.get('task')) != task or received.get('completedPCs') != receipts or
            received.get('state') != 'removed'):
        raise ValueError('Archive changed during retirement; preserve both records for review')
    if received is None or received['state']=='prepared':
        atomic_json(archive_file, archive)
    intent.unlink(missing_ok=True)
    return dict(remoteRemoved=True, removedBy=machine, archive=archive_file.relative_to(repo).as_posix(),
                publicationRequired=True)


def register(args, repo, machine):
    registered = sorted(machine_id(file.stem) for file in (repo / 'machines').glob('*.json'))
    if args.targets in ('*', 'other'):
        targets = [target for target in registered if args.targets == '*' or target != machine]
    else:
        targets = sorted(set(machine_id(s.strip()) for s in args.targets.split(',')))
    if not targets or not re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}', args.id):
        raise ValueError('A task needs a valid id and at least one target PC')
    detail = Path(args.detail)
    if detail.is_absolute() or not (repo / detail).resolve().is_relative_to(repo) or not (repo / detail).is_file():
        raise ValueError('Detail must be an existing repository-relative file')
    if any(c in detail.as_posix() for c in '|\r\n'):
        raise ValueError('Invalid detail path')
    root, _ = paths(repo, machine)
    root.mkdir(parents=True, exist_ok=True)
    with Locked(str(root)):
        existing = next((item for item in tasks(repo) if item['id'] == args.id), None)
        target_text, summary = ','.join(targets), clean(args.summary, 240)
        if not summary:
            raise ValueError('Task summary is empty')
        if getattr(args,'requires',None):
            if not all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:>=-]{0,63}', token) for token in args.requires):
                raise ValueError('Invalid requirement token')
            summary += ' [requires: '+', '.join(args.requires)+']'
        pointer = '[상세](' + detail.as_posix() + ')'
        if existing:
            if (existing['targets'], existing['summary'], existing['detail']) == (target_text, summary, pointer):
                return dict(id=args.id, registered=True, alreadyRegistered=True, targets=targets)
            raise ValueError('Task id already exists with different content; review instead of overwriting it')
        fingerprint = task_fingerprint(args.id, target_text, summary, pointer)
        if (repo / 'todo-queue/archive' / args.id / (fingerprint + '.json')).is_file():
            return dict(id=args.id, registered=False, alreadyArchived=True, targets=targets)
        if ledger.enabled(repo):
            task = dict(id=args.id, targets=target_text, summary=summary, status='대기', detail=pointer, fingerprint=fingerprint)
            ledger.append(repo,machine,'add',ledger.to_payload(task))
            ledger.sync(repo,machine)
        else:
            before = (repo / 'TODO.md').read_bytes()
            text = before.decode('utf-8-sig')
            row = f'| {args.id} | {target_text} | {summary} | 대기 | {pointer} |\n'
            replace_queue(repo, before, text.replace(END, row + END, 1))
        return dict(id=args.id, registered=True, targets=targets, publicationRequired=True)


def paths(repo, machine):
    # LOCALAPPDATA is virtualized in the Codex sandbox; a checkout-local ignored root is stable.
    root = repo / '.local' / 'todo-queue' / machine
    return root, repo / 'todo-queue/completed' / (machine + '.json')


def legacy_roots(repo, machine):
    key = hashlib.sha256(str(repo).casefold().encode('utf-8')).hexdigest()[:12]
    bases = [Path.home() / 'AppData/Local/agent-work' if os.name == 'nt' else Path.home() / '.local/share/agent-work']
    if os.environ.get('LOCALAPPDATA'):
        bases.append(Path(os.environ['LOCALAPPDATA']) / 'agent-work')
    if os.environ.get('AGENT_WORK_DIR'):
        bases.append(Path(os.environ['AGENT_WORK_DIR']))
    return list(dict.fromkeys(base / 'shared-todo' / key / machine for base in bases))


def migrate_state(repo, machine):
    root = paths(repo, machine)[0]
    root.mkdir(parents=True, exist_ok=True)
    names = ('state.json', 'worker/config.json', 'worker/scopes.json', 'worker/dispatch.json')
    with Locked(str(root)):
        if (root / 'migration.json').is_file():
            return dict(migrated=False, alreadyMigrated=True, root=str(root))
        sources = [old for old in legacy_roots(repo, machine) if any((old / name).is_file() for name in names)]
        with ExitStack() as locks:
            (root / 'worker').mkdir(exist_ok=True)
            locks.enter_context(Locked(str(root / 'worker')))
            for old in sorted(sources):
                locks.enter_context(Locked(str(old)))
                if (old / 'worker').is_dir():
                    locks.enter_context(Locked(str(old / 'worker')))
            prepared = {}
            for old in sources:
                for name in names:
                    source = old / name
                    if source.is_file():
                        data = source.read_bytes()
                        if name in prepared and prepared[name] != data:
                            raise ValueError('Legacy queue states disagree; preserve both and reconcile ownership manually')
                        if (root / name).is_file() and (root / name).read_bytes() != data:
                            raise ValueError('New and legacy queue states disagree; do not overwrite either')
                        prepared[name] = data
            for name, data in prepared.items():
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(destination.name + '.' + uuid.uuid4().hex + '.tmp')
                temporary.write_bytes(data)
                os.replace(temporary, destination)
            atomic_json(root / 'migration.json', dict(at=time.time(), sources=[str(old) for old in sources],
                                                     files=list(prepared), originalsPreserved=True))
    return dict(migrated=True, root=str(root), originalsPreserved=True)


def require_migrated(repo, machine):
    root = paths(repo, machine)[0]
    if not (root / 'migration.json').exists() and any(
            (old / 'state.json').exists() or (old / 'worker/config.json').exists() for old in legacy_roots(repo, machine)):
        raise ValueError('Legacy PC state exists; run migrate-state before resuming work')


def state_for(file, now):
    value = load_json(file, dict(schema=1, floor=0.0, boostAt=now, sequence=0, jobs={}))
    if value.get('schema') != 1 or not isinstance(value.get('jobs'), dict):
        raise ValueError('Invalid local queue state; preserve it for review')
    if any(not isinstance(value.get(k), (int, float)) or not math.isfinite(value[k]) or value[k] < 0
           for k in ('floor', 'boostAt')):
        raise ValueError('Invalid scheduler clocks; preserve state for review')
    for row in value['jobs'].values():
        if (row.get('state') not in ('ready', 'running', 'blocked', 'done') or
                type(row.get('level')) is not int or not 0 <= row['level'] < len(QUANTA) or
                any(not isinstance(row.get(k), (int, float)) or not math.isfinite(row[k]) or row[k] < 0
                    for k in ('used', 'vruntime', 'service'))):
            raise ValueError('Invalid job accounting; preserve state for review')
    return value


def live_tasks(repo, machine, state, history, definitions=None):
    if not isinstance(history, dict):
        raise ValueError('Invalid completion history; preserve it for review')
    for identity, row in list(state['jobs'].items()):
        if history.get(identity, {}).get('fingerprint') == row.get('fingerprint'):
            state['jobs'].pop(identity)
    result, excluded = [], 0
    for task in (tasks(repo, machine) if definitions is None else definitions):
        if history.get(task['id'], {}).get('fingerprint') == task['fingerprint']:
            excluded += 1
            continue
        row = state['jobs'].get(task['id'])
        if row and row.get('fingerprint') == task['fingerprint'] and row['state'] == 'done':
            row.update(state='blocked', reason='Completion receipt missing; review before unblocking')
        if not row or row.get('fingerprint') != task['fingerprint']:
            if row and row.get('state') == 'running':
                result.append(({**task, 'definitionChanged': True}, row))
                continue
            state['sequence'] = state.get('sequence', 0) + 1
            row = dict(fingerprint=task['fingerprint'], order=state['sequence'], level=0, used=0.0, vruntime=state['floor'],
                       service=0.0, state='ready')
            state['jobs'][task['id']] = row
        result.append((task, row))
    return result, excluded


def boost(state, now):
    if now - state['boostAt'] >= BOOST_SECONDS:
        for row in state['jobs'].values():
            if row['state'] != 'running':
                row.update(level=0, used=0.0)
        state['boostAt'] = now


def account(row, stopped_at=None):
    elapsed = time.monotonic() - row['startedMono']
    if elapsed < 0 or not math.isfinite(elapsed) or abs(time.time() - time.monotonic() - row['bootEpoch']) > 5:
        raise ValueError('System clock/boot changed; account this lease manually before continuing')
    if stopped_at is not None:
        elapsed = min(elapsed, max(0.0, stopped_at - row['startedAt']))
    remaining = elapsed
    # Yielding early never resets consumed allotment, so repeated short yields cannot game the queue.
    while remaining > 0:
        level = row['level']
        segment = min(remaining, max(0.0, QUANTA[level] - row['used']))
        row['vruntime'] += segment * 1024 / WEIGHTS[level]
        row['used'] += segment
        remaining -= segment
        if row['used'] >= QUANTA[level]:
            row.update(level=min(level + 1, len(QUANTA) - 1), used=0.0)
    row['service'] += elapsed
    for key in ('lease', 'startedMono', 'startedAt', 'bootEpoch'):
        row.pop(key, None)
    return elapsed


def parallel_config(root):
    return parallel_settings(load_json(root / 'worker/config.json', {}))


def parallel_settings(config):
    """Read approved local routing and resource declarations for the two-slot scheduler."""
    policy = config.get('parallel')
    if not policy or not config.get('enabled') or config.get('mode', 'standard') != 'standard':
        return None
    if not isinstance(policy, dict):
        raise ValueError('Invalid local parallel policy')
    if policy.get('machine') != machine_id():
        return None
    threads = [config.get('threadId')]
    if 'secondThread' in policy:
        threads.append(policy['secondThread'])
    if (any(not isinstance(t, str) or not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', t)
            for t in threads) or len(set(threads)) != len(threads) or not isinstance(policy.get('jobs'), dict)):
        raise ValueError('Expected distinct local receiver UUIDs and a jobs map')
    rules = {}
    for identity, spec in policy['jobs'].items():
        if not isinstance(spec, dict) or not re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}', identity):
            raise ValueError('Invalid parallel task rule')
        resources = spec.get('resources')
        if (not isinstance(spec.get('fingerprint'), str) or not re.fullmatch(r'[0-9a-f]{64}', spec['fingerprint'])
                or not isinstance(resources, list) or not resources or len(resources) > 32
                or any(not isinstance(key, str) or not re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,79}', key)
                       or key == 'queue-maintenance' for key in resources)):
            raise ValueError('Invalid reviewed task fingerprint or resource scope')
        rules[(identity, spec['fingerprint'])] = sorted(set(resources))
    return threads, rules


def parallel_offer(root, state, candidates, policy, live_fingerprints, owner=None, coordinator=None):
    """Choose from already eligible tasks. The caller serializes a real acquisition under the queue lock."""
    threads, rules = policy
    active = [(key, row) for key, row in state['jobs'].items() if row['state'] == 'running']
    claims = load_json(root / 'worker/scopes.json', {})
    busy = {row.get('owner') for _, row in active}
    if 'queue-maintenance' in claims or len(active) >= 2 or (owner and owner in busy):
        return None
    if coordinator is not None and coordinator not in threads:
        raise ValueError('Coordinator is not a registered local receiver')
    if owner is None:
        # The same coordinator can forward offers to distinct real subagents, including Claude.
        coordinator = next((thread for thread in threads if thread not in busy), threads[0])
    elif coordinator is None and owner in threads:
        coordinator = owner
    occupied = {key for key, claim in claims.items() if claim.get('owner') != owner}
    for identity, row in active:
        held = row.get('leaseResources')
        if (coordinator is None or row.get('leaseCoordinator') not in threads or not held
                or held != rules.get((identity, row['fingerprint']))
                or live_fingerprints.get(identity) != row['fingerprint']):
            return None
        occupied.update(held)
    for task in candidates:  # already in scheduler order
        scopes = rules.get((task['id'], task['fingerprint']), []) if coordinator else []
        if not scopes and (active or occupied):
            # Let an exclusive job at the head drain, instead of starving it behind new grants.
            return None
        if not occupied.intersection(scopes):
            return dict(id=task['id'], fingerprint=task['fingerprint'], receiver=coordinator, resources=scopes)
    return None


def validate_ticket(root, offer, row, ticket, now):
    if not re.fullmatch(r'[0-9a-f]{32}', ticket):
        raise ValueError('Invalid dispatch ticket')
    # Only an enrolled receiver can have a ticket file; never build a path from an arbitrary manual owner.
    if not offer['receiver'] or not re.fullmatch(r'[0-9a-f-]{36}', offer['receiver']):
        raise ValueError('Ticket receiver is not a registered thread')
    issued = load_json(root / 'worker' / ('dispatch-' + offer['receiver'] + '.json'), {})
    if (issued.get('ticket') != ticket or issued.get('offer') != offer
            or not 0 <= now - issued.get('at', 0) < 900 or row.get('heartbeatAt', 0) >= issued.get('at', 0)
            or row.get('lastTicket') == ticket):
        raise ValueError('Dispatch ticket expired, consumed, or superseded; wait for a fresh offer')



def view(repo, machine, limit=12):
    root, history_file = paths(repo, machine)
    if machine == machine_id():
        require_migrated(repo, machine)
    now = time.time()
    preview = machine != machine_id()
    state = dict(schema=1, floor=0.0, boostAt=now, sequence=0, jobs={}) if preview else state_for(root / 'state.json', now)
    history = load_json(history_file, {})
    definitions = tasks(repo, machine)
    live, excluded = live_tasks(repo, machine, state, history, definitions)
    boost(state, now)  # only this in-memory view changes; show never writes state
    profile = capability_profile(repo,machine)
    ready, running, blocked = [], [], []
    for task, row in live:
        value = {**task, 'level': row['level'], 'vruntime': round(row['vruntime'], 3),
                 'serviceSeconds': round(row['service'], 3), 'state': row['state'],
                 'queueOrder': row.get('order', 0),
                 'sliceSeconds': max(0.0, QUANTA[row['level']] - row['used'])}
        # The public task-definition digest is used to pin reviewed local resource declarations.
        if task['held'] and row['state'] != 'running':
            value.update(state='blocked', reason='Shared queue is on hold')
        # Computed on every read and never stored: the row keeps its order and vruntime, and a new profile lifts it.
        elif row['state'] == 'ready' and (need := blocked_reason(task['summary'], profile)):
            value.update(state='blocked', reason=need)
        if row.get('reason'):
            value['reason'] = row['reason']
        if row.get('checkpoint'):
            value['checkpoint'] = row['checkpoint']
        if row['state'] == 'running':
            value.update(owner=row.get('owner', 'legacy-owner'),
                         heartbeatAt=row.get('heartbeatAt', row.get('startedAt')),
                         stale=now - row.get('heartbeatAt', row.get('startedAt', now)) > 900)
        (running if value['state'] == 'running' else blocked if value['state'] == 'blocked' else ready).append(value)
    present = {task['id'] for task, _ in live}
    running.extend(dict(id=identity, state='running', reason='Definition removed; recover the orphaned lease')
                   for identity, row in state['jobs'].items() if row['state'] == 'running' and identity not in present)
    ready.sort(key=lambda row: (row['level'], state['jobs'][row['id']]['vruntime'], row['queueOrder'], row['id']))
    policy = parallel_config(root) if not preview else None
    offer = None
    if policy:
        offer = parallel_offer(root, state, ready, policy,
                               {task['id']: task['fingerprint'] for task, _ in live
                                if not task['held'] and not blocked_reason(task['summary'], profile)})
    histories = {machine: history}
    finalization = [task['id'] for task in definitions if history.get(task['id'], {}).get('fingerprint') == task['fingerprint']
                    and not completion_records(repo, task, histories)[1]]
    return dict(scope='pc', machine=machine, source=ledger.SOURCE if ledger.enabled(repo) else 'TODO.md', policy='MLFQ+weighted-vruntime',
                quantaSeconds=list(QUANTA), weights=list(WEIGHTS), boostSeconds=BOOST_SECONDS,
                ready=ready[:limit], running=running, blocked=blocked[:limit],
                completedExcluded=excluded, totalReady=len(ready), truncated=len(ready) > limit or len(blocked) > limit,
                previewOnly=preview, runtimeStateAvailable=not preview and (root / 'state.json').is_file(),
                remoteFinalizationPending=finalization,
                next=(offer['id'] if offer else None) if policy else (None if preview or running or not ready else ready[0]['id']),
                parallelOffer=offer)


def handoff_revisions(handoff):
    if not handoff:
        return {}
    records = [handoff]
    if handoff.get('sharedHandoff'):
        records.append(handoff['sharedHandoff'])
    return {record['audience']: record['revision'] for record in records}


def handoff_at_transition(repo, machine, task, owner, expected=None):
    if todo_handoff is None:
        if expected:
            raise ValueError('Handoff module required for a requested revision')
        return None
    handoff = todo_handoff.read_handoff(repo, machine, task, actor=owner)
    revisions = handoff_revisions(handoff)
    if expected and expected not in revisions.values():
        raise ValueError('Handoff revision changed; read the current handoff before transitioning')
    return handoff


def mutate(args, repo, machine):
    root, history_file = paths(repo, machine)
    require_migrated(repo, machine)
    root.mkdir(parents=True, exist_ok=True)
    with Locked(str(root)):
        now = time.time()
        state = state_for(root / 'state.json', now)
        if args.command == 'heartbeat':
            row = state['jobs'].get(args.id)
            if not row or row['state'] != 'running' or row.get('lease') != args.lease:
                raise ValueError('A matching running lease is required')
            result = dict(id=args.id, state='running', owner=row.get('owner'), heartbeatAt=now)
            revision = getattr(args, 'handoff_ack', None)
            if revision:
                task = next((t for t in tasks(repo) if t['id'] == args.id and applies(t, machine)), None)
                if not task or task['fingerprint'] != row['fingerprint']:
                    raise ValueError('Cannot acknowledge a changed or missing task definition')
                handoff_at_transition(repo, machine, task, row['owner'], revision)
                todo_handoff.acknowledge_handoff(repo, machine, task, actor=row['owner'], revision=revision)
                result['handoffAcknowledged'] = revision
            row['heartbeatAt'] = now
            atomic_json(root / 'state.json', state)
            return result
        history = load_json(history_file, {})
        live, _ = live_tasks(repo, machine, state, history)
        indexed = {task['id']: (task, row) for task, row in live}
        if args.command == 'complete' and args.id not in indexed:
            previous = history.get(args.id, {})
            expected = getattr(args, 'handoff_revision', None)
            if expected and expected not in previous.get('handoffRevisions', {}).values():
                raise ValueError('Completion receipt does not contain the requested handoff revision')
            original = next((task for task in tasks(repo) if task['id'] == args.id and applies(task, machine)), None)
            if original and previous.get('fingerprint') == original['fingerprint']:
                return dict(id=args.id, machine=machine, state='completed', removedFromActiveQueue=True, alreadyCompleted=True,
                            **finalize_remote(repo, original, machine))
            if not original and previous.get('fingerprint'):
                return dict(id=args.id, machine=machine, state='completed', alreadyCompleted=True,
                            **resume_finalization(repo, machine, args.id, previous))
        active = [identity for identity, row in state['jobs'].items() if row['state'] == 'running']
        if args.command == 'recover':
            row = state['jobs'].get(args.id)
            if not row or row['state'] != 'running':
                raise ValueError('No running lease to recover')
            for key in ('lease', 'startedMono', 'startedAt', 'bootEpoch'):
                row.pop(key, None)
            row.update(state='blocked', reason='Unmeasured recovery: ' + clean(args.reason, 100))
            output = dict(id=args.id, state='blocked', serviceMeasured=False)
        elif args.command == 'take':
            policy = parallel_config(root)
            if 'queue-maintenance' in load_json(root / 'worker/scopes.json', {}):
                raise ValueError('A queue-maintenance claim is held; do not acquire a new lease')
            owner = args.owner if policy else clean(args.owner, 100)
            if not owner or (policy and not re.fullmatch(r'[A-Za-z0-9_/][A-Za-z0-9_.:@/-]{0,99}', owner)):
                raise ValueError('Explicit owner identity required')
            if active and not policy:
                raise ValueError('A local lease is already running: ' + ', '.join(active))
            boost(state, now)
            profile = capability_profile(repo,machine)
            ready = [(task, row) for task, row in live if row['state'] == 'ready' and not task['held']
                     and not blocked_reason(task['summary'], profile)]
            ready.sort(key=lambda pair: (pair[1]['level'], pair[1]['vruntime'], pair[1].get('order', 0), pair[0]['id']))
            ticket = getattr(args, 'ticket', None)
            offer = None
            if policy:
                offer = parallel_offer(root, state, [task for task, _ in ready], policy,
                                       {task['id']: task['fingerprint'] for task, _ in live
                                if not task['held'] and not blocked_reason(task['summary'], profile)},
                                       owner, getattr(args, 'coordinator', None))
                if not offer:
                    return dict(state='unavailable' if active or ready else 'empty', machine=machine)
                task, row = indexed[offer['id']]
            elif ready:
                task, row = ready[0]
            else:
                return dict(state='empty', machine=machine)
            if ticket:
                if not offer:
                    raise ValueError('Parallel dispatch was disabled; ticket is no longer valid')
                validate_ticket(root, offer, row, ticket, now)
                row['lastTicket'] = ticket
            if getattr(args, 'coordinator', None) and not policy:
                raise ValueError('Coordinated execution requires a local parallel policy')
            if offer:
                row.update(leaseResources=offer['resources'], leaseCoordinator=offer['receiver'])
            else:
                row.pop('leaseResources', None)
                row.pop('leaseCoordinator', None)
            lease = uuid.uuid4().hex
            row.update(state='running', lease=lease, startedMono=time.monotonic(), startedAt=now,
                       bootEpoch=time.time() - time.monotonic(), owner=owner, heartbeatAt=now)
            output = dict(id=task['id'], lease=lease, level=row['level'],
                          sliceSeconds=QUANTA[row['level']] - row['used'], summary=task['summary'], detail=task['detail'],
                          checkpoint=row.get('checkpoint'))
            handoff = handoff_at_transition(repo, machine, task, owner)
            previous_revisions = row.get('handoffRevisions', {})
            current = handoff_revisions(handoff)
            for lane, revision in previous_revisions.items():
                if current.get(lane) != revision:
                    if todo_handoff is None:
                        raise ValueError('Previous handoff requires the handoff reader')
                    todo_handoff.read_handoff(repo, machine, task, actor=owner, audience=lane, revision=revision)
            if previous_revisions:
                output['previousHandoffRevisions'] = previous_revisions
            if handoff:
                output['handoff'] = handoff
        else:
            if args.id not in indexed:
                raise ValueError('Task is not active for this PC: ' + args.id)
            task, row = indexed[args.id]
            if args.command == 'unblock':
                if task['held']:
                    raise ValueError('Shared queue is on hold; update that status after review')
                if row['state'] != 'blocked':
                    raise ValueError('Task is not blocked')
                row.update(state='ready', vruntime=max(row['vruntime'], state['floor']))
                row.pop('reason', None)
                output = dict(id=args.id, state='ready')
            else:
                if (args.lease and (row['state'] != 'running' or row.get('lease') != args.lease)
                        or row['state'] == 'running' and not args.lease):
                    raise ValueError('The current lease is required')
                if args.command == 'yield' and row['state'] != 'running':
                    raise ValueError('Task has no running lease')
                evidence = None
                if args.command == 'complete':
                    previous_fingerprint = history.get(args.id, {}).get('fingerprint')
                    if previous_fingerprint and previous_fingerprint != task['fingerprint'] and not args.lease:
                        raise ValueError('A revised task needs its current lease before completion')
                    if task.get('definitionChanged'):
                        raise ValueError('Definition changed; yield the previous lease and review the new task before completion')
                    evidence = Path(args.evidence)
                    if evidence.is_absolute() or not (repo / evidence).resolve().is_relative_to(repo) or not (repo / evidence).is_file():
                        raise ValueError('Evidence must be an existing repository-relative file')
                expected = getattr(args, 'handoff_revision', None)
                if expected and (row['state'] != 'running' or task.get('definitionChanged')):
                    raise ValueError('Referencing a revision requires the matching live task and lease')
                handoff = None
                if row['state'] == 'running' and not task.get('definitionChanged'):
                    handoff = handoff_at_transition(repo, machine, task, row.get('owner'), expected)
                revisions = handoff_revisions(handoff)
                revision = expected or (handoff['revision'] if handoff else None)
                elapsed = account(row) if row['state'] == 'running' else 0.0
                if elapsed > 0 or args.command == 'complete':
                    state['lastWorkedAt'] = now
                if args.command == 'complete':
                    previous = history.get(args.id, {})
                    prior = list(previous.get('history', []))
                    if previous:
                        prior.append({k: v for k, v in previous.items() if k != 'history'})
                    history[args.id] = dict(fingerprint=task['fingerprint'], completedAt=now,
                                            evidence=evidence.as_posix(), note=clean(args.note, 240), history=prior,
                                            serviceSeconds=round(row['service'], 3), vruntime=round(row['vruntime'], 3))
                    if revision:
                        history[args.id].update(handoffRevision=revision, handoffRevisions=revisions)
                    # History is the commit point: a later state-write failure must not requeue completed work.
                    atomic_json(history_file, history)
                    row['state'] = 'done'
                    state['jobs'].pop(args.id, None)
                    output = dict(id=args.id, machine=machine, state='completed', removedFromActiveQueue=True)
                    output.update(finalize_remote(repo, task, machine))
                else:
                    if args.checkpoint:
                        checkpoint = Path(args.checkpoint).resolve(strict=True)
                        if not checkpoint.is_file():
                            raise ValueError('Checkpoint must be an existing evidence file')
                        row['checkpoint'] = dict(file=str(checkpoint), savedAt=now)
                    row['state'] = 'blocked' if args.blocked else 'ready'
                    if args.blocked:
                        row['reason'] = clean(args.blocked, 120)
                    output = dict(id=args.id, state=row['state'], level=row['level'],
                                  chargedSeconds=round(elapsed, 3), vruntime=round(row['vruntime'], 3))
                if revision:
                    output.update(handoffRevision=revision, handoffRevisions=revisions)
                    if args.command == 'yield':
                        row.update(handoffRevision=revision, handoffRevisions=revisions)
                remaining = [r['vruntime'] for _, r in live if r['state'] in ('ready', 'running')]
                state['floor'] = max(state['floor'], min(remaining, default=row['vruntime']))
        atomic_json(root / 'state.json', state)
        return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default=str(Path(__file__).resolve().parents[1]))
    sub = parser.add_subparsers(dest='command', required=True)
    show = sub.add_parser('show')
    show.add_argument('--machine')
    show.add_argument('--json', action='store_true')
    show.add_argument('--limit', type=int, default=12)
    sub.add_parser('remote')
    sub.add_parser('migrate-state')
    for action in ('migrate-definitions','sync-definitions','render-definitions'):
        sub.add_parser(action)
    add = sub.add_parser('add')
    add.add_argument('id')
    add.add_argument('--targets', default='other')
    add.add_argument('--summary', required=True)
    add.add_argument('--requires', action='append', metavar='TOKEN', help='Required capability; repeat for multiple tokens')
    add.add_argument('--detail', required=True)
    take = sub.add_parser('take')
    take.add_argument('--owner', required=True, help='Stable harness session/job identity')
    take.add_argument('--coordinator', help='Registered receiver delegating to this actual execution owner')
    take.add_argument('--ticket', help='Accept this short-lived task-bound local dispatch offer')
    heartbeat = sub.add_parser('heartbeat')
    heartbeat.add_argument('id')
    heartbeat.add_argument('--lease', required=True)
    heartbeat.add_argument('--handoff-ack', help='Exact current handoff revision for this lease')
    release = sub.add_parser('yield')
    release.add_argument('id')
    release.add_argument('--lease', required=True)
    release.add_argument('--handoff-revision', help='Exact current handoff revision for this lease')
    release.add_argument('--blocked', metavar='REASON')
    release.add_argument('--checkpoint', help='Existing local evidence file to read on resume')
    done = sub.add_parser('complete')
    done.add_argument('id')
    done.add_argument('--lease')
    done.add_argument('--handoff-revision', help='Exact current handoff revision for this lease')
    done.add_argument('--evidence', required=True)
    done.add_argument('--note', required=True)
    sub.add_parser('unblock').add_argument('id')
    recover = sub.add_parser('recover')
    recover.add_argument('id')
    recover.add_argument('--reason', required=True)
    reconcile = sub.add_parser('reconcile')
    reconcile.add_argument('id')
    reconcile.add_argument('--last-completer', action='store_true', required=True)
    args = parser.parse_args()
    if hasattr(args, 'id') and not re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}', args.id):
        raise ValueError('Invalid task id')
    repo = Path(args.repo).resolve(strict=True)
    machine = machine_id(args.machine if args.command == 'show' else None)
    if args.command == 'show':
        value = view(repo, machine, max(1, min(args.limit, 64)))
    elif args.command == 'remote':
        value = remote_view(repo)
    elif args.command == 'migrate-state':
        value = migrate_state(repo, machine)
    elif args.command in ('migrate-definitions','sync-definitions','render-definitions'):
        root = paths(repo,machine)[0]
        root.mkdir(parents=True,exist_ok=True)
        with Locked(str(root)):
            if args.command == 'migrate-definitions' and not ledger.enabled(repo):
                before = (repo/'TODO.md').read_bytes()
                ledger.migrate(repo,machine,markdown_tasks(repo,before),before)
            value = ledger.sync(repo,machine)
            if args.command == 'render-definitions':
                before = (repo/'TODO.md').read_bytes()
                text = before.decode('utf-8-sig')
                # A peer might still have edited the Markdown table with a pre-migration client.
                # Keep an unfamiliar row or edit in place for explicit import.
                known = set()
                for event in ledger.read_events(ledger.file_snapshot(repo)):
                    data = event['payload']
                    rows = data['rows'] if event['op'] == 'bootstrap' else [data] if event['op'] == 'add' else []
                    known.update((row['fingerprint'], row['status']) for row in rows)
                if any((row['fingerprint'], row['status']) not in known for row in markdown_tasks(repo, before)):
                    raise ValueError('Unimported Markdown edit; preserve it and register or reconcile it before rendering')
                head, tail = text.split(START,1)
                section, rest = tail.split(END,1)
                # Preserve local introductory text, table headers and everything after the queue.
                lines = [line for line in section.splitlines(keepends=True)
                         if not (line.startswith('|') and re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{2,79}',line.split('|')[1].strip()))]
                rows = ['| {id} | {targets} | {summary} | {status} | {detail} |\n'.format(**task) for task in tasks(repo)]
                next_text = head+START+''.join(lines)+''.join(rows)+END+rest
                if next_text != text:
                    replace_queue(repo,before,next_text)
                value['rendered'] = 'TODO.md'
    elif args.command == 'add':
        value = register(args, repo, machine)
    elif args.command == 'reconcile':
        root, history_file = paths(repo, machine)
        root.mkdir(parents=True, exist_ok=True)
        with Locked(str(root)):
            receipt = load_json(history_file, {}).get(args.id, {})
            value = resume_finalization(repo, machine, args.id, receipt)
    else:
        value = mutate(args, repo, machine)
    print(json.dumps(value, ensure_ascii=True, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, TimeoutError, sqlite3.Error) as error:
        print('todo_queue: ' + str(error), file=sys.stderr)
        sys.exit(1)


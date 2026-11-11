"""Local queue wakeups and cooperative cross-harness scope ownership; never runs task text as code."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid
from contextlib import nullcontext
import heapq

from todo_queue import account, parallel_config, parallel_settings, atomic_json, load_json, machine_id, paths, view, require_migrated, state_for
from work_table import Locked


def directory(repo):
    require_migrated(repo, machine_id())
    root = paths(repo, machine_id())[0] / 'worker'
    root.mkdir(parents=True, exist_ok=True)
    return root


def scope(repo, args):
    drain_events(repo)
    root = directory(repo)
    with Locked(str(root.parent)), Locked(str(root)):
        file = root / 'scopes.json'
        claims = load_json(file, {})
        current = claims.get(args.scope)
        if args.action == 'status':
            return {key: {**row, 'stale': time.time() - row['heartbeatAt'] > 900}
                    for key, row in claims.items()}
        if args.action == 'claim':
            if current:
                return dict(acquired=False, scope=args.scope, owner=current['owner'],
                            stale=time.time() - current['heartbeatAt'] > 900)
            state = state_for(root.parent / 'state.json', time.time())
            enrolled = parallel_config(root)
            for row in state['jobs'].values():
                if row['state'] != 'running' or row.get('owner') == args.owner:
                    continue
                held = row.get('leaseResources')
                if args.scope == 'queue-maintenance' or ((enrolled or held) and (not held or args.scope in held)):
                    return dict(acquired=False, scope=args.scope, owner=row.get('owner'), reason='lease-resource-held')
            current = dict(owner=args.owner, token=uuid.uuid4().hex, heartbeatAt=time.time())
            claims[args.scope] = current
        else:
            if not current or current['token'] != args.token or current['owner'] != args.owner:
                raise ValueError('The matching scope owner and token are required')
            if args.action == 'release':
                del claims[args.scope]
            else:
                current['heartbeatAt'] = time.time()
        atomic_json(file, claims)
        return dict(acquired=True, scope=args.scope, **current)


def check_event(event, now):
    if (not isinstance(event, dict) or event.get('command') not in ('owner-active', 'owner-finished')
            or not isinstance(event.get('owner'), str) or not 0 < len(event['owner']) <= 100
            or type(event.get('at')) not in (float, int) or not 0 < event['at'] <= now + 60):
        raise ValueError('Invalid owner event')


def apply_owner_events(root, events):
    """Commit a bounded batch under queue -> worker lock order. Retained inputs allow replay."""
    now = time.time()
    for _, event in events:
        check_event(event, now)
    has_finish = any(event['command'] == 'owner-finished' for _, event in events)
    queue_lock = Locked(str(root.parent)) if has_finish else nullcontext()
    outcomes = {}
    with queue_lock, Locked(str(root)):
        owners = load_json(root / 'owners.json', {})
        claims = state = None
        owners_dirty = scopes_dirty = state_dirty = False
        for key, event in events:
            name, at = event['owner'], event['at']
            owner = owners.setdefault(name, {})
            if event['command'] == 'owner-active':
                if at > owner.get('activeAt', 0):
                    owner['activeAt'] = at
                    owners_dirty = True
                outcomes[key] = dict(owner=name, state='active')
                continue
            if owner.get('activeAt', 0) > at:
                outcomes[key] = dict(owner=name, released=[], reason='newer-activity')
                continue
            if owner.get('finishedAt', 0) >= at:
                outcomes[key] = dict(owner=name, released=[], reason='already-finished')
                continue
            if state is None:
                claims = load_json(root / 'scopes.json', {})
                state = state_for(root.parent / 'state.json', now)
            released = [scope for scope, row in claims.items()
                        if row['owner'] == name and row['heartbeatAt'] <= at]
            for scope in released:
                del claims[scope]
            recovered = []
            for identity, row in state['jobs'].items():
                if (row['state'] == 'running' and row.get('owner') == name
                        and row.get('heartbeatAt', row.get('startedAt', 0)) <= at):
                    reason = 'Owner finished without yield; review checkpoint before resuming'
                    try:
                        account(row, stopped_at=at)
                    except (KeyError, ValueError):
                        # A reboot or legacy lease has no reliable elapsed time.
                        reason += '; manual service accounting required'
                        for field in ('lease', 'startedMono', 'startedAt', 'bootEpoch'):
                            row.pop(field, None)
                    row.update(state='blocked', reason=reason)
                    recovered.append(identity)
            scopes_dirty |= bool(released)
            state_dirty |= bool(recovered)
            if at > owner.get('finishedAt', 0):
                owner['finishedAt'] = at
                owners_dirty = True
            outcomes[key] = dict(owner=name, released=released, blockedForReview=recovered)
        if state_dirty:
            atomic_json(root.parent / 'state.json', state)
        if scopes_dirty:
            atomic_json(root / 'scopes.json', claims)
        if owners_dirty:
            atomic_json(root / 'owners.json', owners)
    return outcomes


def lifecycle(repo, args):
    return apply_owner_events(directory(repo), [(None, vars(args))])[None]


def drain_events(repo):
    inbox = directory(repo) / 'owner-events'
    if not inbox.is_dir() or next(inbox.glob('*.json'), None) is None:
        return {}
    with Locked(str(inbox)):
        # New filenames index occurrence time. Older UUID-only events remain readable.
        outcomes = {}
        now = time.time()
        def quarantine(file):
            folder = inbox / 'invalid'
            folder.mkdir(exist_ok=True)
            dest = folder / file.name
            if dest.exists():
                dest = folder / (uuid.uuid4().hex + '-' + file.name)
            os.replace(file, dest)
            outcomes[file.name] = {'state': 'quarantined', 'file': dest.name}

        def pending():
            for file in inbox.glob('*.json'):
                timestamp, separator, _ = file.stem.partition('_')
                try:
                    event = None if separator else load_json(file, {})
                    if not separator:
                        check_event(event, now)
                    at = float(timestamp) if separator else event['at']
                    if not 0 < at <= now + 60:
                        raise ValueError('Invalid time index')
                except (ValueError, UnicodeError):
                    quarantine(file)
                    continue
                yield at, file.name, event
        batch = []
        for at, name, event in heapq.nsmallest(64, pending(), key=lambda item: item[:2]):
            if event is None:
                try:
                    event = load_json(inbox / name, {})
                    check_event(event, now)
                    if event['at'] != at:
                        raise ValueError('Invalid time index')
                except (ValueError, UnicodeError):
                    quarantine(inbox / name)
                    continue
            batch.append((name, event))
        if not batch:
            return outcomes
        outcomes.update(apply_owner_events(inbox.parent, batch))
        for name, _ in batch:
            (inbox / name).unlink(missing_ok=True)
        return outcomes


def tick(repo, trigger="agent"):
    drain_events(repo)
    root = directory(repo)
    config = load_json(root / 'config.json', {})
    if not config.get('enabled'):
        return dict(queued=False, reason='not-configured')
    if config.get('mode') == 'hub-weekly' and trigger != 'hub-start':
        return dict(queued=False, reason='hub-start-only')
    claims = load_json(root / 'scopes.json', {})
    if 'queue-maintenance' in claims:
        return dict(queued=False, reason='scope-held')
    # SQL/definition reads and scheduling are outside both mutation locks. A real take always
    # repeats admission against current definitions. Fence local ownership changes before sending.
    state = load_json(root.parent / 'state.json', {})
    queue = view(repo, machine_id())
    if not queue['next']:
        return dict(queued=False, reason='running-or-empty')
    offer = queue.get('parallelOffer')
    receiver = offer['receiver'] if offer else config['threadId']
    dispatch = root / ('dispatch-' + receiver + '.json' if offer else 'dispatch.json')
    with Locked(str(root.parent)), Locked(str(root)):
        if (config != load_json(root / 'config.json', {}) or claims != load_json(root / 'scopes.json', {})
                or state != load_json(root.parent / 'state.json', {})):
            return dict(queued=False, reason='queue-changed')
        previous = load_json(dispatch, {})
        if config.get('mode') == 'hub-weekly':
            last_work = max(float(config.get('lastProcessedAt', 0)), float(state.get('lastWorkedAt', 0)))
            weekly_dispatch = previous.get('at', 0) if previous.get('mode') == 'hub-weekly' and (previous.get('queued') or previous.get('state') == 'sending' or previous.get('reason') == 'cli-timeout-uncertain') else 0
            if time.time() - max(last_work, weekly_dispatch) < 7 * 24 * 60 * 60:
                return dict(queued=False, reason='weekly-not-due', lastWorkedAt=last_work)
        if time.time() - previous.get('at', 0) < 900 and (not offer or previous.get('offer') == offer):
            return dict(queued=False, reason='wake-pending')
        ticket = uuid.uuid4().hex
        pending = dict(ticket=ticket, at=time.time(), state='sending', mode=config.get('mode', 'standard'))
        if offer:
            pending['offer'] = offer
        atomic_json(dispatch, pending)
    take_command = 'tools/todo_queue.py take --owner <현재 세션 ID>'
    if offer:
        take_command += ' --coordinator ' + receiver + ' --ticket ' + ticket
        take_command += (f" (업무 {offer['id']}. 하위 Codex·Claude가 실행하면 해당 작업자가 "
                         "자신의 실제 고유 ID로 같은 명령을 직접 사용) ")

    message = (f'공유 PC 큐 후속 실행 요청 {ticket}. 저장소 {repo}. '
               'AGENTS.md와 docs/todo-queue.md를 읽으세요. 먼저 tools/todo_worker.py scope status로 '
               f'다른 소유자의 작업 범위를 확인하고, {take_command}로 '
               '원자적 lease를 얻은 작업만 진행하세요. 자신의 일치하는 lease를 얻지 못했다면 실행하지 마세요. '
               '기존 승인 범위 안에서 안전한 중단점까지 처리하고 yield 또는 근거가 있는 complete를 하세요. '
               '승인 대기는 yield --blocked. 빌드·테스트 금지. 완료 후 안전한 더미 정리 목록을 만들고 '
               '그 목록에 대한 승인이 있을 때만 자동 삭제하세요. 백업과 기능 의존 파일을 보존하세요. '
               '변화나 실행 가능한 업무가 없으면 조용히 끝내고 의미 있는 진행·실패·사용자 조치만 알리세요.')
    try:
        result = subprocess.run([config['codex'], 'queue', '--thread', receiver, '--message', message],
                                cwd=repo, capture_output=True, timeout=60,
                                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        outcome = dict(queued=result.returncode == 0, reason='sent' if result.returncode == 0 else 'cli-failed')
    except subprocess.TimeoutExpired:
        outcome = dict(queued=False, reason='cli-timeout-uncertain')
    except OSError:
        outcome = dict(queued=False, reason='cli-unavailable')
    with Locked(str(root)):
        pending = load_json(dispatch, {})
        if pending.get('ticket') == ticket:
            pending.pop('state', None)
            atomic_json(dispatch, {**pending, **outcome})
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default=str(Path(__file__).resolve().parents[1]))
    commands = parser.add_subparsers(dest='command', required=True)
    tick_parser = commands.add_parser('tick')
    tick_parser.add_argument('--trigger', choices=['agent', 'hub-start', 'hub-timer'], default='agent')
    for action in ('owner-active', 'owner-finished'):
        event = commands.add_parser(action)
        event.add_argument('--owner', required=True)
        event.add_argument('--at', type=float, required=True, help='Time of the observed event, not delivery time')
        if action == 'owner-finished':
            event.add_argument('--confirmed-stopped', action='store_true', required=True)
    commands.add_parser('event-status', help='Inspect retained malformed event filenames without applying them')
    setup = commands.add_parser('configure')
    setup.add_argument('--thread', required=True)
    setup.add_argument('--codex', required=True)
    setup.add_argument('--mode', choices=['standard', 'hub-weekly'], help='Keep the current machine mode when omitted')
    modes = setup.add_mutually_exclusive_group()
    modes.add_argument('--parallel-policy', type=Path, help='Reviewed local machine, job fingerprints and resources JSON')
    modes.add_argument('--serial', action='store_true', help='Remove the optional local parallel policy')
    claim = commands.add_parser('scope')
    claim.add_argument('action', choices=['status', 'claim', 'renew', 'release'])
    claim.add_argument('--scope', default='queue-maintenance')
    claim.add_argument('--owner')
    claim.add_argument('--token')
    args = parser.parse_args()
    repo = Path(args.repo).resolve(strict=True)
    if args.command in ('owner-active', 'owner-finished'):
        if not args.owner or len(args.owner) > 100:
            raise ValueError('Explicit bounded owner identity required')
        if not (0 < args.at <= time.time() + 60):
            raise ValueError('Invalid lifecycle event time')
        inbox = directory(repo) / 'owner-events'
        file = inbox / f'{args.at!r}_{uuid.uuid4().hex}.json'
        atomic_json(file, dict(command=args.command, owner=args.owner, at=args.at))
        outcomes = drain_events(repo)
        result = outcomes.get(file.name, dict(owner=args.owner, eventQueued=file.exists()))
    elif args.command == 'event-status':
        invalid = directory(repo) / 'owner-events' / 'invalid'
        files = sorted(p.name for p in invalid.glob('*.json'))
        result = dict(quarantined=len(files), files=files[:64])
    elif args.command == 'configure':
        uuid.UUID(args.thread)
        executable = Path(args.codex).resolve(strict=True)
        root = directory(repo)
        policy = None
        if args.parallel_policy:
            with args.parallel_policy.open('rb') as source:
                data = source.read(65537)
            if len(data) > 65536:
                raise ValueError('Parallel policy exceeds 64 KiB')
            policy = json.loads(data)
            if not isinstance(policy, dict) or policy.get('machine') != machine_id():
                raise ValueError('Parallel policy must explicitly name this PC')
        with Locked(str(root.parent)), Locked(str(root)):
            current = load_json(root / 'config.json', {})
            config = {**current, 'enabled': True, 'threadId': args.thread, 'codex': str(executable)}
            if args.mode:
                config['mode'] = args.mode
            if args.serial:
                config.pop('parallel', None)
            if policy is not None:
                config['parallel'] = policy
                if not parallel_settings(config):
                    raise ValueError('Enrolling parallel workers requires standard mode')
            if config.get('parallel'):
                parallel_settings(config)
            routing_keys = ('threadId', 'mode', 'parallel')
            if any(current.get(k) != config.get(k) for k in routing_keys):
                state = state_for(root.parent / 'state.json', time.time())
                if any(r['state'] == 'running' for r in state['jobs'].values()):
                    raise ValueError('Drain running leases before changing local worker routing')
            if config != current:
                atomic_json(root / 'config.json', config)
        result = dict(configured=True, machine=machine_id())
    elif args.command == 'scope':
        if not re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,79}', args.scope):
            raise ValueError('Invalid scope')
        if args.action != 'status' and (not args.owner or len(args.owner) > 100):
            raise ValueError('Explicit owner is required')
        result = scope(repo, args)
    else:
        result = tick(repo, args.trigger)
    print(json.dumps(result, ensure_ascii=True))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, TimeoutError) as error:
        raise SystemExit('todo_worker: ' + str(error))


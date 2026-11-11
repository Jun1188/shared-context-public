"""Regression checks for batching, recovery and old event filenames in isolated local queues."""
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import todo_worker as worker
from todo_definitions import projection_journal


class OwnerEventTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.root = self.repo / 'worker'
        self.inbox = self.root / 'owner-events'
        self.inbox.mkdir(parents=True)
        directory = patch.object(worker, 'directory', return_value=self.root)
        directory.start()
        self.addCleanup(directory.stop)
        self.at = time.time() - 20

    def event(self, index, command, name, at, legacy=False):
        filename = ('' if legacy else repr(at) + '_') + f'{index:032x}.json'
        (self.inbox / filename).write_text(json.dumps(dict(command=command, owner=name, at=at)))
        return filename

    def test_idle_and_active_bursts_touch_only_needed_files(self):
        with patch.object(worker, 'Locked', wraps=worker.Locked) as locks:
            self.assertEqual(worker.drain_events(self.repo), {})
            self.assertEqual(locks.call_count, 0)
            for i in range(75):
                self.event(i, 'owner-active', 'agent', self.at - i, legacy=(i == 74))
            with patch.object(worker, 'atomic_json', wraps=worker.atomic_json) as writes:
                self.assertEqual(len(worker.drain_events(self.repo)), 64)
                self.assertEqual(writes.call_count, 1)
                self.assertEqual(locks.call_count, 2)
            self.assertEqual(worker.load_json(self.root / 'owners.json', {})['agent']['activeAt'],
                             self.at - 11)
            self.assertEqual(len(worker.drain_events(self.repo)), 11)

    def test_interrupted_commit_replays_without_double_accounting(self):
        row = dict(state='running', owner='one', lease='owned', level=0, used=0, vruntime=0, service=0,
                   bootEpoch=time.time() - time.monotonic(), startedAt=self.at - 5,
                   startedMono=time.monotonic() - 25, heartbeatAt=self.at - 1)
        state = dict(schema=1, floor=0, boostAt=self.at, jobs={'TASK': row})
        worker.atomic_json(self.repo / 'state.json', state)
        worker.atomic_json(self.root / 'scopes.json', {'a': dict(owner='one', heartbeatAt=self.at - 1),
                                                       'b': dict(owner='two', heartbeatAt=self.at)})
        self.event(0, 'owner-finished', 'one', self.at, legacy=True)
        write = worker.atomic_json
        def fail_on_scopes(path, value):
            if path.name == 'scopes.json':
                raise OSError('simulated interrupted write')
            write(path, value)
        with patch.object(worker, 'atomic_json', side_effect=fail_on_scopes):
            with self.assertRaises(OSError):
                worker.drain_events(self.repo)
        self.assertEqual(len(list(self.inbox.glob('*.json'))), 1)
        charged = worker.load_json(self.repo / 'state.json', {})['jobs']['TASK']
        self.assertEqual((charged['state'], charged['service']), ('blocked', 5))
        worker.drain_events(self.repo)
        self.assertFalse(list(self.inbox.glob('*.json')))
        self.assertEqual(worker.load_json(self.repo / 'state.json', {})['jobs']['TASK'], charged)
        self.assertEqual(set(worker.load_json(self.root / 'scopes.json', {})), {'b'})
        self.assertEqual(worker.load_json(self.root / 'owners.json', {})['one']['finishedAt'], self.at)
        worker.atomic_json(self.root / 'scopes.json', {'new-scope': dict(owner='one', heartbeatAt=self.at)})
        retry = self.event(1, 'owner-finished', 'one', self.at)
        self.assertEqual(worker.drain_events(self.repo)[retry]['reason'], 'already-finished')
        self.assertIn('new-scope', worker.load_json(self.root / 'scopes.json', {}))

    def test_sqlite_wal_fix_version_families(self):
        for version in ((3, 44, 5), (3, 45, 4), (3, 50, 4), (3, 51, 2)):
            self.assertEqual(projection_journal(version), 'DELETE')
        for version in ((3, 44, 6), (3, 50, 7), (3, 51, 3), (3, 52, 0)):
            self.assertEqual(projection_journal(version), 'WAL')

    def test_malformed_events_preserved_while_other_owners_advance(self):
        broken = self.inbox / 'legacy.json'
        broken.write_bytes(b'{incomplete')
        bad = self.event(2, 'bogus', 'never-release', self.at)
        good = self.event(3, 'owner-active', 'active', self.at)
        outcomes = worker.drain_events(self.repo)
        self.assertEqual(outcomes[good]['state'], 'active')
        self.assertEqual(outcomes[bad]['state'], 'quarantined')
        self.assertEqual((self.inbox / 'invalid' / broken.name).read_bytes(), b'{incomplete')
        self.assertNotIn('never-release', worker.load_json(self.root / 'owners.json', {}))


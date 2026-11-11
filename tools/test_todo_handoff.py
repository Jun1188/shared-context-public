"""Handoff publication races, using only temporary local PC state and a local Git repository."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import todo_handoff as handoff
import todo_queue


class HandoffPublicationTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = Path(tmp.name) / 'repo'
        self.repo.mkdir()
        profile = str(Path(tmp.name) / 'home')
        Path(profile).mkdir()
        environment = patch.dict(os.environ, {
            'COMPUTERNAME': 'BOX-A', 'HOME': profile, 'USERPROFILE': profile, 'LOCALAPPDATA': profile,
            'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1', 'PYTHONDONTWRITEBYTECODE': '1',
        })
        environment.start()
        self.addCleanup(environment.stop)
        (self.repo / 'TODO.md').write_text(
            '<!-- shared-todo-queue:start -->\n| JOB-ONE | BOX-A,BOX-B | Synthetic | ready | docs.md |\n'
            '<!-- shared-todo-queue:end -->\n', encoding='utf8')
        (self.repo / 'docs.md').write_bytes(b'Fixture evidence\n')
        self.task = todo_queue.tasks(self.repo)[0]
        self.lease = self.queue('take', '--owner', 'writer')['lease']

    def queue(self, *args, machine='BOX-A'):
        env = dict(os.environ, COMPUTERNAME=machine)
        output = subprocess.check_output([sys.executable, '-B', str(Path(todo_queue.__file__).resolve()),
                                          '--repo', str(self.repo), *args], cwd=self.repo, env=env, timeout=10)
        return json.loads(output)

    def command(self, action, *args, machine='BOX-A', lease=None):
        argv = ['todo_handoff.py', '--repo', str(self.repo), '--machine', machine, action, self.task['id'],
                '--actor', 'writer', '--definition', self.task['fingerprint']]
        if action != 'read':
            argv.extend(['--lease', lease or self.lease])
        with patch.object(sys, 'argv', [*argv, *args]):
            return handoff.cli()

    def save(self, *options, **kwargs):
        return self.command('save', '--parent', 'none', '--summary', 'Initial context', '--next', 'Review',
                            '--evidence', 'docs.md', *options, **kwargs)

    def test_lease_checked_before_preparation_and_again_before_publish(self):
        with patch.object(handoff, 'evidence_files', side_effect=AssertionError('stale lease prepared evidence')):
            with self.assertRaisesRegex(ValueError, 'Active owner, lease'):
                self.save(lease='invalid')
        read_evidence = handoff.evidence_files
        def release_while_preparing(*args):
            refs = read_evidence(*args)
            result = self.queue('yield', self.task['id'], '--lease', self.lease, '--blocked', 'Fixture pause')
            self.assertEqual(result['state'], 'blocked')
            return refs
        with patch.object(handoff, 'evidence_files', side_effect=release_while_preparing):
            with self.assertRaisesRegex(ValueError, 'Active owner, lease'):
                self.save()
        self.assertIsNone(self.command('read')['handoff'])

    def test_foreign_pc_can_read_but_cannot_publish_or_ack_with_a_valid_foreign_lease(self):
        foreign = self.queue('take', '--owner', 'writer', machine='BOX-B')['lease']
        self.assertIsNone(self.command('read', machine='BOX-B')['handoff'])
        with self.assertRaisesRegex(ValueError, 'only for this PC'):
            self.save(machine='BOX-B', lease=foreign)
        with self.assertRaisesRegex(ValueError, 'only for this PC'):
            self.command('ack', '--revision', '1'*64, machine='BOX-B', lease=foreign)

    def test_two_children_contending_for_the_same_parent_publish_one_successor(self):
        common = self.save()['revision']
        gate = threading.Barrier(2)
        def submit(child):
            gate.wait(timeout=5)
            try:
                saved = handoff.save_handoff(self.repo, 'BOX-A', self.task, actor=child, parent=common,
                                             summary=child, next_action='Review', decisions=[child])
                return saved['revision']
            except ValueError as exc:
                self.assertIn('Stale', str(exc))
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            winners = [r for r in pool.map(submit, ['child-a','child-b']) if r]
        self.assertEqual(len(winners), 1)
        tip = self.command('read')['handoff']
        self.assertEqual((tip['revision'], tip['parent'], tip['decisions']), (winners[0], common, [tip['actor']]))

    def test_shared_git_work_precedes_queue_lock_and_publication_holds_it(self):
        for args in [('init','-q'), ('add','--','docs.md'),
                     ('-c','user.name=Fixture','-c','user.email=test@example.invalid','commit','-qm','fixture')]:
            subprocess.run(['git','-C',str(self.repo),*args], check=True, capture_output=True)
        central = self.repo / '.local/todo-queue/BOX-A'
        real_lock, real_popen, real_atom = handoff.Locked, subprocess.Popen, handoff.atom
        held = set()
        git_calls, publications = [], []
        @contextmanager
        def observed_lock(directory):
            path = Path(directory)
            with real_lock(directory):
                held.add(path)
                try:
                    yield
                finally:
                    held.remove(path)
        def observed_popen(args, *rest, **kwargs):
            if Path(args[0]).stem.lower() == 'git':
                self.assertNotIn(central, held)
                git_calls.append(args[0])
            return real_popen(args, *rest, **kwargs)
        def observed_write(path, data):
            self.assertIn(central, held)
            publications.append(path)
            return real_atom(path, data)
        with patch.object(handoff, 'Locked', observed_lock), patch.object(subprocess, 'Popen', observed_popen), \
                patch.object(handoff, 'atom', observed_write):
            saved = self.save('--audience','shared','--confirm-non-sensitive')
        self.assertTrue(git_calls)
        self.assertEqual([p.stem for p in publications], [saved['revision']])
        self.assertEqual(self.command('read', '--audience','shared')['handoff']['revision'], saved['revision'])


if __name__ == '__main__':
    unittest.main()


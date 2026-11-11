"""Synthetic check: two PCs finalizing the same task after receipt sync must write identical files.

Run from a checkout's tools folder (needs work_table.py beside todo_queue.py):
    python test_todo_queue_finalize.py
Uses only temporary folders; no real queue, state or remote is touched.
"""
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import todo_queue as q

PCS = ('PC-A', 'PC-B')


def make_repo(root, extra_rows=''):
    repo = Path(root)
    (repo / 'docs').mkdir(parents=True)
    (repo / 'docs/detail.md').write_text('detail\n', encoding='utf-8')
    (repo / 'evidence.md').write_text('done\n', encoding='utf-8')
    row = '| SYN-TASK-1 | PC-A,PC-B | synthetic | 대기 | [상세](docs/detail.md) |\n'
    (repo / 'TODO.md').write_text('# q\n' + q.START + '\n| ID | PC | 요약 | 상태 | 상세 |\n|---|---|---|---|---|\n'
                                  + row + extra_rows + q.END + '\n', encoding='utf-8-sig')
    task = next(t for t in q.tasks(repo) if t['id'] == 'SYN-TASK-1')
    for index, pc in enumerate(PCS):  # both receipts already synchronized to this clone
        q.atomic_json(repo / 'todo-queue/completed' / (pc + '.json'), {task['id']: dict(
            fingerprint=task['fingerprint'], completedAt=1000.0 + index, evidence='evidence.md', note='', history=[])})
    return repo, task


class FinalizeTest(unittest.TestCase):
    def test_concurrent_finalizers_write_identical_bytes(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            results = {}
            for pc, folder, clock in (('PC-A', a, 5000.0), ('PC-B', b, 9000.0)):
                repo, task = make_repo(folder)
                # The complete path passes a row with local-only keys; they must not reach the archive.
                with mock.patch.object(time, 'time', return_value=clock):
                    out = q.finalize_remote(repo, dict(task, definitionChanged=False), pc)
                self.assertTrue(out['remoteRemoved'])
                results[pc] = ((repo / out['archive']).read_bytes(), (repo / 'TODO.md').read_bytes())
            self.assertEqual(results['PC-A'], results['PC-B'])
            archive = json.loads(results['PC-A'][0].decode('utf-8-sig'))
            self.assertEqual((archive['lastCompleter'], archive['at'], archive['state']), ('PC-B', 1001.0, 'removed'))

    def test_second_finalizer_after_sync_is_a_no_op(self):
        with tempfile.TemporaryDirectory() as folder:
            repo, task = make_repo(folder)
            q.finalize_remote(repo, task, 'PC-A')
            before = (repo / 'TODO.md').read_bytes()
            self.assertEqual(q.finalize_remote(repo, task, 'PC-B'), dict(remoteRemoved=True, alreadyRemoved=True))
            self.assertEqual((repo / 'TODO.md').read_bytes(), before)

    def test_missing_receipt_keeps_the_row(self):
        with tempfile.TemporaryDirectory() as folder:
            repo, task = make_repo(folder)
            (repo / 'todo-queue/completed/PC-B.json').unlink()
            out = q.finalize_remote(repo, task, 'PC-A')
            self.assertEqual((out['remoteRemoved'], out['remainingPCs']), (False, ['PC-B']))
            self.assertFalse((repo / 'todo-queue/archive').exists())


if __name__ == '__main__':
    unittest.main()


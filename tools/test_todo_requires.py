"""Synthetic check of the `[requires: ...]` gate: two made-up PCs, temporary folders only, no model, no wake.

Run from a folder holding todo_queue.py, todo_requires.py and work_table.py:
    python test_todo_requires.py
"""
import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import todo_queue as q
from todo_requires import blocked_reason, requirements

GB = 10 ** 9
LITE = dict(schema='machine-profile/1', formFactor='laptop', ramBytes=8 * GB + 247586816,
            gpus=[dict(name='integrated', vramBytes=0, shared=True)], runtimes=dict(node=True, python='3.12', lmstudio=False))
FULL = dict(schema='machine-profile/1', formFactor='desktop', ramBytes=32 * GB,
            gpus=[dict(name='discrete', vramBytes=12 * GB, shared=False)], runtimes=dict(node=True, python='3.12', lmstudio=True))
ROWS = [('T-OLD', '조건 없는 기존 작업', '대기'),
        ('T-GPU', '로컬 모델 평가 [requires: gpu, vram>=8]', '대기'),
        ('T-UNK', '엔진 작업 [requires: runtime:unity]', '대기'),
        ('T-HELD', '승인 대기 작업', '보류'),
        ('T-NODE', '문서 색인 [requires: runtime:node, ram>=8]', '대기')]


def make_repo(folder, rows=ROWS, targets='PC-LITE,PC-FULL'):
    repo = Path(folder)
    (repo / 'docs').mkdir(parents=True)
    (repo / 'docs/detail.md').write_text('detail\n', encoding='utf-8')
    body = ''.join('| %s | %s | %s | %s | [상세](docs/detail.md) |\n' % (identity, targets, summary, status)
                   for identity, summary, status in rows)
    (repo / 'TODO.md').write_text('# q\n' + q.START + '\n| ID | PC | 요약 | 상태 | 상세 |\n|---|---|---|---|---|\n' + body + q.END + '\n',
                                  encoding='utf-8-sig')
    for name, profile in (('PC-LITE', LITE), ('PC-FULL', FULL)):
        q.atomic_json(repo / 'machines' / (name + '.json'), profile)
    return repo


def take(repo, machine):
    return q.mutate(argparse.Namespace(command='take', owner='synthetic'), repo, machine)


class RequiresTest(unittest.TestCase):
    def test_tokens(self):
        self.assertIsNone(requirements('plain row'))
        self.assertEqual(requirements('x [Requires: GPU , vram>=8] y'), ['gpu', 'vram>=8'])
        self.assertIsNone(blocked_reason('plain row', None))
        self.assertEqual(blocked_reason('[requires: gpu, vram>=8]', LITE), 'requires unmet: gpu, vram>=8')
        self.assertIsNone(blocked_reason('[requires: gpu, vram>=12, ram>=32, form:desktop, runtime:lmstudio]', FULL))
        self.assertEqual(blocked_reason('[requires: vram>=16]', FULL), 'requires unmet: vram>=16')
        self.assertIsNone(blocked_reason('[requires: ram>=8]', LITE))  # an "8 GB" PC reports about 8.25e9 bytes
        self.assertEqual(blocked_reason('[requires: runtime:unity]', FULL), 'requires uncertain: runtime:unity')
        self.assertEqual(blocked_reason('[requires: runtime:lmstudio]', LITE), 'requires unmet: runtime:lmstudio')
        self.assertEqual(blocked_reason('[requires: fast]', FULL), 'requires uncertain: fast')
        self.assertEqual(blocked_reason('[requires: gpu, fast]', LITE), 'requires unmet: gpu')  # certain failure wins
        self.assertEqual(blocked_reason('[requires: gpu]', None), 'requires uncertain: no machine profile for this PC')
        self.assertEqual(blocked_reason('[requires: ]', FULL), 'requires uncertain: empty tag')
        self.assertEqual(blocked_reason('[requires: ram>=8]', dict(schema='machine-profile/1')), 'requires uncertain: ram>=8')

    def test_view_per_pc(self):
        with tempfile.TemporaryDirectory() as folder:
            repo = make_repo(folder)
            lite, full, none = (q.view(repo, name) for name in ('PC-LITE', 'PC-FULL', 'PC-NONE'))
            self.assertEqual([row['id'] for row in lite['ready']], ['T-OLD', 'T-NODE'])
            self.assertEqual({row['id']: row['reason'] for row in lite['blocked']},
                             {'T-GPU': 'requires unmet: gpu, vram>=8', 'T-UNK': 'requires uncertain: runtime:unity',
                              'T-HELD': 'Shared queue is on hold'})
            self.assertEqual([row['id'] for row in full['ready']], ['T-OLD', 'T-GPU', 'T-NODE'])
            self.assertEqual(sorted(row['id'] for row in full['blocked']), ['T-HELD', 'T-UNK'])
            self.assertEqual((none['ready'], none['blocked']), ([], []))  # not a target: nothing applies
            text = json.dumps(lite, ensure_ascii=False)
            self.assertNotIn('integrated', text)

    def test_unfit_only_queue_wakes_nothing(self):
        with tempfile.TemporaryDirectory() as folder:
            repo = make_repo(folder, rows=[ROWS[1]])
            # view() only computes `next` for the PC it runs on, so each synthetic PC takes a turn as "this PC".
            with mock.patch.dict(os.environ, COMPUTERNAME='PC-LITE'):
                lite = q.view(repo, 'PC-LITE')
            with mock.patch.dict(os.environ, COMPUTERNAME='PC-FULL'):
                full = q.view(repo, 'PC-FULL')
            self.assertEqual((lite['previewOnly'], lite['next'], lite['totalReady']), (False, None, 0))  # tick stops on a falsy next
            self.assertEqual((full['previewOnly'], full['next'], full['totalReady']), (False, 'T-GPU', 1))
        with tempfile.TemporaryDirectory() as folder:
            repo = make_repo(folder, rows=[ROWS[1], ROWS[4]])
            with mock.patch.dict(os.environ, COMPUTERNAME='PC-LITE'):
                self.assertEqual(q.view(repo, 'PC-LITE')['next'], 'T-NODE')

    def test_take_skips_unfit_and_keeps_order(self):
        for machine, expected in (('PC-LITE', 'T-OLD'), ('PC-FULL', 'T-OLD')):
            with tempfile.TemporaryDirectory() as folder:
                repo = make_repo(folder)
                self.assertEqual(take(repo, machine)['id'], expected)
        with tempfile.TemporaryDirectory() as folder:
            repo = make_repo(folder, rows=[ROWS[1], ROWS[4]])
            taken = take(repo, 'PC-LITE')
            self.assertEqual(taken['id'], 'T-NODE')  # T-GPU is first in the file but cannot run here
            state = q.load_json(repo / '.local/todo-queue/PC-LITE/state.json', {})
            self.assertEqual(state['jobs']['T-GPU']['state'], 'ready')  # the gate is not written into local state
            self.assertEqual(state['jobs']['T-GPU']['order'], 1)
        with tempfile.TemporaryDirectory() as folder:
            repo = make_repo(folder, rows=[ROWS[1]])
            self.assertEqual(take(repo, 'PC-LITE'), dict(state='empty', machine='PC-LITE'))
            self.assertEqual(take(repo, 'PC-FULL')['id'], 'T-GPU')

    def test_rows_without_tags_are_unchanged(self):
        with tempfile.TemporaryDirectory() as folder:
            repo = make_repo(folder, rows=[('T-A', 'a', '대기'), ('T-B', 'b', '대기')])
            for name in ('PC-LITE', 'PC-FULL'):
                (repo / 'machines' / (name + '.json')).unlink()  # even without any profile
                out = q.view(repo, name)
                self.assertEqual(([row['id'] for row in out['ready']], out['blocked'], out['next'] if not out['previewOnly'] else None),
                                 (['T-A', 'T-B'], [], None))


if __name__ == '__main__':
    os.environ['COMPUTERNAME'] = 'PC-OTHER'  # every synthetic PC is then a read-only preview in view()
    unittest.main()


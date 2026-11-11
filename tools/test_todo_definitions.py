"""Isolated real CLI and Git tests for the Markdown-to-ledger transition."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from todo_definitions import projection_journal

SCRIPT = Path(__file__).with_name('todo_queue.py').resolve()
FIXTURE = '''# Queue
<!-- shared-todo-queue:start -->
## Tasks
| ID | PC | Summary | Status | Detail |
|---|---|---|---|---|
| SHARED-TASK | PC-A,PC-B | Both | ready | [details](evidence.md) |
| HEAVY-TASK | PC-A,PC-B | Advanced [requires: runtime:node] | ready | [details](evidence.md) |
<!-- shared-todo-queue:end -->
Keep this note.
'''


def cli(repo,pc,*args,success=True):
    env = dict(os.environ,COMPUTERNAME=pc, PYTHONUTF8='1', PYTHONDONTWRITEBYTECODE='1')
    p = subprocess.run([sys.executable,'-B',str(SCRIPT),'--repo',str(repo),*args],env=env,
                       capture_output=True,encoding='utf-8',timeout=30)
    if success:
        if p.returncode:
            raise AssertionError((args,p.stderr))
        return json.loads(p.stdout)
    if not p.returncode:
        raise AssertionError(('unexpected success',args))
    return p.stderr


def git(repo,*args):
    return subprocess.check_output(['git','-c','core.autocrlf=false','-c','user.name=Fixture',
                                   '-c','user.email=fixture@example.invalid','-C',str(repo),*args],
                                  stderr=subprocess.STDOUT,timeout=30)


class LedgerTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.repo = self.root/'a'
        self.repo.mkdir()
        (self.repo/'TODO.md').write_text(FIXTURE,encoding='utf-8')
        (self.repo/'evidence.md').write_text('Synthetic evidence\n',encoding='utf-8')
        (self.repo/'machines').mkdir()
        for pc in ('PC-A','PC-B'):
            (self.repo/'machines'/f'{pc}.json').write_text(json.dumps({'runtimes':{'node':pc=='PC-A'}}))
        cli(self.repo,'PC-A','migrate-state')

    def test_order_receipts_filters_add_render_and_fenced_completion(self):
        def view():
            result = cli(self.repo,'PC-A','show')
            result.pop('source')
            return result
        baseline = view()
        seed = cli(self.repo,'PC-A','migrate-definitions')
        self.assertEqual(seed['definitions'],2)
        self.assertEqual(view(),baseline)
        db = self.repo/'.local/todo-queue/PC-A/definitions.sqlite3'
        with closing(sqlite3.connect(db)) as connection:
            self.assertEqual(connection.execute('PRAGMA journal_mode').fetchone(),
                             (projection_journal(sqlite3.sqlite_version_info).lower(),))
            self.assertEqual(connection.execute('SELECT count(*) FROM needs').fetchone(),(4,))
            self.assertEqual(connection.execute('SELECT count(*) FROM requires').fetchone(),(1,))
        args = ('add','NEW-TASK','--targets','PC-A','--summary','Example','--detail','evidence.md','--requires','runtime:node')
        self.assertTrue(cli(self.repo,'PC-A',*args)['registered'])
        journal = self.repo/'todo-queue/definitions/pc-PC-A.jsonl'
        prefix = journal.read_bytes()
        self.assertTrue(cli(self.repo,'PC-A',*args)['alreadyRegistered'])
        self.assertEqual(journal.read_bytes(),prefix)
        self.assertEqual(len(cli(self.repo,'PC-A','show','--machine','PC-B')['ready']),1)
        cli(self.repo,'PC-A','render-definitions')
        self.assertIn('NEW-TASK', (self.repo/'TODO.md').read_text(encoding='utf-8-sig'))
        self.assertIn('Keep this note.', (self.repo/'TODO.md').read_text(encoding='utf-8-sig'))
        record = cli(self.repo,'PC-A','take','--owner','fixture')
        self.assertEqual(record['id'],'SHARED-TASK')
        self.assertIn('lease',cli(self.repo,'PC-A','heartbeat',record['id'],'--lease','wrong',success=False))
        cli(self.repo,'PC-A','yield',record['id'],'--lease',record['lease'])
        self.assertIn('lease',cli(self.repo,'PC-A','complete',record['id'],'--lease',record['lease'],
                                  '--evidence','evidence.md','--note','late',success=False))
        # The original CLI permits a manual evidence-backed completion without a lease when no worker owns it.
        cli(self.repo,'PC-A','complete',record['id'],'--evidence','evidence.md','--note','ok')
        self.assertEqual(cli(self.repo,'PC-A','show')['completedExcluded'],1)
        cli(self.repo,'PC-A','add','ANOTHER-TASK','--targets','PC-B','--summary','More','--detail','evidence.md')
        self.assertTrue(journal.read_bytes().startswith(prefix))

    def test_explicit_machine_capabilities_gate_each_target(self):
        cli(self.repo,'PC-A','migrate-definitions')
        for pc,known in [('PC-A',True),('PC-B',False)]:
            folder = self.repo/'machines'/pc
            folder.mkdir()
            (folder/'capabilities.json').write_text(json.dumps({'schemaVersion':1,'machine':pc,
                                                              'capabilities':{'hub:full':known,'unity':known}}))
        cli(self.repo,'PC-A','add','FEATURE-TASK','--targets','PC-A,PC-B','--summary','Smoke',
            '--requires','hub:full','--requires','unity','--detail','evidence.md')
        self.assertTrue(any(r['id']=='FEATURE-TASK' for r in cli(self.repo,'PC-A','show')['ready']))
        self.assertTrue(any(r['id']=='FEATURE-TASK' for r in cli(self.repo,'PC-A','show','--machine','PC-B')['blocked']))

    def test_authoritative_source_and_corrupt_log(self):
        cli(self.repo,'PC-A','migrate-definitions')
        seed = self.repo/'todo-queue/definitions/bootstrap.jsonl'
        data = seed.read_bytes()
        (self.repo/'TODO.md').write_text('Independent human note with no queue',encoding='utf-8')
        cli(self.repo,'PC-A','migrate-definitions')
        self.assertEqual(seed.read_bytes(),data)
        self.assertEqual(len(cli(self.repo,'PC-A','remote')['tasks']),2)
        seed.write_bytes(data.replace(b'Both',b'Forged'))
        self.assertIn('hash chain',cli(self.repo,'PC-A','show',success=False))
        seed.write_bytes(data[:-2])
        self.assertIn('Incomplete',cli(self.repo,'PC-A','show',success=False))
        seed.unlink()
        self.assertIn('missing',cli(self.repo,'PC-A','show',success=False))

    def test_branches_merge_concurrent_adds_receipts_and_finalizers(self):
        a,b = self.repo,self.root/'b'
        cli(a,'PC-A','migrate-definitions')
        (a/'.gitignore').write_text('.local/\n')
        git(a,'init','-b','pc-a')
        git(a,'add','.')
        git(a,'commit','-qm','seed')
        git(a,'worktree','add','-qb','pc-b',str(b))
        cli(b,'PC-B','migrate-state')
        def finish(pair):
            repo,pc=pair
            cli(repo,pc,'add','ADD-'+pc,'--targets','PC-A,PC-B','--summary',pc,'--detail','evidence.md')
            task = cli(repo,pc,'take','--owner',pc)
            self.assertEqual(task['id'],'SHARED-TASK')
            result = cli(repo,pc,'complete',task['id'],'--lease',task['lease'],'--evidence','evidence.md','--note','ok')
            self.assertFalse(result['remoteRemoved'])
            git(repo,'add','todo-queue')
            git(repo,'commit','-qm','local additions and receipt')
        pairs = [(a,'PC-A'),(b,'PC-B')]
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(finish,pairs))
        git(a,'merge','--no-edit','pc-b')
        git(b,'merge','--ff-only','pc-a')
        self.assertEqual(cli(a,'PC-A','remote'),cli(b,'PC-B','remote'))
        def finalize(pair):
            repo,pc = pair
            self.assertTrue(cli(repo,pc,'reconcile','SHARED-TASK','--last-completer')['remoteRemoved'])
            git(repo,'add','todo-queue')
            git(repo,'commit','-qm','finalize')
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(finalize,pairs))
        git(a,'merge','--no-edit','pc-b')
        git(b,'merge','--ff-only','pc-a')
        result = cli(b,'PC-B','remote')
        self.assertEqual(result,cli(a,'PC-A','remote'))
        self.assertEqual([t['id'] for t in result['tasks']],['HEAVY-TASK','ADD-PC-A','ADD-PC-B'])
        self.assertEqual(git(a,'diff','--name-only','--diff-filter=U'),b'')
        self.assertEqual((a/'TODO.md').read_text(),FIXTURE)

    def test_render_preserves_new_work_from_an_older_pc(self):
        cli(self.repo, 'PC-A', 'migrate-definitions')
        file = self.repo / 'TODO.md'
        file.write_text(FIXTURE.replace('Both |', 'Updated from an older PC |'), encoding='utf-8')
        original = file.read_bytes()
        self.assertIn('Unimported Markdown edit', cli(self.repo, 'PC-A', 'render-definitions', success=False))
        self.assertEqual(original, file.read_bytes())


if __name__=='__main__':
    unittest.main()


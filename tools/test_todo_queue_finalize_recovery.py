import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import todo_queue as q
from test_todo_queue_finalize import make_repo

HERE=Path(__file__).resolve().parent
PC='PC-A'

def make(test,structured=False):
    tmp=tempfile.TemporaryDirectory(dir=HERE)
    test.addCleanup(tmp.cleanup)
    repo,task=make_repo(tmp.name)
    if structured:
        q.ledger.migrate(repo,PC,q.tasks(repo),(repo/'TODO.md').read_bytes())
    q.atomic_json(q.paths(repo,PC)[0]/'migration.json',{})
    return repo,task

def rec(repo,task,pc=PC):
    return q.load_json(q.paths(repo,pc)[1],{})[task['id']]

def cli(repo,task,action,ok=True):
    cmd=[sys.executable,str(HERE/'todo_queue.py'),'--repo',str(repo),action,task['id']]
    cmd+=['--last-completer'] if action=='reconcile' else ['--evidence','evidence.md','--note','synthetic retry']
    env={**os.environ,'COMPUTERNAME':PC,'USERPROFILE':str(repo/'test-home'),'LOCALAPPDATA':str(repo/'test-home/AppData/Local'),'PYTHONUTF8':'1'}
    run=subprocess.run(cmd,env=env,capture_output=True,encoding='utf8')
    if (run.returncode==0)!=ok: raise AssertionError((run.stdout,run.stderr))
    return json.loads(run.stdout) if ok else run.stderr

class Recovery(unittest.TestCase):
    def test_crash_before_and_after_retirement_and_after_archive(self):
        for structured in (False,True):
            for phase in ('before','after','archived'):
                for command in ('complete','reconcile'):
                    with self.subTest(structured=structured,phase=phase,command=command):
                        repo,t=make(self,structured)
                        intent,archive=q.finalization_paths(repo,PC,t['id'],t['fingerprint'])
                        old=q.atomic_json
                        def cuts(path,value):
                            if path==archive and phase=='after': raise OSError('interrupted')
                            old(path,value)
                            if path==(intent if phase=='before' else archive) and phase!='after':raise OSError('interrupted')
                        with mock.patch.object(q,'atomic_json',cuts),self.assertRaises(OSError):
                            q.finalize_remote(repo,t,PC)
                        self.assertTrue(intent.is_file())
                        self.assertEqual(archive.is_file(),phase=='archived')
                        self.assertEqual(bool(q.tasks(repo)),phase=='before')
                        self.assertTrue(cli(repo,t,command)['remoteRemoved'])
                        self.assertEqual(q.load_json(archive,{})['state'],'removed')
                        self.assertFalse(intent.exists())
                        self.assertTrue(cli(repo,t,command)['remoteRemoved'])

    def test_different_status_and_receipt_dictionary_order(self):
        outputs=[]
        for pc,status in [('PC-A','review'),('PC-B','blocked by meeting')]:
            repo,t=make(self)
            p=repo/'TODO.md'
            p.write_bytes(p.read_bytes().replace('대기'.encode(),status.encode()))
            history=q.paths(repo,pc)[1]
            data=q.load_json(history,{})
            data[t['id']]=dict(reversed(list(data[t['id']].items())))
            q.atomic_json(history,data)
            path=q.finalize_remote(repo,t,pc)['archive']
            outputs.append((repo/path).read_bytes())
        self.assertEqual(*outputs)
        self.assertIn(b'cross-PC chronology unverified',outputs[0])

    def test_old_prepared_removed_and_missing_or_wrong_records(self):
        for state in ('prepared','removed'):
            repo,t=make(self)
            data=q.finalize_remote(repo,t,PC)
            archive=repo/data['archive']
            legacy=dict(task=t,completedPCs=q.completion_records(repo,t)[0],state=state,removedBy='PC-A',at=9000)
            q.atomic_json(archive,legacy)
            before=archive.read_bytes()
            self.assertTrue(cli(repo,t,'reconcile')['remoteRemoved'])
            self.assertEqual(q.load_json(archive,{})['state'],'removed')
            if state=='removed': self.assertEqual(before,archive.read_bytes())
            q.atomic_json(archive,{**legacy,'completedPCs':{PC:rec(repo,t)}})
            self.assertIn('conflict',cli(repo,t,'complete',False))
            q.atomic_json(archive,legacy)
            (repo/'todo-queue/completed/PC-B.json').unlink()
            self.assertEqual(cli(repo,t,'reconcile')['remainingPCs'],['PC-B'])

    def test_revised_definition_and_no_journal_protected(self):
        repo,t=make(self)
        row=(repo/'TODO.md').read_bytes()
        q.finalize_remote(repo,t,PC)
        (repo/'TODO.md').write_bytes(row.replace(b'synthetic',b'revised'))
        existing=(repo/'TODO.md').read_bytes()
        self.assertIn('Revised task',cli(repo,t,'reconcile',False))
        self.assertIn('current lease',cli(repo,t,'complete',False))
        self.assertEqual(existing,(repo/'TODO.md').read_bytes())
        (repo/'TODO.md').write_text(q.START+'\n'+q.END,encoding='utf8')
        _,archive=q.finalization_paths(repo,PC,t['id'],t['fingerprint'])
        archive.unlink()
        self.assertIn('no local intent',cli(repo,t,'complete',False))

    def test_local_intent_does_not_obstruct_git_fast_forward(self):
        repo,t=make(self)
        (repo/'.gitignore').write_text('.local/\n',encoding='utf8')
        def git(root,*args):return subprocess.check_output(['git','-c','user.name=Fixture','-c','user.email=fixture@example.invalid','-C',str(root),*args],stderr=subprocess.STDOUT)
        git(repo,'init','-q')
        git(repo,'add','.')
        git(repo,'commit','-qm','Synthetic baseline')
        tmp=tempfile.TemporaryDirectory(dir=HERE)
        self.addCleanup(tmp.cleanup)
        peer=Path(tmp.name)
        git(peer,'clone','--quiet','--local',str(repo),'.')
        old=q.atomic_json
        intent,archive=q.finalization_paths(repo,PC,t['id'],t['fingerprint'])
        def crash(path,value):
            old(path,value)
            if path==intent: raise OSError('after preparing local intent')
        with mock.patch.object(q,'atomic_json',crash),self.assertRaises(OSError):
            q.finalize_remote(repo,t,PC)
        self.assertFalse(archive.exists())
        self.assertFalse(git(repo,'status','--porcelain').strip())
        q.finalize_remote(peer,t,'PC-B')
        git(peer,'add','-A')
        git(peer,'commit','-qm','Peer retirement')
        git(repo,'fetch','-q',str(peer),'HEAD')
        git(repo,'merge','--ff-only','FETCH_HEAD')
        received=archive.read_bytes()
        self.assertTrue(cli(repo,t,'reconcile')['alreadyRemoved'])
        self.assertEqual(received,archive.read_bytes())
        self.assertFalse(intent.exists())

if __name__=='__main__':
    unittest.main(verbosity=2)


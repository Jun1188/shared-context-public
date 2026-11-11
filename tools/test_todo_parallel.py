"""Real local CLI processes and synthetic PC profiles. The only wake receiver is a local Python fixture."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

TOOLS = Path(__file__).resolve().parent
HEAVY, SECOND, LAPTOP = 'WORKSTATION-A', 'WORKSTATION-B', 'NOTEBOOK-C'
RECEIVER = '00000000-0000-4000-8000-000000000001'


class ParallelQueueTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.repo = self.root / 'checkout'
        self.repo.mkdir()
        (self.root / 'home').mkdir()
        (self.repo / 'machines').mkdir()
        pcs = ','.join((HEAVY, SECOND, LAPTOP))
        definitions = [
            ('HOLD-ME', 'Hold for explicit approval', '보류'),
            ('UNFIT', 'Cannot run [requires: runtime:missing]', 'ready'),
            ('JOB-A', 'First shared resource', 'ready'), ('JOB-B','Same shared resource','ready'),
            ('JOB-C', 'Independent resource', 'ready'), ('EXCLUSIVE','Unclassified','ready'),
            ('JOB-D','After exclusive','ready')]
        rows = [f'| {key} | {pcs} | {name} | {status} | [detail](evidence.md) |' for key, name, status in definitions]
        (self.repo/'TODO.md').write_text('# Queue\n<!-- shared-todo-queue:start -->\n| ID | PC | Summary | Status | Detail |\n'
                                        '|---|---|---|---|---|\n'+'\n'.join(rows)+'\n<!-- shared-todo-queue:end -->\n',encoding='utf8')
        (self.repo/'evidence.md').write_text('Synthetic evidence only.\n',encoding='utf8')
        (self.repo/'queue').write_text('import sys\nassert sys.argv[1] == "--thread" and sys.argv[3] == "--message"\n'
                                        'with open("received.log","a",encoding="utf8") as f:f.write(sys.argv[2]+" "+sys.argv[4]+"\\n")\n',encoding='utf8')
        for pc in (HEAVY, SECOND, LAPTOP):
            (self.repo/'machines'/f'{pc}.json').write_text('{"runtimes":{"missing":false}}',encoding='utf8')
            self.queue(pc,'migrate-state')
        self.queue(HEAVY,'migrate-definitions')
        for pc in (HEAVY, SECOND, LAPTOP):
            self.queue(pc,'sync-definitions')
        items=self.queue(HEAVY,'show','--json')['ready']
        self.fp={item['id']:item['fingerprint'] for item in items}
        self.configure(HEAVY)
        self.configure(SECOND, parallel=False)
        self.configure(LAPTOP, mode='hub-weekly')

    def run_cli(self, pc, tool, *args, ok=True):
        profile=str(self.root/'home')
        env=dict(os.environ,COMPUTERNAME=pc,HOME=profile,USERPROFILE=profile,LOCALAPPDATA=profile,APPDATA=profile,
                 PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1')
        cmd=[sys.executable,'-B',str(TOOLS/tool),'--repo',str(self.repo),*args]
        result=subprocess.run(cmd,env=env,cwd=self.repo,capture_output=True,encoding='utf8',timeout=40)
        if ok:
            self.assertEqual(result.returncode,0,(args,result.stderr))
            return json.loads(result.stdout)
        self.assertNotEqual(result.returncode,0,(args,result.stdout))
        return result.stderr

    def queue(self, pc, *args, **kwargs):
        return self.run_cli(pc,'todo_queue.py',*args,**kwargs)

    def worker(self, pc, *args, **kwargs):
        return self.run_cli(pc,'todo_worker.py',*args,**kwargs)

    def state(self, pc):
        return self.repo/'.local/todo-queue'/pc

    def configure(self, pc, parallel=True, mode='standard'):
        config={'enabled':True,'codex':sys.executable,'threadId':RECEIVER,'mode':mode}
        if parallel:
            scopes={'JOB-A':['repository'],'JOB-B':['repository'],'JOB-C':['gpu'],'JOB-D':['assets']}
            config['parallel']={'machine':pc,'jobs':{i:{'fingerprint':self.fp[i],'resources':r} for i,r in scopes.items()}}
        f=self.state(pc)/'worker/config.json'
        f.parent.mkdir(exist_ok=True)
        f.write_text(json.dumps(config),encoding='utf8')

    def take(self, owner, *extra, **kwargs):
        return self.queue(HEAVY,'take','--owner',owner,'--coordinator',RECEIVER,*extra,**kwargs)

    def test_eight_processes_disjoint_leases_and_sqlite(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes=list(pool.map(lambda i:self.take(f'child-{i}'),range(8)))
        self.assertEqual(sorted(v['id'] for v in outcomes if 'lease' in v),['JOB-A','JOB-C'])
        self.assertEqual(sum(v.get('state')=='unavailable' for v in outcomes),6)
        self.assertEqual(len({v['lease'] for v in outcomes if 'lease' in v}),2)
        for pc in (HEAVY, SECOND, LAPTOP):
            db=self.state(pc)/'definitions.sqlite3'
            self.assertTrue(db.is_file())
            conn=sqlite3.connect('file:'+db.as_posix()+'?mode=ro',uri=True)
            try:self.assertEqual(conn.execute('pragma integrity_check').fetchone()[0],'ok')
            finally:conn.close()
        self.assertEqual(self.queue(SECOND,'show')['running'],[])

    def test_single_receiver_fanout_tickets_and_owner_finished(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            ticks=list(pool.map(lambda _:self.worker(HEAVY,'tick'),range(4)))
        self.assertEqual(sum(r['queued'] for r in ticks),1)
        dispatch=self.state(HEAVY)/'worker'/f'dispatch-{RECEIVER}.json'
        first=json.loads(dispatch.read_bytes())['ticket']
        a=self.take('codex-child','--ticket',first)
        self.assertEqual(a['id'],'JOB-A')
        self.assertEqual(self.take('codex-child')['state'],'unavailable')
        self.assertFalse(self.worker(HEAVY,'scope','claim','--scope','repository','--owner','outside')['acquired'])
        self.assertFalse(self.worker(HEAVY,'scope','claim','--scope','queue-maintenance','--owner','outside')['acquired'])
        self.assertTrue(self.worker(HEAVY,'tick')['queued'])
        second=json.loads(dispatch.read_bytes())['ticket']
        self.assertNotEqual(first,second)
        self.take('claude-child','--ticket',first,ok=False)
        c=self.take('claude-child','--ticket',second)
        self.assertEqual(c['id'],'JOB-C')
        received=(self.repo/'received.log').read_text(encoding='utf8').splitlines()
        self.assertEqual(len(received),2)
        self.assertTrue(all(line.startswith(RECEIVER+' ') and '--coordinator' in line for line in received))
        self.assertFalse(self.worker(HEAVY,'tick')['queued'])
        self.queue(HEAVY,'heartbeat','JOB-A','--lease',a['lease'])
        self.worker(HEAVY,'owner-finished','--owner','codex-child','--at',str(time.time()-60),'--confirmed-stopped')
        self.assertEqual(len(self.queue(HEAVY,'show')['running']),2)
        self.worker(HEAVY,'owner-finished','--owner','codex-child','--at',str(time.time()),'--confirmed-stopped')
        visible=self.queue(HEAVY,'show')
        self.assertEqual([v['id'] for v in visible['running']],['JOB-C'])
        stopped=next(v for v in visible['blocked'] if v['id']=='JOB-A')
        self.assertGreater(stopped['serviceSeconds'],0)
        self.assertIn('JOB-A',[v['id'] for v in visible['blocked']])
        self.queue(HEAVY,'heartbeat','JOB-A','--lease',a['lease'],ok=False)
        self.queue(HEAVY,'heartbeat','JOB-C','--lease',c['lease'])
        self.queue(HEAVY,'yield','JOB-C','--lease',c['lease'],'--blocked','Waiting for approval')
        self.assertIn('JOB-C',[v['id'] for v in self.queue(HEAVY,'show')['blocked']])

    def test_scope_exclusive_and_definition_fence(self):
        claim=self.worker(HEAVY,'scope','claim','--scope','queue-maintenance','--owner','maintenance')
        self.assertEqual(self.worker(HEAVY,'tick')['reason'],'scope-held')
        self.take('child',ok=False)
        self.worker(HEAVY,'scope','release','--scope','queue-maintenance','--owner','maintenance','--token',claim['token'])
        gpu=self.worker(HEAVY,'scope','claim','--scope','gpu','--owner','separate')
        a=self.take('child-a')
        self.assertEqual(self.take('child-c')['state'],'unavailable')
        self.worker(HEAVY,'scope','release','--scope','gpu','--owner','separate','--token',gpu['token'])
        config=self.state(HEAVY)/'worker/config.json'
        changed=json.loads(config.read_bytes())
        changed['parallel']['jobs']['JOB-A']['fingerprint']='f'*64
        config.write_text(json.dumps(changed),encoding='utf8')
        self.assertEqual(self.take('child-c')['state'],'unavailable')
        self.queue(HEAVY,'yield',a['id'],'--lease',a['lease'],'--blocked','Review')
        # Remove a second mapping: the newly highest-priority unknown runs exclusively.
        del changed['parallel']['jobs']['JOB-B']
        config.write_text(json.dumps(changed),encoding='utf8')
        b=self.take('child-b')
        self.assertEqual(b['id'],'JOB-B')
        self.assertEqual(self.take('child-c')['state'],'unavailable')
        self.assertEqual({r['id'] for r in self.queue(HEAVY,'show')['blocked']},{'JOB-A','HOLD-ME','UNFIT'})

    def test_single_and_weekly_defaults(self):
        self.assertTrue(self.worker(SECOND,'tick')['queued'])
        self.queue(SECOND,'take','--owner','single')
        self.queue(SECOND,'take','--owner','second',ok=False)
        self.assertFalse(self.worker(SECOND,'tick')['queued'])
        self.assertEqual(self.worker(LAPTOP,'tick','--trigger','hub-timer')['reason'],'hub-start-only')
        self.assertTrue(self.worker(LAPTOP,'tick','--trigger','hub-start')['queued'])
        self.assertEqual(self.worker(LAPTOP,'tick','--trigger','hub-start')['reason'],'weekly-not-due')
        self.queue(LAPTOP,'take','--owner','local')
        self.queue(LAPTOP,'take','--owner','second',ok=False)
        stamp=self.state(LAPTOP)/'worker/dispatch.json'
        before=(stamp.read_bytes(),stamp.stat().st_mtime_ns)
        self.worker(LAPTOP,'tick','--trigger','hub-timer')
        self.assertEqual(before,(stamp.read_bytes(),stamp.stat().st_mtime_ns))

    def test_expired_changed_and_concurrent_ticket_retries(self):
        self.assertTrue(self.worker(HEAVY,'tick')['queued'])
        dispatch=self.state(HEAVY)/'worker'/f'dispatch-{RECEIVER}.json'
        issued=json.loads(dispatch.read_bytes())
        ticket=issued['ticket']
        config=self.state(HEAVY)/'worker/config.json'
        altered=json.loads(config.read_bytes())
        original=config.read_bytes()
        altered['parallel']['jobs']['JOB-A']['resources']=['other-resource']
        config.write_text(json.dumps(altered),encoding='utf8')
        self.take('first','--ticket',ticket,ok=False)
        config.write_bytes(original)
        issued['at']-=901
        dispatch.write_text(json.dumps(issued),encoding='utf8')
        self.take('first','--ticket',ticket,ok=False)
        self.assertTrue(self.worker(HEAVY,'tick')['queued'])
        current=json.loads(dispatch.read_bytes())['ticket']
        self.assertNotEqual(current,ticket)
        def receiver(i):
            try:
                return self.take(f'real-worker-{i}','--ticket',current)
            except AssertionError as err:
                self.assertIn('Dispatch ticket expired, consumed, or superseded',str(err))
                return {}
        with ThreadPoolExecutor(max_workers=5) as pool:
            results=list(pool.map(receiver,range(5)))
        self.assertEqual([r['id'] for r in results if 'lease' in r],['JOB-A'])
        self.assertEqual(len(self.queue(HEAVY,'show')['running']),1)
        # Copying an enrolled desktop policy onto another PC must not enroll it.
        foreign=self.state(SECOND)/'worker/config.json'
        foreign.write_bytes(original)
        self.queue(SECOND,'take','--owner','one')
        self.queue(SECOND,'take','--owner','two',ok=False)

    def test_configure_validates_and_drains_before_switching(self):
        policy_file=self.repo/'reviewed-resources.json'
        config_file=self.state(HEAVY)/'worker/config.json'
        current=json.loads(config_file.read_bytes())
        policy_file.write_text(json.dumps(current['parallel']),encoding='utf8')
        configure=['configure','--thread',RECEIVER,'--codex',sys.executable]
        self.worker(HEAVY,*configure,'--serial')
        self.assertNotIn('parallel',json.loads(config_file.read_bytes()))
        self.worker(HEAVY,*configure,'--mode','standard','--parallel-policy',str(policy_file))
        self.assertEqual(current['parallel'],json.loads(config_file.read_bytes())['parallel'])
        self.worker(SECOND,*configure,'--parallel-policy',str(policy_file),ok=False)
        self.take('child')
        self.worker(HEAVY,*configure,'--serial',ok=False)
        self.assertIn('parallel',json.loads(config_file.read_bytes()))

    def test_handoff_across_real_workers(self):
        if not (TOOLS/'todo_handoff.py').is_file():
            self.skipTest('requires optional todo_handoff.py')
        job=self.take('reader-codex')
        common=['JOB-A','--actor','reader-codex','--lease',job['lease'],'--definition',self.fp['JOB-A']]
        handoff=self.run_cli(HEAVY,'todo_handoff.py','save',*common,'--parent','none','--summary','Reviewed fixture',
                             '--next','Continue offline','--evidence','evidence.md')
        revision=handoff['revision']
        self.queue(HEAVY,'heartbeat','JOB-A','--lease',job['lease'],'--handoff-ack',revision)
        self.queue(HEAVY,'yield','JOB-A','--lease',job['lease'],'--handoff-revision','0'*64,ok=False)
        returned=self.queue(HEAVY,'yield','JOB-A','--lease',job['lease'],'--handoff-revision',revision,
                            '--checkpoint',str(self.repo/'evidence.md'))
        self.assertEqual(returned['handoffRevision'],revision)
        # MLFQ may schedule untouched tasks first. Block their fixtures while waiting for the saved one.
        for n in range(6):
            next_job=self.take(f'claude-reader-{n}')
            if next_job.get('id')=='JOB-A':break
            self.queue(HEAVY,'yield',next_job['id'],'--lease',next_job['lease'],'--blocked','fixture wait')
        self.assertEqual(next_job['id'],'JOB-A')
        self.assertEqual(next_job['handoff']['revision'],revision)
        self.assertFalse(next_job['handoff']['acknowledged'])
        ack=self.queue(HEAVY,'heartbeat','JOB-A','--lease',next_job['lease'],'--handoff-ack',revision)
        self.assertEqual(ack['handoffAcknowledged'],revision)
        final=self.queue(HEAVY,'complete','JOB-A','--lease',next_job['lease'],'--handoff-revision',revision,
                         '--evidence','evidence.md','--note','verified fixture')
        self.assertEqual(final['handoffRevisions'],{'local':revision})
        self.queue(HEAVY,'complete','JOB-A','--lease',next_job['lease'],'--handoff-revision','f'*64,
                   '--evidence','evidence.md','--note','stale',ok=False)


if __name__=='__main__':
    unittest.main()


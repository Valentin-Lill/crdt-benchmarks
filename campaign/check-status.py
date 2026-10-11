#!/usr/bin/env python3
"""Small read-only status for low-token AI monitoring; never interrupts workers."""
import collections,datetime,json,pathlib,subprocess
root=pathlib.Path(__file__).resolve().parent
job=pathlib.Path(json.loads((root/'results/RUNNING.json').read_text())['job_dir'])
meta=json.loads((root/'results/RUNNING.json').read_text())
def state(unit):
 r=subprocess.run(['systemctl','--user','is-active',unit],text=True,capture_output=True);return r.stdout.strip()
s=json.loads((job/'large-offline/status.json').read_text());raw=json.loads((job/'large-offline/results.json').read_text());latest={(r['scenario'],r['library'],r['trial']):r for r in raw};counts=collections.Counter(r['status'] for r in latest.values())
age=(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(s['updated'])).total_seconds()
print(datetime.datetime.now().astimezone().strftime('%H:%M'), 'main='+state(meta['unit']), 'recovery='+state('crdt-benchmark-handoff-recovery.service'), 'controller='+json.loads((job/'status.json').read_text())['state'])
print('phase='+':'.join(str(s.get(k,'')) for k in ['scenario','library','trial','stage']),f'age={age/60:.1f}min', 'trials='+str(len(latest)),'outcomes='+str(dict(counts)))
if age>3600 or state(meta['unit'])!='active':
 processes=subprocess.check_output(['ps','-eo','pid,etimes,pcpu,rss,args'],text=True)
 print('\n'.join(line[:500] for line in processes.splitlines() if any(x in line for x in ['night-native night','night-native-memory night','adapter-night.mjs','upstream-worker.mjs'])))
 print('last-log:', '\n'.join((job/'controller.log').read_text().splitlines()[-3:]))

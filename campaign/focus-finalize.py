#!/usr/bin/env python3
"""Publish the combined comparison automatically after both serial test services exit."""
import pathlib,subprocess,sys,time,json,shutil
import campaign as c
import overnight as n
ROOT=pathlib.Path(__file__).resolve().parent;job=ROOT/'runs/focus-20261011T072400Z';diagnostic=ROOT/'runs/focus-history16-20261011'
units=['crdt-focus-20261011t072400z.service','crdt-focus-history-followup.service']
while any(subprocess.run(['systemctl','--user','is-active','--quiet',unit]).returncode==0 for unit in units):time.sleep(15)
subprocess.run([sys.executable,str(ROOT/'focus-analyze.py'),'--job-dir',str(job),'--followup-dir',str(diagnostic)],check=True)
dest=ROOT.parent/'reports'/job.name;dest.mkdir(parents=True,exist_ok=True)
for name in ['comparison.json','comparison.csv','comparison.html','COMPARISON.txt']:shutil.copy2(job/name,dest/name)
(ROOT.parent/'reports/LATEST.txt').write_text(str(dest.relative_to(ROOT.parent))+'\n')
log=job/'final-git.log';c.command(['git','add','--','reports'],log)
if subprocess.run(['git','diff','--cached','--quiet'],cwd=ROOT.parent).returncode:c.command(['git','commit','-m','Publish combined 100 MiB storage, latency and history diagnostics'],log)
pushed=False
for delay in [0,5,15]:
 if delay:time.sleep(delay)
 if c.command(['git','push','origin','HEAD:main'],log,timeout=60)==0:pushed=True;break
n.atomic(job/'FINAL-READY.json',{'finished':n.utc(),'pushed':pushed,'report':str(job/'comparison.html')})
print('FINAL COMPARISON READY',pushed,flush=True)

#!/usr/bin/env python3
"""Serial smaller-history diagnostic after the main run, within its reporting window."""
import datetime,json,pathlib,subprocess,sys,time
from zoneinfo import ZoneInfo
ROOT=pathlib.Path(__file__).resolve().parent
unit='crdt-focus-20261011t072400z.service';job=ROOT/'runs/focus-history16-20261011'
while subprocess.run(['systemctl','--user','is-active','--quiet',unit]).returncode==0:time.sleep(30)
current=datetime.datetime.now(ZoneInfo('Europe/Berlin'))
if current.hour>10 or current.hour==10 and current.minute>=45:
 print('Diagnostic skipped: insufficient remaining time before final reporting',flush=True);sys.exit(0)
print('Starting serial 16-MiB history diagnostic for Loro full operations',flush=True)
subprocess.run([sys.executable,str(ROOT/'focus.py'),'--job-dir',str(job),'--libraries','loro-updates','--history-mib','16','--core-only'],check=True)

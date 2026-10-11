#!/usr/bin/env python3
"""Finish an existing validated campaign without any time-based test termination."""
import argparse, datetime, hashlib, json, os, pathlib, signal, subprocess, sys, time
import campaign as controller
import overnight as night

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--job-dir',required=True);args=ap.parse_args()
 job=pathlib.Path(args.job_dir).resolve();metadata=json.loads((job/'metadata.json').read_text())
 if not metadata.get('standard_offline_complete') or not metadata.get('standard_upstream_complete'):
  raise RuntimeError('This resume entry point requires the completed standard campaign')
 cpus=metadata['affinity'];os.sched_setaffinity(0,set(cpus))
 signal.signal(signal.SIGTERM,controller.stop);signal.signal(signal.SIGINT,controller.stop)
 commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=controller.REPO,text=True).strip()
 metadata.setdefault('resumptions',[]).append({'at':controller.now(),'prior_stop_berlin':metadata.get('stop_berlin'),'source_commit':commit,'source_sha256':{str(p.relative_to(controller.REPO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [pathlib.Path(__file__),night.ROOT/'campaign.py',night.ROOT/'overnight.py']}})
 metadata.update(stop_berlin=None,time_policy='No global, phase or trial deadlines; finish all planned work',large_complete=False)
 controller.atomic(job/'metadata.json',metadata)
 libraries=metadata.get('large_eligible',night.LIBS)
 controller.status(job,'resuming-without-time-limits');controller.render(job)
 controller.publish(job,'Continue full CRDT campaign without clock cutoffs')
 try:
  while not controller.STOP:
   cmd=[sys.executable,str(night.ROOT/'overnight.py'),'--job-dir',str(job/'large-offline'),'--resume','--no-time-limit','--profile','large','--repeats',str(metadata.get('repeats',3)),'--libraries',*libraries]
   controller.status(job,'large-offline',time_limits_disabled=True)
   code=controller.child(cmd,job,float('inf'))
   state=json.loads((job/'large-offline/status.json').read_text())
   controller.render(job);controller.publish(job,'Record large offline-first benchmark progress')
   if code==0 and state['state']=='complete':break
   if controller.STOP:return
   # Host reserves are transient: wait for recovery and resume without user input.
   if state['state'] not in ['budget-ended','reference-or-input-error']:
    raise RuntimeError(f'Offline harness did not complete: {state}')
   while not controller.STOP and (night.available_memory()<5*1024**3 or __import__('shutil').disk_usage(job).free<11*1024**3):time.sleep(30)
   time.sleep(30)
  if controller.STOP:return
  metadata['large_offline_complete']=True;controller.atomic(job/'metadata.json',metadata)
  while not controller.STOP:
   complete,invalid,limited=controller.upstream(job,float('inf'),[x for x in night.JS_LIBS if x in libraries],1,large=True)
   controller.render(job);controller.publish(job,'Record remaining expanded upstream trace tests')
   if complete:break
   if controller.STOP:return
   time.sleep(30)
  metadata.update(large_upstream_complete=complete,large_complete=complete,finished=controller.now());controller.atomic(job/'metadata.json',metadata)
  controller.status(job,'complete',time_limits_disabled=True);controller.render(job)
 except Exception as e:
  controller.status(job,'campaign-error',error=repr(e));controller.render(job);raise
 finally:
  if controller.STOP:controller.status(job,'stopped');controller.render(job)
  pushed=controller.publish(job,'Publish CRDT campaign results and decision notes')
  controller.atomic(job/'delivery.json',{'pushed_to_github':pushed,'finished':controller.now(),'report':str(job/'report.html')})

if __name__=='__main__':main()

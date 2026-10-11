#!/usr/bin/env python3
"""Recover only the new trial intentionally paused during the controller handoff."""
import argparse, json, pathlib, subprocess, sys, time
import overnight as night

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--job-dir',required=True);ap.add_argument('--wait-unit',required=True);args=ap.parse_args();job=pathlib.Path(args.job_dir)
 while subprocess.run(['systemctl','--user','is-active','--quiet',args.wait_unit]).returncode==0:time.sleep(30)
 path=job/'large-offline/results.json';records=json.loads(path.read_text());key=('text-128m','loro-tail',1)
 matches=[r for r in records if (r['scenario'],r['library'],r['trial'])==key]
 if matches and matches[-1]['status']=='ok':return
 target=next(r for r in matches if r['failed_stage']=='init' and r.get('failure',{}).get('returncode')==-15 and r.get('failure',{}).get('wall_s',999)<5)
 target['original_status']=target['status'];target['status']='stopped';target['harness_interruption']='The safe handoff paused this init before CRDT execution; SIGTERM stopped the paused wrapper, not the engine.'
 night.atomic(path,records)
 subprocess.run([sys.executable,str(night.ROOT/'resume.py'),'--job-dir',str(job)],check=True)
 # This exact handoff-interrupted combination must have a real completed attempt.
 records=json.loads(path.read_text());matches=[r for r in records if (r['scenario'],r['library'],r['trial'])==key]
 assert len(matches)>=2 and matches[-1]['status']!='stopped'

if __name__=='__main__':main()

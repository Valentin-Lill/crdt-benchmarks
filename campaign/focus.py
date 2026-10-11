#!/usr/bin/env python3
"""Storage-first 100-MiB campaign: causal offline return, churn and edit latency."""
import argparse,collections,datetime,hashlib,html,json,os,pathlib,shutil,signal,subprocess,sys,time
from zoneinfo import ZoneInfo
import overnight as n
import campaign as c
ROOT=n.ROOT;BERLIN=ZoneInfo('Europe/Berlin');LIBS=['yrs','loro','loro-updates']

def emit(job,state,**kw):n.atomic(job/'status.json',{'state':state,'at':n.utc(),'pid':os.getpid(),**kw})
def render(job):
 raw=json.loads((job/'results.json').read_text()) if (job/'results.json').exists() else []
 rows=[]
 for record in raw:
  p=record['phases'];summary={'library':record['library'],'case':record['case'],'trial':record['trial'],'status':record['status'],'failed_stage':record.get('failed_stage')}
  for stage,prefix in [('server','server'),('offline','offline'),('merge-server-history','server_merge'),('merge-client-history','client_merge')]:
   r=p.get(stage,{}).get('result',{})
   for key in ['edit_ms','edit_peak_rss_bytes','merge_ms','merge_peak_rss_bytes','load_ms']: 
    if key in r:summary[prefix+'_'+key]=r[key]
  summary['history_points']=p.get('history',{}).get('result',{}).get('points',record.get('partial_history_points',[]))
  summary['persisted_storage']=p.get('final-storage',{}).get('result',{}).get('storage')
  summary['probes']=p.get('probe',{}).get('result')
  summary['cold_after_history']=p.get('cold-after-history',{}).get('result')
  rows.append(summary)
 n.atomic(job/'summary.json',rows)
 meta=json.loads((job/'metadata.json').read_text())
 notes=['FOCUSED 100-MiB CRDT COMPARISON','Updated '+n.utc(),'','Configuration: Yrs 0.28.0 V2/GC, Loro 1.16.2 full snapshot and full operations.','All cases start with 104857600 actual ASCII text bytes plus 6000 scalar map fields.','Cluster: 6000 server and 1500 offline commands. Random: 10000 and 2500 commands.',f"Core history repeatedly replaces the same 128 KiB range, up to {meta.get('history_max_mib',64)} MiB inserted AND deleted payload. Visible size remains effectively constant. Exact checkpoints are in the recipe.",'Offline client branches before server editing/history; both branches are saved/reloaded and merged; local state is checked against an independent Rope reference, both merged replicas against each other, sentinels and duplicates checked.','Probes use single-operation transactions: 256 local replacements, 512 random replacements, edge edits, 1/10 MiB paste followed by deletion. First edit is measured BEFORE full text materialization. p95/p99 of small sample counts are descriptive, not robust production tail estimates.','Every measured phase is a fresh process owning one CRDT replica. Reference computations run separately. Peaks use Linux VmHWM reset and kernel RSS.','Storage is the actual exported CRDT payload plus optional lossless Zstd-3; both representations exist as benchmark fixtures, but a server needs only the chosen representation plus unapplied updates. No shallow snapshots/history pruning. No old backup snapshots or test-only replicas are included in payload cost.','Raw snapshot writing includes fsync; database indexes, backups, replication, network and Flutter/Web UI costs are not included. Zstd compression/decompression and exact byte roundtrip are verified.','Resource limits: 12 GiB per worker, 4 GiB host RAM reserve, 10 GiB disk reserve. Cases interrupted by time/resource guards remain INCOMPLETE, never counted as passed.','Different scenarios and history volumes must not be combined into one ranking. Exact recipes/versions/source hashes and failures are retained.','']
 def mib(x):return f'{x/2**20:.2f}'
 for r in rows:
  notes.append(f"{r['case']} {r['library']} trial {r['trial']}: {r['status']}")
  if r['failed_stage']:notes.append('  interrupted/failed stage: '+r['failed_stage'])
  storage=r['persisted_storage']
  if storage:notes.append(f"  Final stored raw {mib(storage['raw_bytes'])} MiB / Zstd {mib(storage['zstd3_bytes'])} MiB; save {storage['save_ms']:.1f} ms, compress {storage['compress_ms']:.1f} ms, decompress {storage['decompress_ms']:.1f} ms")
  for point in r['history_points']:
   st=point['storage'];notes.append(f"  After {mib(point['inserted_bytes'])} MiB replaced: raw {mib(st['raw_bytes'])} MiB / Zstd {mib(st['zstd3_bytes'])} MiB")
  if r['probes']:
   for name,q in r['probes']['latencies'].items():notes.append(f"  {name}: p50 {q['median_ms']:.3f}, p95 {q['p95_ms']:.3f}, p99 {q['p99_ms']:.3f}, max {q['max_ms']:.3f} ms ({q['samples']} samples)")
  if r['cold_after_history']:
   cold=r['cold_after_history'];notes.append(f"  Cold after history: load {cold['load_ms']:.1f} ms, full read {cold['read_ms']:.1f} ms, first edit after read {cold['first_edit_ms']:.1f} ms; final RSS {mib(cold['edited_rss_bytes'])} MiB")
  notes.append('')
 (job/'DECISION.txt').write_text('\n'.join(notes)+'\n')
 state=json.loads((job/'status.json').read_text()) if (job/'status.json').exists() else {}
 (job/'report.html').write_text('<!doctype html><meta charset="utf-8"><title>Focused 100 MiB CRDT results</title><style>body{font:16px/1.5 system-ui;max-width:1300px;margin:30px auto;padding:20px}pre{white-space:pre-wrap}a{color:#0759a5}</style><h1>Focused 100 MiB CRDT results</h1><p>Status: '+html.escape(state.get('state',''))+'</p><p><a href="summary.json">Normalized measurements</a> · <a href="results.json">Raw phase data</a> · <a href="metadata.json">Environment and methodology</a></p><pre>'+html.escape('\n'.join(notes))+'</pre>')

def publish(job,message):
 render(job);dest=c.REPO/'reports'/job.name;dest.mkdir(parents=True,exist_ok=True)
 for name in ['results.json','summary.json','metadata.json','status.json','report.html','DECISION.txt','references.json']:
  if (job/name).exists():shutil.copy2(job/name,dest/name)
 (c.REPO/'reports/LATEST.txt').write_text(str(dest.relative_to(c.REPO))+'\n')
 log=job/'git.log';c.command(['git','add','--','reports'],log)
 if subprocess.run(['git','diff','--cached','--quiet'],cwd=c.REPO).returncode:c.command(['git','commit','-m',message],log)
 for delay in [0,5,15]:
  if delay:time.sleep(delay)
  if c.command(['git','push','origin','HEAD:main'],log,timeout=60)==0:return True
 return False

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--job-dir',required=True);ap.add_argument('--smoke',action='store_true');ap.add_argument('--libraries',nargs='+',choices=LIBS,default=LIBS);ap.add_argument('--history-mib',type=int,default=64);ap.add_argument('--core-only',action='store_true');args=ap.parse_args();libraries=args.libraries;job=pathlib.Path(args.job_dir).resolve();job.mkdir(parents=True,exist_ok=True)
 for name in ['logs','inputs','work']:(job/name).mkdir(exist_ok=True)
 current=datetime.datetime.now(BERLIN);gate=current.replace(hour=10,minute=30,second=0,microsecond=0).timestamp();end=current.replace(hour=10,minute=50,second=0,microsecond=0).timestamp()
 if args.smoke:gate=time.time();end=time.time()+600
 deadline=time.monotonic()+max(0,end-time.time());os.sched_setaffinity(0,{min(os.sched_getaffinity(0))});signal.signal(signal.SIGTERM,n.request_stop);signal.signal(signal.SIGINT,n.request_stop)
 records=[];refs={};binary=ROOT/'bin/focus-native';size=4096 if args.smoke else 100*2**20
 text=job/'inputs/text.txt';n.make_text(text,size,deadline)
 meta={'started':n.utc(),'minimum_testing_until_berlin':datetime.datetime.fromtimestamp(gate,BERLIN).isoformat(),'worker_budget_end_berlin':datetime.datetime.fromtimestamp(end,BERLIN).isoformat(),'libraries':libraries,'history_max_mib':args.history_mib,'text_bytes':size,'smoke':args.smoke,'cpu_affinity':sorted(os.sched_getaffinity(0)),'cpu':subprocess.check_output(['lscpu'],text=True),'rustc':subprocess.check_output(['rustc','+1.96.0','--version'],text=True).strip(),'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=c.REPO,text=True).strip(),'versions':json.loads((ROOT/'results/FOCUS-VERSIONS.json').read_text()),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'source_sha256':{str(p.relative_to(c.REPO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'focus.py',ROOT/'src/night/focused.rs',ROOT/'src/main.rs',ROOT/'src/night.rs',ROOT/'Cargo.lock']},'repeats_policy':'Mandatory cluster/storage/history pass for all three first; optional random and repeat passes until minimum testing time; no new optional cases after 10:30; end worker budget 10:50 for reporting before 11:00.'}
 n.atomic(job/'metadata.json',meta);n.atomic(job/'results.json',records);emit(job,'starting');render(job)
 def run(lib,recipe,action,work,stem,mode='night',sample_deadline=None):
  emit(job,'running',library=lib,stage=action,case=recipe.stem,recorded=len(records))
  data=n.measured([str(binary),mode,lib,str(recipe),action,str(work)],stem,job,min(deadline,sample_deadline or deadline),480 if not args.smoke else 30,12*2**30)
  if data['status']!='ok':raise n.Failure(action,data)
  return data
 def recipe_for(case):
  recipe={'text_path':str(text),'text_bytes':size,'keys':24 if args.smoke else 6000,'server_edits':64 if args.smoke else 6000 if case=='cluster' else 10000,'offline_edits':32 if args.smoke else 1500 if case=='cluster' else 2500,'pattern':'cluster' if case=='cluster' else 'random','churn_bytes':0,'batch':32,'history_rounds':[2,4,8] if args.smoke else sorted({int(m*2**20/(128*1024)) for m in [2,16,args.history_mib] if m<=args.history_mib}),'history_chunk':64 if args.smoke else 128*1024,'local_ops':16 if args.smoke else 256,'random_ops':16 if args.smoke else 512}
  p=job/'inputs'/f'{case}.json';n.atomic(p,recipe);return p
 def references(case,recipe):
  if case in refs:return
  ref={}
  for action,mode in [('reference-server','night'),('reference-offline','night'),('probe-reference','focus'),('history-reference','focus')]:
   ref[action]=run('yrs',recipe,action,job/'work/reference',case+'-'+action,mode)
  refs[case]=ref;n.atomic(job/'references.json',refs)
 def test(case,lib,trial,history):
  recipe=recipe_for(case);references(case,recipe);work=job/'work'/f'{case}-{lib}-{trial}';work.mkdir(exist_ok=True);stem=f'{case}-{lib}-{trial}';record={'case':case,'library':lib,'trial':trial,'started':n.utc(),'status':'running','phases':{}};p=record['phases'];started_timer=time.monotonic();limit=min(deadline,started_timer+1200)
  def phase(action,key=None,mode='night'):
   key=key or action;p[key]=run(lib,recipe,action,work,stem+'-'+key,mode,limit)
   n.atomic(job/'active-phases.json',record)
   return p[key]['result']
  try:
   phase('init')
   for branch in ['server','offline']:
    phase(branch);r=phase('check-'+branch);assert r['hash']==refs[case]['reference-'+branch]['result']['hash'],branch+' local-reference mismatch'
   if history:
    phase('history',mode='focus');r=phase('check-server','check-history-server');expected=refs[case]['history-reference']['result']['points'][-1];assert r['hash']==expected['hash'],'history reference mismatch'
   phase('merge-server','merge-server-history');phase('merge-client','merge-client-history')
   a=phase('check-merged-server');b=phase('check-merged-client');assert a['hash']==b['hash'],'merged convergence mismatch'
   phase('storage','final-storage',mode='focus');problems=[]
   for action,key in [('cold','cold-after-history'),('probe','probe'),('reverse-check','reverse-check')]:
    try:
     r=phase(action,key,mode='night' if action=='cold' else 'focus')
     if action=='probe':assert r['hash']==refs[case]['probe-reference']['result']['hash'],'probe reference mismatch'
     if action=='reverse-check':assert r['hash']==a['hash'],'reverse delivery convergence mismatch'
    except n.Failure as e:p[key]=e.data;problems.append({'stage':key,'status':e.data['status']})
   record.update(status='ok' if not problems else 'incomplete-performance',converged=True,performance_limits=problems)
  except n.Failure as e:record.update(status=e.data['status'],failed_stage=e.stage,failure=e.data)
  except Exception as e:record.update(status='harness-error',failure=repr(e))
  finally:
   record['finished']=n.utc();record['wall_s']=time.monotonic()-started_timer
   record['partial_history_points']=[json.loads(path.read_text()) for path in sorted(work.glob('history-point-*.json'),key=lambda p:int(p.stem.split('-')[-1]))]
   records.append(record);n.atomic(job/'results.json',records);render(job);shutil.rmtree(work,ignore_errors=True)
  publish(job,f'Record focused 100 MiB {case} {lib} trial {trial}')
 try:
  for lib in libraries:
   if n.STOP or time.monotonic()>=deadline:break
   test('cluster',lib,1,True)
  trial=1
  while not args.smoke and not args.core_only and not n.STOP and time.time()<gate:
   for lib in libraries[trial%len(libraries):]+libraries[:trial%len(libraries)]:
    if n.STOP or time.time()>=gate:break
    test('random',lib,trial,False)
   trial+=1
  emit(job,'stopped' if n.STOP else 'complete' if time.monotonic()<deadline else 'budget-ended',recorded=len(records));render(job)
 except Exception as e:emit(job,'harness-error',error=repr(e));raise
 finally:
  pushed=publish(job,'Publish focused 100 MiB CRDT storage and performance results');n.atomic(job/'delivery.json',{'pushed':pushed,'finished':n.utc()})
 if args.smoke and (len(records)!=len(libraries) or any(r['status']!='ok' for r in records)):raise RuntimeError('Focused smoke validation failed')

if __name__=='__main__':main()

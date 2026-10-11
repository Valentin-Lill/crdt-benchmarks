#!/usr/bin/env python3
"""Detached campaign: validated adapters, standard suite, conditional larger suite.

No model polling is required. Every completed phase is persisted by overnight.py;
this controller writes reports and commits/pushes checkpoints itself.
"""
import argparse, collections, datetime, hashlib, html, json, os, pathlib, shutil, signal, statistics, subprocess, sys, time
from zoneinfo import ZoneInfo
import overnight as night

ROOT=pathlib.Path(__file__).resolve().parent
REPO=ROOT.parent
BERLIN=ZoneInfo('Europe/Berlin')
TESTS=[f'B1.{i}' for i in range(1,12)]+[f'B2.{i}' for i in range(1,5)]+[f'B3.{i}' for i in range(1,6)]+['B4']
JS=night.JS_LIBS
ALIASES={'automerge-js':'automerge','json-joy-adapter':'json-joy'}
STOP=False;ACTIVE=None

def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(p,d):night.atomic(p,d)
def stop(*_):
 global STOP
 STOP=True
 night.STOP=True
 if ACTIVE and ACTIVE.poll() is None:
  try:os.killpg(ACTIVE.pid,signal.SIGTERM)
  except ProcessLookupError:pass

def command(args,log,timeout=300):
 with log.open('a') as f:
  f.write('\n'+now()+' '+repr(args)+'\n');f.flush()
  r=subprocess.run(args,cwd=REPO,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
 return r.returncode

def publish(job,message):
 dest=REPO/'reports'/job.name;dest.mkdir(parents=True,exist_ok=True)
 # Synthetic inputs, checkpoints and binaries are deliberately not uploaded.
 for rel in ['status.json','metadata.json','upstream-results.json','upstream-summary.json','upstream-summary.csv','report.html','DECISION.txt','validation.json','upstream-trace.sha256']:
  if (job/rel).exists():shutil.copy2(job/rel,dest/rel)
 for stage in ['smoke','standard-offline','large-offline']:
  if not (job/stage).exists():continue
  out=dest/stage;out.mkdir(exist_ok=True)
  for rel in ['results.json','summary.json','summary.csv','metadata.json','status.json','report.html','phases.jsonl']:
   if (job/stage/rel).exists():shutil.copy2(job/stage/rel,out/rel)
 (REPO/'reports'/'LATEST.txt').write_text(str(dest.relative_to(REPO))+'\n')
 log=job/'git.log'
 if command(['git','add','--','reports'],log):return False
 dirty=subprocess.run(['git','diff','--cached','--quiet'],cwd=REPO).returncode
 if dirty and command(['git','commit','-m',message],log):return False
 for delay in [0,20,60]:
  if delay:time.sleep(delay)
  if command(['git','push','origin','HEAD:main'],log,timeout=120)==0:return True
 return False

def child(args,job,deadline):
 global ACTIVE
 with (job/'controller.log').open('a') as f:
  ACTIVE=subprocess.Popen(args,cwd=REPO,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  checkpoint_at=time.monotonic()
  while ACTIVE.poll() is None:
   if STOP or (job/'STOP').exists() or time.time()>deadline:
    os.killpg(ACTIVE.pid,signal.SIGTERM)
    try:ACTIVE.wait(timeout=20)
    except subprocess.TimeoutExpired:os.killpg(ACTIVE.pid,signal.SIGKILL);ACTIVE.wait()
    break
   if time.monotonic()-checkpoint_at>3600:
    render(job);publish(job,'Record automatic CRDT benchmark progress');checkpoint_at=time.monotonic()
   time.sleep(5)
  code=ACTIVE.returncode
 ACTIVE=None;return code

def upstream(job,deadline,libraries,repeats,large=False):
 if not libraries:return True,set(),set()
 records=json.loads((job/'upstream-results.json').read_text()) if (job/'upstream-results.json').exists() else []
 latest={(r['library'],r['test'],r['trial']):r for r in records}
 invalid=set();limited=set();failed_pairs=set()
 tests=['B4x100'] if large else TESTS
 for trial in range(1,repeats+1):
  for index,test in enumerate(tests):
   libs=libraries[index%len(libraries):]+libraries[:index%len(libraries)]
   for lib in libs:
    if (lib,test) in failed_pairs:continue
    prior=latest.get((lib,test,trial))
    if prior and prior['status'] not in ['timeout','trial-timeout','budget-limit','stopped','host-memory-reserve','host-disk-reserve']:continue
    if lib=='diamond-types' and test not in ['B1.1','B1.2','B1.3','B1.4','B1.5','B1.6','B1.7','B2.1','B2.2','B2.3','B2.4','B3.5','B4','B4x100']:continue
    if STOP or (job/'STOP').exists() or time.time()>=deadline:return False,invalid,limited
    attempt=1+sum(r['library']==lib and r['test']==test and r['trial']==trial for r in records);stem=f'{lib}-{test}-{trial}'+(f'-attempt{attempt}' if attempt>1 else '');out=job/'logs'/(stem+'.result.json')
    status(job,'large-upstream' if large else 'standard-upstream',library=lib,test=test,trial=trial)
    cmd=['node','--expose-gc','--max-old-space-size=8192',str(ROOT/'upstream-worker.mjs'),ALIASES.get(lib,lib),test,str(out)]
    # night.measured captures wait4 CPU and kernel max RSS independently of JS heap.
    data=night.measured(cmd,stem,job,time.monotonic()+max(0,deadline-time.time()),(0 if deadline==float('inf') else 600 if large else 120),12*1024**3)
    row={'library':lib,'test':test,'trial':trial,'status':data['status'],'measurement':data}
    if data['status']=='ok' and out.exists():row['result']=json.loads(out.read_text())
    elif data['status']=='ok':row['status']='invalid-output'
    if row['status']!='ok':
     failed_pairs.add((lib,test))
     (invalid if row['status'] in ['failed','invalid-output'] else limited).add(lib)
    records.append(row);atomic(job/'upstream-results.json',records)
    render(job)
    print(now(),stem,row['status'],flush=True)
    if data['status'] in ['stopped','budget-limit','host-memory-reserve','host-disk-reserve']:return False,invalid,limited
 if not large:
  for lib in sorted(night.NATIVE):
   if STOP or (job/'STOP').exists() or time.time()>=deadline:return False,invalid,limited
   stem=f'{lib}-B4-native';out=job/'logs'/(stem+'.result.json')
   status(job,'standard-native-trace',library=lib)
   data=night.measured([str(ROOT/'bin/night-native'),'upstream-text',lib,str(ROOT/'upstream-trace.json'),str(out)],stem,job,time.monotonic()+max(0,deadline-time.time()),120,12*1024**3)
   row={'library':lib,'test':'B4-native','trial':1,'status':data['status'],'measurement':data}
   if data['status']=='ok' and out.exists():row['result']=json.loads(out.read_text())
   elif data['status']=='failed':invalid.add(lib)
   else:limited.add(lib)
   records.append(row);atomic(job/'upstream-results.json',records);render(job)
 return True,invalid,limited

def offline(job,name,profile,libs,deadline,repeats,smoke=False):
 if not libs:return False
 remaining=max(0,deadline-time.time())
 if remaining<30:return False
 cmd=[sys.executable,str(ROOT/'overnight.py'),'--job-dir',str(job/name),'--hours',str(remaining/3600 if remaining!=float('inf') else 8),'--profile',profile,'--repeats',str(repeats),'--phase-timeout','600' if profile=='large' else '120','--trial-timeout','1200' if profile=='large' else '360','--libraries',*libs]
 if deadline==float('inf'):cmd+=['--no-time-limit']
 if smoke:cmd+=['--smoke','--phase-timeout','30','--trial-timeout','90']
 status(job,name);code=child(cmd,job,deadline)
 if not (job/name/'status.json').exists():return False
 s=json.loads((job/name/'status.json').read_text())
 return code==0 and s['state']=='complete'

def status(job,state,**extra):
 atomic(job/'status.json',{'state':state,'updated':now(),'pid':os.getpid(),**extra})

def render(job):
 records=json.loads((job/'upstream-results.json').read_text()) if (job/'upstream-results.json').exists() else []
 groups=collections.defaultdict(list)
 latest={(r['test'],r['library'],r['trial']):r for r in records}
 for r in latest.values():groups[r['test'],r['library']].append(r)
 rows=[]
 for (test,lib),rs in groups.items():
  good=[r for r in rs if r['status']=='ok'];row={'test':test,'library':lib,'passed':len(good),'attempted':len(rs),'failures':[r['status'] for r in rs if r['status']!='ok']}
  if good:
   row['peak_rss_bytes']=statistics.median(r['measurement'].get('process',{}).get('max_rss_kib',0)*1024 for r in good)
   row['process_wall_s']=statistics.median(r['measurement']['wall_s'] for r in good)
   if test=='B4-native':
    for k in ['edit_ms','save_ms','load_ms','read_ms','snapshot_bytes']:row[k]=statistics.median(r['result'][k] for r in good)
   else:
    metrics=good[0]['result']['results']
    for metric in metrics:
     raw=[next(iter(r['result']['results'][metric].values())).split() for r in good]
     unit=raw[0][1] if len(raw[0])>1 else ''
     if 'memUsed' in metric:
      scale={'B':1,'kB':1000,'MB':1000**2,'GB':1000**3}
      values=[float(v[0])*scale.get(v[1],1) for v in raw];unit='bytes of JS heap only'
     else:values=[float(v[0]) for v in raw]
     row[metric]=f'{statistics.median(values):g} {unit}'
  rows.append(row)
 atomic(job/'upstream-summary.json',rows)
 import csv
 with (job/'upstream-summary.csv').open('w',newline='') as f:
  keys=sorted({k for r in rows for k in r});w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)
 state=json.loads((job/'status.json').read_text()) if (job/'status.json').exists() else {}
 parts=[]
 for test in sorted({r['test'] for r in rows}):
  cells=[]
  for r in [x for x in rows if x['test']==test]:
   value='<br>'.join(html.escape(k)+': '+html.escape(str(v)) for k,v in r.items() if k not in ['test','library','peak_rss_bytes','passed','attempted','failures'])
   cells.append(f'<tr><td>{html.escape(r["library"])}</td><td>{r["passed"]}/{r["attempted"]}</td><td>{r.get("peak_rss_bytes",0)/1024**2:.1f}</td><td>{value}</td><td>{html.escape(str(r["failures"]))}</td></tr>')
  parts.append('<details><summary>'+test+'</summary><table><tr><th>Variant</th><th>Passes</th><th>Process peak RSS MiB</th><th>Metrics</th><th>Failures</th></tr>'+''.join(cells)+'</table></details>')
 links=[]
 for stage in ['standard-offline','large-offline','smoke']:
  if (job/stage/'report.html').exists():links.append(f'<li><a href="{stage}/report.html">{stage}: phase-separated RAM, editing, offline merge and storage</a></li>')
 text='<!doctype html><meta charset="utf-8"><title>CRDT decision benchmark</title><style>body{font:16px/1.5 system-ui;max-width:1400px;margin:30px auto;padding:20px}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:left}details{margin:20px 0}</style><h1>CRDT decision benchmark</h1><p>Status: '+html.escape(state.get('state','starting'))+'</p><p>Upstream B1–B4 at N=6000, unchanged editing trace; corrected B3.5 content assertion. Every test/variant is a fresh process with seed 42. Source, exact versions, timeouts and failures are preserved. Heap figures from upstream exclude WASM memory; use process RSS and the phase-separated offline report for memory comparisons. Upstream process peaks include all replicas/reference checks and are not one-client memory. Upstream native B4 omits per-operation wire serialization and has one sample; do not rank it directly against JS/WASM B4 timing. A virtual offline month is represented by divergent versions and repeated save/reload, without sleeping a real month.</p><ul>'+''.join(links)+'</ul><p><a href="DECISION.txt">Decision notes</a> · <a href="metadata.json">Environment and versions</a> · <a href="upstream-results.json">Raw upstream results</a> · <a href="upstream-summary.csv">CSV</a></p>'+''.join(parts)
 (job/'report.html').write_text(text)
 notes=['CRDT decision benchmark — '+state.get('state','starting'),'','Use native Rust results for Flutter/Rust decisions. JS/WASM results describe those runtimes separately. No mobile, Web or Flutter bridge integration has been measured.','Only configurations that pass the persistence/offline checks are candidates. A timeout or memory guard is an operational limit, not evidence of incorrect CRDT behavior.','Shallow snapshots, reset-to-JSON and destructive CRDT history pruning are excluded.','Raw serialized disk size and optional lossless Zstd-3 size are separate from actual database, fsync and encrypted storage costs.','Measured peak RSS includes runtime, resident CRDT state and operation buffers. Process peaks in upstream may include many replicas; offline report isolates each replica and stage.','Nested objects and arrays are covered by upstream JS/WASM tests; the native offline scenarios use text and flat scalar maps. Production rich-text, Unicode offset handling, schema migration and security still need integration validation.','']
 for stage in ['standard-offline','large-offline']:
  if not (job/stage/'summary.json').exists():continue
  summary=json.loads((job/stage/'summary.json').read_text());notes.append(stage+':')
  for scenario in sorted({r['scenario'] for r in summary}):
   candidates=[r for r in summary if r['scenario']==scenario and r['library'] in night.NATIVE and r['library']!='diamond' and r['successful'] and not r['failures']]
   if not candidates:notes.append('  '+scenario+': no fully successful native candidates');continue
   for metric,label in [('edit_ms','fastest editing'),('server_merge_ms','fastest server merge'),('best_disk_bytes','smallest stored payload'),('steady_rss_bytes','lowest steady process RAM'),('server_merge_peak_rss_bytes','lowest server merge peak RAM')]:
    r=min(candidates,key=lambda x:x[metric]);notes.append(f'  {scenario}, {label}: {r["library"]}, {r[metric]:.3f}; successful samples {r["successful"]}')
   notes.append('  Per-metric leaders are provisional; compare the full tradeoffs and repeatability in the tables.')
 (job/'DECISION.txt').write_text('\n'.join(notes)+'\n')

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--job-dir');ap.add_argument('--repeats',type=int,default=3);args=ap.parse_args()
 job=pathlib.Path(args.job_dir).resolve() if args.job_dir else ROOT/'runs'/datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
 job.mkdir(parents=True,exist_ok=True);(job/'logs').mkdir(exist_ok=True)
 current=datetime.datetime.now(BERLIN);next_day=current.date()+datetime.timedelta(days=1) if current.hour>=3 else current.date()
 gate=datetime.datetime.combine(next_day,datetime.time(3),BERLIN).timestamp();end=float('inf')
 # Stable single-core comparison; no overlapping benchmark workers.
 cpus=sorted(os.sched_getaffinity(0));os.sched_setaffinity(0,{cpus[0]})
 signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
 metadata={'started':now(),'gate_berlin':datetime.datetime.fromtimestamp(gate,BERLIN).isoformat(),'stop_berlin':None,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),'versions':json.loads((ROOT/'VERSIONS.json').read_text()),'node':subprocess.check_output(['node','--version'],text=True).strip(),'rustc':subprocess.check_output(['rustc','+1.96.0','--version'],text=True).strip(),'cpu':subprocess.check_output(['lscpu'],text=True),'affinity':[cpus[0]],'libraries':night.LIBS,'upstream_tests':TESTS,'repeats':args.repeats,'resource_limits':{'per_process_rss_gib':12,'host_available_ram_reserve_gib':4,'disk_free_reserve_gib':10},'source_sha256':{str(p.relative_to(REPO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'factories.mjs',ROOT/'campaign.py',ROOT/'overnight.py',ROOT/'bin/night-native',ROOT/'bin/night-native-memory',ROOT/'Cargo.lock',REPO/'package-lock.json']}}
 atomic(job/'metadata.json',metadata);(job/'upstream-trace.sha256').write_text(hashlib.sha256((ROOT/'upstream-trace.json').read_bytes()).hexdigest()+'\n');status(job,'validating');render(job)
 try:
  if command(['node','--expose-gc',str(ROOT/'validate.mjs'),str(job/'validation.json')],job/'validation.log')!=0:raise RuntimeError('Adapter persistence/offline validation failed')
  if not offline(job,'smoke','standard',night.LIBS,min(gate,time.time()+1200),1,smoke=True):raise RuntimeError('Isolated smoke campaign failed; see smoke/results.json')
  publish(job,'Record validated adapters and smoke campaign')
  standard_ok=offline(job,'standard-offline','standard',night.LIBS,gate,args.repeats)
  publish(job,'Record standard offline-first benchmark results')
  finished,invalid,limited=upstream(job,gate,JS,args.repeats)
  metadata.update(standard_offline_complete=standard_ok,standard_upstream_complete=finished,invalid_adapters=sorted(invalid),resource_limited_adapters=sorted(limited));atomic(job/'metadata.json',metadata)
  publish(job,'Record standard upstream B1–B4 benchmark results')
  if standard_ok and finished and time.time()<gate and not STOP:
   records=json.loads((job/'standard-offline/results.json').read_text())
   invalid.update(r['library'] for r in records if r['status']!='ok')
   eligible=[lib for lib in night.LIBS if lib not in invalid]
   metadata['large_started']=now();metadata['large_eligible']=eligible;atomic(job/'metadata.json',metadata)
   # The expanded suite has no time cutoff.
   upstream(job,end,[x for x in JS if x in eligible],1,large=True)
   large_ok=offline(job,'large-offline','large',eligible,end,args.repeats);metadata['large_complete']=large_ok;atomic(job/'metadata.json',metadata)
  else:
   metadata['large_skipped_reason']='standard tests incomplete at 03:00, adapter failure, or stop request';atomic(job/'metadata.json',metadata)
  status(job,'stopped' if STOP else 'complete' if time.time()<end else 'time-budget-ended');render(job)
 except Exception as e:
  status(job,'campaign-error',error=repr(e));render(job)
  print(now(),'CAMPAIGN ERROR',repr(e),flush=True)
 finally:
  pushed=publish(job,'Publish final CRDT benchmark report and decision notes')
  atomic(job/'delivery.json',{'pushed_to_github':pushed,'finished':now(),'report':str(job/'report.html')})
  print(now(),'FINISHED',job/'report.html','GitHub push:',pushed,flush=True)

if __name__=='__main__':main()

#!/usr/bin/env python3
"""Budgeted, unattended CRDT campaign with one replica per measurement process."""
import argparse, collections, datetime, hashlib, html, json, os, pathlib, platform, random, shutil, signal, subprocess, sys, time
ROOT=pathlib.Path(__file__).resolve().parent
NATIVE={'yrs','yrs-v1','yrs-no-gc','yrs-v1-no-gc','yrs-tail','loro','loro-updates','loro-tail','automerge','automerge-chunked','automerge-bounded','automerge-chunked-tail','diamond'}
JS_LIBS=['yjs-v2-gc','yjs-v1-gc','yjs-v2-no-gc','yjs-v1-no-gc','yrs-wasm-v2-gc','yrs-wasm-v1-gc','yrs-wasm-v2-no-gc','yrs-wasm-v1-no-gc','loro-snapshot','loro-operations','automerge-js','json-joy-adapter','diamond-types','yjs-v2-gc-tail','yrs-wasm-v2-gc-tail','loro-snapshot-tail','automerge-tail','json-joy-tail']
LIBS=['yrs','loro','loro-updates','automerge','automerge-chunked','automerge-bounded','diamond','yrs-v1','yrs-no-gc','yrs-v1-no-gc','yrs-tail','loro-tail','automerge-chunked-tail']+JS_LIBS
LABELS={'loro-updates':'Loro full operations','loro':'Loro full snapshot','yrs':'Yrs GC + forward deltas','automerge-chunked':'Automerge 64 KiB initial import','automerge-bounded':'Automerge 64 KiB + 32-change imports','diamond':'Diamond Types (text only)','yjs':'Yjs / Node','json-joy':'json-joy / Node'}
LABELS.update({k:k+' / native Rust' for k in NATIVE if k not in LABELS});LABELS.update({k:k+' / JS or WASM' for k in JS_LIBS});LABELS['automerge']='Automerge native full save'
MIB=1024**2
STANDARD=[('standard-text',104852,0,6000,1500,'cluster',0),('standard-mixed',104852,6000,6000,1500,'cluster',0),('standard-random',104852,6000,6000,1500,'random',0)]
CASES=[
 ('mixed-32m-250k',32*MIB,250000,200000,50000,'cluster',0),
 ('random-32m',32*MIB,0,100000,25000,'random',0),
 ('text-128m',128*MIB,0,100000,25000,'cluster',0),
 ('mixed-128m-1m-keys',128*MIB,1000000,200000,50000,'cluster',0),
 ('text-500m',500*MIB,0,50000,20000,'cluster',0),
 ('history-8m-1m-edits',8*MIB,0,1000000,250000,'cluster',0),
 ('churn-8m-1g-replaced',8*MIB,0,262144,65536,'churn',4096),
 ('random-128m',128*MIB,0,50000,10000,'random',0),
]
SMOKE=[('smoke-cluster',4096,300,100,40,'cluster',0),('smoke-random',4096,300,100,40,'random',0),('smoke-churn',4096,0,64,32,'churn',32)]
ACTIVE=None; STOP=False

def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(path,value):
 tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(value,indent=2));tmp.replace(path)
def available_memory():
 for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
  if line.startswith('MemAvailable:'):return int(line.split()[1])*1024
 return 0

def request_stop(*_):
 global STOP
 STOP=True

def measured(command,stem,job,deadline,timeout,memory_limit):
 """wait4 captures child-only CPU; Linux phase workers also reset VmHWM."""
 global ACTIVE
 log=job/'logs'/(stem+'.stderr.log');usage=job/'logs'/(stem+'.usage.json')
 cmd=[sys.executable,str(ROOT/'measure.py'),str(usage)]+command
 start=time.monotonic();environment=os.environ.copy();environment['CRDT_TRACE_MEMORY']='0'
 reason=None;observed=0
 with log.open('w') as err:
  p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=err,text=True,cwd=ROOT,start_new_session=True,env=environment);ACTIVE=p
  while True:
   try:stdout,_=p.communicate(timeout=.25);break
   except subprocess.TimeoutExpired:
    try:
     children=pathlib.Path(f'/proc/{p.pid}/task/{p.pid}/children').read_text().split()
     for child in children:
      for line in pathlib.Path(f'/proc/{child}/status').read_text().splitlines():
       if line.startswith('VmRSS:'):observed=max(observed,int(line.split()[1])*1024)
    except (FileNotFoundError,ProcessLookupError):pass
    if STOP or (job/'STOP').exists():reason='stopped'
    elif time.monotonic()>=deadline:reason='budget-limit'
    elif timeout and time.monotonic()-start>timeout:reason='timeout'
    elif observed>memory_limit:reason='memory-limit'
    elif available_memory()<4*1024**3:reason='host-memory-reserve'
    elif shutil.disk_usage(job).free<10*1024**3:reason='host-disk-reserve'
    if reason:
     try:os.killpg(p.pid,signal.SIGKILL)
     except ProcessLookupError:pass
     p.communicate();stdout='';break
 ACTIVE=None
 data={'status':reason or ('ok' if p.returncode==0 else 'failed'),'wall_s':time.monotonic()-start,'command':command,'observed_rss_bytes':observed,'log':str(log.relative_to(job))}
 if usage.exists():data['process']=json.loads(usage.read_text())
 if data['status']=='ok':
  try:data.update(json.loads(stdout.strip().splitlines()[-1]))
  except (ValueError,IndexError):data.update(status='invalid-output',stdout=stdout[-2000:])
 if data['status']!='ok':data['stderr']=log.read_text()[-3000:];data['returncode']=p.returncode
 return data

def worker(lib,recipe,stage,work,allocation=False):
 if lib in NATIVE:
  binary='night-native-memory' if allocation else 'night-native'
  return [str(ROOT/'bin'/binary),'night',lib,str(recipe),stage,str(work)]
 adapter_lib={'automerge-js':'automerge','json-joy-adapter':'json-joy'}.get(lib,lib)
 return ['node','--expose-gc','--max-old-space-size=8192',str(ROOT/'adapter-night.mjs'),adapter_lib,str(recipe),stage,str(work)]

def make_text(path,size,deadline):
 """Streaming ASCII prose with unique random IDs; never constructs huge JSON."""
 if path.exists() and path.stat().st_size==size:return
 words='document collaborative offline storage memory compact merge client server section paragraph project meeting calendar notes version update network table record field index editor cursor insert remove persistent local'.split()
 rand=random.Random(42);written=0;line=0;tmp=path.with_suffix('.tmp')
 with tmp.open('wb') as f:
  while written<size:
   if STOP or time.monotonic()>=deadline:raise TimeoutError('campaign stopped while generating input')
   raw=rand.randbytes(16*8192).hex();lines=[]
   for p in range(0,len(raw),32):
    token=raw[p:p+32];n=int(token[:4],16)
    lines.append(f'{line:09x} '+' '.join(words[(n+j*7)%len(words)] for j in range(8))+' '+token+'\n');line+=1
   b=''.join(lines).encode('ascii')[:size-written];f.write(b);written+=len(b)
 tmp.replace(path)

def phase_peak(d):
 r=d.get('result',{});return max([d.get('process',{}).get('max_rss_kib',0)*1024,d.get('observed_rss_bytes',0)]+[v for k,v in r.items() if k.endswith('peak_rss_bytes')])
def sample_summary(record):
 p=record['phases'];r=lambda stage:p[stage]['result']
 return {'edit_ms':r('server')['edit_ms'],'offline_edit_ms':r('offline')['edit_ms'],'server_merge_ms':r('merge-server')['merge_ms'],'client_merge_ms':r('merge-client')['merge_ms'],'server_merge_peak_rss_bytes':r('merge-server')['merge_peak_rss_bytes'],'client_merge_peak_rss_bytes':r('merge-client')['merge_peak_rss_bytes'],'server_merge_load_peak_rss_bytes':r('merge-server')['load_peak_rss_bytes'],'client_merge_load_peak_rss_bytes':r('merge-client')['load_peak_rss_bytes'],'server_merge_rss_bytes':r('merge-server')['merge_rss_bytes'],'client_merge_rss_bytes':r('merge-client')['merge_rss_bytes'],'init_peak_rss_bytes':phase_peak(p['init']),'server_edit_peak_rss_bytes':r('server')['edit_peak_rss_bytes'],'offline_edit_peak_rss_bytes':r('offline')['edit_peak_rss_bytes'],'server_worker_peak_rss_bytes':phase_peak(p['server']),'offline_worker_peak_rss_bytes':phase_peak(p['offline']),'cold_ms':sum(r('cold')[k] for k in ['load_ms','read_ms','first_edit_ms']),'cold_peak_rss_bytes':phase_peak(p['cold']),'steady_rss_bytes':r('cold')['edited_rss_bytes'],'live_bytes':r('memory' if 'memory' in p else 'cold')['edited_live_bytes'],'best_disk_bytes':r('storage')['best_disk_bytes'],'snapshot_bytes':r('storage')['snapshot_bytes'],'zstd3_bytes':r('storage')['zstd3_bytes'],'upload_bytes':record['payload_bytes']['upload.bin'],'download_bytes':record['payload_bytes']['download-1.bin']+record['payload_bytes']['download-2.bin'],'peak_reset_supported':all(v.get('result',{}).get('peak_reset_supported',True) for v in p.values())}

def report(job):
 import statistics
 raw=json.loads((job/'results.json').read_text());meta=json.loads((job/'metadata.json').read_text());status=json.loads((job/'status.json').read_text())
 groups=collections.defaultdict(list)
 latest={(r['scenario'],r['library'],r['trial']):r for r in raw}
 for r in latest.values():groups[(r['scenario'],r['library'])].append(r)
 rows=[]
 for (case,lib),records in groups.items():
  good=[x for x in records if x['status']=='ok'];row={'scenario':case,'library':lib,'successful':len(good),'attempted':len(records),'failures':[{'trial':x['trial'],'stage':x.get('failed_stage'),'status':x['status']} for x in records if x['status']!='ok']}
  if good:
   for k in good[0]['summary']:
    if k=='peak_reset_supported':row[k]=all(x['summary'][k] for x in good)
    else:row[k]=statistics.median(x['summary'][k] for x in good)
   row['edit_range_ms']=[min(x['summary']['edit_ms'] for x in good),max(x['summary']['edit_ms'] for x in good)]
  rows.append(row)
 atomic(job/'summary.json',rows)
 import csv
 fields=sorted({k for r in rows for k in r if k not in ['failures','edit_range_ms']})
 with (job/'summary.csv').open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
 def ms(v):return f'{v/1000:,.2f} s' if v>=1000 else f'{v:,.1f} ms'
 def mb(v):return f'{v/MIB:,.1f}'
 sections=[]
 for case,recipe in meta['recipes'].items():
  selected=[r for r in rows if r['scenario']==case];tr=[]
  for r in selected:
   if not r['successful']:cells=[LABELS[r['library']],html.escape(str(r['failures']))]+['—']*13
   else:cells=[LABELS[r['library']],ms(r['edit_ms']),ms(r['server_merge_ms']),ms(r['client_merge_ms']),mb(r['init_peak_rss_bytes']),mb(r['server_edit_peak_rss_bytes']),mb(r['offline_edit_peak_rss_bytes']),mb(r['server_merge_peak_rss_bytes']),mb(r['client_merge_peak_rss_bytes']),mb(r['cold_peak_rss_bytes']),mb(r['steady_rss_bytes']),mb(r['best_disk_bytes']),f'{r["successful"]}/{r["attempted"]}',mb(r['upload_bytes']),mb(r['download_bytes'])]
   tr.append('<tr>'+''.join('<td>'+str(c)+'</td>' for c in cells)+'</tr>')
  headers=['Engine','Server edits','Server merge','Client merge','Initial build peak MiB','Server editing peak MiB','Offline editing peak MiB','Server merge peak MiB','Client merge peak MiB','Cold open peak MiB','Steady RSS MiB','Disk MiB','Completed/tried','Upload MiB','Download MiB']
  table='<div class="scroll"><table><tr>'+''.join('<th>'+h+'</th>' for h in headers)+'</tr>'+''.join(tr)+'</table></div>'
  ref=meta.get('references',{}).get(case,{})
  changed=sum(v.get('result',{}).get('inserted_bytes',0) for v in ref.values())
  description=f'{recipe["text_bytes"]/MIB:g} MiB initial text, {recipe["keys"]:,} fields, {recipe["server_edits"]:,} server + {recipe["offline_edits"]:,} offline commands; pattern {recipe["pattern"]}; {changed/MIB:,.1f} MiB total inserted payload. Batch {recipe["batch"]}.'
  sections.append(f'<section data-case="{case}"><h2>{case}</h2><p>{description}</p>{table}</section>')
 details=[]
 for r in raw:
  if r['status']!='ok':details.append({k:r.get(k) for k in ['scenario','library','trial','status','failed_stage','failure']})
 failures='<details><summary>Failures, resource limits and timeouts</summary><pre>'+html.escape(json.dumps(details,indent=2))+'</pre></details>'
 coverage=f'{len(raw)} scenario trials recorded; {sum(r["status"]=="ok" for r in raw)} completed with verified convergence. Planned first-pass trials: {meta["planned_first_pass"]}. Untested combinations remain untested; budget exits are not correctness failures.'
 method='''<p>Native release Rust and Node on Linux; actual CPU affinity is recorded in metadata (the automated campaign pins one logical CPU). Each init, edit, merge, cold-open and verification stage uses a fresh process. Server and client merges each own exactly one document. Edits use identical procedural ASCII traces. A separate non-CRDT Rope reference computes expected states for each local branch. After two server save/reload boundaries and a client save/reload boundary, both replicas must converge and preserve branch sentinels. Duplicate updates are checked in separate verification processes. Reverse-order delivery is not part of this night campaign.</p>
<p>Merge peak: resident-memory high-water mark reset immediately before importing updates, including update buffers and CRDT work. Editing peaks exclude the checkpoint serialization/loading operation itself; checkpoint peaks remain in raw results. Merge peaks start at already-resident post-load memory; cold-load and later persistence peaks are separately recorded in raw phase results. Linux /proc/self/clear_refs=5 resets VmHWM; support is recorded. The worker exit/kernel peak can be lower after a reset, so all phase peaks are retained. Verification/reference peaks are excluded from merge rankings. Lazy work triggered by merging remains inside merge cost.</p>
<p>Cold open: load raw persisted document, read all text/map values, discard the returned view, perform one middle insertion. The table includes the largest cold-stage peak and final process RSS, not just retained CRDT heap. Native live heap is measured in a separate allocation-instrumented process; JavaScript post-GC heap + ArrayBuffers is approximate and not directly comparable. JavaScript forces GC at load/reload boundaries, outside load timing; total stage wall time includes it.</p>
<p>Disk: smaller of native saved bytes and a lossless Zstd-3 wrapper; cold loads use raw bytes. Loro full operation exports retain all history; no shallow snapshots, CRDT resets or destructive history pruning. Yrs/Yjs GC and wire-format settings are encoded in each variant name (native Yrs defaults to GC and V2, with explicit V1 and no-GC variants). Snapshot-tail variants use an application frame containing a full base snapshot and the incremental tail, without deleting CRDT metadata. Native Automerge tuned variants use 64 KiB initial transactions; the bounded adapter uses compressed native changes imported 32 at a time with causal delivery. Diamond Types is text-only in this harness. All inputs are synthetic ASCII with unique random line IDs; mixed fields are flat scalars. No Flutter bridge, mobile/Web builds, rich text, nested structures, network, database fsync, UI, concurrent-server load or real elapsed month is measured. Inputs differ from the earlier small campaign; compare engines within this campaign. Small timing differences and production tail latency require more samples.</p>'''
 limits=f'<p>Global budget: {meta["hours"]} h; phase timeout: {meta["phase_timeout_s"]} s; trial budget: {meta["trial_timeout_s"]} s; sampled per-process RSS guard: {meta["memory_limit_bytes"]/1024**3:g} GiB; host reserves: 4 GiB available RAM and 10 GiB free disk. Guards may overshoot between samples. First pass prioritizes coverage; subsequent passes repeat successful combinations up to {meta["repeats"]} trials. Failed configurations are not retried in later passes. A null global budget and zero phase/trial limits disable all time-based stops. Earlier attempts remain in raw results; summaries use the latest attempt per trial. Work files are cleaned after every trial; timings/results/logs remain.</p>'
 options='<option value="all">All workloads</option>'+''.join(f'<option value="{html.escape(c)}">{html.escape(c)}</option>' for c in meta['recipes'])
 doc='<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Large offline-first CRDT benchmark</title><style>body{font:16px/1.5 system-ui;background:#f1f4f7;color:#142336;margin:0}main{max-width:1600px;margin:auto;padding:24px}section,.intro{background:white;border:1px solid #dae1e8;border-radius:8px;padding:20px;margin:20px 0}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:10px;text-align:right;border-bottom:1px solid #ddd;white-space:nowrap}td:first-child,th:first-child{text-align:left}th{background:#edf3f9}pre{overflow:auto;white-space:pre-wrap}select{font:inherit;padding:8px}a{color:#0759a5}</style><main><h1>Large offline-first CRDT benchmark</h1><div class="intro"><p><strong>Status: '+html.escape(status['state'])+'</strong>; updated '+html.escape(status['updated'])+'</p><p>'+coverage+'</p>'+method+limits+'<p><a href="summary.csv">CSV medians</a> · <a href="summary.json">Summary</a> · <a href="results.json">All raw phase results</a> · <a href="metadata.json">Environment, inputs, source hashes</a> · <a href="status.json">Current status</a></p></div><label>Workload: <select id="case">'+options+'</select></label>'+''.join(sections)+failures+'</main><script>document.querySelector("#case").addEventListener("change",e=>{for(const s of document.querySelectorAll("[data-case]"))s.hidden=e.target.value!=="all"&&s.dataset.case!==e.target.value})</script></html>'
 tmp=job/'report.html.tmp';tmp.write_text(doc);tmp.replace(job/'report.html')

class Failure(Exception):
 def __init__(self,stage,data):self.stage=stage;self.data=data

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--hours',type=float,default=8);ap.add_argument('--memory-gib',type=float,default=12);ap.add_argument('--phase-timeout',type=int,default=600);ap.add_argument('--trial-timeout',type=int,default=1800);ap.add_argument('--repeats',type=int,default=3);ap.add_argument('--smoke',action='store_true');ap.add_argument('--job-dir');ap.add_argument('--report',action='store_true');ap.add_argument('--libraries',nargs='+',default=LIBS);ap.add_argument('--profile',choices=['standard','large'],default='large')
 ap.add_argument('--resume',action='store_true');ap.add_argument('--no-time-limit',action='store_true')
 args=ap.parse_args()
 if args.no_time_limit:args.phase_timeout=0;args.trial_timeout=0
 job=pathlib.Path(args.job_dir).resolve() if args.job_dir else ROOT/'night-runs'/datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');job.mkdir(parents=True,exist_ok=True)
 if args.report:report(job);return
 assert all(lib in LIBS for lib in args.libraries)
 for p in ['logs','inputs','work']: (job/p).mkdir(exist_ok=True)
 cases=SMOKE if args.smoke else STANDARD if args.profile=='standard' else CASES;deadline=float('inf') if args.no_time_limit else time.monotonic()+args.hours*3600;started=utc();records=[];references={};recipes={}
 previous=json.loads((job/'metadata.json').read_text()) if args.resume and (job/'metadata.json').exists() else None
 if previous:
  records=json.loads((job/'results.json').read_text());references=previous['references'];recipes=previous['recipes'];started=previous['started']
 signal.signal(signal.SIGTERM,request_stop);signal.signal(signal.SIGINT,request_stop)
 sources=['src/main.rs','src/night.rs','bench-js.mjs','night-js.mjs','overnight.py','factories.mjs','adapter-night.mjs','Cargo.toml','Cargo.lock','../package-lock.json','bin/night-native','bin/night-native-memory']
 metadata={'started':started,'deadline_utc':None if args.no_time_limit else (datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=args.hours)).isoformat(),'hours':None if args.no_time_limit else args.hours,'memory_limit_bytes':int(args.memory_gib*1024**3),'phase_timeout_s':args.phase_timeout,'trial_timeout_s':args.trial_timeout,'repeats':args.repeats,'platform':platform.platform(),'cpu':subprocess.check_output(['lscpu'],text=True),'cpu_affinity':sorted(os.sched_getaffinity(0)),'node':subprocess.check_output(['node','--version'],text=True).strip(),'rustc':subprocess.check_output(['rustc','+1.96.0','--version'],text=True).strip(),'source_sha256':{p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in sources},'recipes':recipes,'references':references,'libraries':args.libraries,'planned_first_pass':sum(not (lib in ['diamond','diamond-types'] and c[2]) for c in cases for lib in args.libraries),'versions':{'yrs':'0.28.0','loro':'1.16.2','automerge':'0.12.0','diamond-types':'1.0.0','yjs':'13.6.33','json-joy':'18.30.0','ywasm':'0.28.0','loro-crdt':'1.16.4','automerge-js':'3.5.0','diamond-types-node':'1.0.2'}}
 if previous:
  metadata['resumptions']=previous.get('resumptions',[])+[{'at':utc(),'prior_policy':{k:previous.get(k) for k in ['deadline_utc','hours','phase_timeout_s','trial_timeout_s']},'prior_source_sha256':previous.get('source_sha256',{})}]
 def status(state,**extra):atomic(job/'status.json',{'state':state,'pid':os.getpid(),'started':started,'updated':utc(),'deadline_utc':metadata['deadline_utc'],'remaining_s':None if args.no_time_limit else max(0,deadline-time.monotonic()),'recorded':len(records),**extra})
 def flush():atomic(job/'results.json',records);atomic(job/'metadata.json',metadata);report(job)
 status('starting');flush();latest={(r['scenario'],r['library'],r['trial']):r for r in records};retryable={'timeout','trial-timeout','budget-limit','stopped','host-memory-reserve','host-disk-reserve'};bad={(r['scenario'],r['library']) for r in latest.values() if r['status']!='ok' and r['status'] not in retryable};aborted=False
 try:
  for trial in range(1,args.repeats+1):
   for index,case in enumerate(cases):
    name,size,keys,server,offline,pattern,churn=case
    if STOP or (job/'STOP').exists() or time.monotonic()>=deadline:aborted=True;break
    if name not in recipes:
     status('generating-input',scenario=name);text=job/'inputs'/f'text-{size}.txt';make_text(text,size,deadline)
     recipe={'text_path':str(text),'text_bytes':size,'keys':keys,'server_edits':server,'offline_edits':offline,'pattern':pattern,'churn_bytes':churn,'batch':32}
     recipes[name]=recipe;recipe_path=job/'inputs'/f'{name}.json';atomic(recipe_path,recipe);references[name]={};flush()
     for branch in ['server','offline']:
      stage='reference-'+branch;status('reference',scenario=name,stage=stage)
      ref=measured(worker('yrs',recipe_path,stage,job/'work/reference'),name+'-'+stage,job,deadline,args.phase_timeout,metadata['memory_limit_bytes']);references[name][branch]=ref
      if ref['status']!='ok':raise Failure(stage,ref)
     flush()
    recipe_path=job/'inputs'/f'{name}.json'
    for branch in ['server','offline']:
     if references.get(name,{}).get(branch,{}).get('status')!='ok':
      stage='reference-'+branch;status('reference',scenario=name,stage=stage)
      ref=measured(worker('yrs',recipe_path,stage,job/'work/reference'),name+'-'+stage,job,deadline,args.phase_timeout,metadata['memory_limit_bytes']);references.setdefault(name,{})[branch]=ref;flush()
      if ref['status']!='ok':raise Failure(stage,ref)
    libs=args.libraries[(index+trial-1)%len(args.libraries):]+args.libraries[:(index+trial-1)%len(args.libraries)]
    for lib in libs:
     if lib in ['diamond','diamond-types'] and keys:continue
     if (name,lib) in bad:continue
     prior=latest.get((name,lib,trial))
     if prior and prior['status']=='ok':continue
     if STOP or (job/'STOP').exists() or time.monotonic()>=deadline:aborted=True;break
     work=job/'work'/f'{name}-{lib}-{trial}';work.mkdir(parents=True,exist_ok=True);attempt=1+sum(r['scenario']==name and r['library']==lib and r['trial']==trial for r in records);stem=f'{name}-{lib}-{trial}' + (f'-attempt{attempt}' if attempt>1 else '');record={'scenario':name,'library':lib,'trial':trial,'attempt':attempt,'started':utc(),'status':'running','phases':{}};recorded_start=time.monotonic();sample_deadline=min(deadline,recorded_start+args.trial_timeout) if args.trial_timeout else deadline
     print(f'{utc()} START {stem}',flush=True)
     try:
      stages=['init','server','check-server','offline','check-offline','merge-server','merge-client','check-merged-server','check-merged-client','cold','storage']
      if lib in NATIVE:stages.append('memory')
      for stage in stages:
       status('running',scenario=name,library=lib,trial=trial,stage=stage)
       cmd=worker(lib,recipe_path,'cold' if stage=='memory' else stage,work,allocation=stage=='memory')
       data=measured(cmd,stem+'-'+stage,job,sample_deadline,args.phase_timeout,metadata['memory_limit_bytes']);record['phases'][stage]=data
       with (job/'phases.jsonl').open('a') as f:f.write(json.dumps({'scenario':name,'library':lib,'trial':trial,'stage':stage,'data':data})+'\n');f.flush();os.fsync(f.fileno())
       if data['status']!='ok':raise Failure(stage,data)
       if stage in ['check-server','check-offline']:
        branch=stage.split('-')[1]
        if data['result']['hash']!=references[name][branch]['result']['hash']:raise Failure(stage,{'status':'local-reference-mismatch','actual':data['result'],'expected':references[name][branch]['result']})
       if stage=='check-merged-client':
        if data['result']['hash']!=record['phases']['check-merged-server']['result']['hash']:raise Failure(stage,{'status':'convergence-mismatch','actual':data['result']})
      record['payload_bytes']={p:(work/p).stat().st_size for p in ['upload.bin','download-1.bin','download-2.bin']};record['status']='ok';record['converged']=True;record['summary']=sample_summary(record)
     except Failure as e:
      record.update(status=e.data['status'],failed_stage=e.stage,failure=e.data);bad.add((name,lib))
      if e.data['status']=='budget-limit' and time.monotonic()<deadline:
       record['status']='trial-timeout'
      elif e.data['status'] in ['stopped','budget-limit','host-memory-reserve','host-disk-reserve']:aborted=True
     except Exception as e:
      record.update(status='harness-error',failure=repr(e));bad.add((name,lib))
     finally:
      record['wall_s']=time.monotonic()-recorded_start;record['finished']=utc();records.append(record);shutil.rmtree(work,ignore_errors=True);status('running',last_result={k:record.get(k) for k in ['scenario','library','trial','status','failed_stage']});flush();print(f'{utc()} END {stem}: {record["status"]} ({record["wall_s"]:.1f}s)',flush=True)
     if aborted:break
    if aborted:break
   if aborted:break
  status('stopped' if STOP or (job/'STOP').exists() else 'budget-ended' if aborted else 'complete');flush()
 except (Failure,TimeoutError) as e:
  status('budget-ended' if time.monotonic()>=deadline else 'reference-or-input-error',error=str(e),detail=e.data if isinstance(e,Failure) else None);flush()
 except Exception as e:
  status('harness-error',error=repr(e));flush();raise
 finally:
  if ACTIVE and ACTIVE.poll() is None:
   try:os.killpg(ACTIVE.pid,signal.SIGKILL)
   except ProcessLookupError:pass
  shutil.rmtree(job/'work',ignore_errors=True)
 print(f'{utc()} REPORT {job / "report.html"}',flush=True)
 if args.smoke:
  final={(r['scenario'],r['library'],r['trial']):r for r in records}
  if len(final)!=metadata['planned_first_pass']*args.repeats or any(r['status']!='ok' for r in final.values()):sys.exit(1)
if __name__=='__main__':main()

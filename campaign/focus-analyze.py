#!/usr/bin/env python3
"""Compare only equivalent workloads; preserve resource-limited outcomes explicitly."""
import argparse,collections,csv,datetime,html,json,pathlib,statistics
import overnight as n
NAMES={'yrs':'Yrs V2 / GC','loro':'Loro full snapshot','loro-updates':'Loro full operations'}
def median(values):return statistics.median(values) if values else None

def analyze(job,followup=None):
 raw=json.loads((job/'results.json').read_text());meta=json.loads((job/'metadata.json').read_text());cost=[];latency=[];operation=[];diagnostic=[]
 for lib in NAMES:
  core=next((r for r in raw if r['library']==lib and r['case']=='cluster'),None)
  if not core:continue
  p=core['phases'];h=p.get('history',{}).get('result',{});points=h.get('points',core.get('partial_history_points',[]));base=h.get('baseline_storage')
  for point in ([{'inserted_bytes':0,'storage':base}] if base else [])+points:
   s=point['storage'];cost.append({'library':lib,'history_inserted_bytes':point['inserted_bytes'],'history_deleted_bytes':point.get('deleted_bytes',point['inserted_bytes']),'raw_bytes':s['raw_bytes'],'zstd3_bytes':s['zstd3_bytes'],'compressed_roundtrip_pass':s['compressed_roundtrip_pass'],'offline_roundtrip_status':core['status'],'cost_scope':'server branch before offline return; full causal data retained'})
  for stage in ['server','offline','merge-server-history','merge-client-history','cold-after-history']:
   phase=p.get(stage,{});r=phase.get('result',{})
   if r:
    operation.append({'library':lib,'case':'cluster + 64 MiB history','stage':stage,'phase_wall_s':phase['wall_s'],'load_ms':r.get('load_ms'),'edit_ms':r.get('edit_ms'),'merge_ms':r.get('merge_ms'),'read_ms':r.get('read_ms'),'first_edit_ms':r.get('first_edit_ms'),'save_ms':r.get('save_ms'),'processing_peak_rss_bytes':r.get('edit_peak_rss_bytes',r.get('merge_peak_rss_bytes')),'phase_peak_rss_bytes':n.phase_peak(phase),'edited_rss_bytes':r.get('edited_rss_bytes')})
  good=[r for r in raw if r['library']==lib and r['status']=='ok' and r['phases'].get('probe',{}).get('status')=='ok']
  # Probes always load the same unedited base, regardless of cluster/random branch scenario.
  for group in sorted({k for r in good for k in r['phases']['probe']['result']['latencies']}):
   qs=[r['phases']['probe']['result']['latencies'][group] for r in good]
   latency.append({'library':lib,'group':group,'runs':len(qs),'operations_per_run':qs[0]['samples'],'median_of_run_medians_ms':median([q['median_ms'] for q in qs]),'median_of_run_p95_ms':median([q['p95_ms'] for q in qs]),'median_of_run_p99_ms':median([q['p99_ms'] for q in qs]),'maximum_observed_ms':max(q['max_ms'] for q in qs),'median_operation_peak_rss_bytes':median([r['phases']['probe']['result']['operation_peak_rss_bytes'][group] for r in good])})
  random=[r for r in raw if r['library']==lib and r['case']=='random' and r['status']=='ok']
  for stage in ['server','offline','merge-server-history','merge-client-history','cold-after-history']:
   if not random:continue
   phases=[r['phases'][stage] for r in random];rs=[p['result'] for p in phases]
   operation.append({'library':lib,'case':'random, no extra churn history','stage':stage,'runs':len(rs),'phase_wall_s':median([p['wall_s'] for p in phases]),**{k:median([r[k] for r in rs if k in r]) for k in ['load_ms','edit_ms','merge_ms','read_ms','first_edit_ms','save_ms','edit_peak_rss_bytes','merge_peak_rss_bytes','edited_rss_bytes']},'phase_peak_rss_bytes':median([n.phase_peak(p) for p in phases])})
 if followup and (followup/'results.json').exists():
  diagnostic=json.loads((followup/'results.json').read_text())
 summary={'generated':n.utc(),'scope':'100 MiB actual text and 6000 scalar fields, synthetic ASCII; native Linux Rust; no Flutter/Web integration','coverage':dict(collections.Counter(r['status'] for r in raw)),'core_status':{r['library']:{'status':r['status'],'failed_stage':r.get('failed_stage'),'failure':r.get('failure'),'performance_limits':r.get('performance_limits',[])} for r in raw if r['case']=='cluster'},'versions':meta['versions'],'cost':cost,'latency':latency,'operations':operation,'diagnostic16':diagnostic,'latency_aggregation':'Median of per-run quantiles. Probes use identical unedited 100-MiB base and operation stream. Not a pooled p99; single-sample groups have no meaningful tail estimate.','storage_scope':'A single current full encoded CRDT payload. Test fixtures/replicas and both raw/Zstd alternatives are not cumulative production storage. Unapplied deltas, DB overhead, backups and replicas are extra. No required history is pruned.'}
 n.atomic(job/'comparison.json',summary)
 allrows=[{'category':'cost',**r} for r in cost]+[{'category':'latency',**r} for r in latency]+[{'category':'operations',**r} for r in operation]
 with (job/'comparison.csv').open('w',newline='') as f:
  keys=sorted({k for r in allrows for k in r});w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(allrows)
 def text_table(headers,rows):
  return '| '+' | '.join(headers)+' |\n| '+' | '.join(['---']*len(headers))+' |\n'+''.join('| '+' | '.join(str(v) for v in row)+' |\n' for row in rows)
 notes=['FOCUSED 100-MiB CRDT DECISION COMPARISON','Generated '+n.utc(),'','All three tested with the same actual 100-MiB text and 6000 scalar fields. Native Rust versions verified immediately before this run. Single logical CPU on Ryzen 7 3700X.','Core history replaces the same 128 KiB 512 times (64 MiB inserted AND deleted); visible size remains near 100 MiB. The old client branches before server edits and this history. This simulates causal divergence, not an elapsed calendar interval.','Server storage below is the equivalent server-only state before offline return. All required causal data is retained; optional Zstd-3 is lossless and verified. Baseline includes initial 6000 server commands.','',text_table(['Configuration','Replaced MiB','Raw MiB','Zstd MiB'],[[NAMES[r['library']],f"{r['history_inserted_bytes']/2**20:.0f}",f"{r['raw_bytes']/2**20:.2f}",f"{r['zstd3_bytes']/2**20:.2f}"] for r in cost]),'Core correctness/resource status:']
 for lib,status in summary['core_status'].items():
  notes.append(f"  {NAMES[lib]}: {status['status']}; interrupted stage {status['failed_stage']}")
  if status['failure']:notes.append('    '+status['failure']['status']+f"; observed process RSS {status['failure'].get('observed_rss_bytes',0)/2**30:.2f} GiB")
 notes+=['','Single-operation latency on the unedited 100-MiB base. Local and random groups have 256 and 512 samples per run. Numbers are medians across run-specific medians/p99 values; not a pooled p99. First-edit/paste/delete groups have one sample per run.','',text_table(['Configuration','Group','Runs','Median ms','Median run-p99 ms','Peak MiB'],[[NAMES[r['library']],r['group'],r['runs'],f"{r['median_of_run_medians_ms']:.3f}",f"{r['median_of_run_p99_ms']:.3f}",f"{r['median_operation_peak_rss_bytes']/2**20:.1f}"] for r in latency]),'','Merge and cold-open costs must include load and any later materialization: a fast import alone can defer substantial work. The raw comparison.json records phase wall times, load, merge, read, first-edit and save separately.','']
 for r in operation:
  notes.append(f"{NAMES[r['library']]} / {r['case']} / {r['stage']}: wall {r['phase_wall_s']:.3f} s, load {r.get('load_ms')}, edit {r.get('edit_ms')}, merge {r.get('merge_ms')}, read {r.get('read_ms')}, first edit {r.get('first_edit_ms')}, save {r.get('save_ms')} ms, phase peak {r['phase_peak_rss_bytes']/2**20:.1f} MiB, post-edit RSS {(r.get('edited_rss_bytes') or 0)/2**20:.1f} MiB")
 notes+=['','Smaller-history diagnostic (16 MiB; separate workload, not a substitute for the failed 64-MiB case):']
 for r in diagnostic:notes.append(f"  {r['library']}: {r['status']}, failed stage {r.get('failed_stage')}; complete phase data in comparison.json and follow-up report.")
 notes+=['','Interpretation: use measured bytes after churn, not initial saved size alone. History growth is workload-dependent; this pattern does not establish a universal bound for GC or a long-term compression ratio.','A RAM/time guard is an incomplete operational result, not proof of a correctness defect. Never rank an incomplete variant as fully validated.','Only successful full paths verify local Rope reference hashes, merged replica hashes, independent sentinels and duplicate/reverse delivery.','Process RSS includes allocator-retained pages, runtime and buffers. Timing includes OS scheduling; page cache was not flushed. One process owns one replica, references run separately.','No measured Flutter bridge/UI, Unicode, rich-text schema, database indexes, backups, replication, encrypted storage or crash-recovery protocol. App/library executable size is not part of these figures.']
 (job/'COMPARISON.txt').write_text('\n'.join(notes)+'\n')
 def table(headers,rows):return '<div class="scroll"><table><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr>'+''.join('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in row)+'</tr>' for row in rows)+'</table></div>'
 tables=table(['Configuration','Replaced MiB','Raw MiB','Zstd MiB'],[[NAMES[r['library']],r['history_inserted_bytes']/2**20,round(r['raw_bytes']/2**20,2),round(r['zstd3_bytes']/2**20,2)] for r in cost])+table(['Configuration','Operation','Runs','Median ms','Median run-p99 ms','Peak MiB'],[[NAMES[r['library']],r['group'],r['runs'],round(r['median_of_run_medians_ms'],3),round(r['median_of_run_p99_ms'],3),round(r['median_operation_peak_rss_bytes']/2**20,1)] for r in latency])
 (job/'comparison.html').write_text('<!doctype html><meta charset="utf-8"><title>100 MiB CRDT comparison</title><style>body{font:16px/1.5 system-ui;max-width:1600px;margin:30px auto;padding:20px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:right}td:first-child,th:first-child{text-align:left}pre{white-space:pre-wrap}</style><h1>100 MiB CRDT comparison</h1><p><a href="comparison.json">Normalized data</a> · <a href="comparison.csv">CSV</a> · <a href="results.json">Raw main run</a></p>'+tables+'<pre>'+html.escape('\n'.join(notes))+'</pre>')
 return summary

if __name__=='__main__':
 ap=argparse.ArgumentParser();ap.add_argument('--job-dir',required=True);ap.add_argument('--followup-dir');args=ap.parse_args();s=analyze(pathlib.Path(args.job_dir),pathlib.Path(args.followup_dir) if args.followup_dir else None);print('Compared',s['coverage'])

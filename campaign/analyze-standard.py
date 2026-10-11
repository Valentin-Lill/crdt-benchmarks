#!/usr/bin/env python3
"""Generate the final analysis exclusively from completed standard measurements."""
import argparse,collections,datetime,html,json,pathlib
import overnight as night

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--job-dir',required=True);args=ap.parse_args();job=pathlib.Path(args.job_dir).resolve()
 raw=json.loads((job/'standard-offline/results.json').read_text());up=json.loads((job/'upstream-results.json').read_text());up=[r for r in up if r['test']!='B4x100']
 assert len(raw)==267 and all(r['status']=='ok' for r in raw)
 assert len(up)==1123 and all(r['status']=='ok' for r in up)
 rows=json.loads((job/'standard-offline/summary.json').read_text());urows=[r for r in json.loads((job/'upstream-summary.json').read_text()) if r['test']!='B4x100']
 metadata=json.loads((job/'metadata.json').read_text());versions=json.loads((night.ROOT/'VERSIONS.json').read_text())
 analysis={'generated':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Completed standard tests only; expanded tests excluded','coverage':{'smoke_passed':89,'standard_offline_passed':267,'standard_upstream_passed':1123,'native_variants':13,'js_wasm_variants':18,'offline_repeats':3,'native_real_trace_repeats':1},'versions':versions,'environment':{'affinity':metadata['affinity'],'node':metadata['node'],'rustc':metadata['rustc'],'cpu':metadata['cpu']},'offline':rows,'upstream':urows}
 night.atomic(job/'standard-analysis.json',analysis)
 notes='''STANDARD CRDT BENCHMARK ANALYSIS
All planned standard checks passed: 89 smoke, 267 isolated offline, 1123 upstream.
13 native Rust variants and 18 JS/WASM variants. Version verification: 2026-10-10.
Only completed standard results are included; no partial large-test ranking.

Offline workloads: 104852 bytes ASCII text, 0 or 6000 flat scalar fields,
6000 server commands and 1500 offline-client commands. Text-only, mixed and
random scenarios, three samples each; reported values are medians.
One logical CPU on Ryzen 7 3700X Linux; compiled release Rust and Node.

In standard mixed data, native Yrs V2+GC: edits 15.7 ms, server merge 1.2 ms,
steady process RSS 8.3 MiB, raw saved state 288.0 KiB, Zstd-3 66.2 KiB.
Native Loro full operations: edits 5.6 ms, server merge 5.2 ms, steady RSS
9.8 MiB, raw 385.5 KiB, Zstd-3 82.6 KiB; cold open 15.4 ms.
Native Loro full snapshot: edits 16.8 ms, server merge 16.3 ms, steady RSS
9.1 MiB, raw 272.2 KiB, Zstd-3 180.5 KiB; cold open 8.5 ms.
Native Automerge full save: edits 137.3 ms, server merge 49.1 ms, steady RSS
10.7 MiB, raw 108.8 KiB; extra Zstd has no useful reduction here.

Random mixed edits: Yrs 92.9 ms vs Loro full operations 6.5 ms, Loro full
snapshot 19.8 ms and Automerge 144.9 ms. Editing rankings depend on workload.
Native upstream real editing trace, 259778 edits (ONE sample per variant):
Yrs 17694.8 ms; Loro full snapshot 353.6 ms; Automerge 3259.3 ms;
Diamond Types 19.7 ms. Native trace omits per-operation wire serialization;
compare native engines within this test, not directly with JS trace timings.
Diamond Types covers text only in this harness and is not a complete mixed
text/structured-data candidate based on these measurements.

Tail means a full CRDT base snapshot plus persisted incremental changes in
an application frame. It does not mean destructive history pruning. Tail
variants are not uniformly better; native Automerge tail doubles steady RAM
in the mixed workload and slows cold open. Every variant retained the causal
metadata needed by the tested divergent offline client. Loro full snapshots
passed persistence/offline merge checks; shallow snapshots were not tested.
A virtual offline month is represented by divergent states and save/reload
boundaries, not an actual elapsed month or every possible production history.

RSS includes runtime and buffers. Editing/merge peaks and final resident RAM
are separate; each phase owns one replica. It is valid for an edit peak to be
lower than cold-open final RSS, because these belong to separate processes.
Node/WASM results include Node overhead and do not predict native Flutter
process RAM. Zstd is optional lossless compression of saved state; database
indexes, fsync and storage wrappers were not measured. Flutter bridge costs,
mobile/Web builds, Unicode and production schemas were not measured.

The standard results keep Yrs and Loro as the principal native candidates:
Yrs for low RAM/quick merges; Loro for text-editing-heavy workloads. There is
no validated overall winner for huge documents or all Flutter platforms from
this standard suite alone. Exact variant metrics are in the HTML/JSON/CSV.
'''
 (job/'STANDARD-ANALYSIS.txt').write_text(notes)
 def table(items,fields):
  return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+html.escape(label)+'</th>' for label,_ in fields)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(str(fn(r)))+'</td>' for _,fn in fields)+'</tr>' for r in items)+'</tbody></table></div>'
 fields=[('Variant',lambda r:r['library']),('Edit ms',lambda r:f"{r['edit_ms']:.2f}"),('Server merge ms',lambda r:f"{r['server_merge_ms']:.2f}"),('Client merge ms',lambda r:f"{r['client_merge_ms']:.2f}"),('Cold load/read/edit ms',lambda r:f"{r['cold_ms']:.2f}"),('Steady RSS MiB',lambda r:f"{r['steady_rss_bytes']/2**20:.2f}"),('Editing peak MiB',lambda r:f"{r['server_edit_peak_rss_bytes']/2**20:.2f}"),('Server merge peak MiB',lambda r:f"{r['server_merge_peak_rss_bytes']/2**20:.2f}"),('Raw saved KiB',lambda r:f"{r['snapshot_bytes']/1024:.2f}"),('Zstd-3 KiB',lambda r:f"{r['zstd3_bytes']/1024:.2f}"),('Upload KiB',lambda r:f"{r['upload_bytes']/1024:.2f}"),('Download KiB',lambda r:f"{r['download_bytes']/1024:.2f}")]
 sections=[]
 for scenario in ['standard-mixed','standard-random','standard-text']:
  for native in [True,False]:
   rs=sorted([r for r in rows if r['scenario']==scenario and (r['library'] in night.NATIVE)==native],key=lambda r:r['edit_ms'])
   sections.append('<h2>'+scenario+' — '+('native Rust' if native else 'JS/WASM')+'</h2>'+table(rs,fields))
 native=[r for r in urows if r['test']=='B4-native']
 sections.append('<h2>Real editing trace: native Rust (one sample each)</h2>'+table(sorted(native,key=lambda r:r['edit_ms']),[('Variant',lambda r:r['library']),('Edit ms',lambda r:round(r['edit_ms'],2)),('Load ms',lambda r:round(r['load_ms'],2)),('Process peak RSS MiB',lambda r:round(r['peak_rss_bytes']/2**20,2)),('Raw saved KiB',lambda r:round(r['snapshot_bytes']/1024,2))]))
 for test in sorted({r['test'] for r in urows if r['test']!='B4-native'}):
  rs=[r for r in urows if r['test']==test]
  sections.append('<details><summary>'+test+' — all JS/WASM variants</summary>'+table(rs,[('Variant',lambda r:r['library']),('Process peak RSS MiB',lambda r:round(r['peak_rss_bytes']/2**20,2)),('Metrics',lambda r:'; '.join(f'{k}: {v}' for k,v in r.items() if k.startswith('[')))])+'</details>')
 vtable=table(versions['packages'],[('Runtime',lambda r:r['registry']),('Package',lambda r:r['package']),('Version',lambda r:r['version'])])
 doc='<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Completed standard CRDT results</title><style>body{font:16px/1.5 system-ui;margin:30px auto;padding:20px;max-width:1700px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}td,th{border-bottom:1px solid #ddd;padding:8px;text-align:right;white-space:nowrap}td:first-child,th:first-child{text-align:left}pre{white-space:pre-wrap}details{margin:20px 0}a{color:#0759a5}</style><h1>Completed standard CRDT results</h1><p>All standard tests completed successfully. Expanded tests were stopped at the user\'s request and are excluded from this analysis.</p><p><a href="standard-analysis.json">Normalized data</a> · <a href="standard-offline/summary.csv">Offline medians CSV</a> · <a href="standard-offline/results.json">Raw offline measurements</a> · <a href="upstream-results.json">Raw upstream measurements (includes clearly labeled partial expanded tests)</a></p><pre>'+html.escape(notes)+'</pre><h2>Exact versions</h2>'+vtable+''.join(sections)+'</html>'
 (job/'standard-analysis.html').write_text(doc)
 print('Generated',job/'standard-analysis.html')

if __name__=='__main__':main()

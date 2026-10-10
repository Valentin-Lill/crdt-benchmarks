import fs from 'node:fs';
import assert from 'node:assert/strict';
import {performance} from 'node:perf_hooks';
import {zstdCompressSync, constants} from 'node:zlib';
const [mode,lib,input,output] = process.argv.slice(2);
// Import only the tested implementation so baseline RSS includes its actual runtime.
const Y=lib==='yjs'?await import('yjs'):null;
const Model=lib==='json-joy'?(await import('json-joy/lib/json-crdt/model/Model.js')).Model:null;
const Delta=lib==='json-joy'?(await import('json-joy/lib/json-crdt/delta/Delta.js')).Delta:null;
const s=lib==='json-joy'?(await import('json-joy/lib/json-crdt-patch/schema.js')).s:null;
const now=()=>performance.now();
const actor=n=>100+n; // json-joy reserves small session identifiers.
function empty(n) {
  if(lib==='yjs') {const d=new Y.Doc({gc:true});d.clientID=actor(n);return d;}
  const d=Model.create(undefined,actor(n));d.api.set({text:'',map:{}});d.api.flush();return d;
}
function load(bytes,n) {
  if(lib==='yjs') {const d=empty(n);Y.applyUpdateV2(d,bytes);return d;}
  return Model.load(bytes,actor(n));
}
const save=d=>lib==='yjs'?Y.encodeStateAsUpdateV2(d):d.toBinary();
const version=d=>lib==='yjs'?Y.encodeStateVector(d):d.clock.clone();
const delta=(d,v)=>lib==='yjs'?Y.encodeStateAsUpdateV2(d,v):d.delta(v).toBinary();
function applyUpdate(d,b) {if(lib==='yjs')Y.applyUpdateV2(d,b);else d.applyDelta(Delta.from(b));}
function apply(d,ops) {
  if(lib==='yjs') {
    const text=d.getText('text'),map=d.getMap('map');
    d.transact(()=>{for(const op of ops) {
      if(op.kind==='text') {if(op.delete)text.delete(op.pos,op.delete);if(op.insert)text.insert(op.pos,op.insert);}
      else if(op.kind==='set')map.set(op.key,op.value);
      else map.delete(op.key);
    }});
  }else {
    const text=d.api.str('text'),map=d.api.obj('map');
    d.api.transaction(()=>{for(const op of ops) {
      if(op.kind==='text') {if(op.delete)text.del(op.pos,op.delete);if(op.insert)text.ins(op.pos,op.insert);}
      else if(op.kind==='set')map.set({[op.key]:s.con(op.value)});
      else map.del([op.key]);
    }});
    // Do not retain an application operation history in the API patch builder.
    d.api.flush();
  }
}
function init(d,state,batch) {
  apply(d,[{kind:'text',pos:0,delete:0,insert:state.text}]);
  const ops=Object.entries(state.map).map(([key,value])=>({kind:'set',key,value}));
  for(let i=0;i<ops.length;i+=Math.max(batch,1024))apply(d,ops.slice(i,i+Math.max(batch,1024)));
}
function state(d) {
  const v=lib==='yjs'?{text:d.getText('text').toString(),map:d.getMap('map').toJSON()}:d.view();
  return {text:v.text,map:Object.fromEntries(Object.keys(v.map).sort().map(k=>[k,v.map[k]]))};
}
function equal(d,expected,msg) {
  const v=state(d);
  assert(v.text===expected.text,msg+' (text)');
  assert(JSON.stringify(v.map)===JSON.stringify(Object.fromEntries(Object.keys(expected.map).sort().map(k=>[k,expected.map[k]]))),msg+' (map)');
}
function edit(d,ops,batch) {
  const l=[],start=now();
  for(let i=0;i<ops.length;i+=batch){const t=now();apply(d,ops.slice(i,i+batch));l.push(now()-t);}
  const elapsed=now()-start;l.sort((a,b)=>a-b);
  return [elapsed,l[Math.floor(l.length/2)],l[Math.min(l.length-1,Math.floor(l.length*.99))]];
}
if(mode==='run') {
  const f=JSON.parse(fs.readFileSync(input,'utf8'));
  const baseline_rss_bytes=process.memoryUsage().rss;
  let d=empty(1),t=now();init(d,f.initial,f.batch);const init_ms=now()-t;equal(d,f.initial,'initial state differs');
  const common=version(d),base=save(d);d=null;
  let server=load(base,2),offline=load(base,3);
  const split=Math.floor(f.server.length/2);
  const a=edit(server,f.server.slice(0,split),f.batch),midway=version(server),delta1=delta(server,common);
  t=now();let compact=save(server);const checkpoint_save_ms=now()-t;server=null;
  t=now();server=load(compact,4);const checkpoint_load_ms=now()-t;compact=null;
  const b=edit(server,f.server.slice(split),f.batch);equal(server,f.server_expected,'server trace differs');
  const delta2=delta(server,midway);
  t=now();compact=save(server);const compact_ms=now()-t,pre_merge_snapshot_bytes=compact.length;server=null;
  t=now();server=load(compact,5);const reload_ms=now()-t;compact=null;
  const c=edit(offline,f.offline,f.batch);equal(offline,f.offline_expected,'offline trace differs');
  let offlineSaved=save(offline);offline=null;offline=load(offlineSaved,6);offlineSaved=null;
  t=now();const upload=delta(offline,common);const upload_encode_ms=now()-t;
  t=now();applyUpdate(server,upload);const reconnect_server_ms=now()-t;
  t=now();applyUpdate(offline,delta1);applyUpdate(offline,delta2);const reconnect_client_ms=now()-t;
  const merged=state(server);equal(offline,merged,'replicas did not converge');
  assert(merged.text.includes('OFFLINE_SENTINEL_2026'));assert(merged.text.includes('SERVER_SENTINEL_2026'));
  if(Object.keys(f.initial.map).length){assert(merged.map['offline-only']==='preserved');assert(merged.map['server-only']==='preserved');}
  applyUpdate(server,upload);applyUpdate(offline,delta1);applyUpdate(offline,delta2);
  equal(server,merged,'duplicate upload');equal(offline,merged,'duplicate download');
  let reordered=load(base,20);
  // json-joy Delta requires causal ordering, so an application must buffer gaps.
  if(lib==='yjs'){applyUpdate(reordered,delta2);applyUpdate(reordered,delta1);}
  else {applyUpdate(reordered,delta1);applyUpdate(reordered,delta2);}
  applyUpdate(reordered,upload);equal(reordered,merged,'reordered replay');reordered=null;
  t=now();const snapshot=save(server);const save_ms=now()-t;
  t=now();const compressed=zstdCompressSync(snapshot,{params:{[constants.ZSTD_c_compressionLevel]:3}});const zstd3_ms=now()-t;
  fs.writeFileSync(output,snapshot);fs.writeFileSync(output+'.state.json',JSON.stringify(merged));
  console.log(JSON.stringify({library:lib,scenario:f.name,batch:f.batch,initial_text_bytes:f.initial.text.length,initial_map_keys:Object.keys(f.initial.map).length,server_edits:f.server.length,offline_edits:f.offline.length,init_ms,server_edit_ms:a[0]+b[0],server_batch_p50_ms:(a[1]+b[1])/2,server_batch_p99_ms:Math.max(a[2],b[2]),offline_edit_ms:c[0],offline_batch_p50_ms:c[1],offline_batch_p99_ms:c[2],checkpoint_save_ms,checkpoint_load_ms,compact_ms,reload_ms,pre_merge_snapshot_bytes,upload_encode_ms,reconnect_server_ms,reconnect_client_ms,upload_bytes:upload.length,download_bytes:delta1.length+delta2.length,snapshot_bytes:snapshot.length,zstd3_bytes:compressed.length,zstd3_ms,save_ms,final_text_bytes:merged.text.length,final_map_keys:Object.keys(merged.map).length,converged:true,offline_sentinel_preserved:true,duplicate_updates_pass:true,reordered_updates_pass:lib==='yjs',baseline_rss_bytes}));
}else if(mode==='memory') {
  global.gc();const before=process.memoryUsage();let bytes=fs.readFileSync(input);const size=bytes.length;
  let t=now();const d=load(bytes,90);const load_ms=now()-t;bytes=null;global.gc();const loaded=process.memoryUsage();
  t=now();let v=state(d);const materialize_ms=now()-t,text_bytes=v.text.length,map_keys=Object.keys(v.map).length;v=null;global.gc();const materialized=process.memoryUsage();
  t=now();apply(d,[{kind:'text',pos:Math.floor(text_bytes/2),delete:0,insert:'probe'}]);const first_edit_ms=now()-t;global.gc();const edited=process.memoryUsage();
  console.log(JSON.stringify({library:lib,snapshot_bytes:size,load_ms,materialize_ms,first_edit_ms,baseline_rss_bytes:before.rss,loaded_rss_bytes:loaded.rss,materialized_rss_bytes:materialized.rss,edited_rss_bytes:edited.rss,loaded_live_bytes:Math.max(0,loaded.heapUsed+loaded.arrayBuffers-before.heapUsed-before.arrayBuffers),materialized_live_bytes:Math.max(0,materialized.heapUsed+materialized.arrayBuffers-before.heapUsed-before.arrayBuffers),edited_live_bytes:Math.max(0,edited.heapUsed+edited.arrayBuffers-before.heapUsed-before.arrayBuffers),text_bytes,map_keys,allocation_metrics:'js-heap-plus-arraybuffers'}));
}else if(mode==='night'){
  const {runNight}=await import('./night-js.mjs');
  await runNight({lib,empty,load,save,version,delta,applyUpdate,apply,state},input,output,process.argv[6]);
}else throw Error('usage: run LIB FIXTURE SNAPSHOT | memory LIB SNAPSHOT');

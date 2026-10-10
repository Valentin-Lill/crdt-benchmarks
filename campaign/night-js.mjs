import fs from 'node:fs';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {zstdCompressSync,constants} from 'node:zlib';
const now=()=>performance.now();
const rss=()=>process.memoryUsage().rss;
function peak(){return Number(fs.readFileSync('/proc/self/status','utf8').match(/^VmHWM:\s+(\d+)/m)[1])*1024;}
function reset(){try{fs.writeFileSync('/proc/self/clear_refs','5');return true;}catch{return false;}}
function marker(stage){console.error(JSON.stringify({stage,rss_bytes:rss(),peak_rss_bytes:peak()}));}
function* trace(r,branch){
 let x=branch==='server'?123456:987654,len=r.text_bytes,pos=100;
 const rand=()=>{x^=x<<13;x^=x>>>17;x^=x<<5;return x>>>0;};
 const count=branch==='server'?r.server_edits:r.offline_edits,churn=r.churn_bytes||0;
 for(let i=0;i<count;i++){
  if(r.keys && i%3!==0){const key='field-'+(rand()%r.keys).toString().padStart(6,'0');yield i%7===0?{kind:'delete',key}:{kind:'set',key,value:i%5===0?`${branch} revision ${i}`:i%3===1?i:!!(i%2)};continue;}
  if(r.pattern==='random'||r.pattern==='churn'||i%32===0)pos=64+rand()%(len-128-churn);
  pos=Math.min(pos,len-32-churn);
  let del,insert;
  if(r.pattern==='churn'){const b=Buffer.allocUnsafe(churn);for(let j=0;j<churn;j++)b[j]=97+rand()%26;del=churn;insert=b.toString('ascii');}
  else if(i%5===0){del=8;insert='';}
  else if(i%11===0){del=4;insert=`word${rand().toString(16).padStart(8,'0')} `;}
  else if(i%4===0){del=0;insert=`text${rand().toString(16).padStart(8,'0')} `;}
  else{del=0;insert=String.fromCharCode(97+rand()%26);}
  yield {kind:'text',pos,delete:del,insert};len=len-del+insert.length;pos+=insert.length;
 }
 yield {kind:'text',pos:0,delete:0,insert:branch.toUpperCase()+'_SENTINEL_2026\n'};
 if(r.keys)yield {kind:'set',key:branch+'-only',value:'preserved'};
}
function hashState(s){const h=crypto.createHash('sha256');h.update(s.text);h.update('\0');for(const key of Object.keys(s.map).sort()){h.update(key);h.update('\0');h.update(JSON.stringify(s.map[key]));h.update('\0');}return h.digest('hex');}
export async function runNight(api,recipePath,action,dir){
 const {lib,empty,load,save,version,delta,applyUpdate,apply,state}=api;
 const r=JSON.parse(fs.readFileSync(recipePath));fs.mkdirSync(dir,{recursive:true});const path=n=>dir+'/'+n;
 const baseline=rss();
 function readDoc(name,n){let bytes=fs.readFileSync(path(name));const t=now(),d=load(bytes,n),elapsed=now()-t;bytes=null;global.gc();return [d,elapsed];}
 function saveDoc(d,name){const t=now(),bytes=save(d),elapsed=now()-t;fs.writeFileSync(path(name),bytes);return [bytes.length,elapsed];}
 let result;
 if(action==='init'){
  let d=empty(1);const text=fs.readFileSync(r.text_path,'utf8');let t=now();apply(d,[{kind:'text',pos:0,delete:0,insert:text}]);let init=now()-t;
  for(let start=0;start<r.keys;start+=1024){let ops=[];for(let i=start;i<Math.min(start+1024,r.keys);i++)ops.push({kind:'set',key:'field-'+i.toString().padStart(6,'0'),value:i%3===0?`Record ${i}: persistent document`:i%3===1?i:!!(i%2)});t=now();apply(d,ops);init+=now()-t;}
  const init_peak=peak();marker('initial document built');const supported=reset();const [size,elapsed]=saveDoc(d,'base.bin');result={init_ms:init,init_peak_rss_bytes:init_peak,save_ms:elapsed,save_peak_rss_bytes:peak(),snapshot_bytes:size,peak_reset_supported:supported};
 }else if(action==='server'||action==='offline'){
  let [d,load_ms]=readDoc('base.bin',action==='server'?2:3),since=version(d),load_peak=peak();const supported=reset();const it=trace(r,action);let done=0,checkpointed=false,edit_ms=0,encode_ms=0,checkpoint_save_ms=0,checkpoint_load_ms=0,edit_high=0,checkpoint_peak=0;const split=Math.floor(r.server_edits/2),latencies=[];
  for(;;){const ops=[],take=action==='server'&&!checkpointed?Math.min(r.batch,split-done):r.batch;for(let i=0;i<take;i++){const v=it.next();if(v.done)break;ops.push(v.value);}if(!ops.length)break;done+=ops.length;let t=now();apply(d,ops);const ms=now()-t;edit_ms+=ms;latencies.push(ms);
   if(action==='server'&&!checkpointed&&done===split){edit_high=Math.max(edit_high,peak());t=now();let b=delta(d,since);encode_ms+=now()-t;fs.writeFileSync(path('download-1.bin'),b);b=null;since=version(d);checkpoint_save_ms=saveDoc(d,'checkpoint.bin')[1];d.dispose?.();d=null;global.gc();[d,checkpoint_load_ms]=readDoc('checkpoint.bin',4);fs.unlinkSync(path('checkpoint.bin'));checkpointed=true;checkpoint_peak=peak();reset();marker('server checkpoint reloaded');}
  }
  const edit_peak=Math.max(edit_high,peak());marker('branch edits complete');let t=now(),b=delta(d,since);encode_ms+=now()-t;const delta_size=b.length;fs.writeFileSync(path(action==='server'?'download-2.bin':'upload.bin'),b);b=null;const [,save_ms]=saveDoc(d,action+'.bin');latencies.sort((a,b)=>a-b);result={load_ms,load_peak_rss_bytes:load_peak,edit_ms,edit_peak_rss_bytes:edit_peak,checkpoint_peak_rss_bytes:checkpoint_peak,export_and_save_peak_rss_bytes:peak(),encode_ms,last_delta_bytes:delta_size,save_ms,checkpoint_save_ms,checkpoint_load_ms,commands:done,batch_p99_ms:latencies[Math.min(Math.floor(latencies.length*.99),latencies.length-1)],peak_reset_supported:supported};
 }else if(action==='merge-server'||action==='merge-client'){
  const server=action==='merge-server';let [d,load_ms]=readDoc(server?'server.bin':'offline.bin',server?5:6);const load_peak=peak();marker('merge input loaded');const supported=reset();let merge_ms=0;
  for(const p of server?['upload.bin']:['download-1.bin','download-2.bin']){let b=fs.readFileSync(path(p));const t=now();applyUpdate(d,b);merge_ms+=now()-t;b=null;}
  const merge_peak=peak(),merge_rss=rss();marker('merge complete');reset();const [size,save_ms]=saveDoc(d,server?'merged-server.bin':'merged-client.bin');result={load_ms,load_peak_rss_bytes:load_peak,merge_ms,merge_peak_rss_bytes:merge_peak,merge_rss_bytes:merge_rss,save_ms,save_peak_rss_bytes:peak(),snapshot_bytes:size,peak_reset_supported:supported};
 }else if(action.startsWith('check-')){
  const name=action.slice(6);let [d]=readDoc(name+'.bin',20),s=state(d);const hash=hashState(s),text_bytes=s.text.length,map_keys=Object.keys(s.map).length;
  if(name.startsWith('merged')){assert(s.text.includes('SERVER_SENTINEL_2026')&&s.text.includes('OFFLINE_SENTINEL_2026'));if(r.keys){assert(s.map['server-only']==='preserved'&&s.map['offline-only']==='preserved');}}s=null;global.gc();
  if(name.startsWith('merged')){for(const p of name.endsWith('server')?['upload.bin']:['download-1.bin','download-2.bin'])applyUpdate(d,fs.readFileSync(path(p)));assert.equal(hash,hashState(state(d)),'duplicate update changed state');}
  result={hash,text_bytes,map_keys,duplicate_updates_pass:name.startsWith('merged')};
 }else if(action==='storage'){
  const b=fs.readFileSync(path('merged-server.bin')),t=now(),c=zstdCompressSync(b,{params:{[constants.ZSTD_c_compressionLevel]:3}});result={snapshot_bytes:b.length,zstd3_bytes:c.length,zstd3_ms:now()-t,best_disk_bytes:Math.min(b.length,c.length)};
 }else if(action==='cold'){
  global.gc();const base=process.memoryUsage();let [d,load_ms]=readDoc('merged-server.bin',90);const loaded=process.memoryUsage(),load_peak=peak();const supported=reset();let t=now(),s=state(d);const read_ms=now()-t,pos=Math.floor(s.text.length/2);s=null;global.gc();const materialized=process.memoryUsage(),read_peak=peak();reset();t=now();apply(d,[{kind:'text',pos,delete:0,insert:'probe'}]);const first_edit_ms=now()-t;global.gc();const edited=process.memoryUsage();result={load_ms,read_ms,first_edit_ms,load_peak_rss_bytes:load_peak,read_peak_rss_bytes:read_peak,first_edit_peak_rss_bytes:peak(),edited_rss_bytes:edited.rss,edited_live_bytes:Math.max(0,edited.heapUsed+edited.arrayBuffers-base.heapUsed-base.arrayBuffers),loaded_rss_bytes:loaded.rss,materialized_rss_bytes:materialized.rss,peak_reset_supported:supported,allocation_metrics:'js-heap-plus-arraybuffers'};
 }else throw Error('unknown night stage '+action);
 console.log(JSON.stringify({action,library:lib,baseline_rss_bytes:baseline,result}));
}

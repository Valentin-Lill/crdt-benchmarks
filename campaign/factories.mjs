// Adapters use the upstream CrdtFactory contract. Only the selected engine is
// imported, so each process measures that engine's runtime and memory.
import crypto from 'node:crypto';

export const variants = [
  'yjs-v2-gc', 'yjs-v1-gc', 'yjs-v2-no-gc', 'yjs-v1-no-gc',
  'yrs-wasm-v2-gc', 'yrs-wasm-v1-gc', 'yrs-wasm-v2-no-gc', 'yrs-wasm-v1-no-gc',
  'loro-snapshot', 'loro-operations', 'automerge', 'json-joy', 'diamond-types',
  'yjs-v2-gc-tail', 'yrs-wasm-v2-gc-tail', 'loro-snapshot-tail', 'automerge-tail', 'json-joy-tail'
];
const FRAME = Buffer.from('CRDTAIL1');
function pack(base, tail) {
  const header = Buffer.alloc(16); FRAME.copy(header); header.writeBigUInt64LE(BigInt(base.length), 8);
  return Buffer.concat([header, base, tail]);
}
function unpack(bytes) {
  const b = Buffer.from(bytes);
  if (!b.subarray(0, 8).equals(FRAME)) throw Error('Missing snapshot-tail frame');
  const n = Number(b.readBigUInt64LE(8));
  if (n > b.length - 16) throw Error('Invalid snapshot-tail frame');
  return [b.subarray(16, 16+n), b.subarray(16+n)];
}

export async function makeFactory(name) {
  if (!variants.includes(name)) throw Error('Unknown variant '+name);
  const tail = name.endsWith('-tail'); const baseName = tail ? name.slice(0, -5) : name;
  let create;
  if (baseName.startsWith('yjs-') || baseName.startsWith('yrs-wasm-')) {
    const wasm = baseName.startsWith('yrs-wasm-'); const Y = await import(wasm ? 'ywasm' : 'yjs');
    const v2 = baseName.includes('-v2-'); const gc = !baseName.includes('-no-gc');
    const encode = v2 ? Y.encodeStateAsUpdateV2 : Y.encodeStateAsUpdate;
    const apply = v2 ? Y.applyUpdateV2 : Y.applyUpdate;
    create = handler => {
      const d = wasm ? new Y.YDoc({gc}) : new Y.Doc({gc});
      const text=d.getText('text'), map=d.getMap('map'), array=d.getArray('array');
      let txn=null; const doc={
        getEncodedState:()=>encode(d), version:()=>Y.encodeStateVector(d), delta:v=>encode(d,v),
        applyUpdate:b=>{ if(wasm && txn) (v2 ? txn.applyV2(b) : txn.applyV1(b)); else apply(d,b); },
        transact(f){if(wasm){if(txn)return f(doc);txn=d.beginTransaction(null);try{return f(doc);}finally{txn.free();txn=null;}}return d.transact(()=>f(doc));},
        insertText(i,s){if(wasm)doc.transact(()=>text.insert(i,s,null,txn));else text.insert(i,s);},
        deleteText(i,n){if(wasm)doc.transact(()=>text.delete(i,n,txn));else text.delete(i,n);},
        getText:()=>wasm?text.toString(txn??undefined):text.toString(),
        insertArray(i,a){if(wasm)doc.transact(()=>array.insert(i,a,txn));else array.insert(i,a);},
        deleteArray(i,n){if(wasm)doc.transact(()=>array.delete(i,n,txn));else array.delete(i,n);},
        getArray:()=>wasm?array.toJson(txn??undefined):array.toArray(),
        setMap(k,v){if(wasm)doc.transact(()=>map.set(k,v,txn));else map.set(k,v);},
        deleteMap(k){if(wasm)doc.transact(()=>map.delete(k,txn));else map.delete(k);},
        getMap:()=>wasm?map.toJson(txn??undefined):map.toJSON(),
        dispose(){if(wasm){text.free();map.free();array.free();d.free();}else d.destroy();}
      };
      if(wasm)d.on(v2?'updateV2':'update',handler);else d.on(v2?'updateV2':'update',handler);
      return doc;
    };
  } else if(baseName.startsWith('loro-')) {
    const {LoroDoc}=await import('loro-crdt');
    create=handler=>{
      const d=new LoroDoc(),text=d.getText('text'),map=d.getMap('map'),array=d.getList('array');
      let active=false, pending=[]; const doc={
        getEncodedState:()=>d.export({mode:baseName==='loro-operations'?'update':'snapshot'}),
        version:()=>d.version(), delta:from=>d.export({mode:'update',from}),
        applyUpdate(b){if(active)pending.push(b);else d.import(b);},
        transact(f){if(active)return f(doc);const from=d.version();active=true;try{f(doc);d.commit();if(pending.length){d.importBatch(pending);pending=[];}}finally{active=false;}handler(d.export({mode:'update',from}));},
        insertText(i,s){doc.transact(()=>text.insert(i,s));},deleteText(i,n){doc.transact(()=>text.delete(i,n));},getText:()=>text.toString(),
        insertArray(i,a){doc.transact(()=>{for(let j=0;j<a.length;j++)array.insert(i+j,a[j]);});},
        deleteArray(i,n){doc.transact(()=>array.delete(i,n));},getArray:()=>array.toArray(),
        setMap(k,v){doc.transact(()=>map.set(k,v));},deleteMap(k){doc.transact(()=>map.delete(k));},getMap:()=>map.toJSON(),
        dispose(){text.free();map.free();array.free();d.free();}
      };return doc;
    };
  } else if(baseName==='automerge') {
    const A=await import('@automerge/automerge');
    const seed=A.from({array:[],map:{},text:''}); const initial=A.save(seed);A.free(seed);
    create=handler=>{
      let d=A.load(initial),txn=null; const doc={
        getEncodedState:()=>A.save(d),version:()=>A.getHeads(d),delta:v=>A.saveSince(d,v),
        loadSnapshot:b=>{A.free(d);d=A.load(b);},
        applyUpdate(b){d=A.loadIncremental(d,b);},
        transact(f,isUpdate=false){if(isUpdate)return f(doc);if(txn)return f(doc);d=A.change(d,draft=>{txn=draft;try{f(doc);}finally{txn=null;}});handler(A.saveIncremental(d));},
        insertText(i,s){doc.transact(()=>A.splice(txn,['text'],i,0,s));},deleteText(i,n){doc.transact(()=>A.splice(txn,['text'],i,n,''));},getText:()=>d.text,
        insertArray(i,a){doc.transact(()=>txn.array.splice(i,0,...a));},deleteArray(i,n){doc.transact(()=>txn.array.splice(i,n));},getArray:()=>A.toJS(d).array,
        setMap(k,v){doc.transact(()=>{txn.map[k]=typeof v==='string'?new A.RawString(v):v;});},
        deleteMap(k){doc.transact(()=>{delete txn.map[k];});},
        getMap(){const m=A.toJS(d).map;return Object.fromEntries(Object.entries(m).map(([k,v])=>[k,v instanceof A.RawString?v.toString():v]));},
        dispose:()=>A.free(d)
      };return doc;
    };
  } else if(baseName==='json-joy') {
    const {Model}=await import('json-joy/lib/json-crdt/model/Model.js');
    const {Delta}=await import('json-joy/lib/json-crdt/delta/Delta.js');
    const {Patch}=await import('json-joy/lib/json-crdt-patch/Patch.js');
    const {s}=await import('json-joy/lib/json-crdt-patch/schema.js');
    const seed=Model.create(undefined,100);seed.api.set({text:'',map:{},array:[]});seed.api.flush();const initial=seed.toBinary();
    create=handler=>{
      let d=Model.load(initial,1000+crypto.randomInt(2**40)),active=false;const doc={
        getEncodedState:()=>d.toBinary(),version:()=>d.clock.clone(),delta:v=>Buffer.concat([Buffer.from([1]),d.delta(v).toBinary()]),
        applyUpdate:b=>b[0]===1?d.applyDelta(Delta.from(b.subarray(1))):d.applyPatch(Patch.fromBinary(b.subarray(1))),
        loadSnapshot:b=>{d=Model.load(b,1000+crypto.randomInt(2**40));},
        transact(f,isUpdate=false){if(isUpdate||active)return f(doc);active=true;let patch;try{d.api.transaction(()=>f(doc));patch=d.api.flush();}finally{active=false;}if(patch?.ops.length)handler(Buffer.concat([Buffer.from([0]),patch.toBinary()]));},
        insertText(i,t){doc.transact(()=>d.api.str('text').ins(i,t));},deleteText(i,n){doc.transact(()=>d.api.str('text').del(i,n));},getText:()=>d.view().text,
        insertArray(i,a){doc.transact(()=>d.api.arr('array').ins(i,a));},deleteArray(i,n){doc.transact(()=>d.api.arr('array').del(i,n));},getArray:()=>d.view().array,
        setMap(k,v){doc.transact(()=>d.api.obj('map').set({[k]:s.con(v)}));},deleteMap(k){doc.transact(()=>d.api.obj('map').del([k]));},getMap:()=>d.view().map,
        dispose(){d=null;}
      };return doc;
    };
  } else {
    const {DiamondFactory}=await import('../benchmarks/diamond-types/factory.js');
    const factory=new DiamondFactory();
    create=h=>{const doc=factory.create(h);doc.getVersion=()=>doc.oplog.getLocalVersion();doc.delta=v=>doc.oplog.getPatchSince(v);doc.dispose=()=>{doc.doc.free();doc.oplog.free();};return doc;};
  }
  const factory={getName:()=>name,create,load(handler,bytes){const d=create(handler);if(d.loadSnapshot)d.loadSnapshot(bytes);else d.applyUpdate(bytes);return d;}};
  if(!tail)return factory;
  function wrap(d,base,clock){
    const encode=d.getEncodedState.bind(d);
    d.getEncodedState=()=>{if(!base){base=encode();clock=d.version();}return pack(base,d.delta(clock));};return d;
  }
  return {getName:()=>name,create:h=>wrap(create(h)),load(h,b){const [base,changes]=unpack(b);const d=factory.load(h,base),v=d.version();d.applyUpdate(changes);return wrap(d,base,v);}};
}

import {makeFactory} from './factories.mjs';
import {runNight} from './night-js.mjs';
const [lib,recipe,action,dir]=process.argv.slice(2);const f=await makeFactory(lib);
const api={lib,empty:()=>f.create(()=>{}),load:b=>f.load(()=>{},b),save:d=>d.getEncodedState(),version:d=>d.getVersion?d.getVersion():d.version(),delta:(d,v)=>d.delta(v),applyUpdate:(d,b)=>d.applyUpdate(b),
  apply(d,ops){d.transact(()=>{for(const o of ops){if(o.kind==='text'){if(o.delete)d.deleteText(o.pos,o.delete);if(o.insert)d.insertText(o.pos,o.insert);}else if(o.kind==='set')d.setMap(o.key,o.value);else d.deleteMap(o.key);}});},
  state:d=>({text:d.getText(),map:lib==='diamond-types'?{}:d.getMap()})};
await runNight(api,recipe,action,dir);

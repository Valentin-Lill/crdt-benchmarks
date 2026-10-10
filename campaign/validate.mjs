import assert from 'node:assert/strict';
import fs from 'node:fs';
import {makeFactory,variants} from './factories.mjs';
const out=process.argv[2];const results=[];
for(const name of variants){
  const f=await makeFactory(name),noop=()=>{};
  let base=f.create(noop);base.insertText(0,'initial text to retain');
  if(name!=='diamond-types'){base.setMap('unchanged',42);base.insertArray(0,[1,2]);}
  const bytes=base.getEncodedState();base.dispose?.();
  let server=f.load(noop,bytes),offline=f.load(noop,bytes);
  const sv=server.getVersion?server.getVersion():server.version();
  const cv=offline.getVersion?offline.getVersion():offline.version();
  server.insertText(0,'SERVER ');server.deleteText(7,3);
  if(name!=='diamond-types')server.setMap('server-only','saved');
  let checkpoint=server.getEncodedState();server.dispose?.();server=f.load(noop,checkpoint);
  server.insertText(server.getText().length,' SERVER_AFTER_RELOAD');
  offline.insertText(offline.getText().length,' OFFLINE');
  if(name!=='diamond-types')offline.setMap('offline-only','saved');
  const upload=offline.delta(cv),download=server.delta(sv);
  checkpoint=offline.getEncodedState();offline.dispose?.();offline=f.load(noop,checkpoint);
  server.applyUpdate(upload);offline.applyUpdate(download);
  assert.equal(server.getText(),offline.getText(),name+' convergence');
  assert(server.getText().includes('OFFLINE')&&server.getText().includes('SERVER_AFTER_RELOAD'));
  if(name!=='diamond-types'){assert.deepEqual(server.getMap(),offline.getMap());assert.equal(server.getMap()['unchanged'],42);assert.equal(server.getMap()['offline-only'],'saved');assert.equal(server.getMap()['server-only'],'saved');assert.deepEqual(server.getArray(),[1,2]);}
  const expected=server.getText();server.applyUpdate(upload);offline.applyUpdate(download);assert.equal(server.getText(),expected);assert.equal(offline.getText(),expected);
  const reload=f.load(noop,server.getEncodedState());assert.equal(reload.getText(),expected);
  server.dispose?.();offline.dispose?.();reload.dispose?.();
  results.push({variant:name,status:'ok',save_reload:true,offline_merge:true,duplicate_delivery:true});
  console.log(name,'validated');
}
if(out)fs.writeFileSync(out,JSON.stringify(results,null,2));

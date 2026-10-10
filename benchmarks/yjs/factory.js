// Modern adapter; additional variants are selected by campaign/factories.mjs.
import {makeFactory} from '../../campaign/factories.mjs';
const factory=await makeFactory('yjs-v2-gc');
export const name='yjs';
export class YjsFactory {
  create(handler) {return factory.create(handler);}
  load(handler,bytes) {return factory.load(handler,bytes);}
  getName() {return name;}
}

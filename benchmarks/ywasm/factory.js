// Modern adapter; additional variants are selected by campaign/factories.mjs.
import {makeFactory} from '../../campaign/factories.mjs';
const factory=await makeFactory('yrs-wasm-v2-gc');
export const name='ywasm';
export class YwasmFactory {
  create(handler) {return factory.create(handler);}
  load(handler,bytes) {return factory.load(handler,bytes);}
  getName() {return name;}
}

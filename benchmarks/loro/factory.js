// Modern adapter; additional variants are selected by campaign/factories.mjs.
import {makeFactory} from '../../campaign/factories.mjs';
const factory=await makeFactory('loro-snapshot');
export const name='loro';
export class LoroFactory {
  create(handler) {return factory.create(handler);}
  load(handler,bytes) {return factory.load(handler,bytes);}
  getName() {return name;}
}

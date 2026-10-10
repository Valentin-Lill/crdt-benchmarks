import fs from 'node:fs';
import {edits,finalText} from '../js-lib/b4-editing-trace.js';
// Upstream delete entries omit the third element. Normalize that omission to
// an empty insertion string, preserving every position and editing operation.
fs.writeFileSync(new URL('./upstream-trace.json',import.meta.url),JSON.stringify({edits:edits.map(([p,n,t])=>[p,n,t||'']),finalText}));

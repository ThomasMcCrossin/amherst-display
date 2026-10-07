// Probe helper: node bakeoff/probe.mjs name key=val ... ; caches raw to bakeoff/data/raw/<name>.json
import fs from 'node:fs'; import { request } from '../scripts/hockeytech.mjs';
const [name, ...kv] = process.argv.slice(2);
const params = Object.fromEntries(kv.map(s => { const i = s.indexOf('='); return [s.slice(0,i), s.slice(i+1)]; }));
try { const d = await request(params); fs.writeFileSync(`bakeoff/data/raw/${name}.json`, JSON.stringify(d));
  const s = JSON.stringify(d); console.log(name, 'OK', s.length, 'bytes', s.slice(0, +process.env.N||300)); }
catch (e) { console.log(name, 'ERR', e.message); }
await new Promise(r=>setTimeout(r,400));

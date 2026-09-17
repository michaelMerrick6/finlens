import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import ts from 'typescript';
const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync('src/lib/reviewed-holdings.ts','utf8'), {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText, {exports});
const { checkCrockettIndex, modelUnchangedHolding } = exports;
const header = 'First\tLast\tStateDst\tDocID\tFilingType\tFilingDate\n';
const annual = 'Jasmine\tCrockett\tTX30\t10074813\tO\t8/12/2026';
test('index identity, baseline and extension handling', () => {
 const result = checkCrockettIndex(header + annual + '\nJasmine\tCrockett\tTX30\t30026765\tX\t4/1/2026\nDouglas\tCrockett\tVA09\tother\tC\t7/5/2026');
 assert.equal(result.foundBaseline, true); assert.equal(result.pending.length,0);
});
test('new trades, backdated amendments and identity conflicts stop carry-forward', () => {
 for (const row of ['Jasmine\tCrockett\tTX30\tnew\tP\t9/17/2026','Jasmine\tCrockett\tTX30\tamendment\tW\t1/1/2025','Jane\tCrockett\tTX30\tconflict\tP\t9/17/2026']) {
  assert.equal(checkCrockettIndex(header + annual + '\n' + row).pending.length,1);
 }
 assert.throws(() => checkCrockettIndex(header + annual.replace('TX30','TX99')));
 assert.throws(() => checkCrockettIndex('<html>unavailable</html>'));
 assert.throws(() => checkCrockettIndex(header + annual + '\nbroken'));
 assert.equal(checkCrockettIndex(header + 'Douglas\tCrockett\tVA09\tother\tC\t7/5/2026').foundBaseline,false);
});
test('range valuation uses both ends, excludes intraday price and does not reinvest dividends', () => {
 const value = modelUnchangedHolding({min:1001,max:15000}, [{date:'2025-12-31',close:50},{date:'2026-09-16',close:75},{date:'2026-09-17',close:100}], '2026-09-17');
 assert.equal(value.min,1501.5); assert.equal(value.max,22500); assert.equal(value.latest.date,'2026-09-16');
});
test('split-adjusted endpoints do not apply the split twice', () => {
 const value = modelUnchangedHolding({min:1000,max:10000}, [{date:'2025-12-31',close:25},{date:'2026-09-16',close:30}], '2026-09-17');
 assert.equal(value.minShares,40); assert.equal(value.maxShares,400); assert.equal(value.max,12000);
});
test('missing, invalid and stale prices never become current values', () => {
 for (const prices of [[],[{date:'2025-12-31',close:0}], [{date:'2025-12-01',close:50},{date:'2026-09-16',close:75}], [{date:'2025-12-31',close:50},{date:'2026-09-01',close:75}]]) {
  assert.equal(modelUnchangedHolding({min:1001,max:15000},prices,'2026-09-17'),null);
 }
});

async function serverFixture({pending=false, sourceChanged=false, offline=false}={}) {
 const crypto = await import('node:crypto');
 const fixture = Buffer.from('reviewed-pdf-fixture');
 let priceRequests = 0;
 const serverExports = {};
 const baseline = {...exports.crockettBaseline, sha256:crypto.createHash('sha256').update(fixture).digest('hex')};
 class FixedDate extends Date { constructor(...args) { super(...(args.length ? args : ['2026-09-17T12:00:00Z'])); } }
 const fetch = async url => {
  if (offline) throw new Error('offline');
  if (url.endsWith('2025FD.txt')) return {ok:true,text:async()=>header+annual};
  if (url.endsWith('2026FD.txt')) return {ok:true,text:async()=>header+(pending?'Jasmine\tCrockett\tTX30\tnew\tP\t9/17/2026':'Douglas\tCrockett\tVA09\tother\tC\t7/5/2026')};
  if (url.endsWith('.pdf')) return {ok:true,arrayBuffer:async()=>sourceChanged?Buffer.from('changed'):fixture};
  priceRequests++;
  const ticker = url.match(/chart\/([^?]+)/)[1];
  return {ok:true,json:async()=>({chart:{result:[{meta:{currency:'USD',symbol:ticker},timestamp:[Date.parse('2025-12-31')/1000,Date.parse('2026-09-16')/1000],indicators:{quote:[{close:[50,75]}]}}]}})};
 };
 vm.runInNewContext(ts.transpileModule(fs.readFileSync('src/lib/reviewed-holdings-server.ts','utf8'), {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText, {
  exports:serverExports,Buffer,Date:FixedDate,AbortSignal,fetch,
  require:name=>name==='server-only'?{}:name==='node:crypto'?crypto:name==='next/cache'?{unstable_cache:fn=>fn}:{...exports,getReviewedBaseline:()=>baseline,crockettBaseline:baseline}
 });
 return {snapshot:await serverExports.getCrockettHoldings(), priceRequests};
}
test('server only values the unchanged, source-verified baseline', async () => {
 const normal = await serverFixture();
 assert.equal(normal.snapshot.status,'checked'); assert.equal(normal.priceRequests,3);
 assert.ok(normal.snapshot.rows.every(row=>row.estimate !== null));
 const pending = await serverFixture({pending:true});
 assert.equal(pending.snapshot.status,'review-needed');assert.equal(pending.priceRequests,0);
 assert.ok(pending.snapshot.rows.every(row=>row.estimate === null));
});
test('source changes and outages suppress current estimates',async()=>{
 for (const scenario of [{sourceChanged:true},{offline:true}]) {
  const result = await serverFixture(scenario);
  assert.equal(result.snapshot.status,'unavailable');assert.equal(result.priceRequests,0);
  assert.ok(result.snapshot.rows.every(row=>row.estimate === null));
 }
});

test('Massie baseline preserves all five annual entries and uses its own identity', () => {
 const baseline = exports.getReviewedBaseline('M001184');
 assert.equal(baseline.positions.length,1);
 assert.equal(baseline.positions[0].ticker,'TSLA');
 assert.equal(baseline.positions[0].min,15001);
 assert.equal(baseline.positions[0].max,50000);
 assert.equal(baseline.otherAssets.length,4);
 const row = 'Thomas\tMassie\tKY04\t10077444\tO\t05/15/2026';
 assert.equal(exports.checkReviewedIndex(header+row, baseline).foundBaseline,true);
 assert.equal(exports.checkReviewedIndex(header+annual, baseline).foundBaseline,false);
 assert.equal(exports.checkReviewedIndex(header+row, exports.crockettBaseline).foundBaseline,false);
 assert.throws(()=>exports.checkReviewedIndex(header+row.replace('KY04','TX30'),baseline));
 assert.equal(exports.getReviewedBaseline('toString'),undefined);
 assert.equal(exports.getReviewedBaseline('unknown'),undefined);
});
test('valuation respects each reviewed annual date rather than assuming Crockett date', () => {
 const prices = [{date:'2024-12-31',close:25},{date:'2025-12-31',close:50},{date:'2026-09-16',close:75}];
 assert.equal(modelUnchangedHolding({min:1000,max:10000},prices,'2026-09-17','2024-12-31').max,30000);
 assert.equal(modelUnchangedHolding({min:15001,max:50000},prices,'2026-09-17','2025-12-31').max,75000);
});

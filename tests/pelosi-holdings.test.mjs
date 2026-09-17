import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import ts from 'typescript';
function load(file, require) {
 const exports = {};
 vm.runInNewContext(ts.transpileModule(fs.readFileSync(file, 'utf8'), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true } }).outputText, { exports, require });
 return exports;
}
const ledger = load('src/lib/pelosi-holdings.ts', name => JSON.parse(fs.readFileSync('src/lib/' + name, 'utf8')));
const events = JSON.parse(fs.readFileSync('artifacts/pelosi-audit/2026-position-changes.json')).events;
const rows = ledger.buildPelosiHoldings('2026-09-17');
test('reviewed exercises consume contracts and add shares without inventing stock totals', () => {
 assert.equal(rows.filter(p => p.status === 'reconstructed').length, 8);
 assert.equal(rows.filter(p => p.status === 'closed').length, 5);
 for (const ticker of ['GOOGL','AMZN','NVDA','TEM','VST']) {
  const stock = rows.find(p => p.ticker === ticker && (p.kind === 'stock' || p.kind === 'units'));
  assert.equal(stock.shareChange, 5000); assert.equal(stock.totalShares, null);
 }
 assert.equal(rows.find(p => p.ticker === 'BE' && p.kind === 'stock').shareChange, 15000);
 assert.equal(rows.find(p => p.ticker === 'VSNT').shareChange, 776);
 assert.equal(rows.find(p => p.ticker === 'CMCSA').shareChange, 0);
});
test('option series, owner and asset types remain separate; None is not a positive holding', () => {
 assert.equal(rows.filter(p => p.ticker === 'INTC' && p.kind === 'call').length, 2);
 assert.equal(rows.filter(p => p.ticker === 'AVGO').length, 1);
 assert.ok(!rows.some(p => ['PYPL','DIS'].includes(p.ticker)));
 assert.equal(rows.find(p => p.kind === 'private').status, 'unresolved');
 const separate = ledger.buildPelosiHoldings('2026-09-17', [...events, { ...events[7], doc_id: 'test-owner', owner: 'JT' }]);
 assert.equal(separate.filter(p => p.ticker === 'INTC' && p.expiration === '2027-03-19').length, 2);
});
test('invalid duplicate or excessive exercise fails closed; expiration does not imply exercise', () => {
 assert.throws(() => ledger.buildPelosiHoldings('2026-09-17', [...events, events[0]]), /Duplicate/);
 assert.throws(() => ledger.buildPelosiHoldings('2026-09-17', [{...events[1], contract_delta: -51}]), /exceeds/);
 const future = ledger.buildPelosiHoldings('2028-01-01');
 assert.equal(future.filter(p => p.status === 'reconstructed').length, 0);
 assert.equal(future.find(p => p.ticker === 'BE' && p.kind === 'stock').shareChange, 15000);
});
test('late-filed baseline transactions are not counted a second time', () => {
 const repeated = ledger.buildPelosiHoldings('2026-09-17', [...events, {...events[0], doc_id: 'prior-year', date: '2025-12-20'}]);
 assert.equal(repeated.find(p => p.ticker === 'AB').shareChange, 25000);
});
const coverage = load('src/lib/pelosi-holdings-coverage.ts', name => name === 'server-only' ? {} : ledger);
test('coverage detects new or missing filings and refuses malformed indexes', () => {
 const header = 'Last\tFirst\tDocID\tFilingType\n';
 const lines = ledger.pelosiHoldingsCoverage.reviewedDocuments.map(id => `Pelosi\tNancy\t${id}\tP`).join('\n');
 assert.equal(coverage.comparePelosiIndex(header + lines), 'matched');
 assert.equal(coverage.comparePelosiIndex(header + lines + '\nPelosi\tNancy\tNEW\tA'), 'review-needed');
 assert.equal(coverage.comparePelosiIndex(header + lines.split('\n')[0]), 'review-needed');
 assert.equal(coverage.comparePelosiIndex('<html>unavailable</html>'), 'unavailable');
});

test('full annual inventory accounts for all entries and attaches REOF once', () => {
 const full = load('src/lib/pelosi-full-holdings.ts', name => JSON.parse(fs.readFileSync('src/lib/' + name, 'utf8')));
 const other = full.getPelosiOtherAssets();
 assert.equal(other.length, 36);
 assert.equal(other.filter(p => p.reported_value !== 'None').length, 32);
 assert.equal(full.getPelosiExcludedAssets().length, 7);
 assert.equal(other.filter(p => p.changes.length).length, 1);
 const reof = other.find(p => p.name === 'REOF XXV, LLC');
 assert.equal(reof.changes.length, 1);
 assert.equal(reof.reported_value, '$500,001 - $1,000,000');
 assert.equal(other.find(p => p.name.startsWith('Congressional Credit')).owner, null);
 assert.equal(other.find(p => p.name === 'The Art of Power Book Contract').reported_value, 'Undetermined');
 assert.equal(new Set(other.map(p => p.name)).size, 36);
});

test('an amendment to the baseline year triggers a review', () => {
 const header = 'Last\tFirst\tDocID\tFilingType\n';
 assert.equal(coverage.comparePelosiIndex(header + 'Pelosi\tNancy\tANNUAL\tO', ['ANNUAL']), 'matched');
 assert.equal(coverage.comparePelosiIndex(header + 'Pelosi\tNancy\tANNUAL\tO\nPelosi\tNancy\tAMENDMENT\tA', ['ANNUAL']), 'review-needed');
});

const quantities = load('src/lib/pelosi-share-reconciliation.ts', () => { throw new Error('Unexpected runtime dependency'); });
test('share estimates account for sales and avoid applying splits twice', () => {
 const expected = { NVDA: 45000, AVGO: 20000, PANW: 14000, BE: 15000, INTC: 10000, TEM: 5000, VST: 5000, VSNT: 776, MSFT: 25000, CRWD: 20000, NFLX: 30000, RBLX: 5000, AXP: 5000, DBX: 10000, SQ: 5000, AB: 75000 };
 for (const [ticker, shares] of Object.entries(expected)) {
  const result = quantities.reconcilePelosiShares(rows.find(p => p.ticker === ticker && (p.kind === 'stock' || p.kind === 'units')));
  assert.equal(result.shares, shares, ticker);
  assert.equal(result.status, 'estimated');
  assert.ok(result.assumptions.length >= 2);
 }
 const stockRows = rows.filter(p => p.kind === 'stock' || p.kind === 'units');
 assert.equal(stockRows.filter(p => quantities.reconcilePelosiShares(p).status === 'estimated').length, 16);
 assert.equal(stockRows.filter(p => quantities.reconcilePelosiShares(p).status === 'unresolved').length, 0);
});
test('unknown openings, owner mismatches and contradictory balances never become exact holdings', () => {
 const nvidia = rows.find(p => p.ticker === 'NVDA' && p.kind === 'stock');
 assert.equal(quantities.reconcilePelosiShares({...nvidia, owner: 'JT'}).shares, null);
 assert.equal(quantities.reconcilePelosiShares({...nvidia, shareChange: -50000}).shares, null);
 for (const ticker of ['AAPL', 'WBD']) {
  assert.equal(quantities.reconcilePelosiShares(rows.find(p => p.ticker === ticker && p.kind !== 'call')).shares, null);
 }
 const bloom = rows.find(p => p.ticker === 'BE' && p.kind === 'stock');
 assert.equal(quantities.reconcilePelosiShares({...bloom, reportedValue: '$1,001 - $15,000'}).shares, null);
});

const display = load('src/lib/pelosi-holdings-display.ts', () => { throw new Error('Unexpected dependency'); });
test('holdings ranking keeps equal disclosure brackets tied and unvalued positions last', () => {
 const ranked = display.rankPelosiHoldings(rows);
 assert.equal(ranked.length, 28);
 assert.equal(ranked.filter(r => r.min === null).length, 5);
 assert.ok(ranked.slice(-5).every(r => r.rank === null));
 assert.ok(ranked.slice(0, 8).every(r => r.rank === 1 && r.min === 5000001));
 for (let i = 1; i < ranked.length; i++) {
  assert.ok((ranked[i - 1].min ?? -1) >= (ranked[i].min ?? -1));
 }
 assert.ok(ranked.every(r => r.position.kind !== 'call'));
});

test('chronological splits affect existing shares only; unknown openings stay unknown', () => {
 const step = (date, delta, factor) => ({date, delta, factor, source: 'test', note: date});
 const history = [step('2020-03-01', 100), step('2020-02-01', 0, 4), step('2020-01-01', 100)];
 assert.equal(quantities.replayShareHistory(history), 500);
 assert.equal(quantities.replayShareHistory(history, null), null);
 assert.equal(quantities.replayShareHistory(history, 0, '2020-01-31'), 100);
 assert.equal(quantities.replayShareHistory([step('2020-01-01', -1), step('2020-02-01', 10)]), null);
 assert.throws(() => quantities.replayShareHistory([history[0], history[0]]), /Duplicate/);
 assert.throws(() => quantities.replayShareHistory([step('2020-01-01', 1, 4)]), /Invalid/);
});

test('value-based ranges remain distinct from disclosed quantity estimates', () => {
 for (const ticker of ['AAPL','CRM','T','CLNE','IBKR','MORN','QCOM','AMZN','CMCSA','GOOGL','V','WBD']) {
  const p = rows.find(p => p.ticker === ticker && p.kind === 'stock');
  const q = quantities.reconcilePelosiShares(p);
  assert.equal(quantities.reconcilePelosiShares({...p, reportedValue: '$1 - $1,000'}).range, undefined);
  assert.equal(q.status, 'modeled-range'); assert.equal(q.shares, null);
  assert.ok(q.range.min > 0 && q.range.max > q.range.min);
  assert.equal(quantities.reconcilePelosiShares({...p, shareChange: 100}).range, undefined);
  assert.equal(quantities.reconcilePelosiShares({...p, owner: 'JT'}).range, undefined);
 }
});

test('modeled ranges use unadjusted Comcast price and carry Amazon exercise forward', () => {
 const q = quantities.reconcilePelosiShares(rows.find(p => p.ticker === 'QCOM' && p.kind === 'stock'));
 assert.equal(q.range.min, 88); assert.equal(q.range.max, 292);
 const c = quantities.reconcilePelosiShares(rows.find(p => p.ticker === 'CMCSA' && p.kind === 'stock'));
 assert.equal(c.status, 'modeled-range'); assert.equal(c.shares, null);
 assert.equal(c.range.min, 16729); assert.equal(c.range.max, 33456);
 assert.match(c.reason, /spinoff adjustment/);
 const a = rows.find(p => p.ticker === 'AMZN' && p.kind === 'stock');
 const qAmazon = quantities.reconcilePelosiShares(a);
 assert.equal(qAmazon.range.min, 26662); assert.equal(qAmazon.range.max, 113309);
 assert.equal(quantities.reconcilePelosiShares({...a, shareChange: 0}).range, undefined);
 const models = JSON.parse(fs.readFileSync('docs/research/pelosi-annual-ranges.json', 'utf8'));
 for (const [ticker, model] of Object.entries(models)) {
  const position = rows.find(p => p.ticker === ticker && p.kind === 'stock');
  const range = quantities.reconcilePelosiShares(position).range;
  assert.equal(range.min, model.conditional_min, ticker);
  assert.equal(range.max, model.conditional_max, ticker);
 }
});

test('newer snapshots keep share classes separate and do not double-count spinoffs', () => {
 const alphabet = rows.find(p => p.ticker === 'GOOGL' && p.kind === 'stock');
 const q = quantities.reconcilePelosiShares(alphabet);
 assert.equal(q.range.min, Math.ceil(5000001 / 313) + 5000);
 assert.equal(q.range.max, Math.floor(25000000 / 313) + 5000);
 assert.match(q.reason, /Class A only/);
 assert.equal(quantities.reconcilePelosiShares({...alphabet, ticker: 'GOOG'}).range, undefined);
 assert.equal(quantities.reconcilePelosiShares({...alphabet, shareChange: 0}).range, undefined);
 const w = quantities.reconcilePelosiShares(rows.find(p => p.ticker === 'WBD' && p.kind === 'stock'));
 assert.equal(w.range.min, 1735); assert.equal(w.range.max, 3469);
 assert.match(w.reason, /not added again/); assert.match(w.reason, /assumes no subsequent closing/);
});

const portfolio = load('src/lib/pelosi-portfolio.ts', () => { throw new Error('Unexpected dependency'); });
test('portfolio ranks modeled dollars, excludes stale prices, and preserves share ranges', () => {
 const position = rows.find(p => p.ticker === 'AAPL' && p.kind === 'stock');
 const input = (ticker, shares, range, price, date = '2026-09-16') => ({ position: {...position, ticker}, quantity: {shares, range}, quote: {price, date} });
 const result = portfolio.valuePelosiPortfolio([
  input('A', null, {min: 10, max: 30}, 10), input('B', 5, undefined, 100),
  input('OLD', 500, undefined, 100, '2026-09-01'), input('ZERO', 500, undefined, 0),
  input('FUTURE', 500, undefined, 100, '2026-09-18'),
 ], '2026-09-17');
 assert.equal(result.priced, 2); assert.equal(result.total, 700);
 assert.equal(result.rows[0].position.ticker, 'B'); assert.equal(result.rows[0].rank, 1);
 assert.equal(result.rows[1].min, 100); assert.equal(result.rows[1].max, 300);
 assert.ok(Math.abs(result.rows[0].weight + result.rows[1].weight - 1) < 1e-10);
 for (const row of result.rows.slice(2)) { assert.equal(row.weight, null); assert.equal(row.rank, null); }
 assert.equal(portfolio.holdingMarketTicker('SQ'), 'XYZ');
 assert.equal(portfolio.holdingMarketTicker('GOOGL'), 'GOOGL');
});

const live = load('src/lib/pelosi-live-holdings.ts', name => {
 if (name === 'server-only' || name === './supabase-server') return {};
 if (name === './pelosi-holdings') return ledger;
 if (name === './pelosi-share-reconciliation') return quantities;
 return JSON.parse(fs.readFileSync('artifacts/pelosi-audit/2026-position-changes.json', 'utf8'));
});
test('live overlay shifts ranges, adds new stocks, closes positions and rejects overselling', () => {
 const event = (ticker, delta, id) => ({...events[0], ticker, share_delta: delta, contract_delta: 0, strike:null, expiration:null, kind:delta > 0 ? 'purchase':'sale', date:'2026-09-16', doc_id:id});
 const newEvents = [event('AAPL', -100, 'new-1'), event('TEST', 200, 'new-2'), event('MSFT', -25000, 'new-3')];
 const result = live.applyPelosiOverlay(newEvents, '2026-09-17');
 const quantity = ticker => result.quantities.get(result.positions.find(p => p.ticker === ticker && p.kind === 'stock').key);
 assert.equal(quantity('AAPL').range.min, 26151); assert.equal(quantity('AAPL').range.max, 91859);
 const renamed = live.applyPelosiOverlay([event('XYZ', 100, 'rename-1')], '2026-09-17');
 assert.ok(!renamed.positions.some(p => p.ticker === 'XYZ'));
 assert.equal(renamed.quantities.get(renamed.positions.find(p => p.ticker === 'SQ').key).shares, 5100);
 assert.equal(quantity('TEST').shares, 200); assert.equal(quantity('MSFT').shares, 0);
 const again = live.applyPelosiOverlay(newEvents, '2026-09-17');
 assert.equal(again.quantities.get(result.positions.find(p => p.ticker === 'TEST').key).shares, 200);
 assert.throws(() => live.applyPelosiOverlay([event('MSFT', -25001, 'new-4')], '2026-09-17'), /exceeds/);
 assert.throws(() => live.applyPelosiOverlay([newEvents[0],newEvents[0]], '2026-09-17'), /Duplicate/);
});

const congressEngine = load('src/lib/congress-holdings-engine.ts', () => { throw new Error('Unexpected dependency'); });
test('shared Congress engine rolls quantities forward without merging owners or assuming missing openings', () => {
 const base = {key:'SP|ABC|ST',ticker:'ABC',owner:'SP',assetType:'ST',date:'2025-12-31',minShares:100,maxShares:200,source:'annual'};
 const ev = (id,date,type,extra={}) => ({id,key:base.key,date,type,source:'filing',...extra});
 const changes = [ev('buy','2026-01-01','shares',{delta:50}),ev('split','2026-02-01','split',{factor:2}),ev('sell','2026-03-01','shares',{delta:-20}),ev('other','2026-04-01','shares',{key:'SELF|ABC|ST',delta:1000})];
 const q = congressEngine.rollForwardCongressHolding(base,changes,'2026-09-17');
 assert.equal(q.minShares,280); assert.equal(q.maxShares,480);
 assert.equal(congressEngine.rollForwardCongressHolding(base,[ev('over','2026-01-01','shares',{delta:-250})],'2026-09-17').status,'review-needed');
 assert.equal(congressEngine.rollForwardCongressHolding(base,[ev('amend','2026-01-01','review')],'2026-09-17').status,'review-needed');
 assert.throws(()=>congressEngine.rollForwardCongressHolding(base,[changes[0],changes[0]],'2026-09-17'),/Duplicate/);
 assert.equal(congressEngine.rollForwardCongressHolding(base,[ev('split','2026-01-01','split',{factor:2}),changes[0]],'2026-09-17').status,'review-needed');
});

function disclosureReader(fixture) {
 return load('src/lib/politician-disclosure-holdings.ts', name => {
  if (name === 'server-only') return {};
  if (name.endsWith('bounded-rows')) return {readBoundedRows: async () => ({rows:fixture,hasMore:false})};
  if (name.endsWith('supabase-admin')) return {getAdminSupabase:()=>({})};
  if (name.endsWith('politician-amount-range')) return {parsePoliticianAmountRange:()=>({min:1})};
  throw new Error(name);
 });
}
test('annual baseline selection preserves owners, uses valuation date, and never revives older positions', async () => {
 const row=(id,value='None',extra={})=>({source_document_id:id,filed_at:'2026-05-01',payload:{doc_id:'annual',filing_date:'2026-05-01',period_covered_end:'2025-12-31',filing_type:'A',asset_name:'Apple',ticker:'AAPL',asset_type:'ST',owner:'SP',value_range:value,...extra}});
 const data = await disclosureReader([row('1','$1,001 - $15,000'),row('2','$1,001 - $15,000',{owner:'JT'})]).getLatestPoliticianDisclosureHoldings('TEST');
 assert.equal(data.length,2); assert.equal(data[0].valuationDate,'2025-12-31'); assert.notEqual(data[0].key,data[1].key);
 const none = await disclosureReader([row('new'),row('old','$1,001 - $15,000',{doc_id:'older',filing_date:'2025-05-01'})]).getLatestPoliticianDisclosureHoldings('TEST');
 assert.equal(none.length,0);
 await assert.rejects(disclosureReader([row('1','$1,001 - $15,000',{period_covered_end:null})]).getLatestPoliticianDisclosureHoldings('TEST'),/valuation date/);
 await assert.rejects(disclosureReader([row('1','$1,001 - $15,000',{filing_type:'W'})]).getLatestPoliticianDisclosureHoldings('TEST'),/amendment/);
});

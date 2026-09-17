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
  const stock = rows.find(p => p.ticker === ticker && p.kind === 'stock');
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

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { typescriptLoader } from './helpers/load-typescript.mjs';

const { pelosiStrategyOverview, pelosiStrategyActivity } = typescriptLoader()('src/lib/strategies/pelosi-overview.ts');

test('allocation excludes unpriced positions while sectors retain their full denominator', () => {
  const rows = [
    { ticker: 'A', name: 'First', midpoint: 60, sector: 'Information Technology' },
    { ticker: 'B', name: 'Second', midpoint: 40, sector: 'Financials' },
    { ticker: 'MISSING', name: 'Unpriced', midpoint: null, sector: 'Unclassified' },
  ];
  const { mix, sectors } = pelosiStrategyOverview(rows);
  assert.equal(mix.positions, 2);
  assert.deepEqual(Array.from(mix.segments, row => row.percent), [60, 40]);
  assert.equal(sectors.reduce((sum, row) => sum + row.count, 0), 3);
  assert.ok(sectors.every(row => Math.abs(row.percent - 100 / 3) < 1e-10));
  assert.equal(pelosiStrategyOverview([]).mix.segments.length, 0);
  assert.equal(pelosiStrategyOverview(rows.map(row => ({ ...row, midpoint: null }))).mix.segments.length, 0);
});

test('other slice retains every priced position and allocation totals 100 percent', () => {
  const rows = Array.from({ length: 8 }, (_, i) => ({ ticker: `S${i}`, name: `Stock ${i}`, midpoint: i + 1, sector: 'Unclassified' }));
  const { mix } = pelosiStrategyOverview(rows);
  assert.equal(mix.positions, 8);
  assert.equal(mix.segments.length, 6);
  assert.equal(mix.segments.at(-1).ticker, 'Other');
  assert.equal(mix.segments.at(-1).value, 6);
  assert.ok(Math.abs(mix.segments.reduce((sum, row) => sum + row.percent, 0) - 100) < 1e-10);
});

test('activity preserves exercises and spinoffs without treating them as purchases', () => {
  const event = (kind, share_delta, doc_id, ticker = 'TEST') => ({ kind, share_delta, doc_id, ticker, date: '2026-07-24', source: 'https://example.com/source.pdf', page: 2 });
  const rows = pelosiStrategyActivity([
    event('stock_purchase', 100, 'buy'), event('sale', -25, 'sell'), event('exercise', 5000, 'exercise'),
    event('spinoff', 776, 'spinoff'), event('call_purchase', 0, 'option'),
    event('public_partnership_units_purchase', 50, 'units', 'AB'), event('purchase', 200, 'renamed', 'SQ'),
  ], { TEST: 'Test Company' });
  assert.equal(rows.length, 6);
  assert.equal(rows.find(row => row.id === 'exercise').direction, 'other');
  assert.equal(rows.find(row => row.id === 'exercise').action, 'Exercise');
  assert.equal(rows.find(row => row.id === 'spinoff').action, 'Spinoff');
  assert.equal(rows.find(row => row.id === 'sell').quantity, 25);
  assert.equal(rows.find(row => row.id === 'units').unit, 'units');
  assert.equal(rows.find(row => row.id === 'renamed').ticker, 'XYZ');
  assert.ok(rows.every(row => row.source.endsWith('#page=2')));
});

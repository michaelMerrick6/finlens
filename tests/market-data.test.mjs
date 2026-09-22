import assert from 'node:assert/strict';
import { test } from 'node:test';
import { typescriptLoader } from './helpers/load-typescript.mjs';

function harness() {
  const requests = [];
  const points = ['2022-01-03', '2025-01-03', '2026-09-21'];
  const load = typescriptLoader({}, { fetch: async url => {
    const start = Number(new URL(url).searchParams.get('period1'));
    requests.push(start);
    const timestamp = points.map(date => Date.parse(`${date}T00:00:00Z`) / 1000).filter(time => time >= start);
    return { ok: true, status: 200, json: async () => ({ chart: { result: [{
      meta: { instrumentType: 'EQUITY', regularMarketPrice: 100 }, timestamp,
      indicators: { quote: [{ close: timestamp.map(() => 100) }] },
    }] } }) };
  } });
  return { requests, ...load('src/lib/market-data.ts') };
}

test('a request for older prices expands a cached series instead of omitting history', async () => {
  const h = harness();
  await h.getMarketPriceSeries('ABC', '2025-01-01');
  const older = await h.getMarketPriceSeries('abc', '2022-01-01');
  assert.equal(h.requests.length, 2);
  assert.equal(h.getPriceOnOrBefore(older, '2022-01-03'), 100);
  await h.getMarketPriceSeries('ABC', '2025-01-01');
  assert.equal(h.requests.length, 2, 'wider history should serve narrower requests');
});

test('an all-history request expands a dated cache and is then reusable', async () => {
  const h = harness();
  await h.getMarketPriceSeries('ABC', '2025-01-01');
  await h.getMarketPriceSeries('ABC');
  await h.getMarketPriceSeries('ABC', '2022-01-01');
  assert.equal(h.requests.length, 2);
  assert.equal(h.requests[1], 0);
});

test('duplicate ticker requests use their earliest date regardless of input order', async () => {
  for (const dates of [['2022-01-01', '2025-01-01'], ['2025-01-01', '2022-01-01'], [null, '2025-01-01'], ['2025-01-01', null]]) {
    const h = harness();
    const series = await h.getMarketPriceSeriesMap(dates.map(earliestDate => ({ ticker: 'ABC', earliestDate })));
    assert.equal(h.requests.length, 1);
    assert.equal(h.getPriceOnOrBefore(series.get('ABC'), '2022-01-03'), 100);
    if (dates.includes(null)) assert.equal(h.requests[0], 0);
  }
});

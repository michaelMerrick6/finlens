import assert from 'node:assert/strict';
import { test } from 'node:test';
import { typescriptLoader } from './helpers/load-typescript.mjs';

function harness(rows, { companies = [], members = [{ id: 'P000197' }], fail = false } = {}) {
  const calls = [];
  const db = { from() {
    const filters = [];
    let start = 0, end = rows.length - 1;
    const query = {
      select() { return query; }, not() { return query; }, neq() { return query; }, order() { return query; },
      gte() { return query; }, eq(key, value) { filters.push(['eq', key, value]); return query; },
      in(key, value) { filters.push(['in', key, [...value]]); return query; },
      or(value) { filters.push(['or', value]); return query; },
      range(first, last) { start = first; end = last; return query; },
      then(resolve) {
        calls.push({ start, end, filters });
        return Promise.resolve({ data: rows.slice(start, end + 1), error: fail ? new Error('Database unavailable') : null }).then(resolve);
      },
    };
    return query;
  } };
  const load = typescriptLoader({
    'next/server': { NextResponse: { json: (body, options = {}) => ({ body, status: options.status ?? 200 }) } },
    '@/lib/supabase-server': { getPublicSupabase: () => db },
    '@/lib/entity-search': { searchCompaniesDetailed: async () => companies, searchPoliticiansDetailed: async () => members },
    '@/lib/api-errors': { routeErrorMessage: () => 'Unavailable' },
  });
  return { calls, ...load('src/app/api/search-trades/route.ts') };
}

function sampleRows(count) {
  return Array.from({ length: count }, (_, i) => ({
    id: String(i), doc_id: `row-${i}`, member_id: 'P000197', politician_name: 'Nancy Pelosi',
    ticker: 'AAPL', chamber: 'House', published_date: '2025-10-01', transaction_date: '2025-09-01',
    created_at: '2025-10-01', source_url: 'https://example.com', amount_range: '$1,001 - $15,000',
  }));
}
const request = (offset = 0, limit = 2) => new Request(`http://localhost/api/search-trades?q=Pelosi&offset=${offset}&limit=${limit}`);

test('search continues after a source correction removes a row from the first page', async () => {
  const rows = sampleRows(5);
  rows[0] = { ...rows[0], doc_id: 'house-2025-20030630-1', ticker: 'MATW',
    source_url: 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2025/20030630.pdf' };
  const h = harness(rows);
  const first = (await h.GET(request())).body;
  assert.deepEqual([...first.trades.map(row => row.id)], ['1', '2']);
  assert.equal(first.hasMore, true);
  const second = (await h.GET(request(first.nextOffset))).body;
  assert.deepEqual([...second.trades.map(row => row.id)], ['3', '4']);
  assert.equal(second.hasMore, false);
});

test('search paginates past 250 results without skipping or repeating trades', async () => {
  const h = harness(sampleRows(320));
  const ids = [];
  let offset = 0;
  for (let page = 0; page < 10; page++) {
    const response = (await h.GET(request(offset, 50))).body;
    ids.push(...response.trades.map(row => row.id));
    if (!response.hasMore) break;
    assert.ok(response.nextOffset > offset);
    offset = response.nextOffset;
  }
  assert.deepEqual(ids, sampleRows(320).map(row => row.id));
});

test('sparse search pages retain a raw-row continuation cursor', async () => {
  const rows = sampleRows(510).map((row, i) => i < 505 ? { ...row, chamber: 'House', published_date: '2014-01-01' } : row);
  const h = harness(rows);
  const first = (await h.GET(request())).body;
  assert.equal(first.trades.length, 0);
  assert.equal(first.hasMore, true);
  assert.equal(first.nextOffset, 500);
  const second = (await h.GET(request(first.nextOffset))).body;
  assert.deepEqual([...second.trades.map(row => row.id)], ['505', '506']);
});

test('company and politician matches use one union query, preserving direction filters', async () => {
  const h = harness(sampleRows(3), { companies: [{ ticker: 'AAPL', exactMatch: true }] });
  const response = await h.GET(new Request('http://localhost/api/search-trades?q=Apple&direction=buy'));
  assert.equal(response.status, 200);
  assert.equal(h.calls.length, 1);
  assert.deepEqual(h.calls[0].filters, [
    ['eq', 'transaction_type', 'buy'],
    ['or', 'ticker.in.("AAPL"),member_id.in.("P000197")'],
  ]);
});

test('fallback text search keeps pagination and database failures return an error', async () => {
  const h = harness(sampleRows(4), { members: [] });
  assert.equal((await h.GET(request())).body.hasMore, true);
  assert.deepEqual(h.calls[0].filters, [['or', 'politician_name.ilike.%Pelosi%,ticker.ilike.%Pelosi%']]);
  assert.equal((await harness([], { fail: true }).GET(request())).status, 500);
});

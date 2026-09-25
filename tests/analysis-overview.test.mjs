import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFileSync } from 'node:fs';
import { typescriptLoader } from './helpers/load-typescript.mjs';

const load = typescriptLoader();
const { aggregateAnalysis } = load('src/lib/congress-analysis.ts');
const { activityOverview, analysisWindow, matchesAnalysisStock, summarizeAnalysisStock } = load('src/lib/analysis-overview.ts');
const { analysisSector } = load('src/lib/analysis-sectors.ts');
const trade = (id, extra = {}) => ({ id, doc_id: id, source_url: 'https://example.com/filing',
  member_id: 'A000001', politician_name: 'Test member', ticker: 'AAPL', asset_type: 'ST',
  asset_name: 'Apple', transaction_type: 'buy', amount_range: '$1,001 - $15,000',
  transaction_date: '2026-09-17', published_date: '2026-09-23', ...extra });
const stocks = rows => aggregateAnalysis(rows, 'stocks').stocks.map(stock => ({ ...stock, sector: analysisSector(stock.ticker) }));

test('week, month, YTD and rolling year use inclusive dates including leap-day boundaries', () => {
  for (const [period, today, start] of [
    ['7', '2026-09-23', '2026-09-17'], ['30', '2026-09-23', '2026-08-25'],
    ['ytd', '2026-09-23', '2026-01-01'], ['year', '2026-09-23', '2025-09-24'],
    ['year', '2024-02-29', '2023-03-01'], ['30', '2024-03-01', '2024-02-01'],
    ['7', '2026-01-03', '2025-12-28'],
  ]) {
    const window = analysisWindow(period, new Date(today + 'T23:30:00Z'));
    assert.equal(window.start, start);
    assert.equal(window.end, today);
  }
});

test('visuals count complete, deduplicated activity without dropping missing amounts or counting a person twice', () => {
  const result = activityOverview(stocks([
    trade('1'), trade('1'), trade('2', { transaction_type: 'sell', amount_range: 'Unknown' }),
    trade('3', { ticker: 'JPM', member_id: 'B000002' }), trade('4', { ticker: 'JPM' }),
    trade('5', { asset_type: 'OP' }), trade('6', { ticker: null }),
  ]));
  assert.equal(result.total, 4);
  assert.equal(result.buys, 3);
  assert.equal(result.sells, 1);
  assert.equal(result.members, 2);
  assert.equal(result.stocks, 2);
  assert.equal(result.sectors.reduce((sum, sector) => sum + sector.count, 0), result.total);
  assert.equal(result.sectors.reduce((sum, sector) => sum + sector.percent, 0), 100);
});

test('unclassified and ETF transactions remain in the chart denominator; options use the underlying sector', () => {
  const list = stocks([trade('1'), trade('2', { ticker: 'ZZZZTEST', asset_name: 'Unknown company' }),
    trade('3', { ticker: 'SPY', asset_type: 'ETF' })]);
  const result = activityOverview(list);
  assert.equal(result.total, 3);
  assert.equal(result.sectors.find(sector => sector.name === 'Unclassified').count, 1);
  assert.equal(result.sectors.find(sector => sector.name === 'Funds & ETFs').count, 1);
  assert.ok(Math.abs(result.sectors.find(sector => sector.name === 'Information Technology').percent - 100 / 3) < 1e-10);
  const options = aggregateAnalysis([trade('4', { asset_type: 'OP' })], 'options').stocks.map(stock => ({ ...stock, sector: analysisSector(stock.ticker) }));
  assert.equal(activityOverview(options).sectors[0].name, 'Information Technology');
  assert.equal(matchesAnalysisStock(summarizeAnalysisStock(list[1]), 'unknown company', 'Unclassified'), true);
  assert.equal(matchesAnalysisStock(summarizeAnalysisStock(list[0]), 'AAPL', 'Financials'), false);
  assert.equal(matchesAnalysisStock(summarizeAnalysisStock(list[2]), '', 'Funds & ETFs'), true);
});

test('chart includes rows beyond the first 25 stocks; empty periods do not invent percentages', () => {
  const list = Array.from({ length: 40 }, (_, index) => ({ ticker: `TEST${index}`, sector: 'Unclassified', trades: [trade(String(index))] }));
  assert.equal(activityOverview(list).total, 40);
  assert.equal(activityOverview(list).stocks, 40);
  const empty = activityOverview([]);
  assert.equal(empty.total, 0);
  assert.equal(empty.sectors.length, 0);
});

function route(rows, { fail = false, capped = false, details = false } = {}) {
  const pages = [], filters = [], orders = [];
  const db = { from() {
    let predicates = [];
    const query = { select() { return query; }, order(key) { orders.push(key); return query; },
      gte(key, value) { filters.push(['gte', key, value]); predicates.push(row => row[key] >= value); return query; },
      lte(key, value) { filters.push(['lte', key, value]); predicates.push(row => row[key] <= value); return query; },
      in(key, values) { predicates.push(row => values.includes(row[key])); return query; },
      or(value) { const ticker = value.match(/ticker\.ilike\.\*([^*]+)\*/)[1]; predicates.push(row => (row.ticker || '').toUpperCase().includes(ticker) || row.member_id === 'P000197'); return query; },
      async range(start, end) { pages.push([start, end]); return { error: fail ? Error('Database unavailable') : null,
        data: capped ? Array.from({ length: 500 }, (_, i) => trade(`${start + i}`)) : rows.filter(row => predicates.every(test => test(row))).slice(start, end + 1) }; },
    };
    return query;
  } };
  class FixedDate extends Date { constructor(value) { super(value ?? '2026-09-23T12:00:00Z'); } }
  return { pages, filters, orders, ...typescriptLoader({
    'next/server': { NextResponse: { json: (body, options) => ({ body, status: options?.status ?? 200 }) } },
    'next/cache': { unstable_cache: fn => fn },
    './supabase-server': { getPublicSupabase: () => db },
  }, { Date: FixedDate })(details ? 'src/app/api/analysis/trades/route.ts' : 'src/app/api/analysis/route.ts') };
}

test('API pages through the entire period and returns complete classifications without downloading trade details', async () => {
  const api = route(Array.from({ length: 1001 }, (_, i) => trade(String(i))));
  const result = await api.GET(new Request('http://localhost/api/analysis?period=30'));
  assert.equal(result.status, 200);
  assert.equal(result.body.scanned, 1001);
  assert.deepEqual(api.orders.slice(0, 2), ['published_date', 'id']);
  assert.equal(result.body.overview.total, 1001);
  assert.equal(result.body.stocks[0].sector, 'Information Technology');
  assert.equal(result.body.sectorSources.length, 3);
  assert.deepEqual(api.pages, [[0, 499], [500, 999], [1000, 1499], [1500, 1999], [2000, 2499]]);
});

test('stock labels use sourced ticker names rather than mismatched filing text, and are searchable', async () => {
  const result = await route([
    trade('named', { ticker: 'ETN', asset_name: 'Mismatched imported issuer' }),
    trade('unknown', { ticker: 'ZZZZTEST', asset_name: 'Unverified issuer' }),
  ]).GET(new Request('http://localhost/api/analysis?period=30'));
  const named = result.body.stocks.find(stock => stock.ticker === 'ETN');
  assert.equal(named.companyName, 'Eaton Plc');
  assert.equal(matchesAnalysisStock(named, 'Eaton', ''), true);
  assert.equal(named.trades, undefined);
  assert.equal(matchesAnalysisStock(named, 'Mismatched imported issuer', ''), true);
  assert.equal(result.body.stocks.find(stock => stock.ticker === 'ZZZZTEST').companyName, null);
});

test('disclosure and transaction timing select different records and exclude future disclosures', async () => {
  const rows = [trade('new-filing-old-trade', { transaction_date: '2026-08-01' }),
    trade('current-trade'), trade('future-filing', { published_date: '2026-09-24' }),
    trade('before-period', { published_date: '2026-09-16', transaction_date: '2026-09-16' })];
  const filed = await route(rows).GET(new Request('http://localhost/api/analysis?period=7&basis=filed'));
  const traded = await route(rows).GET(new Request('http://localhost/api/analysis?period=7&basis=trade'));
  assert.equal(filed.body.overview.total, 2);
  assert.equal(traded.body.overview.total, 1);
  const details = await route(rows, { details: true }).GET(new Request('http://localhost/api/analysis/trades?period=7&basis=trade&ticker=AAPL'));
  assert.equal(details.body.trades[0].id, 'current-trade');
});

test('API fails closed on incomplete scans or database errors instead of showing partial charts', async () => {
  for (const options of [{ fail: true }, { capped: true }]) {
    const result = await route([], options).GET(new Request('http://localhost/api/analysis?period=year'));
    assert.equal(result.status, 503);
    assert.equal(result.body.stocks, undefined);
  }
  assert.equal((await route([]).GET(new Request('http://localhost/api/analysis?period=bad'))).status, 400);
});


test('full-year summaries remain cacheable and retain counts, search names and totals', async () => {
  const rows = Array.from({ length: 12000 }, (_, i) => trade(String(i), { ticker: `TEST${i % 1200}`, asset_name: 'Repeated filing description '.repeat(20) }));
  const result = await route(rows).GET(new Request('http://localhost/api/analysis?period=year'));
  assert.equal(result.status, 200);
  assert.equal(result.body.overview.total, 12000);
  assert.equal(result.body.stocks.length, 1200);
  assert.ok(result.body.stocks.every(stock => !('trades' in stock) && !('together' in stock)));
  assert.ok(Buffer.byteLength(JSON.stringify(result.body)) < 2 * 1024 * 1024);
  const stock = result.body.stocks[0];
  assert.equal(stock.tradeCount, 10);
  assert.equal(stock.purchaseMin, 10010);
  assert.equal(stock.purchaseMax, 150000);
  assert.equal(matchesAnalysisStock(stock, 'repeated filing', 'Unclassified'), true);
});

test('on-demand details page through only the selected stock with stable ordering and matching totals', async () => {
  const rows = Array.from({ length: 117 }, (_, i) => trade(String(i), { transaction_date: `2026-09-${String(i % 20 + 1).padStart(2, '0')}` }));
  rows.push(trade('other', { ticker: 'MSFT' }), trade('option', { asset_type: 'OP' }), trade('0'));
  const summary = await route(rows).GET(new Request('http://localhost/api/analysis?period=30'));
  const expected = aggregateAnalysis(rows, 'stocks', false).stocks.find(stock => stock.ticker === 'AAPL').trades
    .sort((a, b) => b.published_date.localeCompare(a.published_date) || b.transaction_date.localeCompare(a.transaction_date) || a.id.localeCompare(b.id));
  const received = [];
  for (const offset of [0, 50, 100]) {
    const result = await route(rows, { details: true }).GET(new Request(`http://localhost/api/analysis/trades?period=30&ticker=aapl&offset=${offset}`));
    assert.equal(result.status, 200);
    assert.equal(result.body.total, 117);
    assert.equal(result.body.nextOffset, offset === 100 ? null : offset + 50);
    assert.ok(result.body.trades.length <= 50);
    received.push(...result.body.trades);
  }
  assert.deepEqual(received.map(trade => trade.id), Array.from(expected, trade => trade.id));
  assert.equal(received.length, summary.body.stocks.find(stock => stock.ticker === 'AAPL').tradeCount);
});

test('on-demand details retain reviewed option exercises, ticker corrections, and contribution exclusions', async () => {
  const review = Object.assign({}, ...[2022, 2023, 2024, 2025].map(year => JSON.parse(readFileSync(`src/lib/pelosi-${year}-review.json`, 'utf8'))));
  const corrected = Object.entries(review).find(([, row]) => row.ticker && !row.exclude_from_totals && ['ST', 'STOCK', 'ETF', 'ET'].includes(row.asset_type));
  assert.ok(corrected, 'fixture contains a reviewed ticker correction');
  const [docId, correction] = corrected;
  const rows = [trade('corrected', { member_id: 'P000197', ticker: 'WRONG', doc_id: docId, source_url: correction.source_url }),
    trade('exercise', { member_id: 'P000197', ticker: 'GOOGL', asset_type: 'OP', doc_id: 'house-2026-20033725-1', source_url: 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf' }),
    trade('donated', { asset_name: 'Apple charitable contribution' })];
  for (const ticker of [correction.ticker, 'GOOGL', 'AAPL']) {
    const summary = await route(rows).GET(new Request('http://localhost/api/analysis?period=year'));
    const details = await route(rows, { details: true }).GET(new Request(`http://localhost/api/analysis/trades?period=year&ticker=${ticker}`));
    assert.equal(details.status, 200);
    assert.equal(details.body.total, summary.body.stocks.find(stock => stock.ticker === ticker)?.tradeCount || 0);
  }
});

test('details enforce snapshot date, validate pagination, and never return partial evidence', async () => {
  const rows = [trade('new', { published_date: '2026-09-23' }), trade('snapshot', { published_date: '2026-09-22' })];
  const result = await route(rows, { details: true }).GET(new Request('http://localhost/api/analysis/trades?ticker=AAPL&end=2026-09-22'));
  assert.equal(result.body.total, 1);
  assert.equal(result.body.trades[0].id, 'snapshot');
  for (const query of ['ticker=AAPL&offset=-1', 'ticker=AAPL&offset=1.5', 'ticker=AAPL&offset=50000', 'ticker=AAPL&end=2026-02-30', 'ticker=AAPL&end=2026-09-24', 'ticker=AAPL&end=bad', 'ticker=*', 'ticker=AAPL&offset=NaN', 'ticker=AAPL&period=bad']) {
    assert.equal((await route([], { details: true }).GET(new Request(`http://localhost/api/analysis/trades?${query}`))).status, 400, query);
  }
  const failed = await route([], { details: true, fail: true }).GET(new Request('http://localhost/api/analysis/trades?ticker=AAPL'));
  assert.equal(failed.status, 503);
  assert.equal(failed.body.trades, undefined);
});

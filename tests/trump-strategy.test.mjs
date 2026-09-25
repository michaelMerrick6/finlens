import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import { typescriptLoader } from './helpers/load-typescript.mjs';
const load = typescriptLoader();
const { trumpAnnual, trumpModel, reportedValue, filterReportedAssets, historicalCoverage, parseSourceIndex } = load('src/lib/strategies/trump.ts');
const { annualPriceReturns, usablePrices, lastCompletedMarketDate } = load('src/lib/strategies/price-returns.ts');
const { identifiedStockHoldings, reportedValueMidpoint, reviewedStockActivity, trumpStockReview, trumpActivity, trumpDjt } = load('src/lib/strategies/trump.ts');
const { trumpFirstDisclosure, historicalReviewSummary } = load('src/lib/strategies/trump.ts');
const { runDisclosureBacktest } = load('src/lib/strategies/disclosure-backtest.ts');
const { holdingsMix, sectorBreakdown, holdingSector, filterStrategyHoldings, filterStrategyTrades, compactMoney } = load('src/lib/strategies/trump-overview.ts');
const point = (date, price) => ({ date, price });

test('compact dollar labels pin fraction bounds so Safari and the server render the same text', () => {
  // Model Safari's currency default: omitted minimumFractionDigits retains a trailing zero.
  const safari = typescriptLoader({}, { Intl: { NumberFormat: function (locale, options) {
    return new Intl.NumberFormat(locale, { minimumFractionDigits: Math.min(2, options.maximumFractionDigits), ...options });
  } } })('src/lib/strategies/trump-overview.ts').compactMoney;
  for (const [value, label] of [[39000005, '$39M'], [35250004, '$35.3M'], [26975006, '$27M'],
    [250001, '$250K'], [500000, '$500K'], [0, '$0']]) {
    assert.equal(compactMoney(value), label);
    assert.equal(safari(value), label);
  }
});

test('holdings visualization cannot mix DJT shares, invalid values or missing estimates into dollar percentages', () => {
  const rows = [
    { ticker: 'A', name: 'First', estimatedReportedValue: 60 },
    { ticker: 'B', name: 'Second', estimatedReportedValue: 40 },
    { ticker: 'DJT', name: 'Trust shares', estimatedReportedValue: 114750000 },
    ...[null, NaN, Infinity, 0, -2].map((value, i) => ({ ticker: `INVALID${i}`, name: 'Invalid', estimatedReportedValue: value })),
  ];
  const result = holdingsMix(rows);
  assert.equal(result.total, 100);
  assert.equal(result.positions, 2);
  assert.deepEqual(Array.from(result.segments, row => row.percent), [60, 40]);
  assert.equal(holdingsMix([]).segments.length, 0);
});

test('holdings chart accounts for every displayed estimate without modifying model allocation weights', () => {
  const stocks = identifiedStockHoldings();
  const result = holdingsMix(stocks);
  assert.equal(result.positions, 44);
  assert.equal(result.segments.length, 6);
  assert.equal(result.segments.at(-1).name, '39 other stocks');
  assert.equal(result.segments.reduce((sum, row) => sum + row.value, 0), result.total);
  assert.ok(Math.abs(result.segments.reduce((sum, row) => sum + row.percent, 0) - 100) < 1e-9);
  assert.ok(stocks.every(row => row.weight === null && row.currentValue === null));
});

test('sector counts include DJT once and keep separate share classes as separate ticker positions', () => {
  const tickers = [...identifiedStockHoldings().map(row => row.ticker), 'DJT', 'DJT'];
  const result = sectorBreakdown(tickers);
  assert.equal(result.reduce((sum, row) => sum + row.count, 0), 45);
  assert.equal(result.length, 8);
  assert.equal(result.find(row => row.name === 'Communication services').count, 6);
  assert.ok(Math.abs(result.reduce((sum, row) => sum + row.percent, 0) - 100) < 1e-9);
  assert.equal(holdingSector('UNKNOWN'), 'Unclassified');
  assert.equal(sectorBreakdown([]).length, 0);
});

test('stock and sector filters combine while empty searches retain the full eligible list', () => {
  const rows = [{ ticker: 'AAPL', name: 'Apple' }, { ticker: 'DJT', name: 'Trump Media & Technology Group' }];
  assert.equal(filterStrategyHoldings(rows, 'trump media', '').length, 1);
  assert.equal(filterStrategyHoldings(rows, 'DJT', 'Information Technology').length, 0);
  assert.equal(filterStrategyHoldings(rows, '', '').length, 2);
});

test('activity filters sort by actual trade dates without rewriting duplicates or treating report dates as trade dates', () => {
  const result = filterStrategyTrades(trumpActivity.rows, '', 'sale');
  assert.equal(result.length, 41);
  assert.equal(result.filter(row => row.date === '2026-06-24').length, 27);
  assert.equal(result[0].date, '2026-06-24');
  assert.notEqual(result[0].date, trumpActivity.source.signedOn);
  assert.ok(result.every(row => row.type === 'sale'));
  assert.ok(filterStrategyTrades(trumpActivity.rows, 'GS', 'purchase').every(row => row.ticker === 'GS'));
  assert.equal(filterStrategyTrades(trumpActivity.rows, 'zzzz', '').length, 0);
});

function modelFixture(dates = ['2015-07-23', '2015-07-24', '2015-07-27', '2015-07-28', '2015-07-29']) {
  const series = values => ({ currency: 'USD', corporateActionsReviewed: true,
    points: dates.map((date, index) => ({ date, close: values[index], adjustedClose: values[index] })) });
  return { historyComplete: true, throughDate: dates.at(-1),
    calendar: { verified: true, startDate: '2015-07-22', endDate: dates.at(-1), sessions: dates },
    snapshots: [{ id: 'first', publishedOn: '2015-07-22', complete: true,
      holdings: ['A', 'B'].map(securityId => ({ securityId, basis: { type: 'range', low: 100, high: 100 } })) }],
    prices: { A: series([100, 110, 120, 120, 120]), B: series([100, 100, 100, 110, 99]) } };
}

test('model waits until after publication, lets weights drift and preserves rebalance-day gains', () => {
  const input = modelFixture();
  input.snapshots.push({ id: 'second', publishedOn: '2015-07-24', complete: true,
    holdings: [{ securityId: 'B', basis: { type: 'range', low: 100, high: 100 } }] });
  const result = runDisclosureBacktest(input);
  assert.equal(result.status, 'ready');
  assert.deepEqual(Array.from(result.points, p => p.value), [10000, 10500, 11000, 12100, 10890]);
  assert.deepEqual(Array.from(result.points, p => p.snapshotId), ['first', 'first', 'second', 'second', 'second']);
  assert.ok(Math.abs(result.years[0].returnPercent - 8.9) < 1e-9);
  assert.equal(result.years[0].partial, true);
});

test('exact share counts use raw market value while returns use adjusted closes', () => {
  const input = modelFixture(['2015-07-23', '2015-07-24']);
  input.snapshots[0].holdings = [{ securityId: 'A', basis: { type: 'shares', shares: 10 } },
    { securityId: 'B', basis: { type: 'range', low: 1000, high: 1000 } }];
  input.prices.A.points = [{ date: '2015-07-23', close: 100, adjustedClose: 10 },
    { date: '2015-07-24', close: 200, adjustedClose: 20 }];
  const result = runDisclosureBacktest(input);
  assert.equal(result.status, 'ready');
  assert.equal(result.points[1].value, 15000);
});

test('a weekend disclosure superseded before trading does not require an unused price series', () => {
  const input = modelFixture(['2015-07-27', '2015-07-28']);
  input.snapshots[0].publishedOn = '2015-07-25';
  input.snapshots[0].holdings = [{ securityId: 'UNUSED', basis: { type: 'range', low: 100, high: 100 } }];
  input.snapshots.push({ id: 'newer', publishedOn: '2015-07-26', complete: true,
    holdings: [{ securityId: 'B', basis: { type: 'range', low: 100, high: 100 } }] });
  const result = runDisclosureBacktest(input);
  assert.equal(result.status, 'ready');
  assert.ok(result.points.every(point => point.snapshotId === 'newer'));
});

test('unreviewed history, open ranges and ambiguous snapshot duplicates cannot yield returns', () => {
  const mutations = [
    input => { input.historyComplete = false; },
    input => { input.snapshots[0].complete = false; },
    input => { input.snapshots[0].publishedOn = null; },
    input => { input.snapshots[0].holdings[0].basis.high = null; },
    input => { input.snapshots[0].holdings[0].basis.low = 0; },
    input => { input.snapshots[0].holdings.push(input.snapshots[0].holdings[0]); },
    input => { input.snapshots.push({ ...input.snapshots[0], id: 'amendment' }); },
    input => { input.calendar.verified = false; },
    input => { input.calendar.sessions[1] = '2015-07-23'; },
    input => { input.calendar.endDate = '2015-07-28'; },
  ];
  for (const mutate of mutations) {
    const input = modelFixture(); mutate(input);
    const result = runDisclosureBacktest(input);
    assert.equal(result.status, 'blocked');
    assert.ok(result.issues.length > 0);
    assert.equal(result.points.length, 0);
    assert.equal(result.years.length, 0);
  }
});

test('missing delisted security, an interior missing close or unreviewed actions block the entire model', () => {
  for (const mutate of [input => { delete input.prices.A; },
    input => { input.prices.A.points.splice(1, 1); },
    input => { input.prices.A.corporateActionsReviewed = false; },
    input => { input.prices.A.points.push(input.prices.A.points[0]); },
    input => { input.prices.A.points[1].adjustedClose = NaN; }]) {
    const input = modelFixture(); mutate(input);
    const result = runDisclosureBacktest(input);
    assert.equal(result.status, 'blocked');
    assert.match(result.issues.join(' '), /A:/);
    assert.equal(result.years.length, 0, 'never rescale the remaining stock or report zero');
  }
});

test('a reviewed empty stock snapshot holds model cash until new disclosed stocks are available', () => {
  const input = modelFixture();
  input.snapshots.push({ id: 'no-eligible-stocks', publishedOn: '2015-07-24', complete: true, holdings: [] });
  const result = runDisclosureBacktest(input);
  assert.equal(result.status, 'ready');
  assert.deepEqual(Array.from(result.points, p => p.value), [10000, 10500, 11000, 11000, 11000]);
});

test('portfolio calendar returns use the prior year-end value, without annualizing the first period', () => {
  const input = modelFixture(['2015-07-23', '2015-12-31', '2016-01-04', '2016-12-30']);
  input.throughDate = '2016-12-31'; input.calendar.endDate = input.throughDate;
  input.snapshots[0].holdings = [input.snapshots[0].holdings[0]];
  input.prices.A.points.forEach((point, index) => { point.close = point.adjustedClose = [100, 120, 130, 144][index]; });
  const result = runDisclosureBacktest(input);
  assert.equal(result.status, 'ready');
  assert.ok(result.years.every(y => Math.abs(y.returnPercent - 20) < 1e-9));
  assert.equal(result.years[0].partial, true);
  assert.equal(result.years[1].partial, false);
});

test('2015 transcription preserves every sequential row across all five brokerage accounts', () => {
  const rows = trumpFirstDisclosure.rows;
  assert.equal(rows.length, 326);
  assert.equal(new Set(rows.map(row => row.id)).size, 326);
  const accounts = { 'Other assets': 26, 'Barclays brokerage': 33, 'Oppenheimer brokerage': 32,
    'Deutsche A/C 1': 116, 'Deutsche A/C 2': 59, 'JP Morgan Clearing brokerage': 60 };
  for (const [account, count] of Object.entries(accounts)) {
    assert.deepEqual(Array.from(rows.filter(row => row.account === account), row => row.line),
      Array.from({ length: count }, (_, i) => i + 1));
  }
  assert.ok(rows.every(row => row.page >= 35 && row.page <= 45 && row.high >= row.low));
  assert.equal(trumpFirstDisclosure.part6Complete, true);
  assert.equal(trumpFirstDisclosure.portfolioComplete, false);
  assert.equal(trumpFirstDisclosure.identityReviewComplete, false);
  assert.equal(trumpFirstDisclosure.holdingsAsOf, null, 'candidate filing is not a year-end snapshot');
  assert.equal(trumpFirstDisclosure.filedOn, '2015-07-15');
  assert.equal(trumpFirstDisclosure.publishedOn, '2015-07-22', 'filing date must not introduce look-ahead');
  assert.equal(historicalReviewSummary().verifiedReturnYears, 0);
});

test('2015 stock candidates exclude issuer debt, funds, ambiguous instruments and threshold rows', () => {
  const apple = trumpFirstDisclosure.rows.filter(row => /APPLE/i.test(row.name));
  const candidates = apple.filter(row => row.scopeStatus === 'stock-candidate');
  assert.equal(candidates.length, 3);
  assert.equal(candidates.reduce((sum, row) => sum + row.low, 0), 1750003);
  assert.equal(candidates.reduce((sum, row) => sum + row.high, 0), 6500000);
  assert.equal(apple.filter(row => row.scopeStatus === 'excluded-interest-bearing').length, 2);
  assert.equal(apple.filter(row => row.scopeStatus === 'below-reporting-threshold').length, 1);
  const unresolved = trumpFirstDisclosure.rows.filter(row => row.scopeStatus === 'unresolved-instrument');
  assert.equal(unresolved.length, 11);
  assert.ok(unresolved.some(row => row.name.startsWith('ENERGY TRANSFER')), 'EIF flag alone cannot classify an operating partnership as a fund');
  assert.ok(unresolved.some(row => row.account === 'Deutsche A/C 2' && row.name === 'COMCAST CORP'));
  assert.ok(trumpFirstDisclosure.rows.every(row => !('weight' in row) && !('ticker' in row)), 'no invented allocations or historical ticker aliases');
});

test('historical download cannot leak a modern DJT holding into the 2015 evidence', async () => {
  const { GET } = typescriptLoader({}, { Response })('src/app/api/strategies/trump/holdings/route.ts');
  const historical = await GET(new Request('http://localhost/api/strategies/trump/holdings?filing=2015'));
  assert.equal(historical.status, 200);
  assert.match(historical.headers.get('Content-Disposition'), /2015/);
  const data = await historical.json();
  assert.equal(data.rows.length, 326);
  assert.equal(data.coverage.currentHoldingsVerified, false);
  assert.equal(data.coverage.allocationAvailable, false);
  assert.equal(data.djtOwnershipEvidence, undefined);
  const latest = await (await GET(new Request('http://localhost/api/strategies/trump/holdings'))).json();
  assert.equal(latest.rows.length, 6181);
  assert.equal(latest.djtOwnershipEvidence.shares, 114750000);
  assert.equal((await GET(new Request('http://localhost/api/strategies/trump/holdings?filing=2022'))).status, 400);
});

test('located 2019 and 2021 filings improve source coverage without manufacturing returns', () => {
  const years = historicalCoverage(2026);
  assert.match(years.find(row => row.year === 2019).source.url, /trump-oge-2019/);
  assert.match(years.find(row => row.year === 2021).source.url, /2021Termination/);
  assert.ok(years.every(row => row.returnPercent === null));
});

test('stock identities keep a company bond out of its common-stock evidence', () => {
  const apple = identifiedStockHoldings(' AAPL ');
  assert.equal(apple.length, 1);
  assert.equal(apple[0].ticker, 'AAPL');
  assert.equal(apple[0].rows.length, 9);
  assert.equal(apple[0].rows.some(r => r.id === '96-3'), false, 'Apple 4.3% bond must not become AAPL');
  assert.equal(apple[0].rows.some(r => r.name.includes('HOSPITALITY')), false);
  assert.equal(identifiedStockHoldings('apple microsoft').length, 0);
  assert.equal(identifiedStockHoldings('Microsoft')[0].ticker, 'MSFT');
});

test('reviewed spelling variants and former company names retain their annual holdings', () => {
  // Added after comparing the original PDF pages against the displayed totals.
  const cases = [
    ['GE', ['19-6', '72-1'], 2480008, 11050000],
    ['IBM', ['27-3', '77-18'], 2195010, 7450000],
    ['JNJ', ['19-24'], 3330010, 12850000],
    ['KO', ['10-12', '24-34'], 2545009, 11250000],
    ['LLY', ['18-37'], 2550007, 11200000],
    ['META', ['19-2'], 7500009, 33100000],
    ['RTX', ['20-21'], 1896008, 6915000],
    ['WFC', ['21-24'], 1431007, 5965000],
  ];
  const stocks = new Map(identifiedStockHoldings().map(stock => [stock.ticker, stock]));
  for (const [ticker, addedIds, low, high] of cases) {
    const stock = stocks.get(ticker);
    assert.ok(addedIds.every(id => stock.rows.some(row => row.id === id)), `${ticker}: verified source row was omitted`);
    assert.equal(stock.low, low, ticker);
    assert.equal(stock.high, high, ticker);
    assert.equal(stock.estimatedReportedValue, (low + high) / 2, ticker);
  }
});

test('expanded company aliases still exclude issuer notes, preferred shares and separate businesses', () => {
  const rows = identifiedStockHoldings().flatMap(stock => stock.rows);
  for (const id of ['21-32', '21-37', '22-10', '22-23', '78-23', '79-19']) {
    assert.equal(rows.some(row => row.id === id), false, `${id}: fixed-income or preferred holding entered common stocks`);
  }
  const stocks = new Map(identifiedStockHoldings().map(stock => [stock.ticker, stock]));
  assert.ok(stocks.get('JNJ').rows.every(row => !/CONTROLS|CTLS/.test(row.name)));
  assert.ok(stocks.get('GE').rows.every(row => !/HEALTHCARE|VERNOVA/.test(row.name)));
});

test('ambiguous share classes and income-only family trusts do not enter the stock index', () => {
  const rows = identifiedStockHoldings().flatMap(s => s.rows);
  assert.ok(rows.every(r => r.account && !r.account.startsWith('FAMILY TRUST')));
  assert.equal(rows.some(r => r.name === 'ALPHABET INC'), false, 'unspecified Class A/C remains unresolved');
  assert.equal(rows.some(r => r.name === 'BERKSHIRE HATHAWAY INC'), false);
  const google = identifiedStockHoldings('Alphabet');
  assert.deepEqual(Array.from(google, s => s.ticker), ['GOOG', 'GOOGL']);
  assert.ok(google.every(s => s.rows.every(r => s.ticker === 'GOOG' ? /CL-?C|CL C|CLASS C|CLASS CLASS C/.test(r.name) : /CL-?A|CL A|CLASS CLASS A/.test(r.name))));
});

test('partial review never normalizes a subset or marks annual ranges as current values', () => {
  assert.equal(trumpStockReview.complete, false);
  assert.equal(trumpModel.scope, 'public-company-stocks');
  const stocks = identifiedStockHoldings();
  assert.equal(stocks.length, 44);
  assert.ok(stocks.every(s => s.weight === null && s.currentValue === null));
  for (const stock of stocks) {
    assert.equal(stock.low, stock.rows.reduce((sum, r) => sum + r.low, 0));
    assert.equal(stock.high, stock.rows.reduce((sum, r) => sum + r.high, 0));
  }
});

test('a single disclosed-value estimate preserves uncertainty and does not become a live balance', async () => {
  const apple = identifiedStockHoldings('AAPL')[0];
  assert.equal(apple.low, 14000009);
  assert.equal(apple.high, 64000000);
  assert.equal(apple.estimatedReportedValue, 39000004.5);
  assert.equal(apple.currentValue, null);
  assert.equal(apple.weight, null);
  assert.equal(reportedValueMidpoint(15001, 50000), 32500.5);
  const { GET } = typescriptLoader({}, { Response })('src/app/api/strategies/trump/holdings/route.ts');
  const data = await (await GET(new Request('http://localhost/api/strategies/trump/holdings'))).json();
  assert.equal(data.stockIndex.holdings.find(stock => stock.ticker === 'AAPL').estimatedReportedValue, 39000004.5);
  assert.match(data.stockIndex.estimateMethod, /not an exact balance/);
  assert.equal(data.coverage.currentHoldingsVerified, false);
});

test('open-ended and invalid ranges cannot produce an invented point estimate', () => {
  for (const [low, high] of [[50000001, null], [NaN, 1000], [1, Infinity], [-1, 1000], [1000, 1]]) {
    assert.equal(reportedValueMidpoint(low, high), null);
  }
  assert.equal(reportedValueMidpoint(250, 250), 250);
});

test('newer checked sales retain their source and cannot imply a closed position or a rebalance date', () => {
  assert.equal(trumpActivity.complete, false);
  assert.equal(trumpActivity.source.publishedOn, null);
  assert.equal(trumpActivity.rows.length, 56);
  assert.equal(new Set(trumpActivity.rows.map(r => r.id)).size, 56);
  assert.equal(trumpActivity.rows.some(r => r.account !== null), false, 'no invented account match');
  const apple = reviewedStockActivity('AAPL');
  assert.equal(apple.length, 1);
  assert.equal(apple[0].date, '2026-06-23');
  assert.equal(apple[0].type, 'sale');
  assert.equal(apple[0].page, 2);
  assert.equal(apple[0].line, 23);
  assert.equal(apple[0].low, 50001);
  assert.equal(apple[0].high, 100000);
  assert.equal('shares' in apple[0], false);
  assert.ok(trumpActivity.rows.every(r => r.low > 0 && r.high > r.low));
});

test('the SEC trustee filing corroborates a single DJT trust stake, excluding the trustee personal awards', () => {
  assert.equal(trumpDjt.shares, 114750000);
  assert.equal(trumpDjt.shares, trumpAnnual.djt.shares);
  assert.equal(trumpDjt.ownershipAsOf, '2026-06-19');
  assert.match(trumpDjt.sourceUrl, /^https:\/\/www\.sec\.gov\/Archives\//);
  assert.match(trumpDjt.note, /own direct shares and RSUs are excluded/);
  assert.equal(identifiedStockHoldings('DJT').length, 0, 'trust stake is displayed separately, not duplicated among account rows');
});

test('annual evidence retains every original row, including repeated names and line numbers', () => {
  assert.equal(trumpAnnual.rows.length, 6181);
  assert.equal(new Set(trumpAnnual.rows.map(r => r.id)).size, 6181);
  assert.equal(trumpAnnual.rows.filter(r => r.low > 0).length, 4686);
  assert.ok(trumpAnnual.rows.filter(r => r.name === 'APPLE INC').length > 1);
  assert.equal(trumpAnnual.djt.shares, 114750000);
  assert.equal(trumpAnnual.djt.page, 865);
  assert.equal(trumpModel.includesDjt, true);
  assert.equal(trumpModel.holdingsReconciled, false);
  assert.ok(trumpAnnual.rows.every(r => r.low >= 0 && (r.high === null || r.high >= r.low)));
});

test('annual account context changes within pages and survives page breaks', () => {
  assert.equal(trumpAnnual.rows.find(r => r.page === 21 && r.line === 1).account, 'INVESTMENT ACCOUNT #3');
  assert.equal(trumpAnnual.rows.find(r => r.page === 31 && r.line === 1).account, 'INVESTMENT ACCOUNT #4');
  assert.equal(trumpAnnual.rows.find(r => r.id === '136-31').account, 'INVESTMENT ACCOUNT #8');
  assert.equal(trumpAnnual.rows.filter(r => r.account === 'FAMILY TRUST 1*').length, 20);
  assert.ok(trumpAnnual.rows.every(r => r.account && typeof r.incomeType === 'string'));
});

test('wrapped PDF descriptions are retained, and bond names are not relabeled as stock tickers', () => {
  const apple = trumpAnnual.rows.find(r => r.page === 31 && r.line === 14);
  assert.equal(apple.name, 'APPLE INC COM');
  assert.ok(trumpAnnual.rows.some(r => r.name.includes('APPLE INC. 4.3%33 DUE 05/10/33')));
  assert.equal(trumpAnnual.rows.some(r => 'ticker' in r), false);
});

test('none/under-threshold rows are excluded by default without claiming a full sale', () => {
  const rows = [{ name: 'APPLE INC', low: 1001, high: 15000 }, { name: 'APPLE INC BOND', low: 0, high: 1000 }];
  assert.equal(filterReportedAssets(rows, ' apple ').length, 1);
  assert.equal(filterReportedAssets(rows, 'apple bond', true).length, 1);
  assert.equal(filterReportedAssets(rows, 'apple bond').length, 0);
  assert.equal(reportedValue(0, 1000), 'None or under $1,001');
  assert.equal(reportedValue(50000001, null), 'Over $50,000,000');
});

test('missing historical coverage is unavailable, never zero or an invented backtest', () => {
  const years = historicalCoverage(2026);
  assert.equal(years[0].year, 2015);
  assert.equal(years.length, 12);
  assert.ok(years.every(y => y.returnPercent === null));
  assert.equal(years.find(y => y.year === 2022).source, null);
});

test('discovery only accepts official Trump PDF links, deduplicates, and ignores unrelated names', () => {
  const html = `<a href="/wp-content/a.pdf">President Donald J. Trump <span>2025 Annual Report</span></a>
    <a href="/wp-content/a.pdf">President Donald J. Trump 2025 Annual Report</a>
    <a href="https://evil.example/a.pdf">President Donald J. Trump</a>
    <a href="https://www.whitehouse.gov.evil.example/a.pdf">President Donald J. Trump</a>
    <a href="/a.pdf">Vice President JD Vance</a><a href="/administration/trump/">President Donald J. Trump</a>`;
  const result = parseSourceIndex(html);
  assert.equal(result.length, 1);
  assert.equal(result[0].url, 'https://www.whitehouse.gov/wp-content/a.pdf');
});

test('annual returns chain prior year-end closes and label the first year and YTD as partial', () => {
  const rows = annualPriceReturns([point('2024-03-26', 100), point('2024-12-31', 80),
    point('2025-01-02', 90), point('2025-12-31', 120), point('2026-09-21', 60)], '2024-03-26', '2026-09-21');
  assert.ok(Math.abs(rows[0].returnPercent + 20) < 1e-8);
  assert.equal(rows[1].returnPercent, 50, 'use prior year end, not the first January close');
  assert.equal(rows[2].returnPercent, -50);
  assert.deepEqual(Array.from(rows, r => r.partial), [true, false, true]);
});

test('missing price endpoints, stale prices and absent inception do not produce returns', () => {
  assert.equal(annualPriceReturns([point('2024-05-01', 50), point('2024-12-31', 70)], '2024-03-26', '2024-12-31')[0].returnPercent, null);
  assert.equal(annualPriceReturns([point('2024-03-26', 50), point('2024-11-30', 70)], '2024-03-26', '2024-12-31')[0].returnPercent, null);
  const gap = annualPriceReturns([point('2024-03-26', 50), point('2025-01-10', 75), point('2025-12-31', 100)], '2024-03-26', '2025-12-31');
  assert.equal(gap[1].returnPercent, null);
  assert.ok(annualPriceReturns([], '2024-03-26', '2026-09-21').every(r => r.returnPercent === null));
});

test('predecessor prices, future points and invalid prices cannot contaminate DJT history', () => {
  const values = usablePrices([point('2024-03-25', 1), point('2024-03-26', 10), point('2024-03-27', 0),
    point('2024-03-28', NaN), point('2024-03-29', 20), point('2027-01-01', 100)], '2024-03-26', '2026-09-21');
  assert.deepEqual(Array.from(values, r => r.date), ['2024-03-26', '2024-03-29']);
});

test('current session is excluded until after New York close, with daylight saving time respected', () => {
  assert.equal(lastCompletedMarketDate(new Date('2026-09-21T19:00:00Z')), '2026-09-20');
  assert.equal(lastCompletedMarketDate(new Date('2026-09-21T20:16:00Z')), '2026-09-21');
  assert.equal(lastCompletedMarketDate(new Date('2026-01-15T20:30:00Z')), '2026-01-14');
  assert.equal(lastCompletedMarketDate(new Date('2026-01-15T21:16:00Z')), '2026-01-15');
});

test('failed source checks do not claim success or advance freshness', async () => {
  const h = typescriptLoader({}, { fetch: async () => ({ ok: false }) })('src/lib/strategies/trump-server.ts');
  const result = await h.checkTrumpSources();
  assert.equal(result.available, false);
  assert.equal(result.checkedAt, null);
});

test('new source documents are flagged instead of silently changing holdings', async () => {
  const h = typescriptLoader({}, { fetch: async () => ({ ok: true, text: async () => `<a href="/new.pdf">President Donald J. Trump 2026 Annual Report</a>` }) })('src/lib/strategies/trump-server.ts');
  const result = await h.checkTrumpSources();
  assert.equal(result.available, true);
  assert.equal(result.newReports.length, 1);
  assert.equal(result.newReports[0].url, 'https://www.whitehouse.gov/new.pdf');
});

test('source artifact carries an immutable checksum and explicit reporting period', () => {
  assert.match(trumpAnnual.sha256, /^[a-f0-9]{64}$/);
  assert.equal(trumpAnnual.holdingsAsOf, '2025-12-31');
  assert.equal(trumpAnnual.publishedOn, '2026-06-30');
  assert.ok(fs.readFileSync('scripts/strategies/import_trump_annual.py', 'utf8').includes(trumpAnnual.sha256));
});

test('performance never mixes unadjusted closing prices with adjusted history', async () => {
  for (const meta of [{ symbol: 'DJT', currency: 'USD' }, { symbol: 'OTHER', currency: 'USD' }, { symbol: 'DJT', currency: 'CAD' }]) {
    const h = typescriptLoader({}, { fetch: async () => ({ ok: true, json: async () => ({ chart: { result: [{
      meta, timestamp: [1711459800], indicators: { quote: [{ close: [50] }] },
    }] } }) }) })('src/lib/strategies/trump-server.ts');
    assert.equal((await h.getDjtAdjustedPrices()).length, 0);
  }
  const h = typescriptLoader({}, { fetch: async () => ({ ok: true, json: async () => ({ chart: { result: [{
    meta: { symbol: 'DJT', currency: 'USD', dataGranularity: '1d' }, timestamp: [1711459800, 1711546200],
    indicators: { quote: [{ close: [50, 60] }], adjclose: [{ adjclose: [40, null] }] },
  }] } }) }) })('src/lib/strategies/trump-server.ts');
  const prices = await h.getDjtAdjustedPrices();
  assert.equal(prices.length, 1);
  assert.equal(prices[0].price, 40);
});

test('nearby weekly quotes cannot stand in for first-listing and year-end closes', () => {
  const missingLastDay = annualPriceReturns([point('2024-03-26', 50), point('2024-12-30', 30),
    point('2025-12-29', 20)], '2024-03-26', '2025-12-31');
  assert.ok(missingLastDay.every(r => r.returnPercent === null));
  assert.equal(annualPriceReturns([point('2024-03-27', 50), point('2024-12-31', 30)], '2024-03-26', '2024-12-31')[0].returnPercent, null);
});

test('weekly vendor data is rejected even if it includes adjusted prices', async () => {
  const h = typescriptLoader({}, { fetch: async () => ({ ok: true, json: async () => ({ chart: { result: [{
    meta: { symbol: 'DJT', currency: 'USD', dataGranularity: '1wk' }, timestamp: [1711459800],
    indicators: { adjclose: [{ adjclose: [50] }] },
  }] } }) }) })('src/lib/strategies/trump-server.ts');
  assert.equal((await h.getDjtAdjustedPrices()).length, 0);
});

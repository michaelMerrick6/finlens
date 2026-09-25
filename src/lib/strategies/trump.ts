import annualData from './trump-annual.json';
import sourceData from './trump-sources.json';
import stockData from './trump-stocks.json';
import activityData from './trump-reviewed-activity.json';
import djtData from './trump-djt.json';
import firstDisclosure from './trump-2015.json';

export type ReportedAsset = {
  id: string;
  name: string;
  page: number;
  line: number;
  low: number;
  high: number | null;
  account?: string;
  incomeType?: string;
  eif?: string;
  scopeStatus?: string;
};

export const trumpAnnual = annualData;
export const trumpSources = sourceData;
export const trumpStockReview = stockData;
export const trumpActivity = activityData;
export const trumpDjt = djtData;
export const trumpFirstDisclosure = firstDisclosure;

export const historicalScopeLabels: Record<string, string> = {
  'stock-candidate': 'Stock candidate · identity review pending',
  'unresolved-instrument': 'Instrument needs review',
  'below-reporting-threshold': 'None / under reporting threshold',
  'excluded-interest-bearing': 'Interest-bearing holding · outside stock model',
  'excluded-fund': 'Fund · outside stock model',
  'excluded-cash': 'Cash / deposit · outside stock model',
  'excluded-other': 'Other asset · outside stock model',
};

export function historicalReviewSummary() {
  const rows = firstDisclosure.rows;
  return {
    sourceRows: rows.length,
    stockCandidates: rows.filter(row => row.scopeStatus === 'stock-candidate').length,
    unresolvedInstruments: rows.filter(row => row.scopeStatus === 'unresolved-instrument').length,
    accounts: new Set(rows.filter(row => row.account !== 'Other assets').map(row => row.account)).size,
    verifiedReturnYears: 0,
  };
}
export const trumpModel = {
  includesDjt: true,
  scope: 'public-company-stocks',
  earliestSourceYear: 2015,
  holdingsReconciled: false,
  historyReviewed: false,
  // None/$1,000 is not a confirmed holding or a confirmed complete sale.
  note: 'The annual snapshot has not yet been reconciled with subsequent transaction reports.',
} as const;

export function reportedValueMidpoint(low: number, high: number | null): number | null {
  if (high === null || !Number.isFinite(low) || !Number.isFinite(high) || low < 0 || high < low) return null;
  return low + (high - low) / 2;
}

export function identifiedStockHoldings(query = '') {
  // A stale identity index must not attach a ticker to changed source rows.
  if (stockData.annualSha256 !== annualData.sha256) return [];
  const evidence = new Map(annualData.rows.map(row => [row.id, row]));
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  return stockData.stocks.flatMap(stock => {
    const rows = stock.rows.map(ref => evidence.get(ref.id));
    if (rows.some((row, index) => !row || row.name !== stock.rows[index].name || row.low <= 0)) return [];
    const verified = rows.filter((row): row is NonNullable<typeof row> => !!row);
    const search = `${stock.ticker} ${stock.name} ${verified.map(row => row.name).join(' ')}`.toLowerCase();
    if (!terms.every(term => search.includes(term))) return [];
    const low = verified.reduce((sum, row) => sum + row.low, 0);
    const high = verified.some(row => row.high === null) ? null : verified.reduce((sum, row) => sum + row.high!, 0);
    return [{ ticker: stock.ticker, name: stock.name, rows: verified,
      low, high,
      // A point estimate of disclosed value, not a known balance or a current market value.
      estimatedReportedValue: reportedValueMidpoint(low, high),
      accounts: [...new Set(verified.map(row => row.account))],
      // Never normalize a partly reviewed subset into a claimed full allocation.
      weight: null, currentValue: null,
    }];
  });
}

export function reviewedStockActivity(query = '') {
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  return activityData.rows.filter(row => terms.every(term => `${row.ticker} ${row.name}`.toLowerCase().includes(term)))
    .sort((a, b) => b.date.localeCompare(a.date) || b.line - a.line);
}

export function reportedValue(low: number, high: number | null): string {
  const dollars = (n: number) => new Intl.NumberFormat('en-US', {
    style: 'currency', currency: 'USD', maximumFractionDigits: 0,
  }).format(n);
  if (low === 0 && high === 1000) return 'None or under $1,001';
  if (high === null) return `Over ${dollars(low - 1)}`;
  return `${dollars(low)}–${dollars(high)}`;
}

export function filterReportedAssets(rows: ReportedAsset[], query: string, includeSmall = false) {
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  return rows.filter(row => (includeSmall || row.low > 0)
    && terms.every(term => row.name.toLowerCase().includes(term)));
}

export function parseSourceIndex(html: string): { title: string; url: string }[] {
  const links = new Map<string, { title: string; url: string }>();
  const anchors = html.matchAll(/<a\b[^>]*\bhref=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi);
  for (const [, href, body] of anchors) {
    const title = body.replace(/<[^>]*>/g, '').replace(/&(?:nbsp|#160);/g, ' ').trim();
    if (!/^President Donald J\. Trump(?:\s|$)/.test(title)) continue;
    try {
      const url = new URL(href.replace(/&amp;/g, '&'), sourceData.indexUrl);
      if (url.origin !== 'https://www.whitehouse.gov' || !url.pathname.endsWith('.pdf')) continue;
      links.set(url.href, { title, url: url.href });
    } catch { /* Skip malformed source links. */ }
  }
  return [...links.values()];
}

export function historicalCoverage(throughYear: number) {
  const available: Record<number, string> = {
    2015: '2015 candidate disclosure', 2016: '2016 candidate disclosure',
    2017: '2017 disclosure', 2018: '2018 annual disclosure', 2019: '2019 annual disclosure',
    2020: '2020 annual disclosure', 2021: '2021 termination disclosure', 2023: '2023 candidate disclosure, part 1',
    2024: '2024 candidate disclosure, part 1',
  };
  return Array.from({ length: Math.max(0, throughYear - trumpModel.earliestSourceYear + 1) }, (_, i) => {
    const year = trumpModel.earliestSourceYear + i;
    const source = year === 2025 ? sourceData.sources.find(s => s.url.includes('/2025/06/'))
      : year === 2026 ? sourceData.sources.find(s => s.status === 'extracted')
      : sourceData.sources.find(s => s.title === available[year]);
    return { year, source: source ?? null, returnPercent: null,
      reason: year === 2015 ? `${firstDisclosure.rows.length} source rows checked; identities and prices pending`
        : source ? 'Holdings and price history need reconciliation' : 'Source coverage not verified' };
  });
}

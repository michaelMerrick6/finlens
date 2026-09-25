import sectorData from './trump-sectors.json';

export type StrategyHolding = {
  ticker: string;
  name: string;
  estimatedReportedValue: number | null;
  low: number;
  high: number | null;
  rows: { id: string; name: string; account: string; page: number; line: number; low: number; high: number | null }[];
};

export type StrategyTrade = {
  id: string; ticker: string; name: string; type: string; date: string;
  low: number; high: number; page: number; line: number;
};

const sectors: Record<string, { sector: string; sourceId: string }> = sectorData.stocks;
export const sectorSources = sectorData.sources;
export const sectorColors: Record<string, string> = {
  'Information Technology': '#345b4b',
  Financials: '#729d91',
  'Health Care': '#8d9d64',
  'Communication services': '#b5a26d',
  'Consumer Staples': '#829bad',
  'Consumer Discretionary': '#ab8476',
  Industrials: '#88859c',
  Energy: '#bcbd91',
  Unclassified: '#cbd0c6',
};

export function holdingSector(ticker: string): string {
  const sector = sectors[ticker]?.sector;
  return sector === 'Communication' ? 'Communication services' : sector || 'Unclassified';
}

export function sectorBreakdown(tickers: string[]) {
  const unique = [...new Set(tickers)];
  const counts = new Map<string, number>();
  for (const ticker of unique) {
    const name = holdingSector(ticker);
    counts.set(name, (counts.get(name) ?? 0) + 1);
  }
  return [...counts].map(([name, count]) => ({ name, count,
    percent: unique.length ? count / unique.length * 100 : 0,
    color: sectorColors[name] || sectorColors.Unclassified,
  })).sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
}

// Shares and dollars are deliberately never combined. These chart fractions
// describe only the displayed annual estimates, not the complete portfolio.
export function holdingsMix(holdings: Pick<StrategyHolding, 'ticker' | 'name' | 'estimatedReportedValue'>[]) {
  const priced = holdings.filter(row => row.ticker !== 'DJT' && row.estimatedReportedValue !== null
    && Number.isFinite(row.estimatedReportedValue) && row.estimatedReportedValue > 0)
    .sort((a, b) => b.estimatedReportedValue! - a.estimatedReportedValue! || a.ticker.localeCompare(b.ticker));
  const total = priced.reduce((sum, row) => sum + row.estimatedReportedValue!, 0);
  const colors = ['#284d3b', '#527864', '#8ca783', '#b1bf9d', '#a68c60', '#e1e6dc'];
  const top = priced.slice(0, 5).map((row, i) => ({ ticker: row.ticker, name: row.name,
    value: row.estimatedReportedValue!, percent: row.estimatedReportedValue! / total * 100, color: colors[i] }));
  const other = priced.slice(5).reduce((sum, row) => sum + row.estimatedReportedValue!, 0);
  if (other > 0) top.push({ ticker: 'Other', name: `${priced.length - 5} other stocks`, value: other,
    percent: other / total * 100, color: colors[5] });
  return { positions: priced.length, total, segments: top };
}

export function filterStrategyHoldings<T extends { ticker: string; name: string; rows?: { name: string }[] }>(holdings: T[], query: string, sector: string) {
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  return holdings.filter(row => (!sector || holdingSector(row.ticker) === sector)
    && terms.every(term => `${row.ticker} ${row.name} ${row.rows?.map(source => source.name).join(' ') || ''}`.toLowerCase().includes(term)));
}

export function filterStrategyTrades(trades: StrategyTrade[], query: string, direction: string) {
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  return trades.filter(row => (!direction || row.type === direction)
    && terms.every(term => `${row.ticker} ${row.name}`.toLowerCase().includes(term)))
    .sort((a, b) => b.date.localeCompare(a.date) || b.line - a.line);
}

export { strategyDate, strategyMoney, compactMoney, strategyRange } from './strategy-format';

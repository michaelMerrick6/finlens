import type { valuePelosiPortfolio } from '../pelosi-portfolio';

export type PelosiStrategyHolding = ReturnType<typeof valuePelosiPortfolio>['rows'][number] & {
  ticker: string; name: string; sector: string;
};
export type PelosiStrategyActivity = {
  id: string; ticker: string; name: string; date: string; action: string;
  direction: 'buy' | 'sell' | 'other'; quantity: number; unit: string; source: string;
};

export function pelosiStrategyOverview(rows: PelosiStrategyHolding[]) {
  const priced = rows.filter(row => row.midpoint !== null && Number.isFinite(row.midpoint) && row.midpoint > 0)
    .sort((a, b) => b.midpoint! - a.midpoint! || a.ticker.localeCompare(b.ticker));
  const total = priced.reduce((sum, row) => sum + row.midpoint!, 0);
  const colors = ['#284d3b', '#527864', '#8ca783', '#b1bf9d', '#a68c60', '#e1e6dc'];
  const segments = priced.slice(0, 5).map((row, i) => ({ ticker: row.ticker, name: row.name,
    value: row.midpoint!, percent: row.midpoint! / total * 100, color: colors[i] }));
  const other = priced.slice(5).reduce((sum, row) => sum + row.midpoint!, 0);
  if (other > 0) segments.push({ ticker: 'Other', name: `${priced.length - 5} other positions`,
    value: other, percent: other / total * 100, color: colors[5] });
  const sectorColors: Record<string, string> = {
    'Information Technology': '#345b4b', Financials: '#729d91', 'Health Care': '#8d9d64',
    'Communication services': '#b5a26d', 'Consumer Staples': '#829bad',
    'Consumer Discretionary': '#ab8476', Industrials: '#88859c', Energy: '#bcbd91', Utilities: '#71998c',
  };
  const unique = [...new Map(rows.map(row => [row.ticker, row])).values()];
  const counts = new Map<string, number>();
  for (const row of unique) counts.set(row.sector, (counts.get(row.sector) ?? 0) + 1);
  const sectors = [...counts].map(([name, count]) => ({ name, count,
    percent: count / unique.length * 100, color: sectorColors[name] || '#cbd0c6',
  })).sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  return { mix: { positions: priced.length, segments }, sectors };
}

export function pelosiStrategyActivity(events: {
  doc_id: string; ticker: string; date: string; kind: string; share_delta: number; source: string; page: number;
}[], names: Record<string, string>): PelosiStrategyActivity[] {
  // Exercises, gifts and corporate actions remain distinct from market purchases/sales.
  return events.filter(event => event.share_delta !== 0).map(event => {
    const purchase = ['purchase', 'stock_purchase', 'public_partnership_units_purchase'].includes(event.kind);
    const sale = ['sale', 'stock_sale'].includes(event.kind);
    return { id: event.doc_id, ticker: event.ticker === 'SQ' ? 'XYZ' : event.ticker,
      name: names[event.ticker] || event.ticker, date: event.date,
      action: purchase ? 'Buy' : sale ? 'Sell' : event.kind === 'exercise' ? 'Exercise' : event.kind === 'spinoff' ? 'Spinoff' : event.kind.replaceAll('_', ' '),
      direction: purchase ? 'buy' as const : sale ? 'sell' as const : 'other' as const,
      quantity: Math.abs(event.share_delta), unit: event.kind === 'public_partnership_units_purchase' ? 'units' : 'shares',
      source: `${event.source}#page=${event.page}` };
  }).sort((a, b) => b.date.localeCompare(a.date) || a.ticker.localeCompare(b.ticker) || a.id.localeCompare(b.id));
}

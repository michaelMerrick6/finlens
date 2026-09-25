import type { aggregateAnalysis, AnalysisTrade } from './congress-analysis';

export type AnalysisPeriod = '7' | '30' | 'ytd' | 'year';
export type AnalysisBasis = 'filed' | 'trade';
export type AnalysisStockWithTrades = ReturnType<typeof aggregateAnalysis>['stocks'][number] & { sector: string | null; companyName?: string | null };
export type AnalysisStock = Omit<AnalysisStockWithTrades, 'trades' | 'together'> & {
  tradeCount: number; sectors: string[]; assetNames: string[];
};
export type AnalysisData = {
  stocks: AnalysisStock[]; excluded: number; scanned: number; start: string; end: string;
  overview: ReturnType<typeof activityOverview>;
  sectorSources: { id: string; url: string; holdingsAsOf: string; checkedOn: string }[];
};
export type AnalysisTradePage = { trades: AnalysisTrade[]; total: number; nextOffset: number | null };

export function analysisWindow(period: AnalysisPeriod, now = new Date()) {
  const end = now.toISOString().slice(0, 10);
  const start = new Date(`${end}T00:00:00Z`);
  if (period === 'ytd') start.setUTCMonth(0, 1);
  else if (period === 'year') {
    const month = start.getUTCMonth();
    start.setUTCFullYear(start.getUTCFullYear() - 1);
    if (start.getUTCMonth() !== month) start.setUTCDate(0); // Feb 29 -> Feb 28.
    start.setUTCDate(start.getUTCDate() + 1);
  } else start.setUTCDate(start.getUTCDate() - (period === '7' ? 6 : 29));
  return { start: start.toISOString().slice(0, 10), end };
}

export const analysisColors: Record<string, string> = {
  'Information Technology': '#345b4b', Financials: '#729d91', 'Health Care': '#8d9d64',
  'Communication services': '#b5a26d', 'Consumer Staples': '#829bad', 'Consumer Discretionary': '#ab8476',
  Industrials: '#88859c', Energy: '#bcbd91', Utilities: '#6c9289', Materials: '#b4977d',
  'Real Estate': '#8d99ad', 'Funds & ETFs': '#b4c4ae', Unclassified: '#d3d8ce',
};

export function tradeSector(trade: AnalysisTrade, stock: { sector: string | null }) {
  return ['ETF', 'ET'].includes((trade.asset_type || '').toUpperCase()) ? 'Funds & ETFs' : stock.sector || 'Unclassified';
}

/** Use every eligible transaction, never the paginated stock list or dollar midpoints. */
export function activityOverview(stocks: Pick<AnalysisStockWithTrades, 'trades' | 'sector'>[]) {
  let buys = 0, sells = 0;
  const members = new Set<string>();
  const sectors = new Map<string, { name: string; buys: number; sells: number }>();
  for (const stock of stocks) for (const trade of stock.trades) {
    const name = tradeSector(trade, stock);
    const sector = sectors.get(name) || { name, buys: 0, sells: 0 };
    if (trade.transaction_type === 'buy') { buys++; sector.buys++; }
    else if (trade.transaction_type === 'sell') { sells++; sector.sells++; }
    else continue;
    if (trade.member_id) members.add(trade.member_id.toUpperCase());
    sectors.set(name, sector);
  }
  const total = buys + sells;
  return { buys, sells, total, stocks: stocks.length, members: members.size,
    sectors: [...sectors.values()].map(row => ({ ...row, count: row.buys + row.sells,
      percent: total ? (row.buys + row.sells) / total * 100 : 0, color: analysisColors[row.name] || analysisColors.Unclassified,
    })).sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
  };
}

export function summarizeAnalysisStock(stock: AnalysisStockWithTrades): AnalysisStock {
  const { trades, together, ...summary } = stock;
  void together;
  return { ...summary, tradeCount: trades.length,
    sectors: [...new Set(trades.map(trade => tradeSector(trade, stock)))],
    assetNames: [...new Set(trades.map(trade => trade.asset_name || '').filter(Boolean))],
  };
}

export function matchesAnalysisStock(stock: AnalysisStock, query: string, sector: string) {
  return (!sector || stock.sectors.includes(sector))
    && `${stock.ticker} ${stock.companyName || ''} ${stock.assetNames.join(' ')}`.toLowerCase().includes(query.trim().toLowerCase());
}

export const analysisNumber = (value: number) => value.toLocaleString('en-US');
export const analysisMoney = (value: number) => new Intl.NumberFormat('en-US', {
  style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0,
}).format(value);

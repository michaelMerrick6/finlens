import 'server-only';
import { unstable_cache } from 'next/cache';
import { getPublicSupabase } from './supabase-server';
import { readCompleteRows } from './complete-rows';
import { aggregateAnalysis, type AnalysisTrade } from './congress-analysis';
import { activityOverview, summarizeAnalysisStock, type AnalysisData, type AnalysisTradePage } from './analysis-overview';
import { analysisCompanyName, analysisSector, analysisSectorSources } from './analysis-sectors';

export async function loadAnalysisRows(start: string, end: string, basis: string, ticker?: string) {
  const rows = await readCompleteRows<AnalysisTrade>(async (offset, count) => {
    let query = getPublicSupabase().from('politician_trades')
      .select('id,doc_id,member_id,politician_name,ticker,asset_type,amount_range,asset_name,transaction_type,transaction_date,published_date,source_url')
      .gte(basis, start).lte(basis, end).lte('published_date', end).in('transaction_type', ['buy', 'sell']);
    // Include reviewed filings whose stored ticker may change during aggregation.
    // Tickers are validated before reaching this PostgREST filter.
    if (ticker) query = query.or(`ticker.ilike.*${ticker}*,member_id.eq.P000197`);
    const { data, error } = await query.order(basis).order('id').range(offset, offset + count - 1);
    if (error) throw error;
    return (data || []) as AnalysisTrade[];
  });
  // Preserve the original ID precedence when reconciling duplicate source rows.
  return rows.sort((a, b) => a.id.localeCompare(b.id));
}

/** Cache only the overview; full-year evidence exceeds Next's 2 MB cache limit. */
export const loadAnalysisSummary = unstable_cache(async (start: string, end: string, basis: string, instrument: string): Promise<AnalysisData> => {
  const rows = await loadAnalysisRows(start, end, basis);
  const result = aggregateAnalysis(rows, instrument, false);
  const stocks = result.stocks.map(stock => ({ ...stock, sector: analysisSector(stock.ticker), companyName: analysisCompanyName(stock.ticker) }));
  return { stocks: stocks.map(summarizeAnalysisStock), overview: activityOverview(stocks), excluded: result.excluded,
    sectorSources: analysisSectorSources, scanned: rows.length, start, end };
}, ['congress-analysis-summary-v1'], { revalidate: 300 });

export const loadAnalysisTrades = unstable_cache(async (start: string, end: string, basis: string, instrument: string, ticker: string, offset: number): Promise<AnalysisTradePage> => {
  const rows = await loadAnalysisRows(start, end, basis, ticker);
  const trades = aggregateAnalysis(rows, instrument, false).stocks.find(stock => stock.ticker === ticker)?.trades || [];
  trades.sort((a, b) => (b.published_date || '').localeCompare(a.published_date || '')
    || (b.transaction_date || '').localeCompare(a.transaction_date || '') || a.id.localeCompare(b.id));
  const next = offset + 50;
  return { trades: trades.slice(offset, next), total: trades.length, nextOffset: next < trades.length ? next : null };
}, ['congress-analysis-trades-v1'], { revalidate: 300 });

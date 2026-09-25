import 'server-only';
import catalog from './analysis-sectors.json';

const stocks: Record<string, { sector: string; name?: string; sourceId: string }> = catalog.stocks;
export const analysisSectorSources = catalog.sources;
export const analysisSector = (ticker: string): string | null => stocks[ticker]?.sector ?? null;
export const analysisCompanyName = (ticker: string): string | null => stocks[ticker]?.name || null;

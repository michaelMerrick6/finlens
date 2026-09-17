import type { ReviewedPosition } from './pelosi-holdings';
import type { ShareReconciliation } from './pelosi-share-reconciliation';
export type HoldingQuote = { price: number; date: string };
export function holdingMarketTicker(ticker: string) { return ticker === 'SQ' ? 'XYZ' : ticker; }
export function valuePelosiPortfolio(inputs: { position: ReviewedPosition; quantity: ShareReconciliation; quote: HoldingQuote | null }[], asOf: string) {
  const rows = inputs.map(({ position, quantity, quote }) => {
    const age = quote ? (Date.parse(asOf) - Date.parse(quote.date)) / 86400000 : NaN;
    const usable = quote && Number.isFinite(quote.price) && quote.price > 0 && age >= 0 && age <= 5;
    const low = quantity.range?.min ?? quantity.shares;
    const high = quantity.range?.max ?? quantity.shares;
    const min = usable && low !== null && low >= 0 ? low * quote.price : null;
    const max = usable && high !== null && high >= (low ?? 0) ? high * quote.price : null;
    return { position, quantity, quote: usable ? quote : null, min, max,
      midpoint: min !== null && max !== null ? (min + max) / 2 : null, rank: null as number | null, weight: null as number | null };
  }).sort((a, b) => (b.midpoint ?? -1) - (a.midpoint ?? -1) || a.position.ticker.localeCompare(b.position.ticker));
  const total = rows.reduce((sum, r) => sum + (r.midpoint ?? 0), 0);
  rows.forEach((row, i) => { if (row.midpoint !== null) { row.rank = i + 1; row.weight = total > 0 ? row.midpoint / total : 0; } });
  return { rows, total, priced: rows.filter(r => r.midpoint !== null).length };
}
export function holdingDollars(value: number) {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1 }).format(value);
}

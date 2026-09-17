// Chamber-independent quantities. Prices and disclosure dollar ranges are not share counts.
export type HoldingBaseline = { key: string; ticker: string; owner: string; assetType: string; date: string; minShares: number; maxShares: number; source: string };
export type HoldingEvent = { id: string; key: string; date: string; source: string; type: 'shares' | 'split' | 'review'; delta?: number; factor?: number };
export function rollForwardCongressHolding(baseline: HoldingBaseline, events: HoldingEvent[], asOf: string) {
  if (!baseline.source || baseline.date > asOf || !Number.isFinite(baseline.minShares) || !Number.isFinite(baseline.maxShares) || baseline.minShares < 0 || baseline.maxShares < baseline.minShares) throw new Error('Invalid holdings baseline');
  let min = baseline.minShares, max = baseline.maxShares;
  const seen = new Set<string>();
  const applicable = events.filter(e => e.key === baseline.key && e.date > baseline.date && e.date <= asOf).sort((a,b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));
  for (const event of applicable) {
    if (!event.id || seen.has(event.id)) throw new Error('Duplicate holdings event');
    seen.add(event.id);
    if (!event.source || event.type === 'review') return {status:'review-needed' as const, minShares:null, maxShares:null};
    if (event.type === 'split') {
      if (!event.factor || !Number.isFinite(event.factor) || event.factor <= 0 || applicable.some(e => e !== event && e.date === event.date)) return {status:'review-needed' as const, minShares:null, maxShares:null};
      min *= event.factor; max *= event.factor;
    } else {
      if (!Number.isFinite(event.delta)) throw new Error('Invalid share quantity');
      min += event.delta!; max += event.delta!;
      if (min < 0) return {status:'review-needed' as const, minShares:null, maxShares:null};
    }
  }
  return {status:max === 0 ? 'closed' as const : 'estimated' as const, minShares:min, maxShares:max};
}

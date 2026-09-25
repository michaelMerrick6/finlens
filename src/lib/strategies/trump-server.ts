import 'server-only';
import { parseSourceIndex, trumpSources } from './trump';
import type { PricePoint } from './price-returns';

// Performance must not silently substitute unadjusted closes when the vendor
// omits adjusted history. A missing series is an unavailable result.
export async function getDjtAdjustedPrices(): Promise<PricePoint[]> {
  try {
    // range=max can silently return weekly points. Bound the interval and
    // require daily metadata. Use one URL per UTC day so caching is reusable.
    const end = Math.floor(Date.now() / 86_400_000) * 86400 + 86400;
    const response = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/DJT?period1=1711411200&period2=${end}&interval=1d&includeAdjustedClose=true`, {
      headers: { 'User-Agent': 'Mozilla/5.0' }, next: { revalidate: 900 },
      signal: AbortSignal.timeout(8000),
    });
    if (!response.ok) return [];
    const result = (await response.json()).chart?.result?.[0];
    if (result?.meta?.symbol !== 'DJT' || result?.meta?.currency !== 'USD'
      || result?.meta?.dataGranularity !== '1d') return [];
    const times: number[] = result.timestamp ?? [];
    const adjusted: (number | null)[] = result.indicators?.adjclose?.[0]?.adjclose ?? [];
    return times.flatMap((time, index) => {
      const price = adjusted[index];
      return Number.isFinite(time) && typeof price === 'number' && Number.isFinite(price) && price > 0
        ? [{ date: new Date(time * 1000).toISOString().slice(0, 10), price }] : [];
    });
  } catch { return []; }
}

export type SourceCheck = {
  checkedAt: string | null;
  available: boolean;
  newReports: { title: string; url: string }[];
};
let cached: { expires: number; result: SourceCheck } | undefined;
let pending: Promise<SourceCheck> | undefined;

export async function checkTrumpSources(): Promise<SourceCheck> {
  if (cached && cached.expires > Date.now()) return cached.result;
  if (pending) return pending;
  pending = (async () => {
    try {
      const response = await fetch(trumpSources.indexUrl, {
        cache: 'no-store', signal: AbortSignal.timeout(6000),
      });
      if (!response.ok) throw new Error('Source index unavailable');
      const links = parseSourceIndex(await response.text());
      if (!links.some(link => /Annual Report/.test(link.title))) throw new Error('Source index format changed');
      const known = new Set(trumpSources.sources.map(source => source.url));
      const result = { available: true, checkedAt: new Date().toISOString(),
        newReports: links.filter(link => !known.has(link.url)) };
      cached = { expires: Date.now() + 60 * 60 * 1000, result };
      return result;
    } catch {
      // An unsuccessful check must not advance the last-success date or report
      // that the portfolio is up to date. Retry failures sooner than successes.
      const result = { available: false, checkedAt: cached?.result.checkedAt ?? null,
        newReports: cached?.result.newReports ?? [] };
      cached = { expires: Date.now() + 60 * 1000, result };
      return result;
    } finally { pending = undefined; }
  })();
  return pending;
}

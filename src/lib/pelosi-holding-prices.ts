import 'server-only';
import { holdingMarketTicker, type HoldingQuote } from './pelosi-portfolio';
// Use actual daily closes, not dividend-adjusted historical performance prices.
export async function getHoldingPrices(tickers: string[]): Promise<Map<string, HoldingQuote | null>> {
  const result = new Map<string, HoldingQuote | null>();
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(6, tickers.length) }, async () => {
    while (cursor < tickers.length) {
      const ticker = tickers[cursor++];
      try {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(holdingMarketTicker(ticker))}?range=5d&interval=1d`;
        const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, next: { revalidate: 900 }, signal: AbortSignal.timeout(6000) });
        if (!response.ok) throw new Error('Quote unavailable');
        const data = (await response.json()).chart?.result?.[0];
        const times: number[] = data?.timestamp ?? [];
        const closes: (number | null)[] = data?.indicators?.quote?.[0]?.close ?? [];
        let quote: HoldingQuote | null = null;
        for (let i = times.length - 1; i >= 0; i--) {
          const price = closes[i];
          if (typeof price === 'number' && Number.isFinite(price) && price > 0 && Number.isFinite(times[i])) {
            quote = { price, date: new Date(times[i] * 1000).toISOString().slice(0, 10) }; break;
          }
        }
        result.set(ticker, quote);
      } catch { result.set(ticker, null); }
    }
  }));
  return result;
}

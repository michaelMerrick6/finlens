import 'server-only';
import { createHash } from 'node:crypto';
import { unstable_cache } from 'next/cache';
import { getReviewedBaseline, checkReviewedIndex, modelUnchangedHolding, type DailyPrice } from './reviewed-holdings';

// This project does not enable Cache Components. Cache a dated snapshot rather
// than labelling old results with the current request time. No database writes.
const dailySnapshot = unstable_cache(async (day: string, memberId: string, sourceHash: string) => {
  const baseline = getReviewedBaseline(memberId);
  if (!baseline || baseline.sha256 !== sourceHash) throw new Error('Unreviewed baseline');
  const startYear = Number(baseline.date.slice(0, 4));
  const indexes = await Promise.all(Array.from({ length: Number(day.slice(0,4)) - startYear + 1 }, async (_, i) => {
    const response = await fetch(`https://disclosures-clerk.house.gov/public_disc/financial-pdfs/${startYear+i}FD.txt`, { cache: 'no-store', signal: AbortSignal.timeout(15000) });
    if (!response.ok) throw new Error('House index unavailable');
    return checkReviewedIndex(await response.text(), baseline);
  }));
  if (!indexes.some(index => index.foundBaseline)) throw new Error('Annual baseline missing from source index');
  const pending = indexes.flatMap(index => index.pending);
  const source = await fetch(baseline.source, { cache: 'no-store', signal: AbortSignal.timeout(15000) });
  if (!source.ok || createHash('sha256').update(Buffer.from(await source.arrayBuffer())).digest('hex') !== baseline.sha256) throw new Error('Annual source needs revalidation');
  const checkedAt = new Date().toISOString();
  if (pending.length) return { status: 'review-needed' as const, checkedAt, pending, rows: baseline.positions.map(p => ({ ...p, estimate: null })) };
  const rows = await Promise.all(baseline.positions.map(async position => {
    try {
      const start = (Date.parse(`${baseline.date}T00:00:00Z`) - 7 * 86400000) / 1000;
      const end = Date.parse(`${day}T00:00:00Z`) / 1000;
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${position.ticker}?period1=${start}&period2=${end}&interval=1d`;
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(10000), headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!response.ok) throw new Error('Price unavailable');
      const chart = (await response.json()).chart?.result?.[0];
      if (chart?.meta?.currency !== 'USD' || chart?.meta?.symbol !== position.ticker) throw new Error('Unexpected price series');
      const closes = chart.indicators?.quote?.[0]?.close ?? [];
      const prices: DailyPrice[] = (chart.timestamp ?? []).map((time: number, i: number) => ({ date: new Date(time*1000).toISOString().slice(0,10), close: closes[i] }));
      return { ...position, estimate: modelUnchangedHolding(position, prices, day, baseline.date) };
    } catch { return { ...position, estimate: null }; }
  }));
  return { status: 'checked' as const, checkedAt, pending, rows };
}, ['member-reviewed-holdings-v2'], { revalidate: 86400 });

export async function getReviewedHoldings(memberId: string) {
  const baseline = getReviewedBaseline(memberId);
  if (!baseline) throw new Error('Unreviewed member');
  try { return await dailySnapshot(new Date().toISOString().slice(0,10), memberId, baseline.sha256); }
  catch { return { status: 'unavailable' as const, checkedAt: null, pending: [] as string[], rows: baseline.positions.map(p => ({ ...p, estimate: null })) }; }
}

export async function getCrockettHoldings() { return getReviewedHoldings('C001130'); }

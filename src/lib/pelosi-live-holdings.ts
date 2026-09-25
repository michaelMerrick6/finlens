import 'server-only';
import { getPublicSupabase } from './supabase-server';
import changes from '../../artifacts/pelosi-audit/2026-position-changes.json';
import { buildPelosiHoldings, type ReviewedPosition } from './pelosi-holdings';
import { reconcilePelosiShares, type ShareReconciliation } from './pelosi-share-reconciliation';

type LiveEvent = typeof changes.events[number];
export function applyPelosiOverlay(events: LiveEvent[], asOf: string) {
  const baseline = buildPelosiHoldings(asOf);
  const positions = buildPelosiHoldings(asOf, [...changes.events, ...events.map(e => ({...e, ticker: e.ticker === 'XYZ' ? 'SQ' : e.ticker}))]);
  const quantities = new Map<string, ShareReconciliation>();
  for (const p of positions.filter(p => p.kind === 'stock' || p.kind === 'units')) {
    const old = baseline.find(b => b.key === p.key);
    const q = old ? reconcilePelosiShares(old) : { status: 'estimated' as const, shares: 0, steps: [], assumptions: ['Assumes no unreported opening position before the newly disclosed purchase.'], reason: 'New stock position from explicit disclosed share quantities.' };
    const delta = p.shareChange - (old?.shareChange ?? 0);
    if ((q.range && q.range.min + delta < 0) || (q.shares !== null && q.shares + delta < 0)) throw new Error(`Sale exceeds supported balance for ${p.ticker}; review required`);
    quantities.set(p.key, { ...q, shares: q.shares === null ? null : q.shares + delta,
      ...(q.range ? { range: { min: q.range.min + delta, max: q.range.max + delta } } : {}),
      reason: q.reason + (delta ? ` New automatically parsed disclosures change the balance by ${delta > 0 ? '+' : ''}${delta.toLocaleString('en-US')} shares.` : '') });
  }
  return { positions, quantities, events: [...changes.events, ...events].filter(event => event.date > '2025-12-31' && event.date <= asOf) };
}
export async function getLivePelosiHoldings(): Promise<{ positions: ReviewedPosition[]; quantities: Map<string, ShareReconciliation>; events: LiveEvent[]; note: string; checkedAt: string | null }> {
  const asOf = new Date().toISOString().slice(0, 10);
  const fallback = applyPelosiOverlay([], asOf);
  try {
    const { data, error } = await getPublicSupabase().from('politician_holdings_sync').select('snapshot').eq('member_id', 'P000197').maybeSingle();
    if (error || !data) throw new Error('Updater not yet available');
    const s = (data as { snapshot: {version: number; checked_at: string; issues: string[]; events: LiveEvent[]} }).snapshot;
    const age = Date.now() - Date.parse(s.checked_at);
    if (s.version !== 1 || !Number.isFinite(age) || age < 0 || age > 36 * 3600000) throw new Error('Automatic check overdue');
    if (!Array.isArray(s.issues) || s.issues.length) throw new Error('New disclosures need review');
    if (!Array.isArray(s.events) || s.events.length > 1000) throw new Error('Invalid updater data');
    for (const e of s.events) {
      const id = /^house-(\d{4})-(\d+)-(\d+)$/.exec(e.doc_id);
      if (!id || e.source !== `https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${id[1]}/${id[2]}.pdf` ||
          e.owner !== 'SP' || !/^[A-Z][A-Z0-9.\-]{0,9}$/.test(e.ticker) || !/^\d{4}-\d{2}-\d{2}$/.test(e.date) ||
          e.date <= changes.latest_transaction_reviewed || e.date > asOf || !['purchase','sale'].includes(e.kind) ||
          !Number.isSafeInteger(e.share_delta) || !Number.isSafeInteger(e.contract_delta) ||
          (e.kind === 'purchase' ? e.share_delta < 0 || e.contract_delta < 0 : e.share_delta > 0 || e.contract_delta > 0)) throw new Error('Unsupported updater event');
    }
    return { ...applyPelosiOverlay(s.events, asOf), checkedAt: s.checked_at, note: s.events.length ? 'Updated automatically from explicit quantities in new official disclosures. Select a holding for source records.' : 'Automatic filing check is current. No new supported transactions.' };
  } catch (error) {
    return { ...fallback, checkedAt: null, note: `${error instanceof Error ? error.message : 'Automatic update unavailable'}. Showing the reviewed baseline; newer changes may be missing.` };
  }
}

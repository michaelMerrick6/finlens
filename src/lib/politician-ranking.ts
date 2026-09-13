import { reviewPoliticianTrade, isReviewedPublicPartnership } from "./reviewed-politician-trades";
import 'server-only';
import { unstable_cache } from 'next/cache';
import { getPublicSupabase } from './supabase-server';
import { filterDisplayPoliticianTrades } from './politician-trade-scope';
import { parsePoliticianAmountRange } from './politician-amount-range';
import { readBoundedRows } from './bounded-rows';

type Member = { id: string; first_name: string; last_name: string; chamber: string; party: string; state: string };
type Trade = { doc_id: string; source_url: string; member_id: string | null; ticker: string | null; chamber: string | null; published_date: string | null; amount_range: string | null; asset_type: string | null };

export const loadPoliticianRanking = unstable_cache(async (end: string) => {
  const startDate = new Date(`${end}T00:00:00Z`);
  startDate.setUTCFullYear(startDate.getUTCFullYear() - 1);
  const start = startDate.toISOString().slice(0, 10);
  const db = getPublicSupabase();
  const members = await readBoundedRows<Member>(2000, async (offset, count) => {
    const result = await db.from('congress_members').select('id,first_name,last_name,chamber,party,state')
      .eq('active', true).not('id', 'like', 'unknown-%').order('id').range(offset, offset + count - 1);
    if (result.error) throw result.error;
    return (result.data || []) as Member[];
  });
  if (members.hasMore) throw new Error('Directory exceeds ranking limit');
  const totals = new Map<string, { volume: number; tradeCount: number; unpricedCount: number }>();
  let complete = false;
  for (let offset = 0; offset < 50000; offset += 500) {
    const result = await db.from('politician_trades')
      .select('id,doc_id,source_url,member_id,ticker,chamber,published_date,amount_range,transaction_type,asset_type,asset_name')
      .gte('transaction_date', start).lte('transaction_date', end).lte('published_date', end)
      .in('transaction_type', ['buy', 'sell']).order('id').range(offset, offset + 499);
    if (result.error) throw result.error;
    for (const raw of filterDisplayPoliticianTrades((result.data || []) as Trade[])) {
      const trade = reviewPoliticianTrade(raw);
      if (trade.ticker === "US-TREAS" || !trade.member_id || trade.exclude_from_totals || trade.is_contribution || (!isReviewedPublicPartnership(trade) && !["ST", "STOCK", "ETF", "ET", "OP"].includes((trade.asset_type || "").toUpperCase()))) continue;
      const total = totals.get(trade.member_id) || { volume: 0, tradeCount: 0, unpricedCount: 0 };
      const amount = parsePoliticianAmountRange(trade.amount_range);
      total.volume += amount?.min || 0;
      total.tradeCount++;
      if (!amount) total.unpricedCount++;
      totals.set(trade.member_id, total);
    }
    if ((result.data?.length || 0) < 500) { complete = true; break; }
  }
  if (!complete) throw new Error('Ranking history exceeds safe scan limit');
  return { start, end, members: members.rows.map(m => ({ ...m, ...(totals.get(m.id) || { volume: 0, tradeCount: 0, unpricedCount: 0 }) })) };
}, ['politician-ranking-v4'], { revalidate: 300 });

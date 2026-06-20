import 'server-only';

import { enrichPoliticianTradesWithAssetNames } from '@/lib/politician-asset-names';
import { normalizeProfileDate, type PoliticianProfileTrade } from '@/lib/politician-profile';
import { filterDisplayPoliticianTrades } from '@/lib/politician-trade-scope';
import type { DashboardPoliticianWorkspaceData } from '@/lib/politician-workspace-types';
import { getAdminSupabase } from '@/lib/supabase-admin';

type CongressMemberRow = {
  id: string;
  first_name: string | null;
  last_name: string | null;
  party: string | null;
  chamber: string | null;
  state: string | null;
};

const WORKSPACE_TRADE_SELECT = `
  id,
  doc_id,
  member_id,
  politician_name,
  ticker,
  asset_name,
  asset_type,
  transaction_type,
  amount_range,
  published_date,
  transaction_date,
  source_url,
  chamber,
  party,
  congress_members (
    first_name,
    last_name,
    party,
    chamber,
    state
  )
`;

function clampPageValue(value: number, fallback: number, min: number, max: number) {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function memberDisplayName(member: CongressMemberRow | null, trade: PoliticianProfileTrade | null) {
  const memberName = [member?.first_name, member?.last_name].filter(Boolean).join(' ').trim();
  if (memberName) {
    return memberName;
  }

  const joinedName = [trade?.congress_members?.first_name, trade?.congress_members?.last_name].filter(Boolean).join(' ').trim();
  return joinedName || String(trade?.politician_name || 'Unknown Politician').trim();
}

export async function getPoliticianWorkspaceData(
  memberId: string,
  {
    offset = 0,
    limit = 8,
  }: {
    offset?: number;
    limit?: number;
  } = {},
): Promise<DashboardPoliticianWorkspaceData | null> {
  const normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) {
    return null;
  }

  const safeOffset = clampPageValue(offset, 0, 0, 10_000);
  const safeLimit = clampPageValue(limit, 8, 4, 20);
  const supabase = getAdminSupabase();

  const memberPromise = supabase
    .from('congress_members')
    .select('id,first_name,last_name,party,chamber,state')
    .eq('id', normalizedMemberId)
    .maybeSingle();

  const tradesPromise = supabase
    .from('politician_trades')
    .select(WORKSPACE_TRADE_SELECT)
    .eq('member_id', normalizedMemberId)
    .not('ticker', 'is', null)
    .not('ticker', 'in', '("N/A","NA","UNKNOWN","MULTI")')
    .order('transaction_date', { ascending: false })
    .order('published_date', { ascending: false })
    .order('id', { ascending: false })
    .range(safeOffset, safeOffset + safeLimit);

  const [memberResult, tradesResult] = await Promise.all([memberPromise, tradesPromise]);

  if (memberResult.error) {
    throw new Error(memberResult.error.message);
  }
  if (tradesResult.error) {
    throw new Error(tradesResult.error.message);
  }

  const member = (memberResult.data as CongressMemberRow | null) || null;
  const displayTrades = filterDisplayPoliticianTrades((tradesResult.data || []) as PoliticianProfileTrade[]);
  const pageTrades = displayTrades.slice(0, safeLimit);
  const enrichedTrades = await enrichPoliticianTradesWithAssetNames(pageTrades);
  const firstTrade = enrichedTrades[0] || displayTrades[0] || null;

  if (!member && !firstTrade) {
    return null;
  }

  return {
    memberId: normalizedMemberId,
    summary: {
      displayName: memberDisplayName(member, firstTrade),
      party: member?.party || firstTrade?.congress_members?.party || null,
      chamber: member?.chamber || firstTrade?.congress_members?.chamber || firstTrade?.chamber || null,
      state: member?.state || firstTrade?.congress_members?.state || null,
      latestTradeDate:
        normalizeProfileDate(firstTrade?.transaction_date) ||
        normalizeProfileDate(firstTrade?.published_date) ||
        null,
    },
    trades: enrichedTrades,
    nextOffset: displayTrades.length > safeLimit ? safeOffset + safeLimit : null,
  };
}

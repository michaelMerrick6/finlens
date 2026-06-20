import 'server-only';

import { buildPoliticianLivePortfolio } from '@/lib/politician-live-portfolio';
import { getLatestPoliticianDisclosureHoldings } from '@/lib/politician-disclosure-holdings';
import { enrichPoliticianTradesWithAssetNames } from '@/lib/politician-asset-names';
import {
  buildPoliticianProfileSummary,
  type PoliticianLivePortfolioSummary,
  type PoliticianProfileSummary,
  type PoliticianProfileTrade,
} from '@/lib/politician-profile';
import { filterProductPoliticianTrades } from '@/lib/politician-trade-scope';
import { getAdminSupabase } from '@/lib/supabase-admin';

export type PoliticianProfileData = {
  memberId: string;
  summary: PoliticianProfileSummary;
  livePortfolio: PoliticianLivePortfolioSummary;
  trades: PoliticianProfileTrade[];
};

const EMPTY_LIVE_PORTFOLIO: PoliticianLivePortfolioSummary = {
  holdingCount: 0,
  totalEstimatedCurrentValue: 0,
  totalEstimatedCostBasis: 0,
  totalEstimatedUnrealizedGain: 0,
  eligibleTradeCount: 0,
  pricedTradeCount: 0,
  skippedTradeCount: 0,
  priceAsOf: null,
  disclosureSnapshotDate: null,
  disclosureHoldingCount: 0,
  holdings: [],
};

export async function getPoliticianProfileData(
  memberId: string,
  { limit = 1200, includeLivePortfolio = true }: { limit?: number; includeLivePortfolio?: boolean } = {},
): Promise<PoliticianProfileData | null> {
  const supabase = getAdminSupabase();
  const { data, error } = await supabase
    .from('politician_trades')
    .select(`
      *,
      congress_members (
        first_name,
        last_name,
        party,
        chamber,
        state
      )
    `)
    .eq('member_id', memberId)
    .order('transaction_date', { ascending: false })
    .order('published_date', { ascending: false })
    .limit(limit);

  if (error) {
    throw new Error(error.message);
  }

  const scopedTrades = filterProductPoliticianTrades((data || []) as PoliticianProfileTrade[]);
  if (!scopedTrades.length) {
    return null;
  }

  const trades = await enrichPoliticianTradesWithAssetNames(scopedTrades);
  const summary = buildPoliticianProfileSummary(trades);
  const livePortfolio = includeLivePortfolio
    ? await getLatestPoliticianDisclosureHoldings(memberId).then((disclosureHoldings) =>
        buildPoliticianLivePortfolio(trades, disclosureHoldings),
      )
    : EMPTY_LIVE_PORTFOLIO;

  return {
    memberId,
    summary,
    livePortfolio,
    trades,
  };
}

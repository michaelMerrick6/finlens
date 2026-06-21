import 'server-only';

import { stat } from 'node:fs/promises';
import path from 'node:path';

import { unstable_cache } from 'next/cache';

import type { FundDirectoryEntry } from '@/lib/hedge-funds';
import { loadFundDirectory, loadInstitutionalHoldingRows } from '@/lib/hedge-funds-server';
import { getMarketPriceSeries, getMarketPriceSeriesMap, getPriceOnOrBefore } from '@/lib/market-data';
import { enrichPoliticianTradesWithAssetNames } from '@/lib/politician-asset-names';
import {
  buildPoliticianProfileSummary,
  normalizeProfileDate,
  normalizeProfileDirection,
  normalizeProfileTicker,
  parseEstimatedTradeAmount,
  type PoliticianProfileTrade,
  type PoliticianProfileSummary,
} from '@/lib/politician-profile';
import type { PoliticianProfileData } from '@/lib/politician-profile-server';
import { getPoliticianProfileData } from '@/lib/politician-profile-server';
import {
  filterDisplayPoliticianTrades,
  filterProductPoliticianTrades,
  HOUSE_PRODUCT_START_DATE,
} from '@/lib/politician-trade-scope';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { getPublicSupabase } from '@/lib/supabase-server';
import type { BroadcastStory } from '@/lib/tweet-candidates';
import {
  fetchTweetCandidateStories,
  PUBLIC_BROADCAST_STORY_STATUSES,
} from '@/lib/tweet-candidates';

const PUBLIC_FEED_REVALIDATE_SECONDS = 60;
const HEDGE_FUNDS_REVALIDATE_SECONDS = 60 * 60;
const POLITICIAN_PROFILE_REVALIDATE_SECONDS = 5 * 60;
const INITIAL_POLITICIAN_FEED_LIMIT = 20;
const POLITICIAN_FEED_SORT_BUFFER = 80;
const INITIAL_INSIDER_FEED_LIMIT = 200;
const DASHBOARD_SIGNAL_LIMIT = 6;
const DASHBOARD_SIGNAL_CANDIDATE_LIMIT = 24;
const DASHBOARD_SIGNAL_LOOKBACK_DAYS = 30;
const DASHBOARD_CLUSTER_PREVIEW_LIMIT = 4;
const DASHBOARD_RETURN_TICKER_LIMIT = 8;
const CLUSTER_PAGE_SIGNAL_LIMIT = 750;
const DASHBOARD_MIN_SIGNAL_SCORE = 0.72;
const MATERIAL_INSIDER_CLUSTER_MIN_SCORE = 0.68;
const MATERIAL_INSIDER_CLUSTER_MIN_VALUE = 1_000_000;
const CLUSTER_FEED_RULES = new Set([
  'congress_cluster',
  'cross_source_accumulation',
  'grouped_congress_buy',
  'grouped_insider_buy',
  'insider_cluster',
]);
const TRACKED_13F_FUNDS_CONFIG_PATH = path.join(process.cwd(), 'config', 'tracked_13f_funds.json');

async function getFundDirectoryCacheVersion() {
  let holdingsVersion = 'missing-institutional-holdings-version';
  try {
    const { data, error } = await getPublicSupabase()
      .from('institutional_holdings')
      .select('created_at')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    const row = data as { created_at?: string | null } | null;
    if (!error && row?.created_at) {
      holdingsVersion = String(row.created_at);
    }
  } catch {
    // Keep the directory usable if the lightweight version check fails.
  }

  try {
    const stats = await stat(TRACKED_13F_FUNDS_CONFIG_PATH);
    return `${Math.round(stats.mtimeMs)}:${stats.size}:${holdingsVersion}`;
  } catch {
    return `missing-tracked-13f-config:${holdingsVersion}`;
  }
}

const POLITICIAN_FEED_SELECT = `
  id,
  member_id,
  politician_name,
  ticker,
  asset_name,
  asset_type,
  transaction_type,
  amount_range,
  published_date,
  created_at,
  transaction_date,
  source_url,
  chamber,
  congress_members (
    first_name,
    last_name,
    party,
    chamber,
    state
  )
`;

const INSIDER_FEED_SELECT = `
  id,
  ticker,
  filer_name,
  filer_relation,
  transaction_date,
  published_date,
  transaction_code,
  amount,
  price,
  value,
  source_url
`;

export type DashboardFeaturedSignal = {
  id: string;
  ticker: string;
  title: string;
  summary: string;
  ruleLabel: string;
  actorPreview: string | null;
  actorPreviews: {
    memberId: string | null;
    name: string;
    party: string | null;
    chamber: string | null;
  }[];
  actorCount: number;
  amountLabel: string | null;
  sourceLabel: string;
  publishedAt: string | null;
  direction: 'buy' | 'sell' | null;
};

export type PublicClusterSignal = DashboardFeaturedSignal & {
  ruleKey: string;
  sourceGroup: 'congress' | 'insiders' | 'cross-source';
  score: number;
};

export type DashboardFeaturedPolitician = {
  memberId: string;
  name: string;
  party: string | null;
  chamber: string | null;
  oneYearReturnPct: number | null;
  returnLabel: string;
  totalBuyAmount: number;
  totalSellAmount: number;
  timeline: Array<{
    key: string;
    label: string;
    buyAmount: number;
    sellAmount: number;
    netAmount: number;
  }>;
};

export type DashboardData = {
  politicianCount: number;
  insiderCount: number;
  fundCount: number;
  featuredSignals: DashboardFeaturedSignal[];
  featuredPoliticians: DashboardFeaturedPolitician[];
};

async function countPublicRows(
  supabase: ReturnType<typeof getPublicSupabase>,
  table: 'politician_trades' | 'insider_trades' | 'institutional_holdings',
) {
  const exact = await supabase.from(table).select('id', { count: 'exact', head: true });
  if (!exact.error && typeof exact.count === 'number') {
    return exact.count;
  }

  const planned = await supabase.from(table).select('id', { count: 'planned', head: true });
  if (!planned.error && typeof planned.count === 'number') {
    return planned.count;
  }

  throw new Error(
    exact.error?.message ||
      planned.error?.message ||
      `Failed to count rows for ${table}.`,
  );
}

// Retained for richer dashboard variants outside the minimal home experience.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
async function safeCountPublicRows(
  supabase: ReturnType<typeof getPublicSupabase>,
  table: 'politician_trades' | 'insider_trades' | 'institutional_holdings',
) {
  try {
    return await countPublicRows(supabase, table);
  } catch (error) {
    console.warn(
      `[dashboard] Falling back to 0 for ${table} row count.`,
      error instanceof Error ? error.message : error,
    );
    return 0;
  }
}

function dashboardSinceDate() {
  const now = new Date();
  now.setUTCDate(now.getUTCDate() - (DASHBOARD_SIGNAL_LOOKBACK_DAYS - 1));
  return now.toISOString().slice(0, 10);
}

function trim(value: unknown) {
  return String(value || '').trim();
}

function toNumber(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function parseAmountLowerBound(value: unknown) {
  const raw = String(value ?? '').trim();
  if (!raw) {
    return 0;
  }

  const normalized = raw.replace(/,/g, '');
  const match = normalized.match(/\$([\d.]+)/);
  if (!match) {
    return 0;
  }

  return Number(match[1]) || 0;
}

function moneyFloorLabel(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  return `$${Math.round(value).toLocaleString()}+`;
}

function sourceMixLabel(story: {
  sourceMix: {
    congress: number;
    insiders: number;
    funds: number;
  };
}) {
  const parts: string[] = [];
  if (story.sourceMix.congress) {
    parts.push(story.sourceMix.congress > 1 ? `Congress ${story.sourceMix.congress}` : 'Congress');
  }
  if (story.sourceMix.insiders) {
    parts.push(story.sourceMix.insiders > 1 ? `Insiders ${story.sourceMix.insiders}` : 'Insiders');
  }
  if (story.sourceMix.funds) {
    parts.push(story.sourceMix.funds > 1 ? `Funds ${story.sourceMix.funds}` : 'Funds');
  }
  return parts.length ? parts.join(' • ') : 'Single source';
}

type DashboardSignalEventRow = {
  id: string;
  actor_name?: string | null;
  payload?: Record<string, unknown> | null;
};

type ClusterStoryResolvedStats = {
  amountFloor: number;
  economicActorCount: number | null;
};

type DashboardSignalActorPreview = {
  memberId: string | null;
  name: string;
  party: string | null;
  chamber: string | null;
};

function nestedEventIds(value: unknown) {
  if (!Array.isArray(value)) {
    return [] as string[];
  }

  return value
    .map((item) => String(item ?? '').trim())
    .filter(Boolean);
}

function eventAmountLowerBound(row: DashboardSignalEventRow) {
  const payload = row.payload || {};
  return Math.max(
    parseAmountLowerBound(payload.amount_range),
    toNumber(payload.cluster_combined_lower_bound),
    toNumber(payload.cluster_total_value),
    toNumber(payload.insider_total_buy_value),
    toNumber(payload.insider_total_sell_value),
    toNumber(payload.insider_change_value),
    toNumber(payload.value),
    toNumber(payload.amount),
  );
}

function normalizedMoneyComponent(value: unknown) {
  const numeric = toNumber(value);
  if (!numeric) {
    return '';
  }
  return numeric.toFixed(4).replace(/\.?0+$/, '');
}

function insiderEconomicTransactionKey(row: DashboardSignalEventRow) {
  const payload = row.payload || {};
  const sourceUrl = trim(payload.source_url);
  const hasInsiderFilingShape = Boolean(sourceUrl.includes('sec.gov') || trim(payload.transaction_code));
  if (!hasInsiderFilingShape) {
    return row.id;
  }

  const ticker = trim(payload.ticker).toUpperCase();
  const direction = trim(payload.transaction_type || payload.transaction_code).toLowerCase();
  const transactionDate = trim(payload.transaction_date || payload.occurred_at).slice(0, 10);
  const amount = normalizedMoneyComponent(payload.amount);
  const price = normalizedMoneyComponent(payload.price);
  const value = normalizedMoneyComponent(payload.value);

  if (!ticker || !direction || !transactionDate || (!amount && !value)) {
    return row.id;
  }

  return ['insider-economic', ticker, direction, transactionDate, amount, price, value].join('::');
}

function uniqueEconomicLeafRows(rows: DashboardSignalEventRow[]) {
  const rowsByEconomicKey = new Map<string, DashboardSignalEventRow>();
  for (const row of rows) {
    const key = insiderEconomicTransactionKey(row);
    if (!rowsByEconomicKey.has(key)) {
      rowsByEconomicKey.set(key, row);
    }
  }
  return [...rowsByEconomicKey.values()];
}

function insiderEconomicActorSignature(row: DashboardSignalEventRow) {
  const payload = row.payload || {};
  return trim(payload.filer_name || payload.politician_name || payload.fund_name || row.actor_name).toLowerCase() || row.id;
}

function uniqueEconomicActorCount(rows: DashboardSignalEventRow[]) {
  const rowsByActor = new Map<string, DashboardSignalEventRow[]>();
  for (const row of rows) {
    const actorKey = insiderEconomicActorSignature(row);
    const existingRows = rowsByActor.get(actorKey) || [];
    existingRows.push(row);
    rowsByActor.set(actorKey, existingRows);
  }

  const actorEconomicSignatures = new Set<string>();
  for (const actorRows of rowsByActor.values()) {
    const economicKeys = uniqueEconomicLeafRows(actorRows)
      .map((row) => insiderEconomicTransactionKey(row))
      .sort();
    if (economicKeys.length) {
      actorEconomicSignatures.add(economicKeys.join('|'));
    }
  }

  return actorEconomicSignatures.size;
}

function storyEconomicStats(story: BroadcastStory, rowsById: Map<string, DashboardSignalEventRow>) {
  const allLeafRows = collectLeafSignalRows(story.supportingEventIds, rowsById);
  const uniqueLeafRows = uniqueEconomicLeafRows(allLeafRows);
  const amountFloor = uniqueLeafRows.reduce((total, row) => total + eventAmountLowerBound(row), 0);

  if (story.ruleKey !== 'insider_cluster') {
    return {
      amountFloor,
      economicActorCount: null,
    };
  }

  return {
    amountFloor,
    economicActorCount: uniqueEconomicActorCount(allLeafRows) || null,
  };
}

async function loadSignalEventRowsById(eventIds: string[]) {
  if (!eventIds.length) {
    return new Map<string, DashboardSignalEventRow>();
  }

  const supabase = getAdminSupabase();
  const rowsById = new Map<string, DashboardSignalEventRow>();
  const pendingIds = [...new Set(eventIds.filter(Boolean))];

  while (pendingIds.length) {
    const batch = pendingIds.splice(0, 200).filter((id) => !rowsById.has(id));
    if (!batch.length) {
      continue;
    }

    const { data, error } = await supabase.from('signal_events').select('id, actor_name, payload').in('id', batch);
    if (error) {
      throw new Error(error.message);
    }

    for (const row of (data ?? []) as DashboardSignalEventRow[]) {
      rowsById.set(row.id, row);

      const payload = row.payload || {};
      const childIds = [
        ...nestedEventIds(payload.cluster_event_ids),
        ...nestedEventIds(payload.group_event_ids),
      ];

      for (const childId of childIds) {
        if (!rowsById.has(childId)) {
          pendingIds.push(childId);
        }
      }
    }
  }

  return rowsById;
}

function collectLeafSignalRows(
  eventIds: string[],
  rowsById: Map<string, DashboardSignalEventRow>,
) {
  const leafRows: DashboardSignalEventRow[] = [];
  const seen = new Set<string>();
  const queue = [...eventIds];

  while (queue.length) {
    const eventId = queue.shift();
    if (!eventId || seen.has(eventId)) {
      continue;
    }
    seen.add(eventId);

    const row = rowsById.get(eventId);
    if (!row) {
      continue;
    }

    const payload = row.payload || {};
    const childIds = [
      ...nestedEventIds(payload.cluster_event_ids),
      ...nestedEventIds(payload.group_event_ids),
    ];

    if (childIds.length) {
      for (const childId of childIds) {
        if (!seen.has(childId)) {
          queue.push(childId);
        }
      }
      continue;
    }

    leafRows.push(row);
  }

  return leafRows;
}

function uniqueActorPreviews(actors: DashboardSignalActorPreview[]) {
  const seen = new Set<string>();
  const unique: DashboardSignalActorPreview[] = [];

  for (const actor of actors) {
    const key = actor.memberId || actor.name.toLowerCase();
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(actor);
  }

  return unique;
}

function politicianPreviewForEvent(row: DashboardSignalEventRow): DashboardSignalActorPreview | null {
  const payload = row.payload || {};
  const memberId = trim(payload.member_id) || null;
  const name = trim(payload.politician_name || payload.actor_name);
  const party = trim(payload.party) || null;
  const chamber = trim(payload.chamber) || null;

  if (!memberId && !trim(payload.politician_name)) {
    return null;
  }
  if (!name && !memberId) {
    return null;
  }

  return {
    memberId,
    name: name || memberId || 'Unknown',
    party,
    chamber,
  };
}

function shiftIsoDate(value: string, daysBack: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() - daysBack);
  return date.toISOString().slice(0, 10);
}

type FeaturedPoliticianCandidate = {
  memberId: string;
  name: string;
  party: string | null;
  chamber: string | null;
};

type DashboardPoliticianProfileLite = {
  summary: PoliticianProfileSummary;
  trades: PoliticianProfileTrade[];
};

function uniqueFeaturedPoliticianCandidates(candidates: FeaturedPoliticianCandidate[]) {
  const seen = new Set<string>();
  const unique: FeaturedPoliticianCandidate[] = [];

  for (const candidate of candidates) {
    const memberId = trim(candidate.memberId).toUpperCase();
    if (!memberId || seen.has(memberId)) {
      continue;
    }
    seen.add(memberId);
    unique.push({
      memberId,
      name: trim(candidate.name) || memberId,
      party: trim(candidate.party) || null,
      chamber: trim(candidate.chamber) || null,
    });
  }

  return unique;
}

function resolveDashboardTradeDate(trade: PoliticianProfileTrade, asOfDate: string) {
  const transactionDate = normalizeProfileDate(trade.transaction_date);
  if (transactionDate && transactionDate <= asOfDate) {
    return transactionDate;
  }

  const publishedDate = normalizeProfileDate(trade.published_date);
  if (publishedDate && publishedDate <= asOfDate) {
    return publishedDate;
  }

  return null;
}

function trailingMonthBuckets(asOfDate: string, count = 12) {
  return Array.from({ length: count }, (_, index) => {
    const date = new Date(`${asOfDate.slice(0, 7)}-01T12:00:00Z`);
    date.setUTCMonth(date.getUTCMonth() - (count - 1 - index));

    return {
      key: date.toISOString().slice(0, 7),
      label: date.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' }),
    };
  });
}

function politicianTradeTimeline(trades: PoliticianProfileTrade[], asOfDate: string) {
  const buckets = trailingMonthBuckets(asOfDate, 12).map((bucket) => ({
    ...bucket,
    buyAmount: 0,
    sellAmount: 0,
    netAmount: 0,
  }));
  const bucketsByKey = new Map(buckets.map((bucket) => [bucket.key, bucket]));

  for (const trade of trades) {
    const tradeDate = resolveDashboardTradeDate(trade, asOfDate);
    const ticker = normalizeProfileTicker(trade.ticker);
    const direction = normalizeProfileDirection(trade.transaction_type);
    const estimatedAmount = parseEstimatedTradeAmount(trade.amount_range);

    if (!tradeDate || !ticker || direction === 'other' || estimatedAmount <= 0) {
      continue;
    }

    const bucket = bucketsByKey.get(tradeDate.slice(0, 7));
    if (!bucket) {
      continue;
    }

    if (direction === 'buy') {
      bucket.buyAmount += estimatedAmount;
    } else {
      bucket.sellAmount += estimatedAmount;
    }
    bucket.netAmount = bucket.buyAmount - bucket.sellAmount;
  }

  return buckets;
}

function tradeBasedOneYearReturnPct(
  trades: PoliticianProfileTrade[],
  seriesByTicker: Map<string, Awaited<ReturnType<typeof getMarketPriceSeries>>>,
  anchorDate: string,
  asOfDate: string,
) {
  let weightedReturn = 0;
  let totalTrackedAmount = 0;

  for (const trade of trades) {
    const ticker = normalizeProfileTicker(trade.ticker);
    const tradeDate = resolveDashboardTradeDate(trade, asOfDate);
    const direction = normalizeProfileDirection(trade.transaction_type);
    const estimatedAmount = parseEstimatedTradeAmount(trade.amount_range);

    if (!ticker || !tradeDate || direction === 'other' || estimatedAmount <= 0) {
      continue;
    }
    if (tradeDate < anchorDate || tradeDate > asOfDate) {
      continue;
    }

    const series = seriesByTicker.get(ticker);
    const tradePrice = series ? getPriceOnOrBefore(series, tradeDate) : null;
    const currentPrice = series?.currentPrice || series?.points.at(-1)?.price || null;
    if (!tradePrice || tradePrice <= 0 || !currentPrice || currentPrice <= 0) {
      continue;
    }

    const legReturn =
      direction === 'sell'
        ? (tradePrice - currentPrice) / tradePrice
        : (currentPrice - tradePrice) / tradePrice;

    weightedReturn += estimatedAmount * legReturn;
    totalTrackedAmount += estimatedAmount;
  }

  if (totalTrackedAmount <= 0) {
    return null;
  }

  return (weightedReturn / totalTrackedAmount) * 100;
}

function topDashboardReturnTickers(
  trades: PoliticianProfileTrade[],
  anchorDate: string,
  asOfDate: string,
  limit = DASHBOARD_RETURN_TICKER_LIMIT,
) {
  const trackedAmountByTicker = new Map<string, number>();

  for (const trade of trades) {
    const ticker = normalizeProfileTicker(trade.ticker);
    const tradeDate = resolveDashboardTradeDate(trade, asOfDate);
    const direction = normalizeProfileDirection(trade.transaction_type);
    const estimatedAmount = parseEstimatedTradeAmount(trade.amount_range);

    if (!ticker || !tradeDate || direction === 'other' || estimatedAmount <= 0) {
      continue;
    }
    if (tradeDate < anchorDate || tradeDate > asOfDate) {
      continue;
    }

    trackedAmountByTicker.set(ticker, (trackedAmountByTicker.get(ticker) || 0) + estimatedAmount);
  }

  return [...trackedAmountByTicker.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, limit)
    .map(([ticker]) => ticker);
}

async function loadDashboardFeaturedPoliticianProfile(
  memberId: string,
): Promise<DashboardPoliticianProfileLite | null> {
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
    .limit(1200);

  if (error) {
    throw new Error(error.message);
  }

  const scopedTrades = filterProductPoliticianTrades((data || []) as PoliticianProfileTrade[]);
  if (!scopedTrades.length) {
    return null;
  }

  const trades = await enrichPoliticianTradesWithAssetNames(scopedTrades);

  return {
    summary: buildPoliticianProfileSummary(trades),
    trades,
  };
}

async function loadDashboardFeaturedPoliticians(
  featuredSignals: DashboardFeaturedSignal[],
): Promise<DashboardFeaturedPolitician[]> {
  const signalCandidates = uniqueFeaturedPoliticianCandidates(
    featuredSignals.flatMap((signal) =>
      signal.actorPreviews
        .filter((actor) => actor.memberId)
        .map((actor) => ({
          memberId: actor.memberId || '',
          name: actor.name,
          party: actor.party,
          chamber: actor.chamber,
        })),
    ),
  );

  let candidatePool = [...signalCandidates];

  if (candidatePool.length < DASHBOARD_SIGNAL_LIMIT) {
    const recentTrades = (await loadPoliticianFeedTrades().catch(() => [])) as PoliticianProfileTrade[];
    const fallbackCandidates = uniqueFeaturedPoliticianCandidates(
      recentTrades.map((trade) => ({
        memberId: trim(trade.member_id),
        name:
          trim(
            trade.congress_members?.first_name && trade.congress_members?.last_name
              ? `${trade.congress_members.first_name} ${trade.congress_members.last_name}`
              : trade.politician_name,
          ) || 'Unknown politician',
        party: trade.congress_members?.party || null,
        chamber: trade.congress_members?.chamber || trade.chamber || null,
      })),
    );

    candidatePool = uniqueFeaturedPoliticianCandidates([...candidatePool, ...fallbackCandidates]);
  }

  const shortlistedCandidates = candidatePool.slice(0, 12);
  const profiles = (
    await Promise.all(
      shortlistedCandidates.map(async (candidate) => {
        try {
          const profile = await loadDashboardFeaturedPoliticianProfile(candidate.memberId);
          return profile ? { candidate, profile } : null;
        } catch {
          return null;
        }
      }),
    )
  ).filter((entry): entry is { candidate: FeaturedPoliticianCandidate; profile: DashboardPoliticianProfileLite } =>
    Boolean(entry),
  );

  if (!profiles.length) {
    return shortlistedCandidates.slice(0, DASHBOARD_SIGNAL_LIMIT).map((candidate) => ({
      ...candidate,
      oneYearReturnPct: null,
      returnLabel: 'Return',
      totalBuyAmount: 0,
      totalSellAmount: 0,
      timeline: [],
    }));
  }

  const anchorDate = shiftIsoDate(new Date().toISOString().slice(0, 10), 365);
  const asOfDate = new Date().toISOString().slice(0, 10);
  const tickers = Array.from(
    new Set(
      profiles.flatMap(({ profile }) =>
        topDashboardReturnTickers(profile.trades, anchorDate, asOfDate, DASHBOARD_RETURN_TICKER_LIMIT),
      ),
    ),
  );

  const seriesByTicker = await getMarketPriceSeriesMap(
    tickers.map((ticker) => ({ ticker, earliestDate: anchorDate })),
    6,
  );

  return profiles
    .map(({ candidate, profile }) => {
      const timeline = politicianTradeTimeline(profile.trades, asOfDate);
      const totalBuyAmount = timeline.reduce((sum, bucket) => sum + bucket.buyAmount, 0);
      const totalSellAmount = timeline.reduce((sum, bucket) => sum + bucket.sellAmount, 0);
      const tradeBasedReturnPct = tradeBasedOneYearReturnPct(profile.trades, seriesByTicker, anchorDate, asOfDate);

      return {
        memberId: candidate.memberId,
        name: profile.summary.displayName || candidate.name,
        party: profile.summary.party || candidate.party,
        chamber: profile.summary.chamber || candidate.chamber,
        oneYearReturnPct: tradeBasedReturnPct,
        returnLabel:
          tradeBasedReturnPct !== null
            ? '1Y return'
            : 'No 1Y data',
        totalBuyAmount,
        totalSellAmount,
        timeline,
      };
    })
    .sort((left, right) => {
      const leftScore =
        typeof left.oneYearReturnPct === 'number' && Number.isFinite(left.oneYearReturnPct)
          ? left.oneYearReturnPct
          : left.totalBuyAmount - left.totalSellAmount;
      const rightScore =
        typeof right.oneYearReturnPct === 'number' && Number.isFinite(right.oneYearReturnPct)
          ? right.oneYearReturnPct
          : right.totalBuyAmount - right.totalSellAmount;
      return rightScore - leftScore;
    })
    .slice(0, DASHBOARD_SIGNAL_LIMIT);
}

async function resolveStoryEconomicStats(stories: BroadcastStory[]) {
  const eventIds = Array.from(new Set(stories.flatMap((story) => story.supportingEventIds).filter(Boolean)));

  if (!eventIds.length) {
    return new Map<string, ClusterStoryResolvedStats>();
  }

  const rowsById = await loadSignalEventRowsById(eventIds);

  return new Map(
    stories.map((story) => {
      return [story.candidateKey, storyEconomicStats(story, rowsById)];
    })
  );
}

async function resolveStoryActorPreviews(stories: BroadcastStory[]) {
  const eventIds = Array.from(new Set(stories.flatMap((story) => story.supportingEventIds).filter(Boolean)));

  if (!eventIds.length) {
    return new Map<string, DashboardSignalActorPreview[]>();
  }

  const rowsById = await loadSignalEventRowsById(eventIds);

  return new Map(
    stories.map((story) => {
      const actorPreviews = uniqueActorPreviews(
        collectLeafSignalRows(story.supportingEventIds, rowsById)
          .map((row) => politicianPreviewForEvent(row))
          .filter((value): value is DashboardSignalActorPreview => Boolean(value)),
      ).slice(0, 3);

      return [story.candidateKey, actorPreviews];
    }),
  );
}

function ruleLabel(ruleKey: string, direction?: string | null) {
  if (ruleKey === 'congress_cluster') {
    return 'Congress Cluster';
  }
  if (ruleKey === 'cross_source_accumulation') {
    return trim(direction).toLowerCase() === 'sell' ? 'Cross-Source Sell' : 'Cross-Source Buy';
  }
  if (ruleKey === 'insider_cluster') {
    return 'Insider Cluster';
  }
  if (ruleKey === 'grouped_congress_buy') {
    return 'Congress Sweep';
  }
  if (ruleKey === 'grouped_insider_buy') {
    return 'Insider Sweep';
  }
  if (ruleKey === 'large_politician_buy') {
    return 'Large Position';
  }
  if (ruleKey === 'substantial_insider_buy') {
    return 'Heavy Insider Buy';
  }
  return ruleKey
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function storyPreview(actorLabels: string[]) {
  if (!actorLabels.length) {
    return null;
  }
  return actorLabels.slice(0, 3).join(', ');
}

function sourceGroupForRule(ruleKey: string): PublicClusterSignal['sourceGroup'] {
  if (ruleKey === 'cross_source_accumulation') {
    return 'cross-source';
  }
  if (ruleKey === 'insider_cluster' || ruleKey === 'grouped_insider_buy') {
    return 'insiders';
  }
  return 'congress';
}

function storyTitle(story: {
  ruleKey: string;
  ticker: string | null;
  title: string | null;
  actorName: string | null;
  actorCount: number;
  direction: string | null;
}) {
  const ticker = trim(story.ticker).toUpperCase();
  const title = trim(story.title);
  const actorName = trim(story.actorName);
  const normalizedDirection = trim(story.direction).toLowerCase();

  if (story.ruleKey === 'congress_cluster') {
    const action = normalizedDirection === 'sell' ? 'sellers' : 'buyers';
    return `${ticker} drew ${story.actorCount} congressional ${action}`;
  }
  if (story.ruleKey === 'insider_cluster') {
    const action = normalizedDirection === 'sell' ? 'sellers' : 'buyers';
    return `${ticker} drew ${story.actorCount} insider ${action}`;
  }
  if (story.ruleKey === 'cross_source_accumulation') {
    if (normalizedDirection === 'sell') {
      return `${ticker} is seeing cross-source selling`;
    }
    return `${ticker} is attracting cross-source accumulation`;
  }
  if (story.ruleKey === 'grouped_congress_buy') {
    return title || (actorName ? `${actorName} reported repeated ${ticker} buys` : `${ticker} drew repeat congressional buying`);
  }
  if (story.ruleKey === 'grouped_insider_buy') {
    return title || (actorName ? `${actorName} reported repeated insider buys in ${ticker}` : `${ticker} drew repeated insider buying`);
  }
  return `${ticker} flagged as a high-conviction signal`;
}

function storySummary(story: {
  ruleKey: string;
  actorCount: number;
  clusterWindowDays: number | null;
  sourceMix: {
    congress: number;
    insiders: number;
    funds: number;
  };
}) {
  const parts: string[] = [];
  const clusterWindowDays = story.clusterWindowDays && story.clusterWindowDays > 0 ? story.clusterWindowDays : null;
  const windowLabel = clusterWindowDays
    ? ` inside ${clusterWindowDays} day${clusterWindowDays === 1 ? '' : 's'}`
    : '';

  if (story.ruleKey === 'congress_cluster' || story.ruleKey === 'insider_cluster') {
    parts.push(`${story.actorCount} actors${windowLabel}`);
  } else if (story.ruleKey === 'grouped_congress_buy' || story.ruleKey === 'grouped_insider_buy') {
    parts.push('Repeated buys landed in one filing');
  } else if (story.ruleKey === 'cross_source_accumulation') {
    parts.push(`${sourceMixLabel(story)}${windowLabel || ' aligned'}`);
  }

  return parts.join(' • ');
}

function compareSignalsByScoreThenFreshness(left: PublicClusterSignal, right: PublicClusterSignal) {
  if (right.score !== left.score) {
    return right.score - left.score;
  }

  return (right.publishedAt || '').localeCompare(left.publishedAt || '');
}

function passesClusterFeedScoreGate(story: BroadcastStory) {
  if (story.score >= DASHBOARD_MIN_SIGNAL_SCORE) {
    return true;
  }

  const direction = trim(story.direction).toLowerCase();
  if (
    story.ruleKey === 'insider_cluster' &&
    direction === 'sell' &&
    story.score >= MATERIAL_INSIDER_CLUSTER_MIN_SCORE &&
    (story.actorCount >= 3 || story.amountFloor >= MATERIAL_INSIDER_CLUSTER_MIN_VALUE)
  ) {
    return true;
  }

  return false;
}

function redundantClusterStoryKey(story: BroadcastStory) {
  if (!['congress_cluster', 'insider_cluster', 'cross_source_accumulation'].includes(story.ruleKey)) {
    return null;
  }

  const ticker = trim(story.ticker).toUpperCase();
  const direction = trim(story.direction).toLowerCase() || 'unknown';
  const actorKey = story.actorLabels
    .map((label) => trim(label).toLowerCase())
    .filter(Boolean)
    .sort()
    .join('|');

  if (!ticker || !actorKey) {
    return null;
  }

  return `${story.ruleKey}::${ticker}::${direction}::${actorKey}`;
}

function strongerClusterStory(left: BroadcastStory, right: BroadcastStory) {
  if (left.score !== right.score) {
    return left.score > right.score ? left : right;
  }
  if (left.actorCount !== right.actorCount) {
    return left.actorCount > right.actorCount ? left : right;
  }
  if (left.amountFloor !== right.amountFloor) {
    return left.amountFloor > right.amountFloor ? left : right;
  }
  return (left.latestPublishedAt || left.createdAt) >= (right.latestPublishedAt || right.createdAt) ? left : right;
}

function dedupeClusterFeedStories(stories: BroadcastStory[]) {
  const passthrough: BroadcastStory[] = [];
  const storyByKey = new Map<string, BroadcastStory>();

  for (const story of stories) {
    const key = redundantClusterStoryKey(story);
    if (!key) {
      passthrough.push(story);
      continue;
    }

    const existing = storyByKey.get(key);
    storyByKey.set(key, existing ? strongerClusterStory(story, existing) : story);
  }

  return [...passthrough, ...storyByKey.values()];
}

async function loadClusterSignals({
  limit,
  buyOnly,
  sinceDate,
  statuses,
  sort,
}: {
  limit: number;
  buyOnly: boolean;
  sinceDate: string | null;
  statuses: string[];
  sort: 'score' | 'newest';
}): Promise<PublicClusterSignal[]> {
  const stories = await fetchTweetCandidateStories({
    status: statuses,
    sinceDate,
    storyLimit: Math.max(limit * 5, 60),
    category: 'cluster_feed',
    sort,
    resolveAmounts: false,
    ruleKeys: [...CLUSTER_FEED_RULES],
  });

  const candidateStories = dedupeClusterFeedStories(
    stories.filter((story) => {
      const ticker = trim(story.ticker).toUpperCase();
      if (!ticker || ['N/A', 'UNKNOWN', 'MULTI'].includes(ticker)) {
        return false;
      }
      if (!CLUSTER_FEED_RULES.has(story.ruleKey)) {
        return false;
      }
      if (!passesClusterFeedScoreGate(story)) {
        return false;
      }
      if (!story.supportingEventIds.length) {
        return false;
      }

      const direction = trim(story.direction).toLowerCase();
      if (buyOnly && direction && direction !== 'buy') {
        return false;
      }

      return true;
    }),
  )
    .sort((left, right) => {
      if (sort === 'newest') {
        return (right.latestPublishedAt || right.createdAt).localeCompare(left.latestPublishedAt || left.createdAt);
      }
      return right.score - left.score || (right.latestPublishedAt || right.createdAt).localeCompare(left.latestPublishedAt || left.createdAt);
    });

  const validationPool = candidateStories.slice(0, Math.max(limit * 3, limit + 120));
  const economicStatsByStory = await resolveStoryEconomicStats(validationPool).catch(
    () => new Map<string, ClusterStoryResolvedStats>(),
  );
  const filteredStories = validationPool.filter((story) => {
    const stats = economicStatsByStory.get(story.candidateKey);
    if (story.ruleKey !== 'insider_cluster') {
      return true;
    }
    return (stats?.economicActorCount ?? story.actorCount) >= 2;
  }).slice(0, limit);
  const actorPreviewsByStory = await resolveStoryActorPreviews(filteredStories).catch(
    () => new Map<string, DashboardSignalActorPreview[]>(),
  );

  return filteredStories.map((story) => {
      const normalizedDirection = trim(story.direction).toLowerCase();
      const stats = economicStatsByStory.get(story.candidateKey);
      const amountFloor = stats?.amountFloor && stats.amountFloor > 0 ? stats.amountFloor : story.amountFloor;
      const actorCount =
        story.ruleKey === 'insider_cluster'
          ? stats?.economicActorCount ?? story.actorCount
          : story.actorCount;
      const amountLabel = moneyFloorLabel(amountFloor);
      return {
        id: story.candidateKey,
        ticker: trim(story.ticker).toUpperCase(),
        title: storyTitle({ ...story, actorCount }),
        summary: storySummary({
          ruleKey: story.ruleKey,
          actorCount,
          clusterWindowDays: story.clusterWindowDays,
          sourceMix: story.sourceMix,
        }),
        ruleLabel: ruleLabel(story.ruleKey, normalizedDirection),
        actorPreview: storyPreview(story.actorLabels),
        actorPreviews: actorPreviewsByStory.get(story.candidateKey) || [],
        actorCount,
        amountLabel,
        sourceLabel: sourceMixLabel(story),
        publishedAt: story.latestPublishedAt,
        ruleKey: story.ruleKey,
        sourceGroup: sourceGroupForRule(story.ruleKey),
        score: story.score,
        direction:
          normalizedDirection === 'buy'
            ? 'buy'
            : normalizedDirection === 'sell'
              ? 'sell'
              : null,
      };
    });
}

async function loadDashboardFeaturedSignals(): Promise<DashboardFeaturedSignal[]> {
  const recentSignals = await loadClusterSignals({
    limit: DASHBOARD_SIGNAL_CANDIDATE_LIMIT,
    buyOnly: true,
    sinceDate: dashboardSinceDate(),
    statuses: PUBLIC_BROADCAST_STORY_STATUSES,
    sort: 'newest',
  });

  const fallbackSignals =
    recentSignals.length < DASHBOARD_SIGNAL_LIMIT
      ? await loadClusterSignals({
          limit: DASHBOARD_SIGNAL_CANDIDATE_LIMIT,
          buyOnly: true,
          sinceDate: null,
          statuses: PUBLIC_BROADCAST_STORY_STATUSES,
          sort: 'newest',
        })
      : [];

  const rankedRecentSignals = [...recentSignals]
    .sort(compareSignalsByScoreThenFreshness)
    .slice(0, DASHBOARD_SIGNAL_LIMIT);

  const seenSignalIds = new Set(rankedRecentSignals.map((signal) => signal.id));
  const rankedFallbackSignals = [...fallbackSignals]
    .filter((signal) => !seenSignalIds.has(signal.id))
    .sort(compareSignalsByScoreThenFreshness)
    .slice(0, Math.max(DASHBOARD_SIGNAL_LIMIT - rankedRecentSignals.length, 0));

  const signals = [...rankedRecentSignals, ...rankedFallbackSignals];

  return signals.map((signal) => ({
    id: signal.id,
    ticker: signal.ticker,
    title: signal.title,
    summary: signal.summary,
    ruleLabel: signal.ruleLabel,
    actorPreview: signal.actorPreview,
    actorPreviews: signal.actorPreviews,
    actorCount: signal.actorCount,
    amountLabel: signal.amountLabel,
    sourceLabel: signal.sourceLabel,
    publishedAt: signal.publishedAt,
    direction: signal.direction,
  }));
}

async function loadDashboardData(): Promise<DashboardData> {
  const featuredSignals = await loadDashboardFeaturedSignals().catch(() => []);
  const featuredPoliticians = await loadDashboardFeaturedPoliticians(featuredSignals).catch(() => []);

  return {
    politicianCount: 0,
    insiderCount: 0,
    fundCount: 0,
    featuredSignals,
    featuredPoliticians,
  };
}

type SortablePoliticianFeedTrade = {
  id?: string | null;
  published_date?: string | null;
  created_at?: string | null;
};

function sortPoliticianFeedTrades<T>(rows: T[]): T[] {
  return rows.sort((leftRow, rightRow) => {
    const left = leftRow as SortablePoliticianFeedTrade;
    const right = rightRow as SortablePoliticianFeedTrade;
    const publishedDelta =
      new Date(right.published_date || '').getTime() - new Date(left.published_date || '').getTime();
    if (publishedDelta !== 0) {
      return publishedDelta;
    }
    const createdDelta = new Date(right.created_at || '').getTime() - new Date(left.created_at || '').getTime();
    if (createdDelta !== 0) {
      return createdDelta;
    }
    return String(left.id || '').localeCompare(String(right.id || ''));
  });
}

async function loadPoliticianFeedTrades() {
  const supabase = getPublicSupabase();
  const { data, error } = await supabase
    .from('politician_trades')
    .select(POLITICIAN_FEED_SELECT)
    .not('ticker', 'is', null)
    .neq('ticker', '')
    .not('ticker', 'in', '("N/A","NA","UNKNOWN","MULTI")')
    .gte('published_date', HOUSE_PRODUCT_START_DATE)
    .order('published_date', { ascending: false })
    .limit(INITIAL_POLITICIAN_FEED_LIMIT + POLITICIAN_FEED_SORT_BUFFER);

  if (error) {
    console.warn('[dashboard] politician feed query failed, returning empty:', error.message);
    return [];
  }

  // Keep the public feed query lean. Raw filing enrichment can timeout on the
  // large JSON table, and non-ticker rows are filtered out below anyway.
  return sortPoliticianFeedTrades(filterDisplayPoliticianTrades(data || [])).slice(0, INITIAL_POLITICIAN_FEED_LIMIT);
}

async function loadInsiderFeedTrades() {
  const supabase = getPublicSupabase();
  const { data, error } = await supabase
    .from('insider_trades')
    .select(INSIDER_FEED_SELECT)
    .order('transaction_date', { ascending: false })
    .limit(INITIAL_INSIDER_FEED_LIMIT);

  if (error) {
    throw new Error(error.message);
  }

  return data || [];
}

const loadClusterFeedSignals = unstable_cache(
  async () =>
    loadClusterSignals({
      limit: CLUSTER_PAGE_SIGNAL_LIMIT,
      buyOnly: false,
      sinceDate: null,
      statuses: PUBLIC_BROADCAST_STORY_STATUSES,
      sort: 'newest',
    }),
  ['public-cluster-feed-v2'],
  { revalidate: PUBLIC_FEED_REVALIDATE_SECONDS },
);

const loadDashboardClusterPreviewSignals = unstable_cache(
  async () =>
    loadClusterSignals({
      limit: DASHBOARD_CLUSTER_PREVIEW_LIMIT,
      buyOnly: false,
      sinceDate: dashboardSinceDate(),
      statuses: PUBLIC_BROADCAST_STORY_STATUSES,
      sort: 'newest',
    }),
  ['dashboard-cluster-preview-v2'],
  { revalidate: PUBLIC_FEED_REVALIDATE_SECONDS },
);

const loadCachedFundDirectory = unstable_cache(
  async (configVersion: string): Promise<FundDirectoryEntry[]> => {
    void configVersion;
    return loadFundDirectory();
  },
  ['public-fund-directory-v7'],
  { revalidate: HEDGE_FUNDS_REVALIDATE_SECONDS },
);

const loadCachedPoliticianProfile = unstable_cache(
  async (memberId: string): Promise<PoliticianProfileData | null> =>
    getPoliticianProfileData(memberId, { limit: 240, includeLivePortfolio: false }),
  ['public-politician-profile'],
  { revalidate: POLITICIAN_PROFILE_REVALIDATE_SECONDS },
);

export async function getDashboardData() {
  return loadDashboardData();
}

export async function getPublicPoliticianFeedTrades() {
  return loadPoliticianFeedTrades();
}

export async function getPublicInsiderFeedTrades() {
  return loadInsiderFeedTrades();
}

export async function getPublicClusterSignals() {
  return loadClusterFeedSignals();
}

export async function getDashboardRecentClusterSignals() {
  return loadDashboardClusterPreviewSignals();
}

export async function getCachedFundDirectory() {
  return loadCachedFundDirectory(await getFundDirectoryCacheVersion());
}

export async function getCachedFundHoldings(fundName: string) {
  // Large 13F managers like Vanguard and State Street can exceed Next's 2MB
  // unstable_cache payload limit. Keep directory data cached, but load fund
  // detail rows directly so the detail page stays functional for large filers.
  return loadInstitutionalHoldingRows(fundName);
}

export async function getCachedPoliticianProfileData(memberId: string) {
  return loadCachedPoliticianProfile(memberId);
}

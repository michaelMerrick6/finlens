import 'server-only';

import { MIN_CONGRESS_CLUSTER_ACTORS } from '@/lib/cluster-quality';
import { buildPoliticianClusterKey } from '@/lib/politician-cluster-key';
import { getPublicSupabase } from '@/lib/supabase-server';

export const POLITICIAN_CLUSTER_WINDOW_DAYS = 45;

const PAGE_SIZE = 1_000;
const PAGE_CONCURRENCY = 4;
const MAX_ROWS = 20_000;
const INVALID_TICKERS = new Set(['N/A', 'NA', 'UNKNOWN', 'MULTI', 'US-TREAS']);

type PoliticianClusterRow = {
  id: string;
  member_id: string | null;
  politician_name: string | null;
  ticker: string | null;
  asset_name: string | null;
  asset_type: string | null;
  transaction_type: string | null;
  amount_range: string | null;
  transaction_date: string | null;
  published_date: string | null;
};

type NormalizedPoliticianClusterRow = PoliticianClusterRow & {
  actorKey: string;
  actorName: string;
  direction: 'buy' | 'sell';
  publishedDate: string;
  tickerSymbol: string;
};

type PoliticianWindow = {
  actorCount: number;
  actorNames: string[];
  amountFloor: number;
  direction: 'buy' | 'sell';
  ticker: string;
  windowEnd: string;
  windowStart: string;
};

function addUtcDays(value: string, days: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function normalizeDirection(value: string | null) {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized.startsWith('purchase') || normalized.startsWith('buy')) return 'buy' as const;
  if (normalized.startsWith('sale') || normalized.startsWith('sell')) return 'sell' as const;
  return null;
}

function parseAmountFloor(value: string | null) {
  const match = String(value || '').replace(/,/g, '').match(/\$([\d.]+)/);
  return match ? Number(match[1]) || 0 : 0;
}

function moneyFloorLabel(value: number) {
  return value > 0 ? `$${Math.round(value).toLocaleString()}+` : null;
}

function normalizeRow(row: PoliticianClusterRow): NormalizedPoliticianClusterRow | null {
  const tickerSymbol = String(row.ticker || '').trim().toUpperCase();
  const publishedDate = String(row.published_date || '').slice(0, 10);
  const actorName = String(row.politician_name || '').replace(/[,\s]+$/g, '').trim();
  const actorKey = String(row.member_id || '').trim().toLowerCase() || actorName.toLowerCase();
  const direction = normalizeDirection(row.transaction_type);

  if (
    !tickerSymbol ||
    INVALID_TICKERS.has(tickerSymbol) ||
    !/^\d{4}-\d{2}-\d{2}$/.test(publishedDate) ||
    !actorKey ||
    !actorName ||
    !direction
  ) {
    return null;
  }

  return {
    ...row,
    actorKey,
    actorName,
    direction,
    publishedDate,
    tickerSymbol,
  };
}

async function loadPoliticianClusterRows(rangeStart: string, rangeEnd: string) {
  const supabase = getPublicSupabase();
  const queryStart = addUtcDays(rangeStart, -(POLITICIAN_CLUSTER_WINDOW_DAYS - 1));

  async function loadPage(offset: number) {
    const { data, error } = await supabase
      .from('politician_trades')
      .select('id, member_id, politician_name, ticker, asset_name, asset_type, transaction_type, amount_range, transaction_date, published_date')
      .gte('published_date', queryStart)
      .lte('published_date', rangeEnd)
      .not('ticker', 'is', null)
      .neq('ticker', '')
      .order('published_date', { ascending: true })
      .order('id', { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(error.message);
    return (data || []) as PoliticianClusterRow[];
  }

  const rows: PoliticianClusterRow[] = [];
  for (let offset = 0; offset < MAX_ROWS; offset += PAGE_SIZE * PAGE_CONCURRENCY) {
    const offsets = Array.from(
      { length: PAGE_CONCURRENCY },
      (_, index) => offset + index * PAGE_SIZE,
    ).filter((pageOffset) => pageOffset < MAX_ROWS);
    const pages = await Promise.all(offsets.map(loadPage));
    pages.forEach((page) => rows.push(...page));
    if (pages.some((page) => page.length < PAGE_SIZE)) break;
  }

  return rows;
}

function strongerWindow(left: PoliticianWindow | null, right: PoliticianWindow) {
  if (!left) return right;
  if (right.actorCount !== left.actorCount) return right.actorCount > left.actorCount ? right : left;
  if (right.amountFloor !== left.amountFloor) return right.amountFloor > left.amountFloor ? right : left;
  return right.windowEnd > left.windowEnd ? right : left;
}

function buildStrongestWindows(
  rows: PoliticianClusterRow[],
  rangeStart: string,
  rangeEnd: string,
) {
  const buckets = new Map<string, NormalizedPoliticianClusterRow[]>();

  for (const rawRow of rows) {
    const row = normalizeRow(rawRow);
    if (!row) continue;
    const key = `${row.tickerSymbol}::${row.direction}`;
    const bucket = buckets.get(key) || [];
    bucket.push(row);
    buckets.set(key, bucket);
  }

  const strongestWindows: PoliticianWindow[] = [];
  for (const bucket of buckets.values()) {
    bucket.sort((left, right) => left.publishedDate.localeCompare(right.publishedDate) || left.id.localeCompare(right.id));
    const anchorDates = [...new Set(
      bucket
        .map((row) => row.publishedDate)
        .filter((date) => date >= rangeStart && date <= rangeEnd),
    )];
    let strongest: PoliticianWindow | null = null;

    for (const windowEnd of anchorDates) {
      const windowStart = addUtcDays(windowEnd, -(POLITICIAN_CLUSTER_WINDOW_DAYS - 1));
      const windowRows = bucket.filter(
        (row) => row.publishedDate >= windowStart && row.publishedDate <= windowEnd,
      );
      const actors = new Map<string, string>();
      const economicTrades = new Set<string>();
      let amountFloor = 0;
      for (const row of windowRows) {
        if (!actors.has(row.actorKey)) actors.set(row.actorKey, row.actorName);
        const economicKey = [
          row.actorKey,
          row.tickerSymbol,
          row.direction,
          String(row.transaction_date || '').slice(0, 10),
          String(row.amount_range || '').replace(/\s+/g, '').toLowerCase(),
          String(row.asset_name || '').trim().toLowerCase(),
          String(row.asset_type || '').trim().toLowerCase(),
        ].join('::');
        if (economicTrades.has(economicKey)) continue;
        economicTrades.add(economicKey);
        amountFloor += parseAmountFloor(row.amount_range);
      }

      if (actors.size < MIN_CONGRESS_CLUSTER_ACTORS) continue;
      strongest = strongerWindow(strongest, {
        actorCount: actors.size,
        actorNames: [...actors.values()],
        amountFloor,
        direction: bucket[0].direction,
        ticker: bucket[0].tickerSymbol,
        windowEnd,
        windowStart,
      });
    }

    if (strongest) strongestWindows.push(strongest);
  }

  return strongestWindows;
}

export async function loadPoliticianClusterFeed(rangeStart: string, rangeEnd: string) {
  const rows = await loadPoliticianClusterRows(rangeStart, rangeEnd);
  return buildStrongestWindows(rows, rangeStart, rangeEnd)
    .sort((left, right) => {
      const freshnessDelta = right.windowEnd.localeCompare(left.windowEnd);
      if (freshnessDelta !== 0) return freshnessDelta;
      if (right.actorCount !== left.actorCount) return right.actorCount - left.actorCount;
      return right.amountFloor - left.amountFloor;
    })
    .map((cluster) => {
      const score = Math.min(0.99, 0.84 + Math.min(0.12, 0.04 * Math.max(cluster.actorCount - 2, 0)));
      const action = cluster.direction === 'sell' ? 'sellers' : 'buyers';
      return {
        id: buildPoliticianClusterKey({
          ticker: cluster.ticker,
          direction: cluster.direction,
          windowStart: cluster.windowStart,
          windowEnd: cluster.windowEnd,
        }),
        ticker: cluster.ticker,
        title: `${cluster.ticker} drew ${cluster.actorCount} congressional ${action}`,
        summary: `${cluster.actorCount} actors inside ${POLITICIAN_CLUSTER_WINDOW_DAYS} days`,
        ruleLabel: 'Congress Cluster',
        actorPreview: cluster.actorNames.slice(0, 3).join(', ') || null,
        actorPreviews: [],
        actorCount: cluster.actorCount,
        amountLabel: moneyFloorLabel(cluster.amountFloor),
        amountFloor: cluster.amountFloor,
        includesCongress: true,
        sourceLabel: 'Congress',
        publishedAt: cluster.windowEnd,
        ruleKey: 'congress_cluster',
        sourceGroup: 'congress' as const,
        sourceCounts: {
          congress: cluster.actorCount,
          insiders: 0,
          funds: 0,
        },
        score,
        windowDays: POLITICIAN_CLUSTER_WINDOW_DAYS,
        direction: cluster.direction,
      };
    });
}

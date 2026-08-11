import 'server-only';

import { unstable_cache } from 'next/cache';

import type {
  CongressClusterCalendarData,
  CongressClusterPlay,
  CongressClusterRange,
} from '@/lib/congress-cluster-calendar-types';
import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import { stripPoliticianOptionMetadata } from '@/lib/politician-option-trades';
import { getPublicSupabase } from '@/lib/supabase-server';

const RANGE_DAYS: Partial<Record<CongressClusterRange, number>> = {
  week: 7,
  month: 30,
};
const PAGE_SIZE = 1_000;
const PAGE_CONCURRENCY = 4;
const MAX_ROWS = 20_000;
const MIN_CLUSTER_ACTORS = 2;
const HIGH_CONVICTION_ACTORS = 4;

type CongressBuyRow = {
  id: string;
  member_id: string | null;
  politician_name: string | null;
  ticker: string | null;
  asset_name: string | null;
  asset_type: string | null;
  amount_range: string | null;
  transaction_date: string | null;
  published_date: string | null;
};

type MutablePlay = {
  ticker: string;
  companyNames: Map<string, number>;
  actors: Set<string>;
  tradeCount: number;
  amountFloor: number;
  latestTransactionDate: string | null;
  latestDisclosureDate: string;
};

function toIsoDate(date: Date) {
  return date.toISOString().slice(0, 10);
}

function currentPacificDate() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function addUtcDays(value: string, days: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return toIsoDate(date);
}

function rangeDates(range: CongressClusterRange) {
  const rangeEnd = currentPacificDate();
  const rangeStart = range === 'ytd'
    ? `${rangeEnd.slice(0, 4)}-01-01`
    : addUtcDays(rangeEnd, -((RANGE_DAYS[range] || 1) - 1));
  return {
    rangeEnd,
    rangeStart,
  };
}

function actorKey(row: CongressBuyRow) {
  const memberId = String(row.member_id || '').trim().toLowerCase();
  const politicianName = String(row.politician_name || '').replace(/[,\s]+$/g, '').trim();
  return memberId || politicianName.toLowerCase();
}

function normalizedCompanyName(row: CongressBuyRow) {
  const name = stripPoliticianOptionMetadata(row.asset_name)
    .replace(/\s+-\s+(?:Class\s+[A-Z]\s+)?Common\s+Stock.*$/i, '')
    .replace(/\s+(?:Class\s+[A-Z]\s+)?Common\s+Stock.*$/i, '')
    .replace(/\s+-\s+Class\s+[A-Z]\s+Capital\s+Stock.*$/i, '')
    .replace(/\s+(?:Class\s+[A-Z]\s+)?Ordinary\s+Shares?.*$/i, '')
    .replace(/\s+\[[A-Z]{2,4}\]\s*$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
  const ticker = String(row.ticker || '').trim().toUpperCase();
  if (
    !name ||
    name.toUpperCase() === ticker ||
    /^(common stock|ordinary shares?|capital stock|stock)$/i.test(name)
  ) {
    return null;
  }
  return name;
}

function preferredCompanyName(names: Map<string, number>) {
  return [...names.entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))[0]?.[0] || null;
}

function normalizedTransactionDate(row: CongressBuyRow, publishedDate: string) {
  const transactionDate = String(row.transaction_date || '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(transactionDate)) return null;
  return transactionDate > publishedDate ? publishedDate : transactionDate;
}

function economicTradeKey(row: CongressBuyRow, ticker: string, politicianKey: string) {
  return [
    politicianKey,
    ticker,
    String(row.transaction_date || '').slice(0, 10),
    String(row.amount_range || '').replace(/\s+/g, '').toLowerCase(),
    String(row.asset_name || '').trim().toLowerCase(),
    String(row.asset_type || '').trim().toLowerCase(),
  ].join('::');
}

async function loadCongressBuys(rangeStart: string, rangeEnd: string): Promise<CongressBuyRow[]> {
  const supabase = getPublicSupabase();

  async function loadPage(offset: number) {
    const { data, error } = await supabase
      .from('politician_trades')
      .select(`
        id,
        member_id,
        politician_name,
        ticker,
        asset_name,
        asset_type,
        amount_range,
        transaction_date,
        published_date
      `)
      .gte('published_date', rangeStart)
      .lte('published_date', rangeEnd)
      .not('ticker', 'is', null)
      .neq('ticker', '')
      .not('ticker', 'in', '("N/A","NA","UNKNOWN","MULTI","US-TREAS")')
      .or('transaction_type.ilike.buy%,transaction_type.ilike.purchase%')
      .order('published_date', { ascending: false })
      .order('id', { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) {
      throw new Error(error.message);
    }

    return (data || []) as CongressBuyRow[];
  }

  const rows = await loadPage(0);
  if (rows.length < PAGE_SIZE) return rows;

  for (let offset = PAGE_SIZE; offset < MAX_ROWS; offset += PAGE_SIZE * PAGE_CONCURRENCY) {
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

function buildAccumulationData(
  range: CongressClusterRange,
  rows: CongressBuyRow[],
): CongressClusterCalendarData {
  const plays = new Map<string, MutablePlay>();
  const allActors = new Set<string>();
  const allTickers = new Set<string>();
  const economicTrades = new Set<string>();
  let processedTradeCount = 0;
  let totalAmountFloor = 0;
  let latestDisclosureDate = '';

  for (const row of rows) {
    const ticker = String(row.ticker || '').trim().toUpperCase();
    const publishedDate = String(row.published_date || '').slice(0, 10);
    const politicianKey = actorKey(row);
    if (!ticker || !publishedDate || !politicianKey) {
      continue;
    }

    const tradeKey = economicTradeKey(row, ticker, politicianKey);
    if (economicTrades.has(tradeKey)) continue;
    economicTrades.add(tradeKey);

    const amountFloor = parsePoliticianAmountRange(row.amount_range)?.min || 0;
    const companyName = normalizedCompanyName(row);
    const transactionDate = normalizedTransactionDate(row, publishedDate);
    const play = plays.get(ticker) || {
      ticker,
      companyNames: new Map<string, number>(),
      actors: new Set<string>(),
      tradeCount: 0,
      amountFloor: 0,
      latestTransactionDate: null,
      latestDisclosureDate: publishedDate,
    };
    play.actors.add(politicianKey);
    play.tradeCount += 1;
    play.amountFloor += amountFloor;
    if (transactionDate && (!play.latestTransactionDate || transactionDate > play.latestTransactionDate)) {
      play.latestTransactionDate = transactionDate;
    }
    play.latestDisclosureDate =
      publishedDate > play.latestDisclosureDate ? publishedDate : play.latestDisclosureDate;
    if (companyName) {
      play.companyNames.set(companyName, (play.companyNames.get(companyName) || 0) + 1);
    }
    plays.set(ticker, play);

    allActors.add(politicianKey);
    allTickers.add(ticker);
    processedTradeCount += 1;
    totalAmountFloor += amountFloor;
    if (publishedDate > latestDisclosureDate) latestDisclosureDate = publishedDate;
  }

  const qualifyingPlays = [...plays.values()].filter(
    (play) => play.actors.size >= MIN_CLUSTER_ACTORS,
  );

  const topClusters: CongressClusterPlay[] = qualifyingPlays
    .sort((left, right) => {
      if (right.actors.size !== left.actors.size) return right.actors.size - left.actors.size;
      if (right.amountFloor !== left.amountFloor) return right.amountFloor - left.amountFloor;
      if (right.tradeCount !== left.tradeCount) return right.tradeCount - left.tradeCount;
      const freshnessDelta = right.latestDisclosureDate.localeCompare(left.latestDisclosureDate);
      return freshnessDelta || left.ticker.localeCompare(right.ticker);
    })
    .map((play) => ({
      ticker: play.ticker,
      companyName: preferredCompanyName(play.companyNames),
      actorCount: play.actors.size,
      lawmakersSharePct: allActors.size
        ? Math.round((play.actors.size / allActors.size) * 100)
        : 0,
      tradeCount: play.tradeCount,
      amountFloor: play.amountFloor,
      latestTransactionDate: play.latestTransactionDate,
      latestDisclosureDate: play.latestDisclosureDate,
      conviction: play.actors.size >= HIGH_CONVICTION_ACTORS ? 'high' : 'building',
    }));

  return {
    range,
    latestDisclosureDate: latestDisclosureDate || null,
    topClusters,
    totals: {
      actorCount: allActors.size,
      clusterCount: qualifyingPlays.length,
      tradeCount: processedTradeCount,
      tickerCount: allTickers.size,
      amountFloor: totalAmountFloor,
    },
  };
}

const loadCachedCongressClusterCalendar = unstable_cache(
  async (range: CongressClusterRange) => {
    const { rangeStart, rangeEnd } = rangeDates(range);
    const rows = await loadCongressBuys(rangeStart, rangeEnd);
    return buildAccumulationData(range, rows);
  },
  ['congress-cluster-accumulation-v4'],
  { revalidate: 2 * 60 },
);

export function getCongressClusterCalendar(range: CongressClusterRange) {
  return loadCachedCongressClusterCalendar(range);
}

import 'server-only';

import { unstable_cache } from 'next/cache';

import type {
  CongressClusterCalendarData,
  CongressClusterDay,
  CongressClusterPlay,
  CongressClusterRange,
} from '@/lib/congress-cluster-calendar-types';
import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import { stripPoliticianOptionMetadata } from '@/lib/politician-option-trades';
import { getPublicSupabase } from '@/lib/supabase-server';

const RANGE_DAYS: Record<CongressClusterRange, number> = {
  week: 7,
  month: 30,
  year: 365,
};
const PAGE_SIZE = 1_000;
const MAX_ROWS = 20_000;
const MIN_CLUSTER_ACTORS = 2;
const HIGH_CONVICTION_ACTORS = 4;

type CongressBuyRow = {
  id: string;
  member_id: string | null;
  politician_name: string | null;
  ticker: string | null;
  asset_name: string | null;
  transaction_type: string | null;
  amount_range: string | null;
  published_date: string | null;
  transaction_date: string | null;
};

type MutablePlay = {
  ticker: string;
  companyNames: Map<string, number>;
  actors: Map<string, string>;
  tradeCount: number;
  amountFloor: number;
  latestDisclosureDate: string;
};

type MutableDay = {
  actors: Set<string>;
  tickers: Map<string, { actors: Set<string>; tradeCount: number }>;
  tradeCount: number;
  amountFloor: number;
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
  return {
    rangeEnd,
    rangeStart: addUtcDays(rangeEnd, -(RANGE_DAYS[range] - 1)),
  };
}

function actorIdentity(row: CongressBuyRow) {
  const memberId = String(row.member_id || '').trim().toLowerCase();
  const politicianName = String(row.politician_name || '').replace(/[,\s]+$/g, '').trim();
  return {
    key: memberId || politicianName.toLowerCase(),
    name: politicianName || 'Member of Congress',
  };
}

function normalizedCompanyName(row: CongressBuyRow) {
  const name = stripPoliticianOptionMetadata(row.asset_name)
    .replace(/\s+-\s+(?:Class\s+[A-Z]\s+)?Common\s+Stock.*$/i, '')
    .replace(/\s+(?:Class\s+[A-Z]\s+)?Common\s+Stock.*$/i, '')
    .replace(/\s+-\s+Class\s+[A-Z]\s+Capital\s+Stock.*$/i, '')
    .replace(/\s+(?:Class\s+[A-Z]\s+)?Ordinary\s+Shares?.*$/i, '')
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

async function loadCongressBuys(rangeStart: string, rangeEnd: string): Promise<CongressBuyRow[]> {
  const supabase = getPublicSupabase();
  const rows: CongressBuyRow[] = [];

  for (let offset = 0; offset < MAX_ROWS; offset += PAGE_SIZE) {
    const { data, error } = await supabase
      .from('politician_trades')
      .select(`
        id,
        member_id,
        politician_name,
        ticker,
        asset_name,
        transaction_type,
        amount_range,
        published_date,
        transaction_date
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

    const page = (data || []) as CongressBuyRow[];
    rows.push(...page);
    if (page.length < PAGE_SIZE) {
      break;
    }
  }

  return rows;
}

function buildCalendarData(
  range: CongressClusterRange,
  rangeStart: string,
  rangeEnd: string,
  rows: CongressBuyRow[],
): CongressClusterCalendarData {
  const plays = new Map<string, MutablePlay>();
  const daily = new Map<string, MutableDay>();
  const allActors = new Set<string>();
  const allTickers = new Set<string>();
  let processedTradeCount = 0;
  let totalAmountFloor = 0;

  for (const row of rows) {
    const ticker = String(row.ticker || '').trim().toUpperCase();
    const publishedDate = String(row.published_date || '').slice(0, 10);
    const actor = actorIdentity(row);
    if (!ticker || !publishedDate || !actor.key) {
      continue;
    }

    const amountFloor = parsePoliticianAmountRange(row.amount_range)?.min || 0;
    const companyName = normalizedCompanyName(row);
    const play = plays.get(ticker) || {
      ticker,
      companyNames: new Map<string, number>(),
      actors: new Map<string, string>(),
      tradeCount: 0,
      amountFloor: 0,
      latestDisclosureDate: publishedDate,
    };
    play.actors.set(actor.key, actor.name);
    play.tradeCount += 1;
    play.amountFloor += amountFloor;
    play.latestDisclosureDate =
      publishedDate > play.latestDisclosureDate ? publishedDate : play.latestDisclosureDate;
    if (companyName) {
      play.companyNames.set(companyName, (play.companyNames.get(companyName) || 0) + 1);
    }
    plays.set(ticker, play);

    const day = daily.get(publishedDate) || {
      actors: new Set<string>(),
      tickers: new Map<string, { actors: Set<string>; tradeCount: number }>(),
      tradeCount: 0,
      amountFloor: 0,
    };
    day.actors.add(actor.key);
    const dayTicker = day.tickers.get(ticker) || { actors: new Set<string>(), tradeCount: 0 };
    dayTicker.actors.add(actor.key);
    dayTicker.tradeCount += 1;
    day.tickers.set(ticker, dayTicker);
    day.tradeCount += 1;
    day.amountFloor += amountFloor;
    daily.set(publishedDate, day);

    allActors.add(actor.key);
    allTickers.add(ticker);
    processedTradeCount += 1;
    totalAmountFloor += amountFloor;
  }

  const topClusters: CongressClusterPlay[] = [...plays.values()]
    .filter((play) => play.actors.size >= MIN_CLUSTER_ACTORS)
    .sort((left, right) => {
      if (right.actors.size !== left.actors.size) return right.actors.size - left.actors.size;
      if (right.amountFloor !== left.amountFloor) return right.amountFloor - left.amountFloor;
      if (right.tradeCount !== left.tradeCount) return right.tradeCount - left.tradeCount;
      return right.latestDisclosureDate.localeCompare(left.latestDisclosureDate);
    })
    .slice(0, 5)
    .map((play) => ({
      ticker: play.ticker,
      companyName: preferredCompanyName(play.companyNames),
      actorCount: play.actors.size,
      tradeCount: play.tradeCount,
      amountFloor: play.amountFloor,
      latestDisclosureDate: play.latestDisclosureDate,
      politicianNames: [...play.actors.values()].sort().slice(0, 4),
      conviction: play.actors.size >= HIGH_CONVICTION_ACTORS ? 'high' : 'building',
    }));

  const days: CongressClusterDay[] = [];
  for (let date = rangeStart; date <= rangeEnd; date = addUtcDays(date, 1)) {
    const value = daily.get(date);
    const coordinatedTickers = value
      ? [...value.tickers.entries()].filter(([, ticker]) => ticker.actors.size >= MIN_CLUSTER_ACTORS)
      : [];
    const topTicker = coordinatedTickers.length
      ? coordinatedTickers
          .sort((left, right) =>
            right[1].actors.size - left[1].actors.size ||
            right[1].tradeCount - left[1].tradeCount ||
            left[0].localeCompare(right[0])
          )[0]?.[0] || null
      : null;
    days.push({
      date,
      actorCount: value?.actors.size || 0,
      clusterCount: coordinatedTickers.length,
      tradeCount: value?.tradeCount || 0,
      tickerCount: value?.tickers.size || 0,
      amountFloor: value?.amountFloor || 0,
      topTicker,
    });
  }

  return {
    range,
    rangeStart,
    rangeEnd,
    latestDisclosureDate: rows[0]?.published_date?.slice(0, 10) || null,
    generatedAt: new Date().toISOString(),
    topClusters,
    days,
    totals: {
      actorCount: allActors.size,
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
    return buildCalendarData(range, rangeStart, rangeEnd, rows);
  },
  ['congress-cluster-calendar-v4'],
  { revalidate: 2 * 60 },
);

export function getCongressClusterCalendar(range: CongressClusterRange) {
  return loadCachedCongressClusterCalendar(range);
}

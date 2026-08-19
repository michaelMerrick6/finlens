import 'server-only';

import { unstable_cache } from 'next/cache';

import {
  actorKey,
  economicTradeKey,
  isEquityPurchase,
  normalizedCompanyName,
  normalizedTransactionDate,
  preferredCompanyName,
  type CongressBuyDetailRow,
} from '@/lib/congress-cluster-calendar-server';
import type {
  CongressBuyingCalendarData,
  CongressBuyingTransaction,
  CongressCalendarCompany,
  CongressCalendarDay,
  CongressCalendarTransactionsData,
} from '@/lib/congress-cluster-calendar-types';
import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import { stripPoliticianOptionMetadata } from '@/lib/politician-option-trades';
import { filterProductPoliticianTrades } from '@/lib/politician-trade-scope';
import { getPublicSupabase } from '@/lib/supabase-server';

const PAGE_SIZE = 1_000;
const MAX_ROWS = 20_000;
const VALID_MONTH = /^\d{4}-(0[1-9]|1[0-2])$/;

type CalendarBuyRow = CongressBuyDetailRow & {
  doc_id?: string | null;
};

type MutableCompany = {
  ticker: string;
  names: Map<string, number>;
  actors: Set<string>;
  tradeCount: number;
  amountFloor: number;
};

type MutableDay = {
  date: string;
  actors: Set<string>;
  tradeCount: number;
  amountFloor: number;
  companies: Map<string, MutableCompany>;
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

export function currentCongressCalendarMonth() {
  return currentPacificDate().slice(0, 7);
}

export function normalizeCongressCalendarMonth(value: string | null | undefined) {
  const normalized = String(value || '').trim();
  if (!VALID_MONTH.test(normalized)) return currentCongressCalendarMonth();
  if (normalized < '2015-01' || normalized > currentCongressCalendarMonth()) {
    return currentCongressCalendarMonth();
  }
  return normalized;
}

function addUtcDays(value: string, days: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return toIsoDate(date);
}

function calendarBounds(month: string) {
  const first = new Date(`${month}-01T12:00:00Z`);
  const last = new Date(Date.UTC(first.getUTCFullYear(), first.getUTCMonth() + 1, 0, 12));
  const mondayOffset = (first.getUTCDay() + 6) % 7;
  const sundayOffset = (7 - last.getUTCDay()) % 7;
  return {
    start: addUtcDays(toIsoDate(first), -mondayOffset),
    end: addUtcDays(toIsoDate(last), sundayOffset),
  };
}

function normalizedTicker(value: string | null | undefined) {
  const ticker = String(value || '').trim().toUpperCase();
  if (!ticker || ['N/A', 'NA', 'UNKNOWN', 'MULTI', 'US-TREAS'].includes(ticker)) return null;
  return /^[A-Z][A-Z0-9.-]{0,9}$/.test(ticker) ? ticker : null;
}

async function loadCalendarRows(start: string, end: string) {
  const supabase = getPublicSupabase();
  const rows: CalendarBuyRow[] = [];

  for (let offset = 0; offset < MAX_ROWS; offset += PAGE_SIZE) {
    const { data, error } = await supabase
      .from('politician_trades')
      .select(`
        id,
        member_id,
        politician_name,
        chamber,
        party,
        ticker,
        asset_name,
        asset_type,
        amount_range,
        transaction_date,
        published_date,
        source_url,
        doc_id
      `)
      .gte('transaction_date', start)
      .lte('transaction_date', end)
      .lte('published_date', currentPacificDate())
      .not('ticker', 'is', null)
      .neq('ticker', '')
      .or('transaction_type.ilike.buy%,transaction_type.ilike.purchase%')
      .order('transaction_date', { ascending: false })
      .order('published_date', { ascending: false })
      .order('id', { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(error.message);
    const page = (data || []) as CalendarBuyRow[];
    rows.push(...page);
    if (page.length < PAGE_SIZE) break;
  }

  return filterProductPoliticianTrades(rows);
}

function companySummary(company: MutableCompany): CongressCalendarCompany {
  return {
    ticker: company.ticker,
    companyName: preferredCompanyName(company.names),
    actorCount: company.actors.size,
    tradeCount: company.tradeCount,
    amountFloor: company.amountFloor,
  };
}

function buildCalendarData(month: string, rows: CalendarBuyRow[]): CongressBuyingCalendarData {
  const bounds = calendarBounds(month);
  const days = new Map<string, MutableDay>();
  const economicTrades = new Set<string>();
  const monthActors = new Set<string>();
  const monthCompanies = new Set<string>();
  let monthTradeCount = 0;
  let monthAmountFloor = 0;
  let latestTransactionDate = '';
  let latestDisclosureDate = '';

  for (const row of rows) {
    const ticker = normalizedTicker(row.ticker);
    const publishedDate = String(row.published_date || '').slice(0, 10);
    const politicianKey = actorKey(row);
    const transactionDate = normalizedTransactionDate(row, publishedDate);
    if (
      !ticker ||
      !publishedDate ||
      !politicianKey ||
      !transactionDate ||
      transactionDate < bounds.start ||
      transactionDate > bounds.end ||
      !isEquityPurchase(row)
    ) {
      continue;
    }

    const tradeKey = economicTradeKey(row, ticker, politicianKey);
    if (economicTrades.has(tradeKey)) continue;
    economicTrades.add(tradeKey);

    const amountFloor = parsePoliticianAmountRange(row.amount_range)?.min || 0;
    const name = normalizedCompanyName(row);
    const day = days.get(transactionDate) || {
      date: transactionDate,
      actors: new Set<string>(),
      tradeCount: 0,
      amountFloor: 0,
      companies: new Map<string, MutableCompany>(),
    };
    const company = day.companies.get(ticker) || {
      ticker,
      names: new Map<string, number>(),
      actors: new Set<string>(),
      tradeCount: 0,
      amountFloor: 0,
    };

    day.actors.add(politicianKey);
    day.tradeCount += 1;
    day.amountFloor += amountFloor;
    company.actors.add(politicianKey);
    company.tradeCount += 1;
    company.amountFloor += amountFloor;
    if (name) company.names.set(name, (company.names.get(name) || 0) + 1);
    day.companies.set(ticker, company);
    days.set(transactionDate, day);

    if (transactionDate.startsWith(month)) {
      monthActors.add(politicianKey);
      monthCompanies.add(ticker);
      monthTradeCount += 1;
      monthAmountFloor += amountFloor;
    }
    if (transactionDate > latestTransactionDate) latestTransactionDate = transactionDate;
    if (publishedDate > latestDisclosureDate) latestDisclosureDate = publishedDate;
  }

  const calendarDays: CongressCalendarDay[] = [...days.values()]
    .sort((left, right) => left.date.localeCompare(right.date))
    .map((day) => ({
      date: day.date,
      actorCount: day.actors.size,
      tradeCount: day.tradeCount,
      amountFloor: day.amountFloor,
      companies: [...day.companies.values()]
        .sort((left, right) =>
          right.amountFloor - left.amountFloor ||
          right.actors.size - left.actors.size ||
          left.ticker.localeCompare(right.ticker),
        )
        .map(companySummary),
    }));

  return {
    month,
    calendarStart: bounds.start,
    calendarEnd: bounds.end,
    latestTransactionDate: latestTransactionDate || null,
    latestDisclosureDate: latestDisclosureDate || null,
    days: calendarDays,
    totals: {
      actorCount: monthActors.size,
      companyCount: monthCompanies.size,
      tradeCount: monthTradeCount,
      amountFloor: monthAmountFloor,
    },
  };
}

const loadCachedCongressBuyingCalendar = unstable_cache(
  async (month: string) => {
    const bounds = calendarBounds(month);
    const rows = await loadCalendarRows(bounds.start, bounds.end);
    return buildCalendarData(month, rows);
  },
  ['congress-buying-calendar-v1'],
  { revalidate: 2 * 60 },
);

export function getCongressBuyingCalendar(inputMonth: string | null | undefined) {
  const month = normalizeCongressCalendarMonth(inputMonth);
  return loadCachedCongressBuyingCalendar(month);
}

function buildCalendarTransactions(date: string, ticker: string, rows: CalendarBuyRow[]) {
  const economicTrades = new Set<string>();
  const actors = new Set<string>();
  const transactions: CongressBuyingTransaction[] = [];
  let amountFloor = 0;

  for (const row of rows) {
    const publishedDate = String(row.published_date || '').slice(0, 10);
    const politicianKey = actorKey(row);
    if (
      normalizedTicker(row.ticker) !== ticker ||
      normalizedTransactionDate(row, publishedDate) !== date ||
      !politicianKey ||
      !isEquityPurchase(row)
    ) {
      continue;
    }

    const tradeKey = economicTradeKey(row, ticker, politicianKey);
    if (economicTrades.has(tradeKey)) continue;
    economicTrades.add(tradeKey);

    const transactionAmountFloor = parsePoliticianAmountRange(row.amount_range)?.min || 0;
    amountFloor += transactionAmountFloor;
    actors.add(politicianKey);
    transactions.push({
      id: row.id,
      memberId: String(row.member_id || '').trim() || null,
      politicianName: String(row.politician_name || '').replace(/[,\s]+$/g, '').trim() || 'Member of Congress',
      chamber: String(row.chamber || '').trim() || null,
      party: String(row.party || '').trim() || null,
      assetName: stripPoliticianOptionMetadata(row.asset_name).replace(/\s+/g, ' ').trim() || null,
      transactionDate: date,
      publishedDate,
      amountRange: String(row.amount_range || '').trim() || null,
      amountFloor: transactionAmountFloor,
      sourceUrl: /^https?:\/\//i.test(String(row.source_url || '').trim()) ? String(row.source_url).trim() : null,
    });
  }

  transactions.sort((left, right) =>
    right.publishedDate.localeCompare(left.publishedDate) || left.politicianName.localeCompare(right.politicianName),
  );

  const data: CongressCalendarTransactionsData = {
    date,
    ticker,
    transactions,
    totals: {
      actorCount: actors.size,
      tradeCount: transactions.length,
      amountFloor,
    },
  };
  return data;
}

const loadCachedCongressCalendarTransactions = unstable_cache(
  async (date: string, ticker: string) => {
    const rows = await loadCalendarRows(date, date);
    return buildCalendarTransactions(date, ticker, rows);
  },
  ['congress-buying-calendar-transactions-v1'],
  { revalidate: 2 * 60 },
);

export function getCongressCalendarTransactions(date: string, inputTicker: string) {
  const ticker = normalizedTicker(inputTicker);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !ticker) return null;
  return loadCachedCongressCalendarTransactions(date, ticker);
}

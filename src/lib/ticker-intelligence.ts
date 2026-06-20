import 'server-only';

import { unstable_cache } from 'next/cache';

import { enrichInsiderTransactionsWithPositions } from '@/lib/form4-position-context';
import { formatFundChangeLabel, getFundChangeKind } from '@/lib/fund-holdings';
import { normalizeInsiderDirection } from '@/lib/insider-trades';
import { getMarketPriceSeries } from '@/lib/market-data';
import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import { normalizeProfileDate, normalizeProfileDirection } from '@/lib/politician-profile';
import { filterProductPoliticianTrades } from '@/lib/politician-trade-scope';
import { getPublicSupabase } from '@/lib/supabase-server';
import type {
  TickerFundHolder,
  TickerFundSummary,
  TickerInsiderHolding,
  TickerInsiderTransaction,
  TickerInsiderWindow,
  TickerIntelligencePayload,
  TickerOverview,
  TickerPoliticianHolder,
  TickerPoliticianTransaction,
  TickerPoliticianTransactionsPage,
} from '@/lib/ticker-intelligence-types';

export const TICKER_INTELLIGENCE_REVALIDATE = 60;
export const TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE = 25;

const FETCH_CHUNK_SIZE = 1000;
const MAX_ROWS_PER_SOURCE = 2000;
const MAX_POLITICIAN_TRANSACTION_PAGE_SIZE = 100;
const INSIDER_POSITION_ENRICHMENT_LIMIT = 32;
const VALID_TICKER_PATTERN = /^[A-Z][A-Z0-9.-]{0,9}$/;
const INSIDER_WINDOWS = [
  { key: '30d', label: '30D', days: 30 },
  { key: '90d', label: '90D', days: 90 },
  { key: '180d', label: '6M', days: 180 },
  { key: '365d', label: '12M', days: 365 },
  { key: 'all', label: 'All', days: null },
];

type PoliticianTradeRow = {
  id: string;
  member_id?: string | null;
  politician_name?: string | null;
  chamber?: string | null;
  party?: string | null;
  ticker?: string | null;
  transaction_date?: string | null;
  published_date?: string | null;
  transaction_type?: string | null;
  amount_range?: string | null;
  source_url?: string | null;
  doc_id?: string | null;
  asset_name?: string | null;
};

type InsiderTradeRow = {
  id: string;
  filer_name?: string | null;
  filer_relation?: string | null;
  transaction_code?: string | null;
  transaction_date?: string | null;
  published_date?: string | null;
  amount?: number | null;
  price?: number | null;
  value?: number | null;
  source_url?: string | null;
};

type FundHoldingRow = {
  id: string;
  fund_name?: string | null;
  published_date?: string | null;
  report_period?: string | null;
  qoq_change_shares?: number | null;
  qoq_change_percent?: number | null;
  shares_held?: number | null;
  value_held?: number | null;
  source_url?: string | null;
};

type CompanyRow = {
  name?: string | null;
  sector?: string | null;
  industry?: string | null;
};

function normalizeText(value: string | null | undefined) {
  return String(value || '').trim();
}

function normalizeTickerSymbol(value: string | null | undefined) {
  const symbol = normalizeText(value).toUpperCase();
  return symbol && VALID_TICKER_PATTERN.test(symbol) ? symbol : null;
}

function clampPositiveInteger(value: number | string | null | undefined, fallback: number, max: number) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return fallback;
  }
  return Math.min(Math.floor(numeric), max);
}

function clampOffset(value: number | string | null | undefined) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return 0;
  }
  return Math.floor(numeric);
}

function asPositiveNumber(value: number | string | null | undefined) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
}

function latestKnownDate(...values: Array<string | null | undefined>) {
  const normalized = values
    .map((value) => normalizeProfileDate(value))
    .filter((value): value is string => Boolean(value))
    .sort()
    .reverse();
  return normalized[0] || null;
}

function sortDescByDate<T>(items: T[], getDate: (item: T) => string | null) {
  return [...items].sort((left, right) => {
    const leftDate = getDate(left) || '';
    const rightDate = getDate(right) || '';
    return rightDate.localeCompare(leftDate);
  });
}

function shiftIsoDate(value: string, daysBack: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() - daysBack);
  return date.toISOString().slice(0, 10);
}

function isOnOrAfter(value: string | null | undefined, cutoffIso: string) {
  const normalized = normalizeProfileDate(value);
  return Boolean(normalized && normalized >= cutoffIso);
}

function formatFundPositionLabel(kind: TickerFundHolder['changeKind'], rawLabel: string) {
  if (kind === 'new') return 'New position';
  if (kind === 'increase') return `Increase ${rawLabel}`;
  if (kind === 'decrease') return `Decrease ${rawLabel}`;
  if (kind === 'exit') return 'Exited';
  if (kind === 'hold') return 'Flat';
  return 'Unknown';
}

async function fetchAllTickerRows<T>({
  supabase,
  table,
  columns,
  ticker,
  orderColumn,
  maxRows = MAX_ROWS_PER_SOURCE,
}: {
  supabase: ReturnType<typeof getPublicSupabase>;
  table: 'politician_trades' | 'insider_trades' | 'institutional_holdings';
  columns: string;
  ticker: string;
  orderColumn: string;
  maxRows?: number;
}) {
  const rows: T[] = [];
  let offset = 0;

  while (rows.length < maxRows) {
    const remainingRows = maxRows - rows.length;
    const batchSize = Math.min(FETCH_CHUNK_SIZE, remainingRows);
    const { data, error } = await supabase
      .from(table)
      .select(columns)
      .eq('ticker', ticker)
      .order(orderColumn, { ascending: false })
      .range(offset, offset + batchSize - 1);

    if (error) {
      throw error;
    }

    const batch = (data || []) as T[];
    rows.push(...batch);

    if (batch.length < batchSize) {
      break;
    }

    offset += batch.length;
  }

  return rows;
}

function buildPoliticianHolderEstimates(trades: PoliticianTradeRow[]): TickerPoliticianHolder[] {
  const sortedTrades = [...trades].sort((left, right) => {
    const leftDate = latestKnownDate(left.transaction_date, left.published_date) || '';
    const rightDate = latestKnownDate(right.transaction_date, right.published_date) || '';
    if (leftDate !== rightDate) {
      return leftDate.localeCompare(rightDate);
    }
    return String(left.id).localeCompare(String(right.id));
  });

  const holders = new Map<
    string,
    {
      key: string;
      memberId: string | null;
      name: string;
      party: string | null;
      chamber: string | null;
      minValue: number;
      midValue: number;
      maxValue: number;
      tradeCount: number;
      lastTradeDate: string | null;
    }
  >();

  for (const trade of sortedTrades) {
    const memberId = normalizeText(trade.member_id) || null;
    const name = normalizeText(trade.politician_name) || 'Unknown member';
    const key = memberId || name.toLowerCase();
    const tradeDate = latestKnownDate(trade.transaction_date, trade.published_date);
    const accumulator = holders.get(key) || {
      key,
      memberId,
      name,
      party: normalizeText(trade.party) || null,
      chamber: normalizeText(trade.chamber) || null,
      minValue: 0,
      midValue: 0,
      maxValue: 0,
      tradeCount: 0,
      lastTradeDate: null,
    };

    accumulator.tradeCount += 1;
    if (!accumulator.lastTradeDate || (tradeDate && tradeDate > accumulator.lastTradeDate)) {
      accumulator.lastTradeDate = tradeDate;
    }

    const direction = normalizeProfileDirection(trade.transaction_type);
    const amountRange = parsePoliticianAmountRange(trade.amount_range);

    if (amountRange && direction === 'buy') {
      accumulator.minValue += amountRange.min;
      accumulator.midValue += amountRange.estimated;
      accumulator.maxValue += amountRange.max;
    } else if (amountRange && direction === 'sell') {
      accumulator.minValue = Math.max(0, accumulator.minValue - amountRange.max);
      accumulator.midValue = Math.max(0, accumulator.midValue - amountRange.estimated);
      accumulator.maxValue = Math.max(0, accumulator.maxValue - amountRange.min);
    }

    holders.set(key, accumulator);
  }

  return [...holders.values()]
    .filter((holder) => holder.maxValue > 0)
    .sort((left, right) => {
      if (right.midValue !== left.midValue) {
        return right.midValue - left.midValue;
      }
      return (right.lastTradeDate || '').localeCompare(left.lastTradeDate || '');
    });
}

function buildPoliticianTransactions(
  trades: PoliticianTradeRow[],
  {
    offset = 0,
    limit = TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE,
  }: {
    offset?: number;
    limit?: number;
  } = {},
): TickerPoliticianTransaction[] {
  return sortDescByDate(trades, (trade) => latestKnownDate(trade.transaction_date, trade.published_date))
    .slice(offset, offset + limit)
    .map((trade) => ({
      id: trade.id,
      memberId: normalizeText(trade.member_id) || null,
      name: normalizeText(trade.politician_name) || 'Unknown member',
      party: normalizeText(trade.party) || null,
      chamber: normalizeText(trade.chamber) || null,
      transactionType: normalizeText(trade.transaction_type) || 'Other',
      amountRange: normalizeText(trade.amount_range) || null,
      transactionDate: normalizeProfileDate(trade.transaction_date),
      publishedDate: normalizeProfileDate(trade.published_date),
      sourceUrl: normalizeText(trade.source_url) || null,
    }));
}

function tradeValue(trade: InsiderTradeRow) {
  const directValue = asPositiveNumber(trade.value);
  if (directValue > 0) {
    return directValue;
  }

  const amount = asPositiveNumber(trade.amount);
  const price = asPositiveNumber(trade.price);
  return amount > 0 && price > 0 ? amount * price : 0;
}

function buildInsiderWindows(trades: InsiderTradeRow[]): TickerInsiderWindow[] {
  const todayIso = new Date().toISOString().slice(0, 10);

  return INSIDER_WINDOWS.map((window) => {
    const filtered =
      typeof window.days === 'number'
        ? trades.filter((trade) =>
            isOnOrAfter(trade.transaction_date || trade.published_date, shiftIsoDate(todayIso, window.days)),
          )
        : trades;

    let buyValue = 0;
    let sellValue = 0;
    let buyCount = 0;
    let sellCount = 0;

    for (const trade of filtered) {
      const direction = normalizeInsiderDirection(trade.transaction_code);
      const value = tradeValue(trade);
      if (direction === 'buy') {
        buyValue += value;
        buyCount += 1;
      } else if (direction === 'sell') {
        sellValue += value;
        sellCount += 1;
      }
    }

    const totalValue = buyValue + sellValue;
    const totalCount = buyCount + sellCount;
    const buyRatio = totalValue > 0 ? buyValue / totalValue : totalCount > 0 ? buyCount / totalCount : 0.5;
    const imbalance = buyRatio * 2 - 1;
    const tone = imbalance > 0.15 ? 'bullish' : imbalance < -0.15 ? 'bearish' : 'neutral';

    return {
      key: window.key,
      label: window.label,
      days: window.days,
      transactionCount: filtered.length,
      buyValue,
      sellValue,
      buyCount,
      sellCount,
      totalValue,
      netValue: buyValue - sellValue,
      buyRatio,
      tone,
    };
  });
}

function buildInsiderTransactions(trades: InsiderTradeRow[]): TickerInsiderTransaction[] {
  return sortDescByDate(trades, (trade) => latestKnownDate(trade.transaction_date, trade.published_date))
    .map((trade) => ({
      id: trade.id,
      identityKey: insiderIdentityKey({
        filerName: normalizeText(trade.filer_name) || 'Unknown insider',
        filerRelation: normalizeText(trade.filer_relation) || null,
      }),
      filerName: normalizeText(trade.filer_name) || 'Unknown insider',
      filerRelation: normalizeText(trade.filer_relation) || null,
      direction: normalizeInsiderDirection(trade.transaction_code),
      transactionCode: normalizeText(trade.transaction_code) || null,
      transactionDate: normalizeProfileDate(trade.transaction_date),
      publishedDate: normalizeProfileDate(trade.published_date),
      value: tradeValue(trade),
      amount: Number.isFinite(Number(trade.amount)) ? Number(trade.amount) : null,
      price: Number.isFinite(Number(trade.price)) ? Number(trade.price) : null,
      sourceUrl: normalizeText(trade.source_url) || null,
      sharesOwnedAfterTransaction: null,
      sharesOwnedBeforeTransaction: null,
      holdingChangePct: null,
    }));
}

function insiderIdentityKey({
  filerName,
  filerRelation,
}: Pick<TickerInsiderTransaction, 'filerName' | 'filerRelation'>) {
  const nameKey = normalizeText(filerName).toLowerCase();
  const relationKey = normalizeText(filerRelation).toLowerCase();
  return relationKey ? `${nameKey}::${relationKey}` : nameKey;
}

function selectLatestInsiderTransactions(transactions: TickerInsiderTransaction[]) {
  const latestByInsider = new Map<string, TickerInsiderTransaction>();

  for (const trade of transactions) {
    const key = insiderIdentityKey(trade);
    if (!latestByInsider.has(key)) {
      latestByInsider.set(key, trade);
    }
  }

  return [...latestByInsider.values()];
}

function buildInsiderHoldings(
  transactions: TickerInsiderTransaction[],
  currentPrice: number | null,
): TickerInsiderHolding[] {
  const price = Number.isFinite(Number(currentPrice)) && Number(currentPrice) > 0 ? Number(currentPrice) : null;

  return selectLatestInsiderTransactions(transactions)
    .filter((trade) => Number(trade.sharesOwnedAfterTransaction || 0) > 0)
    .map((trade) => ({
      key: insiderIdentityKey(trade),
      filerName: trade.filerName,
      filerRelation: trade.filerRelation,
      sharesHeld: Number(trade.sharesOwnedAfterTransaction || 0),
      estimatedValue: price ? Number(trade.sharesOwnedAfterTransaction || 0) * price : null,
      lastTransactionDate: trade.transactionDate,
      publishedDate: trade.publishedDate,
      sourceUrl: trade.sourceUrl,
      lastDirection: trade.direction,
      holdingChangePct: trade.holdingChangePct,
    }))
    .sort((left, right) => {
      if (right.sharesHeld !== left.sharesHeld) {
        return right.sharesHeld - left.sharesHeld;
      }

      return (right.lastTransactionDate || '').localeCompare(left.lastTransactionDate || '');
    });
}

function buildLatestFundHolders(holdings: FundHoldingRow[]) {
  const sorted = [...holdings].sort((left, right) => {
    const leftDate = latestKnownDate(left.report_period, left.published_date) || '';
    const rightDate = latestKnownDate(right.report_period, right.published_date) || '';
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }
    return normalizeText(right.fund_name).localeCompare(normalizeText(left.fund_name));
  });

  const latestByFund = new Map<string, FundHoldingRow>();
  for (const holding of sorted) {
    const fundName = normalizeText(holding.fund_name);
    if (!fundName || latestByFund.has(fundName)) {
      continue;
    }
    latestByFund.set(fundName, holding);
  }

  const activeHolders: TickerFundHolder[] = [...latestByFund.values()]
    .filter((holding) => asPositiveNumber(holding.shares_held) > 0)
    .map((holding) => {
      const kind = getFundChangeKind(holding);
      return {
        key: normalizeText(holding.fund_name).toLowerCase(),
        fundName: normalizeText(holding.fund_name) || 'Unnamed fund',
        reportPeriod: normalizeProfileDate(holding.report_period),
        publishedDate: normalizeProfileDate(holding.published_date),
        sharesHeld: asPositiveNumber(holding.shares_held),
        valueHeld: asPositiveNumber(holding.value_held),
        changeKind: kind,
        changeLabel: formatFundPositionLabel(kind, formatFundChangeLabel(holding)),
        sourceUrl: normalizeText(holding.source_url) || null,
      };
    })
    .sort((left, right) => {
      if (right.valueHeld !== left.valueHeld) {
        return right.valueHeld - left.valueHeld;
      }
      return right.sharesHeld - left.sharesHeld;
    });

  const summary: TickerFundSummary = {
    increased: activeHolders.filter((holder) => holder.changeKind === 'new' || holder.changeKind === 'increase').length,
    decreased: activeHolders.filter((holder) => holder.changeKind === 'decrease').length,
    neutral: activeHolders.filter((holder) => holder.changeKind === 'hold' || holder.changeKind === 'unknown').length,
  };

  return { activeHolders, summary };
}

async function loadTickerIntelligence(symbol: string): Promise<TickerIntelligencePayload | null> {
  const supabase = getPublicSupabase();

  const [companyResult, politicianRows, insiderRows, fundRows] = await Promise.all([
    supabase.from('companies').select('name,sector,industry').eq('ticker', symbol).maybeSingle(),
    fetchAllTickerRows<PoliticianTradeRow>({
      supabase,
      table: 'politician_trades',
      columns:
        'id,member_id,politician_name,chamber,party,ticker,transaction_date,published_date,transaction_type,amount_range,source_url,doc_id,asset_name',
      ticker: symbol,
      orderColumn: 'transaction_date',
    }),
    fetchAllTickerRows<InsiderTradeRow>({
      supabase,
      table: 'insider_trades',
      columns:
        'id,filer_name,filer_relation,transaction_code,transaction_date,published_date,amount,price,value,source_url',
      ticker: symbol,
      orderColumn: 'transaction_date',
    }),
    fetchAllTickerRows<FundHoldingRow>({
      supabase,
      table: 'institutional_holdings',
      columns:
        'id,fund_name,published_date,report_period,qoq_change_shares,qoq_change_percent,shares_held,value_held,source_url',
      ticker: symbol,
      orderColumn: 'report_period',
    }),
  ]);

  const company = (companyResult.data || null) as CompanyRow | null;
  const politicianTrades = filterProductPoliticianTrades(politicianRows);
  const insiderTrades = insiderRows;
  const fundHoldings = fundRows;

  if (!company && !politicianTrades.length && !insiderTrades.length && !fundHoldings.length) {
    return null;
  }

  const latestActivityDate = latestKnownDate(
    politicianTrades[0]?.transaction_date,
    politicianTrades[0]?.published_date,
    insiderTrades[0]?.transaction_date,
    insiderTrades[0]?.published_date,
    fundHoldings[0]?.report_period,
    fundHoldings[0]?.published_date,
  );

  const marketSeries = await getMarketPriceSeries(
    symbol,
    latestActivityDate ? shiftIsoDate(latestActivityDate, 365) : null,
  );

  const politicianHolders = buildPoliticianHolderEstimates(politicianTrades);
  const politicianTransactions = buildPoliticianTransactions(politicianTrades);
  const insiderWindows = buildInsiderWindows(insiderTrades);
  const baseInsiderTransactions = buildInsiderTransactions(insiderTrades);
  const latestInsiderTransactions = selectLatestInsiderTransactions(baseInsiderTransactions);
  const insiderTransactions = await enrichInsiderTransactionsWithPositions(
    baseInsiderTransactions,
    INSIDER_POSITION_ENRICHMENT_LIMIT,
  );
  const insiderHoldings = buildInsiderHoldings(
    await enrichInsiderTransactionsWithPositions(
      latestInsiderTransactions,
      latestInsiderTransactions.length,
    ),
    marketSeries?.currentPrice || null,
  );
  const { activeHolders: hedgeFundHolders, summary: hedgeFundSummary } = buildLatestFundHolders(fundHoldings);

  const overview: TickerOverview = {
    symbol,
    companyName: normalizeText(company?.name) || symbol,
    sector: normalizeText(company?.sector) || null,
    industry: normalizeText(company?.industry) || null,
    currentPrice: marketSeries?.currentPrice || null,
    priceAsOf: marketSeries?.priceAsOf || null,
    latestActivityDate,
    sourceCount: Number(politicianTrades.length > 0) + Number(insiderTrades.length > 0) + Number(fundHoldings.length > 0),
    politicianHolderCount: politicianHolders.length,
    politicianTransactionCount: politicianTrades.length,
    insiderTransactionCount: insiderTrades.length,
    hedgeFundHolderCount: hedgeFundHolders.length,
  };

  return {
    overview,
    politicianHolders,
    politicianTransactions,
    insiderWindows,
    insiderTransactions,
    insiderHoldings,
    hedgeFundHolders,
    hedgeFundSummary,
  };
}

async function loadTickerPoliticianTransactions(
  symbol: string,
  {
    offset = 0,
    limit = TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE,
  }: {
    offset?: number;
    limit?: number;
  } = {},
): Promise<TickerPoliticianTransactionsPage> {
  const supabase = getPublicSupabase();
  const rows = await fetchAllTickerRows<PoliticianTradeRow>({
    supabase,
    table: 'politician_trades',
    columns:
      'id,member_id,politician_name,chamber,party,ticker,transaction_date,published_date,transaction_type,amount_range,source_url,doc_id,asset_name',
    ticker: symbol,
    orderColumn: 'transaction_date',
  });
  const trades = filterProductPoliticianTrades(rows);
  const normalizedOffset = clampOffset(offset);
  const normalizedLimit = clampPositiveInteger(
    limit,
    TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE,
    MAX_POLITICIAN_TRANSACTION_PAGE_SIZE,
  );
  const transactions = buildPoliticianTransactions(trades, {
    offset: normalizedOffset,
    limit: normalizedLimit,
  });
  const consumedCount = normalizedOffset + transactions.length;

  return {
    transactions,
    totalCount: trades.length,
    offset: normalizedOffset,
    limit: normalizedLimit,
    nextOffset: consumedCount < trades.length ? consumedCount : null,
  };
}

const loadCachedTickerIntelligence = unstable_cache(
  async (symbol: string) => loadTickerIntelligence(symbol),
  ['ticker-intelligence'],
  { revalidate: TICKER_INTELLIGENCE_REVALIDATE },
);

const loadCachedTickerPoliticianTransactions = unstable_cache(
  async (symbol: string, offset: number, limit: number) =>
    loadTickerPoliticianTransactions(symbol, { offset, limit }),
  ['ticker-politician-transactions'],
  { revalidate: TICKER_INTELLIGENCE_REVALIDATE },
);

export async function getTickerIntelligence(inputSymbol: string): Promise<TickerIntelligencePayload | null> {
  const symbol = normalizeTickerSymbol(inputSymbol);
  if (!symbol) {
    return null;
  }

  return loadCachedTickerIntelligence(symbol);
}

export async function getTickerPoliticianTransactions(
  inputSymbol: string,
  {
    offset = 0,
    limit = TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE,
  }: {
    offset?: number;
    limit?: number;
  } = {},
): Promise<TickerPoliticianTransactionsPage | null> {
  const symbol = normalizeTickerSymbol(inputSymbol);
  if (!symbol) {
    return null;
  }

  const normalizedOffset = clampOffset(offset);
  const normalizedLimit = clampPositiveInteger(
    limit,
    TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE,
    MAX_POLITICIAN_TRANSACTION_PAGE_SIZE,
  );

  return loadCachedTickerPoliticianTransactions(symbol, normalizedOffset, normalizedLimit);
}

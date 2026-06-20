import 'server-only';

import { formatFundChangeLabel, getFundChangeKind } from '@/lib/fund-holdings';
import { normalizeInsiderDirection } from '@/lib/insider-trades';
import { normalizeProfileDate, normalizeProfileDirection } from '@/lib/politician-profile';
import { filterProductPoliticianTrades } from '@/lib/politician-trade-scope';
import { getPublicSupabase } from '@/lib/supabase-server';
import type {
  DashboardTickerActivity,
  DashboardTickerActivityDirection,
  DashboardTickerActivityFilter,
  DashboardTickerWorkspaceData,
} from '@/lib/ticker-workspace-types';

type CompanyRow = {
  name?: string | null;
  sector?: string | null;
  industry?: string | null;
};

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

const VALID_TICKER_PATTERN = /^[A-Z][A-Z0-9.-]{0,9}$/;
const DEFAULT_ACTIVITY_LIMIT = 10;
const MAX_ACTIVITY_LIMIT = 16;
const MAX_ACTIVITY_OFFSET = 5000;

function normalizeText(value: string | null | undefined) {
  return String(value || '').trim();
}

function normalizeKnownText(value: string | null | undefined) {
  const text = normalizeText(value);
  if (!text || ['unknown', 'n/a', 'na', 'none'].includes(text.toLowerCase())) {
    return null;
  }
  return text;
}

function normalizeTickerSymbol(value: string | null | undefined) {
  const symbol = normalizeText(value).toUpperCase();
  return symbol && VALID_TICKER_PATTERN.test(symbol) ? symbol : null;
}

function normalizeSource(value: string | null | undefined): DashboardTickerActivityFilter {
  const source = normalizeText(value).toLowerCase();
  if (source === 'politician' || source === 'insider' || source === 'fund') {
    return source;
  }
  return 'all';
}

function clampPageValue(value: number | string | null | undefined, fallback: number, min: number, max: number) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.floor(numeric)));
}

function latestKnownDate(...values: Array<string | null | undefined>) {
  const normalized = values
    .map((value) => normalizeProfileDate(value))
    .filter((value): value is string => Boolean(value))
    .sort()
    .reverse();
  return normalized[0] || null;
}

function formatCompactCurrency(value: number | string | null | undefined) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }

  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: numeric >= 1_000_000 ? 'compact' : 'standard',
    maximumFractionDigits: numeric >= 1_000_000 ? 1 : 0,
  }).format(numeric);
}

function formatFundHoldingCurrency(value: number | string | null | undefined) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }

  // SEC 13F value fields are reported in thousands of dollars.
  return formatCompactCurrency(numeric / 1000);
}

function asNumber(value: number | string | null | undefined) {
  if (value == null || value === '') {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatCompactShares(value: number | string | null | undefined) {
  const numeric = asNumber(value);
  if (numeric == null) {
    return null;
  }
  const absolute = Math.abs(numeric);
  const format = (amount: number) => amount.toFixed(amount >= 10 ? 0 : 1).replace(/\.0$/, '');
  if (absolute >= 1_000_000_000) {
    return `${format(absolute / 1_000_000_000)}B shares`;
  }
  if (absolute >= 1_000_000) {
    return `${format(absolute / 1_000_000)}M shares`;
  }
  if (absolute >= 1_000) {
    return `${format(absolute / 1_000)}K shares`;
  }
  const rounded = Math.round(absolute);
  return `${rounded.toLocaleString()} ${rounded === 1 ? 'share' : 'shares'}`;
}

function fundChangeValue(row: FundHoldingRow) {
  const kind = getFundChangeKind(row);
  const shareDelta = asNumber(row.qoq_change_shares);
  const shareDeltaLabel = formatCompactShares(shareDelta);

  if (kind === 'new') {
    return 'New position';
  }
  if (kind === 'exit') {
    return shareDeltaLabel ? `Reduced ${shareDeltaLabel}` : 'Exited position';
  }
  if (kind === 'hold') {
    return 'No share change';
  }
  if (kind === 'increase') {
    return shareDeltaLabel ? `Added ${shareDeltaLabel}` : formatFundChangeLabel(row);
  }
  if (kind === 'decrease') {
    return shareDeltaLabel ? `Reduced ${shareDeltaLabel}` : formatFundChangeLabel(row);
  }
  return 'Change unavailable';
}

function formatInsiderValue(row: InsiderTradeRow) {
  const directValue = Number(row.value);
  if (Number.isFinite(directValue) && directValue > 0) {
    return formatCompactCurrency(directValue);
  }

  const amount = Number(row.amount);
  const price = Number(row.price);
  if (Number.isFinite(amount) && Number.isFinite(price) && amount > 0 && price > 0) {
    return formatCompactCurrency(amount * price);
  }

  return null;
}

function politicianDirection(value: string | null | undefined): {
  direction: DashboardTickerActivityDirection;
  label: string;
} {
  const normalized = normalizeProfileDirection(value);
  if (normalized === 'buy') {
    return { direction: 'buy', label: 'Buy' };
  }
  if (normalized === 'sell') {
    return { direction: 'sell', label: 'Sell' };
  }
  return { direction: 'activity', label: normalizeText(value) || 'Activity' };
}

function insiderDirection(value: string | null | undefined): {
  direction: DashboardTickerActivityDirection;
  label: string;
} {
  const normalized = normalizeInsiderDirection(value);
  if (normalized === 'buy') {
    return { direction: 'buy', label: 'Buy' };
  }
  if (normalized === 'sell') {
    return { direction: 'sell', label: 'Sell' };
  }
  return { direction: 'activity', label: normalizeText(value) || 'Activity' };
}

function fundDirection(row: FundHoldingRow): {
  direction: DashboardTickerActivityDirection;
  label: string;
} {
  const kind = getFundChangeKind(row);
  if (kind === 'new') {
    return { direction: 'new', label: 'New' };
  }
  if (kind === 'increase') {
    return { direction: 'increase', label: 'Bought more' };
  }
  if (kind === 'decrease') {
    return { direction: 'decrease', label: 'Sold' };
  }
  if (kind === 'exit') {
    return { direction: 'exit', label: 'Sold' };
  }
  if (kind === 'hold') {
    return { direction: 'flat', label: 'Neutral' };
  }
  return { direction: 'activity', label: '13F' };
}

function sortActivitiesByDate(items: DashboardTickerActivity[]) {
  return [...items].sort((left, right) => {
    const leftDate = left.date || '';
    const rightDate = right.date || '';
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }
    return right.id.localeCompare(left.id);
  });
}

function politicianActivity(row: PoliticianTradeRow): DashboardTickerActivity {
  const direction = politicianDirection(row.transaction_type);
  const party = normalizeKnownText(row.party);
  const chamber = normalizeKnownText(row.chamber);

  return {
    id: `politician:${row.id}`,
    sourceType: 'politician',
    actorName: normalizeText(row.politician_name) || 'Unknown member',
    actorSubtitle: [party, chamber].filter(Boolean).join(' · ') || 'Congress trade',
    memberId: normalizeText(row.member_id) || null,
    party,
    chamber,
    direction: direction.direction,
    directionLabel: direction.label,
    amountLabel: normalizeText(row.amount_range) || null,
    metricLabel: null,
    metricCaption: null,
    secondaryMetricLabel: null,
    secondaryMetricCaption: null,
    date: latestKnownDate(row.transaction_date, row.published_date),
    filingDate: null,
    sourceUrl: normalizeText(row.source_url) || null,
  };
}

function insiderActivity(row: InsiderTradeRow): DashboardTickerActivity {
  const direction = insiderDirection(row.transaction_code);
  const relation = normalizeText(row.filer_relation) || null;

  return {
    id: `insider:${row.id}`,
    sourceType: 'insider',
    actorName: normalizeText(row.filer_name) || 'Unknown insider',
    actorSubtitle: relation || 'Insider filing',
    memberId: null,
    party: null,
    chamber: null,
    direction: direction.direction,
    directionLabel: direction.label,
    amountLabel: formatInsiderValue(row),
    metricLabel: null,
    metricCaption: null,
    secondaryMetricLabel: null,
    secondaryMetricCaption: null,
    date: latestKnownDate(row.transaction_date, row.published_date),
    filingDate: null,
    sourceUrl: normalizeText(row.source_url) || null,
  };
}

function fundActivity(row: FundHoldingRow): DashboardTickerActivity {
  const direction = fundDirection(row);
  const holdingValue = formatFundHoldingCurrency(row.value_held);
  const sharesHeld = formatCompactShares(row.shares_held);

  return {
    id: `fund:${row.id}`,
    sourceType: 'fund',
    actorName: normalizeText(row.fund_name) || 'Unnamed fund',
    actorSubtitle: sharesHeld ? `${sharesHeld} held` : 'Holding size unavailable',
    memberId: null,
    party: null,
    chamber: null,
    direction: direction.direction,
    directionLabel: direction.label,
    amountLabel: fundChangeValue(row),
    metricLabel: holdingValue,
    metricCaption: 'total holding',
    secondaryMetricLabel: sharesHeld,
    secondaryMetricCaption: 'shares held',
    date: latestKnownDate(row.report_period, row.published_date),
    filingDate: normalizeProfileDate(row.published_date),
    sourceUrl: normalizeText(row.source_url) || null,
  };
}

function fundHoldingValue(row: FundHoldingRow) {
  return asNumber(row.value_held) || 0;
}

function fundActivitySortDate(row: FundHoldingRow) {
  return latestKnownDate(row.published_date, row.report_period) || '';
}

function fundActivityPriority(row: FundHoldingRow) {
  const kind = getFundChangeKind(row);
  if (kind === 'new') return 5;
  if (kind === 'increase') return 4;
  if (kind === 'decrease' || kind === 'exit') return 3;
  if (kind === 'hold') return 1;
  return 0;
}

function sortFundHoldingsByActivity(rows: FundHoldingRow[]) {
  return [...rows].sort((left, right) => {
    const leftDate = fundActivitySortDate(left);
    const rightDate = fundActivitySortDate(right);
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }

    const priorityDelta = fundActivityPriority(right) - fundActivityPriority(left);
    if (priorityDelta !== 0) {
      return priorityDelta;
    }

    const valueDelta = fundHoldingValue(right) - fundHoldingValue(left);
    if (valueDelta !== 0) {
      return valueDelta;
    }

    return (asNumber(right.shares_held) || 0) - (asNumber(left.shares_held) || 0);
  });
}

function latestFundHoldingsByFund(rows: FundHoldingRow[]) {
  const latestRows = [...rows].sort((left, right) => {
    const leftDate = fundActivitySortDate(left);
    const rightDate = fundActivitySortDate(right);
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }
    return String(right.id).localeCompare(String(left.id));
  });
  const byFund = new Map<string, FundHoldingRow>();

  for (const row of latestRows) {
    const key = normalizeText(row.fund_name).toLowerCase();
    if (!key || byFund.has(key)) {
      continue;
    }
    byFund.set(key, row);
  }

  return sortFundHoldingsByActivity([...byFund.values()]);
}

async function fetchFundRows({
  supabase,
  symbol,
  rankedByHolding,
  fetchLimit,
}: {
  supabase: ReturnType<typeof getPublicSupabase>;
  symbol: string;
  rankedByHolding: boolean;
  fetchLimit: number;
}) {
  if (!rankedByHolding) {
    return supabase
      .from('institutional_holdings')
      .select('id,fund_name,published_date,report_period,qoq_change_shares,qoq_change_percent,shares_held,value_held,source_url')
      .eq('ticker', symbol)
      .order('report_period', { ascending: false })
      .order('published_date', { ascending: false })
      .order('id', { ascending: false })
      .range(0, fetchLimit - 1);
  }

  return supabase
    .from('institutional_holdings')
    .select('id,fund_name,published_date,report_period,qoq_change_shares,qoq_change_percent,shares_held,value_held,source_url')
    .eq('ticker', symbol)
    .order('report_period', { ascending: false })
    .order('published_date', { ascending: false })
    .order('id', { ascending: false })
    .range(0, Math.max(fetchLimit, 750) - 1);
}

export async function getTickerWorkspaceData(
  inputSymbol: string,
  {
    offset = 0,
    limit = DEFAULT_ACTIVITY_LIMIT,
    source = 'all',
  }: {
    offset?: number;
    limit?: number;
    source?: string | null;
  } = {},
): Promise<DashboardTickerWorkspaceData | null> {
  const symbol = normalizeTickerSymbol(inputSymbol);
  if (!symbol) {
    return null;
  }

  const safeOffset = clampPageValue(offset, 0, 0, MAX_ACTIVITY_OFFSET);
  const safeLimit = clampPageValue(limit, DEFAULT_ACTIVITY_LIMIT, 4, MAX_ACTIVITY_LIMIT);
  const fetchLimit = safeOffset + safeLimit + 1;
  const selectedSource = normalizeSource(source);
  const includePoliticians = selectedSource === 'all' || selectedSource === 'politician';
  const includeInsiders = selectedSource === 'all' || selectedSource === 'insider';
  const includeFunds = selectedSource === 'all' || selectedSource === 'fund';
  const rankFundsByHolding = selectedSource === 'fund';
  const supabase = getPublicSupabase();

  const [companyResult, politicianResult, insiderResult, fundResult] = await Promise.all([
    supabase.from('companies').select('name,sector,industry').eq('ticker', symbol).maybeSingle(),
    includePoliticians
      ? supabase
          .from('politician_trades')
          .select('id,member_id,politician_name,chamber,party,ticker,transaction_date,published_date,transaction_type,amount_range,source_url,doc_id,asset_name')
          .eq('ticker', symbol)
          .order('transaction_date', { ascending: false })
          .order('published_date', { ascending: false })
          .order('id', { ascending: false })
          .range(0, fetchLimit - 1)
      : Promise.resolve({ data: [], error: null }),
    includeInsiders
      ? supabase
          .from('insider_trades')
          .select('id,filer_name,filer_relation,transaction_code,transaction_date,published_date,amount,price,value,source_url')
          .eq('ticker', symbol)
          .order('transaction_date', { ascending: false })
          .order('published_date', { ascending: false })
          .order('id', { ascending: false })
          .range(0, fetchLimit - 1)
      : Promise.resolve({ data: [], error: null }),
    includeFunds
      ? fetchFundRows({ supabase, symbol, rankedByHolding: rankFundsByHolding, fetchLimit })
      : Promise.resolve({ data: [], error: null }),
  ]);

  if (companyResult.error) throw new Error(companyResult.error.message);
  if (politicianResult.error) throw new Error(politicianResult.error.message);
  if (insiderResult.error) throw new Error(insiderResult.error.message);
  if (fundResult.error) throw new Error(fundResult.error.message);

  const company = (companyResult.data || null) as CompanyRow | null;
  const politicianTrades = filterProductPoliticianTrades((politicianResult.data || []) as PoliticianTradeRow[]);
  const insiderTrades = (insiderResult.data || []) as InsiderTradeRow[];
  const fundHoldings = rankFundsByHolding
    ? latestFundHoldingsByFund((fundResult.data || []) as FundHoldingRow[])
    : ((fundResult.data || []) as FundHoldingRow[]);

  const activity =
    selectedSource === 'fund'
      ? fundHoldings.map(fundActivity)
      : sortActivitiesByDate([
          ...politicianTrades.map(politicianActivity),
          ...insiderTrades.map(insiderActivity),
          ...fundHoldings.map(fundActivity),
        ]);
  const page = activity.slice(safeOffset, safeOffset + safeLimit);

  if (!company && activity.length === 0) {
    return null;
  }

  return {
    symbol,
    companyName: normalizeKnownText(company?.name) || symbol,
    sector: normalizeKnownText(company?.sector),
    industry: normalizeKnownText(company?.industry),
    latestActivityDate: activity[0]?.date || null,
    source: selectedSource,
    recentActivity: page,
    nextOffset: activity.length > safeOffset + safeLimit ? safeOffset + safeLimit : null,
  };
}

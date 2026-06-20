import { stripPoliticianOptionMetadata } from '@/lib/politician-option-trades';
import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';

type TradeDirection = 'buy' | 'sell' | 'other';

export type PoliticianProfileTrade = {
  id: string;
  doc_id?: string | null;
  member_id?: string | null;
  politician_name?: string | null;
  ticker?: string | null;
  asset_name?: string | null;
  asset_type?: string | null;
  transaction_type?: string | null;
  amount_range?: string | null;
  published_date?: string | null;
  transaction_date?: string | null;
  source_url?: string | null;
  chamber?: string | null;
  congress_members?: {
    first_name?: string | null;
    last_name?: string | null;
    party?: string | null;
    chamber?: string | null;
    state?: string | null;
  } | null;
};

export type PoliticianDisclosureHolding = {
  key: string;
  docId: string;
  filingDate: string;
  filingType: string | null;
  filingTypeLabel: string | null;
  ticker: string | null;
  label: string;
  assetType: string | null;
  owner: string | null;
  valueRange: string;
  sourceUrl: string | null;
};

export type PoliticianHoldingEstimate = {
  key: string;
  label: string;
  ticker: string | null;
  netAmount: number;
  grossBuys: number;
  grossSells: number;
  tradeCount: number;
  lastTradeDate: string | null;
};

export type PoliticianLivePortfolioHolding = {
  key: string;
  ticker: string;
  label: string;
  instrumentType: string | null;
  estimatedShares: number;
  estimatedMinHoldingValue: number;
  estimatedCurrentPrice: number;
  estimatedCurrentValue: number;
  estimatedMaxHoldingValue: number;
  estimatedCostBasis: number;
  estimatedUnrealizedGain: number;
  estimatedUnrealizedReturnPct: number | null;
  allocationPct: number;
  tradeCount: number;
  pricedTradeCount: number;
  skippedTradeCount: number;
  lastTradeDate: string | null;
  priceAsOf: string | null;
};

export type PoliticianLivePortfolioSummary = {
  holdingCount: number;
  totalEstimatedCurrentValue: number;
  totalEstimatedCostBasis: number;
  totalEstimatedUnrealizedGain: number;
  eligibleTradeCount: number;
  pricedTradeCount: number;
  skippedTradeCount: number;
  priceAsOf: string | null;
  disclosureSnapshotDate: string | null;
  disclosureHoldingCount: number;
  holdings: PoliticianLivePortfolioHolding[];
};

export type PoliticianYearlyActivity = {
  year: string;
  buys: number;
  sells: number;
  count: number;
};

export type PoliticianProfileSummary = {
  displayName: string;
  party: string | null;
  chamber: string | null;
  state: string | null;
  totalTrades: number;
  buyCount: number;
  sellCount: number;
  totalEstimatedBuys: number;
  totalEstimatedSells: number;
  estimatedNetFlow: number;
  estimatedActiveExposure: number;
  activeHoldingCount: number;
  latestTradeDate: string | null;
  firstTradeDate: string | null;
  yearlyActivity: PoliticianYearlyActivity[];
  holdings: PoliticianHoldingEstimate[];
};

function normalizeText(value: string | null | undefined): string {
  return String(value || '').trim();
}

function toIsoDate(year: number, month: number, day: number): string | null {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }
  const utc = new Date(Date.UTC(year, month - 1, day));
  if (
    utc.getUTCFullYear() !== year ||
    utc.getUTCMonth() !== month - 1 ||
    utc.getUTCDate() !== day
  ) {
    return null;
  }
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function normalizeDirection(value: string | null | undefined): TradeDirection {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized.startsWith('buy') || normalized === 'purchase' || normalized === 'p') {
    return 'buy';
  }
  if (normalized.startsWith('sell') || normalized === 'sale' || normalized === 's') {
    return 'sell';
  }
  return 'other';
}

export function normalizeProfileDirection(value: string | null | undefined): TradeDirection {
  return normalizeDirection(value);
}

function normalizeTicker(value: string | null | undefined): string | null {
  const normalized = normalizeText(value).toUpperCase();
  if (!normalized || normalized === 'N/A' || normalized === 'UNKNOWN') {
    return null;
  }
  return normalized;
}

export function normalizeProfileTicker(value: string | null | undefined): string | null {
  return normalizeTicker(value);
}

function parseEstimatedAmount(value: string | null | undefined): number {
  return parsePoliticianAmountRange(value)?.estimated || 0;
}

export function parseEstimatedTradeAmount(value: string | null | undefined): number {
  return parseEstimatedAmount(value);
}

function formatDateValue(value: string | null | undefined): string | null {
  const normalized = normalizeText(value);
  if (!normalized) {
    return null;
  }

  const isoMatch = normalized.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return toIsoDate(Number(isoMatch[1]), Number(isoMatch[2]), Number(isoMatch[3]));
  }

  const usMatch = normalized.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    const rawYear = Number(usMatch[3]);
    const year = usMatch[3].length === 2 ? 2000 + rawYear : rawYear;
    return toIsoDate(year, month, day);
  }

  const parsed = new Date(normalized);
  if (!Number.isFinite(parsed.getTime())) {
    return null;
  }
  return toIsoDate(parsed.getUTCFullYear(), parsed.getUTCMonth() + 1, parsed.getUTCDate());
}

export function normalizeProfileDate(value: string | null | undefined): string | null {
  return formatDateValue(value);
}

function yearFromTrade(trade: PoliticianProfileTrade): string {
  const dateValue = formatDateValue(trade.transaction_date) || formatDateValue(trade.published_date);
  return dateValue?.slice(0, 4) || 'Unknown';
}

function assetLabel(trade: PoliticianProfileTrade): string {
  const ticker = normalizeTicker(trade.ticker);
  const assetName = stripPoliticianOptionMetadata(trade.asset_name);
  if (ticker === 'US-TREAS') {
    return normalizeText(assetName) || 'U.S. Treasury';
  }
  if (ticker) {
    return ticker;
  }
  return normalizeText(assetName) || 'Unmapped asset';
}

export function politicianTradeAssetLabel(trade: PoliticianProfileTrade): string {
  return assetLabel(trade);
}

function holdingKey(trade: PoliticianProfileTrade): string {
  const ticker = normalizeTicker(trade.ticker);
  if (ticker) {
    return `ticker:${ticker}`;
  }
  const asset = normalizeText(trade.asset_name).toLowerCase();
  if (asset) {
    return `asset:${asset}`;
  }
  return `doc:${normalizeText(trade.doc_id)}`;
}

export function buildPoliticianProfileSummary(trades: PoliticianProfileTrade[]): PoliticianProfileSummary {
  const firstTrade = trades[0];
  const displayName =
    normalizeText(firstTrade?.congress_members?.first_name) && normalizeText(firstTrade?.congress_members?.last_name)
      ? `${normalizeText(firstTrade?.congress_members?.first_name)} ${normalizeText(firstTrade?.congress_members?.last_name)}`
      : normalizeText(firstTrade?.politician_name) || 'Unknown Politician';

  const holdings = new Map<string, PoliticianHoldingEstimate>();
  const yearly = new Map<string, PoliticianYearlyActivity>();

  let totalTrades = 0;
  let buyCount = 0;
  let sellCount = 0;
  let totalEstimatedBuys = 0;
  let totalEstimatedSells = 0;
  let firstTradeDate: string | null = null;
  let latestTradeDate: string | null = null;

  for (const trade of trades) {
    totalTrades += 1;
    const direction = normalizeDirection(trade.transaction_type);
    const estimatedAmount = parseEstimatedAmount(trade.amount_range);
    const tradeDate = formatDateValue(trade.transaction_date) || formatDateValue(trade.published_date);

    if (!firstTradeDate || (tradeDate && tradeDate < firstTradeDate)) {
      firstTradeDate = tradeDate;
    }
    if (!latestTradeDate || (tradeDate && tradeDate > latestTradeDate)) {
      latestTradeDate = tradeDate;
    }

    const year = yearFromTrade(trade);
    const yearBucket = yearly.get(year) || { year, buys: 0, sells: 0, count: 0 };
    yearBucket.count += 1;
    if (direction === 'buy') {
      buyCount += 1;
      totalEstimatedBuys += estimatedAmount;
      yearBucket.buys += estimatedAmount;
    } else if (direction === 'sell') {
      sellCount += 1;
      totalEstimatedSells += estimatedAmount;
      yearBucket.sells += estimatedAmount;
    }
    yearly.set(year, yearBucket);

    const key = holdingKey(trade);
    const current = holdings.get(key) || {
      key,
      label: assetLabel(trade),
      ticker: normalizeTicker(trade.ticker),
      netAmount: 0,
      grossBuys: 0,
      grossSells: 0,
      tradeCount: 0,
      lastTradeDate: null,
    };

    current.tradeCount += 1;
    if (!current.lastTradeDate || (tradeDate && tradeDate > current.lastTradeDate)) {
      current.lastTradeDate = tradeDate;
    }
    if (direction === 'buy') {
      current.netAmount += estimatedAmount;
      current.grossBuys += estimatedAmount;
    } else if (direction === 'sell') {
      current.netAmount -= estimatedAmount;
      current.grossSells += estimatedAmount;
    }
    holdings.set(key, current);
  }

  const holdingList = [...holdings.values()]
    .filter((holding) => holding.netAmount > 0)
    .sort((left, right) => right.netAmount - left.netAmount);

  const yearlyActivity = [...yearly.values()].sort((left, right) => left.year.localeCompare(right.year));
  const estimatedNetFlow = totalEstimatedBuys - totalEstimatedSells;
  const estimatedActiveExposure = holdingList.reduce((sum, holding) => sum + holding.netAmount, 0);

  return {
    displayName,
    party: firstTrade?.congress_members?.party || null,
    chamber: firstTrade?.congress_members?.chamber || firstTrade?.chamber || null,
    state: firstTrade?.congress_members?.state || null,
    totalTrades,
    buyCount,
    sellCount,
    totalEstimatedBuys,
    totalEstimatedSells,
    estimatedNetFlow,
    estimatedActiveExposure,
    activeHoldingCount: holdingList.length,
    latestTradeDate,
    firstTradeDate,
    yearlyActivity,
    holdings: holdingList,
  };
}

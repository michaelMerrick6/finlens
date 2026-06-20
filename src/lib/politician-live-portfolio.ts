import 'server-only';

import { getMarketPriceSeriesMap, getPriceOnOrBefore } from '@/lib/market-data';
import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import {
  type PoliticianDisclosureHolding,
  normalizeProfileDate,
  normalizeProfileDirection,
  normalizeProfileTicker,
  parseEstimatedTradeAmount,
  politicianTradeAssetLabel,
  type PoliticianLivePortfolioHolding,
  type PoliticianLivePortfolioSummary,
  type PoliticianProfileTrade,
} from '@/lib/politician-profile';

const ALLOWED_INSTRUMENT_TYPES = new Set(['EQUITY', 'ETF']);
const DERIVATIVE_ASSET_PATTERNS = [
  'option',
  ' call ',
  ' put ',
  ' warrant',
  'rights',
  '[op]',
  '[ot]',
  'strike',
  'exercised',
];

type PortfolioLot = {
  shares: number;
  costPerShare: number;
};

type HoldingAccumulator = {
  ticker: string;
  label: string;
  tradeCount: number;
  pricedTradeCount: number;
  skippedTradeCount: number;
  lastTradeDate: string | null;
  lots: PortfolioLot[];
  instrumentType: string | null;
  priceAsOf: string | null;
  currentPrice: number | null;
};

type NetValueRangeAccumulator = {
  ticker: string;
  label: string;
  minValue: number;
  maxValue: number;
  tradeCount: number;
  lastTradeDate: string | null;
};

function isDerivativeAssetLabel(value: string | null | undefined): boolean {
  const normalized = ` ${String(value || '').trim().toLowerCase()} `;
  return DERIVATIVE_ASSET_PATTERNS.some((pattern) => normalized.includes(pattern));
}

function isEligibleTickerTrade(trade: PoliticianProfileTrade): boolean {
  const ticker = normalizeProfileTicker(trade.ticker);
  if (!ticker || ticker === 'US-TREAS') {
    return false;
  }
  return !isDerivativeAssetLabel(trade.asset_name);
}

function earliestTradeDateByTicker(trades: PoliticianProfileTrade[]): Map<string, string> {
  const result = new Map<string, string>();
  for (const trade of trades) {
    if (!isEligibleTickerTrade(trade)) {
      continue;
    }
    const ticker = normalizeProfileTicker(trade.ticker);
    const tradeDate = normalizeProfileDate(trade.transaction_date) || normalizeProfileDate(trade.published_date);
    if (!ticker || !tradeDate) {
      continue;
    }
    const current = result.get(ticker);
    if (!current || tradeDate < current) {
      result.set(ticker, tradeDate);
    }
  }
  return result;
}

function eligibleDisclosureHoldings(holdings: PoliticianDisclosureHolding[]): PoliticianDisclosureHolding[] {
  return holdings.filter((holding) => {
    const ticker = normalizeProfileTicker(holding.ticker);
    if (!ticker || ticker === 'US-TREAS') {
      return false;
    }
    return !isDerivativeAssetLabel(holding.label);
  });
}

function roundTo(value: number, digits = 4): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function sumLotsCostBasis(lots: PortfolioLot[]): number {
  return lots.reduce((sum, lot) => sum + lot.shares * lot.costPerShare, 0);
}

export async function buildPoliticianLivePortfolio(
  trades: PoliticianProfileTrade[],
  disclosureHoldings: PoliticianDisclosureHolding[] = [],
): Promise<PoliticianLivePortfolioSummary> {
  const latestDisclosureHoldings = eligibleDisclosureHoldings(disclosureHoldings);
  const disclosureSnapshotDate = latestDisclosureHoldings.reduce<string | null>((latest, holding) => {
    const current = normalizeProfileDate(holding.filingDate);
    if (!current) {
      return latest;
    }
    if (!latest || current > latest) {
      return current;
    }
    return latest;
  }, null);

  const eligibleTrades = trades.filter((trade) => {
    if (!isEligibleTickerTrade(trade)) {
      return false;
    }
    if (!disclosureSnapshotDate) {
      return true;
    }
    const tradeDate = normalizeProfileDate(trade.transaction_date) || normalizeProfileDate(trade.published_date);
    return !tradeDate || tradeDate > disclosureSnapshotDate;
  });

  const earliestByTicker = earliestTradeDateByTicker(eligibleTrades);
  for (const holding of latestDisclosureHoldings) {
    const ticker = normalizeProfileTicker(holding.ticker);
    const filedAt = normalizeProfileDate(holding.filingDate);
    if (!ticker || !filedAt) {
      continue;
    }
    const current = earliestByTicker.get(ticker);
    if (!current || filedAt < current) {
      earliestByTicker.set(ticker, filedAt);
    }
  }

  const priceSeriesByTicker = await getMarketPriceSeriesMap(
    [...earliestByTicker.entries()].map(([ticker, earliestDate]) => ({
      ticker,
      earliestDate,
    })),
    6,
  );
  const sortedTrades = [...eligibleTrades].sort((left, right) => {
    const leftDate = normalizeProfileDate(left.transaction_date) || normalizeProfileDate(left.published_date) || '';
    const rightDate = normalizeProfileDate(right.transaction_date) || normalizeProfileDate(right.published_date) || '';
    if (leftDate !== rightDate) {
      return leftDate.localeCompare(rightDate);
    }
    return String(left.id).localeCompare(String(right.id));
  });

  const netValueRangeByTicker = new Map<string, NetValueRangeAccumulator>();
  const holdings = new Map<string, HoldingAccumulator>();
  let pricedTradeCount = 0;
  let skippedTradeCount = 0;

  for (const holding of latestDisclosureHoldings) {
    const ticker = normalizeProfileTicker(holding.ticker);
    const filedAt = normalizeProfileDate(holding.filingDate);
    const amountRange = parsePoliticianAmountRange(holding.valueRange);
    if (!ticker || !filedAt || !amountRange) {
      continue;
    }

    netValueRangeByTicker.set(ticker, {
      ticker,
      label: holding.label,
      minValue: amountRange.min,
      maxValue: amountRange.max,
      tradeCount: 0,
      lastTradeDate: filedAt,
    });

    const series = priceSeriesByTicker.get(ticker) || null;
    if (!series || !series.currentPrice || !ALLOWED_INSTRUMENT_TYPES.has(series.instrumentType || '')) {
      continue;
    }

    const baselinePrice = getPriceOnOrBefore(series, filedAt);
    if (!baselinePrice || baselinePrice <= 0) {
      continue;
    }

    holdings.set(ticker, {
      ticker,
      label: holding.label,
      tradeCount: 0,
      pricedTradeCount: 0,
      skippedTradeCount: 0,
      lastTradeDate: filedAt,
      lots: [
        {
          shares: amountRange.estimated / baselinePrice,
          costPerShare: baselinePrice,
        },
      ],
      instrumentType: series.instrumentType,
      priceAsOf: series.priceAsOf,
      currentPrice: series.currentPrice,
    });
  }

  for (const trade of sortedTrades) {
    const ticker = normalizeProfileTicker(trade.ticker);
    const direction = normalizeProfileDirection(trade.transaction_type);
    const amountRange = parsePoliticianAmountRange(trade.amount_range);
    const tradeDate = normalizeProfileDate(trade.transaction_date) || normalizeProfileDate(trade.published_date);
    if (!ticker || ticker === 'US-TREAS' || direction === 'other' || !amountRange) {
      continue;
    }

    const current = netValueRangeByTicker.get(ticker) || {
      ticker,
      label: politicianTradeAssetLabel(trade),
      minValue: 0,
      maxValue: 0,
      tradeCount: 0,
      lastTradeDate: null,
    };

    current.tradeCount += 1;
    if (!current.lastTradeDate || (tradeDate && tradeDate > current.lastTradeDate)) {
      current.lastTradeDate = tradeDate;
    }

    if (direction === 'buy') {
      current.minValue += amountRange.min;
      current.maxValue += amountRange.max;
    } else {
      current.minValue = Math.max(0, current.minValue - amountRange.max);
      current.maxValue = Math.max(0, current.maxValue - amountRange.min);
    }
    netValueRangeByTicker.set(ticker, current);
  }

  for (const trade of sortedTrades) {
    const ticker = normalizeProfileTicker(trade.ticker);
    const tradeDate = normalizeProfileDate(trade.transaction_date) || normalizeProfileDate(trade.published_date);
    const direction = normalizeProfileDirection(trade.transaction_type);
    const estimatedAmount = parseEstimatedTradeAmount(trade.amount_range);
    if (!ticker || !tradeDate || direction === 'other' || estimatedAmount <= 0) {
      skippedTradeCount += 1;
      continue;
    }

    const series = priceSeriesByTicker.get(ticker) || null;
    if (!series || !series.currentPrice || !ALLOWED_INSTRUMENT_TYPES.has(series.instrumentType || '')) {
      skippedTradeCount += 1;
      continue;
    }

    const tradePrice = getPriceOnOrBefore(series, tradeDate);
    if (!tradePrice || tradePrice <= 0) {
      skippedTradeCount += 1;
      continue;
    }

    const shares = estimatedAmount / tradePrice;
    const current = holdings.get(ticker) || {
      ticker,
      label: politicianTradeAssetLabel(trade),
      tradeCount: 0,
      pricedTradeCount: 0,
      skippedTradeCount: 0,
      lastTradeDate: null,
      lots: [],
      instrumentType: series.instrumentType,
      priceAsOf: series.priceAsOf,
      currentPrice: series.currentPrice,
    };

    current.tradeCount += 1;
    current.pricedTradeCount += 1;
    if (!current.lastTradeDate || tradeDate > current.lastTradeDate) {
      current.lastTradeDate = tradeDate;
    }

    if (direction === 'buy') {
      current.lots.push({
        shares,
        costPerShare: tradePrice,
      });
    } else if (direction === 'sell') {
      let remainingSharesToSell = shares;
      while (remainingSharesToSell > 0 && current.lots.length > 0) {
        const lot = current.lots[0];
        if (lot.shares <= remainingSharesToSell) {
          remainingSharesToSell -= lot.shares;
          current.lots.shift();
        } else {
          lot.shares -= remainingSharesToSell;
          remainingSharesToSell = 0;
        }
      }

      if (remainingSharesToSell > 0) {
        current.skippedTradeCount += 1;
      }
    }

    pricedTradeCount += 1;
    holdings.set(ticker, current);
  }

  const holdingList: PoliticianLivePortfolioHolding[] = [...holdings.values()]
    .map((holding) => {
      const estimatedShares = holding.lots.reduce((sum, lot) => sum + lot.shares, 0);
      const estimatedCostBasis = sumLotsCostBasis(holding.lots);
      const estimatedCurrentPrice = holding.currentPrice || 0;
      const estimatedCurrentValue = estimatedShares * estimatedCurrentPrice;
      const estimatedUnrealizedGain = estimatedCurrentValue - estimatedCostBasis;
      return {
        key: holding.ticker,
        ticker: holding.ticker,
        label: holding.label,
        instrumentType: holding.instrumentType,
        estimatedShares: roundTo(estimatedShares),
        estimatedMinHoldingValue: roundTo(netValueRangeByTicker.get(holding.ticker)?.minValue || 0, 2),
        estimatedCurrentPrice: roundTo(estimatedCurrentPrice, 2),
        estimatedCurrentValue: roundTo(estimatedCurrentValue, 2),
        estimatedMaxHoldingValue: roundTo(netValueRangeByTicker.get(holding.ticker)?.maxValue || 0, 2),
        estimatedCostBasis: roundTo(estimatedCostBasis, 2),
        estimatedUnrealizedGain: roundTo(estimatedUnrealizedGain, 2),
        estimatedUnrealizedReturnPct: estimatedCostBasis > 0 ? roundTo((estimatedUnrealizedGain / estimatedCostBasis) * 100, 2) : null,
        allocationPct: 0,
        tradeCount: holding.tradeCount,
        pricedTradeCount: holding.pricedTradeCount,
        skippedTradeCount: holding.skippedTradeCount,
        lastTradeDate: holding.lastTradeDate,
        priceAsOf: holding.priceAsOf,
      };
    })
    .filter((holding) => holding.estimatedShares > 0.0001 && holding.estimatedCurrentValue > 0)
    .sort((left, right) => right.estimatedCurrentValue - left.estimatedCurrentValue);

  if (!holdingList.length && (eligibleTrades.length || latestDisclosureHoldings.length)) {
    for (const item of netValueRangeByTicker.values()) {
      if (item.maxValue <= 0) {
        continue;
      }
      const midpointValue = (item.minValue + item.maxValue) / 2;
      holdingList.push({
        key: item.ticker,
        ticker: item.ticker,
        label: item.label,
        instrumentType: null,
        estimatedShares: 0,
        estimatedMinHoldingValue: roundTo(item.minValue, 2),
        estimatedCurrentPrice: 0,
        estimatedCurrentValue: roundTo(midpointValue, 2),
        estimatedMaxHoldingValue: roundTo(item.maxValue, 2),
        estimatedCostBasis: roundTo(midpointValue, 2),
        estimatedUnrealizedGain: 0,
        estimatedUnrealizedReturnPct: 0,
        allocationPct: 0,
        tradeCount: item.tradeCount,
        pricedTradeCount: 0,
        skippedTradeCount: 0,
        lastTradeDate: item.lastTradeDate,
        priceAsOf: null,
      });
    }

    holdingList.sort((left, right) => right.estimatedCurrentValue - left.estimatedCurrentValue);
  }

  const totalEstimatedCurrentValue = holdingList.reduce((sum, holding) => sum + holding.estimatedCurrentValue, 0);
  const totalEstimatedCostBasis = holdingList.reduce((sum, holding) => sum + holding.estimatedCostBasis, 0);
  const totalEstimatedUnrealizedGain = totalEstimatedCurrentValue - totalEstimatedCostBasis;

  for (const holding of holdingList) {
    holding.allocationPct = totalEstimatedCurrentValue > 0
      ? roundTo((holding.estimatedCurrentValue / totalEstimatedCurrentValue) * 100, 2)
      : 0;
  }

  const priceAsOf = holdingList.reduce<string | null>((latest, holding) => {
    if (!holding.priceAsOf) {
      return latest;
    }
    if (!latest || holding.priceAsOf > latest) {
      return holding.priceAsOf;
    }
    return latest;
  }, null);

  return {
    holdingCount: holdingList.length,
    totalEstimatedCurrentValue: roundTo(totalEstimatedCurrentValue, 2),
    totalEstimatedCostBasis: roundTo(totalEstimatedCostBasis, 2),
    totalEstimatedUnrealizedGain: roundTo(totalEstimatedUnrealizedGain, 2),
    eligibleTradeCount: eligibleTrades.length,
    pricedTradeCount,
    skippedTradeCount,
    priceAsOf,
    disclosureSnapshotDate,
    disclosureHoldingCount: latestDisclosureHoldings.length,
    holdings: holdingList,
  };
}

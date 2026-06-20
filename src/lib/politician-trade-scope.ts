export const HOUSE_PRODUCT_START_DATE = '2015-01-01';

type ProductPoliticianTradeScopeLike = {
  chamber?: string | null;
  published_date?: string | null;
  doc_id?: string | null;
};

type DisplayPoliticianTradeScopeLike = ProductPoliticianTradeScopeLike & {
  ticker?: string | null;
};

export function isExcludedLegacyHouseTrade(trade: ProductPoliticianTradeScopeLike): boolean {
  const chamber = String(trade.chamber || '').trim();
  if (chamber && chamber !== 'House') {
    return false;
  }

  const publishedDate = String(trade.published_date || '').trim();
  if (publishedDate) {
    return chamber === 'House' && publishedDate < HOUSE_PRODUCT_START_DATE;
  }

  const docId = String(trade.doc_id || '').trim();
  return /^house-(2013|2014)-/.test(docId);
}

export function filterProductPoliticianTrades<T extends ProductPoliticianTradeScopeLike>(trades: T[]): T[] {
  return trades.filter((trade) => !isExcludedLegacyHouseTrade(trade));
}

export function isDisplayablePoliticianTrade(trade: DisplayPoliticianTradeScopeLike): boolean {
  const ticker = String(trade.ticker || '').trim().toUpperCase();
  if (!ticker) {
    return false;
  }
  if (ticker === 'US-TREAS') {
    return true;
  }
  return !['N/A', 'NA', 'UNKNOWN', 'MULTI'].includes(ticker);
}

export function filterDisplayPoliticianTrades<T extends DisplayPoliticianTradeScopeLike>(trades: T[]): T[] {
  return trades.filter((trade) => !isExcludedLegacyHouseTrade(trade) && isDisplayablePoliticianTrade(trade));
}

export const POLITICIAN_CLUSTER_KEY_PREFIX = 'congress-window-v1';

export type PoliticianClusterKey = {
  ticker: string;
  direction: 'buy' | 'sell';
  windowStart: string;
  windowEnd: string;
};

const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const TICKER_PATTERN = /^[A-Z0-9.-]{1,10}$/;

export function buildPoliticianClusterKey(value: PoliticianClusterKey) {
  return [
    POLITICIAN_CLUSTER_KEY_PREFIX,
    value.ticker,
    value.direction,
    value.windowStart,
    value.windowEnd,
  ].join(':');
}

export function parsePoliticianClusterKey(value: string): PoliticianClusterKey | null {
  const [prefix, rawTicker, rawDirection, windowStart, windowEnd, ...extra] = value.split(':');
  const ticker = rawTicker?.trim().toUpperCase();
  const direction = rawDirection?.trim().toLowerCase();

  if (
    prefix !== POLITICIAN_CLUSTER_KEY_PREFIX ||
    extra.length > 0 ||
    !TICKER_PATTERN.test(ticker || '') ||
    (direction !== 'buy' && direction !== 'sell') ||
    !ISO_DATE_PATTERN.test(windowStart || '') ||
    !ISO_DATE_PATTERN.test(windowEnd || '') ||
    windowStart > windowEnd
  ) {
    return null;
  }

  return {
    ticker,
    direction,
    windowStart,
    windowEnd,
  };
}

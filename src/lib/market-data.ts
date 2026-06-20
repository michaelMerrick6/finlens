import 'server-only';

const YAHOO_CHART_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36',
};

const MARKET_DATA_TTL_MS = 15 * 60 * 1000;

type CachedSeries = {
  expiresAt: number;
  value: MarketPriceSeries | null;
};

export type MarketPricePoint = {
  date: string;
  price: number;
};

export type MarketPriceSeries = {
  ticker: string;
  instrumentType: string | null;
  currentPrice: number | null;
  priceAsOf: string | null;
  points: MarketPricePoint[];
};

type MarketSeriesRequest = {
  ticker: string;
  earliestDate?: string | null;
};

const marketSeriesCache = new Map<string, CachedSeries>();

function toIsoDate(timestampSeconds: number): string {
  return new Date(timestampSeconds * 1000).toISOString().slice(0, 10);
}

function normalizeTicker(value: string): string {
  return value.trim().toUpperCase();
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export async function getMarketPriceSeries(
  ticker: string,
  earliestDate?: string | null,
): Promise<MarketPriceSeries | null> {
  const normalizedTicker = normalizeTicker(ticker);
  const cached = marketSeriesCache.get(normalizedTicker);
  const now = Date.now();

  if (cached && cached.expiresAt > now) {
    return cached.value;
  }

  const period1 = earliestDate
    ? Math.max(
        0,
        Math.floor(new Date(`${earliestDate}T00:00:00Z`).getTime() / 1000) - 7 * 24 * 60 * 60,
      )
    : 0;
  const period2 = Math.floor(now / 1000);

  try {
    const url = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(normalizedTicker)}`);
    url.searchParams.set('period1', String(period1));
    url.searchParams.set('period2', String(period2));
    url.searchParams.set('interval', '1d');
    url.searchParams.set('includeAdjustedClose', 'true');
    url.searchParams.set('events', 'div,splits');

    let payload:
      | {
          chart?: {
            result?: Array<{
              meta?: {
                instrumentType?: string;
                regularMarketPrice?: number;
              };
              timestamp?: number[];
              indicators?: {
                quote?: Array<{ close?: Array<number | null> }>;
                adjclose?: Array<{ adjclose?: Array<number | null> }>;
              };
            }>;
          };
        }
      | null = null;

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const response = await fetch(url.toString(), {
        headers: YAHOO_CHART_HEADERS,
      });

      if (response.status === 429 && attempt < 2) {
        await sleep(450 * (attempt + 1));
        continue;
      }

      if (!response.ok) {
        marketSeriesCache.set(normalizedTicker, { expiresAt: now + 60_000, value: null });
        return null;
      }

      payload = (await response.json()) as {
        chart?: {
          result?: Array<{
            meta?: {
              instrumentType?: string;
              regularMarketPrice?: number;
            };
            timestamp?: number[];
            indicators?: {
              quote?: Array<{ close?: Array<number | null> }>;
              adjclose?: Array<{ adjclose?: Array<number | null> }>;
            };
          }>;
        };
      };
      break;
    }

    if (!payload) {
      marketSeriesCache.set(normalizedTicker, { expiresAt: now + 60_000, value: null });
      return null;
    }

    const result = payload.chart?.result?.[0];
    const timestamps = result?.timestamp || [];
    const closes = result?.indicators?.quote?.[0]?.close || [];
    const adjustedCloses = result?.indicators?.adjclose?.[0]?.adjclose || [];

    const points: MarketPricePoint[] = [];
    for (let index = 0; index < timestamps.length; index += 1) {
      const adjusted = adjustedCloses[index];
      const close = closes[index];
      const price = Number.isFinite(adjusted) ? adjusted : close;
      if (!Number.isFinite(price) || !timestamps[index]) {
        continue;
      }
      points.push({
        date: toIsoDate(timestamps[index]),
        price: Number(price),
      });
    }

    const currentPrice = Number.isFinite(result?.meta?.regularMarketPrice)
      ? Number(result?.meta?.regularMarketPrice)
      : points.at(-1)?.price || null;

    const series: MarketPriceSeries = {
      ticker: normalizedTicker,
      instrumentType: result?.meta?.instrumentType || null,
      currentPrice,
      priceAsOf: points.at(-1)?.date || null,
      points,
    };

    marketSeriesCache.set(normalizedTicker, {
      expiresAt: now + MARKET_DATA_TTL_MS,
      value: series,
    });
    return series;
  } catch {
    marketSeriesCache.set(normalizedTicker, { expiresAt: now + 60_000, value: null });
    return null;
  }
}

export async function getMarketPriceSeriesMap(
  requests: MarketSeriesRequest[],
  concurrency = 6,
): Promise<Map<string, MarketPriceSeries | null>> {
  const uniqueRequests = Array.from(
    new Map(
      requests
        .filter((request) => normalizeTicker(request.ticker))
        .map((request) => [normalizeTicker(request.ticker), request] as const),
    ).values(),
  );
  const cappedConcurrency = Math.max(1, Math.min(concurrency, uniqueRequests.length || 1));
  const result = new Map<string, MarketPriceSeries | null>();
  let currentIndex = 0;

  const worker = async () => {
    while (currentIndex < uniqueRequests.length) {
      const request = uniqueRequests[currentIndex];
      currentIndex += 1;
      const normalizedTicker = normalizeTicker(request.ticker);
      result.set(normalizedTicker, await getMarketPriceSeries(normalizedTicker, request.earliestDate));
    }
  };

  await Promise.all(Array.from({ length: cappedConcurrency }, () => worker()));

  return result;
}

export function getPriceOnOrBefore(series: MarketPriceSeries, date: string): number | null {
  if (!series.points.length) {
    return null;
  }

  let left = 0;
  let right = series.points.length - 1;
  let bestIndex = -1;

  while (left <= right) {
    const middle = Math.floor((left + right) / 2);
    const pointDate = series.points[middle].date;
    if (pointDate <= date) {
      bestIndex = middle;
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }

  return bestIndex >= 0 ? series.points[bestIndex].price : null;
}

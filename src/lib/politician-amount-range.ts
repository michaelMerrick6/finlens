export type PoliticianAmountRange = {
  min: number;
  max: number;
  estimated: number;
};

const CONGRESS_AMOUNT_BUCKETS: Array<{ min: number; max: number }> = [
  { min: 1_001, max: 15_000 },
  { min: 15_001, max: 50_000 },
  { min: 50_001, max: 100_000 },
  { min: 100_001, max: 250_000 },
  { min: 250_001, max: 500_000 },
  { min: 500_001, max: 1_000_000 },
  { min: 1_000_001, max: 5_000_000 },
  { min: 5_000_001, max: 25_000_000 },
  { min: 25_000_001, max: 50_000_000 },
];

function normalizeAmountText(value: string | null | undefined): string {
  return String(value || '').trim();
}
function extractNumericBounds(value: string): number[] {
  return [...value.matchAll(/\$?([\d,]+(?:\.\d+)?)/g)]
    .map((match) => Number(match[1].replace(/,/g, '')))
    .filter((amount) => Number.isFinite(amount) && amount > 0);
}

function midpoint(min: number, max: number): number {
  return (min + max) / 2;
}

export function parsePoliticianAmountRange(value: string | null | undefined): PoliticianAmountRange | null {
  const raw = normalizeAmountText(value);
  if (!raw || raw.toLowerCase() === 'unknown') {
    return null;
  }

  const bounds = extractNumericBounds(raw);
  if (!bounds.length) {
    return null;
  }

  if (bounds.length >= 2) {
    const first = bounds[0];
    const second = bounds[1];
    const min = Math.min(first, second);
    const max = Math.max(first, second);
    return { min, max, estimated: midpoint(min, max) };
  }

  const single = bounds[0];
  const normalized = raw.toLowerCase();
  const explicitOpenEnded = /\bover\b|\babove\b|\bmore than\b|\+/.test(normalized);
  if (explicitOpenEnded) {
    return { min: single, max: single, estimated: single };
  }

  const matchingBucket = CONGRESS_AMOUNT_BUCKETS.find((bucket) => bucket.min === single);
  if (matchingBucket) {
    return {
      min: matchingBucket.min,
      max: matchingBucket.max,
      estimated: midpoint(matchingBucket.min, matchingBucket.max),
    };
  }

  return { min: single, max: single, estimated: single };
}

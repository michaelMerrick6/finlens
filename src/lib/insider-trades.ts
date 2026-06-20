export function normalizeInsiderDirection(value: string | null | undefined) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['buy', 'purchase', 'p', 'a'].includes(normalized)) {
    return 'buy' as const;
  }
  if (['sell', 'sale', 's', 'd'].includes(normalized)) {
    return 'sell' as const;
  }
  return 'other' as const;
}

export function isInsiderBuy(value: string | null | undefined) {
  return normalizeInsiderDirection(value) === 'buy';
}

export function isInsiderSell(value: string | null | undefined) {
  return normalizeInsiderDirection(value) === 'sell';
}

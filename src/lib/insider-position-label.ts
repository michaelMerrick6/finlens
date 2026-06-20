import type { TickerInsiderTransaction } from '@/lib/ticker-intelligence-types';

function formatCompactShareCount(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) {
    return null;
  }
  if (amount >= 1_000_000_000) return `${(amount / 1_000_000_000).toFixed(1)}B`;
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `${(amount / 1_000).toFixed(1)}K`;
  return amount.toLocaleString(undefined, {
    maximumFractionDigits: amount % 1 === 0 ? 0 : 2,
  });
}

function formatPct(value: number | null | undefined) {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount < 0) {
    return null;
  }

  return `${new Intl.NumberFormat('en-US', {
    maximumFractionDigits: amount < 0.1 ? 1 : 0,
  }).format(amount * 100)}%`;
}

export function buildInsiderPositionLabel(trade: TickerInsiderTransaction) {
  const sharesAfter = formatCompactShareCount(trade.sharesOwnedAfterTransaction);
  if (!sharesAfter || trade.direction === 'other') {
    return null;
  }

  const changePct = formatPct(trade.holdingChangePct);
  if (trade.direction === 'sell') {
    if (trade.sharesOwnedAfterTransaction === 0) {
      return changePct ? `Exited position after sale · reduced ${changePct}` : 'Exited position after sale';
    }
    return changePct
      ? `Still held ${sharesAfter} shares after sale · reduced ${changePct}`
      : `Still held ${sharesAfter} shares after sale`;
  }

  if (trade.sharesOwnedBeforeTransaction !== null && trade.sharesOwnedBeforeTransaction <= 0) {
    return `Opened position to ${sharesAfter} shares`;
  }

  return changePct
    ? `Now held ${sharesAfter} shares after buy · increased ${changePct}`
    : `Now held ${sharesAfter} shares after buy`;
}

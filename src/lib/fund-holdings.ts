type FundChangeFields = {
  qoq_change_percent?: number | string | null;
  qoq_change_shares?: number | string | null;
  shares_held?: number | string | null;
};

export type FundChangeKind = 'new' | 'increase' | 'decrease' | 'exit' | 'hold' | 'unknown';

const DE_MINIMIS_PREVIOUS_SHARE_COUNT = 100;
const DE_MINIMIS_PREVIOUS_SHARE_RATIO = 0.001;

function asNumber(value: number | string | null | undefined) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'string' && value.trim() === '') {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function previousShares(holding: FundChangeFields) {
  const sharesHeld = asNumber(holding.shares_held);
  const changeShares = asNumber(holding.qoq_change_shares);
  if (sharesHeld == null || changeShares == null) {
    return null;
  }
  return sharesHeld - changeShares;
}

function isMaterialNewPosition(holding: FundChangeFields) {
  const changeShares = asNumber(holding.qoq_change_shares);
  const sharesHeld = asNumber(holding.shares_held);
  const priorShares = previousShares(holding);

  if (changeShares == null || changeShares <= 0 || sharesHeld == null || sharesHeld <= 0) {
    return false;
  }
  if (asNumber(holding.qoq_change_percent) == null || priorShares == null || priorShares <= 0) {
    return true;
  }

  // Some 13F filings carry a tiny common-stock placeholder next to option exposure.
  // Treat the first material common-stock stake as a new position, not a giant percent increase.
  return priorShares <= DE_MINIMIS_PREVIOUS_SHARE_COUNT
    && priorShares / sharesHeld <= DE_MINIMIS_PREVIOUS_SHARE_RATIO;
}

function compactShareQuantity(value: number) {
  const absolute = Math.abs(value);
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

function formatShareDelta(value: number) {
  if (value === 0) {
    return 'No share change';
  }
  return `${value > 0 ? '+' : '-'}${compactShareQuantity(value)}`;
}

export function getFundChangeKind(holding: FundChangeFields): FundChangeKind {
  const changePercent = asNumber(holding.qoq_change_percent);
  const changeShares = asNumber(holding.qoq_change_shares);
  const sharesHeld = asNumber(holding.shares_held) ?? 0;

  if (changePercent == null && changeShares == null) {
    return 'unknown';
  }
  if (changeShares != null) {
    if (sharesHeld <= 0 && changeShares < 0) {
      return 'exit';
    }
    if (isMaterialNewPosition(holding)) {
      return 'new';
    }
    if (changeShares > 0) {
      return 'increase';
    }
    if (changeShares < 0) {
      return 'decrease';
    }
    return 'hold';
  }

  if (sharesHeld <= 0 && (changePercent ?? 0) < 0) {
    return 'exit';
  }
  if ((changePercent ?? 0) > 0) {
    return 'increase';
  }
  if ((changePercent ?? 0) < 0) {
    return 'decrease';
  }
  return 'hold';
}

export function hasFundChange(holding: FundChangeFields) {
  const kind = getFundChangeKind(holding);
  return kind !== 'unknown' && kind !== 'hold';
}

export function isPositiveFundChange(holding: FundChangeFields) {
  const kind = getFundChangeKind(holding);
  return kind === 'new' || kind === 'increase';
}

export function formatFundChangeLabel(holding: FundChangeFields) {
  const kind = getFundChangeKind(holding);
  const changeShares = asNumber(holding.qoq_change_shares);
  const changePercent = asNumber(holding.qoq_change_percent);
  if (kind === 'new') {
    return 'New position';
  }
  if (kind === 'exit') {
    return 'Exited';
  }
  if (changeShares != null) {
    return formatShareDelta(changeShares);
  }
  if (changePercent == null) {
    return '—';
  }
  if (changePercent === 0) {
    return 'No share change';
  }
  return `${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(Math.abs(changePercent) >= 10 ? 0 : 1)}% shares`;
}

export function formatFundTimelineAction(holding: FundChangeFields) {
  const kind = getFundChangeKind(holding);
  const changeShares = asNumber(holding.qoq_change_shares);
  const changeMagnitude = changeShares == null ? formatFundChangeLabel(holding) : compactShareQuantity(changeShares);
  if (kind === 'new') {
    return 'Started a new position';
  }
  if (kind === 'exit') {
    return 'Exited the position';
  }
  if (kind === 'increase') {
    return `Increased position by ${changeMagnitude}`;
  }
  if (kind === 'decrease') {
    return `Reduced position by ${changeMagnitude}`;
  }
  if (kind === 'hold') {
    return 'Held position flat';
  }
  return 'No recent 13F change';
}

export function fundFlowTone(holding: FundChangeFields | null | undefined) {
  if (!holding) {
    return 'No recent 13F delta';
  }
  const kind = getFundChangeKind(holding);
  if (kind === 'new' || kind === 'increase') {
    return 'Fund inflow';
  }
  if (kind === 'exit' || kind === 'decrease') {
    return 'Fund outflow';
  }
  if (kind === 'hold') {
    return 'Fund unchanged';
  }
  return 'No recent 13F delta';
}

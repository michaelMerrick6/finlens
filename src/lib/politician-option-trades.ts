type OptionBearingTrade = {
  asset_name?: string | null;
  asset_type?: string | null;
};

export type PoliticianOptionDetails = {
  badgeLabel: string;
  side: 'call' | 'put' | null;
  strikePrice: string | null;
  expirationDate: string | null;
  tooltip: string;
};

const OPTION_SUFFIX_RE = /\s*\|\s*(Call|Put) option\b.*$/i;
const OPTION_SIDE_RE = /\b(Call|Put) option\b/i;
const OPTION_STRIKE_RE = /\bStrike(?:\s+price)?(?:\s+of)?\s*\$?([0-9][\d,]*(?:\.\d+)?)\b/i;
const OPTION_EXPIRATION_RE = /\b(?:Expires|Expiration(?:\s+date)?)(?:\s+of)?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{2,4})\b/i;

function normalizeOptionDate(value: string | null): string | null {
  if (!value) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return value;
  }

  const match = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (!match) {
    return null;
  }

  const month = match[1].padStart(2, '0');
  const day = match[2].padStart(2, '0');
  const year = match[3].length === 2 ? `20${match[3]}` : match[3];
  return `${year}-${month}-${day}`;
}

export function stripPoliticianOptionMetadata(assetName: string | null | undefined): string {
  const base = String(assetName || '').replace(OPTION_SUFFIX_RE, '').trim();
  return base.replace(/\s*\[OP\]\s*$/i, '').trim();
}

export function parsePoliticianOptionDetails(trade: OptionBearingTrade): PoliticianOptionDetails | null {
  const assetName = String(trade.asset_name || '').trim();
  const assetType = String(trade.asset_type || '').trim().toUpperCase();
  const isOption =
    assetType === 'OP' ||
    /\[OP\]/i.test(assetName) ||
    /\b(Call|Put) option\b/i.test(assetName) ||
    /\bStrike\b/i.test(assetName) ||
    /\bExpires\b/i.test(assetName);

  if (!isOption) {
    return null;
  }

  const sideMatch = assetName.match(OPTION_SIDE_RE);
  const strikeMatch = assetName.match(OPTION_STRIKE_RE);
  const expirationMatch = assetName.match(OPTION_EXPIRATION_RE);
  const side = sideMatch ? (sideMatch[1].toLowerCase() as 'call' | 'put') : null;
  const strikePrice = strikeMatch ? strikeMatch[1] : null;
  const expirationDate = normalizeOptionDate(expirationMatch ? expirationMatch[1] : null);

  const tooltipParts = [side ? `${side[0].toUpperCase()}${side.slice(1)} option` : 'Option contract'];
  if (strikePrice) {
    tooltipParts.push(`Strike price $${strikePrice}`);
  }
  if (expirationDate) {
    tooltipParts.push(`Expiration ${expirationDate}`);
  }

  return {
    badgeLabel: side ? `${side[0].toUpperCase()}${side.slice(1)}` : 'Option',
    side,
    strikePrice,
    expirationDate,
    tooltip: tooltipParts.join(' | '),
  };
}

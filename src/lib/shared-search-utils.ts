/**
 * Shared search utilities used by both entity-search.ts and account-server.ts.
 *
 * Centralizes text normalization, ticker normalization, query tokenization,
 * Supabase ILIKE operand building, and dirty-name detection so that both
 * the public entity search and the authenticated account search stay in sync.
 */

export function normalizeSearchText(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function normalizeTickerCandidate(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9.\-]/g, '').slice(0, 15);
}

export function queryTokens(value: string): string[] {
  return normalizeSearchText(value).split(' ').filter(Boolean);
}

export function escapeSearchOperand(value: string): string {
  return value.replace(/[%(),]/g, ' ').replace(/\s+/g, ' ').trim();
}

export function buildIlikeOperands(
  column: string,
  values: string[],
  mode: 'contains' | 'prefix' = 'contains',
): string[] {
  const operands: string[] = [];
  for (const value of values) {
    const normalized = escapeSearchOperand(value);
    if (!normalized) {
      continue;
    }
    operands.push(mode === 'prefix' ? `${column}.ilike.${normalized}%` : `${column}.ilike.%${normalized}%`);
  }
  return operands;
}

export const DIRTY_COMPANY_NAME_PATTERNS = [
  'f s:',
  's o:',
  'subholding of',
  'filing status',
  ' fields law firm ',
  ' ira fbo ',
  ' morgan stanley ',
  'etrade',
  'e*trade',
  'trust ',
];

export const DIRTY_COMPANY_NAME_REGEXES = [
  /\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b/,
  /\$\d/,
  /\b\d{1,3}(?:,\d{3})+\s*-\s*\d{1,3}(?:,\d{3})+\b/,
  /\b(?:purchase|sale|bought|sold|call options?|put options?)\b/i,
];

import 'server-only';

import { getPublicSupabase } from '@/lib/supabase-server';
import {
  buildIlikeOperands,
  DIRTY_COMPANY_NAME_PATTERNS,
  DIRTY_COMPANY_NAME_REGEXES,
  normalizeSearchText,
  normalizeTickerCandidate,
  queryTokens,
} from '@/lib/shared-search-utils';

const OFFICIAL_SEC_COMPANY_TICKERS_URL = 'https://www.sec.gov/files/company_tickers_exchange.json';
const OFFICIAL_SEC_HEADERS = {
  'User-Agent': 'Vail/1.0 mikemerricka@gmail.com',
};
const MIN_COMPANY_SEARCH_SCORE = 40;
const CANONICAL_COMPANY_SUFFIX_TOKENS = new Set([
  'inc',
  'incorporated',
  'corp',
  'corporation',
  'co',
  'company',
  'ltd',
  'limited',
  'plc',
  'sa',
  'ag',
  'nv',
  'holdings',
]);

export type CompanySearchMatch = {
  ticker: string;
  name: string;
  score: number;
  exactMatch: boolean;
  strongMatch: boolean;
};

export type PoliticianSearchMatch = {
  id: string;
  first_name: string;
  last_name: string;
  party: string;
  chamber: string;
  state: string;
  fullName: string;
  score: number;
  exactMatch: boolean;
  strongMatch: boolean;
};

type CompanyRow = {
  ticker: string;
  name: string;
};

type PoliticianRow = {
  id: string;
  first_name: string;
  last_name: string;
  party: string;
  chamber: string;
  state: string;
};

let officialCompanyReferencePromise: Promise<CompanyRow[]> | null = null;



function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function compactSearchText(value: string): string {
  return normalizeSearchText(value).replace(/\s+/g, '');
}

function buildSearchFragments(query: string): string[] {
  const normalized = normalizeSearchText(query);
  const ticker = normalizeTickerCandidate(query);
  const parts = queryTokens(normalized);
  const fragments = [normalized, ticker.toLowerCase(), ...parts];
  for (const part of parts) {
    if (part.length >= 3) {
      fragments.push(part.slice(0, 3));
    }
    if (part.length >= 4) {
      fragments.push(part.slice(0, 4));
    }
  }
  return uniqueStrings(fragments.filter((value) => value.length >= 2));
}

function isDirtyCompanyName(value: string | null | undefined): boolean {
  const normalized = normalizeSearchText(value || '');
  if (!normalized) {
    return false;
  }
  return (
    DIRTY_COMPANY_NAME_PATTERNS.some((pattern) => normalized.includes(pattern)) ||
    DIRTY_COMPANY_NAME_REGEXES.some((pattern) => pattern.test(String(value || '')))
  );
}

function isLikelyTickerQuery(query: string, officialTickerSet: Set<string>): boolean {
  const trimmed = query.trim();
  const tickerCandidate = normalizeTickerCandidate(trimmed);
  if (!trimmed || !tickerCandidate) {
    return false;
  }

  if (/[.\-\d$]/.test(trimmed)) {
    return true;
  }

  if (officialTickerSet.has(tickerCandidate)) {
    return true;
  }

  return trimmed === trimmed.toUpperCase() && trimmed.length <= 6;
}

function hasCanonicalCompanySuffixOnly(normalizedName: string, normalizedQuery: string) {
  if (!normalizedQuery || !normalizedName.startsWith(`${normalizedQuery} `)) {
    return false;
  }

  const suffixTokens = normalizedName.slice(normalizedQuery.length).trim().split(' ').filter(Boolean);
  return suffixTokens.length > 0 && suffixTokens.every((token) => CANONICAL_COMPANY_SUFFIX_TOKENS.has(token));
}

function editDistanceWithin(a: string, b: string, maxDistance: number): number | null {
  if (!a || !b) {
    return null;
  }
  if (Math.abs(a.length - b.length) > maxDistance) {
    return null;
  }

  const previous = new Array(b.length + 1).fill(0);
  const current = new Array(b.length + 1).fill(0);
  for (let index = 0; index <= b.length; index += 1) {
    previous[index] = index;
  }

  for (let i = 1; i <= a.length; i += 1) {
    current[0] = i;
    let minInRow = current[0];
    for (let j = 1; j <= b.length; j += 1) {
      const substitutionCost = a[i - 1] === b[j - 1] ? 0 : 1;
      current[j] = Math.min(
        previous[j] + 1,
        current[j - 1] + 1,
        previous[j - 1] + substitutionCost,
      );
      minInRow = Math.min(minInRow, current[j]);
    }
    if (minInRow > maxDistance) {
      return null;
    }
    for (let j = 0; j <= b.length; j += 1) {
      previous[j] = current[j];
    }
  }

  return previous[b.length] <= maxDistance ? previous[b.length] : null;
}

function scoreCandidateText(query: string, values: string[]): number {
  const normalizedQuery = normalizeSearchText(query);
  const compactQuery = compactSearchText(query);
  const queryParts = queryTokens(query);
  let best = 0;

  for (const value of values) {
    const normalizedValue = normalizeSearchText(value);
    const compactValue = compactSearchText(value);
    const valueTokens = queryTokens(value);

    if (!normalizedValue) {
      continue;
    }

    if (compactValue === compactQuery || normalizedValue === normalizedQuery) {
      best = Math.max(best, 1000);
    } else if (compactValue.startsWith(compactQuery) || normalizedValue.startsWith(normalizedQuery)) {
      best = Math.max(best, 850);
    } else if (compactQuery.length >= 3 && compactValue.includes(compactQuery)) {
      best = Math.max(best, 700);
    }

    if (
      queryParts.length > 0 &&
      queryParts.every((part) => valueTokens.some((token) => token.startsWith(part)))
    ) {
      best = Math.max(best, 760);
    }

    for (const part of queryParts) {
      if (part.length < 3) {
        continue;
      }
      for (const token of valueTokens) {
        if (token.startsWith(part)) {
          best = Math.max(best, 720);
          continue;
        }
        const distance = editDistanceWithin(part, token, part.length >= 5 ? 2 : 1);
        if (distance === 1) {
          best = Math.max(best, 640);
        } else if (distance === 2) {
          best = Math.max(best, 590);
        }
      }
    }
  }

  return best;
}

function companyScore(row: CompanyRow, query: string, officialTickerSet: Set<string>): number {
  const ticker = normalizeTickerCandidate(row.ticker);
  const normalizedTicker = normalizeSearchText(ticker);
  const normalizedName = normalizeSearchText(row.name);
  const normalizedQuery = normalizeSearchText(query);
  const officialTicker = officialTickerSet.has(ticker);
  const tickerIntent = isLikelyTickerQuery(query, officialTickerSet);
  const nameScore = scoreCandidateText(query, [row.name]);
  const tickerScore = tickerIntent ? scoreCandidateText(query, [ticker]) : 0;
  let score = Math.max(nameScore, tickerScore);

  if (tickerIntent && ticker && ticker === normalizeTickerCandidate(query)) {
    score += officialTicker ? 220 : 40;
  }
  if (normalizedName === normalizedQuery) {
    score += officialTicker ? 180 : 120;
  }
  if (hasCanonicalCompanySuffixOnly(normalizedName, normalizedQuery)) {
    score += 180;
  }
  if (normalizedName.startsWith(normalizedQuery)) {
    score += officialTicker ? 90 : 60;
  }
  if (tickerIntent && normalizedTicker.startsWith(normalizedQuery)) {
    score += officialTicker ? 70 : 10;
  }
  if (officialTicker) {
    score += 40;
  }
  if (isDirtyCompanyName(row.name)) {
    score -= 700;
  }

  return score;
}

function politicianScore(row: PoliticianRow, query: string): number {
  const fullName = `${row.first_name} ${row.last_name}`.trim();
  const normalizedQuery = normalizeSearchText(query);
  let score = scoreCandidateText(query, [row.first_name, row.last_name, fullName]);

  if (normalizeSearchText(fullName) === normalizedQuery) {
    score += 120;
  }
  if (normalizeSearchText(fullName).startsWith(normalizedQuery)) {
    score += 60;
  }

  return score;
}

async function loadOfficialCompanyReference(): Promise<CompanyRow[]> {
  if (!officialCompanyReferencePromise) {
    officialCompanyReferencePromise = (async () => {
      const response = await fetch(OFFICIAL_SEC_COMPANY_TICKERS_URL, {
        headers: OFFICIAL_SEC_HEADERS,
        next: { revalidate: 24 * 60 * 60 },
      });
      if (!response.ok) {
        throw new Error(`SEC company reference fetch failed: ${response.status}`);
      }
      const payload = (await response.json()) as { data?: Array<[number, string, string, string]> };
      return (payload.data || [])
        .map((row) => ({
          ticker: normalizeTickerCandidate(String(row[2] || '')),
          name: String(row[1] || '').trim(),
        }))
        .filter((row) => row.ticker && row.name);
    })().catch((error) => {
      officialCompanyReferencePromise = null;
      throw error;
    });
  }
  return officialCompanyReferencePromise;
}

export async function searchCompaniesDetailed(query: string, limit = 10): Promise<CompanySearchMatch[]> {
  const supabase = getPublicSupabase();
  const trimmedQuery = query.trim();
  const normalizedQuery = normalizeSearchText(trimmedQuery);
  const tickerQuery = normalizeTickerCandidate(trimmedQuery);

  if (normalizedQuery.length < 2 && tickerQuery.length < 2) {
    return [];
  }

  const fragments = buildSearchFragments(trimmedQuery);
  const operands = [
    ...buildIlikeOperands('ticker', [tickerQuery, ...fragments.map((value) => value.toUpperCase())], 'prefix'),
    ...buildIlikeOperands('name', fragments),
  ];

  const response = await supabase
    .from('companies')
    .select('ticker,name')
    .or(operands.join(','))
    .limit(80);

  if (response.error) {
    throw new Error(response.error.message);
  }

  let officialRows: CompanyRow[] = [];
  try {
    officialRows = await loadOfficialCompanyReference();
  } catch {
    officialRows = [];
  }
  const officialTickerSet = new Set(officialRows.map((row) => row.ticker));

  const candidates = new Map<string, CompanyRow>();
  for (const row of officialRows) {
    if (companyScore(row, trimmedQuery, officialTickerSet) > MIN_COMPANY_SEARCH_SCORE) {
      candidates.set(row.ticker, row);
    }
  }

  for (const row of (response.data || []) as CompanyRow[]) {
    const ticker = normalizeTickerCandidate(row.ticker);
    if (!ticker) {
      continue;
    }
    const existing = candidates.get(ticker);
    if (!existing) {
      candidates.set(ticker, { ticker, name: row.name });
      continue;
    }
    if (isDirtyCompanyName(existing.name) && !isDirtyCompanyName(row.name)) {
      candidates.set(ticker, { ticker, name: row.name });
    }
  }

  return [...candidates.values()]
    .map((row) => {
      const score = companyScore(row, trimmedQuery, officialTickerSet);
      const normalizedName = normalizeSearchText(row.name);
      const tickerIntent = isLikelyTickerQuery(trimmedQuery, officialTickerSet);
      const exactMatch =
        normalizedName === normalizedQuery ||
        (tickerIntent && officialTickerSet.has(row.ticker) && row.ticker === tickerQuery);
      const strongMatch =
        exactMatch ||
        normalizedName.startsWith(normalizedQuery) ||
        (tickerIntent && officialTickerSet.has(row.ticker) && row.ticker.startsWith(tickerQuery));
      return {
        ...row,
        score,
        exactMatch,
        strongMatch,
      };
    })
    .filter((row) => row.score > MIN_COMPANY_SEARCH_SCORE && !isDirtyCompanyName(row.name))
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return left.ticker.localeCompare(right.ticker);
    })
    .slice(0, limit);
}

export async function searchPoliticiansDetailed(query: string, limit = 10): Promise<PoliticianSearchMatch[]> {
  const supabase = getPublicSupabase();
  const trimmedQuery = query.trim();
  const normalizedQuery = normalizeSearchText(trimmedQuery);

  if (normalizedQuery.length < 2) {
    return [];
  }

  const fragments = buildSearchFragments(trimmedQuery);
  const operands = [
    ...buildIlikeOperands('first_name', fragments),
    ...buildIlikeOperands('last_name', fragments),
  ];

  const response = await supabase
    .from('congress_members')
    .select('id,first_name,last_name,party,chamber,state')
    .or(operands.join(','))
    .limit(80);

  if (response.error) {
    throw new Error(response.error.message);
  }

  return ((response.data || []) as PoliticianRow[])
    .map((row) => {
      const fullName = `${row.first_name} ${row.last_name}`.trim();
      const score = politicianScore(row, trimmedQuery);
      const normalizedFullName = normalizeSearchText(fullName);
      const exactMatch = normalizedFullName === normalizedQuery;
      const strongMatch =
        exactMatch ||
        normalizedFullName.startsWith(normalizedQuery) ||
        normalizeSearchText(row.last_name).startsWith(normalizedQuery);
      return {
        ...row,
        fullName,
        score,
        exactMatch,
        strongMatch,
      };
    })
    .filter((row) => row.score > 0)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return left.fullName.localeCompare(right.fullName);
    })
    .slice(0, limit);
}

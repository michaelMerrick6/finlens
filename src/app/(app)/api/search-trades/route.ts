import { NextRequest, NextResponse } from 'next/server';
import {
  searchCompaniesDetailed,
  searchPoliticiansDetailed,
  type CompanySearchMatch,
} from '@/lib/entity-search';
import { routeErrorMessage } from '@/lib/api-errors';
import { filterDisplayPoliticianTrades, HOUSE_PRODUCT_START_DATE } from '@/lib/politician-trade-scope';
import { getPublicSupabase } from '@/lib/supabase-server';

export const dynamic = 'force-dynamic';

const TRADE_SELECT = `*, congress_members ( first_name, last_name, party, chamber, state )`;
const SEARCH_RESULT_LIMIT = 250;
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 50;
const MAX_SEARCH_LENGTH = 100;
const VALID_CHAMBERS = new Set(['All', 'House', 'Senate']);
const VALID_DIRECTIONS = new Set(['All', 'buy', 'sell']);

type TradeRow = {
  id: string;
  chamber?: string | null;
  doc_id?: string | null;
  member_id: string | null;
  politician_name: string | null;
  ticker: string | null;
  transaction_type: string | null;
  amount_range: string | null;
  source_url: string | null;
  published_date: string | null;
  created_at: string | null;
  transaction_date: string | null;
  asset_name?: string | null;
};

function sanitizeSearchValue(value: string): string {
  return value.replace(/[%(),]/g, ' ').replace(/\s+/g, ' ').trim();
}

function applyTradeFilters<T>(query: T, chamber: string, direction: string): T {
  let next = query as T & {
    eq: (column: string, value: string) => typeof next;
  };
  if (chamber !== 'All') {
    next = next.eq('chamber', chamber);
  }
  if (direction !== 'All') {
    next = next.eq('transaction_type', direction);
  }
  return next;
}

function applyDisplayTradeScope<T>(query: T): T {
  let next = query as T & {
    neq: (column: string, value: string) => typeof next;
    not: (column: string, operator: string, value: null | string) => typeof next;
  };
  next = next.not('ticker', 'is', null);
  next = next.neq('ticker', '');
  next = next.not('ticker', 'in', '("N/A","NA","UNKNOWN","MULTI")');
  return next;
}

function readBoundedInteger(value: string | null, fallback: number, maximum?: number, minimum = 0) {
  const parsed = Number.parseInt(value || '', 10);
  if (!Number.isFinite(parsed) || parsed < minimum) {
    return fallback;
  }
  return maximum ? Math.min(parsed, maximum) : parsed;
}

function pageResponse(trades: TradeRow[], offset: number, limit: number, mayHaveMore = false) {
  return NextResponse.json({
    trades: trades.slice(offset, offset + limit),
    hasMore: trades.length > offset + limit || mayHaveMore,
    nextOffset: offset + limit,
  });
}

function selectCompanyMatches(matches: CompanySearchMatch[]) {
  const preferred = matches.find((match) => match.exactMatch) || matches.find((match) => match.strongMatch);
  return preferred ? [preferred] : matches.slice(0, 3);
}

function sortTrades(rows: TradeRow[], tickerScores: Map<string, number>, memberScores: Map<string, number>): TradeRow[] {
  return [...rows].sort((left, right) => {
    const leftScore = Math.max(
      tickerScores.get(String(left.ticker || '').toUpperCase()) || 0,
      memberScores.get(String(left.member_id || '').toLowerCase()) || 0,
    );
    const rightScore = Math.max(
      tickerScores.get(String(right.ticker || '').toUpperCase()) || 0,
      memberScores.get(String(right.member_id || '').toLowerCase()) || 0,
    );
    if (rightScore !== leftScore) {
      return rightScore - leftScore;
    }
    const rightPublished = new Date(right.published_date || right.transaction_date || '').getTime();
    const leftPublished = new Date(left.published_date || left.transaction_date || '').getTime();
    if (rightPublished !== leftPublished) {
      return rightPublished - leftPublished;
    }
    const rightCreated = new Date(right.created_at || '').getTime();
    const leftCreated = new Date(left.created_at || '').getTime();
    if (rightCreated !== leftCreated) {
      return rightCreated - leftCreated;
    }
    return String(left.id || '').localeCompare(String(right.id || ''));
  });
}

export async function GET(request: NextRequest) {
  const supabase = getPublicSupabase();
  const { searchParams } = new URL(request.url);
  const query = (searchParams.get('q') || '').slice(0, MAX_SEARCH_LENGTH);
  const requestedChamber = searchParams.get('chamber') || 'All';
  const requestedDirection = searchParams.get('direction') || 'All';
  const chamber = VALID_CHAMBERS.has(requestedChamber) ? requestedChamber : 'All';
  const direction = VALID_DIRECTIONS.has(requestedDirection) ? requestedDirection : 'All';
  const limit = readBoundedInteger(searchParams.get('limit'), DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, 1);
  const offset = readBoundedInteger(searchParams.get('offset'), 0);
  const trimmedQuery = query.trim();

  if (!trimmedQuery) {
    const baseQuery = applyDisplayTradeScope(
      supabase
        .from('politician_trades')
        .select(TRADE_SELECT)
        .gte('published_date', HOUSE_PRODUCT_START_DATE)
        .order('published_date', { ascending: false })
        .order('created_at', { ascending: false })
        .order('id', { ascending: true })
        .range(offset, offset + limit),
    );

    const { data, error } = await applyTradeFilters(baseQuery, chamber, direction);
    if (error) {
      return NextResponse.json({ trades: [], error: routeErrorMessage(error, 'Failed to load trades.', 'search-trades') }, { status: 500 });
    }
    const trades = filterDisplayPoliticianTrades((data || []) as TradeRow[]);
    return NextResponse.json({
      trades: trades.slice(0, limit),
      hasMore: trades.length > limit,
      nextOffset: offset + Math.min(limit, trades.length),
    });
  }

  try {
    const [rawCompanyMatches, rawPoliticianMatches] = await Promise.all([
      searchCompaniesDetailed(trimmedQuery, 8),
      searchPoliticiansDetailed(trimmedQuery, 8),
    ]);

    const companyMatches = selectCompanyMatches(rawCompanyMatches);
    const politicianMatches = rawPoliticianMatches.slice(0, 5);

    const tickers = [...new Set(companyMatches.map((match) => match.ticker).filter(Boolean))];
    const memberIds = [...new Set(politicianMatches.map((match) => match.id).filter(Boolean))];
    const sanitizedQuery = sanitizeSearchValue(trimmedQuery);
    const requests = [];
    const searchFetchLimit = Math.min(offset + limit + 1, SEARCH_RESULT_LIMIT);

    if (tickers.length > 0) {
      requests.push(
        applyTradeFilters(
          applyDisplayTradeScope(
            supabase
              .from('politician_trades')
              .select(TRADE_SELECT)
              .in('ticker', tickers)
              .order('published_date', { ascending: false })
              .order('created_at', { ascending: false })
              .order('id', { ascending: true })
              .limit(searchFetchLimit),
          ),
          chamber,
          direction,
        ),
      );
    }

    if (memberIds.length > 0) {
      requests.push(
        applyTradeFilters(
          applyDisplayTradeScope(
            supabase
              .from('politician_trades')
              .select(TRADE_SELECT)
              .in('member_id', memberIds)
              .order('published_date', { ascending: false })
              .order('created_at', { ascending: false })
              .order('id', { ascending: true })
              .limit(searchFetchLimit),
          ),
          chamber,
          direction,
        ),
      );
    }

    if (tickers.length === 0 && memberIds.length === 0 && sanitizedQuery.length >= 2) {
      requests.push(
        applyTradeFilters(
          applyDisplayTradeScope(
            supabase
              .from('politician_trades')
              .select(TRADE_SELECT)
              .or(`politician_name.ilike.%${sanitizedQuery}%,ticker.ilike.%${sanitizedQuery}%`)
              .order('published_date', { ascending: false })
              .order('created_at', { ascending: false })
              .order('id', { ascending: true })
              .limit(searchFetchLimit),
          ),
          chamber,
          direction,
        ),
      );
    }

    const responses = await Promise.all(requests);
    const firstError = responses.find((response) => response.error);
    if (firstError?.error) {
      return NextResponse.json({ trades: [], error: routeErrorMessage(firstError.error, 'Failed to search trades.', 'search-trades') }, { status: 500 });
    }

    const tickerScores = new Map(companyMatches.map((match) => [match.ticker.toUpperCase(), match.score]));
    const memberScores = new Map(politicianMatches.map((match) => [match.id.toLowerCase(), match.score]));
    const merged = new Map<string, TradeRow>();

    for (const response of responses) {
      for (const trade of (response.data || []) as TradeRow[]) {
        if (trade.id) {
          merged.set(trade.id, trade);
        }
      }
    }

    const trades = filterDisplayPoliticianTrades(sortTrades([...merged.values()], tickerScores, memberScores));
    return pageResponse(trades, offset, limit);
  } catch (error) {
    const message = routeErrorMessage(error, 'Search failed.', 'search-trades');
    return NextResponse.json({ trades: [], error: message }, { status: 500 });
  }
}

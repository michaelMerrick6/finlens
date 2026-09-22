import { reviewPoliticianTrade } from "@/lib/reviewed-politician-trades";
import { NextRequest, NextResponse } from 'next/server';
import {
  searchCompaniesDetailed,
  searchPoliticiansDetailed,
  type CompanySearchMatch,
} from '@/lib/entity-search';
import { readFilteredPage } from '@/lib/filtered-page';
import { routeErrorMessage } from '@/lib/api-errors';
import { filterDisplayPoliticianTrades, HOUSE_PRODUCT_START_DATE } from '@/lib/politician-trade-scope';
import { getPublicSupabase } from '@/lib/supabase-server';

export const dynamic = 'force-dynamic';

const TRADE_SELECT = `*, congress_members ( first_name, last_name, party, chamber, state )`;
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

function selectCompanyMatches(matches: CompanySearchMatch[]) {
  const preferred = matches.find((match) => match.exactMatch) || matches.find((match) => match.strongMatch);
  return preferred ? [preferred] : matches.slice(0, 3);
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

  const memberIds = (searchParams.get('memberIds') || searchParams.get('memberId') || '').split(',').filter(Boolean);
  const exactTicker = (searchParams.get('ticker') || '').toUpperCase();
  if (memberIds.length > 25 || memberIds.some(id => !/^[A-Za-z0-9-]{1,70}$/.test(id)) || (exactTicker && !/^[A-Z0-9.-]{1,12}$/.test(exactTicker))) {
    return NextResponse.json({error:'Invalid disclosure filter.'},{status:400});
  }
  try {
    // Resolve the search once, then use the same raw-row cursor as browsing.
    // Ordering by filing date also keeps pages stable as filters discard rows.
    let searchTickers: string[] = [];
    let searchMemberIds: string[] = [];
    let textSearch = '';
    const searching = Boolean(trimmedQuery && !memberIds.length && !exactTicker);
    if (searching) {
      const [companyMatches, politicianMatches] = await Promise.all([
        searchCompaniesDetailed(trimmedQuery, 8),
        searchPoliticiansDetailed(trimmedQuery, 8),
      ]);
      searchTickers = [...new Set(selectCompanyMatches(companyMatches).map(match => match.ticker).filter(Boolean))];
      searchMemberIds = [...new Set(politicianMatches.slice(0, 5).map(match => match.id).filter(Boolean))];
      textSearch = sanitizeSearchValue(trimmedQuery);
      if (!searchTickers.length && !searchMemberIds.length && textSearch.length < 2) {
        return NextResponse.json({ trades: [], hasMore: false, nextOffset: offset });
      }
    }

    const page = await readFilteredPage<TradeRow>({
      offset,
      limit,
      include: (trade) => filterDisplayPoliticianTrades([trade]).length > 0,
      fetchRows: async (cursor, count) => {
        let query = applyTradeFilters(
          applyDisplayTradeScope(supabase.from('politician_trades').select(TRADE_SELECT)),
          chamber,
          direction,
        );
        if (!searching) query = query.gte('published_date', HOUSE_PRODUCT_START_DATE);
        if (memberIds.length) query = query.in('member_id', memberIds);
        if (exactTicker) query = query.eq('ticker', exactTicker);
        if (searchTickers.length && searchMemberIds.length) {
          const tickerValues = searchTickers.map(value => JSON.stringify(value)).join(',');
          const memberValues = searchMemberIds.map(value => JSON.stringify(value)).join(',');
          query = query.or(`ticker.in.(${tickerValues}),member_id.in.(${memberValues})`);
        } else if (searchTickers.length) {
          query = query.in('ticker', searchTickers);
        } else if (searchMemberIds.length) {
          query = query.in('member_id', searchMemberIds);
        } else if (searching) {
          query = query.or(`politician_name.ilike.%${textSearch}%,ticker.ilike.%${textSearch}%`);
        }
        const { data, error } = await query
          .order('published_date', { ascending: false })
          .order('created_at', { ascending: false })
          .order('id', { ascending: true })
          .range(cursor, cursor + count - 1);
        if (error) throw error;
        return ((data || []) as TradeRow[]).map(reviewPoliticianTrade);
      },
    });
    return NextResponse.json({ trades: page.rows, hasMore: page.hasMore, nextOffset: page.nextOffset });
  } catch (error) {
    const message = routeErrorMessage(error, 'Failed to load trades.', 'search-trades');
    return NextResponse.json({ trades: [], error: message }, { status: 500 });
  }
}

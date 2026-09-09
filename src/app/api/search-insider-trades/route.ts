import { NextRequest, NextResponse } from 'next/server';
import { routeErrorMessage } from '@/lib/api-errors';
import { getPublicSupabase } from '@/lib/supabase-server';

export const dynamic = 'force-dynamic';

const SEARCH_RESULT_LIMIT = 250;
const MAX_SEARCH_LENGTH = 100;
const TRADE_SELECT = 'id,ticker,filer_name,filer_relation,transaction_date,published_date,transaction_code,amount,price,value,source_url';

function sanitizeSearchValue(value: string): string {
    return value.replace(/[%(),]/g, ' ').replace(/\s+/g, ' ').trim();
}

export async function GET(request: NextRequest) {
    const supabase = getPublicSupabase();
    const { searchParams } = new URL(request.url);
    const query = (searchParams.get('q') || '').slice(0, MAX_SEARCH_LENGTH);
    const requestedDirection = searchParams.get('direction') || 'All';
    const direction = requestedDirection === 'buy' || requestedDirection === 'sell' ? requestedDirection : 'All';
    const sanitizedQuery = sanitizeSearchValue(query);

    let q = supabase
        .from('insider_trades')
        .select(TRADE_SELECT)
        .order('transaction_date', { ascending: false })
        .limit(SEARCH_RESULT_LIMIT);

    // Search by filer name or ticker
    if (sanitizedQuery) {
        q = q.or(`filer_name.ilike.%${sanitizedQuery}%,ticker.ilike.%${sanitizedQuery}%`);
    }

    // Filter by direction
    if (direction !== 'All') {
        const directionCodes =
            direction === 'buy'
                ? ['P', 'A', 'BUY', 'Buy', 'buy']
                : ['S', 'D', 'SELL', 'Sell', 'sell'];
        q = q.in('transaction_code', directionCodes);
    }

    const { data, error } = await q;

    if (error) {
        return NextResponse.json({ trades: [], error: routeErrorMessage(error, 'Failed to load insider trades.', 'search-insider-trades') }, { status: 500 });
    }

    return NextResponse.json({ trades: data || [] });
}

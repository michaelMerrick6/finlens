import { NextRequest, NextResponse } from 'next/server';
import { supabase } from '@/lib/supabase';

export async function GET(request: NextRequest) {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('q') || '';
    const direction = searchParams.get('direction') || 'All';

    let q = supabase
        .from('insider_trades')
        .select('*')
        .order('transaction_date', { ascending: false })
        .limit(500);

    // Search by filer name or ticker
    if (query) {
        q = q.or(`filer_name.ilike.%${query}%,ticker.ilike.%${query}%`);
    }

    // Filter by direction
    if (direction !== 'All') {
        q = q.eq('transaction_code', direction);
    }

    const { data, error } = await q;

    if (error) {
        return NextResponse.json({ trades: [], error: error.message }, { status: 500 });
    }

    return NextResponse.json({ trades: data || [] });
}

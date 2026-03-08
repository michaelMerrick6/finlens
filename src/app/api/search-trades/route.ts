import { NextRequest, NextResponse } from 'next/server';
import { supabase } from '@/lib/supabase';

export async function GET(request: NextRequest) {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('q') || '';
    const chamber = searchParams.get('chamber') || 'All';
    const direction = searchParams.get('direction') || 'All';

    let q = supabase
        .from('politician_trades')
        .select(`*, congress_members ( first_name, last_name, party, chamber, state )`)
        .order('published_date', { ascending: false })
        .limit(500);

    // Apply search — search politician_name and ticker (case-insensitive)
    if (query) {
        q = q.or(`politician_name.ilike.%${query}%,ticker.ilike.%${query}%`);
    }

    // Apply chamber filter
    if (chamber !== 'All') {
        q = q.eq('chamber', chamber);
    }

    // Apply direction filter
    if (direction !== 'All') {
        q = q.eq('transaction_type', direction);
    }

    const { data, error } = await q;

    if (error) {
        return NextResponse.json({ trades: [], error: error.message }, { status: 500 });
    }

    return NextResponse.json({ trades: data || [] });
}

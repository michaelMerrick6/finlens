import { NextResponse } from 'next/server';

import { requireClusterAccess } from '@/lib/account-server';
import { routeErrorMessage } from '@/lib/api-errors';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { getCongressBuyingTransactions } from '@/lib/congress-cluster-calendar-server';
import type { CongressClusterRange } from '@/lib/congress-cluster-calendar-types';

export const dynamic = 'force-dynamic';

const VALID_RANGES = new Set<CongressClusterRange>(['week', 'month', 'ytd']);
const VALID_TICKER = /^[A-Z0-9][A-Z0-9.-]{0,11}$/;

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    await requireClusterAccess(user);

    const params = new URL(request.url).searchParams;
    const requestedRange = params.get('range') as CongressClusterRange | null;
    const range = requestedRange && VALID_RANGES.has(requestedRange) ? requestedRange : 'month';
    const ticker = String(params.get('ticker') || '').trim().toUpperCase();
    if (!VALID_TICKER.test(ticker)) {
      return NextResponse.json({ error: 'A valid ticker is required.' }, { status: 400 });
    }

    const data = await getCongressBuyingTransactions(range, ticker);
    return NextResponse.json(data, {
      headers: {
        'Cache-Control': 'private, max-age=60, stale-while-revalidate=120',
      },
    });
  } catch (error) {
    if (error instanceof ApiRouteError) {
      return NextResponse.json({ code: error.code, error: error.message }, { status: error.status });
    }

    if (error instanceof Error && error.message.startsWith('Upgrade ')) {
      return NextResponse.json({ code: 'PRO_REQUIRED', error: error.message }, { status: 402 });
    }

    return NextResponse.json(
      { error: routeErrorMessage(error, 'Failed to load congressional transactions.', 'congress-buying-transactions') },
      { status: 500 },
    );
  }
}

import { NextResponse } from 'next/server';

import { requireClusterAccess } from '@/lib/account-server';
import { routeErrorMessage } from '@/lib/api-errors';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { getCongressCalendarTransactions } from '@/lib/congress-buying-calendar-server';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    await requireClusterAccess(user);
    const url = new URL(request.url);
    const data = getCongressCalendarTransactions(
      String(url.searchParams.get('date') || ''),
      String(url.searchParams.get('ticker') || ''),
    );
    if (!data) {
      return NextResponse.json({ error: 'A valid trade date and ticker are required.' }, { status: 400 });
    }

    return NextResponse.json(await data, {
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
      { error: routeErrorMessage(error, 'Failed to load calendar transactions.', 'congress-calendar-transactions') },
      { status: 500 },
    );
  }
}

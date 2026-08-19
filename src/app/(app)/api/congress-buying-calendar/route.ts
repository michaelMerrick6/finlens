import { NextResponse } from 'next/server';

import { requireClusterAccess } from '@/lib/account-server';
import { routeErrorMessage } from '@/lib/api-errors';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { getCongressBuyingCalendar } from '@/lib/congress-buying-calendar-server';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    await requireClusterAccess(user);
    const month = new URL(request.url).searchParams.get('month');
    const data = await getCongressBuyingCalendar(month);

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
      { error: routeErrorMessage(error, 'Failed to load the Congress buying calendar.', 'congress-buying-calendar') },
      { status: 500 },
    );
  }
}

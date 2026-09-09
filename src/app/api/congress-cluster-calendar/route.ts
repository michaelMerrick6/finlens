import { NextResponse } from 'next/server';

import { requireClusterAccess } from '@/lib/account-server';
import { routeErrorMessage } from '@/lib/api-errors';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { getCongressClusterCalendar } from '@/lib/congress-cluster-calendar-server';
import type { CongressClusterRange } from '@/lib/congress-cluster-calendar-types';

export const dynamic = 'force-dynamic';

const VALID_RANGES = new Set<CongressClusterRange>(['week', 'month', 'ytd']);

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    await requireClusterAccess(user);

    const requestedRange = new URL(request.url).searchParams.get('range') as CongressClusterRange | null;
    const range = requestedRange && VALID_RANGES.has(requestedRange) ? requestedRange : 'month';
    const data = await getCongressClusterCalendar(range);

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
      { error: routeErrorMessage(error, 'Failed to load Congress accumulation.', 'congress-cluster-calendar') },
      { status: 500 },
    );
  }
}

import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { requireClusterAccess } from '@/lib/account-server';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { getPublicClusterSignals } from '@/lib/public-data';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    await requireClusterAccess(user);
    const clusters = await getPublicClusterSignals();
    return NextResponse.json({ clusters });
  } catch (error) {
    if (error instanceof ApiRouteError) {
      return NextResponse.json({ clusters: [], code: error.code, error: error.message }, { status: error.status });
    }

    if (error instanceof Error && error.message.startsWith('Upgrade ')) {
      return NextResponse.json({ clusters: [], code: 'PRO_REQUIRED', error: error.message }, { status: 402 });
    }

    return NextResponse.json(
      { clusters: [], error: routeErrorMessage(error, 'Failed to load dashboard clusters.', 'dashboard-clusters') },
      { status: 500 },
    );
  }
}

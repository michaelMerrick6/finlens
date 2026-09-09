import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { requireClusterAccess } from '@/lib/account-server';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { getPublicClusterArchiveSignals, getPublicClusterSignals } from '@/lib/public-data';
import type { PublicClusterFeedSource } from '@/lib/public-data';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    const searchParams = new URL(request.url).searchParams;
    const includeHistory = searchParams.get('history') === '1';
    const source: PublicClusterFeedSource = searchParams.get('source') === 'insiders'
      ? 'insiders'
      : 'politicians';
    const [clusters] = await Promise.all([
      includeHistory ? getPublicClusterArchiveSignals(source) : getPublicClusterSignals(source),
      requireClusterAccess(user),
    ]);
    return NextResponse.json({ source, clusters, archiveLoaded: includeHistory });
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

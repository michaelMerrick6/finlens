import { NextRequest, NextResponse } from 'next/server';

import { requireClusterAccess } from '@/lib/account-server';
import { getDashboardClusterDetail } from '@/lib/dashboard-cluster-detail';
import { routeErrorMessage } from '@/lib/api-errors';
import { ApiRouteError, requireApiUser } from '@/lib/auth-server';
import { PUBLIC_BROADCAST_STORY_STATUSES } from '@/lib/tweet-candidates';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const candidateKey = (searchParams.get('key') || '').trim();

  if (!candidateKey) {
    return NextResponse.json({ error: 'Missing cluster key.' }, { status: 400 });
  }

  try {
    const user = await requireApiUser(request);
    await requireClusterAccess(user);
    const detail = await getDashboardClusterDetail(candidateKey, {
      statuses: PUBLIC_BROADCAST_STORY_STATUSES,
    });
    if (!detail) {
      return NextResponse.json({ error: 'Cluster not found.' }, { status: 404 });
    }
    return NextResponse.json(detail);
  } catch (error) {
    if (error instanceof ApiRouteError) {
      return NextResponse.json({ code: error.code, error: error.message }, { status: error.status });
    }

    if (error instanceof Error && error.message.startsWith('Upgrade ')) {
      return NextResponse.json({ code: 'PRO_REQUIRED', error: error.message }, { status: 402 });
    }

    return NextResponse.json(
      { error: routeErrorMessage(error, 'Failed to load cluster detail.', 'dashboard-cluster') },
      { status: 500 },
    );
  }
}

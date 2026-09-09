import { NextResponse } from 'next/server';

import { getClusterAlertsState, updateClusterAlerts } from '@/lib/account-server';
import { accountRouteErrorResponse } from '@/lib/account-route';
import { requireApiUser } from '@/lib/auth-server';
import type { ClusterAlertChannel } from '@/lib/account-types';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    return NextResponse.json({ ok: true, ...(await getClusterAlertsState(user)) });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Upgrade ')) {
      return NextResponse.json({ ok: false, code: 'PRO_REQUIRED', error: error.message }, { status: 402 });
    }

    return accountRouteErrorResponse(error);
  }
}

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as { enabled?: boolean; channels?: ClusterAlertChannel[] };
    const enabled = Boolean(body.enabled);

    const state = await updateClusterAlerts(user, enabled, body.channels || []);
    return NextResponse.json({ ok: true, ...state });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Upgrade ')) {
      return NextResponse.json({ ok: false, code: 'PRO_REQUIRED', error: error.message }, { status: 402 });
    }

    return accountRouteErrorResponse(error);
  }
}

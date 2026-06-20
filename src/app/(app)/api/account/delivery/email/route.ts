import { NextResponse } from 'next/server';

import { getAccountState, updateEmailDelivery } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as { alertEmail?: string; enabled?: boolean };

    await updateEmailDelivery(user, body.alertEmail || null, Boolean(body.enabled));
    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

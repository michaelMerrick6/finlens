import { NextResponse } from 'next/server';

import { accountRouteErrorResponse } from '@/lib/account-route';
import { getAccountState, updateSmsDelivery } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as { phoneNumber?: string; enabled?: boolean };

    await updateSmsDelivery(user, body.phoneNumber || null, Boolean(body.enabled));
    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

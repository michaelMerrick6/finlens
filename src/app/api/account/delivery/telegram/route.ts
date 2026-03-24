import { NextResponse } from 'next/server';

import { getAccountState, updateTelegramDelivery } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as { telegramUsername?: string; enabled?: boolean };

    await updateTelegramDelivery(user, body.telegramUsername || null, Boolean(body.enabled));
    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

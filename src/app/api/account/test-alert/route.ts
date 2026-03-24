import { NextResponse } from 'next/server';

import { getAccountState, sendAccountTestAlert } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const result = await sendAccountTestAlert(user);
    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, result, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

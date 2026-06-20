import { NextResponse } from 'next/server';

import { accountRouteErrorResponse } from '@/lib/account-route';
import { getAccountAlertHistory } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    const history = await getAccountAlertHistory(user);
    return NextResponse.json({ ok: true, history });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

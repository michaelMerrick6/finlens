import { NextResponse } from 'next/server';

import { accountRouteErrorResponse } from '@/lib/account-route';
import { getAccountAlertPreview } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    const alertPreview = await getAccountAlertPreview(user);
    return NextResponse.json({ ok: true, alertPreview });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

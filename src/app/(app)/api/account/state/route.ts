import { NextResponse } from 'next/server';

import { getAccountState } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    const url = new URL(request.url);
    const state = await getAccountState(user, {
      includeHistory: url.searchParams.get('history') !== '0',
      includeAlertPreview: url.searchParams.get('preview') !== '0',
    });
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

import { NextResponse } from 'next/server';

import { createCheckoutSession } from '@/lib/billing-server';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const url = await createCheckoutSession(user);
    return NextResponse.json({ ok: true, url });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

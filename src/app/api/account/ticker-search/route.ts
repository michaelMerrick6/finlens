import { NextResponse } from 'next/server';

import { searchTickerSuggestions } from '@/lib/account-server';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    await requireApiUser(request);
    const { searchParams } = new URL(request.url);
    const query = (searchParams.get('query') || '').trim();
    const suggestions = await searchTickerSuggestions(query);
    return NextResponse.json({ ok: true, suggestions });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

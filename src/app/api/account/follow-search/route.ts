import { NextResponse } from 'next/server';

import { searchInsiderSuggestions, searchPoliticianSuggestions } from '@/lib/account-server';
import type { ActorType } from '@/lib/account-types';
import { requireApiUser } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';

function parseActorType(value: string | null | undefined): ActorType {
  const normalized = (value || '').trim().toLowerCase();
  if (normalized === 'politician' || normalized === 'insider') {
    return normalized;
  }
  throw new Error('Invalid actor type.');
}

export async function GET(request: Request) {
  try {
    await requireApiUser(request);
    const { searchParams } = new URL(request.url);
    const actorType = parseActorType(searchParams.get('actorType'));
    const query = (searchParams.get('query') || '').trim();

    const suggestions =
      actorType === 'politician'
        ? await searchPoliticianSuggestions(query)
        : await searchInsiderSuggestions(query);

    return NextResponse.json({ ok: true, suggestions });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

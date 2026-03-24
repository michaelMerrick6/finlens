import { NextResponse } from 'next/server';

import {
  addActorFollow,
  addTickerFollow,
  deleteActorFollow,
  deleteTickerFollow,
  getAccountState,
  updateActorFollowMode,
  updateTickerFollowMode,
} from '@/lib/account-server';
import type { ActorType, AlertMode } from '@/lib/account-types';
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

function parseAlertMode(value: string | null | undefined): AlertMode {
  const normalized = (value || '').trim().toLowerCase();
  if (normalized === 'activity' || normalized === 'unusual' || normalized === 'both') {
    return normalized;
  }
  throw new Error('Invalid alert mode.');
}

export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as
      | { kind?: 'ticker'; ticker?: string; alertMode?: string }
      | { kind?: 'actor'; actorType?: string; actorName?: string; actorKey?: string; alertMode?: string };

    const kind = body.kind;
    if (kind === 'ticker') {
      await addTickerFollow(user, body.ticker || '', parseAlertMode(body.alertMode));
    } else if (kind === 'actor') {
      await addActorFollow(user, parseActorType(body.actorType), body.actorName || '', parseAlertMode(body.alertMode), body.actorKey || null);
    } else {
      return NextResponse.json({ ok: false, error: 'Invalid follow kind.' }, { status: 400 });
    }

    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

export async function PATCH(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as { kind?: 'ticker' | 'actor'; id?: string; alertMode?: string };

    if (!body.id) {
      return NextResponse.json({ ok: false, error: 'Missing follow id.' }, { status: 400 });
    }

    if (body.kind === 'ticker') {
      await updateTickerFollowMode(user, body.id, parseAlertMode(body.alertMode));
    } else if (body.kind === 'actor') {
      await updateActorFollowMode(user, body.id, parseAlertMode(body.alertMode));
    } else {
      return NextResponse.json({ ok: false, error: 'Invalid follow kind.' }, { status: 400 });
    }

    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

export async function DELETE(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = (await request.json()) as { kind?: 'ticker' | 'actor'; id?: string };

    if (!body.id) {
      return NextResponse.json({ ok: false, error: 'Missing follow id.' }, { status: 400 });
    }

    if (body.kind === 'ticker') {
      await deleteTickerFollow(user, body.id);
    } else if (body.kind === 'actor') {
      await deleteActorFollow(user, body.id);
    } else {
      return NextResponse.json({ ok: false, error: 'Invalid follow kind.' }, { status: 400 });
    }

    const state = await getAccountState(user);
    return NextResponse.json({ ok: true, state });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

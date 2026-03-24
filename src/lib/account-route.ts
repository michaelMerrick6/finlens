import 'server-only';

import { NextResponse } from 'next/server';

import { AccountSchemaError } from '@/lib/account-server';
import { ApiRouteError } from '@/lib/auth-server';

export function accountRouteErrorResponse(error: unknown) {
  if (error instanceof ApiRouteError) {
    return NextResponse.json(
      {
        ok: false,
        code: error.code,
        error: error.message,
      },
      { status: error.status }
    );
  }

  if (error instanceof AccountSchemaError) {
    return NextResponse.json(
      {
        ok: false,
        code: error.code,
        error: error.message,
      },
      { status: 503 }
    );
  }

  const message = error instanceof Error ? error.message : 'Unknown account error.';
  return NextResponse.json({ ok: false, code: 'ACCOUNT_ERROR', error: message }, { status: 500 });
}

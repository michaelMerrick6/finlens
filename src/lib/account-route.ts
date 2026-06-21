import 'server-only';

import { NextResponse } from 'next/server';

import { AccountSchemaError } from '@/lib/account-server';
import { ApiRouteError } from '@/lib/auth-server';

function isProduction() {
  return process.env.NODE_ENV === 'production';
}

function logAccountRouteError(error: unknown) {
  if (isProduction()) {
    console.error('[account-api]', error);
  }
}

function safeAccountErrorMessage(error: unknown) {
  const message = error instanceof Error ? error.message : 'Unknown account error.';
  if (!isProduction()) {
    return message;
  }

  const safePrefixes = [
    'Add ',
    'Could not confidently match',
    'Could not match',
    'Enable email or text',
    'Enter ',
    'Invalid ',
    'Missing ',
    'Upgrade ',
  ];

  if (safePrefixes.some((prefix) => message.startsWith(prefix)) || /\d+\/\d+ free follows used\./.test(message)) {
    return message;
  }

  return 'Account service is temporarily unavailable.';
}

export function accountRouteErrorResponse(error: unknown) {
  if (error instanceof ApiRouteError) {
    logAccountRouteError(error);
    return NextResponse.json(
      {
        ok: false,
        code: error.code,
        error: error.status >= 500 && isProduction() ? 'Service is temporarily unavailable.' : error.message,
      },
      { status: error.status }
    );
  }

  if (error instanceof AccountSchemaError) {
    logAccountRouteError(error);
    return NextResponse.json(
      {
        ok: false,
        code: error.code,
        error: isProduction() ? 'Account service is not ready.' : error.message,
      },
      { status: 503 }
    );
  }

  logAccountRouteError(error);
  return NextResponse.json({ ok: false, code: 'ACCOUNT_ERROR', error: safeAccountErrorMessage(error) }, { status: 500 });
}

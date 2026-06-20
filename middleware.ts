import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';

import { getOpsBasicAuthConfig, isOpsEnabled } from '@/lib/ops-access';

function decodeBasicAuthHeader(value: string | null) {
  if (!value) {
    return null;
  }

  const [scheme, encoded] = value.split(' ');
  if (scheme?.toLowerCase() !== 'basic' || !encoded) {
    return null;
  }

  try {
    const decoded = atob(encoded);
    const separatorIndex = decoded.indexOf(':');
    if (separatorIndex < 0) {
      return null;
    }

    return {
      username: decoded.slice(0, separatorIndex),
      password: decoded.slice(separatorIndex + 1),
    };
  } catch {
    return null;
  }
}

function notFoundResponse() {
  return new NextResponse('Not Found', { status: 404 });
}

function unauthorizedResponse() {
  return new NextResponse('Authentication required.', {
    status: 401,
    headers: {
      'WWW-Authenticate': 'Basic realm="Vail Ops", charset="UTF-8"',
    },
  });
}

export function middleware(request: NextRequest) {
  if (!isOpsEnabled()) {
    return notFoundResponse();
  }

  const authConfig = getOpsBasicAuthConfig();
  if (!authConfig) {
    if (process.env.NODE_ENV === 'production') {
      return notFoundResponse();
    }
    return NextResponse.next();
  }

  const credentials = decodeBasicAuthHeader(request.headers.get('authorization'));
  if (
    !credentials ||
    credentials.username !== authConfig.username ||
    credentials.password !== authConfig.password
  ) {
    return unauthorizedResponse();
  }

  return NextResponse.next();
}

export const config = {
  matcher: ['/ops/:path*', '/api/ops/:path*'],
};

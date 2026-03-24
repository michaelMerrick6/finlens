import 'server-only';

import type { User } from '@supabase/supabase-js';

import { getAdminSupabase } from '@/lib/supabase-admin';

export class ApiRouteError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function extractBearerToken(request: Request) {
  const authorization = request.headers.get('authorization') || '';
  const [scheme, token] = authorization.split(' ');
  if (scheme?.toLowerCase() !== 'bearer' || !token) {
    return null;
  }
  return token.trim();
}

export async function requireApiUser(request: Request): Promise<User> {
  const token = extractBearerToken(request);
  if (!token) {
    throw new ApiRouteError(401, 'AUTH_REQUIRED', 'Missing bearer token.');
  }

  const supabase = getAdminSupabase();
  const response = await supabase.auth.getUser(token);

  if (response.error || !response.data.user) {
    throw new ApiRouteError(401, 'AUTH_INVALID', response.error?.message || 'Invalid auth session.');
  }

  return response.data.user;
}

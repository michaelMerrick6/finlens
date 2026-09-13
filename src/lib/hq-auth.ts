import 'server-only';
import { scryptSync, timingSafeEqual } from 'node:crypto';
import { ApiRouteError, requireApiUser } from './auth-server';
export async function requireHqOwner(request: Request) {
  const user = await requireApiUser(request);
  const owner = process.env.VAIL_HQ_OWNER_EMAIL?.trim().toLowerCase();
  if (!owner || !user.email_confirmed_at || user.email?.toLowerCase() !== owner) {
    throw new ApiRouteError(403, 'HQ_FORBIDDEN', 'HQ is restricted to the configured owner.');
  }
  const [salt, hash] = (process.env.VAIL_HQ_PASSWORD_HASH || '').split(':');
  const password = request.headers.get('x-hq-password') || '';
  if (!salt || !/^[a-f0-9]{128}$/.test(hash || '') || !password || password.length > 128 ||
      !timingSafeEqual(scryptSync(password, salt, 64), Buffer.from(hash, 'hex'))) {
    throw new ApiRouteError(403, 'HQ_PASSWORD_REQUIRED', 'Enter the correct HQ password to continue.');
  }
  return user;
}

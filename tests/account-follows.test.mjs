import assert from 'node:assert/strict';
import { test } from 'node:test';
import { typescriptLoader } from './helpers/load-typescript.mjs';

test('follow writes send authenticated ownership and the resolved target to the atomic writer', async () => {
  const calls = [];
  const load = typescriptLoader({ '@/lib/supabase-admin': { getAdminSupabase: () => ({ rpc: async (name, args) => {
    calls.push({ name, args }); return { error: null };
  } }) } });
  await load('src/lib/account-follows-server.ts').saveAccountFollow('user-1', 'watchlist-1', {
    kind: 'actor', target: 'p000197', actorType: 'politician', actorName: 'Nancy Pelosi', alertMode: 'activity',
    metadata: { member_id: 'P000197' },
  });
  assert.equal(calls[0].name, 'save_account_follow');
  assert.equal(calls[0].args.p_user_id, 'user-1');
  assert.equal(calls[0].args.p_watchlist_id, 'watchlist-1');
  assert.equal(calls[0].args.p_target, 'p000197');
});

for (const [error, status, code] of [
  [{ code: 'P0001', message: 'FOLLOW_LIMIT_REACHED' }, 409, 'FOLLOW_LIMIT_REACHED'],
  [{ code: 'PGRST202', message: 'Missing function' }, 503, 'ACCOUNT_SCHEMA_MISSING'],
]) {
  test(`atomic follow failure ${code} is explicit and never falls back to an unsafe insert`, async () => {
    const load = typescriptLoader({ '@/lib/supabase-admin': { getAdminSupabase: () => ({ rpc: async () => ({ error }) }) } });
    await assert.rejects(load('src/lib/account-follows-server.ts').saveAccountFollow('u', 'w', {
      kind: 'ticker', target: 'AAPL', alertMode: 'activity',
    }), e => e.status === status && e.code === code);
  });
}

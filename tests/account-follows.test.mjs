import assert from 'node:assert/strict';
import { test } from 'node:test';
import { typescriptLoader } from './helpers/load-typescript.mjs';

test('strategy follows have one stable registered target and link back to the strategy', () => {
  const { strategyFollow, followedStrategy } = typescriptLoader()('src/lib/strategies/strategy-follow.ts');
  const strategy = strategyFollow('trump');
  assert.equal(strategy.actorKey, 'strategy:trump');
  assert.equal(followedStrategy({ actorType: 'politician', actorKey: strategy.actorKey }).href, '/analysis/strategies/trump');
  assert.equal(followedStrategy({ actorType: 'politician', actorKey: 'p000197' }), null);
  assert.equal(followedStrategy({ actorType: 'fund', actorKey: strategy.actorKey }), null);
  assert.equal(strategyFollow('not-a-strategy'), null);
});

test('strategy follows use the authenticated account route and existing actor removal', async () => {
  const calls = [];
  let authenticated = true;
  const route = typescriptLoader({
    'next/server': { NextResponse: { json: (body, init) => ({ body, status: init?.status ?? 200 }) } },
    '@/lib/auth-server': { requireApiUser: async () => { if (!authenticated) throw new Error('Sign in'); return { id: 'signed-in-user' }; } },
    '@/lib/account-server': {
      addStrategyFollow: async (user, id) => calls.push(['add', user.id, id]),
      deleteActorFollow: async (user, id) => calls.push(['remove', user.id, id]),
      getAccountState: async user => ({ user }),
    },
    '@/lib/account-route': { accountRouteErrorResponse: error => ({ status: 401, error: error.message }) },
  })('src/app/api/account/follows/route.ts');
  const request = body => ({ json: async () => body });
  const result = await route.POST(request({ kind: 'strategy', strategyId: 'trump', userId: 'spoofed-user' }));
  assert.equal(result.status, 200);
  assert.equal(result.body.state.user.id, 'signed-in-user');
  await route.DELETE(request({ kind: 'actor', id: 'saved-follow' }));
  authenticated = false;
  assert.equal((await route.POST(request({ kind: 'strategy', strategyId: 'trump' }))).status, 401);
  assert.deepEqual(calls, [['add', 'signed-in-user', 'trump'], ['remove', 'signed-in-user', 'saved-follow']]);
});

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

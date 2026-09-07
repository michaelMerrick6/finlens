import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import ts from 'typescript';

function harness() {
  const state = { status: 'canceled', subscriptions: [], sessions: [], updates: [], processed: new Set(), created: new Map(), failRetrieve: false };
  const subscription = () => ({ id: 'sub1', customer: 'cus1', status: state.status, metadata: { supabase_user_id: 'user1' }, items: { data: [{ price: { id: 'price1' }, current_period_end: 2000000000 }] } });
  const stripe = {
    subscriptions: {
      list: () => (async function* () { yield* state.subscriptions; })(),
      retrieve: async () => { if (state.failRetrieve) throw new Error('Stripe unavailable'); return subscription(); },
    },
    checkout: { sessions: {
      list: async () => ({ data: [...state.sessions] }),
      create: async (params, options) => {
        if (!state.created.has(options.idempotencyKey)) state.created.set(options.idempotencyKey, { url: 'https://checkout.example/new', params });
        return state.created.get(options.idempotencyKey);
      },
    } },
  };
  const db = { from(table) {
    let id, update;
    const q = {
      select() { return q; }, eq(key, value) { id = value; return q; },
      update(value) { update = value; return q; },
      async single() { state.updates.push(update); return { data: { id: 'user1' }, error: null }; },
      async maybeSingle() { return { data: table === 'profiles' ? { id: 'user1', stripe_customer_id: 'cus1' } : state.processed.has(id) ? { id } : null, error: null }; },
      async insert(value) { state.processed.add(value.id); return { error: null }; },
    };
    return q;
  } };
  const environment = { STRIPE_SECRET_KEY: 'test', STRIPE_VAIL_PRO_PRICE_ID: 'price1' };
  function compile(path, dependencies) {
    const exports = {};
    const code = ts.transpileModule(fs.readFileSync(path, 'utf8'), { compilerOptions: { esModuleInterop: true, module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
    vm.runInNewContext(code, { exports, process: { env: environment }, require: (name) => {
      if (!(name in dependencies)) throw new Error(`Unexpected dependency: ${name}`);
      return dependencies[name];
    } });
    return exports;
  }
  class ApiRouteError extends Error { constructor(status, code, message) { super(message); this.status = status; this.code = code; } }
  const billing = compile('src/lib/billing-server.ts', {
    'server-only': {}, stripe: function Stripe() { return stripe; },
    '@/lib/billing-config': compile('src/lib/billing-config.ts', {}),
    '@/lib/auth-server': { ApiRouteError }, '@/lib/supabase-admin': { getAdminSupabase: () => db },
  });
  return { state, billing, event: (id, status) => ({ id, type: 'customer.subscription.updated', data: { object: { ...subscription(), status } } }) };
}

test('late active event cannot restore access after cancellation', async () => {
  const { state, billing, event } = harness();
  await billing.syncBillingFromStripeEvent(event('new', 'canceled'));
  await billing.syncBillingFromStripeEvent(event('old', 'active'));
  assert.deepEqual(state.updates.map(row => row.billing_status), ['canceled', 'canceled']);
  assert.equal(state.updates.at(-1).follow_limit, 3);
});

test('failed Stripe read remains retryable; processed events are skipped', async () => {
  const { state, billing, event } = harness();
  state.failRetrieve = true;
  await assert.rejects(billing.syncBillingFromStripeEvent(event('one', 'active')), /Stripe unavailable/);
  assert.equal(state.processed.size, 0);
  state.failRetrieve = false;
  await billing.syncBillingFromStripeEvent(event('one', 'active'));
  await billing.syncBillingFromStripeEvent(event('one', 'active'));
  assert.equal(state.updates.length, 1);
});

for (const status of ['active', 'trialing', 'past_due', 'incomplete', 'unpaid', 'paused']) {
  test(`checkout rejects an existing ${status} subscription even with a stale profile`, async () => {
    const { state, billing } = harness();
    state.subscriptions = [{ status }];
    await assert.rejects(billing.createCheckoutSession({ id: 'user1' }), error => error.code === 'BILLING_SUBSCRIPTION_EXISTS');
    assert.equal(state.created.size, 0);
  });
}

test('open subscription checkout is reused', async () => {
  const { state, billing } = harness();
  state.sessions = [{ id: 'open1', mode: 'subscription', status: 'open', url: 'https://checkout.example/open1' }];
  assert.equal(await billing.createCheckoutSession({ id: 'user1' }), state.sessions[0].url);
  assert.equal(state.created.size, 0);
});

test('concurrent initial checkouts share an idempotency key', async () => {
  const { state, billing } = harness();
  await Promise.all([billing.createCheckoutSession({ id: 'user1' }), billing.createCheckoutSession({ id: 'user1' })]);
  assert.equal(state.created.size, 1);
});

test('expired checkout permits a new session after cancellation', async () => {
  const { state, billing } = harness();
  state.subscriptions = [{ status: 'canceled' }];
  state.sessions = [{ id: 'expired1', mode: 'subscription', status: 'expired' }];
  await billing.createCheckoutSession({ id: 'user1' });
  assert.ok(state.created.has('vail-checkout-cus1-expired1'));
});

test('completed checkout blocks a second purchase before the subscription list catches up', async () => {
  const { state, billing } = harness();
  state.status = 'active';
  state.sessions = [{ id: 'done1', mode: 'subscription', status: 'complete', subscription: 'sub1' }];
  await assert.rejects(billing.createCheckoutSession({ id: 'user1' }), error => error.code === 'BILLING_SUBSCRIPTION_EXISTS');
  assert.equal(state.created.size, 0);
});

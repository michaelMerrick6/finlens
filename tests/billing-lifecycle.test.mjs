import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import Stripe from 'stripe';
import ts from 'typescript';

function harness() {
  const env = { NODE_ENV: 'production', STRIPE_SECRET_KEY: 'sk_test_local_only', STRIPE_WEBHOOK_SECRET: 'whsec_local_test', STRIPE_VAIL_PRO_PRICE_ID: 'price_pro' };
  const profiles = [1, 2].map(n => ({ id: `user${n}`, stripe_customer_id: `cus${n}`, billing_status: 'free', billing_plan_key: 'free', follow_limit: 3 }));
  const events = new Set(); const calls = [];
  const state = { failUpdate: false, failRetrieve: false };
  const subscriptions = new Map();
  function subscription(user, status = 'active', extra = {}) {
    const value = { id: `sub${user}`, customer: `cus${user}`, status, created: user, metadata: { supabase_user_id: `user${user}` },
      items: { data: [{price: {id: 'price_pro'}, current_period_end: 2000000000}] }, ...extra };
    subscriptions.set(value.id, value); return value;
  }
  subscription(1); subscription(2);
  const db = { auth: { getUser: async token => ({data: {user: ['token1','token2'].includes(token) ? {id: `user${token.at(-1)}`} : null},error:null}) }, from(table) {
    const filters = []; let update; let insert;
    const execute = async () => {
      if (insert && table === 'stripe_webhook_events') { const duplicate=events.has(insert.id); events.add(insert.id); return {error: duplicate ? {code:'23505'} : null}; }
      if (table === 'stripe_webhook_events') return {data: events.has(filters[0][1]) ? {id:filters[0][1]} : null, error:null};
      assert.equal(table,'profiles');
      const row = profiles.find(p => filters.every(([k,v]) => p[k] === v));
      if(update) { if(state.failUpdate) return {data:null,error:new Error('private database failure')}; if(row) Object.assign(row,update); }
      return {data: row || null,error:null};
    };
    const q = {select(){return q;},eq(k,v){filters.push([k,v]);return q;},update(value){update=value;return q;},insert(value){insert=value;return q;},
      maybeSingle:execute,single:execute,then(resolve,reject){return execute().then(resolve,reject);} };
    return q;
  } };
  const realStripe = new Stripe(env.STRIPE_SECRET_KEY);
  const stripe = { webhooks: realStripe.webhooks,
    subscriptions: { retrieve: async id => {if(state.failRetrieve) throw new Error('private Stripe failure');return subscriptions.get(id);},
      list: ({customer}) => (async function*(){yield* [...subscriptions.values()].filter(s=>s.customer===customer);})() },
    billingPortal: {sessions: {create: async params => {calls.push({kind:'portal',...params});return {url:'https://billing.example/session'};}}},
    checkout: {sessions: {list:async()=>({data:[]}),create:async params=>{calls.push({kind:'checkout',...params});return {url:'https://checkout.example/session'};}}},
  };
  const modules = new Map();
  const overrides = { 'server-only': {}, stripe: function(){return stripe;}, '@/lib/supabase-admin': {getAdminSupabase:()=>db},
    'next/server': {NextResponse:{json:(body,options={})=>({body,status:options.status??200})}},
    '@/lib/account-route': {accountRouteErrorResponse:error=>({status:error.status||500,body:{ok:false}})},
    '@/lib/api-errors': {routeErrorMessage:()=> 'Stripe webhook failed.'} };
  function load(path) {
    if(modules.has(path))return modules.get(path);
    const exports = {}; modules.set(path,exports);
    const code=ts.transpileModule(fs.readFileSync(path,'utf8'),{compilerOptions:{esModuleInterop:true,module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
    vm.runInNewContext(code,{exports,process:{env},Date,require:name=> {
      if(name in overrides)return overrides[name];
      if(name.startsWith('@/'))return load(`src/${name.slice(2)}.ts`);
      throw new Error(`Unexpected dependency ${name}`);
    }});return exports;
  }
  const billing=load('src/lib/billing-server.ts'); const webhook=load('src/app/api/stripe/webhook/route.ts');
  const event=(id,sub=subscriptions.get('sub1'))=>({id,type:'customer.subscription.updated',livemode:false,data:{object:sub}});
  async function post(value,signature) {
    const payload=JSON.stringify(value);
    const signed=signature ?? realStripe.webhooks.generateTestHeaderString({payload,secret:env.STRIPE_WEBHOOK_SECRET});
    return webhook.POST(new Request('http://localhost/api/stripe/webhook',{method:'POST',headers:{'stripe-signature':signed},body:payload}));
  }
  return {profiles,events,calls,state,subscriptions,subscription,billing,event,post,load,env};
}

test('signed webhook grants the matching account and duplicate delivery is harmless',async()=>{
  const h=harness(); assert.equal((await h.post(h.event('evt1'))).status,200);
  assert.equal(h.profiles[0].follow_limit,25);assert.equal(h.profiles[1].follow_limit,3);
  assert.equal((await h.post(h.event('evt1'))).body.duplicate,true);assert.equal(h.events.size,1);
});
test('invalid signature cannot mutate profiles or webhook history',async()=>{
  const h=harness();assert.equal((await h.post(h.event('evt1'),'invalid')).status,400);
  assert.equal(h.profiles[0].follow_limit,3);assert.equal(h.events.size,0);
});
test('transient database failure returns 500 and succeeds on retry',async()=>{
  const h=harness();h.state.failUpdate=true;let response=await h.post(h.event('evt1'));
  assert.equal(response.status,500);assert.ok(!JSON.stringify(response).includes('private'));assert.equal(h.events.size,0);
  h.state.failUpdate=false;response=await h.post(h.event('evt1'));assert.equal(response.status,200);assert.equal(h.events.size,1);
});
test('Stripe outage remains retryable without granting access',async()=>{
  const h=harness();h.state.failRetrieve=true;assert.equal((await h.post(h.event('evt1'))).status,500);
  assert.equal(h.events.size,0);assert.equal(h.profiles[0].follow_limit,3);
});
test('metadata for another customer cannot transfer billing ownership',async()=>{
  const h=harness();h.subscription(1,'active',{metadata:{supabase_user_id:'user2'}});
  assert.equal((await h.post(h.event('evt1'))).status,500);assert.equal(h.events.size,0);
  assert.deepEqual(h.profiles.map(p=>p.stripe_customer_id),['cus1','cus2']);assert.deepEqual(h.profiles.map(p=>p.follow_limit),[3,3]);
});
test('active subscription to an unknown price cannot grant Pro',async()=>{
  const h=harness();h.subscription(1,'active',{items:{data:[{price:{id:'unrelated_price'}}]}});
  assert.equal((await h.post(h.event('evt1'))).status,200);assert.equal(h.profiles[0].follow_limit,3);
});
test('old cancellation does not downgrade a newer active subscription',async()=>{
  const h=harness();const old=h.subscription(1,'canceled');
  h.subscription(1,'active',{id:'sub_replacement',created:10});
  assert.equal((await h.post(h.event('evt_old',old))).status,200);
  assert.equal(h.profiles[0].stripe_subscription_id,'sub_replacement');assert.equal(h.profiles[0].follow_limit,25);
});
test('cancellation without replacement removes paid access',async()=>{
  const h=harness();await h.post(h.event('active'));h.subscription(1,'canceled');await h.post(h.event('cancelled'));
  assert.equal(h.profiles[0].follow_limit,3);assert.equal(h.profiles[0].billing_status,'canceled');
});
for(const endpoint of ['portal','checkout'])test(`${endpoint} uses the bearer account, ignoring another user in the request body`,async()=>{
  const h=harness();if(endpoint==='checkout'){h.subscriptions.clear();}
  const route=h.load(`src/app/api/account/billing/${endpoint}/route.ts`);
  const response=await route.POST(new Request(`http://localhost/api/account/billing/${endpoint}`,{method:'POST',headers:{authorization:'Bearer token1'},body:JSON.stringify({userId:'user2',customer:'cus2'})}));
  assert.equal(response.status,200);assert.equal(h.calls[0].customer,'cus1');
  for(const token of ['', 'Bearer invalid']){
    assert.equal((await route.POST(new Request('http://localhost',{method:'POST',headers:{authorization:token}}))).status,401);
  }
  assert.equal(h.calls.length,1);
});

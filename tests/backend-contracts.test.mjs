import assert from 'node:assert/strict';
import * as crypto from 'node:crypto';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import ts from 'typescript';

function load(path, dependencies = {}) {
  const exports = {};
  const code = ts.transpileModule(fs.readFileSync(path, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  vm.runInNewContext(code, { exports, Date, URL, process: { env: { NODE_ENV: 'production' } },
    require(name) { if (!(name in dependencies)) throw new Error(`Unexpected dependency ${name}`); return dependencies[name]; },
  });
  return exports;
}
const next = { NextResponse: { json: (body, options = {}) => ({ body, status: options.status ?? 200 }) } };

test('history crosses database page limits and detects a truncated result', async () => {
  const { readBoundedRows } = load('src/lib/bounded-rows.ts');
  const source = Array.from({ length: 1500 }, (_, id) => ({ id }));
  const requests = [];
  const result = await readBoundedRows(1200, async (offset, count) => {
    requests.push([offset, count]); return source.slice(offset, offset + count);
  });
  assert.equal(result.rows.length, 1200);
  assert.equal(result.rows.at(-1).id, 1199);
  assert.equal(result.hasMore, true);
  assert.deepEqual(requests, [[0,500],[500,500],[1000,201]]);
  const exact = await readBoundedRows(1200, async (offset, count) => source.slice(0,1200).slice(offset,offset+count));
  assert.equal(exact.hasMore, false);
  await assert.rejects(readBoundedRows(1200, async () => { throw new Error('database unavailable'); }), /unavailable/);
});

function statsRoute(failed = false) {
  const selects = [];
  const db = { from(table) {
    const response = { count: table === 'signal_events' ? 400 : 2500, error: failed ? { message: 'private db detail' } : null,
      data: table === 'signal_events' ? [{ ticker: 'ABC', title: 'Cluster', created_at: '2026-01-01' }] : null };
    const query = { select(columns, options) { selects.push({table,options}); return query; },
      gte() {return query;}, eq() {return query;}, in() {return query;}, order() {return query;}, limit() {return query;},
      then(resolve, reject) {return Promise.resolve(response).then(resolve,reject);} };
    return query;
  } };
  return { selects, ...load('src/app/api/auth-stats/route.ts', {'next/server': next,
    '@/lib/supabase-server': {getPublicSupabase: () => db}, '@/lib/api-errors': {routeErrorMessage: () => 'Unavailable'} }) };
}
test('statistics use exact counts beyond row caps', async () => {
  const route = statsRoute(); const response = await route.GET();
  assert.equal(response.status,200);
  assert.equal(response.body.stats.congressTradesLastWeek,2500);
  assert.equal(response.body.stats.clusterCount,1200);
  assert.ok(route.selects.every(s => s.options.count === 'exact'));
});
test('statistics failure is unavailable, not successful zero activity', async () => {
  const response = await statsRoute(true).GET();
  assert.equal(response.status,503); assert.equal(response.body.stats,null);
  assert.ok(!JSON.stringify(response).includes('private db detail'));
});
for (const failures of [1,2]) {
  test(`autocomplete reports ${failures === 1 ? 'partial' : 'total'} source failure`, async () => {
    const route = load('src/app/api/search-autocomplete/route.ts', {'next/server': next,
      '@/lib/company-logos': {getTickerLogoUrl: () => null}, '@/lib/entity-search': {
        searchCompaniesDetailed: async () => {throw new Error('unavailable');},
        searchPoliticiansDetailed: async () => {if(failures===2) throw new Error('unavailable'); return [];},
      }});
    const response = await route.GET({url:'https://example.com/api/search-autocomplete?q=test'});
    assert.equal(response.status,failures===2 ? 503 : 200);
    assert.equal(response.body.partial,true); assert.equal(response.body.failedSources.length,failures);
  });
}
test('authentication rejects missing tokens before accessing the database', async () => {
  const auth = load('src/lib/auth-server.ts', {'server-only': {}, '@/lib/supabase-admin': {
    getAdminSupabase() {throw new Error('must not access database');},
  }});
  await assert.rejects(auth.requireApiUser({headers:{get:()=>null}}), error=>error.status===401);
});

test('disclosure scopes use exact member IDs and ticker, and reject unsafe filter syntax', async () => {
  const filters=[];
  const db={from(){const q={select(){return q;},gte(){return q;},order(){return q;},range(){return q;},not(){return q;},neq(){return q;},
    eq(k,v){filters.push([k,v]);return q;},in(k,v){filters.push([k,[...v]]);return q;},then(resolve){return Promise.resolve({data:[],error:null}).then(resolve);}};return q;}};
  const route=load('src/app/api/search-trades/route.ts',{
    '@/lib/reviewed-politician-trades':load('src/lib/reviewed-politician-trades.ts'),
    'next/server':next,'@/lib/entity-search':{},'@/lib/filtered-page':load('src/lib/filtered-page.ts'),
    '@/lib/api-errors':{routeErrorMessage:()=> 'Unavailable'},
    '@/lib/politician-trade-scope':{filterDisplayPoliticianTrades:r=>r,HOUSE_PRODUCT_START_DATE:'2015-01-01'},
    '@/lib/supabase-server':{getPublicSupabase:()=>db}
  });
  const result=await route.GET(new Request('http://localhost/api/search-trades?memberIds=B001236,M001232&ticker=MSFT&direction=buy'));
  assert.equal(result.status,200);assert.deepEqual(filters,[['transaction_type','buy'],['member_id',['B001236','M001232']],['ticker','MSFT']]);
  const invalid=await route.GET(new Request('http://localhost/api/search-trades?memberId=bad%29id'));
  assert.equal(invalid.status,400);
});

test('politician workspace pagination accepts offsets beyond the old 50-row ceiling', async () => {
  let captured;
  const route=load('src/app/api/politician-workspace/[memberId]/route.ts',{'next/server':next,
    '@/lib/api-errors':{routeErrorMessage:()=> 'Unavailable'},
    '@/lib/politician-workspace-server':{getPoliticianWorkspaceData:async(id,options)=>{captured=options;return {memberId:id};}}
  });
  await route.GET(new Request('http://localhost/api/politician-workspace/B001236?offset=120&limit=20'),{params:Promise.resolve({memberId:'B001236'})});
  assert.equal(captured.offset,120);assert.equal(captured.limit,20);
  await route.GET(new Request('http://localhost/api/politician-workspace/B001236'),{params:Promise.resolve({memberId:'B001236'})});
  assert.equal(captured.limit,8);
});

test('new-profile email opt-in migration preserves existing preferences', () => {
  const sql=fs.readFileSync('supabase_vail_phase12_tracking_opt_in.sql','utf8');
  assert.match(sql,/ALTER COLUMN email_enabled SET DEFAULT FALSE/i);
  assert.doesNotMatch(sql,/UPDATE\s+public\.profiles/i);
});


test('source-reviewed Pelosi corrections distinguish contributions and exercised stock', () => {
  const { reviewPoliticianTrade } = load('src/lib/reviewed-politician-trades.ts');
  const base = { member_id: 'P000197', source_url: 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf' };
  const contribution = reviewPoliticianTrade({...base,doc_id:'house-2026-20033725-9',amount_range:'$5,000,001'});
  assert.equal(contribution.is_contribution,true);
  assert.equal(contribution.amount_range,'$5,000,001 - $25,000,000');
  const exercise = reviewPoliticianTrade({...base,doc_id:'house-2026-20033725-1',asset_type:'OP'});
  assert.equal(exercise.asset_type,'ST');
  assert.match(exercise.asset_name,/5,000 shares/);
  const option = reviewPoliticianTrade({...base,doc_id:'house-2026-20033725-2',asset_type:'OP'});
  assert.equal(option.asset_type,'OP');
  const unrelated = {...base,doc_id:'house-2026-20033725-9',member_id:'X000001'};
  assert.equal(reviewPoliticianTrade(unrelated),unrelated);
});


test('HQ denies access when no owner is configured, even for authenticated users', async () => {
  class ApiRouteError extends Error { constructor(status,code,message){super(message);this.status=status;} }
  const { requireHqOwner }=load('src/lib/hq-auth.ts',{'node:crypto':crypto,'server-only':{},'./auth-server':{ApiRouteError,requireApiUser:async()=>({email:'someone@example.com',email_confirmed_at:'2026-01-01'})}});
  await assert.rejects(requireHqOwner(new Request('http://localhost/api/hq')),e=>e.status===403);
});

test('analysis counts distinct buyers, separates options, excludes duplicate rows and uncertain amounts', () => {
 const {aggregateAnalysis}=load('src/lib/congress-analysis.ts',{'./reviewed-politician-trades':load('src/lib/reviewed-politician-trades.ts')});
 const base={id:'1',doc_id:'doc-1',source_url:'https://example.com/source',member_id:'A000001',ticker:'ABC',asset_type:'ST',amount_range:'$1,001 - $15,000',transaction_type:'buy',transaction_date:'2026-09-01',published_date:'2026-09-10'};
 const rows=[base,{...base},{...base,id:'2',doc_id:'doc-2'},{...base,id:'3',doc_id:'doc-3',member_id:'B000002',amount_range:'Unknown'},{...base,id:'4',doc_id:'doc-4',member_id:'C000003',transaction_date:'2026-09-14'},{...base,id:'5',doc_id:'doc-5',asset_type:'OP'}];
 const result=aggregateAnalysis(rows,'stocks'); const stock=result.stocks[0];
 assert.equal(stock.buyers,3);assert.equal(stock.trades.length,4);assert.equal(stock.purchaseMin,3003);assert.equal(stock.unknownAmounts,1);assert.equal(stock.together,3);
 assert.equal(aggregateAnalysis(rows,'options').stocks[0].trades.length,1);
});


test('analysis flags standard-range assumptions and does not infer unrecognized amounts',()=>{
 const {aggregateAnalysis}=load('src/lib/congress-analysis.ts',{'./reviewed-politician-trades':load('src/lib/reviewed-politician-trades.ts')});
 const row={id:'1',member_id:'A000001',ticker:'ABC',asset_type:'ST',transaction_type:'buy',amount_range:'$15,001'};
 const a=aggregateAnalysis([row],'stocks').stocks[0];
 assert.equal(a.purchaseMin,15001);assert.equal(a.purchaseMax,50000);assert.equal(a.inferredAmounts,1);assert.equal(a.unknownAmounts,0);
 const b=aggregateAnalysis([{...row,amount_range:'Over $1,000,000'}],'stocks').stocks[0];assert.equal(b.unknownAmounts,1);assert.equal(b.inferredAmounts,0);
});

test('activity context flags compensation and keeps restricted stock out of totals',()=>{
 const {reviewPoliticianTrade}=load('src/lib/reviewed-politician-trades.ts');
 const award=reviewPoliticianTrade({member_id:'R000614',doc_id:'house-2026-20034375-0',source_url:'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034375.pdf',asset_type:'RS'});
 assert.equal(award.activity_label,'Stock award');assert.equal(award.exclude_from_totals,true);
 const unknown=reviewPoliticianTrade({asset_type:'RS',asset_name:'Common Stock'});
 assert.equal(unknown.activity_label,'Activity needs review');assert.equal(unknown.exclude_from_totals,true);
 const future=reviewPoliticianTrade({asset_type:'ST',description:'Spouse awarded RSUs as part of total comp package'});
 assert.equal(future.activity_label,'Stock award');
 const ordinary=reviewPoliticianTrade({asset_type:'ST',asset_name:'Example Common Stock'});
 assert.equal(ordinary.exclude_from_totals,undefined);
});

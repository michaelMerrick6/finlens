import assert from 'node:assert/strict';
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

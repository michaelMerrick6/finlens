import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import ts from 'typescript';
import {test} from 'node:test';
function loader(overrides={}){const cache={};return function load(file){file=path.resolve(file);if(cache[file])return cache[file];const exports={};cache[file]=exports;const code=ts.transpileModule(fs.readFileSync(file,'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;vm.runInNewContext(code,{exports,Date,URL,Set,Map,AbortSignal,process:{env:{OPENAI_API_KEY:'test'}},fetch:overrides.fetch,require(n){if(n in overrides)return overrides[n];if(n==='server-only')return {};if(n.startsWith('.')){const p=path.resolve(path.dirname(file),n);if(n.endsWith('.json'))return {default:JSON.parse(fs.readFileSync(p,'utf8'))};return load(p+'.ts');}throw Error('Unexpected import '+n);}});return exports;};}
const load=loader();const {DEFAULT_SCREEN,validateScreen,evaluateScreen,screenDates}=load('src/lib/research-screen.ts');
const trade=(id,member='K000389',extra={})=>({id,doc_id:id,member_id:member,politician_name:member,ticker:'NVDA',asset_type:'ST',amount_range:'$1,001 - $15,000',asset_name:'NVIDIA',transaction_type:'buy',transaction_date:'2026-08-01',published_date:'2026-09-01',source_url:'https://example.com/source',...extra});
const classification=[{ticker:'NVDA',company_name:'NVIDIA',industry:'Semiconductors & Related Devices',source_url:'https://data.sec.gov/',verified_at:'2026-09-01'}];
test('four trades from one person do not satisfy three distinct buyers',()=>{const r=evaluateScreen([1,2,3,4].map(i=>trade(String(i))),{...DEFAULT_SCREEN,activity:'buy',minPoliticians:3},classification,null,null);assert.equal(r.stocks.length,0);});
test('duplicate source rows count once, independent purchasers qualify',()=>{const rows=[trade('1'),trade('1'),trade('2','P000197'),trade('3','T000278')];const r=evaluateScreen(rows,DEFAULT_SCREEN,classification,null,null);assert.equal(r.stocks[0].politicians,3);assert.equal(r.stocks[0].trades.length,3);});
test('industry excludes unclassified tickers and reports missing coverage',()=>{const f={...DEFAULT_SCREEN,minPoliticians:1,industry:classification[0].industry};const r=evaluateScreen([trade('1'),trade('2','P000197',{ticker:'ZZZ'})],f,classification,null,null);assert.equal(r.stocks.length,1);assert.equal(r.coverage.unclassifiedTickers,1);});
test('committee and politician filters intersect instead of broadening the search',()=>{const rows=[trade('1'),trade('2','P000197')];const r=evaluateScreen(rows,{...DEFAULT_SCREEN,minPoliticians:1,memberId:'P000197'},classification,new Set(['K000389']),null);assert.equal(r.stocks.length,0);});
test('sales cannot qualify a purchase-only screen',()=>{const r=evaluateScreen([trade('1','P000197',{transaction_type:'sell'})],{...DEFAULT_SCREEN,activity:'buy',minPoliticians:1},classification,null,null);assert.equal(r.stocks.length,0);});
test('unsupported filters, invalid dates, and SQL-like ticker strings are rejected',()=>{for(const patch of [{days:0},{days:367},{minPoliticians:0},{ticker:"NVDA');drop table companies;--"},{profit:1},{industry:''},{days:'30'}])assert.throws(()=>validateScreen({...DEFAULT_SCREEN,...patch}));});
test('rolling windows include exactly the requested days across month and leap boundaries',()=>{assert.equal(screenDates(30,new Date('2026-09-16T12:00Z')).start,'2026-08-18');assert.equal(screenDates(1,new Date('2024-02-29T12:00Z')).start,'2024-02-29');});
const next={'NextResponse':{json:(body,options={})=>({body,status:options.status||200})}};
class ApiRouteError extends Error{constructor(status,code,message){super(message);this.status=status;this.code=code;}}
test('anonymous users cannot invoke the paid model',async()=>{let invoked=false;const route=loader({'next/server':next,'@/lib/research-screen-server':{researchData:async()=>({catalog:{}}),runScreen:async()=>({})},'@/lib/research-model':{interpretScreen:async()=>{invoked=true;}},'@/lib/auth-server':{requireApiUser:async()=>{throw new ApiRouteError(401,'AUTH','Sign in');},ApiRouteError},'@/lib/supabase-admin':{getAdminSupabase:()=>{throw Error('Must not query');}}})('src/app/api/research/route.ts');const r=await route.POST({text:async()=>JSON.stringify({action:'interpret',question:'three buyers'})});assert.equal(r.status,401);assert.equal(invoked,false);});
test('exhausted durable quota blocks model calls',async()=>{let invoked=false;const route=loader({'next/server':next,'@/lib/research-screen-server':{researchData:async()=>({catalog:{}}),runScreen:async()=>({})},'@/lib/research-model':{interpretScreen:async()=>{invoked=true;}},'@/lib/auth-server':{requireApiUser:async()=>({id:'me'}),ApiRouteError},'@/lib/supabase-admin':{getAdminSupabase:()=>({rpc:async()=>({data:false,error:null})})}})('src/app/api/research/route.ts');const r=await route.POST({text:async()=>JSON.stringify({action:'interpret',question:'three buyers'})});assert.equal(r.status,429);assert.equal(invoked,false);});
test('saved screen deletion is scoped to the authenticated user',async()=>{const calls=[];const q={delete(){return q;},eq(...args){calls.push(args);return q;},then(resolve){return Promise.resolve({error:null}).then(resolve);}};const route=loader({'next/server':next,'@/lib/auth-server':{requireApiUser:async()=>({id:'authenticated-user'}),ApiRouteError},'@/lib/account-route':{accountRouteErrorResponse:e=>({status:e.status})},'@/lib/supabase-admin':{getAdminSupabase:()=>({from:()=>q})},'@/lib/research-screen':{validateScreen}})('src/app/api/account/research-screens/route.ts');const r=await route.DELETE({url:'https://example.com?id=00000000-0000-0000-0000-000000000001&user_id=someone-else'});assert.equal(r.status,200);assert.deepEqual(calls[1],['user_id','authenticated-user']);});

test('neutral defaults include a single buyer and sales instead of hiding activity',()=>{
 assert.equal(DEFAULT_SCREEN.minPoliticians,1);assert.equal(DEFAULT_SCREEN.activity,'all');
 const r=evaluateScreen([trade('1'),trade('2','P000197',{ticker:'AMD',transaction_type:'sell'})],DEFAULT_SCREEN,classification,null,null);
 assert.equal(r.stocks.length,2);
});
test('new questions ignore supplied previous filters; only explicit follow-ups inherit them',async()=>{
 const inputs=[];
 const route=loader({'next/server':next,'@/lib/research-screen-server':{researchData:async()=>({catalog:{}})},'@/lib/research-model':{interpretScreen:async(q,c,previous)=>{inputs.push(previous);return {}; }},'@/lib/auth-server':{requireApiUser:async()=>({id:'me'}),ApiRouteError},'@/lib/supabase-admin':{getAdminSupabase:()=>({rpc:async()=>({data:true,error:null})})}})('src/app/api/research/route.ts');
 for(const mode of [undefined,'new','followup']){const r=await route.POST({text:async()=>JSON.stringify({action:'interpret',question:'what stocks were bought last week',mode,previous:{...DEFAULT_SCREEN,minPoliticians:3}})});assert.equal(r.status,200);}
 assert.equal(inputs[0],null);assert.equal(inputs[1],null);assert.equal(inputs[2].minPoliticians,3);
});
test('ranking explanation reports ties rather than inventing a unique winner or motives',()=>{
 const {summarizeScreen}=load('src/lib/research-screen.ts');
 const result={...evaluateScreen([trade('1'),trade('2','P000197',{ticker:'AMD'})],DEFAULT_SCREEN,classification,null,null),filters:DEFAULT_SCREEN,start:'2026-08-18',end:'2026-09-16'};
 const text=summarizeScreen(result);assert.match(text,/tie for first/);assert.match(text,/1 politician each/);assert.match(text,/not investment merit or the reasons/);
 assert.match(summarizeScreen({...result,stocks:[]}),/No matching companies/);
});
test('research briefs attach names, dates, and evidence to ranked companies',()=>{
 const {buildResearchBrief}=load('src/lib/research-brief.ts');
 const rows=[trade('1'),trade('2','P000197'),trade('3','K000389',{ticker:'AMD',transaction_type:'sell'})];
 const result={...evaluateScreen(rows,DEFAULT_SCREEN,classification,null,null),filters:DEFAULT_SCREEN,start:'2026-08-18',end:'2026-09-16'};
 const brief=buildResearchBrief(result);assert.match(brief.title,/NVIDIA leads/);assert.equal(brief.sections[0].members.length,2);assert.match(brief.sections[0].dates,/2026-08-01/);assert.match(brief.sections[1].paragraphs[0],/report sales/);
 assert.equal(brief.sections[0].ticker,'NVDA');assert.match(brief.sections[0].paragraphs[1],/AMD/);
});
test('research brief preserves ties, empty results and missing dates',()=>{
 const {buildResearchBrief}=load('src/lib/research-brief.ts');
 const result={...evaluateScreen([trade('1','K000389',{transaction_date:null}),trade('2','P000197',{ticker:'AMD',transaction_date:null})],DEFAULT_SCREEN,[],null,null),filters:DEFAULT_SCREEN,start:'2026-08-18',end:'2026-09-16'};
 const brief=buildResearchBrief(result);assert.match(brief.title,/share the lead/);assert.match(brief.sections[0].paragraphs[1],/tied/);assert.equal(brief.sections[0].dates,'Transaction dates are unavailable.');assert.equal(buildResearchBrief({...result,stocks:[]}).sections.length,0);
});
test('purchase amount ranking orders money ahead of buyer count without inventing upper bounds',()=>{
 const {purchaseAmounts,formatPurchaseRange}=load('src/lib/research-screen.ts');
 const rows=[trade('1'),trade('2','P000197'),trade('3','T000278',{ticker:'AMD',amount_range:'$100,001 - $250,000'}),trade('4','T000278',{ticker:'AMD',amount_range:'$1,000,001'})];
 const result=evaluateScreen(rows,{...DEFAULT_SCREEN,activity:'buy',rank:'purchase_amount'},classification,null,null);
 assert.equal(result.stocks[0].ticker,'AMD');assert.equal(result.stocks[0].disclosedPurchases.min,100001);assert.equal(result.stocks[0].disclosedPurchases.missing,1);assert.match(formatPurchaseRange(result.stocks[0].disclosedPurchases),/partial/);
 assert.equal(purchaseAmounts([trade('5','P000197',{amount_range:'Over $50,000,000'})]).known,0);
 const {buildResearchBrief}=load('src/lib/research-brief.ts');const brief=buildResearchBrief({...result,filters:{...DEFAULT_SCREEN,activity:'buy',rank:'purchase_amount'},start:'2026-08-03',end:'2026-09-16'});assert.match(brief.title,/purchase minimum/);assert.match(brief.sections[0].headline,/100,001/);assert.doesNotMatch(brief.title,/distinct buyers/);
});
test('old saved screens keep their prior ranking and incompatible amount modes are rejected',()=>{
 const {rank,...legacy}=DEFAULT_SCREEN;assert.equal(validateScreen(legacy).rank,'politicians');assert.throws(()=>validateScreen({...DEFAULT_SCREEN,rank:'purchase_amount',activity:'sell'}));assert.throws(()=>validateScreen({...DEFAULT_SCREEN,rank:'profit'}));
});

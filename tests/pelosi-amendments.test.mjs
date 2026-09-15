import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {test} from 'node:test';
import ts from 'typescript';
const rules=JSON.parse(fs.readFileSync('src/lib/pelosi-amendment-review.json','utf8'));
const exports={};
vm.runInNewContext(ts.transpileModule(fs.readFileSync('src/lib/reviewed-politician-trades.ts','utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022,esModuleInterop:true}}).outputText,{exports,require:(name)=>/202[2345]-review/.test(name)?JSON.parse(fs.readFileSync('src/lib/'+name.replace('./',''),'utf8')):rules});
const review=exports.reviewPoliticianTrade;
const make=id=>({member_id:'P000197',doc_id:id,source_url:rules[id].source_url,amount_range:'$500,001',asset_type:'Stock',filing_status:'Amended'});
test('all 16 supersession pairs count once without mutating raw records',()=>{
 const originals=Object.entries(rules).filter(([,r])=>r.superseded_by);assert.equal(originals.length,16);
 for(const [id,r] of originals){const a=make(id),b=make(r.superseded_by);assert.equal(review(a).exclude_from_totals,true);assert.equal(review(b).exclude_from_totals,false);assert.equal(b.asset_type,'Stock');assert.equal(review(b).amount_range,'$500,001 - $1,000,000');}
});
test('scope requires both Pelosi and exact official source URL',()=>{
 const id=Object.keys(rules)[0];
 assert.notEqual(review({...make(id),member_id:'OTHER',filing_status:'New'}).activity_label,'Superseded filing');
 assert.notEqual(review({...make(id),source_url:'https://example.com',filing_status:'New'}).activity_label,'Superseded filing');
});
test('exercises become shares while option sales remain contracts',()=>{
 assert.equal(review(make('house-2022-20020561-1')).asset_type,'ST');
 assert.equal(review(make('house-2020-20016961-0')).asset_type,'OP');
 assert.equal(review(make('house-2020-20016961-1')).exclude_from_totals,false);
});

test('2025 fund identity and exercise correction preserve original raw records',()=>{
 const source='https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2025/20030630.pdf';
 const fund={member_id:'P000197',doc_id:'house-2025-20030630-1',source_url:source,ticker:'T',asset_type:'OT'};
 assert.equal(review(fund).ticker,null);assert.equal(review(fund).asset_type,'MF');assert.equal(fund.ticker,'T');
 const exercise=review({...fund,doc_id:'house-2025-20030630-0',ticker:'AVGO'});
 assert.equal(exercise.asset_type,'ST');assert.equal(exercise.activity_label,'Option exercise');assert.equal(exercise.amount_range,'$1,000,001 - $5,000,000');
 assert.equal(review({...fund,source_url:'https://example.com'}).ticker,'T');
});

test('2024 private investments and exercises retain their economic identity',()=>{
 const row=(doc)=>({member_id:'P000197',doc_id:doc,source_url:`https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${doc.split('-')[1]}/${doc.split('-')[2]}.pdf`});
 assert.equal(review(row('house-2024-20024625-0')).asset_type,'AB');
 const exercise=review(row('house-2025-20026590-6'));assert.equal(exercise.asset_type,'ST');assert.match(exercise.description,/discrepancy unresolved/);
 assert.equal(review(row('house-2025-20026590-4')).activity_label,'Option exercise');
});

test('2023 expiration is not sale proceeds and contribution is excluded',()=>{
 const row=doc=>({member_id:'P000197',doc_id:doc,source_url:`https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2023/${doc.split('-')[2]}.pdf`});
 const expired=review(row('house-2023-20022320-0'));assert.equal(expired.activity_label,'Expired worthless');assert.equal(expired.exclude_from_totals,true);assert.equal(expired.amount_range,'$1.00');
 assert.equal(review(row('house-2023-20023080-0')).is_contribution,true);
 assert.equal(review(row('house-2023-20022664-0')).asset_type,'ST');
});

test('2022 private investments, share class, expiration and spinoff stay distinct',()=>{
 const row=doc=>({member_id:'P000197',doc_id:doc,source_url:`https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${doc.split('-')[1]}/${doc.split('-')[2]}.pdf`,ticker:'ONE'});
 for(const id of ['house-2022-20021675-0','house-2023-20022260-8']){assert.equal(review(row(id)).ticker,null);assert.equal(review(row(id)).asset_type,'AB');}
 const exercise=review(row('house-2022-20021837-0'));assert.equal(exercise.ticker,'GOOG');assert.equal(exercise.asset_type,'ST');
 assert.equal(review(row('house-2022-20021837-3')).exclude_from_totals,true);
 assert.equal(review(row('house-2022-20020916-0')).activity_label,'Spinoff');
 assert.match(review(row('house-2023-20022260-10')).description,/discrepancy unresolved/);
 assert.equal(review({...row('house-2022-20021837-0'),member_id:'OTHER'}).ticker,'ONE');
});

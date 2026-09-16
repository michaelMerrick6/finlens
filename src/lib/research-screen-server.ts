import 'server-only';
import { unstable_cache } from 'next/cache';
import { getAdminSupabase } from './supabase-admin';
import { evaluateScreen, screenDates, validateScreen, type ScreenCatalog, type Classification, type ScreenFilters } from './research-screen';
import type { AnalysisTrade } from './congress-analysis';

async function pages<T>(fetchPage:(offset:number)=>PromiseLike<{data:unknown[]|null;error:unknown}>,max=50000):Promise<T[]>{
 const rows:T[]=[];
 for(let offset=0;offset<max;offset+=500){const r=await fetchPage(offset);if(r.error)throw Error('Research data unavailable.');rows.push(...(r.data||[]) as T[]);if((r.data?.length||0)<500)return rows;}
 throw Error('This screen exceeds the scan limit. Choose a shorter period.');
}
export const researchData=unstable_cache(async()=>{
 const db=getAdminSupabase();
 const [classifications,members,snapshots]=await Promise.all([
 pages<Classification>(offset=>db.from('company_research_classifications').select('ticker,company_name,industry,source_url,verified_at').order('ticker').range(offset,offset+499)),
 pages<{id:string;first_name:string;last_name:string;chamber:string}>(offset=>db.from('congress_members').select('id,first_name,last_name,chamber').order('id').range(offset,offset+499)),
 Promise.all(['official_house','official_senate'].map(async key=>{
 const r=await db.from('committee_snapshots').select('id,verified_at,congress,committees').eq('source_key',key).order('verified_at',{ascending:false}).limit(1);
 if(r.error||!r.data?.length)throw Error('Current committee coverage unavailable.');return r.data[0] as {id:string;verified_at:string;congress:number;committees:{id:string;name:string;parent_id:string|null}[]};}))]);
 const catalog:ScreenCatalog={industries:[...new Set(classifications.map(c=>c.industry).filter((s):s is string=>Boolean(s)))].sort(),members:members.filter(m=>/^[A-Z]\d{6}$/.test(m.id)).map(m=>({id:m.id,name:[m.first_name,m.last_name].join(' '),chamber:m.chamber})),committees:snapshots.flatMap(s=>s.committees.map(c=>({id:c.id,name:c.name+(c.parent_id?' — '+s.committees.find(p=>p.id===c.parent_id)?.name:''),snapshotId:s.id,verifiedAt:s.verified_at})))};
 return {classifications,catalog};
},['research-catalog-v1'],{revalidate:300});
const trades=unstable_cache(async(start:string,end:string,basis:string)=>pages<AnalysisTrade>(offset=>getAdminSupabase().from('politician_trades')
 .select('chamber,id,doc_id,member_id,politician_name,ticker,asset_type,amount_range,asset_name,transaction_type,transaction_date,published_date,source_url')
 .gte(basis,start).lte(basis,end).lte('published_date',end).in('transaction_type',['buy','sell']).order('id').range(offset,offset+499)),['research-trades-v1'],{revalidate:300});
export async function runScreen(input:unknown){
 const filters=validateScreen(input);const {catalog,classifications}=await researchData();
 validateCatalog(filters,catalog);
 const committee=catalog.committees.find(c=>c.id===filters.committeeId);
 let memberIds:Set<string>|null=null;
 if(committee){
 if(Date.now()-Date.parse(committee.verifiedAt)>72*3600000)throw Error('Committee verification is overdue. Try again after the roster is verified.');
 const rows=await pages<{member_id:string}>(offset=>getAdminSupabase().from('committee_assignments').select('member_id').eq('snapshot_id',committee.snapshotId).eq('committee_id',committee.id).order('member_id').range(offset,offset+499));
 memberIds=new Set(rows.map(r=>r.member_id));
 }
 const chamberIds=filters.chamber==='all'?null:new Set(catalog.members.filter(m=>m.chamber===filters.chamber||m.chamber==='Both').map(m=>m.id));
 const dates=screenDates(filters.days);const rows=await trades(dates.start,dates.end,filters.basis==='trade'?'transaction_date':'published_date');
 return {...evaluateScreen(rows,filters,classifications,memberIds,chamberIds),filters,...dates,scanned:rows.length,computedAt:new Date().toISOString(),committeeVerifiedAt:committee?.verifiedAt||null};
}
export function validateCatalog(f:ScreenFilters,c:ScreenCatalog){
 if(f.industry&&!c.industries.includes(f.industry))throw Error('That industry does not have a supported classification.');
 if(f.memberId&&!c.members.some(m=>m.id===f.memberId))throw Error('Unknown politician.');
 if(f.committeeId&&!c.committees.some(m=>m.id===f.committeeId))throw Error('Unknown current committee.');
}

import { NextResponse } from 'next/server';
import { unstable_cache } from 'next/cache';
import { getPublicSupabase } from '@/lib/supabase-server';
import { aggregateAnalysis, type AnalysisTrade } from '@/lib/congress-analysis';
export const dynamic='force-dynamic';
const load=unstable_cache(async(start:string,end:string,basis:string,instrument:string)=>{
 const rows:AnalysisTrade[]=[];let complete=false;
 for(let offset=0;offset<50000;offset+=500){
 const {data,error}=await getPublicSupabase().from('politician_trades').select('id,doc_id,member_id,politician_name,ticker,asset_type,amount_range,asset_name,transaction_type,transaction_date,published_date,source_url')
 .gte(basis,start).lte(basis,end).lte('published_date',end).in('transaction_type',['buy','sell']).order('id').range(offset,offset+499);
 if(error)throw error;rows.push(...((data||[]) as AnalysisTrade[]));if((data?.length||0)<500){complete=true;break;}
 }
 if(!complete)throw Error('Analysis scan limit reached');
 return {...aggregateAnalysis(rows,instrument),scanned:rows.length,start,end};
},['congress-analysis-v3'],{revalidate:300});
export async function GET(request:Request){
 const p=new URL(request.url).searchParams;const period=p.get('period')||'30';
 const basis=p.get('basis')==='trade'?'transaction_date':'published_date';const instrument=p.get('instrument')==='options'?'options':'stocks';
 const end=new Date().toISOString().slice(0,10);const startDate=new Date(`${end}T00:00:00Z`);
 if(period==='ytd')startDate.setUTCMonth(0,1);else if(period==='year')startDate.setUTCFullYear(startDate.getUTCFullYear()-1);else startDate.setUTCDate(startDate.getUTCDate()-(period==='7'?6:29));
 try{return NextResponse.json(await load(startDate.toISOString().slice(0,10),end,basis,instrument));}catch{return NextResponse.json({error:'Analysis is temporarily unavailable. No partial ranking is shown.'},{status:503});}
}

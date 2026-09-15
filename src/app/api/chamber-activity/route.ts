import { NextResponse } from 'next/server';
import { loadPoliticianRanking } from '@/lib/politician-ranking';
export const dynamic='force-dynamic';
export async function GET(request:Request){
 try {
  const end=new Date().toISOString().slice(0,10);
  const [ranking,response]=await Promise.all([loadPoliticianRanking(end),fetch(new URL('/api/analysis?period=7',request.url),{cache:'no-store'})]);
  if(!response.ok)throw Error('Unavailable');
  const data=await response.json();
  const chambers=new Map(ranking.members.map(m=>[m.id.toUpperCase(),m.chamber]));
  const senate=new Set<string>(),house=new Set<string>();let purchases=0;
  for(const stock of data.stocks)for(const trade of stock.trades){
   const id=String(trade.member_id||'').toUpperCase();
   if(chambers.get(id)==='Senate')senate.add(id);
   if(chambers.get(id)==='House')house.add(id);
   if(trade.transaction_type==='buy')purchases++;
  }
  return NextResponse.json({senate:senate.size,house:house.size,purchases},{headers:{'Cache-Control':'public, max-age=300'}});
 }catch{return NextResponse.json({error:'Activity unavailable'},{status:503});}
}

import { NextResponse } from 'next/server';
import { getAdminSupabase } from '@/lib/supabase-admin';
export async function POST(request:Request){
 if(request.headers.get('origin')!==new URL(request.url).origin)return new NextResponse(null,{status:403});
 if(Number(request.headers.get('content-length')||0)>2048)return new NextResponse(null,{status:413});
 try{
 const body=await request.json();
 const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
 if(!uuid.test(body.sessionId)||!uuid.test(body.visitorId)||!Number.isInteger(body.seconds)||body.seconds<0||body.seconds>30)return new NextResponse(null,{status:400});
 const db=getAdminSupabase();let userId=null;
 const token=request.headers.get('authorization')?.match(/^Bearer (.+)$/i)?.[1];
 if(token){const r=await db.auth.getUser(token);if(r.error||!r.data.user)return new NextResponse(null,{status:401});userId=r.data.user.id;}
 const r=await db.rpc('record_site_engagement',{p_session:body.sessionId,p_visitor:body.visitorId,p_user:userId,p_seconds:body.seconds});
 return new NextResponse(null,{status:r.error?503:204});
 }catch{return new NextResponse(null,{status:400});}
}

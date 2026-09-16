import { NextResponse } from 'next/server';
import { researchData, runScreen } from '@/lib/research-screen-server';
import { interpretScreen } from '@/lib/research-model';
import { requireApiUser, ApiRouteError } from '@/lib/auth-server';
import { getAdminSupabase } from '@/lib/supabase-admin';
export const dynamic='force-dynamic';
export async function GET(){
 try{return NextResponse.json({catalog:(await researchData()).catalog,assistantAvailable:Boolean(process.env.OPENAI_API_KEY)});}catch{return NextResponse.json({error:'Research catalogs are temporarily unavailable.'},{status:503});}
}
export async function POST(request:Request){
 try{
 const raw=await request.text();if(raw.length>8000)return NextResponse.json({error:'Request too large.'},{status:413});
 const body=JSON.parse(raw);
 if(!body||typeof body!=="object"||Array.isArray(body))throw new ApiRouteError(400,"INVALID_BODY","Invalid request.");
 if(body.action==='interpret'){
 const user=await requireApiUser(request);
 if(typeof body.question!=='string'||body.question.trim().length<3||body.question.length>1000)throw new ApiRouteError(400,'INVALID_QUESTION','Enter a question between 3 and 1,000 characters.');
 if(!process.env.OPENAI_API_KEY)throw new ApiRouteError(503,'NOT_CONFIGURED','Conversational research is not configured. Use the filters below.');
 if(body.mode!==undefined && body.mode!=='new' && body.mode!=='followup')throw new ApiRouteError(400,'INVALID_MODE','Choose a new search or follow-up.');
 if(body.mode==='followup' && !body.previous)throw new ApiRouteError(400,'MISSING_CONTEXT','Run a screen before refining it.');
 const claim=await getAdminSupabase().rpc('claim_research_request',{p_user:user.id});
 if(claim.error)throw new ApiRouteError(503,'USAGE_UNAVAILABLE','Research usage is temporarily unavailable.');
 if(!claim.data)throw new ApiRouteError(429,'DAILY_LIMIT','You have used your 20 daily research questions. Filters remain available.');
 const interpretation=await interpretScreen(body.question,(await researchData()).catalog,body.mode==='followup'?body.previous:null);
 return NextResponse.json(interpretation,{headers:{'Cache-Control':'private, no-store'}});
 }
 if(body.action!=='screen')throw new ApiRouteError(400,'INVALID_ACTION','Unknown research action.');
 return NextResponse.json(await runScreen(body.filters),{headers:{'Cache-Control':'no-store'}});
 }catch(e){
 const status=e instanceof ApiRouteError?e.status:e instanceof SyntaxError?400:422;
 return NextResponse.json({error:e instanceof Error?e.message:'Research is unavailable.'},{status});
 }
}

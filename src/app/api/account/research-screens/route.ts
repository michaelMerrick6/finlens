import { NextResponse } from 'next/server';
import { requireApiUser, ApiRouteError } from '@/lib/auth-server';
import { accountRouteErrorResponse } from '@/lib/account-route';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { validateScreen } from '@/lib/research-screen';
const headers={'Cache-Control':'private, no-store'};
export async function GET(request:Request){try{
 const user=await requireApiUser(request);const {data,error}=await getAdminSupabase().from('research_screens').select('id,name,filters,created_at').eq('user_id',user.id).order('created_at',{ascending:false}).limit(100);
 if(error)throw new ApiRouteError(503,'UNAVAILABLE','Saved screens are unavailable.');
 return NextResponse.json({screens:data||[]},{headers});
}catch(e){return accountRouteErrorResponse(e);}}
export async function POST(request:Request){try{
 const user=await requireApiUser(request);const raw=await request.text();if(raw.length>5000)throw new ApiRouteError(413,'TOO_LARGE','Request too large.');
 const body=JSON.parse(raw);const filters=validateScreen(body.filters);const name=String(body.name||'').trim();
 if(!name||name.length>100)throw new ApiRouteError(400,'INVALID_NAME','Use a name between 1 and 100 characters.');
 const {data,error}=await getAdminSupabase().from('research_screens').insert({user_id:user.id,name,filters}).select('id,name,filters,created_at').single();
 if(error)throw new ApiRouteError(error.message.includes('Saved screen limit')?409:503,'UNAVAILABLE',error.message.includes('Saved screen limit')?'You can save up to 20 screens. Delete one to add another.':'Could not save this screen.');
 return NextResponse.json({screen:data},{headers});
}catch(e){return accountRouteErrorResponse(e instanceof ApiRouteError?e:new ApiRouteError(400,'INVALID_SCREEN','Invalid screen.'));}}
export async function DELETE(request:Request){try{
 const user=await requireApiUser(request);const id=new URL(request.url).searchParams.get('id');
 if(!id||!/^[0-9a-f-]{36}$/i.test(id))throw new ApiRouteError(400,'INVALID_ID','Invalid screen ID.');
 const {error}=await getAdminSupabase().from('research_screens').delete().eq('id',id).eq('user_id',user.id);
 if(error)throw new ApiRouteError(503,'UNAVAILABLE','Could not delete this screen.');
 return NextResponse.json({ok:true},{headers});
}catch(e){return accountRouteErrorResponse(e);}}

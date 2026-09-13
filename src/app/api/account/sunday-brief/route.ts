import { NextResponse } from 'next/server';
import { requireApiUser } from '@/lib/auth-server';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { accountRouteErrorResponse } from '@/lib/account-route';
export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    return NextResponse.json({ enabled: user.user_metadata?.sunday_brief?.enabled === true }, { headers: { 'Cache-Control': 'no-store' } });
  } catch (error) { return accountRouteErrorResponse(error); }
}
export async function POST(request: Request) {
  try {
    const user = await requireApiUser(request);
    const body = await request.json();
    if (typeof body.enabled !== 'boolean') return NextResponse.json({ error: 'Choose a subscription preference.' }, { status: 400 });
    if (!user.email || !user.email_confirmed_at) return NextResponse.json({ error: 'Verify your account email before subscribing.' }, { status: 400 });
    const { error } = await getAdminSupabase().auth.admin.updateUserById(user.id, {
      user_metadata: { sunday_brief: { enabled: body.enabled, updated_at: new Date().toISOString(), consent_version: 'sunday-brief-v1' } },
    });
    if (error) throw error;
    return NextResponse.json({ enabled: body.enabled });
  } catch (error) { return accountRouteErrorResponse(error); }
}

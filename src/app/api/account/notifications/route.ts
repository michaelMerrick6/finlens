import { NextResponse } from 'next/server';
import { requireApiUser, ApiRouteError } from '@/lib/auth-server';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { accountRouteErrorResponse } from '@/lib/account-route';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const user = await requireApiUser(request);
    const query = new URL(request.url).searchParams;
    const id = query.get('id');
    const db = getAdminSupabase();
    if (id) {
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
        throw new ApiRouteError(400, 'INVALID_ID', 'Invalid notification ID.');
      }
      const result = await db.rpc('sent_email_detail', { p_user_id: user.id, p_id: id });
      if (result.error) throw new ApiRouteError(503, 'HISTORY_UNAVAILABLE', 'Sent emails are temporarily unavailable.');
      if (!result.data) throw new ApiRouteError(404, 'NOT_FOUND', 'Sent email not found.');
      return NextResponse.json({ notification: result.data }, { headers: { 'Cache-Control': 'private, no-store' } });
    }
    const offset = Number(query.get('offset') || 0);
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > 100000) {
      throw new ApiRouteError(400, 'INVALID_OFFSET', 'Invalid history offset.');
    }
    const result = await db.rpc('sent_email_history', { p_user_id: user.id, p_limit: 21, p_offset: offset });
    if (result.error) throw new ApiRouteError(503, 'HISTORY_UNAVAILABLE', 'Sent emails are temporarily unavailable.');
    const rows = result.data || [];
    return NextResponse.json({ notifications: rows.slice(0, 20), hasMore: rows.length > 20 },
      { headers: { 'Cache-Control': 'private, no-store' } });
  } catch (error) {
    return accountRouteErrorResponse(error);
  }
}

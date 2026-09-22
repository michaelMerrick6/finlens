import 'server-only';

import { ApiRouteError } from '@/lib/auth-server';
import type { ActorType, AlertMode } from '@/lib/account-types';
import { getAdminSupabase } from '@/lib/supabase-admin';

type Follow = {
  kind: 'ticker' | 'actor';
  target: string;
  alertMode: AlertMode;
  actorType?: ActorType;
  actorName?: string;
  metadata?: Record<string, unknown>;
};

/** The database locks the account before checking its quota and saving a follow. */
export async function saveAccountFollow(userId: string, watchlistId: string, follow: Follow) {
  const { error } = await getAdminSupabase().rpc('save_account_follow', {
    p_user_id: userId,
    p_watchlist_id: watchlistId,
    p_kind: follow.kind,
    p_target: follow.target,
    p_alert_mode: follow.alertMode,
    p_actor_type: follow.actorType ?? null,
    p_actor_name: follow.actorName ?? null,
    p_metadata: follow.metadata ?? {},
  });
  if (!error) return;
  if (error.code === 'P0001' && error.message === 'FOLLOW_LIMIT_REACHED') {
    throw new ApiRouteError(409, 'FOLLOW_LIMIT_REACHED', 'Your tracking list is full. Remove an item to add another.');
  }
  if (error.code === 'PGRST202' || error.code === '42883') {
    throw new ApiRouteError(503, 'ACCOUNT_SCHEMA_MISSING', 'Apply supabase_vail_phase20_atomic_follows.sql before saving follows.');
  }
  throw error;
}

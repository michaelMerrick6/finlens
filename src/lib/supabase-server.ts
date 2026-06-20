import 'server-only';

import { createClient } from '@supabase/supabase-js';

let publicSupabaseClient: ReturnType<typeof createClient> | null = null;

export function getPublicSupabase() {
  if (publicSupabaseClient) {
    return publicSupabaseClient;
  }

  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseKey) {
    throw new Error('Missing Supabase public credentials for server-side data access.');
  }

  publicSupabaseClient = createClient(supabaseUrl, supabaseKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
  });

  return publicSupabaseClient;
}

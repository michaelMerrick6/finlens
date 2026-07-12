import { createClient } from '@supabase/supabase-js';

function createBrowserSupabase(supabaseUrl: string, supabaseKey: string) {
  return createClient(supabaseUrl, supabaseKey);
}

let browserSupabase: ReturnType<typeof createBrowserSupabase> | null = null;

function getBrowserSupabase() {
  if (browserSupabase) {
    return browserSupabase;
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    throw new Error('Missing Supabase browser credentials.');
  }

  browserSupabase = createBrowserSupabase(supabaseUrl, supabaseKey);
  return browserSupabase;
}

export const supabase = {
  get auth() {
    return getBrowserSupabase().auth;
  },
};

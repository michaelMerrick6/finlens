import 'server-only';

import type { User } from '@supabase/supabase-js';

import type {
  AccountActorFollow,
  AccountAlertHistoryItem,
  AccountBillingState,
  AccountFollowSuggestion,
  AccountProfileState,
  AccountState,
  AccountSubscriptionState,
  AccountTickerSuggestion,
  AccountTickerFollow,
  ActorType,
  AlertMode,
} from '@/lib/account-types';
import {
  describeBillingSummary,
  isBillingCheckoutConfigured,
  isBillingPortalConfigured,
} from '@/lib/billing-server';
import {
  getFreeFollowLimit,
  normalizeBillingPlanKey,
  normalizeBillingStatus,
  resolveBillingFollowLimit,
} from '@/lib/billing-config';
import { type CongressMemberRecord, normalizeActorKey, resolvePoliticianTarget } from '@/lib/account-targets';
import { sendEmailMessage } from '@/lib/mailer';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { getTelegramBotUsername, normalizeTelegramUsername, resolveTelegramChatId, sendTelegramMessage } from '@/lib/telegram-bot';

const DEFAULT_WATCHLIST_NAME = 'My Signals';
const DEFAULT_MIN_IMPORTANCE = 0.75;
const DEFAULT_FOLLOW_LIMIT = getFreeFollowLimit();
const ACCOUNT_HISTORY_LIMIT = 30;
const TEST_ALERT_TICKER = 'VAILTEST';
const VALID_ALERT_MODES = new Set<AlertMode>(['activity', 'unusual', 'both']);
const DIRTY_COMPANY_NAME_PATTERNS = [
  'f s:',
  's o:',
  'subholding of',
  'filing status',
  ' fields law firm ',
  ' ira fbo ',
  ' morgan stanley ',
  'etrade',
  'e*trade',
  'trust ',
];

type TickerSearchAlias = {
  ticker: string;
  companyName: string;
  aliases: string[];
};

type TickerSuggestionCandidate = {
  ticker: string;
  companyName: string;
  rawCompanyName: string | null;
  aliases: string[];
  score: number;
  exactMatch: boolean;
  strongMatch: boolean;
};

type InsiderSuggestionCandidate = {
  actorName: string;
  actorKey: string;
  subtitle: string | null;
  score: number;
  recency: string;
  exactMatch: boolean;
  strongMatch: boolean;
};

type InsiderSignalRow = {
  actor_name?: string | null;
  ticker?: string | null;
  created_at?: string | null;
};

type InsiderTradeRow = {
  filer_name?: string | null;
  ticker?: string | null;
  transaction_date?: string | null;
};

const TICKER_SEARCH_ALIASES: TickerSearchAlias[] = [
  { ticker: 'NVDA', companyName: 'NVIDIA Corporation', aliases: ['nvidia', 'nvidia corp', 'nvidia corporation'] },
  { ticker: 'MSFT', companyName: 'Microsoft Corporation', aliases: ['microsoft', 'microsoft corporation'] },
  { ticker: 'AAPL', companyName: 'Apple Inc.', aliases: ['apple', 'apple inc', 'apple inc.'] },
  { ticker: 'AMZN', companyName: 'Amazon.com, Inc.', aliases: ['amazon', 'amazon.com', 'amazon inc', 'amazon.com inc'] },
  { ticker: 'META', companyName: 'Meta Platforms, Inc.', aliases: ['meta', 'meta platforms', 'facebook'] },
  { ticker: 'GOOGL', companyName: 'Alphabet Inc.', aliases: ['alphabet', 'google', 'google class a'] },
  { ticker: 'TSLA', companyName: 'Tesla, Inc.', aliases: ['tesla', 'tesla inc', 'tesla inc.'] },
  { ticker: 'AMD', companyName: 'Advanced Micro Devices', aliases: ['amd', 'advanced micro devices'] },
  { ticker: 'AVGO', companyName: 'Broadcom Inc.', aliases: ['broadcom', 'broadcom inc'] },
  { ticker: 'ARM', companyName: 'Arm Holdings plc', aliases: ['arm holdings', 'arm holdings plc'] },
  { ticker: 'SMCI', companyName: 'Super Micro Computer', aliases: ['super micro', 'super micro computer', 'supermicro'] },
  { ticker: 'PLTR', companyName: 'Palantir Technologies', aliases: ['palantir', 'palantir technologies'] },
  { ticker: 'AI', companyName: 'C3.ai', aliases: ['c3 ai', 'c3.ai'] },
  { ticker: 'SOUN', companyName: 'SoundHound AI', aliases: ['soundhound', 'soundhound ai'] },
  { ticker: 'BBAI', companyName: 'BigBear.ai', aliases: ['bigbear', 'bigbear ai', 'bigbear.ai'] },
  { ticker: 'IONQ', companyName: 'IonQ', aliases: ['ionq', 'ion q'] },
  { ticker: 'QBTS', companyName: 'D-Wave Quantum', aliases: ['d wave', 'd-wave', 'dwave', 'd wave quantum', 'd-wave quantum'] },
  { ticker: 'RGTI', companyName: 'Rigetti Computing', aliases: ['rigetti', 'rigetti computing'] },
  { ticker: 'QUBT', companyName: 'Quantum Computing Inc.', aliases: ['quantum computing inc', 'quantum computing'] },
  { ticker: 'QMCO', companyName: 'Quantum Corporation', aliases: ['quantum corporation'] },
  { ticker: 'SKYT', companyName: 'SkyWater Technology', aliases: ['skywater', 'skywater technology'] },
  { ticker: 'VLD', companyName: 'Velo3D', aliases: ['velo3d', 'velo 3d'] },
  { ticker: 'SMR', companyName: 'NuScale Power', aliases: ['nuscale', 'nu scale', 'nuscale power'] },
  { ticker: 'OKLO', companyName: 'Oklo', aliases: ['oklo'] },
  { ticker: 'NNE', companyName: 'NANO Nuclear Energy', aliases: ['nano nuclear', 'nano nuclear energy'] },
  { ticker: 'CCJ', companyName: 'Cameco', aliases: ['cameco'] },
  { ticker: 'BWXT', companyName: 'BWX Technologies', aliases: ['bwx technologies'] },
  { ticker: 'UEC', companyName: 'Uranium Energy Corp.', aliases: ['uranium energy', 'uranium energy corp'] },
  { ticker: 'LEU', companyName: 'Centrus Energy', aliases: ['centrus', 'centrus energy'] },
  { ticker: 'RXRX', companyName: 'Recursion Pharmaceuticals', aliases: ['recursion', 'recursion pharmaceuticals'] },
  { ticker: 'DNA', companyName: 'Ginkgo Bioworks', aliases: ['ginkgo', 'ginkgo bioworks'] },
  { ticker: 'CRSP', companyName: 'CRISPR Therapeutics', aliases: ['crispr', 'crispr therapeutics'] },
  { ticker: 'BEAM', companyName: 'Beam Therapeutics', aliases: ['beam therapeutics'] },
  { ticker: 'EDIT', companyName: 'Editas Medicine', aliases: ['editas', 'editas medicine'] },
  { ticker: 'NTLA', companyName: 'Intellia Therapeutics', aliases: ['intellia', 'intellia therapeutics'] },
  { ticker: 'TGTX', companyName: 'TG Therapeutics', aliases: ['tg therapeutics'] },
  { ticker: 'MRNA', companyName: 'Moderna', aliases: ['moderna'] },
  { ticker: 'SANA', companyName: 'Sana Biotechnology', aliases: ['sana biotech', 'sana biotechnology'] },
  { ticker: 'RTX', companyName: 'RTX Corporation', aliases: ['rtx', 'raytheon', 'raytheon technologies'] },
  { ticker: 'LMT', companyName: 'Lockheed Martin', aliases: ['lockheed', 'lockheed martin'] },
  { ticker: 'NOC', companyName: 'Northrop Grumman', aliases: ['northrop', 'northrop grumman'] },
  { ticker: 'GD', companyName: 'General Dynamics', aliases: ['general dynamics'] },
  { ticker: 'LHX', companyName: 'L3Harris Technologies', aliases: ['l3harris', 'l3 harris'] },
  { ticker: 'LDOS', companyName: 'Leidos', aliases: ['leidos'] },
  { ticker: 'HII', companyName: 'Huntington Ingalls', aliases: ['huntington ingalls'] },
  { ticker: 'KTOS', companyName: 'Kratos Defense', aliases: ['kratos', 'kratos defense'] },
  { ticker: 'AVAV', companyName: 'AeroVironment', aliases: ['aerovironment'] },
  { ticker: 'MRCY', companyName: 'Mercury Systems', aliases: ['mercury systems'] },
  { ticker: 'MSTR', companyName: 'Strategy', aliases: ['microstrategy', 'micro strategy', 'strategy'] },
  { ticker: 'COIN', companyName: 'Coinbase', aliases: ['coinbase', 'coinbase global'] },
  { ticker: 'MARA', companyName: 'MARA Holdings', aliases: ['mara', 'mara holdings'] },
  { ticker: 'RIOT', companyName: 'Riot Platforms', aliases: ['riot platforms'] },
  { ticker: 'CLSK', companyName: 'CleanSpark', aliases: ['cleanspark', 'clean spark'] },
  { ticker: 'IREN', companyName: 'IREN', aliases: ['iren', 'iris energy'] },
  { ticker: 'HUT', companyName: 'Hut 8', aliases: ['hut 8', 'hut8'] },
  { ticker: 'BTBT', companyName: 'Bit Digital', aliases: ['bit digital'] },
  { ticker: 'CIFR', companyName: 'Cipher Mining', aliases: ['cipher mining'] },
  { ticker: 'HIVE', companyName: 'HIVE Digital', aliases: ['hive digital', 'hive blockchain'] },
];

const TICKER_SEARCH_ALIAS_BY_TICKER = new Map(TICKER_SEARCH_ALIASES.map((entry) => [entry.ticker, entry]));
const PRIORITY_TICKER_SET = new Set(['NVDA', 'PLTR', 'IONQ', 'QBTS', 'RGTI', 'SKYT', 'VLD', 'OKLO', 'SMR', 'NNE']);
const NOTABLE_POLITICIAN_ALIASES: Array<{ actorKey: string; aliases: string[] }> = [
  { actorKey: 'p000197', aliases: ['pelosi', 'nancy pelosi'] },
  { actorKey: 'o000172', aliases: ['aoc', 'alexandria ocasio cortez', 'ocasio cortez'] },
  { actorKey: 'm000355', aliases: ['mitch mcconnell', 'mcconnell'] },
  { actorKey: 'j000299', aliases: ['mike johnson'] },
  { actorKey: 'm001190', aliases: ['markwayne mullin', 'markwayne'] },
];
const NOTABLE_INSIDER_ALIASES: Array<{ actorKey: string; canonicalName: string; aliases: string[] }> = [
  { actorKey: normalizeActorKey('Huang Jen Hsun'), canonicalName: 'Huang Jen Hsun', aliases: ['jensen huang', 'jen hsun huang', 'jensen', 'nvidia ceo'] },
  { actorKey: normalizeActorKey('Saylor Michael J'), canonicalName: 'Saylor Michael J', aliases: ['michael saylor', 'saylor', 'microstrategy chairman'] },
];

export class AccountSchemaError extends Error {
  code: string;

  constructor(message: string) {
    super(message);
    this.code = 'MIGRATION_REQUIRED';
  }
}

type ProfileRow = {
  id: string;
  email: string | null;
  display_name: string | null;
  alert_email: string | null;
  telegram_username: string | null;
  telegram_chat_id: string | null;
  email_enabled: boolean;
  telegram_enabled: boolean;
  follow_limit: number;
  billing_plan_key: string | null;
  billing_status: string | null;
  stripe_customer_id: string | null;
  stripe_subscription_id: string | null;
  stripe_price_id: string | null;
  billing_current_period_end: string | null;
  billing_cancel_at_period_end: boolean | null;
};

type WatchlistRow = {
  id: string;
  name: string;
};

type SubscriptionRow = {
  id: string;
  channel: string;
  destination: string;
  active: boolean;
  minimum_importance: number | null;
};

function getErrorCode(error: unknown) {
  if (error && typeof error === 'object' && 'code' in error) {
    return String((error as { code?: string }).code || '');
  }
  return '';
}

function getErrorMessage(error: unknown) {
  if (error && typeof error === 'object' && 'message' in error) {
    return String((error as { message?: string }).message || '');
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Unknown account error.';
}

function isAccountSchemaFailure(error: unknown) {
  const code = getErrorCode(error);
  const message = getErrorMessage(error);
  return (
    code === 'PGRST204' ||
    code === 'PGRST205' ||
    code === '42703' ||
    code === '42P01' ||
    message.includes("public.profiles") ||
    message.includes("profiles") ||
    message.includes("watchlists.user_id") ||
    message.includes("column user_id") ||
    message.includes("billing_plan_key") ||
    message.includes("billing_status") ||
    message.includes("stripe_customer_id") ||
    message.includes("stripe_subscription_id") ||
    message.includes("Could not find the table 'public.profiles'")
  );
}

function handleSupabaseError(error: unknown): never {
  if (isAccountSchemaFailure(error)) {
    throw new AccountSchemaError('Apply supabase_vail_phase5_user_accounts.sql before using the account area.');
  }
  throw error instanceof Error ? error : new Error(getErrorMessage(error));
}

function validAlertMode(candidate: string | null | undefined): AlertMode {
  const value = (candidate || '').trim().toLowerCase() as AlertMode;
  if (!VALID_ALERT_MODES.has(value)) {
    throw new Error('Invalid alert mode.');
  }
  return value;
}

function defaultDisplayName(user: User) {
  const metadata = user.user_metadata || {};
  if (typeof metadata.display_name === 'string' && metadata.display_name.trim()) {
    return metadata.display_name.trim();
  }
  if (typeof metadata.full_name === 'string' && metadata.full_name.trim()) {
    return metadata.full_name.trim();
  }
  if (user.email) {
    return user.email.split('@')[0];
  }
  return null;
}

function normalizeAlertEmail(value: string | null | undefined) {
  const normalized = (value || '').trim().toLowerCase();
  return normalized || null;
}

function normalizeSearchText(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeTickerCandidate(value: string) {
  return value.trim().toUpperCase().replace(/[^A-Z0-9.\-]/g, '').slice(0, 15);
}

function queryTokens(value: string) {
  return value
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function escapeSearchOperand(value: string) {
  return value.replace(/[%(),]/g, ' ').replace(/\s+/g, ' ').trim();
}

function buildIlikeOperands(column: string, values: string[], mode: 'contains' | 'prefix' = 'contains') {
  const operands: string[] = [];
  for (const value of values) {
    const normalized = escapeSearchOperand(value);
    if (!normalized) {
      continue;
    }
    operands.push(mode === 'prefix' ? `${column}.ilike.${normalized}%` : `${column}.ilike.%${normalized}%`);
  }
  return operands;
}

function aliasMatchScore(aliases: string[], query: string) {
  const normalizedQuery = normalizeSearchText(query);
  const tokens = queryTokens(normalizedQuery);
  let score = 0;

  for (const alias of mergeAliases(aliases)) {
    if (alias === normalizedQuery) score = Math.max(score, 260);
    else if (alias.startsWith(normalizedQuery)) score = Math.max(score, 125);
    else if (normalizedQuery.length >= 3 && alias.includes(normalizedQuery)) score = Math.max(score, 40);

    if (
      tokens.length &&
      tokens.every((token) => queryTokens(alias).some((aliasToken) => aliasToken.startsWith(token)))
    ) {
      score = Math.max(score, 55);
    }
  }

  return score;
}

function isDirtyCompanyName(value: string | null | undefined) {
  const normalized = normalizeSearchText(value || '');
  if (!normalized) {
    return true;
  }
  if (normalized.length > 120) {
    return true;
  }
  return DIRTY_COMPANY_NAME_PATTERNS.some((pattern) => normalized.includes(pattern.trim()));
}

function cleanCompanyDisplayName(ticker: string, rawName: string | null | undefined) {
  const alias = TICKER_SEARCH_ALIAS_BY_TICKER.get(ticker);
  const name = String(rawName || '').trim();
  if (!name || isDirtyCompanyName(name)) {
    return alias?.companyName || ticker;
  }

  const cleaned = name
    .replace(/\s+\[[A-Z]+\]$/g, '')
    .replace(/\s+-\s+Class\s+[A-Z]\s+Common\s+Stock.*$/i, '')
    .replace(/\s+-\s+Common\s+Stock.*$/i, '')
    .replace(/\s+Common\s+Stock.*$/i, '')
    .replace(/\s+Ordinary\s+Shares?.*$/i, '')
    .replace(/\s+Corporation\s+-\s+Common.*$/i, ' Corporation')
    .replace(/\s+/g, ' ')
    .trim();

  if (!cleaned || isDirtyCompanyName(cleaned)) {
    return alias?.companyName || ticker;
  }

  return alias?.companyName || cleaned;
}

function mergeAliases(...groups: Array<string[] | undefined>) {
  const seen = new Set<string>();
  const merged: string[] = [];
  for (const group of groups) {
    for (const value of group || []) {
      const normalized = normalizeSearchText(value);
      if (!normalized || seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      merged.push(normalized);
    }
  }
  return merged;
}

async function ensureUserProfile(user: User): Promise<ProfileRow> {
  const supabase = getAdminSupabase();
  const existing = await supabase
    .from('profiles')
    .select(
      'id,email,display_name,alert_email,telegram_username,telegram_chat_id,email_enabled,telegram_enabled,follow_limit,billing_plan_key,billing_status,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_current_period_end,billing_cancel_at_period_end'
    )
    .eq('id', user.id)
    .maybeSingle();

  if (existing.error) {
    handleSupabaseError(existing.error);
  }

  if (!existing.data) {
    const created = await supabase
      .from('profiles')
      .insert({
        id: user.id,
        email: user.email || null,
        display_name: defaultDisplayName(user),
        alert_email: user.email || null,
        follow_limit: DEFAULT_FOLLOW_LIMIT,
        billing_plan_key: 'free',
        billing_status: 'free',
      })
      .select(
        'id,email,display_name,alert_email,telegram_username,telegram_chat_id,email_enabled,telegram_enabled,follow_limit,billing_plan_key,billing_status,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_current_period_end,billing_cancel_at_period_end'
      )
      .single();

    if (created.error) {
      handleSupabaseError(created.error);
    }

    return created.data as ProfileRow;
  }

  const needsEmailSync = existing.data.email !== (user.email || null);
  const needsDisplayNameSync = !existing.data.display_name && Boolean(defaultDisplayName(user));
  const normalizedPlanKey = normalizeBillingPlanKey(existing.data.billing_plan_key);
  const normalizedBillingStatus = normalizeBillingStatus(existing.data.billing_status);
  const expectedFollowLimit = resolveBillingFollowLimit(normalizedPlanKey, normalizedBillingStatus);
  const needsBillingDefaultsSync = !existing.data.billing_plan_key || !existing.data.billing_status;
  const needsFollowLimitSync = Number(existing.data.follow_limit || 0) !== expectedFollowLimit;

  if (!needsEmailSync && !needsDisplayNameSync && !needsBillingDefaultsSync && !needsFollowLimitSync) {
    return existing.data as ProfileRow;
  }

  const updated = await supabase
    .from('profiles')
    .update({
      email: user.email || existing.data.email,
      display_name: existing.data.display_name || defaultDisplayName(user),
      billing_plan_key: normalizedPlanKey,
      billing_status: normalizedBillingStatus,
      follow_limit: expectedFollowLimit,
    })
    .eq('id', user.id)
    .select(
      'id,email,display_name,alert_email,telegram_username,telegram_chat_id,email_enabled,telegram_enabled,follow_limit,billing_plan_key,billing_status,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_current_period_end,billing_cancel_at_period_end'
    )
    .single();

  if (updated.error) {
    handleSupabaseError(updated.error);
  }

  return updated.data as ProfileRow;
}

async function ensureUserWatchlist(userId: string): Promise<WatchlistRow> {
  const supabase = getAdminSupabase();

  const existingByUser = await supabase
    .from('watchlists')
    .select('id,name')
    .eq('user_id', userId)
    .order('created_at', { ascending: true })
    .limit(1);

  if (existingByUser.error) {
    handleSupabaseError(existingByUser.error);
  }
  if (existingByUser.data?.[0]) {
    return existingByUser.data[0] as WatchlistRow;
  }

  const existingByOwner = await supabase
    .from('watchlists')
    .select('id,name')
    .eq('owner_type', 'auth_user')
    .eq('owner_key', userId)
    .order('created_at', { ascending: true })
    .limit(1);

  if (existingByOwner.error) {
    handleSupabaseError(existingByOwner.error);
  }

  if (existingByOwner.data?.[0]) {
    const watchlist = existingByOwner.data[0] as WatchlistRow;
    const updateResponse = await supabase
      .from('watchlists')
      .update({ user_id: userId, active: true })
      .eq('id', watchlist.id)
      .select('id,name')
      .single();

    if (updateResponse.error) {
      handleSupabaseError(updateResponse.error);
    }
    return updateResponse.data as WatchlistRow;
  }

  const created = await supabase
    .from('watchlists')
    .insert({
      user_id: userId,
      owner_type: 'auth_user',
      owner_key: userId,
      name: DEFAULT_WATCHLIST_NAME,
      active: true,
    })
    .select('id,name')
    .single();

  if (created.error) {
    handleSupabaseError(created.error);
  }

  return created.data as WatchlistRow;
}

async function ensureUserWorkspace(user: User) {
  const [profile, watchlist] = await Promise.all([ensureUserProfile(user), ensureUserWatchlist(user.id)]);
  return { profile, watchlist };
}

async function fetchWatchlistTickers(watchlistId: string): Promise<AccountTickerFollow[]> {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('watchlist_tickers')
    .select('id,ticker,alert_mode,created_at')
    .eq('watchlist_id', watchlistId)
    .order('created_at', { ascending: false });

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return (response.data || []).map((row) => ({
    id: String(row.id),
    ticker: String(row.ticker),
    alertMode: validAlertMode(String(row.alert_mode || 'both')),
    createdAt: String(row.created_at),
  }));
}

async function fetchWatchlistActors(watchlistId: string): Promise<AccountActorFollow[]> {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('watchlist_actors')
    .select('id,actor_type,actor_key,actor_name,alert_mode,metadata,created_at')
    .eq('watchlist_id', watchlistId)
    .order('created_at', { ascending: false });

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return (response.data || []).map((row) => ({
    id: String(row.id),
    actorType: String(row.actor_type) as ActorType,
    actorKey: String(row.actor_key),
    actorName: String(row.actor_name),
    alertMode: validAlertMode(String(row.alert_mode || 'both')),
    createdAt: String(row.created_at),
    metadata: (row.metadata as Record<string, unknown>) || {},
  }));
}

async function fetchSubscriptions(watchlistId: string) {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('alert_subscriptions')
    .select('id,channel,destination,active,minimum_importance,updated_at,created_at')
    .eq('watchlist_id', watchlistId)
    .in('channel', ['email', 'telegram'])
    .order('updated_at', { ascending: false });

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return (response.data || []) as SubscriptionRow[];
}

function pickChannelState(
  rows: SubscriptionRow[],
  channel: 'email' | 'telegram',
  fallbackDestination: string | null,
  fallbackActive: boolean
): AccountSubscriptionState {
  const activeRow = rows.find((row) => row.channel === channel && row.active) || rows.find((row) => row.channel === channel) || null;

  return {
    id: activeRow?.id || null,
    channel,
    destination: activeRow?.destination || fallbackDestination,
    active: activeRow?.active ?? fallbackActive,
    minimumImportance: activeRow?.minimum_importance ?? null,
  };
}

function pickChannelSubscription(rows: SubscriptionRow[], channel: 'email' | 'telegram') {
  return rows.find((row) => row.channel === channel && row.active) || rows.find((row) => row.channel === channel) || null;
}

async function fetchAlertHistory(subscriptionIds: string[]): Promise<AccountAlertHistoryItem[]> {
  if (!subscriptionIds.length) {
    return [];
  }

  const supabase = getAdminSupabase();
  const response = await supabase
    .from('alert_deliveries')
    .select('id,channel,destination,status,last_error,queued_at,sent_at,signal_events(title,summary,ticker,actor_name,source_url,published_at)')
    .in('subscription_id', subscriptionIds)
    .order('queued_at', { ascending: false })
    .limit(ACCOUNT_HISTORY_LIMIT);

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return (response.data || []).map((row) => {
    const signalEvent = Array.isArray(row.signal_events) ? row.signal_events[0] : row.signal_events;
    return {
      id: String(row.id),
      channel: String(row.channel),
      destination: row.destination ? String(row.destination) : null,
      status: String(row.status),
      lastError: row.last_error ? String(row.last_error) : null,
      queuedAt: String(row.queued_at),
      sentAt: row.sent_at ? String(row.sent_at) : null,
      title: signalEvent?.title ? String(signalEvent.title) : null,
      summary: signalEvent?.summary ? String(signalEvent.summary) : null,
      ticker: signalEvent?.ticker ? String(signalEvent.ticker) : null,
      actorName: signalEvent?.actor_name ? String(signalEvent.actor_name) : null,
      sourceUrl: signalEvent?.source_url ? String(signalEvent.source_url) : null,
      publishedAt: signalEvent?.published_at ? String(signalEvent.published_at) : null,
    };
  });
}

function toAccountProfile(profile: ProfileRow): AccountProfileState {
  return {
    id: profile.id,
    email: profile.email,
    displayName: profile.display_name,
    alertEmail: profile.alert_email,
    telegramUsername: profile.telegram_username,
    telegramChatId: profile.telegram_chat_id,
    emailEnabled: Boolean(profile.email_enabled),
    telegramEnabled: Boolean(profile.telegram_enabled),
    followLimit: Number(profile.follow_limit || DEFAULT_FOLLOW_LIMIT),
  };
}

function toAccountBilling(profile: ProfileRow): AccountBillingState {
  const summary = describeBillingSummary(profile.billing_plan_key, profile.billing_status);
  return {
    ...summary,
    checkoutReady: isBillingCheckoutConfigured(),
    portalReady: isBillingPortalConfigured(),
    stripeCustomerId: profile.stripe_customer_id,
    stripeSubscriptionId: profile.stripe_subscription_id,
    currentPeriodEnd: profile.billing_current_period_end,
    cancelAtPeriodEnd: Boolean(profile.billing_cancel_at_period_end),
  };
}

export async function getAccountState(user: User): Promise<AccountState> {
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const [tickers, actors, subscriptionRows, telegramBotUsername] = await Promise.all([
    fetchWatchlistTickers(watchlist.id),
    fetchWatchlistActors(watchlist.id),
    fetchSubscriptions(watchlist.id),
    getTelegramBotUsername(),
  ]);

  const subscriptionIds = subscriptionRows.map((row) => String(row.id));
  const history = await fetchAlertHistory(subscriptionIds);

  const emailState = pickChannelState(subscriptionRows, 'email', profile.alert_email || user.email || null, profile.email_enabled);
  const telegramState = pickChannelState(
    subscriptionRows,
    'telegram',
    profile.telegram_username ? `@${normalizeTelegramUsername(profile.telegram_username)}` : null,
    profile.telegram_enabled
  );

  return {
    user: {
      id: user.id,
      email: user.email || null,
    },
    profile: toAccountProfile(profile),
    billing: toAccountBilling(profile),
    watchlist: {
      id: watchlist.id,
      name: watchlist.name,
    },
    followCount: tickers.length + actors.length,
    followLimit: profile.follow_limit || DEFAULT_FOLLOW_LIMIT,
    telegramBotUsername,
    subscriptions: {
      email: emailState,
      telegram: telegramState,
    },
    follows: {
      tickers,
      actors,
    },
    history,
  };
}

async function fetchCongressMembers(): Promise<CongressMemberRecord[]> {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('congress_members')
    .select('id,first_name,last_name,chamber,active,state,party')
    .order('active', { ascending: false })
    .order('last_name', { ascending: true });

  if (response.error) {
    throw new Error(response.error.message || 'Failed to load Congress member list.');
  }

  return (response.data || []) as CongressMemberRecord[];
}

function politicianSuggestionScore(member: CongressMemberRecord, query: string) {
  const fullName = `${member.first_name || ''} ${member.last_name || ''}`.trim();
  const normalizedFullName = fullName.toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  const tokens = queryTokens(normalizedQuery);
  const aliasEntry = NOTABLE_POLITICIAN_ALIASES.find((entry) => entry.actorKey === String(member.id).toLowerCase());

  let score = 0;
  if (normalizedFullName === normalizedQuery) score += 120;
  if (normalizedFullName.startsWith(normalizedQuery)) score += 80;
  if (normalizedFullName.includes(normalizedQuery)) score += 45;
  if ((member.last_name || '').toLowerCase().startsWith(normalizedQuery)) score += 35;
  if (aliasEntry) score += aliasMatchScore(aliasEntry.aliases, query);

  const memberTokens = queryTokens(normalizedFullName);
  if (tokens.length && tokens.every((token) => memberTokens.some((memberToken) => memberToken.startsWith(token)))) {
    score += 40;
  }

  if (score > 0 && member.active !== false) {
    score += 10;
  }

  return score;
}

function insiderSuggestionScore(actorName: string, query: string) {
  const normalizedName = actorName.trim().toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  const tokens = queryTokens(normalizedQuery);
  const actorTokens = queryTokens(normalizedName);
  const actorKey = normalizeActorKey(actorName);
  const aliasEntry = NOTABLE_INSIDER_ALIASES.find((entry) => entry.actorKey === actorKey);
  const aliases = mergeAliases(aliasEntry?.aliases);

  let score = 0;
  if (normalizedName === normalizedQuery) score += 120;
  if (normalizedName.startsWith(normalizedQuery)) score += 70;
  if (normalizedName.includes(normalizedQuery)) score += 25;
  if (actorTokens.some((token) => token === normalizedQuery)) score += 60;
  if (actorTokens.some((token) => token.startsWith(normalizedQuery))) score += 40;
  if (aliasEntry) score += 8;
  score += aliasMatchScore(aliases, query);
  if (tokens.length && tokens.every((token) => actorTokens.some((actorToken) => actorToken.startsWith(token)))) {
    score += 30;
  }
  if (
    tokens.length &&
    tokens.every((token) => aliases.some((alias) => queryTokens(alias).some((aliasToken) => aliasToken.startsWith(token))))
  ) {
    score += 40;
  }

  return score;
}

function tickerSuggestionScore(candidate: { ticker: string; companyName: string; rawCompanyName: string | null; aliases: string[] }, query: string) {
  const normalizedQuery = normalizeSearchText(query);
  const tickerQuery = normalizeTickerCandidate(query);
  const companyName = normalizeSearchText(candidate.companyName);
  const aliases = mergeAliases(candidate.aliases, [candidate.ticker, candidate.companyName]);
  const companyTokens = queryTokens(companyName);
  const queryParts = queryTokens(normalizedQuery);

  let score = 0;
  if (tickerQuery && candidate.ticker === tickerQuery) score += 260;
  if (tickerQuery && candidate.ticker.startsWith(tickerQuery)) score += 180;
  if (companyName === normalizedQuery) score += 180;
  if (companyName.startsWith(normalizedQuery)) score += 120;
  if (companyName.includes(normalizedQuery)) score += 60;

  for (const alias of aliases) {
    if (alias === normalizedQuery) score += 210;
    else if (alias.startsWith(normalizedQuery)) score += 130;
    else if (normalizedQuery.length >= 3 && alias.includes(normalizedQuery)) score += 55;
  }

  if (
    queryParts.length &&
    queryParts.every(
      (part) =>
        companyTokens.some((token) => token.startsWith(part)) ||
        aliases.some((alias) => queryTokens(alias).some((token) => token.startsWith(part)))
    )
  ) {
    score += 55;
  }

  if (PRIORITY_TICKER_SET.has(candidate.ticker)) {
    score += 12;
  }
  if (isDirtyCompanyName(candidate.rawCompanyName)) {
    score -= 20;
  }

  return score;
}

async function searchTickerSuggestionsDetailed(query: string): Promise<TickerSuggestionCandidate[]> {
  const trimmedQuery = query.trim();
  const normalizedQuery = normalizeSearchText(trimmedQuery);
  const tickerQuery = normalizeTickerCandidate(trimmedQuery);
  const queryParts = queryTokens(normalizedQuery);

  if (normalizedQuery.length < 2 && tickerQuery.length < 2) {
    return [];
  }

  const candidates = new Map<
    string,
    {
      ticker: string;
      companyName: string;
      rawCompanyName: string | null;
      aliases: string[];
    }
  >();

  for (const alias of TICKER_SEARCH_ALIASES) {
    const aliasTerms = mergeAliases(alias.aliases, [alias.companyName, alias.ticker]);
    const matched =
      alias.ticker === tickerQuery ||
      alias.ticker.startsWith(tickerQuery) ||
      aliasTerms.some((term) => term === normalizedQuery || term.startsWith(normalizedQuery) || term.includes(normalizedQuery));

    if (!matched) {
      continue;
    }

    candidates.set(alias.ticker, {
      ticker: alias.ticker,
      companyName: alias.companyName,
      rawCompanyName: alias.companyName,
      aliases: mergeAliases(alias.aliases, [alias.companyName]),
    });
  }

  const supabase = getAdminSupabase();
  const searchOperands = buildIlikeOperands('name', [trimmedQuery, ...queryParts]);
  const tickerOperands = buildIlikeOperands('ticker', [tickerQuery || trimmedQuery.toUpperCase()], 'prefix');
  const response = await supabase
    .from('companies')
    .select('ticker,name')
    .or([...tickerOperands, ...searchOperands].join(','))
    .limit(80);

  if (response.error) {
    handleSupabaseError(response.error);
  }

  for (const row of response.data || []) {
    const ticker = normalizeTickerCandidate(String(row.ticker || ''));
    if (!ticker) {
      continue;
    }

    const alias = TICKER_SEARCH_ALIAS_BY_TICKER.get(ticker);
    const companyName = cleanCompanyDisplayName(ticker, row.name ? String(row.name) : null);
    const existing = candidates.get(ticker);
    candidates.set(ticker, {
      ticker,
      companyName: existing?.companyName || companyName,
      rawCompanyName: row.name ? String(row.name) : existing?.rawCompanyName || null,
      aliases: mergeAliases(existing?.aliases, alias?.aliases, alias?.companyName ? [companyName, alias.companyName] : [companyName]),
    });
  }

  const ranked = [...candidates.values()]
    .map((candidate) => {
      const score = tickerSuggestionScore(candidate, trimmedQuery);
      const normalizedCompany = normalizeSearchText(candidate.companyName);
      const aliases = mergeAliases(candidate.aliases, [candidate.companyName]);
      const exactMatch =
        candidate.ticker === tickerQuery ||
        normalizedCompany === normalizedQuery ||
        aliases.some((alias) => alias === normalizedQuery);
      const strongMatch =
        exactMatch ||
        normalizedCompany.startsWith(normalizedQuery) ||
        aliases.some((alias) => alias.startsWith(normalizedQuery));

      return {
        ...candidate,
        score,
        exactMatch,
        strongMatch,
      };
    })
    .filter((candidate) => candidate.score > 0)
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return left.ticker.localeCompare(right.ticker);
    });

  const canonicalAliasMatches = ranked.filter((candidate) => TICKER_SEARCH_ALIAS_BY_TICKER.has(candidate.ticker));
  const filtered = ranked.filter((candidate) => {
    if (TICKER_SEARCH_ALIAS_BY_TICKER.has(candidate.ticker)) {
      return true;
    }

    const normalizedCompany = normalizeSearchText(candidate.companyName);
    return !canonicalAliasMatches.some((aliasCandidate) => {
      if (candidate.ticker === aliasCandidate.ticker || candidate.ticker.length <= 4) {
        return false;
      }

      const normalizedAliasCompany = normalizeSearchText(aliasCandidate.companyName);
      return (
        normalizedCompany === normalizedAliasCompany ||
        normalizedCompany.startsWith(normalizedAliasCompany) ||
        normalizedCompany.includes(normalizedAliasCompany)
      );
    });
  });

  return filtered.slice(0, 8);
}

async function fetchInsiderRowsByAllTokens(
  table: 'signal_events' | 'insider_trades',
  nameColumn: 'actor_name' | 'filer_name',
  dateColumn: 'created_at' | 'transaction_date',
  tokens: string[]
) {
  const supabase = getAdminSupabase();
  let query = supabase.from(table).select(`${nameColumn},ticker,${dateColumn}`);
  if (table === 'signal_events') {
    query = query.eq('actor_type', 'insider');
  }
  for (const token of tokens) {
    query = query.ilike(nameColumn, `%${token}%`);
  }
  return query.order(dateColumn, { ascending: false }).limit(80);
}

async function injectNotableInsiderAliasMatches(
  deduped: Map<string, { actor_name: string; ticker: string | null; created_at: string | null }>,
  query: string
) {
  const normalizedQuery = normalizeSearchText(query);
  const matchingAliases = NOTABLE_INSIDER_ALIASES.filter((entry) => aliasMatchScore(entry.aliases, normalizedQuery) > 0);
  if (!matchingAliases.length) {
    return;
  }

  for (const entry of matchingAliases) {
    if (deduped.has(entry.actorKey)) {
      continue;
    }

    const tokens = queryTokens(entry.canonicalName);
    let foundRow: { actor_name: string; ticker: string | null; created_at: string | null } | null = null;

    const signalResponse = await fetchInsiderRowsByAllTokens('signal_events', 'actor_name', 'created_at', tokens);
    const signalRow = signalResponse.data?.[0] as InsiderSignalRow | undefined;
    if (!signalResponse.error && signalRow) {
      foundRow = {
        actor_name: String(signalRow.actor_name || entry.canonicalName),
        ticker: signalRow.ticker ? String(signalRow.ticker) : null,
        created_at: signalRow.created_at ? String(signalRow.created_at) : null,
      };
    }

    if (!foundRow) {
      const fallback = await fetchInsiderRowsByAllTokens('insider_trades', 'filer_name', 'transaction_date', tokens);
      const fallbackRow = fallback.data?.[0] as InsiderTradeRow | undefined;
      if (!fallback.error && fallbackRow) {
        foundRow = {
          actor_name: String(fallbackRow.filer_name || entry.canonicalName),
          ticker: fallbackRow.ticker ? String(fallbackRow.ticker) : null,
          created_at: fallbackRow.transaction_date ? String(fallbackRow.transaction_date) : null,
        };
      }
    }

    if (foundRow) {
      deduped.set(entry.actorKey, foundRow);
    }
  }
}

export async function searchTickerSuggestions(query: string): Promise<AccountTickerSuggestion[]> {
  const suggestions = await searchTickerSuggestionsDetailed(query);
  return suggestions.map((candidate) => ({
    ticker: candidate.ticker,
    companyName: candidate.companyName,
    subtitle: candidate.ticker === candidate.companyName ? null : candidate.companyName,
  }));
}

async function resolveTickerTarget(value: string) {
  const trimmedValue = value.trim();
  if (!trimmedValue) {
    throw new Error('Enter a ticker or company name.');
  }

  const normalizedTicker = normalizeTickerCandidate(trimmedValue);
  const suggestions = await searchTickerSuggestionsDetailed(trimmedValue);
  const [top, second] = suggestions;

  if (normalizedTicker) {
    const exactTicker = suggestions.find((candidate) => candidate.ticker === normalizedTicker);
    if (exactTicker) {
      return exactTicker.ticker;
    }
    if (TICKER_SEARCH_ALIAS_BY_TICKER.has(normalizedTicker)) {
      return normalizedTicker;
    }
  }

  if (top && (top.exactMatch || suggestions.length === 1 || (top.strongMatch && (!second || top.score >= second.score + 35)))) {
    return top.ticker;
  }

  throw new Error('Could not confidently match that ticker. Choose a suggestion or enter the exact symbol.');
}

async function resolveInsiderTarget(rawName: string, preferredActorKey?: string | null) {
  const trimmedName = rawName.trim();
  if (!trimmedName) {
    throw new Error('Enter an insider name.');
  }

  const ranked = await searchInsiderSuggestionsDetailed(trimmedName);
  if (preferredActorKey) {
    const preferred = ranked.find((candidate) => candidate.actorKey === preferredActorKey);
    if (preferred) {
      return preferred;
    }
  }

  const [top, second] = ranked;
  if (top && (top.exactMatch || ranked.length === 1 || (top.strongMatch && (!second || top.score >= second.score + 35)))) {
    return top;
  }

  if (preferredActorKey && trimmedName) {
    return {
      actorName: trimmedName,
      actorKey: preferredActorKey,
      subtitle: null,
      score: 0,
      recency: '',
      exactMatch: false,
      strongMatch: false,
    };
  }

  throw new Error('Could not confidently match that insider. Choose a suggestion or use the exact filed name.');
}

export async function searchPoliticianSuggestions(query: string): Promise<AccountFollowSuggestion[]> {
  const trimmedQuery = query.trim();
  if (trimmedQuery.length < 2) {
    return [];
  }

  const members = await fetchCongressMembers();
  const ranked = members
    .filter((member) => !String(member.id || '').startsWith('unknown-'))
    .map((member) => ({
      member,
      score: politicianSuggestionScore(member, trimmedQuery),
    }))
    .filter((entry) => entry.score > 0)
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      if ((left.member.active ?? true) !== (right.member.active ?? true)) {
        return left.member.active === false ? 1 : -1;
      }
      return `${left.member.last_name || ''} ${left.member.first_name || ''}`.localeCompare(
        `${right.member.last_name || ''} ${right.member.first_name || ''}`
      );
    })
    .slice(0, 8);

  return ranked.map(({ member }) => {
    const actorName = `${member.first_name || ''} ${member.last_name || ''}`.trim();
    const subtitleBits = [member.party, member.state, member.chamber].filter(Boolean);
    return {
      actorType: 'politician',
      actorName,
      actorKey: String(member.id).toLowerCase(),
      subtitle: subtitleBits.length ? subtitleBits.join(' • ') : null,
    };
  });
}

async function searchInsiderSuggestionsDetailed(query: string): Promise<InsiderSuggestionCandidate[]> {
  const trimmedQuery = query.trim();
  if (trimmedQuery.length < 2) {
    return [];
  }

  const normalizedQuery = normalizeSearchText(trimmedQuery);
  const queryParts = queryTokens(trimmedQuery);

  const supabase = getAdminSupabase();
  const response =
    queryParts.length > 1
      ? await fetchInsiderRowsByAllTokens('signal_events', 'actor_name', 'created_at', queryParts)
      : await supabase
          .from('signal_events')
          .select('actor_name,ticker,created_at')
          .eq('actor_type', 'insider')
          .or(buildIlikeOperands('actor_name', [trimmedQuery, ...queryParts]).join(','))
          .order('created_at', { ascending: false })
          .limit(80);

  if (response.error) {
    handleSupabaseError(response.error);
  }

  const deduped = new Map<string, { actor_name: string; ticker: string | null; created_at: string | null }>();
  for (const row of (response.data || []) as InsiderSignalRow[]) {
    const actorName = String(row.actor_name || '').trim();
    const actorKey = normalizeActorKey(actorName);
    if (!actorName || !actorKey || deduped.has(actorKey)) {
      continue;
    }
    deduped.set(actorKey, {
      actor_name: actorName,
      ticker: row.ticker ? String(row.ticker) : null,
      created_at: row.created_at ? String(row.created_at) : null,
    });
  }

  if (deduped.size < 5) {
    const fallback =
      queryParts.length > 1
        ? await fetchInsiderRowsByAllTokens('insider_trades', 'filer_name', 'transaction_date', queryParts)
        : await supabase
            .from('insider_trades')
            .select('filer_name,ticker,transaction_date')
            .or(buildIlikeOperands('filer_name', [trimmedQuery, ...queryParts]).join(','))
            .order('transaction_date', { ascending: false })
            .limit(80);

    if (fallback.error) {
      handleSupabaseError(fallback.error);
    }

    for (const row of (fallback.data || []) as InsiderTradeRow[]) {
      const actorName = String(row.filer_name || '').trim();
      const actorKey = normalizeActorKey(actorName);
      if (!actorName || !actorKey || deduped.has(actorKey)) {
        continue;
      }
      deduped.set(actorKey, {
        actor_name: actorName,
        ticker: row.ticker ? String(row.ticker) : null,
        created_at: row.transaction_date ? String(row.transaction_date) : null,
      });
    }
  }

  await injectNotableInsiderAliasMatches(deduped, normalizedQuery);

  return [...deduped.entries()]
    .map(([actorKey, row]) => ({
      actorName: row.actor_name,
      actorKey,
      subtitle: row.ticker ? `Recent trade in ${row.ticker}` : null,
      score: insiderSuggestionScore(row.actor_name, trimmedQuery),
      recency: row.created_at || '',
      exactMatch: normalizeSearchText(row.actor_name) === normalizedQuery,
      strongMatch:
        normalizeSearchText(row.actor_name).startsWith(normalizedQuery) ||
        Boolean(
          NOTABLE_INSIDER_ALIASES.find((entry) => entry.actorKey === actorKey)?.aliases.some(
            (alias) => normalizeSearchText(alias) === normalizedQuery || normalizeSearchText(alias).startsWith(normalizedQuery)
          )
        ),
    }))
    .filter((row) => row.score > 0)
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return right.recency.localeCompare(left.recency);
    })
    .slice(0, 8);
}

export async function searchInsiderSuggestions(query: string): Promise<AccountFollowSuggestion[]> {
  const ranked = await searchInsiderSuggestionsDetailed(query);
  return ranked.map(({ actorName, actorKey, subtitle }) => ({
      actorType: 'insider',
      actorName,
      actorKey,
      subtitle,
    }));
}

async function countCurrentFollows(watchlistId: string) {
  const [tickers, actors] = await Promise.all([fetchWatchlistTickers(watchlistId), fetchWatchlistActors(watchlistId)]);
  return tickers.length + actors.length;
}

function assertCanAddFollow(count: number, limit: number) {
  if (count >= limit) {
    throw new Error(`Follow limit reached. Your current plan supports ${limit} follows.`);
  }
}

export async function addTickerFollow(user: User, ticker: string, alertMode: AlertMode) {
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const normalizedTicker = await resolveTickerTarget(ticker);

  const existing = await supabase
    .from('watchlist_tickers')
    .select('id')
    .eq('watchlist_id', watchlist.id)
    .eq('ticker', normalizedTicker)
    .limit(1);

  if (existing.error) {
    handleSupabaseError(existing.error);
  }

  if (!(existing.data?.length)) {
    const count = await countCurrentFollows(watchlist.id);
    assertCanAddFollow(count, profile.follow_limit || DEFAULT_FOLLOW_LIMIT);
  }

  const response = await supabase
    .from('watchlist_tickers')
    .upsert({ watchlist_id: watchlist.id, ticker: normalizedTicker, alert_mode: alertMode }, { onConflict: 'watchlist_id,ticker' });

  if (response.error) {
    handleSupabaseError(response.error);
  }
}

export async function addActorFollow(
  user: User,
  actorType: ActorType,
  actorName: string,
  alertMode: AlertMode,
  preferredActorKey?: string | null
) {
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const trimmedName = actorName.trim();
  if (!trimmedName) {
    throw new Error('Enter a person name.');
  }

  let actorKey = '';
  let resolvedName = trimmedName;
  let metadata: Record<string, unknown> = { resolved_from: trimmedName };

  if (actorType === 'politician') {
    const members = await fetchCongressMembers();
    const resolved =
      (preferredActorKey ? members.find((member) => member.id.toLowerCase() === preferredActorKey.toLowerCase()) : null) ||
      resolvePoliticianTarget(trimmedName, members);
    if (!resolved) {
      throw new Error('Could not match that politician. Use the full name or member id.');
    }
    actorKey = resolved.id.toLowerCase();
    resolvedName = `${resolved.first_name || ''} ${resolved.last_name || ''}`.trim();
    metadata = {
      member_id: resolved.id,
      chamber: resolved.chamber,
      state: resolved.state,
      party: resolved.party,
      resolved_from: trimmedName,
    };
  } else {
    const resolved = await resolveInsiderTarget(trimmedName, preferredActorKey);
    actorKey = resolved.actorKey;
    resolvedName = resolved.actorName;
    metadata = {
      resolved_from: trimmedName,
      subtitle: resolved.subtitle,
    };
  }

  const existing = await supabase
    .from('watchlist_actors')
    .select('id')
    .eq('watchlist_id', watchlist.id)
    .eq('actor_type', actorType)
    .eq('actor_key', actorKey)
    .limit(1);

  if (existing.error) {
    handleSupabaseError(existing.error);
  }

  if (!(existing.data?.length)) {
    const count = await countCurrentFollows(watchlist.id);
    assertCanAddFollow(count, profile.follow_limit || DEFAULT_FOLLOW_LIMIT);
  }

  const response = await supabase.from('watchlist_actors').upsert(
    {
      watchlist_id: watchlist.id,
      actor_type: actorType,
      actor_key: actorKey,
      actor_name: resolvedName,
      alert_mode: alertMode,
      metadata,
    },
    { onConflict: 'watchlist_id,actor_type,actor_key' }
  );

  if (response.error) {
    handleSupabaseError(response.error);
  }
}

export async function updateTickerFollowMode(user: User, followId: string, alertMode: AlertMode) {
  const { watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('watchlist_tickers')
    .update({ alert_mode: alertMode })
    .eq('id', followId)
    .eq('watchlist_id', watchlist.id);

  if (response.error) {
    handleSupabaseError(response.error);
  }
}

export async function updateActorFollowMode(user: User, followId: string, alertMode: AlertMode) {
  const { watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('watchlist_actors')
    .update({ alert_mode: alertMode })
    .eq('id', followId)
    .eq('watchlist_id', watchlist.id);

  if (response.error) {
    handleSupabaseError(response.error);
  }
}

export async function deleteTickerFollow(user: User, followId: string) {
  const { watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('watchlist_tickers')
    .delete()
    .eq('id', followId)
    .eq('watchlist_id', watchlist.id);

  if (response.error) {
    handleSupabaseError(response.error);
  }
}

export async function deleteActorFollow(user: User, followId: string) {
  const { watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('watchlist_actors')
    .delete()
    .eq('id', followId)
    .eq('watchlist_id', watchlist.id);

  if (response.error) {
    handleSupabaseError(response.error);
  }
}

async function setSubscriptionState(
  watchlistId: string,
  channel: 'email' | 'telegram',
  destination: string | null,
  active: boolean
) {
  const supabase = getAdminSupabase();
  const existing = await supabase
    .from('alert_subscriptions')
    .select('id')
    .eq('watchlist_id', watchlistId)
    .eq('channel', channel)
    .order('created_at', { ascending: true });

  if (existing.error) {
    handleSupabaseError(existing.error);
  }

  const rows = existing.data || [];
  if (!rows.length && !active) {
    return;
  }

  if (!rows.length && active) {
    const inserted = await supabase.from('alert_subscriptions').insert({
      watchlist_id: watchlistId,
      channel,
      destination,
      active: true,
      minimum_importance: DEFAULT_MIN_IMPORTANCE,
      event_types: [],
    });
    if (inserted.error) {
      handleSupabaseError(inserted.error);
    }
    return;
  }

  const [primary, ...rest] = rows;
  const primaryUpdate = await supabase
    .from('alert_subscriptions')
    .update({
      destination,
      active,
      minimum_importance: DEFAULT_MIN_IMPORTANCE,
      event_types: [],
    })
    .eq('id', primary.id);

  if (primaryUpdate.error) {
    handleSupabaseError(primaryUpdate.error);
  }

  if (rest.length) {
    const extraIds = rest.map((row) => row.id);
    const deactivateExtras = await supabase.from('alert_subscriptions').update({ active: false }).in('id', extraIds);
    if (deactivateExtras.error) {
      handleSupabaseError(deactivateExtras.error);
    }
  }
}

export async function updateEmailDelivery(user: User, alertEmail: string | null, enabled: boolean) {
  const { watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const normalizedEmail = normalizeAlertEmail(alertEmail || user.email || '');

  if (enabled && !normalizedEmail) {
    throw new Error('Add an email address before enabling email alerts.');
  }

  const profileUpdate = await supabase
    .from('profiles')
    .update({
      alert_email: normalizedEmail,
      email_enabled: enabled,
      email: user.email || normalizedEmail,
    })
    .eq('id', user.id);

  if (profileUpdate.error) {
    handleSupabaseError(profileUpdate.error);
  }

  await setSubscriptionState(watchlist.id, 'email', normalizedEmail, enabled);
}

export async function updateTelegramDelivery(user: User, telegramUsername: string | null, enabled: boolean) {
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const supabase = getAdminSupabase();
  const normalizedUsername = normalizeTelegramUsername(telegramUsername || profile.telegram_username || '');

  let chatId = profile.telegram_chat_id || null;
  if (enabled) {
    if (!normalizedUsername) {
      throw new Error('Enter your Telegram username and message the Vail bot first.');
    }
    chatId = await resolveTelegramChatId(normalizedUsername);
  }

  const profileUpdate = await supabase
    .from('profiles')
    .update({
      telegram_username: normalizedUsername || null,
      telegram_chat_id: chatId,
      telegram_enabled: enabled,
    })
    .eq('id', user.id);

  if (profileUpdate.error) {
    handleSupabaseError(profileUpdate.error);
  }

  await setSubscriptionState(watchlist.id, 'telegram', enabled ? chatId : chatId, enabled);
}

function testAlertCopy(displayName: string | null | undefined) {
  const subject = 'Vail test alert: delivery is working';
  const summary = 'This is a test alert from your private Vail workspace.';
  const title = 'Vail test alert';
  const greeting = displayName ? `Hi ${displayName},` : 'Hi,';
  const text = [
    greeting,
    '',
    summary,
    '',
    'Your account is configured correctly and live signals will arrive here when one of your follows matches.',
    '',
    'Next steps:',
    '- Add a stock follow',
    '- Add a politician or insider follow',
    '- Choose activity, unusual, or both',
    '',
    'Vail',
  ].join('\n');

  const html = `
    <div style="font-family:Arial,sans-serif;line-height:1.6;color:#111827;">
      <p>${greeting}</p>
      <p>${summary}</p>
      <p>Your account is configured correctly and live signals will arrive here when one of your follows matches.</p>
      <p><strong>Next steps:</strong></p>
      <ul>
        <li>Add a stock follow</li>
        <li>Add a politician or insider follow</li>
        <li>Choose activity, unusual, or both</li>
      </ul>
      <p>Vail</p>
    </div>
  `.trim();

  const telegram = [
    'Vail test alert',
    '',
    'Your private alert delivery is working.',
    'Live signals for your follows will arrive here when something matches.',
  ].join('\n');

  return { subject, summary, title, text, html, telegram };
}

export async function sendAccountTestAlert(user: User) {
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const subscriptionRows = await fetchSubscriptions(watchlist.id);
  const emailSubscription = pickChannelSubscription(subscriptionRows, 'email');
  const telegramSubscription = pickChannelSubscription(subscriptionRows, 'telegram');
  const copy = testAlertCopy(profile.display_name || user.email || null);
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  const sourceDocumentId = `account-test-${user.id}-${now.getTime()}`;
  const sentChannels: string[] = [];
  const skippedChannels: string[] = [];

  if (profile.email_enabled && emailSubscription?.destination) {
    await sendEmailMessage({
      to: emailSubscription.destination,
      subject: copy.subject,
      text: copy.text,
      html: copy.html,
    });
    sentChannels.push('email');
  } else {
    skippedChannels.push('email');
  }

  if (profile.telegram_enabled && telegramSubscription?.destination) {
    await sendTelegramMessage(telegramSubscription.destination, copy.telegram);
    sentChannels.push('telegram');
  } else {
    skippedChannels.push('telegram');
  }

  if (!sentChannels.length) {
    throw new Error('Enable email or Telegram first so Vail has a destination for the test alert.');
  }

  const supabase = getAdminSupabase();
  const signalInsert = await supabase
    .from('signal_events')
    .insert({
      source: 'vail',
      signal_type: 'account_test',
      source_document_id: sourceDocumentId,
      ticker: TEST_ALERT_TICKER,
      actor_name: 'Vail',
      actor_type: 'system',
      occurred_at: today,
      published_at: today,
      importance_score: 1,
      title: copy.title,
      summary: `${copy.summary} Channels: ${sentChannels.join(', ')}.`,
      source_url: '/alerts',
      payload: {
        sent_channels: sentChannels,
        skipped_channels: skippedChannels,
      },
    })
    .select('id')
    .single();

  if (signalInsert.error) {
    handleSupabaseError(signalInsert.error);
  }

  const deliveries = [];
  if (sentChannels.includes('email') && emailSubscription) {
    deliveries.push({
      signal_event_id: signalInsert.data.id,
      subscription_id: emailSubscription.id,
      delivery_key: `test:${sourceDocumentId}:${emailSubscription.id}`,
      channel: 'email',
      destination: emailSubscription.destination,
      status: 'sent',
      attempts: 1,
      payload: { kind: 'account_test' },
      queued_at: now.toISOString(),
      sent_at: now.toISOString(),
    });
  }
  if (sentChannels.includes('telegram') && telegramSubscription) {
    deliveries.push({
      signal_event_id: signalInsert.data.id,
      subscription_id: telegramSubscription.id,
      delivery_key: `test:${sourceDocumentId}:${telegramSubscription.id}`,
      channel: 'telegram',
      destination: telegramSubscription.destination,
      status: 'sent',
      attempts: 1,
      payload: { kind: 'account_test' },
      queued_at: now.toISOString(),
      sent_at: now.toISOString(),
    });
  }

  if (deliveries.length) {
    const deliveryInsert = await supabase.from('alert_deliveries').insert(deliveries);
    if (deliveryInsert.error) {
      handleSupabaseError(deliveryInsert.error);
    }
  }

  return { sentChannels, skippedChannels };
}

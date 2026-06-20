import 'server-only';

import type { User } from '@supabase/supabase-js';

import type {
  AccountActorFollow,
  AccountAlertPreview,
  AccountAlertHistoryItem,
  AccountFollowSignalPreview,
  AccountMatchedSignal,
  AccountBillingState,
  AccountFollowSuggestion,
  AccountProfileState,
  AccountState,
  AccountSubscriptionState,
  AccountTickerSuggestion,
  AccountTickerFollow,
  ActorType,
  AlertMode,
  ClusterAlertChannel,
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
import { normalizePhoneNumber, sendSmsMessage } from '@/lib/sms';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { formatFundChangeLabel, getFundChangeKind } from '@/lib/fund-holdings';
import {
  buildIlikeOperands,
  DIRTY_COMPANY_NAME_PATTERNS,
  normalizeSearchText,
  normalizeTickerCandidate,
  queryTokens,
} from '@/lib/shared-search-utils';
import {
  readSearchAliases,
  type TickerSearchAlias,
  type SearchAliases,
} from '@/lib/signal-policy';

const DEFAULT_WATCHLIST_NAME = 'My Signals';
const DEFAULT_MIN_IMPORTANCE = 0.75;
const DEFAULT_FOLLOW_LIMIT = getFreeFollowLimit();
const ACCOUNT_HISTORY_LIMIT = 30;
const ACCOUNT_SIGNAL_PREVIEW_SCAN_LIMIT = 150;
const ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT = 20;
const ACCOUNT_SIGNAL_PREVIEW_PER_FOLLOW_LIMIT = 4;
const ACCOUNT_SIGNAL_PREVIEW_TIMELINE_LIMIT = 48;
const ACCOUNT_ALERT_PREVIEW_CACHE_TTL_MS = 60 * 1000;
const TEST_ALERT_TICKER = 'VAILTEST';
const VALID_ALERT_MODES = new Set<AlertMode>(['activity', 'unusual', 'both']);
const VALID_CLUSTER_ALERT_CHANNELS = new Set<ClusterAlertChannel>(['email', 'sms']);
const FUND_FILING_SIGNAL_TYPES = ['fund_filing_deadline_reminder', 'fund_filing_received'] as const;
const SIGNAL_EVENT_PREVIEW_SELECT =
  'id,source,signal_type,ticker,actor_name,actor_type,direction,occurred_at,published_at,importance_score,title,summary,source_url,payload,created_at';

const accountAlertPreviewCache = new Map<
  string,
  { expiresAt: number; preview: AccountAlertPreview }
>();

type AccountStateOptions = {
  includeHistory?: boolean;
  includeAlertPreview?: boolean;
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

type PoliticianActivityRow = {
  id: string;
  member_id?: string | null;
  politician_name?: string | null;
  chamber?: string | null;
  ticker?: string | null;
  asset_name?: string | null;
  transaction_type?: string | null;
  amount_range?: string | null;
  published_date?: string | null;
  transaction_date?: string | null;
  source_url?: string | null;
};

type InsiderActivityRow = {
  id: string;
  ticker?: string | null;
  filer_name?: string | null;
  filer_relation?: string | null;
  transaction_code?: string | null;
  amount?: number | string | null;
  price?: number | string | null;
  value?: number | string | null;
  published_date?: string | null;
  transaction_date?: string | null;
  source_url?: string | null;
};

type FundActivityRow = {
  id: string;
  fund_name?: string | null;
  ticker?: string | null;
  report_period?: string | null;
  published_date?: string | null;
  shares_held?: number | string | null;
  value_held?: number | string | null;
  qoq_change_shares?: number | string | null;
  qoq_change_percent?: number | string | null;
  source_url?: string | null;
};

// Lazy-loaded search aliases from signal-policy.json
let _aliasesCache: SearchAliases | null = null;
async function getSearchAliases(): Promise<SearchAliases> {
  if (!_aliasesCache) {
    _aliasesCache = await readSearchAliases();
  }
  return _aliasesCache;
}

async function getTickerSearchAliases(): Promise<TickerSearchAlias[]> {
  return (await getSearchAliases()).tickerAliases;
}

async function getTickerAliasByTicker(): Promise<Map<string, TickerSearchAlias>> {
  const aliases = await getTickerSearchAliases();
  return new Map(aliases.map((entry) => [entry.ticker, entry]));
}

async function getNotablePoliticianAliases(): Promise<Array<{ actorKey: string; aliases: string[] }>> {
  return (await getSearchAliases()).politicianAliases;
}

async function getNotableInsiderAliases(): Promise<Array<{ actorKey: string; canonicalName: string; aliases: string[] }>> {
  const aliases = (await getSearchAliases()).insiderAliases;
  return aliases.map((entry) => ({
    actorKey: normalizeActorKey(entry.canonicalName),
    canonicalName: entry.canonicalName,
    aliases: entry.aliases,
  }));
}

const PRIORITY_TICKER_SET = new Set(['NVDA', 'PLTR', 'IONQ', 'QBTS', 'RGTI', 'SKYT', 'VLD', 'OKLO', 'SMR', 'NNE']);

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
  email_enabled: boolean;
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

type SignalEventPreviewRow = {
  id: string;
  source: string | null;
  signal_type: string | null;
  ticker: string | null;
  actor_name: string | null;
  actor_type: string | null;
  direction: string | null;
  occurred_at: string | null;
  published_at: string | null;
  importance_score: number | string | null;
  title: string | null;
  summary: string | null;
  source_url: string | null;
  payload: Record<string, unknown> | null;
  created_at: string | null;
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

async function cleanCompanyDisplayName(ticker: string, rawName: string | null | undefined) {
  const aliasMap = await getTickerAliasByTicker();
  const alias = aliasMap.get(ticker);
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
      'id,email,display_name,alert_email,email_enabled,follow_limit,billing_plan_key,billing_status,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_current_period_end,billing_cancel_at_period_end'
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
        'id,email,display_name,alert_email,email_enabled,follow_limit,billing_plan_key,billing_status,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_current_period_end,billing_cancel_at_period_end'
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
      'id,email,display_name,alert_email,email_enabled,follow_limit,billing_plan_key,billing_status,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_current_period_end,billing_cancel_at_period_end'
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
  await ensureDefaultEmailSubscription(profile, watchlist, user);
  return { profile, watchlist };
}

async function ensureDefaultEmailSubscription(profile: ProfileRow, watchlist: WatchlistRow, user: User) {
  const normalizedEmail = normalizeAlertEmail(profile.alert_email || user.email || '');
  if (!profile.email_enabled || !normalizedEmail) {
    return;
  }

  const supabase = getAdminSupabase();
  const existing = await supabase
    .from('alert_subscriptions')
    .select('id,destination,active')
    .eq('watchlist_id', watchlist.id)
    .eq('channel', 'email')
    .order('created_at', { ascending: true });

  if (existing.error) {
    handleSupabaseError(existing.error);
  }

  const rows = existing.data || [];
  if (!rows.length) {
    const inserted = await supabase.from('alert_subscriptions').insert({
      watchlist_id: watchlist.id,
      channel: 'email',
      destination: normalizedEmail,
      active: true,
      minimum_importance: DEFAULT_MIN_IMPORTANCE,
      event_types: [],
    });
    if (inserted.error) {
      handleSupabaseError(inserted.error);
    }
    return;
  }

  const primary = rows[0];
  if (primary.destination !== normalizedEmail || !primary.active) {
    const updated = await supabase
      .from('alert_subscriptions')
      .update({
        destination: normalizedEmail,
        active: true,
        minimum_importance: DEFAULT_MIN_IMPORTANCE,
        event_types: [],
      })
      .eq('id', primary.id);

    if (updated.error) {
      handleSupabaseError(updated.error);
    }
  }
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
    .in('channel', ['email', 'sms'])
    .order('updated_at', { ascending: false });

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return (response.data || []) as SubscriptionRow[];
}

function pickChannelState(
  rows: SubscriptionRow[],
  channel: 'email' | 'sms',
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

function pickChannelSubscription(rows: SubscriptionRow[], channel: 'email' | 'sms') {
  return rows.find((row) => row.channel === channel && row.active) || rows.find((row) => row.channel === channel) || null;
}

async function fetchAlertHistory(subscriptionIds: string[]): Promise<AccountAlertHistoryItem[]> {
  if (!subscriptionIds.length) {
    return [];
  }

  const supabase = getAdminSupabase();
  const selectColumns =
    'id,signal_event_id,channel,destination,status,last_error,queued_at,sent_at,signal_events(title,summary,ticker,actor_name,source_url,published_at)';
  const [sentResponse, recentResponse] = await Promise.all([
    supabase
      .from('alert_deliveries')
      .select(selectColumns)
      .in('subscription_id', subscriptionIds)
      .eq('status', 'sent')
      .order('sent_at', { ascending: false })
      .limit(ACCOUNT_HISTORY_LIMIT),
    supabase
      .from('alert_deliveries')
      .select(selectColumns)
      .in('subscription_id', subscriptionIds)
      .neq('status', 'sent')
      .order('queued_at', { ascending: false })
      .limit(ACCOUNT_HISTORY_LIMIT),
  ]);

  if (sentResponse.error) {
    handleSupabaseError(sentResponse.error);
  }
  if (recentResponse.error) {
    handleSupabaseError(recentResponse.error);
  }

  const rowsById = new Map<string, NonNullable<typeof sentResponse.data>[number]>();
  for (const row of [...(sentResponse.data || []), ...(recentResponse.data || [])]) {
    rowsById.set(String(row.id), row);
  }

  return [...rowsById.values()]
    .sort((left, right) => {
      const leftDate = String(left.sent_at || left.queued_at || '');
      const rightDate = String(right.sent_at || right.queued_at || '');
      return rightDate.localeCompare(leftDate);
    })
    .slice(0, ACCOUNT_HISTORY_LIMIT * 2)
    .map((row) => {
    const signalEvent = Array.isArray(row.signal_events) ? row.signal_events[0] : row.signal_events;
    return {
      id: String(row.id),
      signalEventId: row.signal_event_id ? String(row.signal_event_id) : null,
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

function signalDate(row: SignalEventPreviewRow) {
  return String(row.published_at || row.occurred_at || row.created_at || '').trim() || null;
}

function latestPreviewDate(signals: AccountMatchedSignal[]) {
  return signals.reduce<string | null>((latest, signal) => {
    const value = signal.publishedAt || signal.occurredAt;
    if (!value) {
      return latest;
    }
    return !latest || value > latest ? value : latest;
  }, null);
}

function compactDollarValue(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(numeric);
}

function compactNumberValue(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  return new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(numeric);
}

function normalizeActivityTicker(value: string | null | undefined) {
  const ticker = String(value || '').trim().toUpperCase();
  return ticker && ticker !== 'N/A' && ticker !== 'UNKNOWN' ? ticker : null;
}

function activityDate(...values: Array<string | null | undefined>) {
  return values.map((value) => String(value || '').trim()).find(Boolean) || null;
}

function politicianTradeDirection(value: string | null | undefined) {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized.includes('purchase') || normalized === 'buy' || normalized === 'p') {
    return { direction: 'buy' as const, verb: 'bought', label: 'Buy' };
  }
  if (normalized.includes('sale') || normalized.includes('sell') || normalized === 's') {
    return { direction: 'sell' as const, verb: 'sold', label: 'Sell' };
  }
  return { direction: 'other' as const, verb: 'reported activity in', label: titleCaseLabel(normalized || 'activity') };
}

function insiderTradeDirection(value: string | null | undefined) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['p', 'a', 'buy', 'purchase'].includes(normalized)) {
    return { direction: 'buy' as const, verb: 'bought', label: 'Buy' };
  }
  if (['s', 'd', 'sell', 'sale'].includes(normalized)) {
    return { direction: 'sell' as const, verb: 'sold', label: 'Sell' };
  }
  return { direction: 'other' as const, verb: 'filed activity in', label: titleCaseLabel(normalized || 'activity') };
}

function fundChangeLabel(row: FundActivityRow) {
  const kind = getFundChangeKind(row);
  const changeLabel = formatFundChangeLabel(row);

  if (kind === 'exit') {
    return { direction: 'sell' as const, title: 'exited', label: 'Fund exit' };
  }
  if (kind === 'new') {
    return { direction: 'buy' as const, title: 'started a position in', label: 'New fund position' };
  }
  if (kind === 'increase') {
    return { direction: 'buy' as const, title: 'increased its position in', label: changeLabel };
  }
  if (kind === 'decrease') {
    return { direction: 'sell' as const, title: 'reduced its position in', label: changeLabel };
  }
  return { direction: 'other' as const, title: 'reported holdings in', label: 'Fund holding' };
}

function titleCaseLabel(value: string) {
  return value
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function payloadString(payload: Record<string, unknown> | null | undefined, key: string) {
  const value = payload?.[key];
  return typeof value === 'string' ? value.trim() : '';
}

function isClusterSignal(row: SignalEventPreviewRow) {
  const signalType = String(row.signal_type || '').toLowerCase();
  const actorType = String(row.actor_type || '').toLowerCase();
  const payload = row.payload || {};
  return (
    actorType === 'cluster' ||
    signalType.includes('cluster') ||
    signalType === 'cross_source_accumulation' ||
    Array.isArray(payload.cluster_event_ids) ||
    Array.isArray(payload.cluster_actors)
  );
}

const CLUSTER_ALERT_EXCLUDED_TICKERS = new Set(['', 'N/A', 'NA', 'UNKNOWN', 'MULTI']);

function isClusterAlertPreviewSignal(row: SignalEventPreviewRow) {
  if (!isClusterSignal(row)) {
    return false;
  }

  const ticker = String(row.ticker || payloadString(row.payload, 'ticker')).trim().toUpperCase();
  if (CLUSTER_ALERT_EXCLUDED_TICKERS.has(ticker)) {
    return false;
  }

  return true;
}

function signalBehavior(row: SignalEventPreviewRow) {
  const signalType = String(row.signal_type || '').toLowerCase();
  const importance = Number(row.importance_score || 0);
  const payload = row.payload || {};
  const cluster = isClusterSignal(row);
  const labels = new Set<string>();

  if (cluster) {
    labels.add(signalType === 'cross_source_accumulation' ? 'Cross-source cluster' : 'Cluster');
  }
  if (importance >= 0.9) {
    labels.add('High-confidence signal');
  } else if (importance >= 0.8) {
    labels.add('Unusual signal');
  }
  if (signalType.includes('gain')) {
    labels.add('Performance update');
  }
  if (signalType.includes('filing_summary') || signalType.includes('grouped')) {
    labels.add('Grouped filing');
  }
  if (signalType.includes('insider')) {
    labels.add('Insider activity');
  }
  if (signalType.includes('politician') || String(row.source || '').toLowerCase() === 'congress') {
    labels.add('Congress activity');
  }
  if ((FUND_FILING_SIGNAL_TYPES as readonly string[]).includes(signalType)) {
    labels.add('13F filing');
  } else if (signalType.includes('fund')) {
    labels.add('Fund positioning');
  }
  if (payload.summary_contains_unusual) {
    labels.add('Unusual filing');
  }

  const unusual =
    cluster ||
    importance >= 0.8 ||
    Boolean(payload.summary_contains_unusual) ||
    /(large|substantial|notable|committee|first|theme|meaningful|gain)/.test(signalType);

  return {
    activity: true,
    unusual,
    labels: [...labels].slice(0, 4),
  };
}

function clusterActors(payload: Record<string, unknown> | null | undefined) {
  const actors = payload?.cluster_actors;
  return Array.isArray(actors) ? actors.filter((actor): actor is Record<string, unknown> => Boolean(actor && typeof actor === 'object')) : [];
}

function signalActorKeys(row: SignalEventPreviewRow) {
  const actorType = String(row.actor_type || '').trim().toLowerCase();
  const signalType = String(row.signal_type || '').trim().toLowerCase();
  const payload = row.payload || {};
  const keys = new Set<string>();

  const addNormalizedKey = (targetActorType: string, value: unknown) => {
    const normalized = normalizeActorKey(String(value || ''));
    if (normalized) {
      keys.add(`${targetActorType}:${normalized}`);
    }
  };

  const addExactKey = (targetActorType: string, value: unknown) => {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized) {
      keys.add(`${targetActorType}:${normalized}`);
    }
  };

  if (actorType === 'politician') {
    addExactKey('politician', payload.member_id);
    addNormalizedKey('politician', payload.politician_name);
    addNormalizedKey('politician', row.actor_name);
  } else if (actorType === 'insider') {
    addNormalizedKey('insider', payload.filer_name);
    addNormalizedKey('insider', row.actor_name);
  } else if (actorType === 'fund' && (FUND_FILING_SIGNAL_TYPES as readonly string[]).includes(signalType)) {
    addNormalizedKey('fund', payload.fund_name);
    addNormalizedKey('fund', row.actor_name);
  } else if (actorType === 'cluster') {
    const baseSignalType = payloadString(payload, 'base_signal_type').toLowerCase();
    for (const actor of clusterActors(payload)) {
      const clusterActorType = String(actor.actor_type || '').trim().toLowerCase();
      if (clusterActorType === 'politician' || baseSignalType === 'politician_trade') {
        addExactKey('politician', actor.member_id);
        addNormalizedKey('politician', actor.name);
      } else if (clusterActorType === 'insider' || baseSignalType === 'insider_trade') {
        addNormalizedKey('insider', actor.name);
      }
    }
  }

  return keys;
}

function followActorKeys(follow: AccountActorFollow) {
  const keys = new Set<string>();
  const actorType = follow.actorType;
  const actorKey = String(follow.actorKey || '').trim().toLowerCase();
  if (actorKey) {
    keys.add(`${actorType}:${actorKey}`);
    keys.add(`${actorType}:${normalizeActorKey(actorKey)}`);
  }
  const normalizedName = normalizeActorKey(follow.actorName);
  if (normalizedName) {
    keys.add(`${actorType}:${normalizedName}`);
  }
  const memberId = typeof follow.metadata.member_id === 'string' ? follow.metadata.member_id.trim().toLowerCase() : '';
  if (follow.actorType === 'politician' && memberId) {
    keys.add(`politician:${memberId}`);
  }
  return keys;
}

function eventMatchesTickerFollow(row: SignalEventPreviewRow, follow: AccountTickerFollow) {
  const ticker = String(follow.ticker || '').trim().toUpperCase();
  const eventTicker = String(row.ticker || payloadString(row.payload, 'ticker')).trim().toUpperCase();
  if (!ticker || eventTicker !== ticker) {
    return null;
  }
  return ['ticker'];
}

function eventMatchesActorFollow(row: SignalEventPreviewRow, follow: AccountActorFollow) {
  const followKeys = followActorKeys(follow);
  const eventKeys = signalActorKeys(row);
  const matched = [...followKeys].some((key) => eventKeys.has(key));
  return matched ? [follow.actorType] : null;
}

function toMatchedSignal(row: SignalEventPreviewRow, matchReasons: string[]): AccountMatchedSignal {
  const behavior = signalBehavior(row);
  const fallbackSignalType = String(row.signal_type || 'signal').trim();
  const actorMemberId = payloadString(row.payload, 'member_id');
  return {
    id: String(row.id),
    title: String(row.title || titleCaseLabel(fallbackSignalType) || 'Signal alert'),
    summary: row.summary ? String(row.summary) : null,
    ticker: row.ticker ? String(row.ticker) : null,
    actorName: row.actor_name ? String(row.actor_name) : null,
    actorMemberId: actorMemberId || null,
    signalType: fallbackSignalType,
    source: row.source ? String(row.source) : null,
    direction: row.direction ? String(row.direction) : null,
    importanceScore: Number(row.importance_score || 0),
    occurredAt: row.occurred_at ? String(row.occurred_at) : null,
    publishedAt: signalDate(row),
    sourceUrl: row.source_url ? String(row.source_url) : null,
    matchReasons,
    behaviorLabels: behavior.labels.length ? behavior.labels : [titleCaseLabel(fallbackSignalType)],
    isCluster: isClusterSignal(row),
  };
}

function toPoliticianActivitySignal(row: PoliticianActivityRow, matchReasons: string[]): AccountMatchedSignal {
  const trade = politicianTradeDirection(row.transaction_type);
  const ticker = normalizeActivityTicker(row.ticker);
  const actorName = String(row.politician_name || '').trim() || 'Congress member';
  const assetLabel = String(row.asset_name || '').trim();
  const amountLabel = String(row.amount_range || '').trim();
  const summaryParts = [
    amountLabel,
    assetLabel && assetLabel !== ticker ? assetLabel : null,
    row.chamber ? `${row.chamber} filing` : 'Congress filing',
    row.transaction_date ? `Trade date ${row.transaction_date}` : null,
  ].filter(Boolean);

  return {
    id: `politician-trade:${row.id}`,
    title: `${actorName} ${trade.verb} ${ticker || assetLabel || 'a security'}`,
    summary: summaryParts.length ? summaryParts.join(' • ') : null,
    ticker,
    actorName,
    actorMemberId: row.member_id ? String(row.member_id) : null,
    signalType: 'politician_trade',
    source: 'politician_trades',
    direction: trade.direction,
    importanceScore: trade.direction === 'buy' ? 0.72 : 0.66,
    occurredAt: activityDate(row.transaction_date),
    publishedAt: activityDate(row.published_date, row.transaction_date),
    sourceUrl: row.source_url ? String(row.source_url) : null,
    matchReasons,
    behaviorLabels: ['Congress trade', trade.label],
    isCluster: false,
  };
}

function toInsiderActivitySignal(row: InsiderActivityRow, matchReasons: string[]): AccountMatchedSignal {
  const trade = insiderTradeDirection(row.transaction_code);
  const ticker = normalizeActivityTicker(row.ticker);
  const actorName = String(row.filer_name || '').trim() || 'Corporate insider';
  const valueLabel = compactDollarValue(row.value);
  const amountLabel = compactNumberValue(row.amount);
  const priceLabel = compactDollarValue(row.price);
  const summaryParts = [
    row.filer_relation ? String(row.filer_relation) : 'Insider filing',
    amountLabel ? `${amountLabel} shares` : null,
    priceLabel ? `at ${priceLabel}` : null,
    valueLabel ? `${valueLabel} value` : null,
    row.transaction_date ? `Trade date ${row.transaction_date}` : null,
  ].filter(Boolean);

  return {
    id: `insider-trade:${row.id}`,
    title: `${actorName} ${trade.verb} ${ticker || 'a security'}`,
    summary: summaryParts.length ? summaryParts.join(' • ') : null,
    ticker,
    actorName,
    signalType: 'insider_trade',
    source: 'insider_trades',
    direction: trade.direction,
    importanceScore: trade.direction === 'buy' ? 0.7 : 0.64,
    occurredAt: activityDate(row.transaction_date),
    publishedAt: activityDate(row.published_date, row.transaction_date),
    sourceUrl: row.source_url ? String(row.source_url) : null,
    matchReasons,
    behaviorLabels: ['Insider filing', trade.label],
    isCluster: false,
  };
}

function toFundActivitySignal(row: FundActivityRow, matchReasons: string[]): AccountMatchedSignal {
  const change = fundChangeLabel(row);
  const ticker = normalizeActivityTicker(row.ticker);
  const actorName = String(row.fund_name || '').trim() || 'Hedge fund';
  const valueLabel = compactDollarValue(Number(row.value_held || 0) / 1_000);
  const shareLabel = compactNumberValue(row.shares_held);
  const summaryParts = [
    row.report_period ? `Report period ${row.report_period}` : '13F filing',
    shareLabel ? `${shareLabel} shares held` : null,
    valueLabel ? `${valueLabel} position` : null,
    row.published_date ? `Filed ${row.published_date}` : null,
  ].filter(Boolean);

  return {
    id: `fund-holding:${row.id}`,
    title: `${actorName} ${change.title} ${ticker || 'a security'}`,
    summary: summaryParts.length ? summaryParts.join(' • ') : null,
    ticker,
    actorName,
    signalType: 'fund_position_change',
    source: 'institutional_holdings',
    direction: change.direction,
    importanceScore: change.direction === 'buy' ? 0.68 : 0.62,
    occurredAt: activityDate(row.report_period),
    publishedAt: activityDate(row.published_date, row.report_period),
    sourceUrl: row.source_url ? String(row.source_url) : null,
    matchReasons,
    behaviorLabels: ['Fund 13F', change.label],
    isCluster: false,
  };
}

function sortMatchedSignals(signals: AccountMatchedSignal[]) {
  return [...signals].sort((left, right) => {
    const leftDate = left.publishedAt || left.occurredAt || '';
    const rightDate = right.publishedAt || right.occurredAt || '';
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }
    return right.importanceScore - left.importanceScore;
  });
}

function dedupeMatchedSignals(signals: AccountMatchedSignal[]) {
  const deduped = new Map<string, AccountMatchedSignal>();
  for (const signal of sortMatchedSignals(signals)) {
    if (!deduped.has(signal.id)) {
      deduped.set(signal.id, signal);
    }
  }
  return [...deduped.values()];
}

async function fetchRecentSignalEvents(): Promise<SignalEventPreviewRow[]> {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('signal_events')
    .select(SIGNAL_EVENT_PREVIEW_SELECT)
    .order('published_at', { ascending: false })
    .order('created_at', { ascending: false })
    .limit(ACCOUNT_SIGNAL_PREVIEW_SCAN_LIMIT);

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return (response.data || []) as SignalEventPreviewRow[];
}

const POLITICIAN_ACTIVITY_SELECT =
  'id,member_id,politician_name,chamber,ticker,asset_name,transaction_type,amount_range,published_date,transaction_date,source_url';
const INSIDER_ACTIVITY_SELECT =
  'id,ticker,filer_name,filer_relation,transaction_code,amount,price,value,published_date,transaction_date,source_url';
const FUND_ACTIVITY_SELECT =
  'id,fund_name,ticker,report_period,published_date,shares_held,value_held,qoq_change_shares,qoq_change_percent,source_url';

function followPreviewKey(kind: 'ticker' | 'actor', id: string) {
  return `${kind}:${id}`;
}

function accountAlertPreviewCacheKey(
  userId: string,
  tickers: AccountTickerFollow[],
  actors: AccountActorFollow[],
  clusterFeedEnabled: boolean,
) {
  const tickerKey = tickers
    .map((follow) => `${follow.id}:${follow.ticker}:${follow.alertMode}`)
    .sort()
    .join('|');
  const actorKey = actors
    .map((follow) => `${follow.id}:${follow.actorType}:${follow.actorKey}:${follow.alertMode}`)
    .sort()
    .join('|');
  return `${userId}::cluster=${Number(clusterFeedEnabled)}::tickers=${tickerKey}::actors=${actorKey}`;
}

function politicianMemberIdForFollow(follow: AccountActorFollow) {
  const metadataMemberId = typeof follow.metadata.member_id === 'string' ? follow.metadata.member_id.trim() : '';
  const actorKey = String(follow.actorKey || '').trim();
  const candidate = metadataMemberId || actorKey;
  return /^[a-z]\d{6}$/i.test(candidate) ? candidate.toUpperCase() : null;
}

async function fetchTickerRawActivity(follow: AccountTickerFollow): Promise<AccountMatchedSignal[]> {
  const ticker = String(follow.ticker || '').trim().toUpperCase();
  if (!ticker) {
    return [];
  }

  const supabase = getAdminSupabase();
  const [politicianResponse, insiderResponse, fundResponse] = await Promise.all([
    supabase
      .from('politician_trades')
      .select(POLITICIAN_ACTIVITY_SELECT)
      .eq('ticker', ticker)
      .order('published_date', { ascending: false })
      .order('transaction_date', { ascending: false })
      .limit(ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT),
    supabase
      .from('insider_trades')
      .select(INSIDER_ACTIVITY_SELECT)
      .eq('ticker', ticker)
      .order('published_date', { ascending: false })
      .order('transaction_date', { ascending: false })
      .limit(ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT),
    supabase
      .from('institutional_holdings')
      .select(FUND_ACTIVITY_SELECT)
      .eq('ticker', ticker)
      .order('published_date', { ascending: false })
      .order('report_period', { ascending: false })
      .limit(ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT),
  ]);

  if (politicianResponse.error) handleSupabaseError(politicianResponse.error);
  if (insiderResponse.error) handleSupabaseError(insiderResponse.error);
  if (fundResponse.error) handleSupabaseError(fundResponse.error);

  return dedupeMatchedSignals([
    ...((politicianResponse.data || []) as PoliticianActivityRow[]).map((row) => toPoliticianActivitySignal(row, ['ticker'])),
    ...((insiderResponse.data || []) as InsiderActivityRow[]).map((row) => toInsiderActivitySignal(row, ['ticker'])),
    ...((fundResponse.data || []) as FundActivityRow[]).map((row) => toFundActivitySignal(row, ['ticker'])),
  ]);
}

async function fetchActorRawActivity(follow: AccountActorFollow): Promise<AccountMatchedSignal[]> {
  const supabase = getAdminSupabase();

  if (follow.actorType === 'politician') {
    const memberId = politicianMemberIdForFollow(follow);
    const tokens = queryTokens(follow.actorName);
    if (!memberId && !tokens.length) {
      return [];
    }

    let query = supabase
      .from('politician_trades')
      .select(POLITICIAN_ACTIVITY_SELECT)
      .order('published_date', { ascending: false })
      .order('transaction_date', { ascending: false })
      .limit(ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT);

    if (memberId) {
      query = query.eq('member_id', memberId);
    } else {
      for (const token of tokens) {
        query = query.ilike('politician_name', `%${token}%`);
      }
    }

    const response = await query;
    if (response.error) {
      handleSupabaseError(response.error);
    }
    return dedupeMatchedSignals(
      ((response.data || []) as PoliticianActivityRow[]).map((row) => toPoliticianActivitySignal(row, ['politician'])),
    );
  }

  if (follow.actorType === 'fund') {
    const tokens = queryTokens(follow.actorName);
    if (!tokens.length) {
      return [];
    }

    let query = supabase
      .from('signal_events')
      .select(SIGNAL_EVENT_PREVIEW_SELECT)
      .eq('actor_type', 'fund')
      .in('signal_type', Array.from(FUND_FILING_SIGNAL_TYPES))
      .order('published_at', { ascending: false })
      .order('created_at', { ascending: false })
      .limit(ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT);

    for (const token of tokens) {
      query = query.ilike('actor_name', `%${token}%`);
    }

    const response = await query;
    if (response.error) {
      handleSupabaseError(response.error);
    }
    return dedupeMatchedSignals(
      ((response.data || []) as SignalEventPreviewRow[]).map((row) => toMatchedSignal(row, ['fund'])),
    );
  }

  const tokens = queryTokens(follow.actorName);
  if (!tokens.length) {
    return [];
  }

  let query = supabase
    .from('insider_trades')
    .select(INSIDER_ACTIVITY_SELECT)
    .order('published_date', { ascending: false })
    .order('transaction_date', { ascending: false })
    .limit(ACCOUNT_ACTIVITY_PER_FOLLOW_SOURCE_LIMIT);

  for (const token of tokens) {
    query = query.ilike('filer_name', `%${token}%`);
  }

  const response = await query;
  if (response.error) {
    handleSupabaseError(response.error);
  }
  return dedupeMatchedSignals(
    ((response.data || []) as InsiderActivityRow[]).map((row) => toInsiderActivitySignal(row, ['insider'])),
  );
}

async function fetchRawActivityByFollow(
  tickers: AccountTickerFollow[],
  actors: AccountActorFollow[],
): Promise<Map<string, AccountMatchedSignal[]>> {
  const entries = await Promise.all([
    ...tickers.map(async (follow) => {
      const key = followPreviewKey('ticker', follow.id);
      return [key, await fetchTickerRawActivity(follow).catch(() => [])] as const;
    }),
    ...actors.map(async (follow) => {
      const key = followPreviewKey('actor', follow.id);
      return [key, await fetchActorRawActivity(follow).catch(() => [])] as const;
    }),
  ]);

  return new Map(entries);
}

async function buildAccountAlertPreview(
  tickers: AccountTickerFollow[],
  actors: AccountActorFollow[],
  clusterFeedEnabled = false,
): Promise<AccountAlertPreview> {
  const [events, rawActivityByFollow] = await Promise.all([
    fetchRecentSignalEvents().catch(() => []),
    fetchRawActivityByFollow(tickers, actors),
  ]);
  const timelineById = new Map<string, AccountMatchedSignal>();
  if (clusterFeedEnabled) {
    for (const event of events.filter(isClusterAlertPreviewSignal)) {
      const signal = toMatchedSignal(event, ['cluster-feed']);
      timelineById.set(signal.id, signal);
    }
  }

  const tickerPreviews: AccountFollowSignalPreview[] = tickers.map((follow) => {
    const eventMatches = events
      .map((event) => {
        const reasons = eventMatchesTickerFollow(event, follow);
        return reasons ? toMatchedSignal(event, reasons) : null;
      })
      .filter((signal): signal is AccountMatchedSignal => Boolean(signal));
    const matched = dedupeMatchedSignals([
      ...eventMatches,
      ...(rawActivityByFollow.get(followPreviewKey('ticker', follow.id)) || []),
    ]);

    for (const signal of matched) {
      timelineById.set(signal.id, signal);
    }

    const clusterSignals = matched.filter((signal) => signal.isCluster);
    return {
      followKind: 'ticker',
      followId: follow.id,
      followLabel: follow.ticker,
      followType: 'stock',
      alertMode: follow.alertMode,
      matchedCount: matched.length,
      latestMatchedAt: latestPreviewDate(matched),
      recentSignals: matched.filter((signal) => !signal.isCluster).slice(0, ACCOUNT_SIGNAL_PREVIEW_PER_FOLLOW_LIMIT),
      clusterSignals: clusterSignals.slice(0, ACCOUNT_SIGNAL_PREVIEW_PER_FOLLOW_LIMIT),
    };
  });

  const actorPreviews: AccountFollowSignalPreview[] = actors.map((follow) => {
    const eventMatches = events
      .map((event) => {
        const reasons = eventMatchesActorFollow(event, follow);
        return reasons ? toMatchedSignal(event, reasons) : null;
      })
      .filter((signal): signal is AccountMatchedSignal => Boolean(signal));
    const matched = dedupeMatchedSignals([
      ...eventMatches,
      ...(rawActivityByFollow.get(followPreviewKey('actor', follow.id)) || []),
    ]);

    for (const signal of matched) {
      timelineById.set(signal.id, signal);
    }

    const clusterSignals = matched.filter((signal) => signal.isCluster);
    return {
      followKind: 'actor',
      followId: follow.id,
      followLabel: follow.actorName,
      followType: follow.actorType,
      alertMode: follow.alertMode,
      matchedCount: matched.length,
      latestMatchedAt: latestPreviewDate(matched),
      recentSignals: matched.filter((signal) => !signal.isCluster).slice(0, ACCOUNT_SIGNAL_PREVIEW_PER_FOLLOW_LIMIT),
      clusterSignals: clusterSignals.slice(0, ACCOUNT_SIGNAL_PREVIEW_PER_FOLLOW_LIMIT),
    };
  });

  const followPreviews = [...tickerPreviews, ...actorPreviews].sort((left, right) => {
    const leftDate = left.latestMatchedAt || '';
    const rightDate = right.latestMatchedAt || '';
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }
    return right.matchedCount - left.matchedCount;
  });
  const allMatchedSignals = sortMatchedSignals([...timelineById.values()]);
  const timeline = allMatchedSignals.slice(0, ACCOUNT_SIGNAL_PREVIEW_TIMELINE_LIMIT);

  return {
    scanLimit: ACCOUNT_SIGNAL_PREVIEW_SCAN_LIMIT,
    matchedSignalCount: timelineById.size,
    clusterSignalCount: allMatchedSignals.filter((signal) => signal.isCluster).length,
    followPreviews,
    timeline,
  };
}

function emptyAccountAlertPreview(): AccountAlertPreview {
  return {
    scanLimit: ACCOUNT_SIGNAL_PREVIEW_SCAN_LIMIT,
    matchedSignalCount: 0,
    clusterSignalCount: 0,
    followPreviews: [],
    timeline: [],
  };
}

function toAccountProfile(profile: ProfileRow): AccountProfileState {
  return {
    id: profile.id,
    email: profile.email,
    displayName: profile.display_name,
    alertEmail: profile.alert_email,
    textPhone: null,
    emailEnabled: Boolean(profile.email_enabled),
    textEnabled: false,
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

export async function getAccountAlertPreview(user: User): Promise<AccountAlertPreview> {
  const watchlist = await ensureUserWatchlist(user.id);
  const [tickers, actors, clusterAlerts] = await Promise.all([
    fetchWatchlistTickers(watchlist.id),
    fetchWatchlistActors(watchlist.id),
    fetchClusterAlertPreference(user.id),
  ]);

  const cacheKey = accountAlertPreviewCacheKey(user.id, tickers, actors, clusterAlerts.enabled);
  const cached = accountAlertPreviewCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.preview;
  }

  const preview = await buildAccountAlertPreview(tickers, actors, clusterAlerts.enabled);
  accountAlertPreviewCache.set(cacheKey, {
    expiresAt: Date.now() + ACCOUNT_ALERT_PREVIEW_CACHE_TTL_MS,
    preview,
  });
  return preview;
}

export async function getAccountAlertHistory(user: User): Promise<AccountAlertHistoryItem[]> {
  const watchlist = await ensureUserWatchlist(user.id);
  const subscriptionRows = await fetchSubscriptions(watchlist.id);
  return fetchAlertHistory(subscriptionRows.map((row) => String(row.id)));
}

export async function getAccountState(user: User, options: AccountStateOptions = {}): Promise<AccountState> {
  const includeHistory = options.includeHistory ?? true;
  const includeAlertPreview = options.includeAlertPreview ?? true;
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const [tickers, actors, subscriptionRows, clusterAlerts] = await Promise.all([
    fetchWatchlistTickers(watchlist.id),
    fetchWatchlistActors(watchlist.id),
    fetchSubscriptions(watchlist.id),
    fetchClusterAlertPreference(user.id),
  ]);

  const subscriptionIds = subscriptionRows.map((row) => String(row.id));
  const [history, alertPreview] = await Promise.all([
    includeHistory ? fetchAlertHistory(subscriptionIds) : Promise.resolve([]),
    includeAlertPreview ? buildAccountAlertPreview(tickers, actors, clusterAlerts.enabled) : Promise.resolve(emptyAccountAlertPreview()),
  ]);

  const emailState = pickChannelState(subscriptionRows, 'email', profile.alert_email || user.email || null, profile.email_enabled);
  const smsState = pickChannelState(subscriptionRows, 'sms', null, false);
  const accountProfile = {
    ...toAccountProfile(profile),
    textPhone: smsState.destination,
    textEnabled: smsState.active,
  };

  return {
    user: {
      id: user.id,
      email: user.email || null,
    },
    profile: accountProfile,
    billing: toAccountBilling(profile),
    watchlist: {
      id: watchlist.id,
      name: watchlist.name,
    },
    followCount: tickers.length + actors.length + Number(clusterAlerts.enabled),
    followLimit: profile.follow_limit || DEFAULT_FOLLOW_LIMIT,
    subscriptions: {
      email: emailState,
      sms: smsState,
    },
    follows: {
      tickers,
      actors,
      cluster: clusterAlerts.enabled
        ? {
            id: 'cluster-feed',
            label: 'Clusters',
            channels: clusterAlerts.channels,
          }
        : null,
    },
    history,
    alertPreview,
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

async function politicianSuggestionScore(member: CongressMemberRecord, query: string) {
  const fullName = `${member.first_name || ''} ${member.last_name || ''}`.trim();
  const normalizedFullName = fullName.toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  const tokens = queryTokens(normalizedQuery);
  const politicianAliases = await getNotablePoliticianAliases();
  const aliasEntry = politicianAliases.find((entry) => entry.actorKey === String(member.id).toLowerCase());

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

async function insiderSuggestionScore(actorName: string, query: string) {
  const normalizedName = actorName.trim().toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  const tokens = queryTokens(normalizedQuery);
  const actorTokens = queryTokens(normalizedName);
  const actorKey = normalizeActorKey(actorName);
  const insiderAliases = await getNotableInsiderAliases();
  const aliasEntry = insiderAliases.find((entry) => entry.actorKey === actorKey);
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

  const tickerAliases = await getTickerSearchAliases();
  for (const alias of tickerAliases) {
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

    const aliasMap = await getTickerAliasByTicker();
    const alias = aliasMap.get(ticker);
    const companyName = await cleanCompanyDisplayName(ticker, row.name ? String(row.name) : null);
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

  const aliasMapFinal = await getTickerAliasByTicker();
  const canonicalAliasMatches = ranked.filter((candidate) => aliasMapFinal.has(candidate.ticker));
  const filtered = ranked.filter((candidate) => {
    if (aliasMapFinal.has(candidate.ticker)) {
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
  const notableInsiders = await getNotableInsiderAliases();
  const matchingAliases = notableInsiders.filter((entry) => aliasMatchScore(entry.aliases, normalizedQuery) > 0);
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
    const aliasMap = await getTickerAliasByTicker();
    if (aliasMap.has(normalizedTicker)) {
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
  const scoredEntries = await Promise.all(
    members
      .filter((member) => !String(member.id || '').startsWith('unknown-'))
      .map(async (member) => ({
        member,
        score: await politicianSuggestionScore(member, trimmedQuery),
      }))
  );
  const ranked = scoredEntries
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

  const insiderAliases = await getNotableInsiderAliases();
  const scoredInsiders = await Promise.all(
    [...deduped.entries()].map(async ([actorKey, row]) => ({
      actorName: row.actor_name,
      actorKey,
      subtitle: row.ticker ? `Recent trade in ${row.ticker}` : null,
      score: await insiderSuggestionScore(row.actor_name, trimmedQuery),
      recency: row.created_at || '',
      exactMatch: normalizeSearchText(row.actor_name) === normalizedQuery,
      strongMatch:
        normalizeSearchText(row.actor_name).startsWith(normalizedQuery) ||
        Boolean(
          insiderAliases.find((entry) => entry.actorKey === actorKey)?.aliases.some(
            (alias) => normalizeSearchText(alias) === normalizedQuery || normalizeSearchText(alias).startsWith(normalizedQuery)
          )
        ),
    }))
  );

  return scoredInsiders
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

export async function searchFundSuggestions(query: string): Promise<AccountFollowSuggestion[]> {
  const trimmedQuery = query.trim();
  if (trimmedQuery.length < 2) {
    return [];
  }

  const tokens = queryTokens(trimmedQuery);
  const supabase = getAdminSupabase();
  let request = supabase
    .from('institutional_holdings')
    .select('fund_name,report_period,published_date')
    .order('published_date', { ascending: false })
    .order('report_period', { ascending: false })
    .limit(120);

  for (const token of tokens.length ? tokens : [trimmedQuery]) {
    request = request.ilike('fund_name', `%${token}%`);
  }

  const response = await request;
  if (response.error) {
    handleSupabaseError(response.error);
  }

  const deduped = new Map<string, AccountFollowSuggestion>();
  for (const row of (response.data || []) as Array<{ fund_name: string | null; report_period: string | null; published_date: string | null }>) {
    const fundName = String(row.fund_name || '').trim();
    const actorKey = normalizeActorKey(fundName);
    if (!fundName || !actorKey || deduped.has(actorKey)) {
      continue;
    }

    deduped.set(actorKey, {
      actorType: 'fund',
      actorName: fundName,
      actorKey,
      subtitle: row.report_period ? `Latest ${row.report_period}` : row.published_date ? `Filed ${row.published_date}` : '13F fund',
    });
  }

  return [...deduped.values()].slice(0, 8);
}

async function countCurrentFollows(watchlistId: string) {
  const [tickers, actors] = await Promise.all([fetchWatchlistTickers(watchlistId), fetchWatchlistActors(watchlistId)]);
  return tickers.length + actors.length;
}

function assertCanAddFollow(count: number, limit: number) {
  if (count >= limit) {
    throw new Error(`${Math.min(count, limit)}/${limit} free follows used.`);
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
    throw new Error('Enter a name.');
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
  } else if (actorType === 'fund') {
    actorKey = normalizeActorKey(preferredActorKey || trimmedName);
    if (!actorKey) {
      throw new Error('Enter a fund name.');
    }
    resolvedName = trimmedName;
    metadata = {
      resolved_from: trimmedName,
      fund_name: resolvedName,
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
  channel: 'email' | 'sms',
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

export async function updateSmsDelivery(user: User, phoneNumber: string | null, enabled: boolean) {
  const { watchlist } = await ensureUserWorkspace(user);
  const normalizedPhone = normalizePhoneNumber(phoneNumber || '');

  if (enabled && !normalizedPhone) {
    throw new Error('Add a phone number before enabling text alerts.');
  }

  await setSubscriptionState(watchlist.id, 'sms', normalizedPhone || null, enabled);
}

function normalizeClusterAlertChannels(value: unknown): ClusterAlertChannel[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return [...new Set(value.map((channel) => String(channel).trim().toLowerCase()).filter((channel) => VALID_CLUSTER_ALERT_CHANNELS.has(channel as ClusterAlertChannel)))] as ClusterAlertChannel[];
}

async function fetchClusterAlertPreference(userId: string) {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('profiles')
    .select('cluster_alerts_enabled,cluster_alert_channels')
    .eq('id', userId)
    .maybeSingle();

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return {
    enabled: Boolean(response.data?.cluster_alerts_enabled),
    channels: normalizeClusterAlertChannels(response.data?.cluster_alert_channels),
  };
}

function availableClusterAlertChannels(subscriptions: SubscriptionRow[]) {
  return subscriptions
    .filter((subscription) => subscription.active && Boolean(subscription.destination?.trim()))
    .map((subscription) => subscription.channel)
    .filter((channel): channel is ClusterAlertChannel => VALID_CLUSTER_ALERT_CHANNELS.has(channel as ClusterAlertChannel));
}

export async function getClusterAlertsState(user: User) {
  const { watchlist } = await ensureUserWorkspace(user);
  const [preference, subscriptions] = await Promise.all([
    fetchClusterAlertPreference(user.id),
    fetchSubscriptions(watchlist.id),
  ]);
  const availableChannels = availableClusterAlertChannels(subscriptions);

  return {
    enabled: preference.enabled,
    channels: preference.channels.length ? preference.channels : availableChannels.slice(0, 1),
    availableChannels,
    deliveryReady: availableChannels.length > 0,
  };
}

export async function updateClusterAlerts(user: User, enabled: boolean, channels: ClusterAlertChannel[] = []) {
  await ensureUserWorkspace(user);
  const state = await getClusterAlertsState(user);
  const normalizedChannels = normalizeClusterAlertChannels(channels);
  const preferredChannels = normalizedChannels.length ? normalizedChannels : state.channels;
  const selectedChannels = preferredChannels.length ? preferredChannels : (['email'] as ClusterAlertChannel[]);

  const supabase = getAdminSupabase();
  const response = await supabase
    .from('profiles')
    .update({
      cluster_alerts_enabled: enabled,
      cluster_alert_channels: selectedChannels,
    })
    .eq('id', user.id);

  if (response.error) {
    handleSupabaseError(response.error);
  }

  return {
    ...state,
    enabled,
    channels: selectedChannels,
  };
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

  const sms = [
    'Vail test alert',
    '',
    'Your private alert delivery is working.',
    'Live signals for your follows will arrive here when something matches.',
  ].join('\n');

  return { subject, summary, title, text, html, sms };
}

export async function sendAccountTestAlert(user: User) {
  const { profile, watchlist } = await ensureUserWorkspace(user);
  const subscriptionRows = await fetchSubscriptions(watchlist.id);
  const emailSubscription = pickChannelSubscription(subscriptionRows, 'email');
  const smsSubscription = pickChannelSubscription(subscriptionRows, 'sms');
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

  if (smsSubscription?.active && smsSubscription.destination) {
    await sendSmsMessage(smsSubscription.destination, copy.sms);
    sentChannels.push('text');
  } else {
    skippedChannels.push('text');
  }

  if (!sentChannels.length) {
    throw new Error('Enable email or text first so Vail has a destination for the test alert.');
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
      source_url: '/account',
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
  if (sentChannels.includes('text') && smsSubscription) {
    deliveries.push({
      signal_event_id: signalInsert.data.id,
      subscription_id: smsSubscription.id,
      delivery_key: `test:${sourceDocumentId}:${smsSubscription.id}`,
      channel: 'sms',
      destination: smsSubscription.destination,
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

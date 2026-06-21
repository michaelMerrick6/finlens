import 'server-only';

import { getAdminSupabase } from '@/lib/supabase-admin';

export type TweetCandidatePayload = {
  signal_type?: string | null;
  broadcast_category?: string | null;
  broadcast_channel?: string | null;
  filer_relation?: string | null;
  actor_name?: string | null;
  ticker?: string | null;
  direction?: string | null;
  amount_range?: string | null;
  cluster_event_ids?: string[] | null;
  cluster_actor_count?: number | null;
  cluster_window_days?: number | null;
  cluster_combined_lower_bound?: number | null;
  cluster_total_value?: number | null;
  cluster_amount_ranges?: string[] | null;
  cluster_actors?:
    | {
        name?: string | null;
        relation?: string | null;
        amount_range?: string | null;
        source?: string | null;
      }[]
    | null;
  congress_actor_count?: number | null;
  insider_actor_count?: number | null;
  fund_actor_count?: number | null;
  committee_roles?: { name?: string | null }[] | null;
  themes?: string[] | null;
  insider_total_buy_value?: number | null;
  insider_total_sell_value?: number | null;
  insider_change_value?: number | null;
  insider_change_pct?: number | null;
  insider_holding_reduction_pct?: number | null;
  insider_holding_increase_pct?: number | null;
  insider_new_position_after_buy?: boolean | null;
  group_event_ids?: string[] | null;
  group_row_count?: number | null;
  group_combined_lower_bound?: number | null;
  group_amount_ranges?: string[] | null;
  group_trade_date_start?: string | null;
  group_trade_date_end?: string | null;
  gain_return_pct?: number | null;
  gain_milestone_pct?: number | null;
  entry_price?: number | null;
  current_price?: number | null;
  holding_days?: number | null;
  days_since_cluster?: number | null;
  trade_date?: string | null;
  cluster_clocked_at?: string | null;
  original_filed_at?: string | null;
  price_as_of?: string | null;
  estimated_gain_lower_bound?: number | null;
  estimated_current_lower_bound?: number | null;
  [key: string]: unknown;
};

export type TweetCandidateSignalEvent = {
  ticker?: string | null;
  actor_name?: string | null;
  signal_type?: string | null;
  source_url?: string | null;
  occurred_at?: string | null;
  published_at?: string | null;
};

export type TweetCandidateRow = {
  id: string;
  candidate_key: string;
  channel: string;
  status: string;
  rule_key: string;
  score: number | null;
  title: string;
  draft_text: string;
  rationale: string | null;
  payload?: TweetCandidatePayload | null;
  created_at: string;
  reviewed_at: string | null;
  posted_at: string | null;
  review_notes: string | null;
  external_post_id?: string | null;
  signal_event_id: string;
  signal_events?: TweetCandidateSignalEvent | null;
};

export type BroadcastStoryChannel = {
  id: string;
  channel: string;
  status: string;
  title: string;
  draftText: string;
  reviewNotes: string | null;
  reviewedAt: string | null;
  postedAt: string | null;
  externalPostId: string | null;
  score: number;
};

export type BroadcastStoryChannelKey = 'twitter' | 'discord_premium';
export type BroadcastStoryStatus = 'pending_review' | 'approved' | 'posted' | 'rejected';

export const PUBLIC_BROADCAST_STORY_STATUSES: BroadcastStoryStatus[] = [
  'pending_review',
  'approved',
  'posted',
];

export type BroadcastStory = {
  candidateKey: string;
  ruleKey: string;
  category: string;
  title: string;
  rationale: string | null;
  score: number;
  ticker: string | null;
  actorName: string | null;
  signalType: string | null;
  direction: string | null;
  actorCount: number;
  amountFloor: number;
  amountLabel: string | null;
  amountRanges: string[];
  clusterWindowDays: number | null;
  tradeDateStart: string | null;
  tradeDateEnd: string | null;
  sourceMix: {
    congress: number;
    insiders: number;
    funds: number;
  };
  actorLabels: string[];
  committees: string[];
  themes: string[];
  latestPublishedAt: string | null;
  sourceUrl: string | null;
  supportingEventIds: string[];
  createdAt: string;
  reviewedAt: string | null;
  postedAt: string | null;
  reviewNotes: string | null;
  channels: Partial<Record<BroadcastStoryChannelKey, BroadcastStoryChannel>>;
};

const RULE_CATEGORY_BY_KEY: Record<string, string> = {
  committee_relevance_buy: 'politicians',
  congress_cluster: 'clusters',
  cross_source_accumulation: 'clusters',
  cluster_gain_milestone: 'updates',
  crypto_politician_sell: 'politicians',
  first_quantum_politician_buy: 'politicians',
  grouped_congress_buy: 'politicians',
  grouped_insider_buy: 'insiders',
  insider_cluster: 'clusters',
  large_politician_buy: 'politicians',
  meaningful_insider_change: 'insiders',
  notable_politician_filing: 'politicians',
  notable_politician_trade: 'politicians',
  politician_gain_milestone: 'updates',
  substantial_insider_buy: 'insiders',
  substantial_insider_sell: 'insiders',
  theme_politician_buy: 'politicians',
};

type FetchStoryOptions = {
  status: string | string[];
  sinceDate: string | null;
  storyLimit: number;
  category?: string | null;
  queryText?: string | null;
  sort?: string | null;
};

type FetchStoryByKeyOptions = {
  statuses?: string[] | null;
};

function trim(value: unknown) {
  return String(value ?? '').trim();
}

function toNumber(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function parseAmountLowerBound(value: string | null | undefined) {
  const raw = trim(value);
  if (!raw) {
    return 0;
  }
  const normalized = raw.replace(/,/g, '');
  const directMatch = normalized.match(/\$([\d.]+)/);
  if (directMatch) {
    return Number(directMatch[1]) || 0;
  }
  return 0;
}

function moneyLabel(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  return `$${Math.round(value).toLocaleString()}+`;
}

function exactMoneyLabel(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  return `$${Math.round(value).toLocaleString()}`;
}

function maxIso(left: string | null | undefined, right: string | null | undefined) {
  if (!left) {
    return right || null;
  }
  if (!right) {
    return left;
  }
  return left > right ? left : right;
}

function minIso(left: string | null | undefined, right: string | null | undefined) {
  if (!left) {
    return right || null;
  }
  if (!right) {
    return left;
  }
  return left < right ? left : right;
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(
    new Set(
      values
        .map((value) => trim(value))
        .filter(Boolean)
    )
  );
}

function rangeSummary(ranges: string[]) {
  const uniqueRanges = uniqueStrings(ranges);
  if (!uniqueRanges.length) {
    return null;
  }
  if (uniqueRanges.length <= 3) {
    return uniqueRanges.join(', ');
  }
  return `${uniqueRanges.slice(0, 3).join(', ')}, +${uniqueRanges.length - 3} more`;
}

function shortDate(value: string | null | undefined) {
  const raw = trim(value).slice(0, 10);
  if (!raw) {
    return null;
  }
  const date = new Date(`${raw}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return raw;
  }
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  });
}

function dateSpanLabel(start: string | null | undefined, end: string | null | undefined) {
  const startLabel = shortDate(start);
  const endLabel = shortDate(end);
  if (startLabel && endLabel) {
    return startLabel === endLabel ? startLabel : `${startLabel} to ${endLabel}`;
  }
  return startLabel || endLabel || null;
}

function appendMissingDraftLines(existingDraft: string, additions: string[]) {
  const existing = trim(existingDraft);
  const lines = existing ? existing.split('\n') : [];
  const prefixes = new Set(
    lines
      .map((line) => trim(line))
      .filter(Boolean)
      .map((line) => {
        const separator = line.indexOf(':');
        return separator > 0 ? line.slice(0, separator + 1).toLowerCase() : line.toLowerCase();
      })
  );

  for (const addition of additions.map((line) => trim(line)).filter(Boolean)) {
    const separator = addition.indexOf(':');
    const prefix = separator > 0 ? addition.slice(0, separator + 1).toLowerCase() : addition.toLowerCase();
    if (prefixes.has(prefix) || lines.includes(addition)) {
      continue;
    }
    lines.push(addition);
    prefixes.add(prefix);
  }

  return lines.join('\n');
}

function candidateCategory(row: TweetCandidateRow) {
  const payloadCategory = trim(row.payload?.broadcast_category);
  if (payloadCategory) {
    return payloadCategory;
  }
  return RULE_CATEGORY_BY_KEY[row.rule_key] || 'all';
}

function candidateActorLabels(row: TweetCandidateRow) {
  const actorRows = row.payload?.cluster_actors || [];
  const clusterLabels = actorRows
    .map((actor) => {
      const name = trim(actor?.name);
      if (!name) {
        return null;
      }
      const relation = trim(actor?.relation);
      const amountRange = trim(actor?.amount_range);
      if (amountRange) {
        return `${name} (${amountRange})`;
      }
      if (relation && relation.toLowerCase() !== 'insider') {
        return `${name} (${relation})`;
      }
      return name;
    })
    .filter((value): value is string => Boolean(value));
  if (clusterLabels.length) {
    return uniqueStrings(clusterLabels);
  }

  const actorName = trim(row.payload?.actor_name || row.signal_events?.actor_name);
  const relation = trim(row.payload?.filer_relation);
  if (!actorName) {
    return [];
  }
  if (relation && relation.toLowerCase() !== 'insider') {
    return [`${actorName} (${relation})`];
  }
  return [actorName];
}

function candidateCommittees(row: TweetCandidateRow) {
  return uniqueStrings((row.payload?.committee_roles || []).map((role) => trim(role?.name)));
}

function candidateThemes(row: TweetCandidateRow) {
  return uniqueStrings((row.payload?.themes || []).map((theme) => trim(theme)));
}

function sourceMixForCandidate(row: TweetCandidateRow, actorCount: number) {
  const congress = Math.max(0, toNumber(row.payload?.congress_actor_count));
  const insiders = Math.max(0, toNumber(row.payload?.insider_actor_count));
  const funds = Math.max(0, toNumber(row.payload?.fund_actor_count));

  if (congress || insiders || funds) {
    return { congress, insiders, funds };
  }

  const category = candidateCategory(row);
  if (category === 'politicians') {
    return { congress: actorCount || 1, insiders: 0, funds: 0 };
  }
  if (category === 'insiders') {
    return { congress: 0, insiders: actorCount || 1, funds: 0 };
  }
  if (row.rule_key === 'insider_cluster') {
    return { congress: 0, insiders: actorCount || 1, funds: 0 };
  }
  if (row.rule_key === 'congress_cluster') {
    return { congress: actorCount || 1, insiders: 0, funds: 0 };
  }
  return { congress: 0, insiders: 0, funds: 0 };
}

function amountFloorForCandidate(row: TweetCandidateRow) {
  const payload = row.payload || {};
  return Math.max(
    toNumber(payload.cluster_combined_lower_bound),
    toNumber(payload.cluster_total_value),
    toNumber(payload.group_combined_lower_bound),
    toNumber(payload.estimated_gain_lower_bound),
    toNumber(payload.estimated_current_lower_bound),
    toNumber(payload.insider_total_buy_value),
    toNumber(payload.insider_total_sell_value),
    toNumber(payload.insider_change_value),
    parseAmountLowerBound(trim(payload.amount_range))
  );
}

function amountRangesForCandidate(row: TweetCandidateRow) {
  const payloadRanges = uniqueStrings([
    ...((row.payload?.cluster_amount_ranges || []) as (string | null | undefined)[]),
    ...((row.payload?.group_amount_ranges || []) as (string | null | undefined)[]),
    trim(row.payload?.amount_range as string | null | undefined),
  ]);
  if (payloadRanges.length) {
    return payloadRanges;
  }
  return uniqueStrings((row.payload?.cluster_actors || []).map((actor) => trim(actor?.amount_range)));
}

function nestedEventIds(value: unknown) {
  if (!Array.isArray(value)) {
    return [] as string[];
  }
  return value.map((item) => trim(item)).filter(Boolean);
}

type SupportingSignalEventRow = {
  id: string;
  payload?: TweetCandidatePayload | null;
};

function normalizedMoneyComponent(value: unknown) {
  const numeric = toNumber(value);
  if (!numeric) {
    return '';
  }
  return numeric.toFixed(4).replace(/\.?0+$/, '');
}

function insiderEconomicTransactionKey(row: SupportingSignalEventRow) {
  const payload = row.payload || {};
  const sourceUrl = trim(payload.source_url);
  const hasInsiderFilingShape = Boolean(sourceUrl.includes('sec.gov') || trim(payload.transaction_code));
  if (!hasInsiderFilingShape) {
    return row.id;
  }

  const ticker = trim(payload.ticker).toUpperCase();
  const direction = trim(payload.transaction_type || payload.transaction_code).toLowerCase();
  const transactionDate = trim(payload.transaction_date || payload.occurred_at).slice(0, 10);
  const amount = normalizedMoneyComponent(payload.amount);
  const price = normalizedMoneyComponent(payload.price);
  const value = normalizedMoneyComponent(payload.value);

  if (!ticker || !direction || !transactionDate || (!amount && !value)) {
    return row.id;
  }

  return ['insider-economic', ticker, direction, transactionDate, amount, price, value].join('::');
}

function uniqueEconomicLeafRows(rows: SupportingSignalEventRow[]) {
  const rowsByEconomicKey = new Map<string, SupportingSignalEventRow>();
  for (const row of rows) {
    const key = insiderEconomicTransactionKey(row);
    if (!rowsByEconomicKey.has(key)) {
      rowsByEconomicKey.set(key, row);
    }
  }
  return [...rowsByEconomicKey.values()];
}

function eventAmountFloor(row: SupportingSignalEventRow) {
  const payload = row.payload || {};
  return Math.max(
    parseAmountLowerBound(trim(payload.amount_range)),
    toNumber(payload.cluster_combined_lower_bound),
    toNumber(payload.cluster_total_value),
    toNumber(payload.group_combined_lower_bound),
    toNumber(payload.estimated_gain_lower_bound),
    toNumber(payload.estimated_current_lower_bound),
    toNumber(payload.insider_total_buy_value),
    toNumber(payload.insider_total_sell_value),
    toNumber(payload.insider_change_value),
    toNumber(payload.value),
    toNumber(payload.amount)
  );
}

function supportingEventIdsForCandidate(row: TweetCandidateRow) {
  const clusterEventIds = uniqueStrings((row.payload?.cluster_event_ids || []) as (string | null | undefined)[]);
  if (clusterEventIds.length) {
    return clusterEventIds;
  }

  const groupEventIds = uniqueStrings((row.payload?.group_event_ids || []) as (string | null | undefined)[]);
  if (groupEventIds.length) {
    return groupEventIds;
  }

  return uniqueStrings([row.signal_event_id]);
}

function hydratedDraftText(row: TweetCandidateRow) {
  const payload = row.payload || {};
  const draft = trim(row.draft_text);
  const amountRanges = amountRangesForCandidate(row);
  const amountRangeLabel = rangeSummary(amountRanges);
  const amountFloor = amountFloorForCandidate(row);
  const filedLabel = shortDate(row.signal_events?.published_at || row.created_at);
  const tradeSpan = dateSpanLabel(payload.group_trade_date_start, payload.group_trade_date_end);

  if (row.rule_key === 'congress_cluster') {
    return appendMissingDraftLines(draft, [
      amountRangeLabel ? `Disclosed ranges: ${amountRangeLabel}` : '',
      amountFloor > 0 ? `Combined floor: ${moneyLabel(amountFloor)}` : '',
      filedLabel ? `Latest filing: ${filedLabel}` : '',
    ]);
  }

  if (row.rule_key === 'cross_source_accumulation') {
    const congressCount = Math.max(0, toNumber(payload.congress_actor_count));
    const insiderCount = Math.max(0, toNumber(payload.insider_actor_count));
    const fundCount = Math.max(0, toNumber(payload.fund_actor_count));
    const includesFund = fundCount > 0;
    const congressFloor = moneyLabel(
      (payload.cluster_actors || [])
        .filter((actor) => trim(actor?.source).toLowerCase() === 'congress')
        .reduce((total, actor) => total + parseAmountLowerBound(trim(actor?.amount_range)), 0)
    );
    return appendMissingDraftLines(draft, [
      amountRangeLabel ? `Congress ranges: ${amountRangeLabel}` : '',
      congressFloor ? `Congress floor: ${congressFloor}` : '',
      amountFloor > 0 ? `Tracked floor: ${moneyLabel(amountFloor)}` : '',
      `Counts: Congress ${congressCount}, insiders ${insiderCount}${includesFund ? `, funds ${fundCount}` : ''}`,
      filedLabel ? `Latest filing: ${filedLabel}` : '',
    ]);
  }

  if (row.rule_key === 'grouped_congress_buy') {
    return appendMissingDraftLines(draft, [
      amountRangeLabel ? `Disclosed ranges: ${amountRangeLabel}` : '',
      amountFloor > 0 ? `Combined floor: ${moneyLabel(amountFloor)}` : '',
      tradeSpan ? `Trade dates: ${tradeSpan}` : '',
      filedLabel ? `Filed: ${filedLabel}` : '',
    ]);
  }

  if (row.rule_key === 'grouped_insider_buy') {
    return appendMissingDraftLines(draft, [
      amountFloor > 0 ? `Combined value: ${exactMoneyLabel(amountFloor)}` : '',
      tradeSpan ? `Trade dates: ${tradeSpan}` : '',
      filedLabel ? `Filed: ${filedLabel}` : '',
    ]);
  }

  if (row.rule_key === 'politician_gain_milestone') {
    const returnPct = toNumber(payload.gain_return_pct);
    const milestonePct = toNumber(payload.gain_milestone_pct);
    const entryPrice = toNumber(payload.entry_price);
    const currentPrice = toNumber(payload.current_price);
    const holdingDays = toNumber(payload.holding_days);
    const tradeDate = shortDate(payload.trade_date);
    const priceAsOf = shortDate(payload.price_as_of || row.signal_events?.published_at);
    return appendMissingDraftLines(draft, [
      amountRangeLabel ? `Disclosed range: ${amountRangeLabel}` : '',
      returnPct > 0 ? `Return: ${returnPct.toFixed(returnPct % 1 ? 1 : 0)}%` : '',
      milestonePct > 0 ? `Milestone: ${Math.round(milestonePct)}%` : '',
      entryPrice > 0 && currentPrice > 0 ? `Entry / current: ${exactMoneyLabel(entryPrice)} -> ${exactMoneyLabel(currentPrice)}` : '',
      amountFloor > 0 ? `Estimated gain floor: ${moneyLabel(amountFloor)}` : '',
      holdingDays > 0 ? `Window: ${Math.round(holdingDays)} days` : '',
      tradeDate ? `Trade date: ${tradeDate}` : '',
      priceAsOf ? `Price as of: ${priceAsOf}` : '',
    ]);
  }

  if (row.rule_key === 'cluster_gain_milestone') {
    const returnPct = toNumber(payload.gain_return_pct);
    const milestonePct = toNumber(payload.gain_milestone_pct);
    const entryPrice = toNumber(payload.entry_price);
    const currentPrice = toNumber(payload.current_price);
    const daysSinceCluster = toNumber(payload.days_since_cluster);
    const clusterDate = shortDate(payload.cluster_clocked_at);
    const priceAsOf = shortDate(payload.price_as_of || row.signal_events?.published_at);
    const clusterFloor = toNumber(payload.cluster_combined_lower_bound);
    return appendMissingDraftLines(draft, [
      returnPct > 0 ? `Return: ${returnPct.toFixed(returnPct % 1 ? 1 : 0)}%` : '',
      milestonePct > 0 ? `Milestone: ${Math.round(milestonePct)}%` : '',
      clusterFloor > 0 ? `Tracked cluster floor: ${moneyLabel(clusterFloor)}` : '',
      entryPrice > 0 && currentPrice > 0 ? `Entry / current: ${exactMoneyLabel(entryPrice)} -> ${exactMoneyLabel(currentPrice)}` : '',
      amountFloor > 0 ? `Estimated gain floor: ${moneyLabel(amountFloor)}` : '',
      daysSinceCluster > 0 ? `Window: ${Math.round(daysSinceCluster)} days` : '',
      clusterDate ? `Cluster date: ${clusterDate}` : '',
      priceAsOf ? `Price as of: ${priceAsOf}` : '',
    ]);
  }

  if (
    ['notable_politician_trade', 'committee_relevance_buy', 'large_politician_buy', 'theme_politician_buy', 'crypto_politician_sell'].includes(
      row.rule_key
    )
  ) {
    const tradeDate = shortDate(payload.trade_date || row.signal_events?.occurred_at);
    return appendMissingDraftLines(draft, [tradeDate ? `Trade date: ${tradeDate}` : '', filedLabel ? `Filed: ${filedLabel}` : '']);
  }

  return draft;
}

function actorCountForCandidate(row: TweetCandidateRow, actorLabels: string[], sourceMix: { congress: number; insiders: number; funds: number }) {
  const payloadCount = Math.max(0, toNumber(row.payload?.cluster_actor_count));
  if (payloadCount) {
    return payloadCount;
  }
  const mixTotal = sourceMix.congress + sourceMix.insiders + sourceMix.funds;
  if (mixTotal) {
    return mixTotal;
  }
  if (actorLabels.length) {
    return actorLabels.length;
  }
  return 1;
}

function searchBlobForStory(story: BroadcastStory) {
  return [
    story.title,
    story.rationale,
    story.ticker,
    story.actorName,
    story.signalType,
    story.direction,
    ...story.actorLabels,
    ...story.committees,
    ...story.themes,
    story.channels.twitter?.draftText,
    story.channels.discord_premium?.draftText,
  ]
    .map((value) => trim(value).toLowerCase())
    .filter(Boolean)
    .join(' ');
}

function storyIsCuratable(story: BroadcastStory) {
  const ticker = trim(story.ticker).toUpperCase();
  const unpublishableTicker = !ticker || ['N/A', 'MULTI', 'UNKNOWN'].includes(ticker);
  if (!unpublishableTicker) {
    return true;
  }
  return story.ruleKey === 'notable_politician_filing';
}

function sortStories(stories: BroadcastStory[], sort: string | null | undefined) {
  const normalized = trim(sort).toLowerCase() || 'score';
  const ranked = [...stories];
  ranked.sort((left, right) => {
    if (normalized === 'newest') {
      return (right.latestPublishedAt || right.createdAt).localeCompare(left.latestPublishedAt || left.createdAt);
    }
    if (normalized === 'size') {
      return right.amountFloor - left.amountFloor || right.score - left.score;
    }
    if (normalized === 'actors') {
      return right.actorCount - left.actorCount || right.score - left.score;
    }
    return right.score - left.score || (right.latestPublishedAt || right.createdAt).localeCompare(left.latestPublishedAt || left.createdAt);
  });
  return ranked;
}

async function resolveStoryAmountFloors(stories: BroadcastStory[]) {
  const eventIds = Array.from(new Set(stories.flatMap((story) => story.supportingEventIds).filter(Boolean)));
  if (!eventIds.length) {
    return new Map<string, number>();
  }

  const supabase = getAdminSupabase();
  const rowsById = new Map<string, SupportingSignalEventRow>();
  const pendingIds = [...eventIds];

  while (pendingIds.length) {
    const batch = pendingIds.splice(0, 200).filter((id) => !rowsById.has(id));
    if (!batch.length) {
      continue;
    }

    const response = await supabase.from('signal_events').select('id, payload').in('id', batch);
    if (response.error) {
      throw response.error;
    }

    for (const row of (response.data ?? []) as SupportingSignalEventRow[]) {
      rowsById.set(row.id, row);
      const payload = row.payload || {};
      for (const childId of [...nestedEventIds(payload.cluster_event_ids), ...nestedEventIds(payload.group_event_ids)]) {
        if (!rowsById.has(childId)) {
          pendingIds.push(childId);
        }
      }
    }
  }

  return new Map(
    stories.map((story) => {
      const seen = new Set<string>();
      const queue = [...story.supportingEventIds];
      const leafRows: SupportingSignalEventRow[] = [];

      while (queue.length) {
        const eventId = queue.shift();
        if (!eventId || seen.has(eventId)) {
          continue;
        }
        seen.add(eventId);

        const row = rowsById.get(eventId);
        if (!row) {
          continue;
        }

        const payload = row.payload || {};
        const childIds = [...nestedEventIds(payload.cluster_event_ids), ...nestedEventIds(payload.group_event_ids)];
        if (childIds.length) {
          for (const childId of childIds) {
            if (!seen.has(childId)) {
              queue.push(childId);
            }
          }
          continue;
        }

        leafRows.push(row);
      }

      const fallbackFloor = uniqueEconomicLeafRows(leafRows).reduce((total, row) => total + eventAmountFloor(row), 0);

      return [story.candidateKey, fallbackFloor > 0 ? fallbackFloor : story.amountFloor];
    })
  );
}

function storyMatchesCategory(story: BroadcastStory, category: string | null | undefined) {
  const normalized = trim(category).toLowerCase();
  if (!normalized || normalized === 'all') {
    return story.category === 'politicians' || story.category === 'insiders';
  }
  if (normalized === 'cluster_feed') {
    return (
      story.ruleKey === 'congress_cluster' ||
      story.ruleKey === 'cross_source_accumulation' ||
      story.ruleKey === 'grouped_congress_buy' ||
      story.ruleKey === 'grouped_insider_buy' ||
      story.ruleKey === 'insider_cluster'
    );
  }
  if (normalized === 'politicians') {
    return story.category === 'politicians' || story.sourceMix.congress > 0;
  }
  if (normalized === 'insiders') {
    return story.category === 'insiders' || story.sourceMix.insiders > 0;
  }
  return story.category === normalized;
}

function groupRows(rows: TweetCandidateRow[]) {
  const stories = new Map<string, BroadcastStory>();

  for (const row of rows) {
    const key = trim(row.candidate_key);
    if (!key) {
      continue;
    }
    const actorLabels = candidateActorLabels(row);
    const sourceMix = sourceMixForCandidate(row, actorLabels.length);
    const actorCount = actorCountForCandidate(row, actorLabels, sourceMix);
    const committees = candidateCommittees(row);
    const themes = candidateThemes(row);
    const amountFloor = amountFloorForCandidate(row);
    const supportingEventIds = supportingEventIdsForCandidate(row);
    const clusterWindowDays = Math.max(0, toNumber(row.payload?.cluster_window_days)) || null;

    const channelData: BroadcastStoryChannel = {
      id: row.id,
      channel: row.channel,
      status: row.status,
      title: row.title,
      draftText: hydratedDraftText(row),
      reviewNotes: row.review_notes,
      reviewedAt: row.reviewed_at,
      postedAt: row.posted_at,
      externalPostId: row.external_post_id || null,
      score: toNumber(row.score),
    };

    const existing = stories.get(key);
    if (existing) {
      existing.channels[row.channel as BroadcastStoryChannelKey] = channelData;
      existing.score = Math.max(existing.score, toNumber(row.score));
      existing.reviewedAt = maxIso(existing.reviewedAt, row.reviewed_at);
      existing.postedAt = maxIso(existing.postedAt, row.posted_at);
      existing.createdAt = minIso(existing.createdAt, row.created_at) || row.created_at;
      existing.reviewNotes = existing.reviewNotes || row.review_notes;
      existing.latestPublishedAt = maxIso(existing.latestPublishedAt, row.signal_events?.published_at);
      existing.sourceUrl = existing.sourceUrl || row.signal_events?.source_url || null;
      existing.amountFloor = Math.max(existing.amountFloor, amountFloor);
      existing.amountLabel = moneyLabel(existing.amountFloor);
      existing.amountRanges = uniqueStrings([...existing.amountRanges, ...amountRangesForCandidate(row)]);
      existing.clusterWindowDays = Math.max(existing.clusterWindowDays || 0, clusterWindowDays || 0) || null;
      existing.actorCount = Math.max(existing.actorCount, actorCount);
      existing.actorLabels = uniqueStrings([...existing.actorLabels, ...actorLabels]);
      existing.committees = uniqueStrings([...existing.committees, ...committees]);
      existing.themes = uniqueStrings([...existing.themes, ...themes]);
      const rowTradeStart =
        trim(row.payload?.group_trade_date_start as string | null | undefined) ||
        trim(row.payload?.trade_date as string | null | undefined) ||
        trim(row.signal_events?.occurred_at as string | null | undefined) ||
        null;
      const rowTradeEnd =
        trim(row.payload?.group_trade_date_end as string | null | undefined) ||
        trim(row.payload?.trade_date as string | null | undefined) ||
        trim(row.signal_events?.occurred_at as string | null | undefined) ||
        null;
      existing.tradeDateStart = minIso(existing.tradeDateStart, rowTradeStart);
      existing.tradeDateEnd = maxIso(existing.tradeDateEnd, rowTradeEnd);
      existing.sourceMix = {
        congress: Math.max(existing.sourceMix.congress, sourceMix.congress),
        insiders: Math.max(existing.sourceMix.insiders, sourceMix.insiders),
        funds: Math.max(existing.sourceMix.funds, sourceMix.funds),
      };
      existing.supportingEventIds = uniqueStrings([...existing.supportingEventIds, ...supportingEventIds]);
      continue;
    }

    stories.set(key, {
      candidateKey: key,
      ruleKey: row.rule_key,
      category: candidateCategory(row),
      title: row.title,
      rationale: row.rationale,
      score: toNumber(row.score),
      ticker: trim(row.payload?.ticker || row.signal_events?.ticker) || null,
      actorName: trim(row.payload?.actor_name || row.signal_events?.actor_name) || null,
      signalType: trim(row.payload?.signal_type || row.signal_events?.signal_type) || null,
      direction: trim(row.payload?.direction) || null,
      actorCount,
      amountFloor,
      amountLabel: moneyLabel(amountFloor),
      amountRanges: amountRangesForCandidate(row),
      clusterWindowDays,
      tradeDateStart:
        trim(row.payload?.group_trade_date_start as string | null | undefined) ||
        trim(row.payload?.trade_date as string | null | undefined) ||
        trim(row.signal_events?.occurred_at as string | null | undefined) ||
        null,
      tradeDateEnd:
        trim(row.payload?.group_trade_date_end as string | null | undefined) ||
        trim(row.payload?.trade_date as string | null | undefined) ||
        trim(row.signal_events?.occurred_at as string | null | undefined) ||
        null,
      sourceMix,
      actorLabels,
      committees,
      themes,
      latestPublishedAt: row.signal_events?.published_at || null,
      sourceUrl: row.signal_events?.source_url || null,
      supportingEventIds,
      createdAt: row.created_at,
      reviewedAt: row.reviewed_at,
      postedAt: row.posted_at,
      reviewNotes: row.review_notes,
      channels: {
        [row.channel]: channelData,
      },
    });
  }

  return Array.from(stories.values());
}

export async function tweetCandidatesEnabled() {
  const supabase = getAdminSupabase();
  try {
    const response = await supabase.from('tweet_candidates').select('id', { count: 'exact', head: true }).limit(1);
    return !response.error;
  } catch {
    return false;
  }
}

export async function fetchTweetCandidateStories(options: FetchStoryOptions) {
  const supabase = getAdminSupabase();
  const storyLimit = Number.isFinite(options.storyLimit) ? options.storyLimit : 60;
  const rowLimit = Math.max(Math.min(storyLimit * 8, 1000), 240);
  const normalizedSort = trim(options.sort).toLowerCase() || 'score';
  const statuses = Array.isArray(options.status)
    ? uniqueStrings(options.status)
    : uniqueStrings([options.status]);

  let query = supabase
    .from('tweet_candidates')
    .select(
      'id, candidate_key, channel, status, rule_key, score, title, draft_text, rationale, payload, created_at, reviewed_at, posted_at, review_notes, external_post_id, signal_event_id, signal_events!inner(ticker, actor_name, signal_type, source_url, occurred_at, published_at)'
    )
    .limit(rowLimit);

  if (normalizedSort === 'newest') {
    query = query
      .order('created_at', { ascending: false })
      .order('score', { ascending: false });
  } else {
    query = query
      .order('score', { ascending: false })
      .order('created_at', { ascending: false });
  }

  if (statuses.length === 1) {
    query = query.eq('status', statuses[0]);
  } else if (statuses.length > 1) {
    query = query.in('status', statuses);
  }

  if (options.sinceDate) {
    query = query.gte('signal_events.published_at', options.sinceDate);
  }

  const response = await query;
  if (response.error) {
    throw response.error;
  }

  const rows = (response.data ?? []) as TweetCandidateRow[];
  let stories = groupRows(rows);

  stories = stories.filter((story) => storyMatchesCategory(story, options.category));

  const queryText = trim(options.queryText).toLowerCase();
  if (queryText) {
    stories = stories.filter((story) => searchBlobForStory(story).includes(queryText));
  }

  stories = stories.filter(storyIsCuratable);
  const resolvedAmountFloors = await resolveStoryAmountFloors(stories);
  stories = stories.map((story) => {
    const amountFloor = Math.max(story.amountFloor, resolvedAmountFloors.get(story.candidateKey) || 0);
    return {
      ...story,
      amountFloor,
      amountLabel: moneyLabel(amountFloor),
      amountRanges: uniqueStrings(story.amountRanges),
    };
  });

  return sortStories(stories, options.sort).slice(0, storyLimit);
}

export async function fetchTweetCandidateStoryByKey(
  candidateKey: string,
  options: FetchStoryByKeyOptions = {},
) {
  const supabase = getAdminSupabase();
  let query = supabase
    .from('tweet_candidates')
    .select(
      'id, candidate_key, channel, status, rule_key, score, title, draft_text, rationale, payload, created_at, reviewed_at, posted_at, review_notes, external_post_id, signal_event_id, signal_events!inner(ticker, actor_name, signal_type, source_url, occurred_at, published_at)'
    )
    .eq('candidate_key', candidateKey)
    .order('score', { ascending: false })
    .order('created_at', { ascending: false })
    .limit(12);

  const statuses = Array.isArray(options.statuses)
    ? uniqueStrings(options.statuses)
    : [];
  if (statuses.length === 1) {
    query = query.eq('status', statuses[0]);
  } else if (statuses.length > 1) {
    query = query.in('status', statuses);
  }

  const response = await query;

  if (response.error) {
    throw response.error;
  }

  const rows = (response.data ?? []) as TweetCandidateRow[];
  const story = groupRows(rows).find((item) => item.candidateKey === candidateKey) || null;
  if (!story || !storyIsCuratable(story)) {
    return null;
  }
  const resolvedAmountFloors = await resolveStoryAmountFloors([story]);
  const amountFloor = Math.max(story.amountFloor, resolvedAmountFloors.get(story.candidateKey) || 0);
  return {
    ...story,
    amountFloor,
    amountLabel: moneyLabel(amountFloor),
    amountRanges: uniqueStrings(story.amountRanges),
  };
}

export function storyAmountRangeLabel(story: BroadcastStory) {
  return rangeSummary(story.amountRanges);
}

export async function fetchTweetCandidateById(id: string) {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('tweet_candidates')
    .select('id, candidate_key, channel, status, title, draft_text, review_notes, external_post_id')
    .eq('id', id)
    .limit(1);
  if (response.error) {
    throw response.error;
  }
  return (response.data ?? [])[0] ?? null;
}

export async function updateTweetCandidate(
  id: string,
  payload: {
    status?: 'pending_review' | 'approved' | 'rejected' | 'posted';
    review_notes?: string;
    reviewed_by?: string;
    external_post_id?: string;
    draft_text?: string;
    title?: string;
  }
) {
  const supabase = getAdminSupabase();
  const update: Record<string, string | null> = {};

  if (payload.draft_text !== undefined) {
    update.draft_text = payload.draft_text ?? null;
  }
  if (payload.title !== undefined) {
    update.title = payload.title ?? null;
  }
  if (payload.review_notes !== undefined) {
    update.review_notes = payload.review_notes || null;
  }
  if (payload.status) {
    update.status = payload.status;
    update.reviewed_by = payload.reviewed_by || 'ops_ui';
    update.reviewed_at = new Date().toISOString();
    if (payload.status === 'posted') {
      update.posted_at = new Date().toISOString();
      update.external_post_id = payload.external_post_id || null;
    }
  }

  if (!Object.keys(update).length) {
    throw new Error('No candidate updates were provided.');
  }

  const response = await supabase.from('tweet_candidates').update(update).eq('id', id).select('id').limit(1);
  if (response.error) {
    throw response.error;
  }
  return response.data?.[0] ?? null;
}

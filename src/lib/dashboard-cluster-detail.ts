import 'server-only';

import type { DashboardClusterDetail, DashboardClusterTransaction } from '@/lib/dashboard-cluster-types';
import { parsePoliticianOptionDetails } from '@/lib/politician-option-trades';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { fetchTweetCandidateStoryByKey } from '@/lib/tweet-candidates';

type SignalEventRow = {
  id: string;
  source: string | null;
  signal_type: string | null;
  actor_name: string | null;
  ticker: string | null;
  source_url: string | null;
  published_at: string | null;
  payload?: Record<string, unknown> | null;
};

type PoliticianTradeMetadata = {
  assetName: string | null;
  assetType: string | null;
};

const CLUSTER_DETAIL_RULES = new Set([
  'congress_cluster',
  'cross_source_accumulation',
  'grouped_congress_buy',
  'grouped_insider_buy',
  'insider_cluster',
]);

function trim(value: unknown) {
  return String(value ?? '').trim();
}

function toNumber(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function titleCase(value: string) {
  return value
    .split(/[\s_]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function isIsoDate(value: string | null | undefined) {
  return Boolean(value && /^\d{4}-\d{2}-\d{2}$/.test(value));
}

function normalizedTransactionDate(
  sourceType: DashboardClusterTransaction['sourceType'],
  transactionDate: string | null,
  publishedDate: string | null,
) {
  const safeTransactionDate = trim(transactionDate) || null;
  const safePublishedDate = trim(publishedDate) || null;

  if (sourceType !== 'politician') {
    return safeTransactionDate;
  }
  if (!isIsoDate(safeTransactionDate) || !isIsoDate(safePublishedDate)) {
    return safeTransactionDate;
  }
  const transactionDateIso = safeTransactionDate as string;
  const publishedDateIso = safePublishedDate as string;
  if (transactionDateIso > publishedDateIso) {
    return publishedDateIso;
  }
  return transactionDateIso;
}

function formatCompactCurrency(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  if (value >= 1_000_000_000) {
    return `$${(value / 1_000_000_000).toFixed(2)}B`;
  }
  if (value >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(2)}M`;
  }
  if (value >= 1_000) {
    return `$${(value / 1_000).toFixed(1)}K`;
  }
  return `$${Math.round(value).toLocaleString()}`;
}

function isGenericAssetName(value: string | null | undefined) {
  const normalized = trim(value).toLowerCase();
  return normalized === '' || normalized === 'stock' || normalized === 'common stock';
}

function shouldPreferPoliticianTradeMetadata(
  currentAssetName: string | null,
  currentAssetType: string | null,
  fallbackAssetName: string | null,
  fallbackAssetType: string | null,
) {
  if (!trim(fallbackAssetName)) {
    return false;
  }

  const currentOption = parsePoliticianOptionDetails({
    asset_name: currentAssetName,
    asset_type: currentAssetType,
  });
  const fallbackOption = parsePoliticianOptionDetails({
    asset_name: fallbackAssetName,
    asset_type: fallbackAssetType,
  });

  if (
    fallbackOption &&
    (!currentOption ||
      (!currentOption.strikePrice && !currentOption.expirationDate && !currentOption.side))
  ) {
    return true;
  }

  return isGenericAssetName(currentAssetName) && trim(fallbackAssetName) !== trim(currentAssetName);
}

function nestedEventIds(payload: Record<string, unknown> | null | undefined) {
  const clusterIds = Array.isArray(payload?.cluster_event_ids) ? payload.cluster_event_ids : [];
  const groupIds = Array.isArray(payload?.group_event_ids) ? payload.group_event_ids : [];

  return [...clusterIds, ...groupIds]
    .map((value) => trim(value))
    .filter(Boolean);
}

async function loadSignalEventGraph(rootIds: string[]) {
  const supabase = getAdminSupabase();
  const rowsById = new Map<string, SignalEventRow>();
  const pendingIds = [...rootIds];

  while (pendingIds.length) {
    const batch = pendingIds.splice(0, 200).filter((id) => !rowsById.has(id));
    if (!batch.length) {
      continue;
    }

    const { data, error } = await supabase
      .from('signal_events')
      .select('id, source, signal_type, actor_name, ticker, source_url, published_at, payload')
      .in('id', batch);

    if (error) {
      throw new Error(error.message);
    }

    for (const row of (data ?? []) as SignalEventRow[]) {
      rowsById.set(row.id, row);
      for (const childId of nestedEventIds(row.payload)) {
        if (!rowsById.has(childId)) {
          pendingIds.push(childId);
        }
      }
    }
  }

  return rowsById;
}

async function loadPoliticianTradeMetadata(docIds: string[]) {
  const supabase = getAdminSupabase();
  const metadataByDocId = new Map<string, PoliticianTradeMetadata>();

  for (let index = 0; index < docIds.length; index += 200) {
    const chunk = docIds.slice(index, index + 200).filter(Boolean);
    if (!chunk.length) {
      continue;
    }

    const { data, error } = await supabase
      .from('politician_trades')
      .select('doc_id, asset_name, asset_type')
      .in('doc_id', chunk);

    if (error) {
      throw new Error(error.message);
    }

    for (const row of (data ?? []) as { doc_id: string | null; asset_name: string | null; asset_type: string | null }[]) {
      const docId = trim(row.doc_id);
      if (!docId) {
        continue;
      }

      const nextAssetName = trim(row.asset_name) || null;
      const nextAssetType = trim(row.asset_type) || null;
      const current = metadataByDocId.get(docId);

      if (
        !current ||
        shouldPreferPoliticianTradeMetadata(current.assetName, current.assetType, nextAssetName, nextAssetType)
      ) {
        metadataByDocId.set(docId, {
          assetName: nextAssetName,
          assetType: nextAssetType,
        });
      }
    }
  }

  return metadataByDocId;
}

function collectLeafRows(rootIds: string[], rowsById: Map<string, SignalEventRow>) {
  const seen = new Set<string>();
  const queue = [...rootIds];
  const leaves: SignalEventRow[] = [];

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

    const childIds = nestedEventIds(row.payload);
    if (childIds.length) {
      for (const childId of childIds) {
        if (!seen.has(childId)) {
          queue.push(childId);
        }
      }
      continue;
    }

    leaves.push(row);
  }

  return leaves.sort((left, right) => {
    const leftPayload = left.payload || {};
    const rightPayload = right.payload || {};
    const leftDate = trim(leftPayload.transaction_date || leftPayload.report_period || leftPayload.published_date || left.published_at);
    const rightDate = trim(rightPayload.transaction_date || rightPayload.report_period || rightPayload.published_date || right.published_at);
    if (leftDate !== rightDate) {
      return rightDate.localeCompare(leftDate);
    }
    return trim(right.actor_name).localeCompare(trim(left.actor_name));
  });
}

function sourceTypeForRow(row: SignalEventRow): DashboardClusterTransaction['sourceType'] {
  const source = trim(row.source).toLowerCase();
  if (source === 'congress') {
    return 'politician';
  }
  if (source === 'insider') {
    return 'insider';
  }
  if (source === 'hedge_fund') {
    return 'fund';
  }
  return 'unknown';
}

function transactionTypeLabel(row: SignalEventRow, payload: Record<string, unknown>) {
  const normalized = trim(payload.transaction_type || payload.transaction_code || payload.change_type || row.signal_type).toLowerCase();
  if (!normalized) {
    return 'Activity';
  }
  if (['buy', 'purchase', 'p', 'a'].includes(normalized)) {
    return 'Buy';
  }
  if (['sell', 'sale', 's', 'd'].includes(normalized)) {
    return 'Sell';
  }
  if (normalized === 'call_options') {
    return 'Call Options';
  }
  if (normalized === 'put_options') {
    return 'Put Options';
  }
  if (normalized === 'new') {
    return 'New Position';
  }
  if (normalized === 'exit') {
    return 'Exit';
  }
  if (normalized === 'increase') {
    return 'Increase';
  }
  if (normalized === 'decrease') {
    return 'Decrease';
  }
  if (normalized === 'hold') {
    return 'Hold';
  }
  return titleCase(normalized);
}

function actorSubtitle(row: SignalEventRow, payload: Record<string, unknown>) {
  const sourceType = sourceTypeForRow(row);
  if (sourceType === 'politician') {
    const chamber = trim(payload.chamber);
    const party = trim(payload.party);
    const parts = [chamber && chamber !== 'Unknown' ? chamber : 'Congress', party && party !== 'Unknown' ? party : '']
      .filter(Boolean);
    return parts.join(' • ') || 'Congress';
  }
  if (sourceType === 'insider') {
    return trim(payload.filer_relation) || 'Insider filing';
  }
  if (sourceType === 'fund') {
    const reportPeriod = trim(payload.report_period);
    return reportPeriod ? `13F ${reportPeriod}` : '13F filing';
  }
  return null;
}

function assetLabel(row: SignalEventRow, payload: Record<string, unknown>, resolvedAssetName?: string | null) {
  return (
    trim(payload.ticker || row.ticker).toUpperCase() ||
    trim(resolvedAssetName || payload.asset_name) ||
    trim(payload.fund_name) ||
    'Unmapped Asset'
  );
}

function amountLabel(row: SignalEventRow, payload: Record<string, unknown>) {
  const sourceType = sourceTypeForRow(row);
  if (sourceType === 'politician') {
    return trim(payload.amount_range) || null;
  }

  if (sourceType === 'insider') {
    return (
      formatCompactCurrency(toNumber(payload.value)) ||
      formatCompactCurrency(toNumber(payload.amount) * toNumber(payload.price)) ||
      null
    );
  }

  if (sourceType === 'fund') {
    return (
      formatCompactCurrency(toNumber(payload.value_held)) ||
      formatCompactCurrency(toNumber(payload.value)) ||
      null
    );
  }

  return trim(payload.amount_range) || formatCompactCurrency(toNumber(payload.value)) || null;
}

function mapRowToTransaction(
  row: SignalEventRow,
  politicianTradeMetadataByDocId: Map<string, PoliticianTradeMetadata>,
): DashboardClusterTransaction {
  const payload = row.payload || {};
  const sourceType = sourceTypeForRow(row);
  const docId = trim(payload.doc_id);
  const currentAssetName = trim(payload.asset_name) || null;
  const currentAssetType = trim(payload.asset_type) || null;
  const fallbackTradeMetadata = docId ? politicianTradeMetadataByDocId.get(docId) : null;
  const assetName = shouldPreferPoliticianTradeMetadata(
    currentAssetName,
    currentAssetType,
    fallbackTradeMetadata?.assetName || null,
    fallbackTradeMetadata?.assetType || null,
  )
    ? fallbackTradeMetadata?.assetName || null
    : currentAssetName;
  const assetType = shouldPreferPoliticianTradeMetadata(
    currentAssetName,
    currentAssetType,
    fallbackTradeMetadata?.assetName || null,
    fallbackTradeMetadata?.assetType || null,
  )
    ? fallbackTradeMetadata?.assetType || null
    : currentAssetType;
  const publishedDate = trim(payload.published_date || row.published_at) || null;
  const transactionDate = normalizedTransactionDate(
    sourceType,
    trim(payload.transaction_date || payload.report_period) || null,
    publishedDate,
  );

  return {
    id: row.id,
    sourceType,
    actorName:
      trim(payload.politician_name || payload.filer_name || payload.fund_name || row.actor_name) ||
      'Unknown actor',
    memberId: trim(payload.member_id) || null,
    party: trim(payload.party) || null,
    actorSubtitle: actorSubtitle(row, payload),
    assetLabel: assetLabel(row, payload, assetName),
    assetName,
    assetType,
    transactionTypeLabel: transactionTypeLabel(row, payload),
    amountLabel: amountLabel(row, payload),
    transactionDate,
    publishedDate,
    sourceUrl: trim(payload.source_url || row.source_url) || null,
  };
}

export async function getDashboardClusterDetail(
  candidateKey: string,
  options: { statuses?: string[] | null } = {},
): Promise<DashboardClusterDetail | null> {
  const story = await fetchTweetCandidateStoryByKey(candidateKey, {
    statuses: options.statuses,
  });
  if (!story) {
    return null;
  }
  if (!CLUSTER_DETAIL_RULES.has(story.ruleKey)) {
    return null;
  }
  if (!story.supportingEventIds.length) {
    return null;
  }

  const rowsById = await loadSignalEventGraph(story.supportingEventIds);
  const leafRows = collectLeafRows(story.supportingEventIds, rowsById);
  if (!leafRows.length) {
    return null;
  }
  const politicianDocIds = leafRows
    .map((row) => trim(row.payload?.doc_id))
    .filter(Boolean);
  const politicianTradeMetadataByDocId = politicianDocIds.length
    ? await loadPoliticianTradeMetadata(politicianDocIds)
    : new Map<string, PoliticianTradeMetadata>();
  const transactions = leafRows.map((row) => mapRowToTransaction(row, politicianTradeMetadataByDocId));

  return {
    transactions,
  };
}

import 'server-only';

import { getAdminSupabase } from '@/lib/supabase-admin';

type PoliticianTradeLike = {
  id: string;
  doc_id?: string | null;
  ticker?: string | null;
  source_url?: string | null;
  politician_name?: string | null;
  transaction_date?: string | null;
  transaction_type?: string | null;
  amount_range?: string | null;
  asset_name?: string | null;
};

type RawFilingRow = {
  payload?: {
    official_source_url?: string | null;
    politician_name?: string | null;
    transaction_date?: string | null;
    transaction_type?: string | null;
    amount_range?: string | null;
    asset_name?: string | null;
  } | null;
};

function normalizeText(value: string | null | undefined) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function normalizeDirection(value: string | null | undefined) {
  const normalized = normalizeText(value);
  if (normalized.startsWith('buy') || normalized === 'purchase' || normalized === 'p') {
    return 'buy';
  }
  if (normalized.startsWith('sell') || normalized === 'sale' || normalized === 's') {
    return 'sell';
  }
  if (normalized.startsWith('exchange') || normalized === 'e') {
    return 'exchange';
  }
  return normalized;
}

function enrichableTicker(value: string | null | undefined) {
  const normalized = String(value || '').trim().toUpperCase();
  return !normalized || normalized === 'N/A' || normalized === 'NA' || normalized === 'UNKNOWN' || normalized === 'US-TREAS';
}

function assetMatchKey(value: {
  source_url?: string | null;
  politician_name?: string | null;
  transaction_date?: string | null;
  transaction_type?: string | null;
  amount_range?: string | null;
}) {
  return [
    normalizeText(value.source_url),
    normalizeText(value.politician_name),
    normalizeText(value.transaction_date),
    normalizeDirection(value.transaction_type),
    normalizeText(value.amount_range),
  ].join('|');
}

async function fetchAssetNameMap(officialSourceUrls: string[]) {
  if (!officialSourceUrls.length) {
    return new Map<string, string>();
  }

  const supabase = getAdminSupabase();
  const rows: RawFilingRow[] = [];
  const chunkSize = 50;

  for (let index = 0; index < officialSourceUrls.length; index += chunkSize) {
    const chunk = officialSourceUrls.slice(index, index + chunkSize);
    const { data, error } = await supabase
      .from('raw_filings')
      .select('payload')
      .eq('source', 'capitol_trades')
      .in('payload->>official_source_url', chunk);

    if (error) {
      throw new Error(error.message);
    }
    rows.push(...((data as RawFilingRow[] | null) || []));
  }

  const assetNames = new Map<string, string>();
  for (const row of rows) {
    const payload = row.payload || {};
    const assetName = String(payload.asset_name || '').trim();
    if (!assetName) {
      continue;
    }
    assetNames.set(
      assetMatchKey({
        source_url: payload.official_source_url,
        politician_name: payload.politician_name,
        transaction_date: payload.transaction_date,
        transaction_type: payload.transaction_type,
        amount_range: payload.amount_range,
      }),
      assetName,
    );
  }
  return assetNames;
}

async function fetchOfficialAssetNameMap(sourceDocumentIds: string[]) {
  if (!sourceDocumentIds.length) {
    return new Map<string, string>();
  }

  const supabase = getAdminSupabase();
  const rows: { source_document_id?: string | null; payload?: { asset_name?: string | null } | null }[] = [];
  const chunkSize = 100;

  for (let index = 0; index < sourceDocumentIds.length; index += chunkSize) {
    const chunk = sourceDocumentIds.slice(index, index + chunkSize);
    const { data, error } = await supabase
      .from('raw_filings')
      .select('source_document_id,payload')
      .eq('source', 'congress')
      .in('source_document_id', chunk);

    if (error) {
      throw new Error(error.message);
    }
    rows.push(...((data as typeof rows | null) || []));
  }

  const assetNames = new Map<string, string>();
  for (const row of rows) {
    const docId = String(row.source_document_id || '').trim();
    const assetName = String(row.payload?.asset_name || '').trim();
    if (!docId || !assetName) {
      continue;
    }
    assetNames.set(docId, assetName);
  }
  return assetNames;
}

export async function enrichPoliticianTradesWithAssetNames<T extends PoliticianTradeLike>(trades: T[]): Promise<T[]> {
  const candidates = trades.filter((trade) => enrichableTicker(trade.ticker) && !String(trade.asset_name || '').trim());
  const docIds = [...new Set(candidates.map((trade) => String(trade.doc_id || '').trim()).filter(Boolean))];
  const officialSourceUrls = [...new Set(candidates.map((trade) => String(trade.source_url || '').trim()).filter(Boolean))];

  if (!docIds.length && !officialSourceUrls.length) {
    return trades;
  }

  const [officialAssetNames, capitolAssetNames] = await Promise.all([
    fetchOfficialAssetNameMap(docIds),
    fetchAssetNameMap(officialSourceUrls),
  ]);
  if (!officialAssetNames.size && !capitolAssetNames.size) {
    return trades;
  }

  return trades.map((trade) => {
    if (!enrichableTicker(trade.ticker) || String(trade.asset_name || '').trim()) {
      return trade;
    }
    const assetName =
      officialAssetNames.get(String(trade.doc_id || '').trim()) ||
      capitolAssetNames.get(
        assetMatchKey({
          source_url: trade.source_url,
          politician_name: trade.politician_name,
          transaction_date: trade.transaction_date,
          transaction_type: trade.transaction_type,
          amount_range: trade.amount_range,
        }),
      );
    if (!assetName) {
      return trade;
    }
    return {
      ...trade,
      asset_name: assetName,
    };
  });
}

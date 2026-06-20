import 'server-only';

import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import { getAdminSupabase } from '@/lib/supabase-admin';

export type PoliticianDisclosureHolding = {
  key: string;
  docId: string;
  filingDate: string;
  filingType: string | null;
  filingTypeLabel: string | null;
  ticker: string | null;
  label: string;
  assetType: string | null;
  owner: string | null;
  valueRange: string;
  sourceUrl: string | null;
};

type RawDisclosureHoldingRow = {
  source_document_id?: string | null;
  filed_at?: string | null;
  source_url?: string | null;
  payload?: {
    member_id?: string | null;
    doc_id?: string | null;
    filing_date?: string | null;
    filing_type?: string | null;
    filing_type_label?: string | null;
    asset_name?: string | null;
    asset_type?: string | null;
    ticker?: string | null;
    owner?: string | null;
    value_range?: string | null;
    source_url?: string | null;
    product_eligible?: boolean | null;
  } | null;
};

function trim(value: string | null | undefined) {
  return String(value || '').trim();
}

function normalizeTicker(value: string | null | undefined) {
  const normalized = trim(value).toUpperCase();
  if (!normalized || normalized === 'N/A' || normalized === 'UNKNOWN') {
    return null;
  }
  return normalized;
}

function disclosureKey(row: RawDisclosureHoldingRow) {
  const ticker = normalizeTicker(row.payload?.ticker);
  if (ticker) {
    return `ticker:${ticker}`;
  }
  const assetName = trim(row.payload?.asset_name).toLowerCase();
  return assetName ? `asset:${assetName}` : trim(row.source_document_id);
}

function lowerBound(valueRange: string | null | undefined) {
  return parsePoliticianAmountRange(valueRange)?.min || 0;
}

export async function getLatestPoliticianDisclosureHoldings(memberId: string): Promise<PoliticianDisclosureHolding[]> {
  const supabase = getAdminSupabase();
  const { data, error } = await supabase
    .from('raw_filings')
    .select('source_document_id,filed_at,source_url,payload')
    .eq('source', 'house_disclosures')
    .eq('payload->>member_id', memberId)
    .order('filed_at', { ascending: false })
    .limit(4000);

  if (error) {
    throw new Error(error.message);
  }

  const rows = ((data as RawDisclosureHoldingRow[] | null) || []).filter((row) => {
    const valueRange = trim(row.payload?.value_range).toLowerCase();
    return row.payload?.product_eligible !== false && valueRange !== 'none';
  });
  if (!rows.length) {
    return [];
  }

  const latestFilingDate = rows.reduce<string | null>((latest, row) => {
    const current = trim(row.payload?.filing_date || row.filed_at);
    if (!current) {
      return latest;
    }
    if (!latest || current > latest) {
      return current;
    }
    return latest;
  }, null);

  if (!latestFilingDate) {
    return [];
  }

  const latestRows = rows.filter((row) => trim(row.payload?.filing_date || row.filed_at) === latestFilingDate);
  const byKey = new Map<string, RawDisclosureHoldingRow>();

  for (const row of latestRows) {
    const valueRange = trim(row.payload?.value_range);
    if (!valueRange) {
      continue;
    }
    const key = disclosureKey(row);
    const current = byKey.get(key);
    if (!current || lowerBound(valueRange) > lowerBound(current.payload?.value_range)) {
      byKey.set(key, row);
    }
  }

  return [...byKey.values()]
    .map((row) => {
      const payload = row.payload || {};
      const label = trim(payload.asset_name);
      const valueRange = trim(payload.value_range);
      if (!label || !valueRange) {
        return null;
      }

      return {
        key: disclosureKey(row),
        docId: trim(payload.doc_id || row.source_document_id),
        filingDate: trim(payload.filing_date || row.filed_at),
        filingType: trim(payload.filing_type) || null,
        filingTypeLabel: trim(payload.filing_type_label) || null,
        ticker: normalizeTicker(payload.ticker),
        label,
        assetType: trim(payload.asset_type) || null,
        owner: trim(payload.owner) || null,
        valueRange,
        sourceUrl: trim(payload.source_url || row.source_url) || null,
      } satisfies PoliticianDisclosureHolding;
    })
    .filter((row): row is PoliticianDisclosureHolding => Boolean(row))
    .sort((left, right) => {
      const leftTicker = left.ticker || '';
      const rightTicker = right.ticker || '';
      if (leftTicker !== rightTicker) {
        return leftTicker.localeCompare(rightTicker);
      }
      return left.label.localeCompare(right.label);
    });
}

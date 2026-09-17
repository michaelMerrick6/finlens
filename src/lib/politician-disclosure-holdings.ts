import 'server-only';
import { readBoundedRows } from '@/lib/bounded-rows';

import { parsePoliticianAmountRange } from '@/lib/politician-amount-range';
import { getAdminSupabase } from '@/lib/supabase-admin';

export type PoliticianDisclosureHolding = {
  key: string;
  docId: string;
  filingDate: string;
  valuationDate: string;
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
    period_covered_end?: string | null;
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
  // Preserve accounts, ownership and asset classes even when tickers repeat.
  return trim(row.source_document_id);
}

function lowerBound(valueRange: string | null | undefined) {
  return parsePoliticianAmountRange(valueRange)?.min || 0;
}

export async function getLatestPoliticianDisclosureHoldings(memberId: string): Promise<PoliticianDisclosureHolding[]> {
  const supabase = getAdminSupabase();
  const history = await readBoundedRows<RawDisclosureHoldingRow>(4000, async (offset, count) => {
    const { data, error } = await supabase
      .from('raw_filings')
      .select('source_document_id,filed_at,source_url,payload')
      .eq('source', 'house_disclosures')
      .eq('payload->>member_id', memberId)
      .order('filed_at', { ascending: false })
      .order('id', { ascending: false })
      .range(offset, offset + count - 1);
    if (error) throw new Error(error.message);
    return (data || []) as RawDisclosureHoldingRow[];
  });
  if (history.hasMore) {
    throw new Error('Disclosure history exceeds the supported snapshot window; refusing a partial portfolio.');
  }

  // Select the document before filtering out None/ineligible rows: an empty newer
  // annual must never resurrect positions from an older filing.
  const rows = history.rows.filter(row => ['A', 'O', 'W'].includes(trim(row.payload?.filing_type)));
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
  const documents = new Set(latestRows.map(row => trim(row.payload?.doc_id)));
  if (documents.size !== 1 || latestRows.some(row => row.payload?.filing_type === 'W')) {
    throw new Error('Annual amendment or competing baselines require review.');
  }
  const byKey = new Map<string, RawDisclosureHoldingRow>();

  for (const row of latestRows) {
    const valueRange = trim(row.payload?.value_range);
    if (!valueRange || valueRange.toLowerCase() === 'none' || row.payload?.product_eligible === false) {
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

      const valuationDate = trim(payload.period_covered_end);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(valuationDate) || valuationDate > trim(payload.filing_date || row.filed_at)) {
        throw new Error('Annual holdings valuation date needs source review.');
      }
      return {
        valuationDate,
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

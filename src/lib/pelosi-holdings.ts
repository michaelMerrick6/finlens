import baseline from '../../artifacts/pelosi-audit/2025-holdings-baseline.json';
import changes from '../../artifacts/pelosi-audit/2026-position-changes.json';

type Evidence = { url: string; page: number; date: string; note: string };
export type ReviewedPosition = {
  key: string; ticker: string; owner: string; kind: 'stock' | 'units' | 'call' | 'private';
  strike: number | null; expiration: string | null; contracts: number | null;
  shareChange: number; reportedValue: string | null; totalShares: null;
  status: 'quantity-uncertain' | 'reconstructed' | 'closed' | 'unresolved'; evidence: Evidence[];
};
export function buildPelosiHoldings(asOf = new Date().toISOString().slice(0, 10), events = changes.events) {
  const positions = new Map<string, ReviewedPosition>();
  const key = (owner: string, ticker: string, kind: string, strike: number | null = null, expiration: string | null = null) =>
    [owner, ticker, kind, strike, expiration].join('|');
  function position(owner: string, ticker: string, kind: ReviewedPosition['kind'], strike: number | null = null, expiration: string | null = null) {
    const id = key(owner, ticker, kind, strike, expiration);
    if (!positions.has(id)) positions.set(id, { key: id, ticker, owner, kind, strike, expiration,
      contracts: kind === 'call' ? 0 : null, shareChange: 0, reportedValue: null, totalShares: null,
      status: kind === 'call' ? 'reconstructed' : kind === 'private' ? 'unresolved' : 'quantity-uncertain', evidence: [] });
    return positions.get(id)!;
  }
  for (const row of baseline.rows) {
    if (row.reported_value === 'None') continue; // Historical descriptions are not positive positions.
    const evidence = { url: baseline.source, page: row.page, date: '2025-12-31', note: `2025 annual disclosure: ${row.reported_value}` };
    if (row.asset_type_as_filed === 'OP') {
      for (const option of row.contracts_as_described || []) {
        const p = position(row.owner, row.ticker_as_filed, 'call', option.strike, option.expiration);
        p.contracts! += option.contracts;
        p.evidence.push({ ...evidence, note: `${option.contracts} calls described in the annual disclosure. Its value bracket covers the asset entry, not this series individually.` });
      }
    } else {
      const p = position(row.owner, row.ticker_as_filed, row.asset_type_as_filed === 'OL' ? 'units' : 'stock');
      p.reportedValue = row.reported_value;
      p.evidence.push(evidence);
    }
  }
  const seen = new Set<string>();
  for (const event of [...events].sort((a, b) => a.date.localeCompare(b.date))) {
    if (event.date <= '2025-12-31' || event.date > asOf) continue;
    if (seen.has(event.doc_id)) throw new Error('Duplicate reviewed holdings event');
    seen.add(event.doc_id);
    const evidence = { url: event.source, page: event.page, date: event.date, note: event.kind.replaceAll('_', ' ') };
    if (event.contract_delta) {
      if (event.strike === null || !event.expiration) throw new Error('Option terms missing');
      const p = position(event.owner, event.ticker, 'call', event.strike, event.expiration);
      p.contracts! += event.contract_delta;
      if (p.contracts! < 0) throw new Error('Exercise exceeds reviewed contracts');
      p.evidence.push({ ...evidence, note: `${evidence.note}: ${event.contract_delta > 0 ? '+' : ''}${event.contract_delta} contracts` });
    }
    if (event.share_delta || event.kind === 'private_investment_quantity_unknown') {
      const p = position(event.owner, event.ticker, event.kind === 'private_investment_quantity_unknown' ? 'private' : event.kind === 'public_partnership_units_purchase' ? 'units' : 'stock');
      p.shareChange += event.share_delta;
      p.evidence.push({ ...evidence, note: event.share_delta ? `${evidence.note}: ${event.share_delta > 0 ? '+' : ''}${event.share_delta.toLocaleString('en-US')} ${p.kind === 'units' ? 'units' : 'shares'}` : 'Private investment; quantity not disclosed' });
    }
  }
  for (const p of positions.values()) {
    if (p.kind === 'call') p.status = p.contracts === 0 ? 'closed' : p.expiration! < asOf ? 'unresolved' : 'reconstructed';
  }
  return [...positions.values()].sort((a, b) => a.ticker.localeCompare(b.ticker) || a.key.localeCompare(b.key));
}
export const pelosiHoldingsCoverage = {
  baselineDate: '2025-12-31', latestFiling: changes.latest_filing_reviewed,
  reviewedDocuments: [...new Set(changes.events.map(e => e.doc_id.split('-')[2]))],
};

export const pelosiCompanyNames: Record<string, string> = {
  AAPL: 'Apple', AB: 'AllianceBernstein', AMZN: 'Amazon', AVGO: 'Broadcom', AXP: 'American Express',
  BE: 'Bloom Energy', CLNE: 'Clean Energy Fuels', CMCSA: 'Comcast', CRM: 'Salesforce', CRWD: 'CrowdStrike',
  DBX: 'Dropbox', GOOGL: 'Alphabet Class A', IBKR: 'Interactive Brokers', INTC: 'Intel', MORN: 'Morningstar',
  MSFT: 'Microsoft', NFLX: 'Netflix', NVDA: 'NVIDIA', PANW: 'Palo Alto Networks', QCOM: 'Qualcomm',
  RBLX: 'Roblox', T: 'AT&T', SQ: 'Block (symbol as filed)', TEM: 'Tempus AI', V: 'Visa', VSNT: 'Versant', VST: 'Vistra',
  WBD: 'Warner Bros. Discovery', UBER: 'Uber',
};

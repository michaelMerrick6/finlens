import type { ReviewedPosition } from './pelosi-holdings';

type Step = { date: string; delta: number; note: string; source: string };
export type ShareReconciliation = {
  status: 'estimated' | 'unresolved'; shares: number | null; reason: string; steps: Step[]; assumptions: string[];
};
const annual = (year: number, doc: string, page: number) => `https://disclosures-clerk.house.gov/public_disc/financial-pdfs/${year}/${doc}.pdf#page=${page}`;
const annual24 = (page: number) => annual(2024, '10066169', page);
const annual25 = (page: number) => annual(2025, '10075701', page);
const historical: Record<string, { steps: Step[]; assumption: string }> = {
  NVDA: {
    assumption: 'Assume zero starting stock after the 2022 annual reported stock value “None”; the 2023 annual lists options only. This is an inferred opening balance, not a brokerage-confirmed zero.',
    steps: [
      { date: '2024-06-26', delta: 10000, note: 'Stock purchase, already after the June 2024 split', source: annual24(7) },
      { date: '2024-07-26', delta: 10000, note: 'Stock purchase', source: annual24(7) },
      { date: '2024-12-20', delta: 50000, note: 'Exercise: disclosed post-split share quantity; do not multiply by ten again', source: annual24(7) },
      { date: '2024-12-31', delta: -10000, note: 'Partial stock sale', source: annual24(8) },
      { date: '2025-12-24', delta: -20000, note: 'Partial stock sale', source: annual25(8) },
    ],
  },
  AVGO: {
    assumption: 'Assume zero common stock before the June 2025 exercise because the 2024 annual lists AVGO options but no AVGO common-stock entry. Absence is not proof of a zero balance.',
    steps: [{ date: '2025-06-20', delta: 20000, note: 'Exercise: 200 adjusted calls delivered 20,000 shares; split adjustment is already included', source: annual25(8) }],
  },
  PANW: {
    assumption: 'Assume zero opening stock before the 2024 exercise. The option purchase and exercise descriptions disagree on expiration; the disclosed 14,000-share acquisition is used, with that conflict unresolved.',
    steps: [{ date: '2024-12-20', delta: 14000, note: 'Exercise: reported share quantity after the two-for-one option adjustment', source: annual24(7) }],
  },
};
const inferredNewStock = new Set(['BE', 'INTC', 'TEM', 'VST', 'VSNT']);
export function reconcilePelosiShares(position: ReviewedPosition): ShareReconciliation {
  const p = position;
  const assumptions = ['Assumes no omitted trades, transfers, reinvestments or later corporate actions beyond the reviewed record. This is a conditional quantity estimate, not confirmed current ownership.'];
  if (p.owner !== 'SP' || p.kind !== 'stock') return { status: 'unresolved', shares: null, reason: 'No reviewed starting quantity for this owner and asset type.', steps: [], assumptions: [] };
  if (p.ticker === 'WBD') return { status: 'unresolved', shares: null, reason: 'The 2022 filing reports 2,419 shares received in the spinoff. A current total is withheld pending reconciliation of earlier interests and the subsequent corporate transaction.',
    steps: [{ date: '2022-04-11', delta: 2419, note: 'Historical spinoff receipt, not a verified current balance', source: annual(2022, '10053231', 10) }], assumptions: [] };
  const history = historical[p.ticker];
  if (!history && !inferredNewStock.has(p.ticker)) return { status: 'unresolved', shares: null,
    reason: 'The reviewed annual reports give a value range but no opening share count. Later share changes cannot establish the total; no dollar midpoint is converted to shares.', steps: [], assumptions: [] };
  if (!history && p.reportedValue) return { status: 'unresolved', shares: null, reason: 'A baseline stock entry prevents assuming a zero opening balance.', steps: [], assumptions: [] };
  const steps = [...(history?.steps || [])];
  // The position ledger supplies the already-reconciled subsequent net change.
  const base = steps.reduce((sum, step) => sum + step.delta, 0);
  const shares = base + p.shareChange;
  if (shares < 0) return { status: 'unresolved', shares: null, reason: 'Reviewed changes exceed the assumed opening balance.', steps, assumptions: [] };
  assumptions.unshift(history?.assumption || 'Assume no starting common-stock position because the 2025 annual has no stock entry for this asset. Any unreported or below-threshold opening balance would change this estimate.');
  return { status: 'estimated', shares, reason: 'Explicit share changes, combined with the stated opening-balance assumption.', steps, assumptions };
}
export const pelosiQuantityBaselineSources = [
  { label: '2022 annual · NVIDIA stock value None', url: annual(2022, '10053231', 5) },
  { label: '2023 annual · NVIDIA options / starting holdings', url: annual(2023, '10059734', 4) },
  { label: '2024 annual · Broadcom options', url: annual24(2) },
  { label: 'Reviewed split adjustments', url: 'https://infomemo.theocc.com/infomemos?number=54623' },
];

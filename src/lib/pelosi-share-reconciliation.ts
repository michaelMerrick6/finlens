import type { ReviewedPosition } from './pelosi-holdings';

export type Step = { date: string; delta: number; factor?: number; note: string; source: string };
// Quantities are in units at the event date. Never apply a split to later purchases.
export function replayShareHistory(steps: Step[], opening: number | null = 0, asOf = new Date().toISOString().slice(0, 10)) {
  let balance = opening;
  const seen = new Set<string>();
  for (const step of [...steps].sort((a, b) => a.date.localeCompare(b.date))) {
    if (step.date > asOf) continue;
    const id = `${step.date}|${step.source}|${step.note}`;
    if (seen.has(id)) throw new Error('Duplicate share event');
    seen.add(id);
    if (!Number.isFinite(step.delta) || (step.factor !== undefined && (!Number.isFinite(step.factor) || step.factor <= 0 || step.delta !== 0))) throw new Error('Invalid share event');
    if (balance !== null) {
      balance = step.factor === undefined ? balance + step.delta : balance * step.factor;
      if (balance < 0) return null;
    }
  }
  return balance;
}
export type ShareReconciliation = {
  status: 'estimated' | 'unresolved' | 'modeled-range'; shares: number | null; range?: { min: number; max: number }; reason: string; steps: Step[]; assumptions: string[];
};
const annual = (year: number, doc: string, page: number) => `https://disclosures-clerk.house.gov/public_disc/financial-pdfs/${year}/${doc}.pdf#page=${page}`;
const annual24 = (page: number) => annual(2024, '10066169', page);
const annual25 = (page: number) => annual(2025, '10075701', page);
const historical: Record<string, { steps: Step[]; assumption: string }> = {
  AB: {
    assumption: 'Assume no opening units: AllianceBernstein is absent from the 2019 annual. Use the amended 2020/2021 purchases once. The December 2022 sale omits ticker AB but names the same partnership; subtract its 20,000 units.',
    steps: [
      { date: '2020-12-22', delta: 20000, note: 'Purchased 20,000 partnership units; amended quantity', source: 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2021/20018539.pdf#page=1' },
      { date: '2021-02-18', delta: 15000, note: 'Purchased 15,000 partnership units; amended quantity', source: 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2021/20018539.pdf#page=1' },
      { date: '2021-02-23', delta: 25000, note: 'Purchased 25,000 partnership units; amended quantity', source: 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2021/20018539.pdf#page=1' },
      { date: '2022-01-27', delta: 10000, note: 'Purchased 10,000 partnership units', source: annual(2022, '10053231', 7) },
      { date: '2022-12-28', delta: -20000, note: 'Sold 20,000 units; annual names Alliance Bernstein but omits ticker', source: annual(2022, '10053231', 7) },
    ],
  },
  AXP: {
    assumption: 'Assume no opening common shares: the 2021 annual lists American Express options only. No later share changes appear in the reviewed annuals and 2026 events.',
    steps: [{ date: '2022-01-21', delta: 5000, note: 'Exercise of 50 calls delivered 5,000 shares', source: annual(2022, '10053231', 8) }],
  },
  DBX: {
    assumption: 'Assume no opening shares before the 2018 purchase; Dropbox is absent from the 2017 annual. The March 27 purchase is after the March 7 reverse split, so that split must not be applied again.',
    steps: [{ date: '2018-03-27', delta: 10000, note: 'Purchased 10,000 Class A shares after the pre-IPO reverse split', source: annual(2018, '10026982', 6) }],
  },
  SQ: {
    assumption: 'Assume no opening common shares: the 2016 annual lists Square options only. Square became Block; the 2021 annual separately lists the old name at None and the renamed holding as positive. This is one continuing position, not a disposal and repurchase. SQ is preserved as filed; the current ticker is XYZ.',
    steps: [{ date: '2017-01-20', delta: 5000, note: 'Exercise of 50 Square calls delivered 5,000 shares', source: annual(2017, '10020995', 6) }],
  },
  MSFT: {
    assumption: 'Assume no opening common shares: the 2020 annual lists Microsoft options only (page 4). Later reviewed annual transactions supply the exercises and sale.',
    steps: [
      { date: '2021-03-19', delta: 15000, note: 'Exercise of 150 calls at $130 delivered 15,000 shares', source: annual(2021, '10047356', 8) },
      { date: '2021-03-19', delta: 10000, note: 'Exercise of 100 calls at $140 delivered 10,000 shares', source: annual(2021, '10047356', 8) },
      { date: '2023-06-15', delta: 5000, note: 'Exercise delivered 5,000 shares', source: annual(2023, '10059734', 7) },
      { date: '2024-07-26', delta: -5000, note: 'Partial stock sale', source: annual24(7) },
    ],
  },
  CRWD: {
    assumption: 'Assume no opening shares before the 2020 purchase; no CrowdStrike holding appears in the 2019 annual. No later share transactions found in reviewed annuals through 2025 or reviewed 2026 PTRs.',
    steps: [
      { date: '2020-09-03', delta: 5000, note: 'Purchased 5,000 Class A shares', source: annual(2020, '10039988', 8) },
      { date: '2026-07-01', delta: 0, factor: 4, note: 'Four-for-one split effected after the close; 5,000 shares become 20,000', source: 'https://ir.crowdstrike.com/news-releases/news-release-details/crowdstrike-reports-second-quarter-fiscal-year-2027-financial' },
    ],
  },
  NFLX: {
    assumption: 'Assume no opening common shares before the 2020 exercise: the 2019 annual lists Netflix options only. The two 2022 sales precede the 2025 split.',
    steps: [
      { date: '2020-06-18', delta: 5000, note: 'Exercise delivered 5,000 shares', source: annual(2020, '10039988', 9) },
      { date: '2022-12-29', delta: -1000, note: 'Partial stock sale', source: annual(2022, '10053231', 8) },
      { date: '2022-12-30', delta: -1000, note: 'Second partial stock sale', source: annual(2022, '10053231', 9) },
      { date: '2025-11-14', delta: 0, factor: 10, note: 'Ten-for-one split distributed after the close; 3,000 shares become 30,000', source: 'https://ir.netflix.net/investor-news-and-events/financial-releases/press-release-details/2025/Netflix-Announces-Ten-For-One-Stock-Split/' },
    ],
  },
  RBLX: {
    assumption: 'Assume no opening shares before the disclosed 2021 purchase. The separate 100-call position expired worthless in 2023 and adds no shares.',
    steps: [
      { date: '2021-03-10', delta: 10000, note: 'Purchased 10,000 shares', source: annual(2021, '10047356', 9) },
      { date: '2022-12-28', delta: -5000, note: 'Partial stock sale', source: annual(2022, '10053231', 9) },
    ],
  },
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
  if (p.owner !== 'SP' || (p.ticker === 'AB' ? p.kind !== 'units' : p.kind !== 'stock')) return { status: 'unresolved', shares: null, reason: 'No reviewed starting quantity for this owner and asset type.', steps: [], assumptions: [] };
  // Year-end bracket intersections, reproduced in docs/research/pelosi-annual-ranges.py.
  // Kept distinct from explicit transaction-quantity reconstructions.
  const valueRanges: Record<string, { min: number; max: number; bracket: string }> = {
    AAPL: { min: 26251, max: 91959, bracket: '$5,000,001 - $25,000,000' },
    CRM: { min: 19002, max: 37710, bracket: '$5,000,001 - $25,000,000' },
    AMZN: { min: 26662, max: 113309, bracket: '$5,000,001 - $25,000,000' },
    CMCSA: { min: 16729, max: 33456, bracket: '$500,001 - $1,000,000' },
    GOOGL: { min: 20975, max: 84872, bracket: '$5,000,001 - $25,000,000' },
    V: { min: 14257, max: 71283, bracket: '$5,000,001 - $25,000,000' },
    WBD: { min: 1735, max: 3469, bracket: '$50,001 - $100,000' },
    T: { min: 4026, max: 10064, bracket: '$100,001 - $250,000' },
    CLNE: { min: 477, max: 7142, bracket: '$1,001 - $15,000' },
    IBKR: { min: 7775, max: 15549, bracket: '$500,001 - $1,000,000' },
    MORN: { min: 461, max: 1150, bracket: '$100,001 - $250,000' },
    QCOM: { min: 88, max: 292, bracket: '$15,001 - $50,000' },
  };
  if (valueRanges[p.ticker] && p.shareChange === (['AMZN', 'GOOGL'].includes(p.ticker) ? 5000 : 0) && p.reportedValue === valueRanges[p.ticker].bracket) return {
    status: 'modeled-range', shares: null, range: valueRanges[p.ticker],
    reason: p.ticker === 'GOOGL' ? 'Class A only: the 2025 annual stock bracket divided by the $313 year-end close, plus 5,000 Class A shares exercised January 16, 2026. Earlier Class C shares are not merged into this estimate; their historical reconciliation remains unresolved.' : p.ticker === 'V' ? 'Value-based range from the 2025 annual stock bracket and $350.71 year-end close. This newer snapshot avoids assumptions about the older opening balance and duplicate-looking sales; it does not resolve that historical chain.' : p.ticker === 'WBD' ? 'Value-based range from the 2025 annual stock bracket and $28.82 year-end close. The 2022 spinoff receipt is already before this baseline and is not added again. The merger remained paused in the September 8, 2026 issuer update; this share estimate assumes no subsequent closing and requires review when the merger closes.' : p.ticker === 'AMZN' ? 'Value-based range from the 2025 stock bracket and $230.82 year-end close, plus 5,000 shares from the January 16, 2026 exercise. The newer annual baseline avoids assuming the outcome of the 2022 options; it does not establish an exact balance.' : p.ticker === 'CMCSA' ? 'Value-based range from the 2025 stock bracket and the issuer-confirmed $29.89 year-end close. The Versant spinoff left Comcast shares unchanged; the feed’s 1067:1000 spinoff adjustment is not a share split.' : !['AAPL', 'CRM'].includes(p.ticker) ? 'Value-based share range, not a disclosed share count. Divides the 2025 annual value bracket by the 2025 year-end closing price; no subsequent share change was found in reviewed filings.' : 'Value-based share range, not a disclosed share count. Intersects 2022–2025 annual value brackets at each year-end closing price, carrying subsequent disclosed share changes forward.',
    steps: [],
    assumptions: [
      'Assumes each annual bracket reflects the last trading-day closing price and the same owner and stock class. Other permitted valuation methods can change this range.',
      p.ticker === 'AAPL' ? 'Carries forward net +12,100 shares in 2023, −31,600 in 2024 and −73,582 in 2025. No reviewed 2026 common-share change.' : p.ticker === 'CRM' ? 'No common-share change found in the reviewed 2023–2026 records; separate option sales do not reduce shares.' : 'Uses a single 2025 snapshot, not a multi-year cross-check. Assumes no subsequent share changes beyond those explicitly included in this calculation. Splits before the snapshot are already reflected in its price.',
      'Assumes no missing changes or later corporate actions. This is a model-dependent interval, not a confidence interval or verified current balance.',
    ],
  };
  if (p.ticker === 'WBD') return { status: 'unresolved', shares: null, reason: 'The 2022 filing reports 2,419 shares received in the spinoff. A current total is withheld pending reconciliation of earlier interests and the subsequent corporate transaction.',
    steps: [{ date: '2022-04-11', delta: 2419, note: 'Historical spinoff receipt, not a verified current balance', source: annual(2022, '10053231', 10) }], assumptions: [] };
  const gaps: Record<string, string> = {
    AAPL: 'Historical exercises, sales and gifts have been located, including 73,582 shares sold or donated in 2025. Apple was already held before the reviewed modern transaction chain; the earlier starting balance still needs reconciliation.',
    AMZN: 'The 2020 exercise delivered 3,000 shares, followed by the 20-for-one split, a 20,000-share sale in 2025 and 5,000 exercise shares in 2026. That path implies 45,000 shares, but the outcome of the separate June 2022 option position is not reconciled; it is not assumed to have expired or exercised.',
    GOOGL: 'The Class A chain and the separately disclosed 20,000 Class C exercise shares must be reconciled against later annuals that list Class A only. GOOG is not silently merged into GOOGL.',
    CRM: 'A pre-existing Salesforce holding, the 10,000-share 2020 exercise and 776 shares received in the 2021 Slack merger must be combined. The earlier starting share count remains unresolved; the later option sale does not reduce common shares.',
    CMCSA: 'The Versant spinoff did not reduce Comcast shares. The price feed includes a spinoff adjustment labeled as a split; the unadjusted baseline price must be reconciled before estimating shares.',
    V: 'Visa was already held before the modern transaction history. Earlier holdings, the 2015 split, gifts and sales must be reconciled; similar-looking 2019 sale rows still require identity review.',
  };
  if (gaps[p.ticker]) return { status: 'unresolved', shares: null, reason: gaps[p.ticker], steps: [], assumptions: [] };
  const history = historical[p.ticker];
  if (p.shareChange !== 0 && history?.steps.some(step => step.factor && step.date > '2025-12-31')) {
    return { status: 'unresolved', shares: null, reason: 'New share changes overlap a split after the baseline; dated events must be reconciled before publishing a total.', steps: history.steps, assumptions: [] };
  }
  if (!history && !inferredNewStock.has(p.ticker)) return { status: 'unresolved', shares: null,
    reason: 'The reviewed annual reports give a value range but no opening share count. Later share changes cannot establish the total; no dollar midpoint is converted to shares.', steps: [], assumptions: [] };
  if (!history && p.reportedValue) return { status: 'unresolved', shares: null, reason: 'A baseline stock entry prevents assuming a zero opening balance.', steps: [], assumptions: [] };
  const steps = [...(history?.steps || [])];
  // The position ledger supplies the already-reconciled subsequent net change.
  const base = replayShareHistory(steps);
  const shares = base === null ? -1 : base + p.shareChange;
  if (shares < 0) return { status: 'unresolved', shares: null, reason: 'Reviewed changes exceed the assumed opening balance.', steps, assumptions: [] };
  assumptions.unshift(history?.assumption || 'Assume no starting common-stock position because the 2025 annual has no stock entry for this asset. Any unreported or below-threshold opening balance would change this estimate.');
  return { status: 'estimated', shares, reason: 'Explicit share changes, combined with the stated opening-balance assumption.', steps, assumptions };
}
export const pelosiQuantityBaselineSources = [
  { label: '2016 annual · Square option opening', url: annual(2016, '10015814', 5) },
  { label: 'Dropbox · reverse split predates the purchase', url: 'https://investors.dropbox.com/node/8226/html' },
  { label: 'Block · SQ to XYZ ticker continuity', url: 'https://investors.block.xyz/investor-news/news-details/2025/Block-Announces-Ticker-Symbol-Change-to-XYZ-To-Report-Fourth-Quarter-Results/default.aspx' },
  { label: 'American Express · stock split history', url: 'https://ir.americanexpress.com/resources/faq/default.aspx' },
  { label: '2019 annual · Netflix options and opening inventory', url: annual(2019, '10035243', 4) },
  { label: '2020 annual · Microsoft options, no common-stock entry', url: annual(2020, '10039988', 4) },
  { label: '2021 annual · Microsoft exercises', url: annual(2021, '10047356', 8) },
  { label: '2021 annual · Roblox purchase', url: annual(2021, '10047356', 9) },
  { label: '2022 annual · NVIDIA stock value None', url: annual(2022, '10053231', 5) },
  { label: '2023 annual · NVIDIA options / starting holdings', url: annual(2023, '10059734', 4) },
  { label: '2024 annual · Broadcom options', url: annual24(2) },
  { label: 'Reviewed split adjustments', url: 'https://infomemo.theocc.com/infomemos?number=54623' },
];

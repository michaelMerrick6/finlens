/** A disclosure-following model, never a reconstruction of personal returns.
 * Rebalance at the close of the first verified session after publication.
 * Adjusted closes model reinvested distributions; raw closes value exact shares.
 * Callers must review security identities, amendments and corporate actions.
 */
export type ModelHolding = {
  securityId: string;
  basis: { type: 'range'; low: number; high: number | null }
    | { type: 'shares'; shares: number };
};
export type DisclosureSnapshot = {
  id: string;
  publishedOn: string | null;
  complete: boolean;
  holdings: ModelHolding[];
};
export type ModelPrices = {
  currency: 'USD';
  corporateActionsReviewed: boolean;
  points: { date: string; close: number; adjustedClose: number }[];
};
export type BacktestInput = {
  snapshots: DisclosureSnapshot[];
  prices: Record<string, ModelPrices>;
  calendar: { verified: boolean; startDate: string; endDate: string; sessions: string[] };
  historyComplete: boolean;
  throughDate: string;
  initialValue?: number;
};
export type PortfolioPoint = { date: string; value: number; snapshotId: string };
export type ModelYear = {
  year: number;
  startDate: string;
  endDate: string;
  returnPercent: number;
  partial: boolean;
};
export type BacktestResult = {
  status: 'ready' | 'blocked';
  issues: string[];
  points: PortfolioPoint[];
  years: ModelYear[];
};
const validDate = (date: string) => /^\d{4}-\d{2}-\d{2}$/.test(date)
  && Number.isFinite(Date.parse(date)) && new Date(date).toISOString().slice(0, 10) === date;
const positive = (value: number) => Number.isFinite(value) && value > 0;
const blocked = (issues: string[]): BacktestResult => ({ status: 'blocked', issues, points: [], years: [] });

export function runDisclosureBacktest(input: BacktestInput): BacktestResult {
  const { calendar, throughDate, snapshots } = input;
  const initialValue = input.initialValue ?? 10000;
  if (!input.historyComplete) return blocked(['Disclosure history has not been fully reviewed.']);
  if (!validDate(throughDate) || !positive(initialValue)) return blocked(['Invalid model end date or initial value.']);
  if (!calendar.verified || !validDate(calendar.startDate) || !validDate(calendar.endDate)
    || calendar.startDate > throughDate || calendar.endDate < throughDate
    || !calendar.sessions.length || calendar.sessions.some((date, index) => !validDate(date)
      || date < calendar.startDate || date > calendar.endDate || (index > 0 && date <= calendar.sessions[index - 1]))) {
    return blocked(['A verified, ordered market calendar covering the model period is required.']);
  }
  if (!snapshots.length) return blocked(['No reviewed disclosure snapshots are available.']);
  const issues: string[] = [];
  const ids = new Set<string>();
  const publicationDates = new Set<string>();
  for (const snapshot of snapshots) {
    if (!snapshot.id || ids.has(snapshot.id)) issues.push('Snapshot IDs must be unique.');
    ids.add(snapshot.id);
    if (!snapshot.publishedOn || !validDate(snapshot.publishedOn)) {
      issues.push(`${snapshot.id}: public availability date is unverified.`);
      continue;
    }
    if (snapshot.publishedOn > throughDate) continue;
    if (publicationDates.has(snapshot.publishedOn)) issues.push(`${snapshot.id}: reconcile reports published on the same date into one snapshot.`);
    publicationDates.add(snapshot.publishedOn);
    if (!snapshot.complete) issues.push(`${snapshot.id}: eligible holdings have not been fully reviewed.`);
    if (snapshot.publishedOn < calendar.startDate) issues.push(`${snapshot.id}: market calendar starts after publication.`);
    const securities = new Set<string>();
    for (const holding of snapshot.holdings) {
      if (!holding.securityId || securities.has(holding.securityId)) issues.push(`${snapshot.id}: aggregate duplicate security identities before modeling.`);
      securities.add(holding.securityId);
      const basis = holding.basis;
      if (basis.type === 'shares' ? !positive(basis.shares)
        : basis.high === null || !positive(basis.low) || !positive(basis.high) || basis.high < basis.low) {
        issues.push(`${snapshot.id}: ${holding.securityId} has an unresolved holding value.`);
      }
    }
  }
  if (issues.length) return blocked(issues);
  const ordered = snapshots.filter(s => s.publishedOn! <= throughDate)
    .sort((a, b) => a.publishedOn!.localeCompare(b.publishedOn!));
  const start = calendar.sessions.find(date => ordered.length && date > ordered[0].publishedOn! && date <= throughDate);
  if (!start) return blocked(['No completed market session follows the first disclosure yet.']);
  const sessions = calendar.sessions.filter(date => date >= start && date <= throughDate);
  const rebalances = new Map<string, DisclosureSnapshot>();
  for (const snapshot of ordered) {
    const session = sessions.find(date => date > snapshot.publishedOn!);
    // When several disclosures precede the same opening, use the latest
    // reconciled snapshot. Superseded portfolios are never bought.
    if (session) rebalances.set(session, snapshot);
  }
  const lookup = new Map<string, Map<string, ModelPrices['points'][number]>>();
  // Build only series used during the modeled period. Extra/future securities
  // must not make an otherwise valid period appear incomplete.
  for (const snapshot of rebalances.values()) {
    for (const { securityId } of snapshot.holdings) {
      if (lookup.has(securityId)) continue;
      const series = input.prices[securityId];
      if (!series || series.currency !== 'USD' || !series.corporateActionsReviewed) {
        issues.push(`${securityId}: reviewed USD prices and corporate actions are required.`);
        continue;
      }
      const points = new Map<string, ModelPrices['points'][number]>();
      for (const point of series.points) {
        if (!validDate(point.date) || points.has(point.date) || !positive(point.close) || !positive(point.adjustedClose)) {
          issues.push(`${securityId}: invalid or duplicate daily prices.`);
          break;
        }
        points.set(point.date, point);
      }
      lookup.set(securityId, points);
    }
  }
  if (issues.length) return blocked(issues);
  let units = new Map<string, number>();
  let cash = initialValue;
  let active: DisclosureSnapshot | undefined;
  const points: PortfolioPoint[] = [];
  for (const date of sessions) {
    let value = cash;
    // Value the old portfolio first, so a rebalance keeps that day's gains.
    for (const [securityId, quantity] of units) {
      const price = lookup.get(securityId)?.get(date);
      if (!price) return blocked([`${securityId}: missing adjusted close on ${date}.`]);
      value += quantity * price.adjustedClose;
    }
    const replacement = rebalances.get(date);
    if (replacement) {
      const values: { securityId: string; value: number; adjustedClose: number }[] = [];
      for (const holding of replacement.holdings) {
        const price = lookup.get(holding.securityId)?.get(date);
        if (!price) return blocked([`${holding.securityId}: missing rebalance price on ${date}.`]);
        const basis = holding.basis;
        const amount = basis.type === 'shares' ? basis.shares * price.close : basis.low / 2 + basis.high! / 2;
        if (!positive(amount)) return blocked([`${holding.securityId}: invalid modeled position value.`]);
        values.push({ securityId: holding.securityId, value: amount, adjustedClose: price.adjustedClose });
      }
      const total = values.reduce((sum, holding) => sum + holding.value, 0);
      if (values.length && !positive(total)) return blocked([`${replacement.id}: invalid total allocation value.`]);
      units = new Map(values.map(holding => [holding.securityId, value * (holding.value / total) / holding.adjustedClose]));
      // An explicitly reviewed empty stock snapshot holds model cash at 0%
      // until the next disclosure. This is not an assumed personal cash balance.
      cash = values.length ? 0 : value;
      active = replacement;
    }
    if (!Number.isFinite(value) || value <= 0 || !active) return blocked(['Portfolio valuation is invalid.']);
    points.push({ date, value, snapshotId: active.id });
  }
  const years: ModelYear[] = [];
  const firstYear = Number(start.slice(0, 4));
  for (let year = firstYear; year <= Number(throughDate.slice(0, 4)); year++) {
    const end = points.findLast(point => Number(point.date.slice(0, 4)) === year);
    if (!end) continue;
    const beginning = year === firstYear ? points[0] : points.findLast(point => point.date < `${year}-01-01`);
    if (!beginning) return blocked([`${year}: preceding year-end value is missing.`]);
    years.push({ year, startDate: beginning.date, endDate: end.date,
      returnPercent: (end.value / beginning.value - 1) * 100,
      partial: year === firstYear || throughDate < `${year}-12-31` });
  }
  return { status: 'ready', issues: [], points, years };
}

// Reviewed pilots only. The machine-parsed Congress inventory is not published here.
export type ReviewedBaseline = {
  memberId: string; name: string; first: string; last: string; district: string;
  date: string; filed: string; docId: string; source: string; sha256: string; reviewed: string;
  annualType: string; reviewNote: string;
  positions: { ticker: string; name: string; min: number; max: number }[];
  otherAssets: { name: string; kind: string; min: number; max: number }[];
};
export const crockettBaseline: ReviewedBaseline = {
  memberId: 'C001130', name: 'Jasmine Crockett', first: 'Jasmine', last: 'Crockett', district: 'TX30',
  date: '2025-12-31', filed: '2026-08-12', docId: '10074813',
  source: 'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10074813.pdf',
  sha256: '70a9effc3b2ee33c48d5ea2c5db707bda38273d60ca7de15ab88cabc637196b7',
  reviewed: '2026-09-17', annualType: 'O',
  reviewNote: 'Schedule A lists exactly three stocks; Schedule B reports no transactions for 2025. Positions in private organizations on Schedule E are roles, not valued stock holdings, and are not added to this total.',
  otherAssets: [],
  positions: [
    { ticker: 'DVN', name: 'Devon Energy', min: 1001, max: 15000 },
    { ticker: 'MGM', name: 'MGM Resorts', min: 1001, max: 15000 },
    { ticker: 'MRNA', name: 'Moderna', min: 1001, max: 15000 },
  ],
};

export const massieBaseline: ReviewedBaseline = {
  memberId: 'M001184', name: 'Thomas Massie', first: 'Thomas', last: 'Massie', district: 'KY04',
  date: '2025-12-31', filed: '2026-05-15', docId: '10077444', annualType: 'O',
  source: 'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10077444.pdf',
  sha256: 'eea9811015030f89f0da5512ad43bd227e13062080e32a38d724ac4770843fb1',
  reviewed: '2026-09-17',
  reviewNote: 'Schedule A lists one public stock and four other assets, all shown here. Schedule B reports no transactions for 2025. Schedule D lists mortgages, which are not subtracted from the stock value.',
  positions: [{ ticker: 'TSLA', name: 'Tesla', min: 15001, max: 50000 }],
  otherAssets: [
    { name: 'Federal Credit Union', kind: 'Bank account', min: 15001, max: 50000 },
    { name: 'Howard Massie Farms, LLC, 50% interest', kind: 'Farm interest', min: 1000001, max: 5000000 },
    { name: "People's Bank", kind: 'Bank account', min: 15001, max: 50000 },
    { name: 'Shireware, LLC, 33% interest', kind: 'Private company interest', min: 1001, max: 15000 },
  ],
};
export const reviewedBaselines: Record<string, ReviewedBaseline> = {
  [crockettBaseline.memberId]: crockettBaseline,
  [massieBaseline.memberId]: massieBaseline,
};
export function getReviewedBaseline(memberId: string): ReviewedBaseline | undefined {
  return Object.hasOwn(reviewedBaselines, memberId) ? reviewedBaselines[memberId] : undefined;
}
function indexDate(value: string) {
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value);
  return match ? `${match[3]}-${match[1].padStart(2, '0')}-${match[2].padStart(2, '0')}` : null;
}

export function checkReviewedIndex(text: string, baseline: ReviewedBaseline) {
  const lines = text.replace(/^\uFEFF/, '').trim().split(/\r?\n/);
  const headers = lines.shift()?.split('\t') ?? [];
  const required = ['First', 'Last', 'StateDst', 'DocID', 'FilingType', 'FilingDate'];
  if (!required.every(h => headers.includes(h)) || !lines.length) throw new Error('Invalid House index');
  let foundBaseline = false;
  const pending: string[] = [];
  for (const line of lines) {
    const values = line.split('\t');
    if (values.length !== headers.length) throw new Error('Incomplete House index row');
    const row = Object.fromEntries(headers.map((h, i) => [h, values[i].trim()]));
    if (row.Last.toUpperCase() !== baseline.last.toUpperCase()) continue;
    if (row.First.toUpperCase() !== baseline.first.toUpperCase()) {
      if (row.StateDst === baseline.district) pending.push(row.DocID);
      continue;
    }
    if (row.DocID === baseline.docId) {
      if (row.StateDst !== baseline.district || row.FilingType !== baseline.annualType || indexDate(row.FilingDate) !== baseline.filed) throw new Error('Baseline index changed');
      foundBaseline = true;
    } else if (row.FilingType !== 'X') {
      // New annuals, amendments and PTRs require source review, even if backdated.
      pending.push(row.DocID);
    }
  }
  return { foundBaseline, pending };
}

export function checkCrockettIndex(text: string) { return checkReviewedIndex(text, crockettBaseline); }

export type DailyPrice = { date: string; close: number };
export function modelUnchangedHolding(position: { min: number; max: number }, prices: DailyPrice[], asOf: string, baselineDate = crockettBaseline.date) {
  // Yahoo quote.close is split-adjusted but not dividend-adjusted. Using the same
  // series at both endpoints expresses quantities on today's split basis once.
  const valid = prices.filter(p => Number.isFinite(p.close) && p.close > 0 && p.date < asOf).sort((a,b) => a.date.localeCompare(b.date));
  const baseline = valid.filter(p => p.date <= baselineDate).at(-1);
  const latest = valid.at(-1);
  if (!baseline || !latest || latest.date < baselineDate ||
      Date.parse(baselineDate) - Date.parse(baseline.date) > 7 * 86400000 ||
      Date.parse(asOf) - Date.parse(latest.date) > 5 * 86400000) return null;
  const minShares = position.min / baseline.close, maxShares = position.max / baseline.close;
  return { minShares, maxShares, min: minShares * latest.close, max: maxShares * latest.close,
    midpoint: (minShares + maxShares) / 2 * latest.close, baseline, latest };
}

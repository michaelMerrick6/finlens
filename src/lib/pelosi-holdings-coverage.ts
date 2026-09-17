import 'server-only';
import { pelosiHoldingsCoverage } from './pelosi-holdings';

// An index match is a freshness check, not proof of complete public disclosure.
export async function checkPelosiHoldingsCoverage() {
  // A new reporting year requires an expanded inventory and reviewed ledger.
  if (new Date().getUTCFullYear() > 2026) return 'review-needed' as const;
  try {
    const response = await fetch('https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2026FD.txt', {
      next: { revalidate: 3600 }, signal: AbortSignal.timeout(6000),
    });
    if (!response.ok) throw new Error('Index unavailable');
    return comparePelosiIndex(await response.text());
  } catch { return 'unavailable' as const; }
}
export function comparePelosiIndex(text: string): 'matched' | 'review-needed' | 'unavailable' {
  const [header, ...lines] = text.replace(/^\uFEFF/, '').trim().split(/\r?\n/);
  const columns = header.split('\t');
  if (!['Last', 'First', 'DocID', 'FilingType'].every(c => columns.includes(c))) return 'unavailable';
  const rows = lines.map(line => Object.fromEntries(line.split('\t').map((value, i) => [columns[i], value.trim()])));
  const filings = rows.filter(r => r.Last?.toUpperCase() === 'PELOSI' && r.First?.toUpperCase() === 'NANCY');
  if (!filings.length) return 'unavailable';
  const relevant = filings.filter(r => ['P', 'A', 'O'].includes(r.FilingType));
  return relevant.length === pelosiHoldingsCoverage.reviewedDocuments.length &&
    relevant.every(r => pelosiHoldingsCoverage.reviewedDocuments.includes(r.DocID)) ? 'matched' : 'review-needed';
}

import type { ReviewedPosition } from './pelosi-holdings';

export function rankPelosiHoldings(positions: ReviewedPosition[]) {
  const rows = positions.filter(p => p.kind === 'stock' || p.kind === 'units').map(position => {
    const match = position.reportedValue?.match(/^\$([\d,]+)\s*-\s*\$([\d,]+)$/);
    return { position, min: match ? Number(match[1].replaceAll(',', '')) : null,
      max: match ? Number(match[2].replaceAll(',', '')) : null, rank: null as number | null };
  }).sort((a, b) => (b.min ?? -1) - (a.min ?? -1) || (b.max ?? -1) - (a.max ?? -1) || a.position.ticker.localeCompare(b.position.ticker));
  let rank = 0;
  rows.forEach((row, i) => {
    if (row.min === null) return;
    if (i === 0 || row.min !== rows[i - 1].min || row.max !== rows[i - 1].max) rank = i + 1;
    row.rank = rank;
  });
  return rows;
}
export function compactHoldingRange(min: number, max: number) {
  const format = (n: number) => n >= 1e6 ? `$${n / 1e6}M` : `$${n / 1e3}K`;
  return `${format(min - 1)}–${format(max)}`;
}

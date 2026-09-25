/** Probe once for small results, then read four ordered pages at a time.
 * Never publish partial results after a failed request or a scan limit.
 */
export async function readCompleteRows<T>(fetchRows: (offset: number, count: number) => Promise<T[]>, limit = 50_000) {
  const size = 500;
  if (!Number.isInteger(limit) || limit < size || limit % size) throw new Error('Invalid scan limit');
  const rows = await fetchRows(0, size);
  if (rows.length < size) return rows;
  for (let offset = size; offset < limit; offset += size * 4) {
    const offsets = Array.from({ length: Math.min(4, (limit - offset) / size) }, (_, i) => offset + i * size);
    const pages = await Promise.all(offsets.map(start => fetchRows(start, size)));
    for (const page of pages) {
      rows.push(...page);
      if (page.length < size) return rows;
    }
  }
  throw new Error('History exceeds safe scan limit');
}

/** Read a bounded history in small pages, with one extra row to detect truncation. */
export async function readBoundedRows<T>(limit: number, fetchRows: (offset: number, count: number) => Promise<T[]>) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 5000) throw new Error('Invalid history limit');
  const rows: T[] = [];
  while (rows.length <= limit) {
    const count = Math.min(500, limit + 1 - rows.length);
    const batch = await fetchRows(rows.length, count);
    rows.push(...batch);
    if (batch.length < count) break;
  }
  return { rows: rows.slice(0, limit), hasMore: rows.length > limit, rowLimit: limit };
}

/** Paginate by source-row position even when application rules discard rows. */
export async function readFilteredPage<T>({
  offset,
  limit,
  fetchRows,
  include,
}: {
  offset: number;
  limit: number;
  fetchRows: (offset: number, count: number) => Promise<T[]>;
  include: (row: T) => boolean;
}) {
  const rows: T[] = [];
  const batchSize = Math.max(limit + 1, 50);
  let cursor = offset;

  // Bound work per request. A sparse page can continue from nextOffset.
  for (let batch = 0; batch < 10; batch += 1) {
    const source = await fetchRows(cursor, batchSize);
    for (const row of source) {
      if (include(row)) {
        if (rows.length === limit) {
          return { rows, hasMore: true, nextOffset: cursor };
        }
        rows.push(row);
      }
      cursor += 1;
    }
    if (source.length < batchSize) {
      return { rows, hasMore: false, nextOffset: cursor };
    }
  }
  return { rows, hasMore: true, nextOffset: cursor };
}

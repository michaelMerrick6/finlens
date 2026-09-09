import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import ts from 'typescript';

const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync('src/lib/filtered-page.ts', 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText, { exports });
const { readFilteredPage } = exports;

test('scans past discarded rows and resumes without skipping or duplicating trades', async () => {
  const source = Array.from({ length: 130 }, (_, id) => ({ id, valid: id % 7 === 0 }));
  const seen = [];
  let offset = 0;
  for (;;) {
    const page = await readFilteredPage({ offset, limit: 3,
      fetchRows: async (start, count) => source.slice(start, start + count),
      include: (row) => row.valid });
    seen.push(...page.rows.map((row) => row.id));
    if (!page.hasMore) break;
    assert.ok(page.nextOffset > offset);
    offset = page.nextOffset;
  }
  assert.deepEqual(seen, source.filter((row) => row.valid).map((row) => row.id));
});

test('an exact full page at the end does not promise another page', async () => {
  const page = await readFilteredPage({ offset: 0, limit: 2,
    fetchRows: async () => [1, 2], include: () => true });
  assert.equal(page.hasMore, false);
  assert.equal(page.nextOffset, 2);
});

test('bounds scanning through invalid records and supplies a continuation cursor', async () => {
  let calls = 0;
  const page = await readFilteredPage({ offset: 10, limit: 20,
    fetchRows: async (_, count) => { calls += 1; return Array(count).fill(null); },
    include: () => false });
  assert.equal(calls, 10);
  assert.equal(page.rows.length, 0);
  assert.equal(page.hasMore, true);
  assert.equal(page.nextOffset, 510);
});

test('database failures propagate instead of looking like an empty feed', async () => {
  await assert.rejects(readFilteredPage({ offset: 0, limit: 20,
    fetchRows: async () => { throw new Error('Database unavailable'); }, include: () => true }), /Database unavailable/);
});

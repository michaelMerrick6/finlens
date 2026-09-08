import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';
import ts from 'typescript';

const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync('src/lib/auth-return-path.ts', 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.CommonJS },
}).outputText, { exports, URL });
const { authReturnPath } = exports;

test('returns to the selected workspace and preserves the pending follow', () => {
  const path = '/dashboard?memberId=P000197&memberName=Nancy+Pelosi&follow=1';
  assert.equal(authReturnPath(path), path);
});

test('rejects external redirects and authentication loops', () => {
  for (const path of [null, '', 'https://example.com', '//example.com', '/\\example.com', '/\n/example.com', '/auth', '/auth/callback']) {
    assert.equal(authReturnPath(path), '/dashboard');
  }
});

test('retains internal filters but drops fragments', () => {
  assert.equal(authReturnPath('/politicians?q=NVDA#row'), '/politicians?q=NVDA');
});

test('recovery links open password reset instead of the dashboard', () => {
  assert.equal(exports.authCallbackPath('/dashboard', '1'), '/auth/reset');
  assert.equal(exports.authCallbackPath('//example.com', null), '/dashboard');
  assert.equal(exports.authCallbackPath('/dashboard?follow=1', null), '/dashboard?follow=1');
});

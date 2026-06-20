import 'server-only';

import { promises as fs } from 'fs';
import path from 'path';

const SIGNAL_POLICY_PATH = path.join(process.cwd(), 'config', 'signal-policy.json');
const TRUE_ENV_VALUES = new Set(['1', 'true', 'yes', 'on']);

function normalizeEnvValue(value: string | undefined) {
  return value?.trim().toLowerCase() ?? '';
}

export type SignalPolicy = Record<string, unknown>;

function assertSignalPolicyShape(value: unknown): asserts value is SignalPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Signal policy must be a JSON object.');
  }
}

export function getSignalPolicyPath() {
  return SIGNAL_POLICY_PATH;
}

export function signalPolicyWritesEnabled() {
  const explicit = normalizeEnvValue(process.env.VAIL_ALLOW_POLICY_FILE_WRITES);
  if (explicit) {
    return TRUE_ENV_VALUES.has(explicit);
  }

  return process.env.NODE_ENV !== 'production';
}

export function signalPolicyWritesDisabledMessage() {
  return 'Signal policy file writes are disabled in this environment. Set VAIL_ALLOW_POLICY_FILE_WRITES=1 only for local or single-host deployments.';
}

export async function readSignalPolicyText() {
  return fs.readFile(SIGNAL_POLICY_PATH, 'utf8');
}

export async function readSignalPolicy(): Promise<SignalPolicy> {
  const raw = await readSignalPolicyText();
  const parsed = JSON.parse(raw) as unknown;
  assertSignalPolicyShape(parsed);
  return parsed;
}

export async function writeSignalPolicyFromText(rawText: string) {
  if (!signalPolicyWritesEnabled()) {
    throw new Error(signalPolicyWritesDisabledMessage());
  }

  const parsed = JSON.parse(rawText) as unknown;
  assertSignalPolicyShape(parsed);
  const normalized = `${JSON.stringify(parsed, null, 2)}\n`;
  await fs.writeFile(SIGNAL_POLICY_PATH, normalized, 'utf8');
  return parsed;
}

export type TickerSearchAlias = { ticker: string; companyName: string; aliases: string[] };
export type PoliticianAlias = { actorKey: string; aliases: string[] };
export type InsiderAlias = { canonicalName: string; aliases: string[] };

export type SearchAliases = {
  tickerAliases: TickerSearchAlias[];
  politicianAliases: PoliticianAlias[];
  insiderAliases: InsiderAlias[];
};

export async function readSearchAliases(): Promise<SearchAliases> {
  const policy = await readSignalPolicy();
  const searchAliases = (policy.search_aliases || {}) as Record<string, unknown>;
  return {
    tickerAliases: (Array.isArray(searchAliases.ticker_aliases) ? searchAliases.ticker_aliases : []) as TickerSearchAlias[],
    politicianAliases: (Array.isArray(searchAliases.politician_aliases) ? searchAliases.politician_aliases : []) as PoliticianAlias[],
    insiderAliases: (Array.isArray(searchAliases.insider_aliases) ? searchAliases.insider_aliases : []) as InsiderAlias[],
  };
}

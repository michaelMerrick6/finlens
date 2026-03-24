import 'server-only';

import { promises as fs } from 'fs';
import path from 'path';

const SIGNAL_POLICY_PATH = path.join(process.cwd(), 'config', 'signal-policy.json');

export type SignalPolicy = Record<string, unknown>;

function assertSignalPolicyShape(value: unknown): asserts value is SignalPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Signal policy must be a JSON object.');
  }
}

export function getSignalPolicyPath() {
  return SIGNAL_POLICY_PATH;
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
  const parsed = JSON.parse(rawText) as unknown;
  assertSignalPolicyShape(parsed);
  const normalized = `${JSON.stringify(parsed, null, 2)}\n`;
  await fs.writeFile(SIGNAL_POLICY_PATH, normalized, 'utf8');
  return parsed;
}

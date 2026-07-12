const TRUE_ENV_VALUES = new Set(['1', 'true', 'yes', 'on']);

function normalizeEnvValue(value: string | undefined) {
  return value?.trim().toLowerCase() ?? '';
}

export function isOpsEnabled() {
  const explicitValue = normalizeEnvValue(process.env.VAIL_ENABLE_OPS);
  if (explicitValue) {
    return TRUE_ENV_VALUES.has(explicitValue);
  }

  return process.env.NODE_ENV !== 'production';
}

export function getOpsBasicAuthConfig() {
  const username = process.env.VAIL_OPS_BASIC_AUTH_USERNAME?.trim() || '';
  const password = process.env.VAIL_OPS_BASIC_AUTH_PASSWORD?.trim() || '';

  if (!username || !password) {
    return null;
  }

  return { username, password };
}

export function secretsMatch(actual: string, expected: string) {
  const length = Math.max(actual.length, expected.length);
  let mismatch = actual.length ^ expected.length;

  for (let index = 0; index < length; index += 1) {
    mismatch |= (actual.charCodeAt(index) || 0) ^ (expected.charCodeAt(index) || 0);
  }

  return mismatch === 0;
}

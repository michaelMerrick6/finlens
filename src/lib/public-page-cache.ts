// Small, short-lived browser cache for public pages only. Never store account data.
const entries = new Map<string, { value: unknown; expires: number }>();
export function readPublicPage<T>(key: string): T | undefined {
  const entry = entries.get(key);
  if (!entry || entry.expires <= Date.now()) { entries.delete(key); return undefined; }
  return entry.value as T;
}
export function storePublicPage(key: string, value: unknown) {
  if (typeof window === 'undefined') return;
  if (entries.size >= 20) entries.delete(entries.keys().next().value!);
  entries.set(key, { value, expires: Date.now() + 60_000 });
}

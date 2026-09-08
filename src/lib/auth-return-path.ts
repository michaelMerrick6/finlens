/** Accept only paths within this app, including their query parameters. */
export function authReturnPath(value: string | null) {
  if (!value || !value.startsWith('/') || value.startsWith('//') || /[\\\u0000-\u001f]/.test(value)) return '/dashboard';
  try {
    const url = new URL(value, 'https://vail.invalid');
    if (url.origin !== 'https://vail.invalid' || url.pathname.startsWith('/auth')) return '/dashboard';
    return url.pathname + url.search;
  } catch {
    return '/dashboard';
  }
}

export function authCallbackPath(next: string | null, recovery: string | null) {
  return recovery === '1' ? '/auth/reset' : authReturnPath(next);
}

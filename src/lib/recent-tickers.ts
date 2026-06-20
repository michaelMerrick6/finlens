const RECENT_KEY = 'vail_recent_tickers';
const RECENT_EVENT = 'vail:recent-tickers-changed';

function notifyRecentTickersChanged() {
  if (typeof window === 'undefined') return;
  window.dispatchEvent(new Event(RECENT_EVENT));
}

export function addRecentTicker(ticker: string) {
  if (typeof window === 'undefined') return;
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    const existing: string[] = raw ? JSON.parse(raw) : [];
    const updated = [ticker, ...existing.filter((value) => value !== ticker)].slice(0, 6);
    localStorage.setItem(RECENT_KEY, JSON.stringify(updated));
    notifyRecentTickersChanged();
  } catch {}
}

export function readRecentTickers(): string[] {
  if (typeof window === 'undefined') {
    return [];
  }

  try {
    const raw = localStorage.getItem(RECENT_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export function subscribeRecentTickers(onChange: () => void) {
  if (typeof window === 'undefined') {
    return () => {};
  }

  const handleStorage = (event: StorageEvent) => {
    if (event.key === null || event.key === RECENT_KEY) {
      onChange();
    }
  };

  window.addEventListener(RECENT_EVENT, onChange);
  window.addEventListener('storage', handleStorage);

  return () => {
    window.removeEventListener(RECENT_EVENT, onChange);
    window.removeEventListener('storage', handleStorage);
  };
}

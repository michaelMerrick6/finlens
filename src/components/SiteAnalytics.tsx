'use client';

import { useEffect } from 'react';
import { usePathname } from 'next/navigation';

const VISITOR_KEY = 'vail.analytics.visitor';
const SESSION_KEY = 'vail.analytics.session';
const ATTRIBUTION_KEY = 'vail.analytics.attribution';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

type Attribution = {
  referrer: string;
  utmSource: string;
  utmMedium: string;
  utmCampaign: string;
};

function randomId() {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (character) => {
    const random = Math.floor(Math.random() * 16);
    const value = character === 'x' ? random : (random & 0x3) | 0x8;
    return value.toString(16);
  });
}

function storedId(storage: Storage, key: string) {
  try {
    const existing = storage.getItem(key);
    if (existing && UUID_PATTERN.test(existing)) return existing;
    const created = randomId();
    storage.setItem(key, created);
    return created;
  } catch {
    return randomId();
  }
}

function sessionAttribution(): Attribution {
  try {
    const existing = sessionStorage.getItem(ATTRIBUTION_KEY);
    if (existing) return JSON.parse(existing) as Attribution;
  } catch {
    // A fresh attribution record is used when browser storage is unavailable.
  }

  const params = new URLSearchParams(window.location.search);
  const attribution = {
    referrer: document.referrer || '',
    utmSource: params.get('utm_source') || '',
    utmMedium: params.get('utm_medium') || '',
    utmCampaign: params.get('utm_campaign') || '',
  };

  try {
    sessionStorage.setItem(ATTRIBUTION_KEY, JSON.stringify(attribution));
  } catch {
    // Tracking still works without browser storage; this only affects attribution continuity.
  }
  return attribution;
}

export default function SiteAnalytics() {
  const pathname = usePathname();

  useEffect(() => {
    if (!pathname || pathname.startsWith('/ops') || pathname.startsWith('/api')) return;
    const globalPrivacyControl = (navigator as Navigator & { globalPrivacyControl?: boolean }).globalPrivacyControl;
    if (navigator.doNotTrack === '1' || globalPrivacyControl) return;

    const visitorId = storedId(localStorage, VISITOR_KEY);
    const sessionId = storedId(sessionStorage, SESSION_KEY);
    const attribution = sessionAttribution();
    const body = JSON.stringify({
      visitorId,
      sessionId,
      path: pathname,
      ...attribution,
    });

    void fetch('/api/analytics/page-view', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body,
      keepalive: true,
    }).catch(() => undefined);
  }, [pathname]);

  return null;
}

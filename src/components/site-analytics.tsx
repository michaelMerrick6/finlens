'use client';

import { useAccount } from './account-provider';
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
  const { session } = useAccount();
  useEffect(() => {
    if (!pathname || /^\/(hq|ops|api)(\/|$)/.test(pathname) || ['localhost','127.0.0.1'].includes(location.hostname) || navigator.doNotTrack === '1' || (navigator as Navigator & {globalPrivacyControl?:boolean}).globalPrivacyControl) return;
    const visitorId = storedId(localStorage, VISITOR_KEY);
    // Engagement windows are separate from the older page-view session identifier.
    const sessionId = crypto.randomUUID();
    let last = performance.now();
    let lastInput = last;
    const active = () => { lastInput = performance.now(); };
    const send = () => {
      const now = performance.now();
      const seconds = document.visibilityState === 'visible' && now-lastInput<60000 ? Math.min(30,Math.floor((now-last)/1000)) : 0;
      last = now;
      if (!seconds) return;
      void fetch('/api/analytics/engagement',{method:'POST',headers:{'content-type':'application/json',...(session?{Authorization:`Bearer ${session.access_token}`}:{})},body:JSON.stringify({visitorId,sessionId,seconds}),keepalive:true}).catch(()=>{});
    };
    const visibility = () => { last=performance.now(); };
    window.addEventListener('pointerdown',active);window.addEventListener('keydown',active);window.addEventListener('scroll',active,{passive:true});
    document.addEventListener('visibilitychange',visibility);
    const timer=window.setInterval(send,15000);
    return()=>{send();clearInterval(timer);window.removeEventListener('pointerdown',active);window.removeEventListener('keydown',active);window.removeEventListener('scroll',active);document.removeEventListener('visibilitychange',visibility);};
  },[pathname,session]);

  useEffect(() => {
    if (!pathname || pathname.startsWith('/hq') || pathname.startsWith('/ops') || pathname.startsWith('/api')) return;
    if (['localhost', '127.0.0.1'].includes(window.location.hostname)) return;
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

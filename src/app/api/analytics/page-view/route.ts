import { createHmac } from 'node:crypto';

import { NextRequest, NextResponse } from 'next/server';

import { getAdminSupabase } from '@/lib/supabase-admin';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const BOT_PATTERN = /bot|crawler|spider|crawling|headless|preview|facebookexternalhit|slurp|bingpreview|uptime|monitor/i;

type PageViewPayload = {
  visitorId?: unknown;
  sessionId?: unknown;
  path?: unknown;
  referrer?: unknown;
  utmSource?: unknown;
  utmMedium?: unknown;
  utmCampaign?: unknown;
};

function cleanText(value: unknown, maxLength: number) {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().replace(/[\u0000-\u001f\u007f]/g, '');
  return normalized ? normalized.slice(0, maxLength) : null;
}

function cleanPath(value: unknown) {
  const path = cleanText(value, 500);
  if (!path || !path.startsWith('/') || path.startsWith('//')) return null;
  return path.split('?')[0].split('#')[0] || '/';
}

function safeDecode(value: string | null) {
  if (!value) return null;
  try {
    return decodeURIComponent(value).slice(0, 120);
  } catch {
    return value.slice(0, 120);
  }
}

function requestHost(request: NextRequest) {
  return (
    request.headers.get('x-forwarded-host') ||
    request.headers.get('host') ||
    request.nextUrl.host
  ).split(',')[0].trim().toLowerCase();
}

function isSameOrigin(request: NextRequest) {
  const origin = request.headers.get('origin');
  if (!origin) return true;

  try {
    return new URL(origin).host.toLowerCase() === requestHost(request);
  } catch {
    return false;
  }
}

function sanitizeReferrer(value: unknown) {
  const raw = cleanText(value, 1000);
  if (!raw) return { url: null, host: null };

  try {
    const parsed = new URL(raw);
    if (!['http:', 'https:'].includes(parsed.protocol)) return { url: null, host: null };
    return {
      url: `${parsed.origin}${parsed.pathname}`.slice(0, 500),
      host: parsed.hostname.toLowerCase().replace(/^www\./, '').slice(0, 160),
    };
  } catch {
    return { url: null, host: null };
  }
}

function sourceFromReferrer(referrerHost: string | null, ownHost: string, utmSource: string | null) {
  if (utmSource) return utmSource;
  if (!referrerHost || ownHost === referrerHost || ownHost.endsWith(`.${referrerHost}`)) return 'Direct';

  const sources: Array<[RegExp, string]> = [
    [/(^|\.)google\./, 'Google'],
    [/(^|\.)bing\.com$/, 'Bing'],
    [/(^|\.)duckduckgo\.com$/, 'DuckDuckGo'],
    [/(^|\.)search\.yahoo\.com$/, 'Yahoo'],
    [/(^|\.)(x\.com|twitter\.com|t\.co)$/, 'X'],
    [/(^|\.)(facebook\.com|instagram\.com)$/, 'Meta'],
    [/(^|\.)reddit\.com$/, 'Reddit'],
    [/(^|\.)linkedin\.com$/, 'LinkedIn'],
    [/(^|\.)discord\.(com|gg)$/, 'Discord'],
    [/(^|\.)producthunt\.com$/, 'Product Hunt'],
    [/(^|\.)(chatgpt\.com|openai\.com)$/, 'ChatGPT'],
  ];

  return sources.find(([pattern]) => pattern.test(referrerHost))?.[1] || referrerHost;
}

function mediumFromSource(source: string, utmMedium: string | null, hasReferrer: boolean) {
  if (utmMedium) return utmMedium;
  if (['Google', 'Bing', 'DuckDuckGo', 'Yahoo'].includes(source)) return 'organic';
  if (['X', 'Meta', 'Reddit', 'LinkedIn', 'Discord'].includes(source)) return 'social';
  return hasReferrer ? 'referral' : 'direct';
}

function deviceFromUserAgent(userAgent: string) {
  if (/ipad|tablet|kindle|silk|playbook/i.test(userAgent)) return 'Tablet';
  if (/mobile|iphone|ipod|android.*mobile|windows phone/i.test(userAgent)) return 'Mobile';
  return 'Desktop';
}

function browserFromUserAgent(userAgent: string) {
  if (/edg\//i.test(userAgent)) return 'Edge';
  if (/opr\//i.test(userAgent)) return 'Opera';
  if (/firefox\//i.test(userAgent)) return 'Firefox';
  if (/crios\//i.test(userAgent)) return 'Chrome';
  if (/chrome\//i.test(userAgent)) return 'Chrome';
  if (/safari\//i.test(userAgent)) return 'Safari';
  return 'Other';
}

function operatingSystemFromUserAgent(userAgent: string) {
  if (/iphone|ipad|ipod/i.test(userAgent)) return 'iOS';
  if (/android/i.test(userAgent)) return 'Android';
  if (/windows/i.test(userAgent)) return 'Windows';
  if (/mac os|macintosh/i.test(userAgent)) return 'macOS';
  if (/linux/i.test(userAgent)) return 'Linux';
  return 'Other';
}

function locationFromHeaders(request: NextRequest) {
  const rawCountry = (
    request.headers.get('x-vercel-ip-country') ||
    request.headers.get('cf-ipcountry') ||
    request.headers.get('x-country-code') ||
    ''
  ).trim().toUpperCase();

  return {
    country: /^[A-Z]{2}$/.test(rawCountry) ? rawCountry : null,
    region: safeDecode(
      request.headers.get('x-vercel-ip-country-region') || request.headers.get('x-region'),
    ),
    city: safeDecode(request.headers.get('x-vercel-ip-city') || request.headers.get('x-city')),
  };
}

function anonymizedIpHash(request: NextRequest) {
  const salt = process.env.VAIL_ANALYTICS_SALT?.trim();
  if (!salt) return null;

  const ip = (
    request.headers.get('x-forwarded-for')?.split(',')[0] ||
    request.headers.get('x-real-ip') ||
    ''
  ).trim();
  if (!ip) return null;

  return createHmac('sha256', salt).update(ip).digest('hex');
}

export async function POST(request: NextRequest) {
  if (!isSameOrigin(request)) {
    return NextResponse.json({ error: 'Invalid origin.' }, { status: 403 });
  }

  const contentLength = Number(request.headers.get('content-length') || 0);
  if (contentLength > 8_192) {
    return NextResponse.json({ error: 'Payload too large.' }, { status: 413 });
  }

  const userAgent = request.headers.get('user-agent') || '';
  if (!userAgent || BOT_PATTERN.test(userAgent)) {
    return new NextResponse(null, { status: 204 });
  }

  let payload: PageViewPayload;
  try {
    payload = (await request.json()) as PageViewPayload;
  } catch {
    return NextResponse.json({ error: 'Invalid request.' }, { status: 400 });
  }

  const visitorId = cleanText(payload.visitorId, 36);
  const sessionId = cleanText(payload.sessionId, 36);
  const path = cleanPath(payload.path);
  if (!visitorId || !UUID_PATTERN.test(visitorId) || !sessionId || !UUID_PATTERN.test(sessionId) || !path) {
    return NextResponse.json({ error: 'Invalid page view.' }, { status: 400 });
  }

  if (path.startsWith('/hq') || path.startsWith('/ops') || path.startsWith('/api')) {
    return new NextResponse(null, { status: 204 });
  }

  const referrer = sanitizeReferrer(payload.referrer);
  const ownHostname = requestHost(request).split(':')[0].replace(/^www\./, '');
  const utmSource = cleanText(payload.utmSource, 100);
  const utmMedium = cleanText(payload.utmMedium, 100);
  const source = sourceFromReferrer(referrer.host, ownHostname, utmSource);
  const location = locationFromHeaders(request);
  const supabase = getAdminSupabase();
  const dedupeSince = new Date(Date.now() - 10_000).toISOString();

  const duplicateResponse = await supabase
    .from('site_page_views')
    .select('id')
    .eq('visitor_id', visitorId)
    .eq('session_id', sessionId)
    .eq('path', path)
    .gte('occurred_at', dedupeSince)
    .limit(1);

  if (duplicateResponse.error) {
    return NextResponse.json({ error: 'Analytics is unavailable.' }, { status: 503 });
  }
  if ((duplicateResponse.data || []).length > 0) {
    return new NextResponse(null, { status: 204 });
  }

  const insertResponse = await supabase.from('site_page_views').insert({
    visitor_id: visitorId,
    session_id: sessionId,
    path,
    referrer_url: referrer.url,
    referrer_host: referrer.host,
    source,
    medium: mediumFromSource(source, utmMedium, Boolean(referrer.host)),
    campaign: cleanText(payload.utmCampaign, 160),
    country_code: location.country,
    region: location.region,
    city: location.city,
    device_type: deviceFromUserAgent(userAgent),
    browser: browserFromUserAgent(userAgent),
    operating_system: operatingSystemFromUserAgent(userAgent),
    ip_hash: anonymizedIpHash(request),
  });

  if (insertResponse.error) {
    return NextResponse.json({ error: 'Analytics is unavailable.' }, { status: 503 });
  }

  return new NextResponse(null, { status: 202 });
}

import 'server-only';

import { getAdminSupabase } from '@/lib/supabase-admin';

export const ANALYTICS_RANGES = [7, 30, 90] as const;

export type AnalyticsRange = (typeof ANALYTICS_RANGES)[number];

export type SiteAnalyticsSummary = {
  views: number;
  visitors: number;
  sessions: number;
  viewsPerSession: number;
  bounceRate: number;
  previousViews: number;
  previousVisitors: number;
  previousSessions: number;
  allTimeViews: number;
  allTimeVisitors: number;
  todayViews: number;
  todayVisitors: number;
};

export type DailyMetric = {
  day: string;
  views: number;
  visitors: number;
};

export type RankedMetric = {
  views: number;
  visitors: number;
};

export type SourceMetric = RankedMetric & {
  source: string;
};

export type LocationMetric = RankedMetric & {
  country_code: string;
  region: string;
  city: string;
};

export type PageMetric = RankedMetric & {
  path: string;
};

export type DeviceMetric = RankedMetric & {
  device_type: string;
};

export type RecentVisit = {
  occurred_at: string;
  path: string;
  source: string;
  country_code: string | null;
  region: string | null;
  city: string | null;
  device_type: string;
  browser: string | null;
};

export type SiteAnalyticsData = {
  generatedAt: string;
  rangeStart: string;
  summary: SiteAnalyticsSummary;
  daily: DailyMetric[];
  sources: SourceMetric[];
  locations: LocationMetric[];
  pages: PageMetric[];
  devices: DeviceMetric[];
  recent: RecentVisit[];
  signups: {
    allTime: number;
    inRange: number;
  };
};

function numberValue(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeRankedMetric<T extends RankedMetric>(metric: T): T {
  return {
    ...metric,
    views: numberValue(metric.views),
    visitors: numberValue(metric.visitors),
  };
}

function normalizeAnalytics(value: unknown, signups: SiteAnalyticsData['signups']): SiteAnalyticsData {
  const data = (value && typeof value === 'object' ? value : {}) as Partial<SiteAnalyticsData>;
  const rawSummary = (data.summary || {}) as Partial<SiteAnalyticsSummary>;

  return {
    generatedAt: String(data.generatedAt || new Date().toISOString()),
    rangeStart: String(data.rangeStart || new Date().toISOString()),
    summary: {
      views: numberValue(rawSummary.views),
      visitors: numberValue(rawSummary.visitors),
      sessions: numberValue(rawSummary.sessions),
      viewsPerSession: numberValue(rawSummary.viewsPerSession),
      bounceRate: numberValue(rawSummary.bounceRate),
      previousViews: numberValue(rawSummary.previousViews),
      previousVisitors: numberValue(rawSummary.previousVisitors),
      previousSessions: numberValue(rawSummary.previousSessions),
      allTimeViews: numberValue(rawSummary.allTimeViews),
      allTimeVisitors: numberValue(rawSummary.allTimeVisitors),
      todayViews: numberValue(rawSummary.todayViews),
      todayVisitors: numberValue(rawSummary.todayVisitors),
    },
    daily: (data.daily || []).map((metric) => ({
      ...metric,
      views: numberValue(metric.views),
      visitors: numberValue(metric.visitors),
    })),
    sources: (data.sources || []).map(normalizeRankedMetric),
    locations: (data.locations || []).map(normalizeRankedMetric),
    pages: (data.pages || []).map(normalizeRankedMetric),
    devices: (data.devices || []).map(normalizeRankedMetric),
    recent: data.recent || [],
    signups,
  };
}

export function parseAnalyticsRange(value: string | string[] | undefined): AnalyticsRange {
  const parsed = Number(Array.isArray(value) ? value[0] : value);
  return ANALYTICS_RANGES.includes(parsed as AnalyticsRange) ? (parsed as AnalyticsRange) : 30;
}

export async function getSiteAnalytics(days: AnalyticsRange): Promise<SiteAnalyticsData> {
  const supabase = getAdminSupabase();
  const rangeStart = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  const [analyticsResponse, allSignupsResponse, rangeSignupsResponse] = await Promise.all([
    supabase.rpc('get_site_analytics', { p_days: days }),
    supabase.from('profiles').select('id', { count: 'exact', head: true }),
    supabase.from('profiles').select('id', { count: 'exact', head: true }).gte('created_at', rangeStart),
  ]);

  if (analyticsResponse.error) {
    throw analyticsResponse.error;
  }

  return normalizeAnalytics(analyticsResponse.data, {
    allTime: allSignupsResponse.count || 0,
    inRange: rangeSignupsResponse.count || 0,
  });
}

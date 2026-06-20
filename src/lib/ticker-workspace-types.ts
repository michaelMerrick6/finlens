export type DashboardTickerActivitySource = 'politician' | 'insider' | 'fund';
export type DashboardTickerActivityFilter = 'all' | DashboardTickerActivitySource;

export type DashboardTickerActivityDirection =
  | 'buy'
  | 'sell'
  | 'increase'
  | 'decrease'
  | 'new'
  | 'exit'
  | 'flat'
  | 'activity';

export type DashboardTickerActivity = {
  id: string;
  sourceType: DashboardTickerActivitySource;
  actorName: string;
  actorSubtitle: string | null;
  memberId: string | null;
  party: string | null;
  chamber: string | null;
  direction: DashboardTickerActivityDirection;
  directionLabel: string;
  amountLabel: string | null;
  metricLabel: string | null;
  metricCaption: string | null;
  secondaryMetricLabel: string | null;
  secondaryMetricCaption: string | null;
  date: string | null;
  filingDate: string | null;
  sourceUrl: string | null;
};

export type DashboardTickerWorkspaceData = {
  symbol: string;
  companyName: string;
  sector: string | null;
  industry: string | null;
  latestActivityDate: string | null;
  source: DashboardTickerActivityFilter;
  recentActivity: DashboardTickerActivity[];
  nextOffset: number | null;
};

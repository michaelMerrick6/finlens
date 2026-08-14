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

export type DashboardCongressOverviewRange = '7d' | '30d' | 'ytd';

export type DashboardCongressTransaction = {
  id: string;
  memberId: string | null;
  politicianName: string;
  party: string | null;
  chamber: string | null;
  direction: 'buy' | 'sell';
  transactionDate: string | null;
  publishedDate: string;
  amountRange: string | null;
  amountFloor: number;
  sourceUrl: string | null;
};

export type DashboardCongressOverviewData = {
  symbol: string;
  range: DashboardCongressOverviewRange;
  periodStart: string;
  periodEnd: string;
  latestDisclosureDate: string | null;
  totals: {
    lawmakerCount: number;
    buyCount: number;
    sellCount: number;
    buyAmountFloor: number;
    sellAmountFloor: number;
  };
  transactions: DashboardCongressTransaction[];
};

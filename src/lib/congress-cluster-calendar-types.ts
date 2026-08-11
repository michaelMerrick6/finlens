export type CongressClusterRange = 'week' | 'month' | 'ytd';

export type CongressClusterPlay = {
  ticker: string;
  companyName: string | null;
  actorCount: number;
  lawmakersSharePct: number;
  tradeCount: number;
  amountFloor: number;
  latestTransactionDate: string | null;
  latestDisclosureDate: string;
  conviction: 'high' | 'building';
};

export type CongressClusterCalendarData = {
  range: CongressClusterRange;
  latestDisclosureDate: string | null;
  topClusters: CongressClusterPlay[];
  totals: {
    actorCount: number;
    clusterCount: number;
    tradeCount: number;
    tickerCount: number;
    amountFloor: number;
  };
};

export type CongressClusterRange = 'week' | 'month' | 'year';

export type CongressClusterPlay = {
  ticker: string;
  companyName: string | null;
  actorCount: number;
  lawmakersSharePct: number;
  tradeCount: number;
  amountFloor: number;
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

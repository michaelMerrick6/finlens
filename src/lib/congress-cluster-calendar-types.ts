export type CongressClusterRange = 'week' | 'month' | 'year';

export type CongressClusterPlay = {
  ticker: string;
  companyName: string | null;
  actorCount: number;
  tradeCount: number;
  amountFloor: number;
  latestDisclosureDate: string;
  politicianNames: string[];
  conviction: 'high' | 'building';
};

export type CongressClusterDay = {
  date: string;
  actorCount: number;
  clusterCount: number;
  tradeCount: number;
  tickerCount: number;
  amountFloor: number;
  topTicker: string | null;
};

export type CongressClusterCalendarData = {
  range: CongressClusterRange;
  rangeStart: string;
  rangeEnd: string;
  latestDisclosureDate: string | null;
  generatedAt: string;
  topClusters: CongressClusterPlay[];
  days: CongressClusterDay[];
  totals: {
    actorCount: number;
    tradeCount: number;
    tickerCount: number;
    amountFloor: number;
  };
};

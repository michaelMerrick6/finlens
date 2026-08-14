export type CongressClusterRange = 'week' | 'month' | 'ytd';

export type CongressBuyingCompany = {
  ticker: string;
  companyName: string | null;
  actorCount: number;
  tradeCount: number;
  amountFloor: number;
  latestTransactionDate: string | null;
  latestDisclosureDate: string;
};

export type CongressClusterCalendarData = {
  range: CongressClusterRange;
  latestDisclosureDate: string | null;
  rankedCompanies: CongressBuyingCompany[];
  totals: {
    actorCount: number;
    companyCount: number;
    tradeCount: number;
    amountFloor: number;
  };
};

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

export type CongressBuyingTransaction = {
  id: string;
  memberId: string | null;
  politicianName: string;
  chamber: string | null;
  party: string | null;
  assetName: string | null;
  transactionDate: string | null;
  publishedDate: string;
  amountRange: string | null;
  amountFloor: number;
  sourceUrl: string | null;
};

export type CongressBuyingTransactionsData = {
  ticker: string;
  range: CongressClusterRange;
  transactions: CongressBuyingTransaction[];
  totals: {
    actorCount: number;
    tradeCount: number;
    amountFloor: number;
  };
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

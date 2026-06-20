export type TickerInsightTone = 'bullish' | 'bearish' | 'neutral';

export type TickerOverview = {
  symbol: string;
  companyName: string;
  sector: string | null;
  industry: string | null;
  currentPrice: number | null;
  priceAsOf: string | null;
  latestActivityDate: string | null;
  sourceCount: number;
  politicianHolderCount: number;
  politicianTransactionCount: number;
  insiderTransactionCount: number;
  hedgeFundHolderCount: number;
};

export type TickerPoliticianHolder = {
  key: string;
  memberId: string | null;
  name: string;
  party: string | null;
  chamber: string | null;
  minValue: number;
  midValue: number;
  maxValue: number;
  tradeCount: number;
  lastTradeDate: string | null;
};

export type TickerPoliticianTransaction = {
  id: string;
  memberId: string | null;
  name: string;
  party: string | null;
  chamber: string | null;
  transactionType: string;
  amountRange: string | null;
  transactionDate: string | null;
  publishedDate: string | null;
  sourceUrl: string | null;
};

export type TickerPoliticianTransactionsPage = {
  transactions: TickerPoliticianTransaction[];
  totalCount: number;
  offset: number;
  limit: number;
  nextOffset: number | null;
};

export type TickerInsiderWindow = {
  key: string;
  label: string;
  days: number | null;
  transactionCount: number;
  buyValue: number;
  sellValue: number;
  buyCount: number;
  sellCount: number;
  totalValue: number;
  netValue: number;
  buyRatio: number;
  tone: TickerInsightTone;
};

export type TickerInsiderTransaction = {
  id: string;
  identityKey: string;
  filerName: string;
  filerRelation: string | null;
  direction: 'buy' | 'sell' | 'other';
  transactionCode: string | null;
  transactionDate: string | null;
  publishedDate: string | null;
  value: number;
  amount: number | null;
  price: number | null;
  sourceUrl: string | null;
  sharesOwnedAfterTransaction: number | null;
  sharesOwnedBeforeTransaction: number | null;
  holdingChangePct: number | null;
};

export type TickerInsiderHolding = {
  key: string;
  filerName: string;
  filerRelation: string | null;
  sharesHeld: number;
  estimatedValue: number | null;
  lastTransactionDate: string | null;
  publishedDate: string | null;
  sourceUrl: string | null;
  lastDirection: 'buy' | 'sell' | 'other';
  holdingChangePct: number | null;
};

export type TickerFundHolder = {
  key: string;
  fundName: string;
  reportPeriod: string | null;
  publishedDate: string | null;
  sharesHeld: number;
  valueHeld: number;
  changeKind: 'new' | 'increase' | 'decrease' | 'exit' | 'hold' | 'unknown';
  changeLabel: string;
  sourceUrl: string | null;
};

export type TickerFundSummary = {
  increased: number;
  decreased: number;
  neutral: number;
};

export type TickerIntelligencePayload = {
  overview: TickerOverview;
  politicianHolders: TickerPoliticianHolder[];
  politicianTransactions: TickerPoliticianTransaction[];
  insiderWindows: TickerInsiderWindow[];
  insiderTransactions: TickerInsiderTransaction[];
  insiderHoldings: TickerInsiderHolding[];
  hedgeFundHolders: TickerFundHolder[];
  hedgeFundSummary: TickerFundSummary;
};

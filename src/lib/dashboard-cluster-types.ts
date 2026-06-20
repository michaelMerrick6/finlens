export type DashboardClusterTransaction = {
  id: string;
  sourceType: 'politician' | 'insider' | 'fund' | 'unknown';
  actorName: string;
  memberId: string | null;
  party: string | null;
  actorSubtitle: string | null;
  assetLabel: string;
  assetName: string | null;
  assetType: string | null;
  transactionTypeLabel: string;
  amountLabel: string | null;
  transactionDate: string | null;
  publishedDate: string | null;
  sourceUrl: string | null;
};

export type DashboardClusterDetail = {
  transactions: DashboardClusterTransaction[];
};

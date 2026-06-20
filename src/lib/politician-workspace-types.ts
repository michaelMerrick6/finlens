import type { PoliticianProfileTrade } from '@/lib/politician-profile';

export type DashboardPoliticianWorkspaceSummary = {
  displayName: string;
  party: string | null;
  chamber: string | null;
  state: string | null;
  latestTradeDate: string | null;
};

export type DashboardPoliticianWorkspaceData = {
  memberId: string;
  summary: DashboardPoliticianWorkspaceSummary;
  trades: PoliticianProfileTrade[];
  nextOffset: number | null;
};

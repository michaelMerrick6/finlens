export type AlertMode = 'activity' | 'unusual' | 'both';
export type ActorType = 'politician' | 'insider' | 'fund';
export type BillingPlanKey = 'free' | 'pro';
export type ClusterAlertChannel = 'email' | 'sms';

export type AccountProfileState = {
  id: string;
  email: string | null;
  displayName: string | null;
  alertEmail: string | null;
  textPhone: string | null;
  emailEnabled: boolean;
  textEnabled: boolean;
  followLimit: number;
};

export type AccountTickerFollow = {
  id: string;
  ticker: string;
  alertMode: AlertMode;
  createdAt: string;
};

export type AccountActorFollow = {
  id: string;
  actorType: ActorType;
  actorKey: string;
  actorName: string;
  alertMode: AlertMode;
  createdAt: string;
  metadata: Record<string, unknown>;
};

export type AccountClusterFollow = {
  id: 'cluster-feed';
  label: 'Clusters';
  channels: ClusterAlertChannel[];
};

export type AccountSubscriptionState = {
  id: string | null;
  channel: 'email' | 'sms';
  destination: string | null;
  active: boolean;
  minimumImportance: number | null;
};

export type AccountBillingState = {
  planKey: BillingPlanKey;
  planName: string;
  status: string;
  followLimit: number;
  freeFollowLimit: number;
  proFollowLimit: number;
  checkoutReady: boolean;
  portalReady: boolean;
  stripeCustomerId: string | null;
  stripeSubscriptionId: string | null;
  currentPeriodEnd: string | null;
  cancelAtPeriodEnd: boolean;
};

export type AccountAlertHistoryItem = {
  id: string;
  signalEventId: string | null;
  channel: string;
  destination: string | null;
  status: string;
  lastError: string | null;
  queuedAt: string;
  sentAt: string | null;
  title: string | null;
  summary: string | null;
  ticker: string | null;
  actorName: string | null;
  sourceUrl: string | null;
  publishedAt: string | null;
};

export type AccountMatchedSignal = {
  id: string;
  title: string;
  summary: string | null;
  ticker: string | null;
  actorName: string | null;
  actorMemberId?: string | null;
  signalType: string;
  source: string | null;
  direction: string | null;
  importanceScore: number;
  occurredAt: string | null;
  publishedAt: string | null;
  sourceUrl: string | null;
  matchReasons: string[];
  behaviorLabels: string[];
  isCluster: boolean;
};

export type AccountFollowSignalPreview = {
  followKind: 'ticker' | 'actor';
  followId: string;
  followLabel: string;
  followType: 'stock' | ActorType;
  alertMode: AlertMode;
  matchedCount: number;
  latestMatchedAt: string | null;
  recentSignals: AccountMatchedSignal[];
  clusterSignals: AccountMatchedSignal[];
};

export type AccountAlertPreview = {
  scanLimit: number;
  matchedSignalCount: number;
  clusterSignalCount: number;
  followPreviews: AccountFollowSignalPreview[];
  timeline: AccountMatchedSignal[];
};

export type AccountFollowSuggestion = {
  actorType: ActorType;
  actorName: string;
  actorKey: string;
  subtitle: string | null;
};

export type AccountTickerSuggestion = {
  ticker: string;
  companyName: string;
  subtitle: string | null;
};

export type AccountState = {
  user: {
    id: string;
    email: string | null;
  };
  profile: AccountProfileState;
  billing: AccountBillingState;
  watchlist: {
    id: string;
    name: string;
  };
  followCount: number;
  followLimit: number;
  subscriptions: {
    email: AccountSubscriptionState;
    sms: AccountSubscriptionState;
  };
  follows: {
    tickers: AccountTickerFollow[];
    actors: AccountActorFollow[];
    cluster: AccountClusterFollow | null;
  };
  history: AccountAlertHistoryItem[];
  alertPreview: AccountAlertPreview;
};

export type AlertMode = 'activity' | 'unusual' | 'both';
export type ActorType = 'politician' | 'insider';
export type BillingPlanKey = 'free' | 'pro';

export type AccountProfileState = {
  id: string;
  email: string | null;
  displayName: string | null;
  alertEmail: string | null;
  telegramUsername: string | null;
  telegramChatId: string | null;
  emailEnabled: boolean;
  telegramEnabled: boolean;
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

export type AccountSubscriptionState = {
  id: string | null;
  channel: 'email' | 'telegram';
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
  telegramBotUsername: string | null;
  subscriptions: {
    email: AccountSubscriptionState;
    telegram: AccountSubscriptionState;
  };
  follows: {
    tickers: AccountTickerFollow[];
    actors: AccountActorFollow[];
  };
  history: AccountAlertHistoryItem[];
};

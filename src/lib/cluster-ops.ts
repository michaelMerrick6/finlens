import 'server-only';

import { readSignalPolicy } from '@/lib/signal-policy';
import type { BroadcastStory } from '@/lib/tweet-candidates';
import { fetchTweetCandidateStories } from '@/lib/tweet-candidates';

export type ClusterStoryStatus = 'pending_review' | 'approved' | 'posted' | 'rejected' | 'mixed';

export type ClusterOpsStory = {
  id: string;
  ticker: string;
  title: string;
  summary: string;
  ruleKey: string;
  ruleLabel: string;
  sourceLabel: string;
  actorPreview: string | null;
  actorCount: number;
  amountLabel: string | null;
  publishedAt: string | null;
  createdAt: string;
  score: number;
  status: ClusterStoryStatus;
  sourceGroup: 'congress' | 'insiders' | 'cross-source';
  direction: 'buy' | 'sell' | null;
};

export type ClusterOpsMetric = {
  label: string;
  value: string;
  sublabel: string;
  tone: 'default' | 'success' | 'warn';
};

export type ClusterOpsRuleMetric = {
  ruleKey: string;
  ruleLabel: string;
  total: number;
  pendingReview: number;
  publicArchive: number;
  last30d: number;
};

export type ClusterOpsPolicySnapshot = {
  minimumImportance: number;
  minimumGroupCount: number;
  largePoliticianBuyMinLowerBound: number;
  committeeRelevanceBuyMinLowerBound: number;
  congressClusterWindowDays: number;
  congressClusterMinMembers: number;
  crossSourceClusterWindowDays: number;
  fundAlignmentWindowDays: number;
  meaningfulInsiderChangeMinPct: number;
  meaningfulInsiderChangeMinValue: number;
};

export type ClusterOpsData = {
  metrics: ClusterOpsMetric[];
  stories: ClusterOpsStory[];
  rules: ClusterOpsRuleMetric[];
  policy: ClusterOpsPolicySnapshot;
};

function trim(value: string | null | undefined) {
  return (value || '').trim();
}

function ruleLabel(ruleKey: string, direction?: string | null) {
  if (ruleKey === 'congress_cluster') return 'Congress Cluster';
  if (ruleKey === 'cross_source_accumulation') return trim(direction).toLowerCase() === 'sell' ? 'Cross-Source Sell' : 'Cross-Source Buy';
  if (ruleKey === 'insider_cluster') return 'Insider Cluster';
  if (ruleKey === 'grouped_congress_buy') return 'Congress Sweep';
  if (ruleKey === 'grouped_insider_buy') return 'Insider Sweep';
  if (ruleKey === 'large_politician_buy') return 'Large Position';
  if (ruleKey === 'substantial_insider_buy') return 'Heavy Insider Buy';
  return ruleKey
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function sourceGroupForRule(ruleKey: string): ClusterOpsStory['sourceGroup'] {
  if (ruleKey === 'cross_source_accumulation') {
    return 'cross-source';
  }
  if (ruleKey === 'insider_cluster' || ruleKey === 'grouped_insider_buy' || ruleKey === 'substantial_insider_buy') {
    return 'insiders';
  }
  return 'congress';
}

function sourceMixLabel(story: {
  sourceMix: {
    congress: number;
    insiders: number;
    funds: number;
  };
}) {
  const parts: string[] = [];
  if (story.sourceMix.congress) {
    parts.push(story.sourceMix.congress > 1 ? `Congress ${story.sourceMix.congress}` : 'Congress');
  }
  if (story.sourceMix.insiders) {
    parts.push(story.sourceMix.insiders > 1 ? `Insiders ${story.sourceMix.insiders}` : 'Insiders');
  }
  if (story.sourceMix.funds) {
    parts.push(story.sourceMix.funds > 1 ? `Funds ${story.sourceMix.funds}` : 'Funds');
  }
  return parts.length ? parts.join(' • ') : 'Single source';
}

function storySummary(story: BroadcastStory) {
  const clusterWindowDays = story.clusterWindowDays && story.clusterWindowDays > 0 ? story.clusterWindowDays : null;
  const windowLabel = clusterWindowDays
    ? ` inside ${clusterWindowDays} day${clusterWindowDays === 1 ? '' : 's'}`
    : '';

  if (story.ruleKey === 'congress_cluster' || story.ruleKey === 'insider_cluster') {
    return `${story.actorCount} actors${windowLabel}`;
  }
  if (story.ruleKey === 'grouped_congress_buy' || story.ruleKey === 'grouped_insider_buy') {
    return 'Repeated buys landed in one filing';
  }
  if (story.ruleKey === 'cross_source_accumulation') {
    return `${sourceMixLabel(story)}${windowLabel || ' aligned'}`;
  }
  if (story.ruleKey === 'large_politician_buy') {
    return 'Large congressional position';
  }
  if (story.ruleKey === 'substantial_insider_buy') {
    return 'Buy size stood out in recent insider activity';
  }
  return story.rationale || 'Cluster signal';
}

function storyStatus(story: BroadcastStory): ClusterStoryStatus {
  const statuses = Array.from(
    new Set(
      Object.values(story.channels)
        .map((channel) => trim(channel?.status))
        .filter(Boolean),
    ),
  );

  if (!statuses.length) {
    return 'mixed';
  }
  if (statuses.length === 1) {
    return statuses[0] as ClusterStoryStatus;
  }
  if (statuses.includes('pending_review')) {
    return 'pending_review';
  }
  if (statuses.includes('approved')) {
    return 'approved';
  }
  if (statuses.includes('posted')) {
    return 'posted';
  }
  if (statuses.includes('rejected')) {
    return 'rejected';
  }
  return 'mixed';
}

function actorPreview(story: BroadcastStory) {
  return story.actorLabels.length ? story.actorLabels.slice(0, 4).join(', ') : null;
}

function sinceIso(days: number) {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() - days);
  return value.toISOString().slice(0, 10);
}

function isRecent(story: ClusterOpsStory, thresholdIso: string) {
  return Boolean(story.publishedAt && story.publishedAt >= thresholdIso);
}

export async function getClusterOpsData(): Promise<ClusterOpsData> {
  const [policy, stories] = await Promise.all([
    readSignalPolicy().catch(() => ({} as Record<string, unknown>)),
    fetchTweetCandidateStories({
      status: ['pending_review', 'approved', 'posted', 'rejected'],
      sinceDate: null,
      storyLimit: 240,
      category: 'cluster_feed',
      sort: 'newest',
    }),
  ]);

  const tweetPolicy = ((policy as Record<string, unknown>).tweet_candidates as Record<string, unknown> | undefined) || {};

  const mappedStories: ClusterOpsStory[] = stories.map((story) => {
    const normalizedDirection = trim(story.direction).toLowerCase();
    const sourceGroup = sourceGroupForRule(story.ruleKey);
    return {
      id: story.candidateKey,
      ticker: trim(story.ticker).toUpperCase(),
      title: story.title,
      summary: storySummary(story),
      ruleKey: story.ruleKey,
      ruleLabel: ruleLabel(story.ruleKey, story.direction),
      sourceLabel: sourceMixLabel(story),
      actorPreview: actorPreview(story),
      actorCount: story.actorCount,
      amountLabel: story.amountLabel,
      publishedAt: story.latestPublishedAt,
      createdAt: story.createdAt,
      score: story.score,
      status: storyStatus(story),
      sourceGroup,
      direction:
        normalizedDirection === 'buy'
          ? 'buy'
          : normalizedDirection === 'sell'
            ? 'sell'
            : null,
    };
  });

  const recentIso = sinceIso(30);
  const retainedStories = mappedStories.filter((story) => story.status !== 'rejected');
  const publicArchiveStories = mappedStories.filter((story) =>
    ['pending_review', 'approved', 'posted'].includes(story.status),
  );
  const pendingStories = mappedStories.filter((story) => story.status === 'pending_review');
  const crossSourceStories = mappedStories.filter((story) => story.sourceGroup === 'cross-source');
  const recentStories = mappedStories.filter((story) => isRecent(story, recentIso));

  const ruleMap = new Map<string, ClusterOpsRuleMetric>();
  for (const story of mappedStories) {
    const existing = ruleMap.get(story.ruleKey) || {
      ruleKey: story.ruleKey,
      ruleLabel: story.ruleLabel,
      total: 0,
      pendingReview: 0,
      publicArchive: 0,
      last30d: 0,
    };
    existing.total += 1;
    if (story.status === 'pending_review') {
      existing.pendingReview += 1;
    }
    if (['pending_review', 'approved', 'posted'].includes(story.status)) {
      existing.publicArchive += 1;
    }
    if (isRecent(story, recentIso)) {
      existing.last30d += 1;
    }
    ruleMap.set(story.ruleKey, existing);
  }

  const metrics: ClusterOpsMetric[] = [
    {
      label: 'Retained Clusters',
      value: retainedStories.length.toLocaleString(),
      sublabel: 'Non-rejected clusters kept in the archive feed.',
      tone: 'success',
    },
    {
      label: 'Pending Review',
      value: pendingStories.length.toLocaleString(),
      sublabel: 'Captured clusters still waiting on internal review.',
      tone: 'warn',
    },
    {
      label: 'Public Archive',
      value: publicArchiveStories.length.toLocaleString(),
      sublabel: 'Clusters now available to the public clusters page.',
      tone: 'default',
    },
    {
      label: 'Last 30 Days',
      value: recentStories.length.toLocaleString(),
      sublabel: 'Recent cluster capture volume across all rule families.',
      tone: 'default',
    },
    {
      label: 'Cross-Source',
      value: crossSourceStories.length.toLocaleString(),
      sublabel: 'Congress plus insider alignment candidates currently retained.',
      tone: 'default',
    },
  ];

  return {
    metrics,
    stories: mappedStories,
    rules: [...ruleMap.values()].sort((left, right) => right.total - left.total || right.last30d - left.last30d),
    policy: {
      minimumImportance: Number(tweetPolicy.minimum_importance) || 0.88,
      minimumGroupCount: Number(tweetPolicy.minimum_group_count) || 2,
      largePoliticianBuyMinLowerBound: Number(tweetPolicy.large_politician_buy_min_lower_bound) || 100000,
      committeeRelevanceBuyMinLowerBound: Number(tweetPolicy.committee_relevance_buy_min_lower_bound) || 15001,
      congressClusterWindowDays: Number(process.env.CONGRESS_CLUSTER_WINDOW_DAYS) || 10,
      congressClusterMinMembers: Number(process.env.CONGRESS_CLUSTER_MIN_MEMBERS) || 2,
      crossSourceClusterWindowDays: Number(process.env.CROSS_SOURCE_CLUSTER_WINDOW_DAYS) || 45,
      fundAlignmentWindowDays: Number(process.env.FUND_ALIGNMENT_WINDOW_DAYS) || 120,
      meaningfulInsiderChangeMinPct: 0.25,
      meaningfulInsiderChangeMinValue: 250000,
    },
  };
}

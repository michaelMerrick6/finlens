export type ClusterPresentationSignal = {
  actorCount: number;
  amountLabel: string | null;
  direction: 'buy' | 'sell' | null;
  ruleKey: string;
  sourceCounts: {
    congress: number;
    insiders: number;
    funds: number;
  };
  ticker: string;
  title: string;
  windowDays: number | null;
};

export type ClusterEvidenceItem = {
  key: 'congress' | 'insiders' | 'funds' | 'window';
  label: string;
};

function plural(value: number, singular: string, pluralLabel = `${singular}s`) {
  return `${value.toLocaleString()} ${value === 1 ? singular : pluralLabel}`;
}

function readableList(parts: string[]) {
  if (parts.length <= 1) return parts[0] || '';
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`;
  return `${parts.slice(0, -1).join(', ')}, and ${parts.at(-1)}`;
}

export function clusterCategoryLabel(signal: ClusterPresentationSignal) {
  if (signal.ruleKey === 'cross_source_accumulation') return 'Cross-source';
  if (signal.ruleKey === 'insider_cluster') return 'Insiders';
  return 'Congress';
}

export function clusterEvidenceItems(signal: ClusterPresentationSignal): ClusterEvidenceItem[] {
  const items: ClusterEvidenceItem[] = [];
  if (signal.sourceCounts.congress) {
    items.push({
      key: 'congress',
      label: plural(signal.sourceCounts.congress, 'member of Congress', 'members of Congress'),
    });
  }
  if (signal.sourceCounts.insiders) {
    items.push({ key: 'insiders', label: plural(signal.sourceCounts.insiders, 'insider') });
  }
  if (signal.sourceCounts.funds) {
    items.push({ key: 'funds', label: plural(signal.sourceCounts.funds, 'fund') });
  }
  if (signal.windowDays) {
    items.push({ key: 'window', label: `${signal.windowDays}-day window` });
  }
  return items;
}

export function clusterHeadline(signal: ClusterPresentationSignal) {
  const action = signal.direction === 'sell' ? 'selling' : 'buying';
  if (signal.ruleKey === 'insider_cluster') {
    return `${plural(signal.actorCount, 'insider')} ${action} ${signal.ticker}`;
  }
  if (signal.ruleKey === 'congress_cluster') {
    return `${plural(signal.actorCount, 'member of Congress', 'members of Congress')} ${action} ${signal.ticker}`;
  }

  const sources: string[] = [];
  if (signal.sourceCounts.congress) {
    sources.push(plural(signal.sourceCounts.congress, 'member of Congress', 'members of Congress'));
  }
  if (signal.sourceCounts.insiders) {
    sources.push(plural(signal.sourceCounts.insiders, 'insider'));
  }
  if (signal.sourceCounts.funds) {
    sources.push(plural(signal.sourceCounts.funds, 'fund'));
  }
  return sources.length ? `${readableList(sources)} align on ${signal.ticker}` : signal.title;
}

export function clusterReason(signal: ClusterPresentationSignal) {
  const action = signal.direction === 'sell' ? 'bearish' : 'bullish';
  if (signal.ruleKey === 'cross_source_accumulation') {
    const sourceFamilies = [
      signal.sourceCounts.congress,
      signal.sourceCounts.insiders,
      signal.sourceCounts.funds,
    ].filter((count) => count > 0).length;
    return `${sourceFamilies} independent filing sources point in the same ${action} direction${signal.windowDays ? ` inside ${signal.windowDays} days` : ''}.`;
  }
  if (signal.ruleKey === 'insider_cluster') {
    return `At least five distinct company insiders reported the same ${action} move${signal.windowDays ? ` inside ${signal.windowDays} days` : ''}.`;
  }
  return `Multiple members of Congress independently reported the same ${action} move${signal.windowDays ? ` inside ${signal.windowDays} days` : ''}.`;
}

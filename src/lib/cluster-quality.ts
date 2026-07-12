export const HIGH_CONVICTION_CLUSTER_SCORE = 0.94;
export const MIN_INSIDER_CLUSTER_ACTORS = 5;
export const MIN_CROSS_SOURCE_CLUSTER_ACTORS = 3;

export function isHighConvictionCluster(signal: {
  actorCount: number;
  ruleKey: string;
  score: number;
}) {
  if (signal.score < HIGH_CONVICTION_CLUSTER_SCORE) {
    return false;
  }
  if (signal.ruleKey === 'insider_cluster') {
    return signal.actorCount >= MIN_INSIDER_CLUSTER_ACTORS;
  }
  if (signal.ruleKey === 'cross_source_accumulation') {
    return signal.actorCount >= MIN_CROSS_SOURCE_CLUSTER_ACTORS;
  }
  return signal.ruleKey === 'congress_cluster' && signal.actorCount >= 3;
}

export const HIGH_CONVICTION_CLUSTER_SCORE = 0.94;
export const MIN_INSIDER_CLUSTER_ACTORS = 5;
export const MIN_CONGRESS_CLUSTER_ACTORS = 4;

type ClusterQualitySignal = {
  actorCount: number;
  amountFloor?: number;
  direction?: 'buy' | 'sell' | null;
  ruleKey: string;
  score: number;
  sourceCounts?: {
    congress: number;
    insiders: number;
    funds: number;
  };
};

export function isHighConvictionCluster(signal: ClusterQualitySignal) {
  if (signal.ruleKey === 'insider_cluster') {
    if (signal.score < HIGH_CONVICTION_CLUSTER_SCORE || signal.actorCount < MIN_INSIDER_CLUSTER_ACTORS) {
      return false;
    }

    // Insider selling is routine compensation activity at many companies. A
    // seven-figure disclosed floor keeps the sell feed focused on material moves.
    return signal.direction !== 'sell' || (signal.amountFloor || 0) >= 1_000_000;
  }

  if (signal.ruleKey === 'cross_source_accumulation') {
    if (signal.score < HIGH_CONVICTION_CLUSTER_SCORE) {
      return false;
    }

    const counts = signal.sourceCounts;
    if (!counts) {
      return signal.actorCount >= 4;
    }

    // When insiders are part of the thesis, require five distinct insiders.
    // This prevents one person's repeated filings from manufacturing conviction.
    if (counts.insiders > 0 && counts.insiders < MIN_INSIDER_CLUSTER_ACTORS) {
      return false;
    }

    const sourceFamilyCount = [counts.congress, counts.insiders, counts.funds].filter((count) => count > 0).length;
    if (sourceFamilyCount === 3) {
      return true;
    }
    if (counts.congress >= 3 && counts.funds >= 1) {
      return true;
    }
    if (counts.congress >= 2 && counts.insiders >= MIN_INSIDER_CLUSTER_ACTORS) {
      return true;
    }
    return counts.insiders >= MIN_INSIDER_CLUSTER_ACTORS && counts.funds >= 1;
  }

  return (
    signal.ruleKey === 'congress_cluster' &&
    signal.score >= 0.84 &&
    signal.actorCount >= MIN_CONGRESS_CLUSTER_ACTORS
  );
}

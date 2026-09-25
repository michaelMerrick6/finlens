import type { AccountActorFollow } from '@/lib/account-types';

const strategies = [{
  id: 'trump',
  name: 'Trump strategy',
  actorKey: 'strategy:trump',
  href: '/analysis/strategies/trump',
}] as const;

export function strategyFollow(id: string) {
  return strategies.find(strategy => strategy.id === id) ?? null;
}

export function followedStrategy(follow: Pick<AccountActorFollow, 'actorType' | 'actorKey'>) {
  return follow.actorType === 'politician'
    ? strategies.find(strategy => strategy.actorKey === follow.actorKey) ?? null
    : null;
}

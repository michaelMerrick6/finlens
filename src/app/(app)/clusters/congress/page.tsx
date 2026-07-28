import type { Metadata } from 'next';

import CongressClusterCalendarGate from '@/components/CongressClusterCalendarGate';

export const metadata: Metadata = {
  title: 'Congress Cluster Calendar — Vail',
  description: 'Explore the companies attracting coordinated congressional buying by week, month, or year.',
};

export default function CongressClusterCalendarPage() {
  return (
    <div className="mx-auto max-w-[1380px] px-4 py-6 sm:px-6 lg:px-8">
      <CongressClusterCalendarGate />
    </div>
  );
}

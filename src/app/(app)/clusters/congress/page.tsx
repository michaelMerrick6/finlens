import type { Metadata } from 'next';

import CongressClusterCalendarGate from '@/components/CongressClusterCalendarGate';

export const metadata: Metadata = {
  title: 'Congress Accumulation — Vail',
  description: 'See where congressional buying is concentrating over the past week, month, or year.',
};

export default function CongressClusterCalendarPage() {
  return (
    <div className="px-4 py-6 sm:px-6 lg:px-8">
      <CongressClusterCalendarGate />
    </div>
  );
}

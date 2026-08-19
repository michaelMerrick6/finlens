import type { Metadata } from 'next';

import CongressBuyingCalendarGate from '@/components/CongressBuyingCalendarGate';

export const metadata: Metadata = {
  title: 'Congress Buying Calendar — Vail',
  description: 'Browse congressional stock purchases by trade date and review the public filings behind each day.',
};

export default function CongressBuyingCalendarPage() {
  return (
    <div className="px-4 py-6 sm:px-6 lg:px-8">
      <CongressBuyingCalendarGate />
    </div>
  );
}

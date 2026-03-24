import type { Metadata } from 'next';

import { AlertsWorkspace } from '@/components/AlertsWorkspace';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Alerts',
  description: 'Manage follows, delivery settings, and alert history.',
};

export default function AlertsPage() {
  return <AlertsWorkspace />;
}

import type { Metadata } from 'next';

import { SignalsPage } from '@/components/SignalsPage';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Signals — Vail',
  description: 'Create custom notifications for politician trades, insider filings, and hedge fund moves.',
};

export default function Signals() {
  return <SignalsPage />;
}

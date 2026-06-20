import type { Metadata } from 'next';

import { AuthPanel } from '@/components/AuthPanel';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Auth',
  description: 'Sign in to manage your Vail account.',
};

export default function AuthPage() {
  return <AuthPanel />;
}

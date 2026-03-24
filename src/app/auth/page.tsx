import type { Metadata } from 'next';

import { AuthPanel } from '@/components/AuthPanel';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Auth',
  description: 'Sign in to manage private Vail alerts.',
};

export default function AuthPage() {
  return <AuthPanel />;
}

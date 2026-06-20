import type { ReactNode } from 'react';

import AppTopNav from '@/components/AppTopNav';
import PageTransition from '@/components/PageTransition';

export default function AppLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-[#050505]">
      <AppTopNav />
      <main className="flex-1">
        <PageTransition>{children}</PageTransition>
      </main>
    </div>
  );
}

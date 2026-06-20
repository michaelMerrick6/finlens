import type { ReactNode } from 'react';

import PageTransition from '@/components/PageTransition';

export default function MarketingLayout({ children }: { children: ReactNode }) {
  return <PageTransition>{children}</PageTransition>;
}

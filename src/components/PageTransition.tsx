'use client';

import type { ReactNode } from 'react';
import { usePathname } from 'next/navigation';

export default function PageTransition({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="page-shell">
      <div key={pathname} className="page-enter">
        {children}
      </div>
    </div>
  );
}

import Link from 'next/link';
import { CalendarDays, ListOrdered } from 'lucide-react';

export default function CongressBuyingViewNav({ active }: { active: 'overview' | 'calendar' }) {
  return (
    <nav
      aria-label="Congress buying views"
      className="inline-flex w-full gap-1 rounded-xl border border-white/[0.08] bg-white/[0.018] p-1 sm:w-auto"
    >
      <Link
        href="/clusters/congress"
        aria-current={active === 'overview' ? 'page' : undefined}
        className={`inline-flex flex-1 items-center justify-center gap-2 rounded-lg px-3.5 py-2 text-xs font-medium transition sm:flex-none ${
          active === 'overview' ? 'bg-white/[0.1] text-white shadow-sm' : 'text-zinc-600 hover:text-zinc-300'
        }`}
      >
        <ListOrdered className="h-3.5 w-3.5" />
        Overview
      </Link>
      <Link
        href="/clusters/congress/calendar"
        aria-current={active === 'calendar' ? 'page' : undefined}
        className={`inline-flex flex-1 items-center justify-center gap-2 rounded-lg px-3.5 py-2 text-xs font-medium transition sm:flex-none ${
          active === 'calendar' ? 'bg-white/[0.1] text-white shadow-sm' : 'text-zinc-600 hover:text-zinc-300'
        }`}
      >
        <CalendarDays className="h-3.5 w-3.5" />
        Calendar
      </Link>
    </nav>
  );
}

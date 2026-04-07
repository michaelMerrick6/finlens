import Link from 'next/link';
import { ArrowUpRight, CircleDot, Sparkles } from 'lucide-react';

import SearchForm from '@/components/SearchForm';

const quickSearches = [
  'Nancy Pelosi',
  'NVDA',
  'Shelley Capito',
  'Berkshire Hathaway',
  'JPM',
  'AAPL',
];

const sourcePills = ['Congress', 'Form 4', '13F', 'Politicians', 'Insiders', 'Funds'];

export default function Home() {
  return (
    <div className="relative isolate overflow-hidden rounded-[2.5rem] border border-white/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.03),rgba(255,255,255,0.015))] px-4 py-10 shadow-[0_40px_120px_rgba(0,0,0,0.45)] sm:px-6 md:px-10 md:py-14">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(59,130,246,0.24),transparent_30%),radial-gradient(circle_at_80%_20%,rgba(14,165,233,0.16),transparent_24%),linear-gradient(rgba(255,255,255,0.04)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.04)_1px,transparent_1px)] bg-[size:auto,auto,28px_28px,28px_28px] bg-[position:0_0,0_0,-1px_-1px,-1px_-1px] opacity-80" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-cyan-300/60 to-transparent" />
      <div className="pointer-events-none absolute left-1/2 top-0 h-72 w-72 -translate-x-1/2 rounded-full bg-cyan-400/10 blur-3xl" />

      <div className="relative mx-auto flex min-h-[calc(100vh-16rem)] max-w-5xl flex-col items-center justify-center text-center">
        <div className="inline-flex items-center gap-2 rounded-full border border-cyan-300/15 bg-cyan-300/8 px-3 py-1.5 text-[11px] uppercase tracking-[0.28em] text-cyan-100">
          <Sparkles className="h-3.5 w-3.5" />
          Signal Search
        </div>

        <h1 className="mt-8 max-w-4xl text-5xl font-semibold tracking-[-0.06em] text-white sm:text-6xl md:text-7xl">
          Search the filing tape.
        </h1>

        <p className="mt-5 max-w-2xl text-sm leading-7 text-zinc-400 sm:text-base">
          Ticker, lawmaker, or company. Start with one query and jump straight into the filings.
        </p>

        <div className="mt-10 w-full">
          <SearchForm
            autoFocus
            placeholder="Search NVDA, Pelosi, Shelley Capito, Berkshire Hathaway..."
            submitLabel="Search"
            variant="hero"
          />
        </div>

        <div className="mt-5 flex flex-wrap items-center justify-center gap-3">
          {quickSearches.map((query) => (
            <Link
              key={query}
              href={`/search?q=${encodeURIComponent(query)}`}
              className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-cyan-300/30 hover:bg-white/8 hover:text-white"
            >
              <CircleDot className="h-3.5 w-3.5 text-cyan-300" />
              {query}
            </Link>
          ))}
        </div>

        <div className="mt-8 flex flex-wrap items-center justify-center gap-3 text-[11px] uppercase tracking-[0.22em] text-zinc-500">
          {sourcePills.map((pill) => (
            <span
              key={pill}
              className="rounded-full border border-white/8 bg-black/15 px-3 py-1.5"
            >
              {pill}
            </span>
          ))}
        </div>

        <div className="mt-10 inline-flex items-center gap-2 text-sm text-zinc-500">
          <span>Open search, type once, and follow the trail.</span>
          <ArrowUpRight className="h-4 w-4" />
        </div>
      </div>
    </div>
  );
}

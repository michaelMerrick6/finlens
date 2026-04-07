'use client';

import { useState, useTransition } from 'react';
import { useRouter } from 'next/navigation';
import { ArrowUpRight, Search } from 'lucide-react';

type SearchFormProps = {
  autoFocus?: boolean;
  className?: string;
  placeholder?: string;
  submitLabel?: string;
  variant?: 'default' | 'hero';
};

function compactClasses(value: string | undefined) {
  return value ? ` ${value}` : '';
}

export default function SearchForm({
  autoFocus = false,
  className,
  placeholder = 'Search politicians, tickers, or company names...',
  submitLabel = 'Analyze',
  variant = 'default',
}: SearchFormProps) {
  const [query, setQuery] = useState('');
  const [isPending, startTransition] = useTransition();
  const router = useRouter();

  const cleanQuery = query.trim();

  const handleSearch = (event: React.FormEvent) => {
    event.preventDefault();
    if (!cleanQuery) {
      return;
    }

    startTransition(() => {
      router.push(`/search?q=${encodeURIComponent(cleanQuery)}`);
    });
  };

  if (variant === 'hero') {
    return (
      <form onSubmit={handleSearch} className={`mx-auto w-full max-w-4xl${compactClasses(className)}`}>
        <div className="relative overflow-hidden rounded-[2rem] border border-white/12 bg-[linear-gradient(180deg,rgba(255,255,255,0.05),rgba(255,255,255,0.02))] p-3 shadow-[0_24px_80px_rgba(0,0,0,0.45)]">
          <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(125,211,252,0.12),transparent_28%),linear-gradient(180deg,rgba(255,255,255,0.04),transparent)]" />
          <div className="relative flex flex-col gap-3 md:flex-row md:items-center">
            <label className="flex flex-1 items-center gap-4 rounded-[1.4rem] border border-white/8 bg-black/20 px-4 py-4 text-left transition focus-within:border-cyan-300/40 focus-within:bg-black/30 md:px-5 md:py-5">
              <Search className="h-5 w-5 shrink-0 text-zinc-500" />
              <input
                autoFocus={autoFocus}
                type="text"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder={placeholder}
                className="w-full bg-transparent text-base text-white outline-none placeholder:text-zinc-500 md:text-lg"
              />
            </label>

            <button
              type="submit"
              disabled={!cleanQuery || isPending}
              className="inline-flex h-14 items-center justify-center gap-2 rounded-[1.2rem] bg-[linear-gradient(135deg,#7dd3fc,#38bdf8)] px-5 text-sm font-semibold text-slate-950 transition hover:brightness-105 disabled:cursor-not-allowed disabled:opacity-40 md:h-[4.5rem] md:min-w-[8.5rem] md:px-6"
            >
              <span>{isPending ? 'Opening…' : submitLabel}</span>
              <ArrowUpRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      </form>
    );
  }

  return (
    <form onSubmit={handleSearch} className={`group relative mt-8 w-full max-w-2xl${compactClasses(className)}`}>
      <div className="absolute inset-y-0 left-0 flex items-center pl-6 pointer-events-none">
        <Search className="h-6 w-6 text-gray-400 transition-colors group-focus-within:text-[var(--color-primary)]" />
      </div>
      <input
        autoFocus={autoFocus}
        type="text"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder={placeholder}
        className="w-full rounded-2xl border border-[rgba(255,255,255,0.1)] bg-[rgba(255,255,255,0.05)] py-5 pl-16 pr-16 text-xl text-white placeholder-gray-500 shadow-[0_0_30px_rgba(59,130,246,0.1)] transition-all focus:border-[var(--color-primary)] focus:outline-none focus:ring-4 focus:ring-blue-500/20 focus:shadow-[0_0_40px_rgba(59,130,246,0.2)]"
      />
      <button
        type="submit"
        disabled={!cleanQuery || isPending}
        className="absolute bottom-3 right-3 top-3 rounded-xl bg-[var(--color-primary)] px-6 font-semibold text-white shadow-lg shadow-blue-500/30 transition-all hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-50"
      >
        {isPending ? 'Opening…' : submitLabel}
      </button>
    </form>
  );
}

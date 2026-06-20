'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { type ReactNode, useCallback, useEffect, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Search, TrendingUp, X } from 'lucide-react';

import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import {
  COMPANY_LOGO_PROVIDER_NAME,
  COMPANY_LOGO_PROVIDER_URL,
  hasCompanyLogoSupport,
} from '@/lib/company-logos';
import { addRecentTicker } from '@/lib/recent-tickers';

type CompanyResult = {
  type: 'company';
  id: string;
  ticker: string;
  name: string;
  logoUrl: string | null;
  score: number;
  exactMatch: boolean;
  strongMatch: boolean;
};

type PoliticianResult = {
  type: 'politician';
  id: string;
  fullName: string;
  party: string;
  chamber: string;
  state: string;
  score: number;
  exactMatch: boolean;
  strongMatch: boolean;
};

type SearchResult = CompanyResult | PoliticianResult;

const PARTY_COLORS: Record<string, string> = {
  Democrat: '#3b82f6',
  Republican: '#ef4444',
  Independent: '#a855f7',
};

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function getHighlightTokens(query: string) {
  return [...new Set(query.trim().split(/\s+/).map((token) => token.trim()).filter(Boolean))]
    .filter((token) => token.length >= 2 || token === token.toUpperCase())
    .sort((left, right) => right.length - left.length);
}

function highlightText(text: string, query: string): ReactNode {
  const tokens = getHighlightTokens(query);
  if (!text || tokens.length === 0) {
    return text;
  }

  const matcher = new RegExp(`(${tokens.map((token) => escapeRegExp(token)).join('|')})`, 'ig');
  const parts = text.split(matcher);
  if (parts.length === 1) {
    return text;
  }

  return parts.map((part, index) => {
    const isMatch = tokens.some((token) => token.toLowerCase() === part.toLowerCase());
    if (!isMatch) {
      return <span key={`${part}-${index}`}>{part}</span>;
    }

    return (
      <mark
        key={`${part}-${index}`}
        className="rounded-[6px] bg-[#10b981]/14 px-1 py-0.5 text-[#7ee7c4]"
      >
        {part}
      </mark>
    );
  });
}

function isCompanyResult(result: SearchResult): result is CompanyResult {
  return result.type === 'company';
}

function pickPreferredResult(results: SearchResult[]): SearchResult | null {
  if (results.length === 0) {
    return null;
  }

  const companyResults = results.filter(isCompanyResult);
  const exactCompany = companyResults.find((result) => result.exactMatch);
  if (exactCompany) {
    return exactCompany;
  }

  const exactAny = results.find((result) => result.exactMatch);
  if (exactAny) {
    return exactAny;
  }

  const strongCompany = companyResults.find((result) => result.strongMatch);
  if (strongCompany) {
    return strongCompany;
  }

  return companyResults[0] || results[0] || null;
}

function TickerAvatar({ ticker, logoUrl }: { ticker: string; logoUrl: string | null }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;

  // Generate a consistent hue from the ticker string
  const hue = ticker.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <div className="h-9 w-9 shrink-0 overflow-hidden rounded-lg border border-white/10 bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={ticker}
          width={36}
          height={36}
          sizes="36px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-white/10 text-[13px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </div>
  );
}

export type { SearchResult, CompanyResult, PoliticianResult };

export default function StockSearchBar({
  className,
  onSelect,
  compact = false,
  placeholder = 'Search for politicians or stocks...',
}: {
  className?: string;
  onSelect?: (result: SearchResult) => void;
  compact?: boolean;
  placeholder?: string;
}) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);

  const router = useRouter();
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const latestQueryRef = useRef('');

  const fetchResults = useCallback(async (q: string) => {
    if (q.length < 1) {
      setResults([]);
      setIsOpen(false);
      setIsLoading(false);
      return [] as SearchResult[];
    }

    setIsLoading(true);
    try {
      const response = await fetch(`/api/search-autocomplete?q=${encodeURIComponent(q)}`, {
        cache: 'no-store',
      });
      if (!response.ok) throw new Error('fetch failed');
      const data = (await response.json()) as { results: SearchResult[] };
      // Only apply if query hasn't changed since we fired
      if (q === latestQueryRef.current) {
        setResults(data.results);
        setIsOpen(data.results.length > 0);
        setActiveIndex(-1);
      }
      return data.results;
    } catch {
      if (q === latestQueryRef.current) {
        setResults([]);
        setIsOpen(false);
      }
      return [] as SearchResult[];
    } finally {
      if (q === latestQueryRef.current) {
        setIsLoading(false);
      }
    }
  }, []);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setQuery(value);
    latestQueryRef.current = value;

    if (debounceRef.current) clearTimeout(debounceRef.current);

    if (!value.trim()) {
      setResults([]);
      setIsOpen(false);
      setIsLoading(false);
      return;
    }

    setIsLoading(true);
    debounceRef.current = setTimeout(() => {
      void fetchResults(value.trim());
    }, 180);
  };

  const handleSelect = useCallback((result: SearchResult) => {
    setIsOpen(false);
    setQuery('');
    latestQueryRef.current = '';
    setResults([]);
    if (result.type === 'company') {
      addRecentTicker(result.ticker.toUpperCase());
    }
    if (onSelect) {
      onSelect(result);
      return;
    }
    if (result.type === 'company') {
      router.push(`/ticker/${result.ticker}`);
    } else {
      router.push(`/politician/${result.id}`);
    }
  }, [onSelect, router]);

  const commitQuerySearch = useCallback((overrideQuery?: string) => {
    if (onSelect) {
      return;
    }
    const trimmedQuery = (overrideQuery ?? query).trim();
    if (!trimmedQuery) {
      return;
    }

    router.push(`/search?q=${encodeURIComponent(trimmedQuery)}`);
  }, [onSelect, query, router]);

  const handleEnterSelection = useCallback(async () => {
    const trimmedQuery = query.trim();
    if (!trimmedQuery) {
      return;
    }

    if (activeIndex >= 0 && activeIndex < results.length) {
      handleSelect(results[activeIndex]);
      return;
    }

    const preferredCurrent = pickPreferredResult(results);
    if (preferredCurrent) {
      handleSelect(preferredCurrent);
      return;
    }

    const fetchedResults = await fetchResults(trimmedQuery);
    const preferredFetched = pickPreferredResult(fetchedResults);
    if (preferredFetched) {
      handleSelect(preferredFetched);
      return;
    }

    commitQuerySearch(trimmedQuery);
  }, [activeIndex, commitQuerySearch, fetchResults, handleSelect, query, results]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      if (results.length === 0) return;
      e.preventDefault();
      if (!isOpen) {
        setIsOpen(true);
      }
      setActiveIndex((prev) => (prev < results.length - 1 ? prev + 1 : prev));
    } else if (e.key === 'ArrowUp') {
      if (results.length === 0) return;
      e.preventDefault();
      if (!isOpen) {
        setIsOpen(true);
      }
      setActiveIndex((prev) => (prev > 0 ? prev - 1 : 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      void handleEnterSelection();
    } else if (e.key === 'Escape') {
      setIsOpen(false);
      setActiveIndex(-1);
      inputRef.current?.blur();
    }
  };

  const clear = () => {
    setQuery('');
    setResults([]);
    setIsOpen(false);
    setIsLoading(false);
    latestQueryRef.current = '';
    inputRef.current?.focus();
  };

  // Close dropdown when clicking outside
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        inputRef.current &&
        !inputRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  return (
    <div className={`relative w-full${className ? ` ${className}` : ''}`}>
      <div
        className={`relative flex items-center border transition-all duration-200 ${
          compact
            ? 'rounded-lg border-white/[0.08] bg-white/[0.03] shadow-none focus-within:border-emerald-500/30 focus-within:bg-white/[0.05]'
            : 'rounded-[1.35rem] border-white/[0.08] bg-[linear-gradient(180deg,rgba(23,24,29,0.96),rgba(18,19,24,0.96))] shadow-[0_18px_40px_rgba(0,0,0,0.18)] focus-within:border-emerald-400/22 focus-within:bg-[linear-gradient(180deg,rgba(27,29,35,0.98),rgba(20,22,28,0.98))] focus-within:shadow-[0_0_0_3px_rgba(16,185,129,0.06),0_18px_42px_rgba(0,0,0,0.24)]'
        }`}
      >
        <Search
          className={`pointer-events-none absolute ${compact ? 'left-3 h-4 w-4' : 'left-4 h-5 w-5'} shrink-0 text-zinc-600`}
          aria-hidden
        />
        {isLoading && (
          <div className={`pointer-events-none absolute ${compact ? 'right-10' : 'right-12'} flex items-center`}>
            <div className={`animate-spin rounded-full border-2 border-white/10 ${compact ? 'h-3.5 w-3.5 border-t-emerald-400' : 'h-4 w-4 border-t-cyan-400'}`} />
          </div>
        )}
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onFocus={() => {
            if (results.length > 0) setIsOpen(true);
          }}
          placeholder={placeholder}
          autoComplete="off"
          spellCheck={false}
          className={`w-full bg-transparent ${compact ? 'h-10 pl-10 pr-10 text-sm' : 'py-[1.15rem] pl-12 pr-12 text-[1.02rem]'} text-white outline-none placeholder:text-zinc-600`}
          aria-label="Search"
          aria-autocomplete="list"
        />
        {query && (
          <button
            type="button"
            onClick={clear}
            className={`absolute ${compact ? 'right-3' : 'right-4'} flex h-5 w-5 items-center justify-center rounded-full text-zinc-500 transition hover:bg-white/10 hover:text-white`}
            tabIndex={-1}
            aria-label="Clear search"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </div>

      {isOpen && results.length > 0 && (
        <div
          ref={dropdownRef}
          className={`absolute left-0 right-0 top-full z-[120] overflow-hidden border border-white/[0.10] bg-[#111114] shadow-[0_28px_90px_rgba(0,0,0,0.72)] ring-1 ring-black/40 backdrop-blur-xl ${
            compact ? 'mt-2 rounded-xl' : 'mt-3 rounded-[1.15rem]'
          }`}
          role="listbox"
        >
          {(() => {
            const companyItems = results.filter((r) => r.type === 'company') as CompanyResult[];
            const politicianItems = results.filter((r) => r.type === 'politician') as PoliticianResult[];
            const showLogoAttribution =
              companyItems.some((item) => Boolean(item.logoUrl)) && hasCompanyLogoSupport();
            let globalIndex = 0;

            const renderCompany = (item: CompanyResult) => {
              const isActive = globalIndex === activeIndex;
              const currentGlobalIndex = globalIndex;
              globalIndex++;
              return (
                <button
                  key={`company-${item.ticker}`}
                  role="option"
                  aria-selected={isActive}
                  onMouseEnter={() => setActiveIndex(currentGlobalIndex)}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => handleSelect(item)}
                  className={`flex w-full items-center gap-3 px-4 py-3 text-left transition-colors ${
                    isActive ? 'bg-white/[0.07]' : 'hover:bg-white/[0.04]'
                  }`}
                >
                  <TickerAvatar ticker={item.ticker} logoUrl={item.logoUrl} />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-white">{highlightText(item.ticker, query)}</span>
                      {item.exactMatch && (
                        <span className="rounded-full bg-cyan-400/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-cyan-300">
                          exact
                        </span>
                      )}
                    </div>
                    <div className="truncate text-sm text-zinc-400">{highlightText(item.name, query)}</div>
                  </div>
                  <TrendingUp className="h-3.5 w-3.5 shrink-0 text-zinc-600" />
                </button>
              );
            };

            const renderPolitician = (item: PoliticianResult) => {
              const isActive = globalIndex === activeIndex;
              const currentGlobalIndex = globalIndex;
              globalIndex++;
              const partyColor = PARTY_COLORS[item.party] ?? '#6b7280';
              return (
                <button
                  key={`politician-${item.id}`}
                  role="option"
                  aria-selected={isActive}
                  onMouseEnter={() => setActiveIndex(currentGlobalIndex)}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => handleSelect(item)}
                  className={`flex w-full items-center gap-3 px-4 py-3 text-left transition-colors ${
                    isActive ? 'bg-white/[0.07]' : 'hover:bg-white/[0.04]'
                  }`}
                >
                  <PoliticianHeadshot
                    memberId={item.id}
                    name={item.fullName}
                    party={item.party}
                    size={36}
                  />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-white">{highlightText(item.fullName, query)}</span>
                      {item.exactMatch && (
                        <span className="rounded-full bg-cyan-400/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-cyan-300">
                          exact
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-1.5 text-sm text-zinc-400">
                      <span
                        className="inline-block h-1.5 w-1.5 rounded-full"
                        style={{ background: partyColor }}
                      />
                      {item.party} · {item.chamber === 'senate' ? 'Sen.' : 'Rep.'} · {item.state}
                    </div>
                  </div>
                </button>
              );
            };

            return (
              <>
                <div className="max-h-[420px] overflow-y-auto bg-[#111114]">
                  {politicianItems.length > 0 && (
                    <>
                      <div className="border-b border-white/[0.06] bg-[#0f0f12] px-4 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-600">
                        Politicians
                      </div>
                      {politicianItems.map((item) => renderPolitician(item))}
                    </>
                  )}
                  {companyItems.length > 0 && (
                    <>
                      <div className={`bg-[#0f0f12] px-4 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-600${politicianItems.length > 0 ? ' border-t border-white/[0.06]' : ''}`}>
                        Tickers &amp; Companies
                      </div>
                      {companyItems.map((item) => renderCompany(item))}
                    </>
                  )}
                </div>

                <div className="flex items-center justify-between gap-3 border-t border-white/[0.06] bg-[#0f0f12] px-4 py-2 text-[11px] text-zinc-600">
                  {showLogoAttribution ? (
                    <a
                      href={COMPANY_LOGO_PROVIDER_URL}
                      target="_blank"
                      rel="noopener"
                      className="transition hover:text-zinc-300"
                    >
                      Company logos via {COMPANY_LOGO_PROVIDER_NAME}
                    </a>
                  ) : (
                    <span />
                  )}
                  <span>↑↓ to navigate · Enter to select · Esc to close</span>
                </div>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}

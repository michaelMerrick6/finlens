'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import {
  ArrowUpRight,
  ExternalLink,
  Loader2,
  Search,
  TrendingDown,
  TrendingUp,
} from 'lucide-react';

import OptionTradeBadge from '@/components/OptionTradeBadge';
import PoliticianProfileModal, { type PoliticianProfileFallbackSummary } from '@/components/PoliticianProfileModal';
import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import { getTickerLogoUrl } from '@/lib/company-logos';
import { formatCalendarDate } from '@/lib/date-format';
import type { PoliticianProfileData } from '@/lib/politician-profile-server';
import { parsePoliticianOptionDetails, stripPoliticianOptionMetadata } from '@/lib/politician-option-trades';
import { getPartyPresentation } from '@/lib/politics';

interface Trade {
  id: string;
  member_id: string;
  politician_name: string;
  ticker: string;
  asset_name?: string | null;
  asset_type?: string | null;
  transaction_type: string;
  amount_range: string;
  published_date: string;
  transaction_date: string;
  source_url: string;
  chamber: string;
  congress_members: {
    first_name: string;
    last_name: string;
    party: string;
    chamber: string;
    state: string;
  } | null;
}

type SearchSuggestion =
  | {
      type: 'company';
      id: string;
      ticker: string;
      name: string;
    }
  | {
      type: 'politician';
      id: string;
      fullName: string;
      party: string;
      chamber: string;
      state: string;
    };

const CHAMBER_OPTIONS = ['All', 'House', 'Senate'] as const;
const PAGE_SIZE = 20;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;
const DIRECTION_OPTIONS = [
  { value: 'All', label: 'All', tone: 'default' as const },
  { value: 'buy', label: 'Buy', tone: 'buy' as const },
  { value: 'sell', label: 'Sell', tone: 'sell' as const },
];

function FilterPill({
  active,
  onClick,
  children,
  tone = 'default',
}: {
  active: boolean;
  onClick: () => void;
  children: string;
  tone?: 'default' | 'buy' | 'sell';
}) {
  const activeClass =
    tone === 'buy'
      ? 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300'
      : tone === 'sell'
        ? 'border-red-500/25 bg-red-500/10 text-red-300'
        : 'border-white/12 bg-white/[0.06] text-white';

  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-md border px-2.5 py-1.5 text-[11px] font-medium transition ${
        active ? activeClass : 'border-transparent text-zinc-500 hover:bg-white/[0.03] hover:text-zinc-300'
      }`}
    >
      {children}
    </button>
  );
}

function normalizeTransactionType(value: string | null | undefined) {
  return String(value || '').trim().toLowerCase();
}

function isBuyTrade(trade: Trade) {
  const normalized = normalizeTransactionType(trade.transaction_type);
  return normalized === 'buy' || normalized === 'purchase';
}

function displayAssetLabel(trade: Trade) {
  if (trade.ticker === 'US-TREAS') {
    return stripPoliticianOptionMetadata(trade.asset_name) || 'U.S. Treasury';
  }
  if (trade.ticker && trade.ticker !== 'N/A' && trade.ticker !== 'UNKNOWN') {
    return trade.ticker;
  }
  return stripPoliticianOptionMetadata(trade.asset_name) || 'Unmapped Asset';
}

function hasTickerPage(trade: Trade) {
  return Boolean(trade.ticker && !['N/A', 'UNKNOWN', 'US-TREAS'].includes(trade.ticker));
}

function TickerAssetLogo({ ticker, label }: { ticker: string | null | undefined; label: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const normalizedTicker = String(ticker || '').trim().toUpperCase();
  const resolvedLogoUrl = normalizedTicker ? getTickerLogoUrl(normalizedTicker, 40) : null;
  const logoUrl = resolvedLogoUrl && failedUrl !== resolvedLogoUrl ? resolvedLogoUrl : null;

  if (logoUrl) {
    return (
      <span className="flex h-7 w-7 shrink-0 items-center justify-center overflow-hidden rounded-lg border border-white/[0.08] bg-black/35">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt=""
          aria-hidden="true"
          width={28}
          height={28}
          sizes="28px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(logoUrl)}
        />
      </span>
    );
  }

  return (
    <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-white/[0.08] bg-white/[0.04] text-[9px] font-semibold tracking-tight text-zinc-400">
      {normalizedTicker.slice(0, 2) || label.slice(0, 2).toUpperCase()}
    </span>
  );
}

function sourceLabel(sourceUrl: string | null | undefined) {
  if (!sourceUrl) {
    return 'No source';
  }
  return sourceUrl.includes('efdsearch.senate.gov') ? 'View Filing' : 'View PDF';
}

function appendUniqueTrades(current: Trade[], incoming: Trade[]) {
  const merged = new Map(current.map((trade) => [trade.id, trade]));
  incoming.forEach((trade) => merged.set(trade.id, trade));
  return [...merged.values()];
}

function profileFallbackFromTrade(trade: Trade | null | undefined): PoliticianProfileFallbackSummary | null {
  if (!trade) {
    return null;
  }

  const displayName = trade.congress_members
    ? `${trade.congress_members.first_name} ${trade.congress_members.last_name}`.trim()
    : String(trade.politician_name || '').trim();

  return {
    displayName: displayName || 'Politician Profile',
    party: trade.congress_members?.party || null,
    chamber: trade.congress_members?.chamber || trade.chamber || null,
    state: trade.congress_members?.state || null,
    latestTradeDate: trade.transaction_date || trade.published_date || null,
  };
}

export default function PoliticiansFeed({ initialTrades }: { initialTrades: Trade[] }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [baseTrades, setBaseTrades] = useState<Trade[]>(initialTrades);
  const [trades, setTrades] = useState<Trade[]>(initialTrades);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchSuggestions, setSearchSuggestions] = useState<SearchSuggestion[]>([]);
  const [suggestionsOpen, setSuggestionsOpen] = useState(false);
  const [chamberFilter, setChamberFilter] = useState<(typeof CHAMBER_OPTIONS)[number]>('All');
  const [directionFilter, setDirectionFilter] = useState<(typeof DIRECTION_OPTIONS)[number]['value']>('All');
  const [isSearching, setIsSearching] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState('');
  const [baseHasMore, setBaseHasMore] = useState(initialTrades.length >= PAGE_SIZE);
  const [baseNextOffset, setBaseNextOffset] = useState(initialTrades.length);
  const [filteredHasMore, setFilteredHasMore] = useState(false);
  const [filteredNextOffset, setFilteredNextOffset] = useState(0);
  const [selectedMemberId, setSelectedMemberId] = useState<string | null>(null);
  const [selectedMemberName, setSelectedMemberName] = useState('');
  const [selectedProfileFallback, setSelectedProfileFallback] = useState<PoliticianProfileFallbackSummary | null>(null);
  const [isProfileLoading, setIsProfileLoading] = useState(false);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileCache, setProfileCache] = useState<Record<string, PoliticianProfileData>>({});
  const debounceRef = useRef<NodeJS.Timeout | null>(null);
  const autocompleteDebounceRef = useRef<NodeJS.Timeout | null>(null);
  const autocompleteRequestRef = useRef<AbortController | null>(null);
  const searchInputFocusedRef = useRef(false);
  const suppressNextSuggestionsRef = useRef(false);
  const attemptedLiveInitialLoadRef = useRef(false);
  const hasFilters = Boolean(searchQuery.trim() || chamberFilter !== 'All' || directionFilter !== 'All');
  const displayedTrades = hasFilters ? trades : baseTrades;
  const hasMore = hasFilters ? filteredHasMore : baseHasMore;
  const profileFromUrl = searchParams.get('profile');

  useEffect(() => {
    if (initialTrades.length > 0) {
      setBaseTrades(initialTrades);
      setTrades(initialTrades);
      setBaseHasMore(initialTrades.length >= PAGE_SIZE);
      setBaseNextOffset(initialTrades.length);
    }
  }, [initialTrades]);

  useEffect(() => {
    if (hasFilters || baseTrades.length > 0 || attemptedLiveInitialLoadRef.current) {
      return;
    }

    let cancelled = false;
    attemptedLiveInitialLoadRef.current = true;
    setIsSearching(true);

    (async () => {
      try {
        const res = await fetch(`/api/search-trades?limit=${PAGE_SIZE}`, { cache: 'no-store' });
        const json = await res.json();
        if (!cancelled) {
          const liveTrades = (json.trades || []) as Trade[];
          setBaseTrades(liveTrades);
          setTrades(liveTrades);
          setBaseHasMore(Boolean(json.hasMore));
          setBaseNextOffset(Number(json.nextOffset) || liveTrades.length);
        }
      } catch (error) {
        console.error('Initial politician feed load failed:', error);
      } finally {
        if (!cancelled) {
          setIsSearching(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [baseTrades.length, hasFilters]);

  useEffect(() => {
    setLoadMoreError('');
    if (!hasFilters) {
      setIsSearching(false);
      setFilteredHasMore(false);
      setFilteredNextOffset(0);
      return;
    }

    if (debounceRef.current) clearTimeout(debounceRef.current);
    const controller = new AbortController();
    setIsSearching(true);
    setTrades([]);
    setFilteredHasMore(false);
    setFilteredNextOffset(0);

    debounceRef.current = setTimeout(async () => {
      try {
        const params = new URLSearchParams();
        if (searchQuery.trim()) params.set('q', searchQuery.trim());
        if (chamberFilter !== 'All') params.set('chamber', chamberFilter);
        if (directionFilter !== 'All') params.set('direction', directionFilter);
        params.set('limit', String(PAGE_SIZE));

        const res = await fetch(`/api/search-trades?${params.toString()}`, {
          cache: 'no-store',
          signal: controller.signal,
        });
        const json = await res.json();
        setTrades(json.trades || []);
        setFilteredHasMore(Boolean(json.hasMore));
        setFilteredNextOffset(Number(json.nextOffset) || PAGE_SIZE);
      } catch (error) {
        if (!controller.signal.aborted) {
          console.error('Search failed:', error);
        }
      } finally {
        if (!controller.signal.aborted) {
          setIsSearching(false);
        }
      }
    }, 400);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      controller.abort();
    };
  }, [hasFilters, searchQuery, chamberFilter, directionFilter]);

  useEffect(() => {
    if (autocompleteDebounceRef.current) clearTimeout(autocompleteDebounceRef.current);
    autocompleteRequestRef.current?.abort();

    if (suppressNextSuggestionsRef.current) {
      suppressNextSuggestionsRef.current = false;
      setSearchSuggestions([]);
      setSuggestionsOpen(false);
      return;
    }

    const trimmedQuery = searchQuery.trim();
    if (trimmedQuery.length < 2) {
      setSearchSuggestions([]);
      setSuggestionsOpen(false);
      return;
    }

    const controller = new AbortController();
    autocompleteRequestRef.current = controller;
    autocompleteDebounceRef.current = setTimeout(async () => {
      try {
        const response = await fetch(`/api/search-autocomplete?q=${encodeURIComponent(trimmedQuery)}`, {
          cache: 'no-store',
          signal: controller.signal,
        });
        const json = await response.json();
        if (!response.ok) {
          throw new Error(json.error || 'Failed to find matching companies.');
        }
        const results = (json.results || []) as SearchSuggestion[];
        setSearchSuggestions(results);
        setSuggestionsOpen(searchInputFocusedRef.current && results.length > 0);
      } catch (error) {
        if (!controller.signal.aborted) {
          console.error('Autocomplete failed:', error);
        }
      }
    }, 180);

    return () => {
      if (autocompleteDebounceRef.current) clearTimeout(autocompleteDebounceRef.current);
      controller.abort();
    };
  }, [searchQuery]);

  useEffect(() => {
    if (!profileFromUrl) {
      setSelectedMemberId(null);
      setSelectedMemberName('');
      setSelectedProfileFallback(null);
      setProfileError(null);
      setIsProfileLoading(false);
      return;
    }

    const trade = [...displayedTrades, ...baseTrades].find((candidate) => candidate.member_id === profileFromUrl);
    const fallbackSummary = profileFallbackFromTrade(trade);
    const fallbackName = fallbackSummary?.displayName || 'Politician Profile';

    setSelectedMemberId(profileFromUrl);
    setSelectedMemberName(fallbackName);
    setSelectedProfileFallback(fallbackSummary);
    setProfileError(null);

    if (profileCache[profileFromUrl]) {
      setIsProfileLoading(false);
      return;
    }

    let cancelled = false;
    setIsProfileLoading(true);

    (async () => {
      try {
        const response = await fetch(`/api/politician-profile/${profileFromUrl}`);
        const json = await response.json();
        if (!response.ok) {
          throw new Error(json.error || 'Failed to load profile.');
        }
        if (!cancelled) {
          setProfileCache((current) => ({ ...current, [profileFromUrl]: json as PoliticianProfileData }));
        }
      } catch (error) {
        if (!cancelled) {
          setProfileError(error instanceof Error ? error.message : 'Failed to load profile.');
        }
      } finally {
        if (!cancelled) {
          setIsProfileLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [baseTrades, displayedTrades, profileCache, profileFromUrl]);

  function openProfile(trade: Trade) {
    if (!trade.member_id) {
      return;
    }
    const fallbackSummary = profileFallbackFromTrade(trade);
    setSelectedMemberId(trade.member_id);
    setSelectedMemberName(fallbackSummary?.displayName || trade.politician_name || 'Politician Profile');
    setSelectedProfileFallback(fallbackSummary);
    setProfileError(null);
    setIsProfileLoading(!profileCache[trade.member_id]);

    const params = new URLSearchParams(searchParams.toString());
    params.set('profile', trade.member_id);
    router.replace(`/politicians?${params.toString()}`, { scroll: false });
  }

  function closeProfile() {
    const params = new URLSearchParams(searchParams.toString());
    params.delete('profile');
    const nextQuery = params.toString();
    router.replace(nextQuery ? `/politicians?${nextQuery}` : '/politicians', { scroll: false });
  }

  function clearFilters() {
    setSearchQuery('');
    setSearchSuggestions([]);
    setSuggestionsOpen(false);
    setChamberFilter('All');
    setDirectionFilter('All');
  }

  function selectSearchSuggestion(suggestion: SearchSuggestion) {
    suppressNextSuggestionsRef.current = true;
    autocompleteRequestRef.current?.abort();
    setSearchSuggestions([]);
    setSuggestionsOpen(false);
    setSearchQuery(suggestion.type === 'company' ? suggestion.ticker : suggestion.fullName);
  }

  async function loadMore() {
    if (isLoadingMore || !hasMore) {
      return;
    }

    setIsLoadingMore(true);
    setLoadMoreError('');
    try {
      const params = new URLSearchParams();
      if (searchQuery.trim()) params.set('q', searchQuery.trim());
      if (chamberFilter !== 'All') params.set('chamber', chamberFilter);
      if (directionFilter !== 'All') params.set('direction', directionFilter);
      params.set('limit', String(PAGE_SIZE));
      params.set('offset', String(hasFilters ? filteredNextOffset : baseNextOffset));

      const res = await fetch(`/api/search-trades?${params.toString()}`, { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error || 'Failed to load more filings.');
      }
      const nextTrades = (json.trades || []) as Trade[];
      const nextHasMore = Boolean(json.hasMore) && nextTrades.length > 0;

      if (hasFilters) {
        setTrades((current) => appendUniqueTrades(current, nextTrades));
        setFilteredHasMore(nextHasMore);
        setFilteredNextOffset(Number(json.nextOffset) || filteredNextOffset + PAGE_SIZE);
      } else {
        setBaseTrades((current) => appendUniqueTrades(current, nextTrades));
        setBaseHasMore(nextHasMore);
        setBaseNextOffset(Number(json.nextOffset) || baseNextOffset + PAGE_SIZE);
      }
    } catch (error) {
      console.error('Load more politician filings failed:', error);
      setLoadMoreError(error instanceof Error ? error.message : 'Could not load more filings.');
    } finally {
      setIsLoadingMore(false);
    }
  }

  return (
    <>
      <div className="space-y-3">
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] px-3.5 py-3">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div className="w-full max-w-lg">
              <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                Search
              </label>
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
                {isSearching ? (
                  <Loader2 className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-blue-400" />
                ) : null}
                <input
                  type="text"
                  placeholder="Search politician, ticker, or company..."
                  value={searchQuery}
                  onChange={(event) => setSearchQuery(event.target.value)}
                  onFocus={() => {
                    searchInputFocusedRef.current = true;
                    setSuggestionsOpen(searchSuggestions.length > 0);
                  }}
                  onBlur={() => {
                    searchInputFocusedRef.current = false;
                    setSuggestionsOpen(false);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === 'Escape') {
                      setSuggestionsOpen(false);
                    }
                    if (event.key === 'Enter' && suggestionsOpen && searchSuggestions.length > 0) {
                      event.preventDefault();
                      selectSearchSuggestion(searchSuggestions[0]);
                    }
                  }}
                  autoComplete="off"
                  spellCheck={false}
                  className="h-10 w-full rounded-lg border border-white/[0.08] bg-white/[0.03] pl-10 pr-10 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-blue-500/30 focus:bg-white/[0.05]"
                />
                {suggestionsOpen ? (
                  <div className="absolute left-0 right-0 top-full z-50 mt-2 overflow-hidden rounded-xl border border-white/[0.1] bg-[#0d0d0f] shadow-2xl shadow-black/50">
                    {searchSuggestions.slice(0, 6).map((suggestion) => (
                      <button
                        key={`${suggestion.type}-${suggestion.id}`}
                        type="button"
                        onMouseDown={(event) => event.preventDefault()}
                        onClick={() => selectSearchSuggestion(suggestion)}
                        className="flex w-full items-center gap-3 border-b border-white/[0.05] px-3 py-2.5 text-left transition last:border-b-0 hover:bg-white/[0.05]"
                      >
                        {suggestion.type === 'company' ? (
                          <TickerAssetLogo ticker={suggestion.ticker} label={suggestion.name} />
                        ) : (
                          <PoliticianHeadshot
                            memberId={suggestion.id}
                            name={suggestion.fullName}
                            party={suggestion.party}
                            size={28}
                          />
                        )}
                        <span className="min-w-0 flex-1">
                          <span className="block truncate text-sm font-medium text-zinc-100">
                            {suggestion.type === 'company' ? suggestion.ticker : suggestion.fullName}
                          </span>
                          <span className="mt-0.5 block truncate text-[11px] text-zinc-500">
                            {suggestion.type === 'company'
                              ? suggestion.name
                              : `${suggestion.party || 'Congress'} · ${suggestion.chamber || 'Congress'}${suggestion.state ? ` · ${suggestion.state}` : ''}`}
                          </span>
                        </span>
                        <span className="text-[9px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                          {suggestion.type === 'company' ? 'Company' : 'Politician'}
                        </span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>
            </div>

            <div className="flex flex-col gap-2.5 sm:flex-row sm:flex-wrap sm:items-end xl:justify-end">
              <div className="min-w-0">
                <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Chamber</div>
                <div className="inline-flex rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5">
                  {CHAMBER_OPTIONS.map((option) => (
                    <FilterPill
                      key={option}
                      active={chamberFilter === option}
                      onClick={() => setChamberFilter(option)}
                    >
                      {option}
                    </FilterPill>
                  ))}
                </div>
              </div>

              <div className="min-w-0">
                <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Direction</div>
                <div className="inline-flex rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5">
                  {DIRECTION_OPTIONS.map((option) => (
                    <FilterPill
                      key={option.value}
                      active={directionFilter === option.value}
                      onClick={() => setDirectionFilter(option.value)}
                      tone={option.tone}
                    >
                      {option.label}
                    </FilterPill>
                  ))}
                </div>
              </div>

              {hasFilters ? (
                <button
                  type="button"
                  onClick={clearFilters}
                  className="h-9 rounded-lg px-3 text-xs font-medium text-zinc-500 transition hover:bg-white/[0.03] hover:text-zinc-300"
                >
                  Clear filters
                </button>
              ) : null}
            </div>
          </div>

          <div className="mt-2.5 text-xs text-zinc-500">
            {isSearching ? (
              <span className="inline-flex items-center gap-2">
                <Loader2 className="h-3.5 w-3.5 animate-spin text-blue-400" />
                Searching filings...
              </span>
            ) : (
              <>
                {displayedTrades.length.toLocaleString()} filings
                {searchQuery.trim() ? (
                  <>
                    {' '}
                    matching <span className="text-blue-300">{searchQuery.trim()}</span>
                  </>
                ) : null}
                {chamberFilter !== 'All' ? (
                  <>
                    {' '}
                    in <span className="text-zinc-300">{chamberFilter}</span>
                  </>
                ) : null}
                {directionFilter !== 'All' ? (
                  <>
                    {' '}
                    filtered to{' '}
                    <span className={directionFilter === 'buy' ? 'text-emerald-300' : 'text-red-300'}>
                      {directionFilter === 'buy' ? 'buys' : 'sells'}
                    </span>
                  </>
                ) : null}
              </>
            )}
          </div>
        </div>

        <div className="overflow-hidden rounded-2xl border border-white/[0.06] bg-white/[0.02]">
          <div className="overflow-x-auto">
            <table className="min-w-[1120px] w-full border-collapse text-left">
              <thead>
                <tr className="border-b border-white/[0.08] bg-white/[0.03]">
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Politician</th>
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Seat</th>
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Asset</th>
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Type</th>
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Amount</th>
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Filed</th>
                  <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Trade Date</th>
                  <th className="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Source</th>
                </tr>
              </thead>
              <tbody>
                {displayedTrades.map((trade) => {
                  const member = trade.congress_members;
                  const memberName = member ? `${member.first_name} ${member.last_name}` : trade.politician_name;
                  const partyPresentation = member ? getPartyPresentation(member.party, trade.member_id) : null;
                  const optionDetails = parsePoliticianOptionDetails(trade);
                  const isBuy = isBuyTrade(trade);

                  return (
                    <tr
                      key={trade.id}
                      className="group border-b border-white/[0.04] transition-colors hover:bg-white/[0.03] last:border-b-0"
                    >
                      <td className="px-4 py-3.5">
                        <button
                          type="button"
                          onClick={() => openProfile(trade)}
                          className="flex items-center gap-3 text-left"
                        >
                          <PoliticianHeadshot
                            memberId={trade.member_id}
                            name={memberName}
                            party={member?.party || null}
                            size={38}
                          />
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium text-white transition-colors group-hover:text-blue-300">
                              {memberName}
                            </div>
                            <div className="mt-0.5 flex flex-wrap items-center gap-2 text-[11px] text-zinc-500">
                              {partyPresentation ? (
                                <span title={partyPresentation.label} style={{ color: partyPresentation.color }}>
                                  {partyPresentation.label}
                                </span>
                              ) : null}
                            </div>
                          </div>
                        </button>
                      </td>
                      <td className="px-4 py-3.5">
                        <div className="text-sm text-zinc-300">{member?.chamber || trade.chamber || 'Congress'}</div>
                        <div className="mt-0.5 text-[11px] text-zinc-600">{member?.state || '—'}</div>
                      </td>
                      <td className="px-4 py-3.5">
                        {hasTickerPage(trade) ? (
                          <Link
                            href={`/ticker/${trade.ticker}`}
                            className="inline-flex max-w-[240px] items-center gap-1.5 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-sm font-semibold text-white transition hover:border-white/[0.14] hover:bg-white/[0.05]"
                          >
                            <TickerAssetLogo ticker={trade.ticker} label={displayAssetLabel(trade)} />
                            <span className="truncate">{displayAssetLabel(trade)}</span>
                            <ArrowUpRight className="h-3.5 w-3.5 shrink-0 text-zinc-500" />
                          </Link>
                        ) : (
                          <span
                            className="inline-flex max-w-[240px] items-center rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-sm font-semibold text-white"
                            title={trade.asset_name || 'Unmapped Asset'}
                          >
                            <TickerAssetLogo ticker={trade.ticker} label={displayAssetLabel(trade)} />
                            <span className="truncate">{displayAssetLabel(trade)}</span>
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3.5">
                        <div className="flex flex-col items-start gap-1.5">
                          <span
                            className={`inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-semibold ${
                              isBuy ? 'bg-emerald-500/10 text-emerald-300' : 'bg-red-500/10 text-red-300'
                            }`}
                          >
                            {isBuy ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
                            {isBuy ? 'Buy' : 'Sell'}
                          </span>
                          {optionDetails ? (
                            <OptionTradeBadge
                              label={optionDetails.badgeLabel}
                              tooltip={optionDetails.tooltip}
                              className="inline-flex items-center rounded-lg border border-orange-500/30 bg-orange-500/10 px-2.5 py-1 text-[11px] font-bold text-orange-300"
                            />
                          ) : null}
                        </div>
                      </td>
                      <td className="px-4 py-3.5 text-sm font-medium text-zinc-200">
                        {trade.amount_range || 'Unknown'}
                      </td>
                      <td className="px-4 py-3.5 text-sm font-medium text-white">
                        {formatCalendarDate(trade.published_date)}
                      </td>
                      <td className="px-4 py-3.5 text-sm text-zinc-400">
                        {formatCalendarDate(trade.transaction_date)}
                      </td>
                      <td className="px-4 py-3.5 text-right">
                        {trade.source_url ? (
                          <a
                            href={trade.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex items-center gap-1.5 text-xs text-blue-300 transition hover:text-blue-200"
                          >
                            {sourceLabel(trade.source_url)}
                            <ExternalLink className="h-3.5 w-3.5" />
                          </a>
                        ) : (
                          <span className="text-xs text-zinc-600">N/A</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>

            {displayedTrades.length === 0 && !isSearching ? (
              <div className="px-6 py-16 text-center">
                <Search className="mx-auto h-6 w-6 text-zinc-700" />
                <div className="mt-3 text-sm text-zinc-400">No filings match the current filters.</div>
                <div className="mt-1 text-xs text-zinc-600">Try a different politician, ticker, chamber, or direction.</div>
                {hasFilters ? (
                  <button
                    type="button"
                    onClick={clearFilters}
                    className="mt-3 text-xs font-medium text-blue-300 transition hover:text-blue-200"
                  >
                    Clear filters
                  </button>
                ) : null}
              </div>
            ) : null}
          </div>
          {hasMore ? (
            <div className="border-t border-white/[0.06] px-4 py-3 text-center">
              <div className="flex justify-center">
                <button
                  type="button"
                  onClick={loadMore}
                  disabled={isLoadingMore}
                  className="inline-flex min-w-36 items-center justify-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-4 py-2 text-xs font-medium text-zinc-300 transition hover:border-white/[0.14] hover:bg-white/[0.06] hover:text-white disabled:cursor-wait disabled:opacity-60"
                >
                  {isLoadingMore ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
                  {isLoadingMore ? 'Loading...' : 'Load 20 more'}
                </button>
              </div>
              {loadMoreError ? (
                <p role="alert" className="mt-2 text-xs text-red-300">
                  {loadMoreError}
                </p>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>

      <PoliticianProfileModal
        key={selectedMemberId || 'politician-profile-empty'}
        memberId={selectedMemberId}
        memberName={selectedMemberName}
        fallbackSummary={selectedProfileFallback}
        open={Boolean(selectedMemberId)}
        loading={isProfileLoading}
        error={profileError}
        profile={selectedMemberId ? (profileCache[selectedMemberId] || null) : null}
        onClose={closeProfile}
      />
    </>
  );
}

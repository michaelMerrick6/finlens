'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import {
  type CSSProperties,
  type FormEvent,
  type ReactNode,
  type ElementType,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';
import type { Session } from '@supabase/supabase-js';
import {
  AlertTriangle,
  Building2,
  CheckCircle2,
  Landmark,
  Search,
  ShieldAlert,
  TrendingUp,
  X,
} from 'lucide-react';

import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import type {
  AccountAlertHistoryItem,
  AccountAlertPreview,
  AccountFollowSuggestion,
  AccountState,
  AccountTickerSuggestion,
  ActorType,
  AlertMode,
} from '@/lib/account-types';
import { getTickerLogoUrl } from '@/lib/company-logos';

type ApiResponse = {
  ok?: boolean;
  code?: string;
  error?: string;
  state?: AccountState;
  alertPreview?: AccountAlertPreview;
  history?: AccountAlertHistoryItem[];
  result?: { sentChannels?: string[]; skippedChannels?: string[] };
};

export type AccountApiClient = (path: string, init?: RequestInit) => Promise<ApiResponse>;

export type SignalKind = 'ticker' | 'politician' | 'insider' | 'fund';
export type NotifyMode = 'all' | 'unusual' | 'custom';

type PersonFilters = {
  transactionType: 'any' | 'purchase' | 'sale' | 'call_options' | 'put_options';
  minValue: string;
};

export const DEFAULT_PERSON_FILTERS: PersonFilters = {
  transactionType: 'any',
  minValue: '',
};

type StockSourceKind = 'politicians' | 'insiders' | 'hedge_funds';

type StockFilters = {
  source: StockSourceKind;
  politicianScope: 'any' | 'specific';
  politicianName: string;
  politicianKey: string | null;
  politicianAction: 'any' | 'buy' | 'sell' | 'call_options';
  politicianCount: number;
  insiderScope: 'any' | 'specific';
  insiderName: string;
  insiderKey: string | null;
  insiderAction: 'any' | 'buy' | 'sell';
  insiderCount: number;
  insiderMinValue: string;
  hedgeFundScope: 'any' | 'specific';
  hedgeFundName: string;
  hedgeFundAction: 'any' | 'new_position' | 'increase' | 'decrease' | 'exit';
};

export const DEFAULT_STOCK_FILTERS: StockFilters = {
  source: 'politicians',
  politicianScope: 'any',
  politicianName: '',
  politicianKey: null,
  politicianAction: 'any',
  politicianCount: 1,
  insiderScope: 'any',
  insiderName: '',
  insiderKey: null,
  insiderAction: 'any',
  insiderCount: 1,
  insiderMinValue: '',
  hedgeFundScope: 'any',
  hedgeFundName: '',
  hedgeFundAction: 'any',
};

const DEBOUNCE = 200;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;
const SIGNAL_CONFETTI = [
  { x: '-86px', y: '-56px', r: '-38deg', c: '#34d399', d: '0ms' },
  { x: '-52px', y: '-88px', r: '44deg', c: '#60a5fa', d: '35ms' },
  { x: '-12px', y: '-76px', r: '-68deg', c: '#fbbf24', d: '75ms' },
  { x: '34px', y: '-84px', r: '58deg', c: '#a78bfa', d: '25ms' },
  { x: '80px', y: '-48px', r: '94deg', c: '#22d3ee', d: '70ms' },
  { x: '-76px', y: '8px', r: '118deg', c: '#f87171', d: '100ms' },
  { x: '-34px', y: '42px', r: '-108deg', c: '#10b981', d: '55ms' },
  { x: '42px', y: '38px', r: '122deg', c: '#f472b6', d: '95ms' },
  { x: '84px', y: '4px', r: '-92deg', c: '#38bdf8', d: '125ms' },
] as const;

function SignalConfettiBurst() {
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden">
      {SIGNAL_CONFETTI.map((piece, index) => (
        <span
          key={`${piece.x}-${piece.y}-${index}`}
          className="cluster-confetti-piece"
          style={
            {
              '--x': piece.x,
              '--y': piece.y,
              '--r': piece.r,
              '--c': piece.c,
              '--d': piece.d,
            } as CSSProperties
          }
        />
      ))}
    </div>
  );
}

function LockedTickerAvatar({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const normalizedTicker = ticker.trim().toUpperCase();
  const logoUrl = getTickerLogoUrl(normalizedTicker, 48);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;

  if (activeLogoUrl) {
    return (
      <div className="h-9 w-9 shrink-0 overflow-hidden rounded-xl border border-[#10b981]/12 bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={normalizedTicker}
          width={36}
          height={36}
          sizes="36px"
          className="h-full w-full object-contain p-1"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-[#10b981]/14 text-sm font-semibold text-[#7ee7c4]">
      {normalizedTicker.slice(0, 2)}
    </div>
  );
}

export function useAccountApi(session: Session | null): AccountApiClient {
  return useCallback(
    async (path: string, init?: RequestInit) => {
      const token = session?.access_token;
      if (!token) throw new Error('Session expired. Sign in again.');
      const headers = new Headers(init?.headers);
      headers.set('Authorization', `Bearer ${token}`);
      if (init?.body) headers.set('Content-Type', 'application/json');
      const response = await fetch(path, { ...init, headers });
      const payload = (await response.json()) as ApiResponse;
      if (!response.ok || !payload.ok) throw new Error(payload.error || 'Request failed.');
      return payload;
    },
    [session],
  );
}

export function Pill({
  active,
  onClick,
  children,
  size = 'md',
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
  size?: 'sm' | 'md';
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-lg border font-medium transition ${
        active
          ? 'border-[#10b981]/30 bg-[#10b981]/10 text-[#10b981]'
          : 'border-white/[0.06] bg-white/[0.02] text-zinc-500 hover:text-zinc-300'
      } ${size === 'sm' ? 'px-2.5 py-1 text-[10px]' : 'px-3.5 py-1.5 text-xs'}`}
    >
      {children}
    </button>
  );
}

function getNotifyModeDescription(kind: SignalKind, notifyMode: NotifyMode, stockSource: StockSourceKind): string {
  if (kind === 'ticker') {
    if (notifyMode === 'all') {
      return 'All activity sends every matched stock event we clock across politicians, insiders, and hedge funds.';
    }
    if (notifyMode === 'unusual') {
      return 'Unusual only sends the higher-signal stock events, like clusters, larger buys or sells, and notable fund positioning changes.';
    }
    if (stockSource === 'politicians') {
      return 'Custom lets you narrow stock alerts to specific politician behavior, actions, and minimum participant counts.';
    }
    if (stockSource === 'insiders') {
      return 'Custom lets you narrow stock alerts to insider buys or sells, specific insiders, and minimum value or participant thresholds.';
    }
    return 'Custom lets you narrow stock alerts to hedge-fund position changes like new positions, increases, decreases, or exits.';
  }

  if (kind === 'politician') {
    if (notifyMode === 'all') {
      return 'All activity sends every new filing we clock for this politician, including buys, sells, and options disclosures.';
    }
    if (notifyMode === 'unusual') {
      return 'Unusual only sends the higher-signal politician trades, like larger purchases, meaningful sells, and options activity.';
    }
    return 'Custom lets you filter this politician by transaction type and minimum estimated value.';
  }

  if (kind === 'fund') {
    return "Follow this fund's 13F-HR calendar: one week before the expected deadline, one day before, the day it is due, and when Vail ingests the filing.";
  }

  if (notifyMode === 'all') {
    return 'All activity sends every insider filing we clock for this person, including buys and sells.';
  }
  if (notifyMode === 'unusual') {
    return 'Unusual only sends the higher-signal insider trades, like larger buys or sells and more meaningful ownership moves.';
  }
  return 'Custom lets you filter this insider by buy or sell direction and minimum estimated value.';
}

function InlineSearch({
  api,
  kind,
  value,
  placeholder,
  onChange,
  onSelect,
}: {
  api: AccountApiClient;
  kind: 'politician' | 'insider' | 'fund';
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
  onSelect: (name: string, key: string | null) => void;
}) {
  const [suggestions, setSuggestions] = useState<AccountFollowSuggestion[]>([]);
  const [busy, setBusy] = useState(false);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (value.trim().length < 2) {
      setSuggestions([]);
      return;
    }

    const timer = setTimeout(async () => {
      setBusy(true);
      try {
        const payload = await api(
          `/api/account/follow-search?actorType=${kind}&query=${encodeURIComponent(value.trim())}`,
        );
        setSuggestions((payload as { suggestions?: AccountFollowSuggestion[] }).suggestions || []);
      } catch {
        setSuggestions([]);
      } finally {
        setBusy(false);
      }
    }, DEBOUNCE);

    return () => clearTimeout(timer);
  }, [api, kind, value]);

  return (
    <div className="relative mt-2">
      <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3 w-3 -translate-y-1/2 text-zinc-600" />
      <input
        value={value}
        onChange={(event) => {
          onChange(event.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        placeholder={placeholder}
        className="w-full rounded-lg border border-white/[0.06] bg-white/[0.02] py-1.5 pl-8 pr-3 text-xs text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/20"
      />
      {open && (suggestions.length > 0 || busy) && (
        <div className="absolute z-30 mt-1 w-full rounded-lg border border-white/[0.06] bg-[#111] p-1 shadow-lg">
          {busy ? (
            <div className="px-2 py-1.5 text-[10px] text-zinc-500">Searching...</div>
          ) : (
            suggestions.map((suggestion) => (
              <button
                key={`${suggestion.actorType}:${suggestion.actorKey}`}
                type="button"
                onMouseDown={(event) => {
                  event.preventDefault();
                  onSelect(suggestion.actorName, suggestion.actorKey);
                  setOpen(false);
                }}
                className="flex w-full flex-col rounded-md px-2 py-1.5 text-left transition hover:bg-white/[0.04]"
              >
                <div className="text-xs text-white">{suggestion.actorName}</div>
                {suggestion.subtitle ? (
                  <div className="text-[10px] text-zinc-600">{suggestion.subtitle}</div>
                ) : null}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}

export function CustomFiltersForPerson({
  signalType,
  filters,
  onChange,
}: {
  signalType: 'politician' | 'insider';
  filters: PersonFilters;
  onChange: (filters: PersonFilters) => void;
}) {
  const isPolitician = signalType === 'politician';

  return (
    <div className="space-y-3 rounded-xl border border-white/[0.06] bg-white/[0.01] p-3">
      <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
        Custom filters
      </div>

      <div>
        <div className="mb-1.5 text-[10px] text-zinc-600">Transaction type</div>
        <div className="flex flex-wrap gap-1.5">
          <Pill size="sm" active={filters.transactionType === 'any'} onClick={() => onChange({ ...filters, transactionType: 'any' })}>
            Any
          </Pill>
          <Pill
            size="sm"
            active={filters.transactionType === 'purchase'}
            onClick={() => onChange({ ...filters, transactionType: 'purchase' })}
          >
            {isPolitician ? 'Stock purchase' : 'Buy'}
          </Pill>
          <Pill
            size="sm"
            active={filters.transactionType === 'sale'}
            onClick={() => onChange({ ...filters, transactionType: 'sale' })}
          >
            {isPolitician ? 'Stock sale' : 'Sell'}
          </Pill>
          {isPolitician ? (
            <>
              <Pill
                size="sm"
                active={filters.transactionType === 'call_options'}
                onClick={() => onChange({ ...filters, transactionType: 'call_options' })}
              >
                Call options
              </Pill>
              <Pill
                size="sm"
                active={filters.transactionType === 'put_options'}
                onClick={() => onChange({ ...filters, transactionType: 'put_options' })}
              >
                Put options
              </Pill>
            </>
          ) : null}
        </div>
      </div>

      <div>
        <div className="mb-1.5 text-[10px] text-zinc-600">Min. estimated value</div>
        <div className="relative">
          <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-xs text-zinc-600">
            $
          </span>
          <input
            type="text"
            inputMode="numeric"
            value={filters.minValue}
            onChange={(event) => {
              const value = event.target.value.replace(/[^0-9]/g, '');
              onChange({ ...filters, minValue: value });
            }}
            placeholder="Any amount"
            className="w-full rounded-lg border border-white/[0.06] bg-white/[0.02] py-1.5 pl-7 pr-3 text-xs text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/20"
          />
        </div>
      </div>
    </div>
  );
}

export function CustomFiltersForStock({
  api,
  filters,
  onChange,
}: {
  api: AccountApiClient;
  filters: StockFilters;
  onChange: (filters: StockFilters) => void;
}) {
  const sourceTabs: { key: StockSourceKind; label: string; icon: ElementType }[] = [
    { key: 'politicians', label: 'Politicians', icon: Landmark },
    { key: 'insiders', label: 'Insiders', icon: ShieldAlert },
    { key: 'hedge_funds', label: 'Hedge Funds', icon: Building2 },
  ];

  return (
    <div className="space-y-3 rounded-xl border border-white/[0.06] bg-white/[0.01] p-3">
      <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
        Alert source (pick one)
      </div>

      <div className="flex gap-1 rounded-xl border border-white/[0.06] bg-white/[0.02] p-1">
        {sourceTabs.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            type="button"
            onClick={() => onChange({ ...filters, source: key })}
            className={`flex flex-1 items-center justify-center gap-1.5 rounded-lg px-2 py-2 text-[11px] font-medium transition ${
              filters.source === key ? 'bg-[#10b981]/15 text-[#10b981]' : 'text-zinc-500 hover:text-zinc-300'
            }`}
          >
            <Icon className="h-3 w-3" />
            {label}
          </button>
        ))}
      </div>

      {filters.source === 'politicians' ? (
        <div className="space-y-2.5 rounded-xl border border-white/[0.08] bg-white/[0.02] p-3">
          <div className="flex items-center gap-2">
            <Landmark className="h-3.5 w-3.5 text-blue-400" />
            <span className="text-xs font-medium text-white">Politicians</span>
          </div>

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Who</div>
            <div className="flex flex-wrap gap-1.5">
              <Pill size="sm" active={filters.politicianScope === 'any'} onClick={() => onChange({ ...filters, politicianScope: 'any' })}>
                Any politician
              </Pill>
              <Pill
                size="sm"
                active={filters.politicianScope === 'specific'}
                onClick={() => onChange({ ...filters, politicianScope: 'specific' })}
              >
                Specific politician
              </Pill>
            </div>
          </div>

          {filters.politicianScope === 'specific' ? (
            <InlineSearch
              api={api}
              kind="politician"
              value={filters.politicianName}
              placeholder="Search politician..."
              onChange={(value) => onChange({ ...filters, politicianName: value, politicianKey: null })}
              onSelect={(name, key) => onChange({ ...filters, politicianName: name, politicianKey: key })}
            />
          ) : null}

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Action</div>
            <div className="flex flex-wrap gap-1.5">
              <Pill size="sm" active={filters.politicianAction === 'any'} onClick={() => onChange({ ...filters, politicianAction: 'any' })}>
                Any
              </Pill>
              <Pill size="sm" active={filters.politicianAction === 'buy'} onClick={() => onChange({ ...filters, politicianAction: 'buy' })}>
                Buys
              </Pill>
              <Pill size="sm" active={filters.politicianAction === 'sell'} onClick={() => onChange({ ...filters, politicianAction: 'sell' })}>
                Sells
              </Pill>
              <Pill
                size="sm"
                active={filters.politicianAction === 'call_options'}
                onClick={() => onChange({ ...filters, politicianAction: 'call_options' })}
              >
                Call options
              </Pill>
            </div>
          </div>

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Min. politicians buying</div>
            <div className="flex gap-1.5">
              {[1, 2, 3, 5].map((count) => (
                <Pill
                  key={count}
                  size="sm"
                  active={filters.politicianCount === count}
                  onClick={() => onChange({ ...filters, politicianCount: count })}
                >
                  {count === 1 ? 'Any (1+)' : `${count}+`}
                </Pill>
              ))}
            </div>
          </div>
        </div>
      ) : null}

      {filters.source === 'insiders' ? (
        <div className="space-y-2.5 rounded-xl border border-white/[0.08] bg-white/[0.02] p-3">
          <div className="flex items-center gap-2">
            <ShieldAlert className="h-3.5 w-3.5 text-violet-400" />
            <span className="text-xs font-medium text-white">Insiders</span>
          </div>

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Who</div>
            <div className="flex flex-wrap gap-1.5">
              <Pill size="sm" active={filters.insiderScope === 'any'} onClick={() => onChange({ ...filters, insiderScope: 'any' })}>
                Any insider
              </Pill>
              <Pill
                size="sm"
                active={filters.insiderScope === 'specific'}
                onClick={() => onChange({ ...filters, insiderScope: 'specific' })}
              >
                Specific insider
              </Pill>
            </div>
          </div>

          {filters.insiderScope === 'specific' ? (
            <InlineSearch
              api={api}
              kind="insider"
              value={filters.insiderName}
              placeholder="Search insider..."
              onChange={(value) => onChange({ ...filters, insiderName: value, insiderKey: null })}
              onSelect={(name, key) => onChange({ ...filters, insiderName: name, insiderKey: key })}
            />
          ) : null}

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Action</div>
            <div className="flex flex-wrap gap-1.5">
              <Pill size="sm" active={filters.insiderAction === 'any'} onClick={() => onChange({ ...filters, insiderAction: 'any' })}>
                Any
              </Pill>
              <Pill size="sm" active={filters.insiderAction === 'buy'} onClick={() => onChange({ ...filters, insiderAction: 'buy' })}>
                Buys only
              </Pill>
              <Pill size="sm" active={filters.insiderAction === 'sell'} onClick={() => onChange({ ...filters, insiderAction: 'sell' })}>
                Sells only
              </Pill>
            </div>
          </div>

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Min. insiders trading</div>
            <div className="flex gap-1.5">
              {[1, 2, 3, 5].map((count) => (
                <Pill
                  key={count}
                  size="sm"
                  active={filters.insiderCount === count}
                  onClick={() => onChange({ ...filters, insiderCount: count })}
                >
                  {count === 1 ? 'Any (1+)' : `${count}+`}
                </Pill>
              ))}
            </div>
          </div>

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Min. transaction value</div>
            <div className="relative">
              <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-xs text-zinc-600">
                $
              </span>
              <input
                type="text"
                inputMode="numeric"
                value={filters.insiderMinValue}
                onChange={(event) => {
                  const value = event.target.value.replace(/[^0-9]/g, '');
                  onChange({ ...filters, insiderMinValue: value });
                }}
                placeholder="Any amount"
                className="w-full rounded-lg border border-white/[0.06] bg-white/[0.02] py-1.5 pl-7 pr-3 text-xs text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/20"
              />
            </div>
          </div>
        </div>
      ) : null}

      {filters.source === 'hedge_funds' ? (
        <div className="space-y-2.5 rounded-xl border border-white/[0.08] bg-white/[0.02] p-3">
          <div className="flex items-center gap-2">
            <Building2 className="h-3.5 w-3.5 text-emerald-400" />
            <span className="text-xs font-medium text-white">Hedge Funds</span>
          </div>

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Which fund</div>
            <div className="flex flex-wrap gap-1.5">
              <Pill size="sm" active={filters.hedgeFundScope === 'any'} onClick={() => onChange({ ...filters, hedgeFundScope: 'any' })}>
                Any fund
              </Pill>
              <Pill
                size="sm"
                active={filters.hedgeFundScope === 'specific'}
                onClick={() => onChange({ ...filters, hedgeFundScope: 'specific' })}
              >
                Specific fund
              </Pill>
            </div>
          </div>

          {filters.hedgeFundScope === 'specific' ? (
            <div className="relative mt-2">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3 w-3 -translate-y-1/2 text-zinc-600" />
              <input
                value={filters.hedgeFundName}
                onChange={(event) => onChange({ ...filters, hedgeFundName: event.target.value })}
                placeholder="e.g. Citadel Advisors"
                className="w-full rounded-lg border border-white/[0.06] bg-white/[0.02] py-1.5 pl-8 pr-3 text-xs text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/20"
              />
            </div>
          ) : null}

          <div>
            <div className="mb-1 text-[10px] text-zinc-600">Change type</div>
            <div className="flex flex-wrap gap-1.5">
              <Pill size="sm" active={filters.hedgeFundAction === 'any'} onClick={() => onChange({ ...filters, hedgeFundAction: 'any' })}>
                Any change
              </Pill>
              <Pill
                size="sm"
                active={filters.hedgeFundAction === 'new_position'}
                onClick={() => onChange({ ...filters, hedgeFundAction: 'new_position' })}
              >
                New position
              </Pill>
              <Pill
                size="sm"
                active={filters.hedgeFundAction === 'increase'}
                onClick={() => onChange({ ...filters, hedgeFundAction: 'increase' })}
              >
                Increase
              </Pill>
              <Pill
                size="sm"
                active={filters.hedgeFundAction === 'decrease'}
                onClick={() => onChange({ ...filters, hedgeFundAction: 'decrease' })}
              >
                Decrease
              </Pill>
              <Pill
                size="sm"
                active={filters.hedgeFundAction === 'exit'}
                onClick={() => onChange({ ...filters, hedgeFundAction: 'exit' })}
              >
                Exit
              </Pill>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function CreateSignalModal({
  session,
  onCreated,
  onClose,
  initialKind = 'ticker',
  initialQuery = '',
  initialActor = null,
  lockTickerContext = false,
  lockActorContext = false,
  zIndex = 50,
}: {
  session: Session;
  onCreated: (state: AccountState) => void;
  onClose: () => void;
  initialKind?: SignalKind;
  initialQuery?: string;
  initialActor?: AccountFollowSuggestion | null;
  lockTickerContext?: boolean;
  lockActorContext?: boolean;
  zIndex?: number;
}) {
  const api = useAccountApi(session);
  const isLockedTickerContext =
    lockTickerContext && initialKind === 'ticker' && initialQuery.trim().length > 0;
  const isLockedActorContext =
    lockActorContext && initialActor != null && initialKind !== 'ticker';
  const isLockedContext = isLockedTickerContext || isLockedActorContext;
  const defaultNotifyMode: NotifyMode = isLockedTickerContext ? 'custom' : 'all';

  const [kind, setKind] = useState<SignalKind>(isLockedContext ? initialKind : initialKind);
  const [query, setQuery] = useState(initialQuery);
  const [notifyMode, setNotifyMode] = useState<NotifyMode>(defaultNotifyMode);
  const [personFilters, setPersonFilters] = useState<PersonFilters>({ ...DEFAULT_PERSON_FILTERS });
  const [stockFilters, setStockFilters] = useState<StockFilters>({ ...DEFAULT_STOCK_FILTERS });
  const [saving, setSaving] = useState(false);
  const [created, setCreated] = useState(false);
  const [error, setError] = useState('');
  const [tickerSuggestions, setTickerSuggestions] = useState<AccountTickerSuggestion[]>([]);
  const [actorSuggestions, setActorSuggestions] = useState<AccountFollowSuggestion[]>([]);
  const [busy, setBusy] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [selectedActor, setSelectedActor] = useState<AccountFollowSuggestion | null>(
    isLockedActorContext ? initialActor : (initialKind === 'ticker' ? null : initialActor),
  );

  useEffect(() => {
    return () => {
      if (closeTimerRef.current) {
        clearTimeout(closeTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    setKind(isLockedContext ? initialKind : initialKind);
  }, [initialKind, isLockedContext]);

  useEffect(() => {
    const useInitialValues = isLockedContext || kind === initialKind;
    setQuery(useInitialValues ? initialQuery : '');
    setTickerSuggestions([]);
    setActorSuggestions([]);
    setSelectedActor(useInitialValues && initialKind !== 'ticker' ? initialActor : null);
    setError('');
    setNotifyMode(defaultNotifyMode);
    setPersonFilters({ ...DEFAULT_PERSON_FILTERS });
    setStockFilters({ ...DEFAULT_STOCK_FILTERS });
    setCreated(false);
    if (closeTimerRef.current) {
      clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }, [defaultNotifyMode, initialActor, initialKind, initialQuery, isLockedContext, kind]);

  useEffect(() => {
    if (kind !== 'ticker') return;
    const cleanQuery = query.trim();
    if (cleanQuery.length < 1) {
      setTickerSuggestions([]);
      return;
    }

    const timer = setTimeout(async () => {
      setBusy(true);
      try {
        const payload = await api(`/api/account/ticker-search?query=${encodeURIComponent(cleanQuery)}`);
        setTickerSuggestions((payload as { suggestions?: AccountTickerSuggestion[] }).suggestions || []);
      } catch {
        setTickerSuggestions([]);
      } finally {
        setBusy(false);
      }
    }, DEBOUNCE);

    return () => clearTimeout(timer);
  }, [api, kind, query]);

  useEffect(() => {
    if (kind === 'ticker') return;
    const cleanQuery = query.trim();
    if (cleanQuery.length < 2) {
      setActorSuggestions([]);
      return;
    }

    const actorType: ActorType =
      kind === 'politician' ? 'politician' : kind === 'fund' ? 'fund' : 'insider';
    const timer = setTimeout(async () => {
      setBusy(true);
      try {
        const payload = await api(
          `/api/account/follow-search?actorType=${actorType}&query=${encodeURIComponent(cleanQuery)}`,
        );
        setActorSuggestions((payload as { suggestions?: AccountFollowSuggestion[] }).suggestions || []);
      } catch {
        setActorSuggestions([]);
      } finally {
        setBusy(false);
      }
    }, DEBOUNCE);

    return () => clearTimeout(timer);
  }, [api, kind, query]);

  function getAlertMode(): AlertMode {
    if (kind === 'fund') return 'activity';
    if (notifyMode === 'all') return 'activity';
    if (notifyMode === 'unusual') return 'unusual';
    return 'both';
  }

  async function handleSave(event: FormEvent) {
    event.preventDefault();
    if (!query.trim()) return;

    setSaving(true);
    setError('');
    setCreated(false);
    let animatingSuccess = false;

    try {
      let payload: ApiResponse;
      if (kind === 'ticker') {
        payload = await api('/api/account/follows', {
          method: 'POST',
          body: JSON.stringify({
            kind: 'ticker',
            ticker: query.trim(),
            alertMode: getAlertMode(),
          }),
        });
      } else {
        const actorType: ActorType =
          kind === 'politician' ? 'politician' : kind === 'fund' ? 'fund' : 'insider';
        payload = await api('/api/account/follows', {
          method: 'POST',
          body: JSON.stringify({
            kind: 'actor',
            actorType,
            actorName: query.trim(),
            actorKey: selectedActor?.actorType === actorType ? selectedActor.actorKey : null,
            alertMode: getAlertMode(),
          }),
        });
      }

      if (payload.state) {
        animatingSuccess = true;
        setCreated(true);
        setSaving(false);
        closeTimerRef.current = setTimeout(() => {
          onCreated(payload.state as AccountState);
          onClose();
        }, 900);
        return;
      }

      onClose();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Failed to create signal.');
    } finally {
      if (!animatingSuccess) {
        setSaving(false);
      }
    }
  }

  const placeholder =
    kind === 'ticker'
      ? 'Search for any stock...'
      : kind === 'politician'
        ? 'Search for a politician...'
        : kind === 'fund'
          ? 'Search for a hedge fund...'
          : 'Search for an insider...';
  const suggestions = kind === 'ticker' ? tickerSuggestions : actorSuggestions;
  const dialogTitle = isLockedTickerContext
    ? 'Create Stock Alert'
    : isLockedActorContext
      ? kind === 'politician'
        ? 'Follow Politician'
        : kind === 'fund'
          ? 'Create Fund Alert'
          : 'Follow Insider'
      : kind === 'fund'
        ? 'Create Fund Alert'
        : 'Create Signal';
  const notifyModeDescription = getNotifyModeDescription(kind, notifyMode, stockFilters.source);

  return (
    <div className="fixed inset-0 flex items-start justify-center p-4 pt-[12vh]" style={{ zIndex }}>
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />

      <div className="relative w-full max-w-md overflow-hidden rounded-2xl border border-white/[0.08] bg-[#111111] shadow-2xl shadow-black/50">
        {created ? (
          <div className="absolute inset-0 z-40 flex flex-col items-center justify-center bg-[#111111]/95 px-8 text-center backdrop-blur-sm">
            <SignalConfettiBurst />
            <div className="relative flex h-14 w-14 items-center justify-center rounded-full border border-emerald-400/25 bg-emerald-400/10 text-emerald-300">
              <CheckCircle2 className="h-7 w-7" />
            </div>
            <div className="relative mt-4 text-xl font-semibold tracking-tight text-white">Alert added.</div>
            <div className="relative mt-2 max-w-[260px] text-sm leading-6 text-zinc-500">
              This follow will now show in Alerts with matching recent activity.
            </div>
          </div>
        ) : null}

        <div className="flex items-center justify-between border-b border-white/[0.06] px-5 py-3.5">
          <h2 className="text-[15px] font-semibold text-white">{dialogTitle}</h2>
          <button
            type="button"
            onClick={onClose}
            className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 transition hover:bg-white/5 hover:text-white"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <form onSubmit={handleSave} className="max-h-[70vh] overflow-y-auto">
          <div className="space-y-4 px-5 py-4">
            {!isLockedContext ? (
              <div>
                <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                  Signal type
                </label>
                <div className="flex gap-1 rounded-xl border border-white/[0.06] bg-white/[0.02] p-1">
                  {([
                    { key: 'ticker' as SignalKind, label: 'Stock', icon: TrendingUp },
                    { key: 'politician' as SignalKind, label: 'Politician', icon: Landmark },
                    { key: 'insider' as SignalKind, label: 'Insider', icon: ShieldAlert },
                    { key: 'fund' as SignalKind, label: 'Fund', icon: Building2 },
                  ]).map(({ key, label, icon: Icon }) => (
                    <button
                      key={key}
                      type="button"
                      onClick={() => setKind(key)}
                      className={`flex flex-1 items-center justify-center gap-1.5 rounded-lg px-3 py-2 text-xs font-medium transition ${
                        kind === key ? 'bg-[#10b981]/15 text-[#10b981]' : 'text-zinc-500 hover:text-zinc-300'
                      }`}
                    >
                      <Icon className="h-3.5 w-3.5" />
                      {label}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}

            <div>
              <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                {isLockedTickerContext
                  ? 'Tracking stock'
                  : isLockedActorContext
                    ? kind === 'politician'
                      ? 'Tracking politician'
                      : kind === 'fund'
                        ? 'Tracking fund'
                        : 'Tracking insider'
                    : kind === 'ticker'
                      ? 'Find a stock'
                      : kind === 'politician'
                        ? 'Find a politician'
                        : kind === 'fund'
                          ? 'Find a hedge fund'
                          : 'Find an insider'}
              </label>
              {isLockedTickerContext ? (
                <div className="rounded-xl border border-[#10b981]/16 bg-[#10b981]/6 px-3.5 py-3">
                  <div className="flex items-center gap-3">
                    <LockedTickerAvatar ticker={query.trim()} />
                    <div className="min-w-0">
                      <div className="text-sm font-semibold text-white">{query.trim().toUpperCase()}</div>
                      <div className="mt-0.5 text-[11px] leading-5 text-zinc-500">
                        This alert is pinned to the stock already open in your dashboard workspace.
                      </div>
                    </div>
                  </div>
                </div>
              ) : isLockedActorContext && initialActor ? (
                <div className="rounded-xl border border-[#10b981]/16 bg-[#10b981]/6 px-3.5 py-3">
                  <div className="flex items-center gap-3">
                    {initialActor.actorType === 'politician' ? (
                      <PoliticianHeadshot
                        memberId={initialActor.actorKey}
                        name={initialActor.actorName}
                        size={36}
                      />
                    ) : initialActor.actorType === 'fund' ? (
                      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-emerald-500/15 bg-emerald-500/10 text-emerald-300">
                        <Building2 className="h-4 w-4" />
                      </div>
                    ) : (
                      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-white/10 bg-white/[0.04] text-zinc-400">
                        <ShieldAlert className="h-4 w-4" />
                      </div>
                    )}
                    <div className="min-w-0">
                      <div className="text-sm font-semibold text-white">{initialActor.actorName}</div>
                      <div className="mt-0.5 text-[11px] leading-5 text-zinc-500">
                        {initialActor.subtitle
                          ? initialActor.subtitle
                          : 'This alert is pinned to the profile you are viewing.'}
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="relative">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
                  <input
                    value={query}
                    onChange={(event) => {
                      setQuery(event.target.value);
                      setShowDropdown(true);
                      setSelectedActor(null);
                    }}
                    onFocus={() => setShowDropdown(true)}
                    onBlur={() => setTimeout(() => setShowDropdown(false), 150)}
                    placeholder={placeholder}
                    autoFocus
                    className="h-11 w-full rounded-xl border border-white/[0.08] bg-white/[0.03] pl-10 pr-4 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/30 focus:ring-1 focus:ring-[#10b981]/20"
                  />
                  {showDropdown && (suggestions.length > 0 || busy) ? (
                    <div className="absolute z-30 mt-1.5 w-full rounded-xl border border-white/[0.08] bg-[#111111] p-1 shadow-xl">
                      {busy ? (
                        <div className="px-3 py-2.5 text-xs text-zinc-500">Searching...</div>
                      ) : kind === 'ticker' ? (
                        tickerSuggestions.map((suggestion) => (
                          <button
                            key={suggestion.ticker}
                            type="button"
                            onMouseDown={(event) => {
                              event.preventDefault();
                              setQuery(suggestion.ticker);
                              setShowDropdown(false);
                            }}
                            className="flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-left transition hover:bg-white/[0.04]"
                          >
                            <div
                              className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-[9px] font-bold text-white"
                              style={{ background: 'linear-gradient(135deg, #10b981, #059669)' }}
                            >
                              {suggestion.ticker.slice(0, 2)}
                            </div>
                            <div>
                              <div className="text-sm font-medium text-white">{suggestion.ticker}</div>
                              <div className="text-[11px] text-zinc-500">{suggestion.companyName}</div>
                            </div>
                          </button>
                        ))
                      ) : (
                        actorSuggestions.map((suggestion) => (
                          <button
                            key={`${suggestion.actorType}:${suggestion.actorKey}`}
                            type="button"
                            onMouseDown={(event) => {
                              event.preventDefault();
                              setQuery(suggestion.actorName);
                              setSelectedActor(suggestion);
                              setShowDropdown(false);
                            }}
                            className="flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-left transition hover:bg-white/[0.04]"
                          >
                            {suggestion.actorType === 'politician' ? (
                              <PoliticianHeadshot
                                memberId={suggestion.actorKey}
                                name={suggestion.actorName}
                                size={34}
                              />
                            ) : suggestion.actorType === 'fund' ? (
                              <div className="flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-lg border border-emerald-500/15 bg-emerald-500/10 text-emerald-300">
                                <Building2 className="h-4 w-4" />
                              </div>
                            ) : (
                              <div className="flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-full border border-white/10 bg-white/[0.04] text-zinc-400">
                                <ShieldAlert className="h-4 w-4" />
                              </div>
                            )}
                            <div className="min-w-0">
                              <div className="truncate text-sm font-medium text-white">{suggestion.actorName}</div>
                              {suggestion.subtitle ? (
                                <div className="truncate text-[11px] text-zinc-500">{suggestion.subtitle}</div>
                              ) : null}
                            </div>
                          </button>
                        ))
                      )}
                    </div>
                  ) : null}
                </div>
              )}
            </div>

            {kind === 'fund' ? (
              <div>
                <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                  13F filing alerts
                </label>
                <div className="rounded-xl border border-[#10b981]/15 bg-[#10b981]/[0.04] p-3">
                  <div className="text-sm font-semibold text-white">Follow the filing cycle</div>
                  <p className="mt-1 text-[11px] leading-5 text-zinc-500">{notifyModeDescription}</p>
                  <div className="mt-3 grid grid-cols-2 gap-2">
                    {['7 days before', '1 day before', 'Due day', 'Filed + ingested'].map((label) => (
                      <div
                        key={label}
                        className="rounded-lg border border-white/[0.06] bg-black/20 px-2.5 py-2 text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-300/80"
                      >
                        {label}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div>
                <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                  Notify me on
                </label>
                <div className="flex gap-2">
                  <Pill active={notifyMode === 'all'} onClick={() => setNotifyMode('all')}>
                    All activity
                  </Pill>
                  <Pill active={notifyMode === 'unusual'} onClick={() => setNotifyMode('unusual')}>
                    Unusual only
                  </Pill>
                  <Pill active={notifyMode === 'custom'} onClick={() => setNotifyMode('custom')}>
                    Custom
                  </Pill>
                </div>
                <div className="mt-2 rounded-lg border border-white/[0.06] bg-white/[0.02] px-3 py-2 text-[11px] leading-5 text-zinc-500">
                  {notifyModeDescription}
                </div>
              </div>
            )}

            {notifyMode === 'custom' && kind === 'ticker' ? (
              <CustomFiltersForStock api={api} filters={stockFilters} onChange={setStockFilters} />
            ) : null}
            {notifyMode === 'custom' && kind !== 'ticker' && kind !== 'fund' ? (
              <CustomFiltersForPerson
                signalType={kind as 'politician' | 'insider'}
                filters={personFilters}
                onChange={setPersonFilters}
              />
            ) : null}

            {error ? (
              <div className="flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2 text-xs text-red-400">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" /> {error}
              </div>
            ) : null}
          </div>

          <div className="flex items-center justify-end gap-3 border-t border-white/[0.06] px-5 py-3.5">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg px-4 py-2 text-sm text-zinc-400 transition hover:text-white"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={saving || created || !query.trim()}
              className="rounded-xl bg-[#10b981] px-5 py-2.5 text-sm font-semibold text-black transition hover:bg-[#34d399] disabled:opacity-40"
            >
              {created ? 'Added' : saving ? 'Saving...' : 'Save'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

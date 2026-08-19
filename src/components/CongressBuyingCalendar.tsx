'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useMemo, useState } from 'react';
import { CalendarDays, ChevronLeft, ChevronRight, Landmark } from 'lucide-react';

import CongressBuyingViewNav from '@/components/CongressBuyingViewNav';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type {
  CongressBuyingCalendarData,
  CongressCalendarCompany,
  CongressCalendarDay,
} from '@/lib/congress-cluster-calendar-types';
import { formatCalendarDate } from '@/lib/date-format';
import { formatCompactCurrency } from '@/lib/hedge-funds';

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function minimumLabel(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '$0';
  const compact = formatCompactCurrency(value);
  return compact ? `${compact}+` : '$0';
}

function countLabel(count: number, singular: string, plural = `${singular}s`) {
  return `${count.toLocaleString()} ${count === 1 ? singular : plural}`;
}

function addDays(value: string, days: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function startOfWeek(value: string) {
  const date = new Date(`${value}T12:00:00Z`);
  const mondayOffset = (date.getUTCDay() + 6) % 7;
  return addDays(value, -mondayOffset);
}

function currentPacificDate() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function fullDateLabel(date: string) {
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'long',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  }).format(new Date(`${date}T12:00:00Z`));
}

function weekdayLabel(date: string, long = false) {
  return new Intl.DateTimeFormat('en-US', {
    weekday: long ? 'long' : 'short',
    timeZone: 'UTC',
  }).format(new Date(`${date}T12:00:00Z`));
}

function weekRangeLabel(start: string) {
  const end = addDays(start, 6);
  const first = new Date(`${start}T12:00:00Z`);
  const last = new Date(`${end}T12:00:00Z`);
  const firstLabel = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  }).format(first);
  const lastLabel = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  }).format(last);
  return `${firstLabel} – ${lastLabel}`;
}

function TickerLogo({ ticker, size = 38 }: { ticker: string; size?: number }) {
  const [failed, setFailed] = useState(false);
  const logoUrl = getTickerLogoUrl(ticker, size);

  if (logoUrl && !failed) {
    return (
      <span
        className="flex shrink-0 items-center justify-center overflow-hidden rounded-xl border border-white/[0.1] bg-zinc-100"
        style={{ width: size, height: size }}
      >
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt=""
          width={size}
          height={size}
          sizes={`${size}px`}
          className="h-full w-full object-contain p-1.5"
          onError={() => setFailed(true)}
        />
      </span>
    );
  }

  return (
    <span
      aria-hidden="true"
      className="flex shrink-0 items-center justify-center rounded-xl border border-white/[0.08] bg-white/[0.04] text-[10px] font-bold text-zinc-300"
      style={{ width: size, height: size }}
    >
      {ticker.slice(0, 3)}
    </span>
  );
}

function DayCard({
  date,
  day,
  selected,
  today,
  onSelect,
}: {
  date: string;
  day: CongressCalendarDay | undefined;
  selected: boolean;
  today: string;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={selected}
      aria-label={`${fullDateLabel(date)}${day ? `, ${countLabel(day.tradeCount, 'purchase')}` : ', no reported purchases'}`}
      onClick={onSelect}
      className={`flex min-h-[158px] min-w-0 flex-col rounded-xl border p-3 text-left transition ${
        selected
          ? 'border-emerald-300/35 bg-emerald-300/[0.055]'
          : 'border-white/[0.065] bg-white/[0.012] hover:border-white/[0.12] hover:bg-white/[0.025]'
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className={`text-[9px] font-semibold uppercase tracking-[0.12em] ${selected ? 'text-emerald-200/80' : 'text-zinc-600'}`}>
            {weekdayLabel(date)}
          </div>
          <div className={`mt-1 text-lg font-semibold tabular-nums ${date === today ? 'text-emerald-200' : 'text-zinc-200'}`}>
            {Number(date.slice(-2))}
          </div>
        </div>
        {day ? <span className="text-[9px] tabular-nums text-zinc-600">{countLabel(day.tradeCount, 'buy')}</span> : null}
      </div>

      {day ? (
        <>
          <div className="mt-4 space-y-2">
            {day.companies.slice(0, 2).map((company) => (
              <div key={company.ticker} className="flex min-w-0 items-center justify-between gap-1.5">
                <span className="truncate text-[11px] font-semibold tracking-[0.04em] text-cyan-100/90">{company.ticker}</span>
                <span className="shrink-0 text-[9px] tabular-nums text-zinc-500">{minimumLabel(company.amountFloor)}</span>
              </div>
            ))}
            {day.companies.length > 2 ? <div className="text-[9px] text-zinc-600">+{day.companies.length - 2} stocks</div> : null}
          </div>
          <div className="mt-auto border-t border-white/[0.05] pt-2.5 text-[10px] font-medium tabular-nums text-emerald-200/80">
            {minimumLabel(day.amountFloor)} minimum
          </div>
        </>
      ) : (
        <div className="mt-auto pb-1 text-[10px] leading-4 text-zinc-700">No purchases</div>
      )}
    </button>
  );
}

function MobileDayButton({
  date,
  day,
  selected,
  today,
  onSelect,
}: {
  date: string;
  day: CongressCalendarDay | undefined;
  selected: boolean;
  today: string;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={selected}
      onClick={onSelect}
      className={`flex min-w-0 flex-col items-center rounded-lg border px-1 py-2.5 transition ${
        selected ? 'border-emerald-300/35 bg-emerald-300/[0.07]' : 'border-transparent bg-white/[0.018]'
      }`}
    >
      <span className="text-[8px] font-semibold uppercase tracking-[0.08em] text-zinc-600">{weekdayLabel(date)}</span>
      <span className={`mt-1 text-sm font-semibold tabular-nums ${date === today ? 'text-emerald-200' : 'text-zinc-300'}`}>
        {Number(date.slice(-2))}
      </span>
      <span className={`mt-1.5 h-1 w-1 rounded-full ${day ? 'bg-emerald-300' : 'bg-transparent'}`} />
    </button>
  );
}

function weekContains(start: string, date: string) {
  return date >= start && date <= addDays(start, 6);
}

function resolveWeekStart(data: CongressBuyingCalendarData, requested: string | null, today: string) {
  if (requested && requested >= data.calendarStart && addDays(requested, 6) <= data.calendarEnd) return requested;
  if (data.latestTransactionDate) return startOfWeek(data.latestTransactionDate);
  if (today >= data.calendarStart && today <= data.calendarEnd) return startOfWeek(today);
  return startOfWeek(`${data.month}-01`);
}

function preferredDateForWeek(start: string, days: CongressCalendarDay[], preferred?: string | null) {
  if (preferred && weekContains(start, preferred)) return preferred;
  const activeDates = days.filter((day) => weekContains(start, day.date)).map((day) => day.date);
  return activeDates.at(-1) || start;
}

export default function CongressBuyingCalendar({
  data,
  loading,
  error,
  initialWeekStart,
  onWeekChange,
  onCompanySelect,
}: {
  data: CongressBuyingCalendarData;
  loading: boolean;
  error: string;
  initialWeekStart: string | null;
  onWeekChange: (weekStart: string) => void;
  onCompanySelect: (date: string, company: CongressCalendarCompany) => void;
}) {
  const today = currentPacificDate();
  const currentWeekStart = startOfWeek(today);
  const resolvedWeekStart = resolveWeekStart(data, initialWeekStart, today);
  const [weekStart, setWeekStart] = useState(resolvedWeekStart);
  const [selectedDate, setSelectedDate] = useState(() =>
    preferredDateForWeek(resolvedWeekStart, data.days, data.latestTransactionDate),
  );
  const weekDates = useMemo(() => Array.from({ length: 7 }, (_, index) => addDays(weekStart, index)), [weekStart]);
  const dayMap = useMemo(() => new Map(data.days.map((day) => [day.date, day])), [data.days]);
  const selectedDay = dayMap.get(selectedDate);
  const weekSummary = data.weeks.find((week) => week.startDate === weekStart);

  const selectWeek = (nextStart: string) => {
    if (nextStart >= data.calendarStart && addDays(nextStart, 6) <= data.calendarEnd) {
      setWeekStart(nextStart);
      setSelectedDate(preferredDateForWeek(nextStart, data.days));
      return;
    }
    onWeekChange(nextStart);
  };

  const showCurrentWeek = () => selectWeek(currentWeekStart);

  return (
    <div className="mx-auto max-w-[1180px] space-y-4">
      <div className="flex justify-end">
        <CongressBuyingViewNav active="calendar" />
      </div>

      <header className="flex flex-col gap-4 border-b border-white/[0.06] pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
            <CalendarDays className="h-3.5 w-3.5" />
            Congress
          </div>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight text-white sm:text-3xl">Weekly buying calendar</h1>
          <p className="mt-1.5 max-w-xl text-sm leading-6 text-zinc-500">
            See where congressional money moved, one week at a time.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <div className="flex min-w-0 flex-1 items-center rounded-xl border border-white/[0.08] bg-white/[0.018] p-1 sm:flex-none">
            <button
              type="button"
              aria-label="Previous week"
              disabled={weekStart <= '2015-01-05'}
              onClick={() => selectWeek(addDays(weekStart, -7))}
              className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-white disabled:cursor-not-allowed disabled:opacity-25"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            <div className="min-w-0 flex-1 px-2 text-center text-xs font-semibold text-zinc-200 sm:min-w-[154px]">
              {weekRangeLabel(weekStart)}
            </div>
            <button
              type="button"
              aria-label="Next week"
              disabled={weekStart >= currentWeekStart}
              onClick={() => selectWeek(addDays(weekStart, 7))}
              className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-white disabled:cursor-not-allowed disabled:opacity-25"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
          <button
            type="button"
            onClick={showCurrentWeek}
            className="shrink-0 rounded-xl border border-white/[0.08] bg-white/[0.018] px-3 py-2.5 text-xs font-medium text-zinc-400 transition hover:bg-white/[0.05] hover:text-white"
          >
            This week
          </button>
        </div>
      </header>

      {error ? (
        <div role="alert" className="rounded-xl border border-red-400/15 bg-red-400/[0.06] px-4 py-3 text-xs text-red-200">
          {error}
        </div>
      ) : null}

      <div
        aria-live="polite"
        aria-busy={loading}
        className={`space-y-4 transition-opacity ${loading ? 'pointer-events-none opacity-55' : 'opacity-100'}`}
      >
        <section className="flex flex-col gap-4 rounded-2xl border border-white/[0.07] bg-white/[0.014] px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-5">
          <div>
            <div className="text-xl font-semibold tabular-nums text-emerald-200 sm:text-2xl">
              {minimumLabel(weekSummary?.amountFloor || 0)}
            </div>
            <div className="mt-0.5 text-[9px] font-semibold uppercase tracking-[0.13em] text-zinc-600">Minimum disclosed</div>
          </div>
          <div className="grid grid-cols-3 divide-x divide-white/[0.06] border-t border-white/[0.06] pt-3 sm:border-l sm:border-t-0 sm:pl-1 sm:pt-0">
            <div className="px-3 sm:px-5">
              <div className="text-base font-semibold tabular-nums text-zinc-200">{weekSummary?.actorCount || 0}</div>
              <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">Lawmakers</div>
            </div>
            <div className="px-3 sm:px-5">
              <div className="text-base font-semibold tabular-nums text-zinc-200">{weekSummary?.tradeCount || 0}</div>
              <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">Purchases</div>
            </div>
            <div className="px-3 sm:px-5">
              <div className="text-base font-semibold tabular-nums text-zinc-200">{weekSummary?.companyCount || 0}</div>
              <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">Stocks</div>
            </div>
          </div>
        </section>

        <section className="hidden grid-cols-7 gap-2 md:grid">
          {weekDates.map((date) => (
            <DayCard
              key={date}
              date={date}
              day={dayMap.get(date)}
              selected={selectedDate === date}
              today={today}
              onSelect={() => setSelectedDate(date)}
            />
          ))}
        </section>

        <section className="grid grid-cols-7 gap-1 md:hidden">
          {weekDates.map((date) => (
            <MobileDayButton
              key={date}
              date={date}
              day={dayMap.get(date)}
              selected={selectedDate === date}
              today={today}
              onSelect={() => setSelectedDate(date)}
            />
          ))}
        </section>

        <section className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.012]">
          <div className="flex flex-col gap-2 border-b border-white/[0.055] px-4 py-3.5 sm:flex-row sm:items-end sm:justify-between sm:px-5">
            <div>
              <div className="text-[9px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                {weekdayLabel(selectedDate, true)}
              </div>
              <h2 className="mt-1 text-base font-semibold text-white">{fullDateLabel(selectedDate).replace(`${weekdayLabel(selectedDate, true)}, `, '')}</h2>
              <p className="mt-1 text-[11px] text-zinc-600">
                {selectedDay
                  ? `${countLabel(selectedDay.actorCount, 'lawmaker')} · ${countLabel(selectedDay.tradeCount, 'purchase')} · ${minimumLabel(selectedDay.amountFloor)} minimum disclosed`
                  : 'No reported congressional stock purchases on this date.'}
              </p>
            </div>
            {selectedDay ? <div className="text-[10px] text-zinc-700">Select a stock to see its filings</div> : null}
          </div>

          {selectedDay ? (
            <div className="divide-y divide-white/[0.05]">
              {selectedDay.companies.map((company) => (
                <button
                  key={company.ticker}
                  type="button"
                  aria-haspopup="dialog"
                  aria-label={`Show ${company.ticker} purchases on ${fullDateLabel(selectedDate)}`}
                  onClick={() => onCompanySelect(selectedDate, company)}
                  className="group grid w-full grid-cols-[38px_minmax(0,1fr)_auto_16px] items-center gap-3 px-4 py-3.5 text-left transition hover:bg-white/[0.025] sm:px-5"
                >
                  <TickerLogo ticker={company.ticker} />
                  <div className="min-w-0">
                    <div className="text-sm font-bold tracking-[0.06em] text-cyan-100">{company.ticker}</div>
                    <div className="mt-0.5 truncate text-xs text-zinc-600">{company.companyName || `${company.ticker} purchases`}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-semibold tabular-nums text-emerald-200">{minimumLabel(company.amountFloor)}</div>
                    <div className="mt-0.5 text-[9px] uppercase tracking-[0.08em] text-zinc-600">
                      {countLabel(company.actorCount, 'lawmaker')} · {countLabel(company.tradeCount, 'buy')}
                    </div>
                  </div>
                  <ChevronRight className="h-3.5 w-3.5 text-zinc-700 transition group-hover:translate-x-0.5 group-hover:text-zinc-400" />
                </button>
              ))}
            </div>
          ) : (
            <div className="flex min-h-[120px] flex-col items-center justify-center px-5 text-center">
              <Landmark className="mb-2.5 h-5 w-5 text-zinc-800" />
              <div className="text-sm text-zinc-600">Choose another day or move to a different week.</div>
            </div>
          )}
        </section>

        <div className="flex flex-col gap-1 px-1 text-[11px] leading-5 text-zinc-700 sm:flex-row sm:items-center sm:justify-between">
          <span>Shown by transaction date. Repeated filings count once and invalid future dates are excluded.</span>
          <span className="shrink-0">
            Filings through {data.latestDisclosureDate ? formatCalendarDate(data.latestDisclosureDate, 'UTC') : 'the latest disclosure'}
          </span>
        </div>
      </div>
    </div>
  );
}

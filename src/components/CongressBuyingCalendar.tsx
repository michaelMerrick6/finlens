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
const WEEKDAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function minimumLabel(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '$0';
  const compact = formatCompactCurrency(value);
  return compact ? `${compact}+` : '$0';
}

function countLabel(count: number, singular: string, plural = `${singular}s`) {
  return `${count.toLocaleString()} ${count === 1 ? singular : plural}`;
}

function monthLabel(month: string) {
  return new Intl.DateTimeFormat('en-US', { month: 'long', year: 'numeric', timeZone: 'UTC' }).format(
    new Date(`${month}-01T12:00:00Z`),
  );
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

function shiftMonth(month: string, delta: number) {
  const date = new Date(`${month}-01T12:00:00Z`);
  date.setUTCMonth(date.getUTCMonth() + delta);
  return date.toISOString().slice(0, 7);
}

function dateSequence(start: string, end: string) {
  const dates: string[] = [];
  const cursor = new Date(`${start}T12:00:00Z`);
  const final = new Date(`${end}T12:00:00Z`);
  while (cursor <= final) {
    dates.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
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

function weekdayLabel(date: string) {
  return new Intl.DateTimeFormat('en-US', { weekday: 'short', timeZone: 'UTC' }).format(
    new Date(`${date}T12:00:00Z`),
  );
}

function weekLabel(dates: string[]) {
  const first = new Date(`${dates[0]}T12:00:00Z`);
  const last = new Date(`${dates[dates.length - 1]}T12:00:00Z`);
  const firstLabel = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }).format(first);
  const lastLabel = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }).format(last);
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

function Metric({ value, label }: { value: string; label: string }) {
  return (
    <div className="min-w-0 px-3 py-3 sm:px-4">
      <div className="truncate text-lg font-semibold tabular-nums text-white sm:text-xl">{value}</div>
      <div className="mt-0.5 text-[9px] font-semibold uppercase tracking-[0.12em] text-zinc-600 sm:text-[10px]">
        {label}
      </div>
    </div>
  );
}

function CalendarCell({
  date,
  day,
  month,
  selected,
  today,
  onSelect,
}: {
  date: string;
  day: CongressCalendarDay | undefined;
  month: string;
  selected: boolean;
  today: string;
  onSelect: () => void;
}) {
  const outsideMonth = !date.startsWith(month);
  const dateNumber = Number(date.slice(-2));
  const visibleCompanies = day?.companies.slice(0, 2) || [];

  return (
    <button
      type="button"
      onClick={onSelect}
      aria-pressed={selected}
      aria-label={`${fullDateLabel(date)}${day ? `, ${countLabel(day.tradeCount, 'congressional buy')}` : ', no congressional buys'}`}
      className={`relative min-h-[118px] border-b border-r border-white/[0.05] p-2.5 text-left transition [&:nth-child(7n)]:border-r-0 hover:bg-white/[0.025] ${
        selected ? 'bg-emerald-400/[0.055] shadow-[inset_0_0_0_1px_rgba(52,211,153,0.32)]' : ''
      } ${outsideMonth ? 'bg-black/20' : ''}`}
    >
      <div className="flex items-center justify-between">
        <span
          className={`flex h-6 min-w-6 items-center justify-center rounded-full px-1 text-[11px] font-medium tabular-nums ${
            date === today
              ? 'bg-emerald-300 text-black'
              : outsideMonth
                ? 'text-zinc-700'
                : 'text-zinc-400'
          }`}
        >
          {dateNumber}
        </span>
        {day ? <span className="text-[9px] tabular-nums text-zinc-600">{countLabel(day.tradeCount, 'buy')}</span> : null}
      </div>

      {visibleCompanies.length ? (
        <div className="mt-2 space-y-1.5">
          {visibleCompanies.map((company) => (
            <div
              key={company.ticker}
              className="flex items-center justify-between gap-1 rounded-md border border-emerald-300/[0.09] bg-emerald-300/[0.055] px-1.5 py-1"
            >
              <span className="truncate text-[10px] font-semibold tracking-[0.04em] text-emerald-100/90">
                {company.ticker}
              </span>
              <span className="shrink-0 text-[9px] tabular-nums text-zinc-500">{minimumLabel(company.amountFloor)}</span>
            </div>
          ))}
          {day && day.companies.length > visibleCompanies.length ? (
            <div className="px-1 text-[9px] text-zinc-600">+{day.companies.length - visibleCompanies.length} more</div>
          ) : null}
        </div>
      ) : null}
    </button>
  );
}

export default function CongressBuyingCalendar({
  data,
  loading,
  error,
  onMonthChange,
  onCompanySelect,
}: {
  data: CongressBuyingCalendarData;
  loading: boolean;
  error: string;
  onMonthChange: (month: string) => void;
  onCompanySelect: (date: string, company: CongressCalendarCompany) => void;
}) {
  const today = currentPacificDate();
  const allDates = useMemo(
    () => dateSequence(data.calendarStart, data.calendarEnd),
    [data.calendarEnd, data.calendarStart],
  );
  const dayMap = useMemo(() => new Map(data.days.map((day) => [day.date, day])), [data.days]);
  const [selectedDate, setSelectedDate] = useState(data.latestTransactionDate || `${data.month}-01`);
  const selectedDay = dayMap.get(selectedDate);
  const weeks = useMemo(
    () => Array.from({ length: Math.ceil(allDates.length / 7) }, (_, index) => allDates.slice(index * 7, index * 7 + 7)),
    [allDates],
  );
  const activeWeeks = weeks.filter((week) => week.some((date) => dayMap.has(date)));
  const currentMonth = today.slice(0, 7);

  const goToToday = () => {
    if (data.month === currentMonth) setSelectedDate(today);
    else onMonthChange(currentMonth);
  };

  return (
    <div className="mx-auto max-w-[1120px] space-y-4">
      <div className="flex justify-end">
        <CongressBuyingViewNav active="calendar" />
      </div>

      <header className="flex flex-col gap-4 border-b border-white/[0.06] pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
            <CalendarDays className="h-3.5 w-3.5" />
            Congress
          </div>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight text-white sm:text-3xl">Buying calendar</h1>
          <p className="mt-1.5 max-w-2xl text-sm leading-6 text-zinc-500">
            See congressional stock purchases on the date they happened. Late disclosures can add activity to past dates.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <div className="flex items-center rounded-xl border border-white/[0.08] bg-white/[0.018] p-1">
            <button
              type="button"
              aria-label="Previous month"
              disabled={data.month <= '2015-01'}
              onClick={() => onMonthChange(shiftMonth(data.month, -1))}
              className="flex h-8 w-8 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-white disabled:cursor-not-allowed disabled:opacity-25"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            <div className="min-w-[132px] px-2 text-center text-sm font-semibold text-zinc-200">{monthLabel(data.month)}</div>
            <button
              type="button"
              aria-label="Next month"
              disabled={data.month >= currentMonth}
              onClick={() => onMonthChange(shiftMonth(data.month, 1))}
              className="flex h-8 w-8 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-white disabled:cursor-not-allowed disabled:opacity-25"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
          <button
            type="button"
            onClick={goToToday}
            className="rounded-xl border border-white/[0.08] bg-white/[0.018] px-3 py-2.5 text-xs font-medium text-zinc-400 transition hover:bg-white/[0.05] hover:text-white"
          >
            Today
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
        <section className="grid grid-cols-3 divide-x divide-white/[0.055] rounded-2xl border border-white/[0.07] bg-white/[0.014]">
          <Metric label={`Lawmakers in ${monthLabel(data.month).split(' ')[0]}`} value={data.totals.actorCount.toLocaleString()} />
          <Metric label="Stock purchases" value={data.totals.tradeCount.toLocaleString()} />
          <Metric label="Minimum disclosed" value={minimumLabel(data.totals.amountFloor)} />
        </section>

        <section className="hidden overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.012] md:block">
          <div className="grid grid-cols-7 border-b border-white/[0.06] bg-white/[0.012]">
            {WEEKDAY_LABELS.map((label) => (
              <div key={label} className="border-r border-white/[0.05] px-3 py-2 text-[9px] font-semibold uppercase tracking-[0.12em] text-zinc-700 last:border-r-0">
                {label}
              </div>
            ))}
          </div>
          <div className="grid grid-cols-7">
            {allDates.map((date) => (
              <CalendarCell
                key={date}
                date={date}
                day={dayMap.get(date)}
                month={data.month}
                selected={selectedDate === date}
                today={today}
                onSelect={() => setSelectedDate(date)}
              />
            ))}
          </div>
        </section>

        <section className="space-y-3 md:hidden">
          {activeWeeks.length ? (
            activeWeeks.map((week) => (
              <div key={week[0]} className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.012]">
                <div className="border-b border-white/[0.055] px-4 py-2.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                  Week of {weekLabel(week)}
                </div>
                <div className="divide-y divide-white/[0.05]">
                  {week.filter((date) => dayMap.has(date)).map((date) => {
                    const day = dayMap.get(date)!;
                    return (
                      <button
                        key={date}
                        type="button"
                        aria-pressed={selectedDate === date}
                        onClick={() => setSelectedDate(date)}
                        className={`flex w-full items-center gap-3 px-4 py-3 text-left transition ${
                          selectedDate === date ? 'bg-emerald-400/[0.055]' : 'hover:bg-white/[0.025]'
                        }`}
                      >
                        <div className="w-12 shrink-0">
                          <div className="text-[10px] font-medium uppercase text-zinc-600">
                            {weekdayLabel(date)}
                          </div>
                          <div className="mt-0.5 text-lg font-semibold tabular-nums text-white">{Number(date.slice(-2))}</div>
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap gap-1.5">
                            {day.companies.slice(0, 4).map((company) => (
                              <span key={company.ticker} className="rounded-md bg-emerald-300/[0.07] px-1.5 py-1 text-[10px] font-semibold text-emerald-100/90">
                                {company.ticker}
                              </span>
                            ))}
                            {day.companies.length > 4 ? <span className="py-1 text-[10px] text-zinc-600">+{day.companies.length - 4}</span> : null}
                          </div>
                          <div className="mt-1.5 text-[10px] text-zinc-600">
                            {countLabel(day.tradeCount, 'buy')} · {minimumLabel(day.amountFloor)} minimum
                          </div>
                        </div>
                        <ChevronRight className="h-3.5 w-3.5 text-zinc-700" />
                      </button>
                    );
                  })}
                </div>
              </div>
            ))
          ) : (
            <div className="rounded-2xl border border-white/[0.07] bg-white/[0.012] px-5 py-10 text-center text-sm text-zinc-500">
              No reported congressional stock purchases appear in this month.
            </div>
          )}
        </section>

        <section className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.012]">
          <div className="flex flex-col gap-2 border-b border-white/[0.055] px-4 py-3.5 sm:flex-row sm:items-end sm:justify-between sm:px-5">
            <div>
              <h2 className="text-sm font-semibold text-white">{fullDateLabel(selectedDate)}</h2>
              <p className="mt-0.5 text-[11px] text-zinc-600">
                {selectedDay
                  ? `${countLabel(selectedDay.tradeCount, 'purchase')} · ${countLabel(selectedDay.actorCount, 'lawmaker')} · ${minimumLabel(selectedDay.amountFloor)} minimum disclosed`
                  : 'No reported congressional stock purchases on this date.'}
              </p>
            </div>
            {selectedDay ? <div className="text-[10px] text-zinc-700">Select a stock to see the filings</div> : null}
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
            <div className="flex min-h-[130px] flex-col items-center justify-center px-5 text-center">
              <Landmark className="mb-2.5 h-6 w-6 text-zinc-800" />
              <div className="text-sm text-zinc-500">Choose a highlighted date to review its purchases.</div>
            </div>
          )}
        </section>

        <div className="flex flex-col gap-1 px-1 text-[11px] leading-5 text-zinc-700 sm:flex-row sm:items-center sm:justify-between">
          <span>Trade dates come from public filings. Invalid future-dated records are excluded and repeated copies count once.</span>
          <span className="shrink-0">
            Filings through {data.latestDisclosureDate ? formatCalendarDate(data.latestDisclosureDate, 'UTC') : 'the latest disclosure'}
          </span>
        </div>
      </div>
    </div>
  );
}

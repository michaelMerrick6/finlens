export type PricePoint = { date: string; price: number };
export type AnnualPriceReturn = {
  year: number;
  startDate: string | null;
  endDate: string | null;
  returnPercent: number | null;
  partial: boolean;
};
const DAY = 86_400_000;

// Keep incomplete current-session prices out of a chart labelled daily closes.
export function lastCompletedMarketDate(now: Date): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(now);
  const part = (name: string) => parts.find(p => p.type === name)!.value;
  const date = `${part('year')}-${part('month')}-${part('day')}`;
  const minutes = Number(part('hour')) * 60 + Number(part('minute'));
  return minutes >= 16 * 60 + 15 ? date
    : new Date(Date.parse(`${date}T12:00:00Z`) - DAY).toISOString().slice(0, 10);
}

export function usablePrices(points: PricePoint[], earliestDate: string, throughDate: string): PricePoint[] {
  const unique = new Map<string, PricePoint>();
  for (const point of points) {
    if (/^\d{4}-\d{2}-\d{2}$/.test(point.date) && point.date >= earliestDate
      && point.date <= throughDate && Number.isFinite(point.price) && point.price > 0) {
      unique.set(point.date, point);
    }
  }
  return [...unique.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export function annualPriceReturns(points: PricePoint[], inception: string, throughDate: string): AnnualPriceReturn[] {
  const prices = usablePrices(points, inception, throughDate);
  const firstYear = Number(inception.slice(0, 4));
  const lastYear = Number(throughDate.slice(0, 4));
  const onOrBefore = (date: string) => prices.findLast(point => point.date <= date);
  // DJT's covered US exchange years have no December 31 holiday. Require the
  // final weekday close, not a nearby quote from an incomplete weekly series.
  const yearEnd = (year: number) => {
    const end = new Date(`${year}-12-31T12:00:00Z`);
    if (end.getUTCDay() === 6) end.setUTCDate(30);
    if (end.getUTCDay() === 0) end.setUTCDate(29);
    return end.toISOString().slice(0, 10);
  };
  const fresh = (point: PricePoint | undefined, target: string) => point
    && Date.parse(target) - Date.parse(point.date) <= 7 * DAY;
  return Array.from({ length: Math.max(0, lastYear - firstYear + 1) }, (_, i) => {
    const year = firstYear + i;
    const target = `${year}-12-31` < throughDate ? `${year}-12-31` : throughDate;
    const first = year === firstYear ? prices[0] : onOrBefore(`${year - 1}-12-31`);
    const last = onOrBefore(target);
    const validStart = year === firstYear
      ? first?.date === inception
      : first?.date === yearEnd(year - 1);
    const validEnd = target === `${year}-12-31` ? last?.date === yearEnd(year) : fresh(last, target);
    const valid = validStart && validEnd && first && last && first.date < last.date;
    return { year, startDate: first?.date ?? null, endDate: last?.date ?? null,
      returnPercent: valid ? (last.price / first.price - 1) * 100 : null,
      partial: (year === firstYear && inception > `${year}-01-01`) || target < `${year}-12-31` };
  });
}

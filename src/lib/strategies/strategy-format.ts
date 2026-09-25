export const strategyDate = (iso: string) => new Date(`${iso.slice(0, 10)}T12:00:00Z`).toLocaleDateString('en-US', {
  year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC',
});
export const strategyMoney = (n: number) => new Intl.NumberFormat('en-US', {
  style: 'currency', currency: 'USD', maximumFractionDigits: 0,
}).format(n);
export const compactMoney = (n: number) => new Intl.NumberFormat('en-US', {
  // Currency defaults differ across runtimes; pin both bounds for hydration.
  style: 'currency', currency: 'USD', notation: 'compact', minimumFractionDigits: 0, maximumFractionDigits: 1,
}).format(n);
export const strategyRange = (low: number, high: number | null) => high === null
  ? `Over ${strategyMoney(low - 1)}` : `${strategyMoney(low)}–${strategyMoney(high)}`;

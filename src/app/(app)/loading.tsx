export default function Loading() {
  return (
    <div role="status" aria-live="polite" className="mx-auto max-w-6xl space-y-5 px-4 py-8">
      <p className="text-sm text-zinc-300">Loading filings…</p>
      <div aria-hidden="true" className="space-y-3 motion-safe:animate-pulse">
        <div className="h-9 w-56 rounded-lg bg-white/10" />
        <div className="h-28 rounded-xl bg-white/5" />
        <div className="h-28 rounded-xl bg-white/5" />
      </div>
    </div>
  );
}

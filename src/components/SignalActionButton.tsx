'use client';

import { BellPlus } from 'lucide-react';

type SignalActionButtonProps = {
  onClick: () => void;
  label?: string;
  className?: string;
};

export default function SignalActionButton({
  onClick,
  label = 'Turn On Signals',
  className = '',
}: SignalActionButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`group relative inline-flex h-10 items-center gap-2 overflow-hidden rounded-xl border border-emerald-500/22 bg-emerald-500/[0.08] px-3.5 pr-4 text-xs font-semibold text-emerald-100 shadow-[inset_0_1px_0_rgba(255,255,255,0.08),0_12px_30px_rgba(16,185,129,0.10)] transition duration-200 hover:-translate-y-0.5 hover:border-emerald-400/35 hover:bg-emerald-500/[0.14] hover:text-emerald-50 hover:shadow-[inset_0_1px_0_rgba(255,255,255,0.12),0_16px_38px_rgba(16,185,129,0.15)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/40 ${className}`}
    >
      <span className="pointer-events-none absolute inset-0 bg-[linear-gradient(90deg,transparent,rgba(255,255,255,0.08),transparent)] opacity-0 transition duration-200 group-hover:opacity-100" />
      <span className="relative flex h-6 w-6 items-center justify-center rounded-lg border border-emerald-200/18 bg-black/18 shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
        <BellPlus className="h-3.5 w-3.5 text-emerald-100" />
      </span>
      <span className="relative whitespace-nowrap">{label}</span>
      <span className="relative hidden h-4 w-px bg-white/15 sm:block" />
      <span className="relative hidden text-[10px] font-bold uppercase tracking-[0.18em] text-emerald-100/70 sm:block">
        Live
      </span>
    </button>
  );
}

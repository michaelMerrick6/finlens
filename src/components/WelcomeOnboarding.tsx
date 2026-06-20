'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { ArrowRight, Bell, ChevronRight, Sparkles, TrendingUp, X, Zap } from 'lucide-react';

import { supabase } from '@/lib/supabase';

const ONBOARDING_STORAGE_KEY = 'vail_onboarding_complete';

type StepConfig = {
  icon: React.ReactNode;
  badge: string;
  badgeColor: string;
  title: string;
  subtitle: string;
  content: React.ReactNode;
};

const STEPS: StepConfig[] = [
  {
    icon: <Sparkles className="h-8 w-8" />,
    badge: 'Welcome',
    badgeColor: 'border-emerald-500/30 bg-emerald-500/15 text-emerald-300',
    title: 'Welcome to Vail!',
    subtitle: "Let's show you around. A few quick things — we promise it's worth it.",
    content: (
      <div className="mt-6 grid gap-3">
        <div className="flex items-center gap-3 rounded-xl border border-white/[0.06] bg-white/[0.02] p-3">
          <div className="text-lg">🏛️</div>
          <div className="text-sm text-zinc-300">Track every politician trade in Congress</div>
        </div>
        <div className="flex items-center gap-3 rounded-xl border border-white/[0.06] bg-white/[0.02] p-3">
          <div className="text-lg">📋</div>
          <div className="text-sm text-zinc-300">Monitor insider Form 4 filings in real time</div>
        </div>
        <div className="flex items-center gap-3 rounded-xl border border-white/[0.06] bg-white/[0.02] p-3">
          <div className="text-lg">🏦</div>
          <div className="text-sm text-zinc-300">See what the top hedge funds are buying</div>
        </div>
      </div>
    ),
  },
  {
    icon: <TrendingUp className="h-8 w-8" />,
    badge: 'Congress',
    badgeColor: 'border-blue-500/30 bg-blue-500/15 text-blue-300',
    title: 'Congress trades come from official disclosures.',
    subtitle: 'House PTR PDFs and Senate ethics filings power the Congress feed.',
    content: (
      <div className="mt-6 space-y-4">
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-medium text-white">Official filing window</div>
              <div className="text-xs text-zinc-500">House Clerk + Senate Ethics</div>
            </div>
            <div className="text-right">
              <div className="text-lg font-semibold text-emerald-400">45 days</div>
              <div className="text-[10px] text-zinc-600">maximum after a trade</div>
            </div>
          </div>
          <div className="mt-3 text-xs text-zinc-500">
            Congress periodic transaction reports are due within 30 days of notification, but no later than 45 days after the transaction date.
          </div>
        </div>
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-medium text-white">Disclosure threshold</div>
              <div className="text-xs text-zinc-500">Official PTR rule</div>
            </div>
            <div className="text-right">
              <div className="text-lg font-semibold text-emerald-400">$1K+</div>
              <div className="text-[10px] text-zinc-600">covered transaction amount</div>
            </div>
          </div>
          <div className="mt-3 text-xs text-zinc-500">
            Vail should only surface public-company trades that can be traced back to the official filing and mapped to the disclosed asset line.
          </div>
        </div>
      </div>
    ),
  },
  {
    icon: <Zap className="h-8 w-8" />,
    badge: 'Smart Money',
    badgeColor: 'border-amber-500/30 bg-amber-500/15 text-amber-300',
    title: 'Insiders & funds know first.',
    subtitle: 'When executives buy their own stock, it matters. When Berkshire loads up, pay attention.',
    content: (
      <div className="mt-6 space-y-4">
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-medium text-white">Berkshire Hathaway</div>
              <div className="text-xs text-zinc-500">13F Holdings</div>
            </div>
            <div className="text-right">
              <div className="text-lg font-semibold text-blue-400">$90B+</div>
              <div className="text-[10px] text-zinc-600">in Apple alone</div>
            </div>
          </div>
        </div>
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-emerald-500/10 text-emerald-400">
              <TrendingUp className="h-5 w-5" />
            </div>
            <div>
              <div className="text-sm font-medium text-white">Insider buys = strongest signal</div>
              <div className="mt-1 text-xs text-zinc-500">
                When a CEO buys $1M+ of their own stock, historically the stock outperforms the next 6 months ~65% of the time.
              </div>
            </div>
          </div>
        </div>
      </div>
    ),
  },
  {
    icon: <Bell className="h-8 w-8" />,
    badge: 'Alerts',
    badgeColor: 'border-purple-500/30 bg-purple-500/15 text-purple-300',
    title: 'Get notified instantly.',
    subtitle: 'Follow the tickers, politicians, and insiders you care about. Vail delivers to you.',
    content: (
      <div className="mt-6 space-y-3">
        <div className="flex items-center gap-4 rounded-xl border border-white/[0.06] bg-white/[0.02] p-4">
          <div className="text-2xl">📱</div>
          <div>
            <div className="text-sm font-medium text-white">Text</div>
            <div className="text-xs text-zinc-500">Instant alerts straight to your phone.</div>
          </div>
        </div>
        <div className="flex items-center gap-4 rounded-xl border border-white/[0.06] bg-white/[0.02] p-4">
          <div className="text-2xl">📧</div>
          <div>
            <div className="text-sm font-medium text-white">Email</div>
            <div className="text-xs text-zinc-500">Detailed summaries that stay in your inbox as a permanent record.</div>
          </div>
        </div>
        <div className="flex items-center gap-4 rounded-xl border border-white/[0.06] bg-white/[0.02] p-4">
          <div className="text-2xl">🎯</div>
          <div>
            <div className="text-sm font-medium text-white">Follow anything</div>
            <div className="text-xs text-zinc-500">Stocks, politicians, insiders — follow what matters to you.</div>
          </div>
        </div>
      </div>
    ),
  },
  {
    icon: <ArrowRight className="h-8 w-8" />,
    badge: 'Go Pro',
    badgeColor: 'border-emerald-500/30 bg-emerald-500/15 text-emerald-300',
    title: 'Ready to go deeper?',
    subtitle: 'Unlock full access with Vail Pro.',
    content: null, // Handled inline for the CTA buttons
  },
];

export default function WelcomeOnboarding() {
  const router = useRouter();
  const [visible, setVisible] = useState(false);
  const [step, setStep] = useState(0);

  const showOnboarding = () => {
    const alreadyDone = localStorage.getItem(ONBOARDING_STORAGE_KEY);
    if (alreadyDone) return;
    // Slight delay so dashboard renders first
    setTimeout(() => setVisible(true), 600);
  };

  useEffect(() => {
    // Check if already completed
    const completed = localStorage.getItem(ONBOARDING_STORAGE_KEY);
    if (completed) return;

    // Check if there's already a session (page refresh while signed in)
    supabase.auth.getSession().then(({ data }) => {
      if (data.session) {
        showOnboarding();
      }
    });

    // Listen for sign-in events (fresh sign-in / OAuth redirect)
    const { data } = supabase.auth.onAuthStateChange((event, session) => {
      if ((event === 'SIGNED_IN' || event === 'INITIAL_SESSION') && session) {
        showOnboarding();
      }
    });

    return () => {
      data.subscription.unsubscribe();
    };
  }, []);

  function dismiss() {
    localStorage.setItem(ONBOARDING_STORAGE_KEY, 'true');
    setVisible(false);
  }

  function handleNext() {
    if (step < STEPS.length - 1) {
      setStep(step + 1);
    } else {
      dismiss();
    }
  }

  function handleBack() {
    if (step > 0) {
      setStep(step - 1);
    }
  }

  function handleUpgrade() {
    dismiss();
    router.push('/pricing');
  }

  if (!visible) return null;

  const currentStep = STEPS[step];
  const isLastStep = step === STEPS.length - 1;
  const isFirstStep = step === 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/70 backdrop-blur-sm"
        onClick={dismiss}
      />

      {/* Modal */}
      <div
        className="relative w-full max-w-md overflow-hidden rounded-3xl border border-white/[0.08] bg-[#0c0c0f] shadow-2xl"
        style={{ animation: 'fadeInScale 0.3s ease-out' }}
      >
        {/* Close button */}
        <button
          type="button"
          onClick={dismiss}
          className="absolute right-4 top-4 z-10 flex h-8 w-8 items-center justify-center rounded-full bg-white/[0.06] text-zinc-500 transition hover:bg-white/[0.12] hover:text-white"
        >
          <X className="h-4 w-4" />
        </button>

        <div className="p-8">
          {/* Badge */}
          <div className="flex justify-center">
            <span className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium ${currentStep.badgeColor}`}>
              {currentStep.icon && <span className="[&>svg]:h-3 [&>svg]:w-3">{currentStep.icon}</span>}
              {currentStep.badge}
            </span>
          </div>

          {/* Title */}
          <h2 className="mt-5 text-center text-2xl font-semibold tracking-tight text-white">
            {currentStep.title}
          </h2>
          <p className="mt-2 text-center text-sm text-zinc-500">
            {currentStep.subtitle}
          </p>

          {/* Step content */}
          {currentStep.content}

          {/* Last step — upgrade CTA */}
          {isLastStep && (
            <div className="mt-6 space-y-3">
              <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/[0.06] p-5">
                <div className="text-sm font-medium text-emerald-300">Vail Pro — $9.99/mo</div>
                <ul className="mt-3 space-y-2">
                  {[
                    '25 follows (vs 3 on Free)',
                    'Text + email alerts',
                    'Cluster & unusual activity detection',
                    'Priority signal processing',
                  ].map((item) => (
                    <li key={item} className="flex items-center gap-2 text-xs text-zinc-400">
                      <span className="text-emerald-400">✓</span>
                      {item}
                    </li>
                  ))}
                </ul>
              </div>

              <button
                type="button"
                onClick={handleUpgrade}
                className="w-full rounded-2xl bg-emerald-600 px-4 py-3 text-sm font-medium text-white transition hover:bg-emerald-500"
              >
                Upgrade Now
              </button>
              <button
                type="button"
                onClick={dismiss}
                className="w-full rounded-2xl border border-white/[0.06] bg-white/[0.02] px-4 py-3 text-sm font-medium text-zinc-400 transition hover:bg-white/[0.05] hover:text-white"
              >
                Continue as Free
              </button>
            </div>
          )}
        </div>

        {/* Footer — navigation */}
        {!isLastStep && (
          <div className="flex items-center justify-between border-t border-white/[0.06] px-8 py-4">
            <div>
              {!isFirstStep && (
                <button
                  type="button"
                  onClick={handleBack}
                  className="text-sm font-medium text-zinc-500 transition hover:text-white"
                >
                  Back
                </button>
              )}
            </div>
            <button
              type="button"
              onClick={handleNext}
              className="inline-flex items-center gap-1.5 rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white transition hover:bg-emerald-500"
            >
              {isFirstStep ? 'Start' : 'Next'}
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        )}

        {/* Progress dots */}
        <div className="flex items-center justify-center gap-2 pb-5">
          <span className="text-[10px] text-zinc-600">Step {step + 1} of {STEPS.length}</span>
          <div className="flex gap-1.5">
            {STEPS.map((_, i) => (
              <div
                key={i}
                className={`h-1.5 w-1.5 rounded-full transition ${
                  i === step ? 'bg-emerald-400' : i < step ? 'bg-emerald-400/40' : 'bg-white/10'
                }`}
              />
            ))}
          </div>
        </div>
      </div>

      <style jsx>{`
        @keyframes fadeInScale {
          from {
            opacity: 0;
            transform: scale(0.95);
          }
          to {
            opacity: 1;
            transform: scale(1);
          }
        }
      `}</style>
    </div>
  );
}

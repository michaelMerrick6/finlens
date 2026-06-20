'use client';

import { useEffect, useRef, useState } from 'react';
import type { Session } from '@supabase/supabase-js';
import Image from 'next/image';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ChevronDown, User } from 'lucide-react';

import { supabase } from '@/lib/supabase';

/* ── Navigation structure ──────────────────────── */

type NavItem = {
  href: string;
  label: string;
  children?: { href: string; label: string; desc: string }[];
};

const NAV_ITEMS: NavItem[] = [
  { href: '/dashboard', label: 'Dashboard' },
  {
    href: '/data',
    label: 'Data',
    children: [
      { href: '/insiders', label: 'Insiders', desc: 'SEC Form 4 insider trades' },
      { href: '/politicians', label: 'Politicians', desc: 'Congress member disclosures' },
      { href: '/hedge-funds', label: 'Hedge Funds', desc: '13F institutional filings' },
    ],
  },
  { href: '/clusters', label: 'Clusters' },
  { href: '/alerts', label: 'Alerts' },
];

/* ── Logo / Wordmark ───────────────────────────── */

function VailMark() {
  return (
    <Link href="/" className="group flex items-center gap-2.5">
      <div className="relative flex h-9 w-9 shrink-0 items-center justify-center overflow-visible transition-transform duration-300 group-hover:scale-110">
        <div className="absolute -inset-3 rounded-full bg-[radial-gradient(circle,rgba(52,211,153,0.34),rgba(16,185,129,0.18)_38%,transparent_70%)] opacity-0 blur-lg transition-opacity duration-300 group-hover:opacity-100" />
        <Image
          src="/vail-logo-mark.svg"
          alt=""
          width={36}
          height={36}
          className="relative h-9 w-9 object-contain drop-shadow-[0_5px_12px_rgba(0,0,0,0.5)] transition duration-300 group-hover:brightness-125 group-hover:saturate-150 group-hover:drop-shadow-[0_0_16px_rgba(52,211,153,0.65)]"
        />
      </div>
      <div className="flex flex-col">
        <span className="text-[17px] font-semibold tracking-[-0.02em] text-white transition-colors duration-300 group-hover:text-emerald-50">Vail</span>
        <span className="text-[10px] uppercase tracking-[0.2em] text-zinc-600 transition-colors duration-300 group-hover:text-emerald-400/80">Signal Intelligence</span>
      </div>
    </Link>
  );
}

/* ── Data dropdown menu ────────────────────────── */

function DataDropdown({
  items,
  open,
  onClose,
  triggerRef,
}: {
  items: { href: string; label: string; desc: string }[];
  open: boolean;
  onClose: () => void;
  triggerRef: React.RefObject<HTMLButtonElement | null>;
}) {
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        dropdownRef.current && !dropdownRef.current.contains(target) &&
        (!triggerRef.current || !triggerRef.current.contains(target))
      ) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open, onClose, triggerRef]);

  return (
    <div
      ref={dropdownRef}
      className="nav-dropdown"
      style={{
        opacity: open ? 1 : 0,
        transform: open ? 'translateY(0) scale(1)' : 'translateY(-6px) scale(0.97)',
        pointerEvents: open ? 'auto' : 'none',
      }}
    >
      {items.map((item) => (
        <Link
          key={item.href}
          href={item.href}
          onClick={onClose}
          className="group flex items-start gap-3 rounded-lg px-3 py-2.5 transition-all duration-200 hover:bg-white/[0.04]"
        >
          <div>
            <div className="text-[13px] font-medium text-zinc-200 transition-colors group-hover:text-white">
              {item.label}
            </div>
            <div className="mt-0.5 text-[11px] leading-snug text-zinc-600 transition-colors group-hover:text-zinc-500">
              {item.desc}
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
}

/* ── Holographic nav pill with sliding indicator ── */

function NavPills() {
  const pathname = usePathname();
  const [dataOpen, setDataOpen] = useState(false);
  const [locationSearch, setLocationSearch] = useState('');
  const pillsRef = useRef<HTMLDivElement>(null);
  const indicatorRef = useRef<HTMLDivElement>(null);
  const itemRefs = useRef<Map<string, HTMLElement>>(new Map());
  const dataButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const syncSearch = () => setLocationSearch(window.location.search);
    syncSearch();
    window.addEventListener('popstate', syncSearch);
    window.addEventListener('vail-dashboard-location-change', syncSearch);

    return () => {
      window.removeEventListener('popstate', syncSearch);
      window.removeEventListener('vail-dashboard-location-change', syncSearch);
    };
  }, [pathname]);

  /* Determine which nav item is active */
  function isItemActive(item: NavItem) {
    if (item.href === '/dashboard') return pathname === '/dashboard';
    if (item.children) {
      return item.children.some((child) => pathname.startsWith(child.href));
    }
    return pathname.startsWith(item.href);
  }

  const activeItem = NAV_ITEMS.find(isItemActive) || null;
  const dashboardParams = new URLSearchParams(locationSearch);
  const dashboardWorkspaceOpen =
    pathname === '/dashboard' && (dashboardParams.has('ticker') || dashboardParams.has('memberId'));
  const animateIndicator = pathname === '/dashboard' && !dashboardWorkspaceOpen;

  /* Slide indicator to active tab */
  useEffect(() => {
    if (!indicatorRef.current || !pillsRef.current) return;

    const activeKey = activeItem?.href || null;
    if (!activeKey) {
      indicatorRef.current.style.opacity = '0';
      return;
    }

    const el = itemRefs.current.get(activeKey);
    if (!el) {
      indicatorRef.current.style.opacity = '0';
      return;
    }

    const pillsRect = pillsRef.current.getBoundingClientRect();
    const elRect = el.getBoundingClientRect();
    const left = elRect.left - pillsRect.left;
    const width = elRect.width;

    indicatorRef.current.style.opacity = '1';
    indicatorRef.current.style.transform = `translateX(${left}px)`;
    indicatorRef.current.style.width = `${width}px`;
  }, [pathname, activeItem]);

  return (
    <div className="relative" ref={pillsRef}>
      {/* Sliding holographic indicator */}
      <div
        ref={indicatorRef}
        className={`nav-sliding-indicator ${animateIndicator ? '' : 'nav-sliding-indicator-static'}`}
        style={{ opacity: 0 }}
      />

      <nav className="nav-pills-container">
        {NAV_ITEMS.map((item) => {
          const active = isItemActive(item);

          if (item.children) {
            return (
              <div key={item.href} className="relative">
                <button
                  ref={(el) => {
                    dataButtonRef.current = el;
                    if (el) itemRefs.current.set(item.href, el);
                  }}
                  type="button"
                  onClick={() => setDataOpen((v) => !v)}
                  className={`nav-pill-item ${active ? 'nav-pill-active' : ''}`}
                >
                  {item.label}
                  <ChevronDown
                    className="h-3 w-3 transition-transform duration-200"
                    style={{ transform: dataOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}
                  />
                </button>
                <DataDropdown
                  items={item.children}
                  open={dataOpen}
                  onClose={() => setDataOpen(false)}
                  triggerRef={dataButtonRef}
                />
              </div>
            );
          }

          return (
            <Link
              key={item.href}
              href={item.href}
              ref={(el) => { if (el) itemRefs.current.set(item.href, el); }}
              className={`nav-pill-item ${active ? 'nav-pill-active' : ''}`}
            >
              {item.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}

/* ── Main export ───────────────────────────────── */

export default function AppTopNav() {
  const [session, setSession] = useState<Session | null>(null);

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (mounted) {
        setSession(data.session);
      }
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  return (
    <header className="sticky top-0 z-40 border-b border-white/[0.06] bg-[#050505]/88 backdrop-blur-xl">
      <div className="mx-auto w-full max-w-[1800px] px-4 py-3 sm:px-6">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
          <VailMark />

          <div className="flex min-w-0 flex-col gap-3 lg:ml-auto lg:flex-row lg:items-center lg:justify-end lg:gap-5">
            <NavPills />

            <div className="flex items-center gap-3 self-start lg:self-auto">
              {!session ? (
                <Link
                  href="/auth"
                  className="nav-signin-btn"
                >
                  <span className="nav-signin-btn-glow" />
                  <span className="relative">Sign In</span>
                </Link>
              ) : (
                <Link
                  href="/account"
                  className="nav-account-btn"
                >
                  <User className="h-4 w-4" />
                  My Account
                </Link>
              )}
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

"use client";
import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import { AccountMenu } from "./account-menu";
import { useAccount } from "./account-provider";
import { Icon } from "./icon";
function AnalysisNavigation({ path }: { path: string }) {
  const [open, setOpen] = useState(false);
  const container = useRef<HTMLDivElement>(null);
  const toggle = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    function outside(event: PointerEvent) {
      if (!container.current?.contains(event.target as Node)) setOpen(false);
    }
    document.addEventListener("pointerdown", outside);
    return () => document.removeEventListener("pointerdown", outside);
  }, []);
  return <div className="analysis-navigation" ref={container}
    onPointerLeave={event => { if (event.pointerType === "mouse" && !container.current?.contains(document.activeElement)) setOpen(false); }}
    onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false); }}
    onKeyDown={event => { if (event.key === "Escape") { setOpen(false); toggle.current?.focus(); } }}>
    <Link href="/analysis" onPointerEnter={event => { if (event.pointerType === "mouse") setOpen(true); }} className={path.startsWith("/analysis") ? "active" : ""}
      aria-current={path === "/analysis" ? "page" : undefined} onClick={() => setOpen(false)}>Analysis</Link>
    <button ref={toggle} className="analysis-nav-toggle" aria-label="Analysis pages" aria-expanded={open}
      aria-controls="analysis-pages" onClick={() => setOpen(value => !value)}>⌄</button>
    <div id="analysis-pages" className="analysis-nav-dropdown" hidden={!open}>
      {[["/analysis", "Activity Overview", "Explore congressional buying and selling."],
        ["/analysis/screener", "Research Screener", "Ask questions and save company screens."],
        ["/analysis/strategies", "Strategies", "Explore portfolios built from public disclosures."]].map(([href, label, description]) =>
        <Link key={href} href={href} aria-current={path === href ? "page" : undefined} onClick={() => setOpen(false)}>
          <strong>{label}</strong><span>{description}</span>
        </Link>)}
    </div>
  </div>;
}
export function SiteHeader() {
  const path = usePathname();
  const { session, openSignIn } = useAccount();
  return (
    <>
      <header className="site-header">
        <div className="header-inner">
          <Link href="/" className="wordmark" aria-label="Vail home">
            <svg width="28" height="30" viewBox="0 0 28 30" aria-hidden="true">
              <path
                d="M2 5h7l7 18h-6zM19 5h7L16 28l-3-8z"
                fill="currentColor"
              />
            </svg>
            vail<span className="wordmark-dot">.</span>
          </Link>
          <nav aria-label="Main navigation">
            {[
              ["/", "Latest"],
              ["/politicians", "Politicians"],
              ["/analysis", "Analysis"],
              ["/tracking", "Tracking"],
              ["/sunday-brief", "Sunday Brief"],
            ].map(([href, label]) => href === "/analysis" ? <AnalysisNavigation key={`analysis-menu:${path}`} path={path} /> : (
              <Link
                key={href}
                href={href}
                prefetch={true}
                className={
                  (href === "/" ? path === "/" : path.startsWith(href))
                    ? "active"
                    : ""
                }
                aria-current={
                  (href === "/" ? path === "/" : path.startsWith(href))
                    ? "page"
                    : undefined
                }
              >
                {label}
              </Link>
            ))}
          </nav>
          <div className="header-end">
            <span className="header-note">
              A clearer view of public trades.
            </span>
            {session ? (
              <AccountMenu key={session.user.id} />
            ) : (
              <button className="button secondary small" onClick={openSignIn}>
                Sign in <Icon name="arrow" size={15} />
              </button>
            )}
          </div>
        </div>
      </header>
    </>
  );
}
export function SiteFooter() {
  return (
    <footer className="site-footer container">
      <div>
        <Link className="footer-brand" href="/">
          vail.
        </Link>
        <span>Public information. A clearer perspective.</span>
      </div>
      <p>
        Disclosures can be delayed. Available records may be incomplete.
        <br />
        Disclosed amounts are ranges, not exact trade values.
      </p>
      <Link href="/about">
        About the data <Icon name="external" size={13} />
      </Link>
    </footer>
  );
}

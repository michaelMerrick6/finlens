"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { AccountMenu } from "./account-menu";
import { useAccount } from "./account-provider";
import { Icon } from "./icon";
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
            ].map(([href, label]) => (
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

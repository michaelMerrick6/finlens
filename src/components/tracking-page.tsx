"use client";
import Link from "next/link";
import { useState } from "react";
import { useAccount } from "./account-provider";
import { CompanyLogo } from "./identity-images";
import { Avatar, DisclosureFeed } from "./disclosure-feed";
import { playTrackingOpenSound } from "@/lib/tracking-chime";
import { AddTracking } from "./add-tracking";
import { Icon } from "./icon";
import type { AccountState } from "@/lib/account-types";
function EmailPreference({ account }: { account: AccountState }) {
  const { mutate } = useAccount();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const active = account.subscriptions.email.active;
  return (
    <aside className="email-preference">
      <span className="outlined-icon">
        <Icon name="bell" />
      </span>
      <div>
        <h3>Your list. Your pace.</h3>
        <p>
          {active
            ? `Activity alerts are on for ${account.subscriptions.email.destination}.`
            : "Check in whenever you like, or receive new activity by email."}
        </p>
        <small>
          Alerts include purchases and sales, after a new disclosure is
          detected.
        </small>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
      </div>
      <button
        className={`button ${active ? "secondary" : "primary"}`}
        disabled={busy}
        onClick={async () => {
          setBusy(true);
          setError("");
          try {
            await mutate("/api/account/delivery/email", {
              alertEmail: account.user.email,
              enabled: !active,
            });
          } catch (e) {
            setError(
              e instanceof Error ? e.message : "Could not update email alerts.",
            );
          } finally {
            setBusy(false);
          }
        }}
      >
        {busy
          ? "Saving…"
          : active
            ? "Turn off email alerts"
            : "Enable email alerts"}
      </button>
    </aside>
  );
}
export function TrackingPage() {
  const { account, session, loading, error, openSignIn, refresh, mutate } =
    useAccount();
  const [adding, setAdding] = useState(false);
  const [removing, setRemoving] = useState("");
  const [saveError, setSaveError] = useState("");
  if (!session && !loading)
    return (
      <main id="main" className="container">
        <div className="tracking-intro">
          <span className="eyebrow">A PERSONAL VIEW OF THE PUBLIC RECORD</span>
          <h1>
            A little less searching.
            <br />
            <em>A little more perspective.</em>
          </h1>
          <p>
            Keep the politicians you’re interested in together.
            <br />
            Your tracking list, ready whenever you are.
          </p>
          <button className="button primary" onClick={openSignIn}>
            Create your tracking list
            <Icon name="arrow" size={17} />
          </button>
          <span className="fine-print">
            Sign in with an email link. No password needed.
          </span>
        </div>
        <div className="tracking-steps">
          {[
            [
              "01",
              "Find your people",
              "Explore politicians and the activity they disclose.",
              "people",
            ],
            [
              "02",
              "Make it personal",
              "Track a politician to add their disclosures to your own feed.",
              "bookmark",
            ],
            [
              "03",
              "Stay in the know",
              "Opt in to email alerts, or simply check your list when you like.",
              "bell",
            ],
          ].map(([n, title, text, icon]) => (
            <article key={n}>
              <div>
                <span>{n}</span>
                <Icon name={icon as "people" | "bookmark" | "bell"} size={26} />
              </div>
              <h2>{title}</h2>
              <p>{text}</p>
            </article>
          ))}
        </div>
        <div className="tracking-bottom">
          <p>Just looking around? Everything in the public feed is open.</p>
          <Link className="text-link" href="/">
            Explore the latest disclosures
            <Icon name="arrow" size={17} />
          </Link>
        </div>
      </main>
    );
  if (loading && !account)
    return (
      <main id="main" className="container loading-page">
        <span role="status">Loading your tracking list…</span>
        <div className="skeleton-rows">
          {[0, 1, 2].map((n) => (
            <div key={n} />
          ))}
        </div>
      </main>
    );
  if (error || !account)
    return (
      <main id="main" className="container empty-page">
        <h1>Your list couldn’t load.</h1>
        <p>{error || "Please try again."}</p>
        <button className="button primary" onClick={refresh}>
          Try again
        </button>
      </main>
    );
  const politicians = account.follows.actors.filter(
    (a) => a.actorType === "politician",
  );
  const canonical = (a: (typeof politicians)[number]) =>
    String(
      a.metadata.member_id ||
        (/^[a-z]\d{6}$/i.test(a.actorKey)
          ? a.actorKey.toUpperCase()
          : a.actorKey),
    );
  return (
    <main id="main" className="container">
      <div className="tracking-heading">
        <div className="page-heading">
          <span className="eyebrow">YOUR VIEW OF THE PUBLIC RECORD</span>
          <h1>Tracking.</h1>
          <p>The people and stocks you’re keeping an eye on, all in one place.</p>
        </div>
        <button className="button secondary" onClick={() => { playTrackingOpenSound(); setAdding(true); }}>
          <Icon name="plus" size={17} />
          Add to tracking
        </button>
      </div>
      {adding && <AddTracking onClose={() => setAdding(false)} />}
      <EmailPreference account={account} />
      <div className="tracking-list-heading">
        <h2>Your list</h2>
        <span>
          {account.followCount} of {account.followLimit} tracking slots used
        </span>
      </div>
      {!account.followCount ? (
        <div className="empty-tracking">
          <Icon name="bookmark" size={30} />
          <h2>A fresh perspective starts with someone.</h2>
          <p>
            Add politicians or stocks to start building your list.
          </p>
          <button className="button primary" onClick={() => { playTrackingOpenSound(); setAdding(true); }}>Add to tracking<Icon name="plus" size={16} /></button>
        </div>
      ) : (
        <div className="tracked-people">
          {account.follows.actors.map((a) => (
            <div className="tracked-person" key={a.id}>
              <Avatar name={a.actorName} memberId={a.actorType === "politician" ? canonical(a) : null} />
              <div>
                {a.actorType === "politician" ? (
                  <Link
                    href={`/politicians/${encodeURIComponent(canonical(a))}`}
                  >
                    {a.actorName}
                  </Link>
                ) : (
                  <strong>{a.actorName}</strong>
                )}
                <small>
                  {a.actorType === "politician"
                    ? "Politician"
                    : "Previously tracked " + a.actorType}
                </small>
              </div>
              <button
                className="icon-button"
                disabled={removing === a.id}
                aria-label={`Stop tracking ${a.actorName}`}
                onClick={async () => {
                  setRemoving(a.id);
                  setSaveError("");
                  try {
                    await mutate(
                      "/api/account/follows",
                      { kind: "actor", id: a.id },
                      "DELETE",
                    );
                  } catch (e) {
                    setSaveError(
                      e instanceof Error ? e.message : "Could not remove.",
                    );
                  } finally {
                    setRemoving("");
                  }
                }}
              >
                <Icon name="close" size={17} />
              </button>
            </div>
          ))}
          {account.follows.cluster && (
            <div className="tracked-person">
              <Icon name="grid" />
              <div>
                <strong>Legacy cluster feed</strong>
                <small>Existing tracking preference</small>
              </div>
              <button
                className="icon-button"
                disabled={removing === "cluster"}
                aria-label="Stop tracking legacy cluster feed"
                onClick={async () => {
                  setRemoving("cluster");
                  setSaveError("");
                  try {
                    await mutate("/api/account/cluster-alerts", {
                      enabled: false,
                      channels: [],
                    });
                  } catch (e) {
                    setSaveError(
                      e instanceof Error ? e.message : "Could not remove.",
                    );
                  } finally {
                    setRemoving("");
                  }
                }}
              >
                <Icon name="close" size={17} />
              </button>
            </div>
          )}
          {account.follows.tickers.map((t) => (
            <div className="tracked-person" key={t.id}>
              <CompanyLogo ticker={t.ticker} />
              <Link href={`/ticker/${encodeURIComponent(t.ticker)}`}>
                {t.ticker}
              </Link>
              <button
                className="icon-button"
                disabled={removing === t.id}
                aria-label={`Stop tracking ${t.ticker}`}
                onClick={async () => {
                  setRemoving(t.id);
                  setSaveError("");
                  try {
                    await mutate(
                      "/api/account/follows",
                      { kind: "ticker", id: t.id },
                      "DELETE",
                    );
                  } catch (e) {
                    setSaveError(
                      e instanceof Error ? e.message : "Could not remove.",
                    );
                  } finally {
                    setRemoving("");
                  }
                }}
              >
                <Icon name="close" size={17} />
              </button>
            </div>
          ))}
        </div>
      )}
      {saveError && (
        <p role="alert" className="error">
          {saveError}
        </p>
      )}
      {!!politicians.length && (
        <DisclosureFeed
          key={politicians.map(canonical).join(",")}
          trackedIds={politicians.map(canonical)}
          heading="From your tracked politicians"
        />
      )}
      {!politicians.length && !!account.followCount && (
        <p className="fine-print">
          Track a politician to see their disclosure feed here. Previously
          tracked stocks remain available through their links above.
        </p>
      )}
    </main>
  );
}

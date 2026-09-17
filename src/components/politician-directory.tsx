"use client";
import { readPublicPage, storePublicPage } from "@/lib/public-page-cache";
import Link from "next/link";
import { fetchDirectoryPage } from "@/lib/directory-request";
import { Suspense, useEffect, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import { Avatar } from "./disclosure-feed";
import { Icon } from "./icon";
import { partyLabel } from "@/lib/ui-format";
type Member = {
  id: string;
  first_name: string;
  last_name: string;
  chamber: string;
  party: string;
  state: string;
  volume: number;
  tradeCount: number;
  unpricedCount: number;
  rank: number;
};
function DirectoryContent() {
  const params = useSearchParams();
  const pathname = usePathname();
  const sort = params.get("sort") || "volume";
  const search = params.get("q") || "";
  const chamber = ["House", "Senate"].includes(params.get("chamber") || "")
    ? params.get("chamber")!
    : "All";
  const [q, setQ] = useState(search);
  function updateFilter(key: string, value: string) {
    const next = new URLSearchParams(params.toString());
    if (!value || value === "All") next.delete(key);
    else next.set(key, value);
    window.history.replaceState(
      null,
      "",
      pathname + (next.size ? "?" + next.toString() : ""),
    );
  }
  const setSearch = (value: string) => updateFilter("q", value);
  const setChamber = (value: string) => updateFilter("chamber", value);
  const [offset, setOffset] = useState(0);
  const cacheKey = `directory:${search}:${chamber}:${sort}`;
  const cached = readPublicPage<{members: Member[]; nextOffset: number | null}>(cacheKey);
  const [members, setMembers] = useState<Member[]>(()=>cached?.members || []);
  const [next, setNext] = useState<number | null>(()=>cached?.nextOffset ?? null);
  const [loading, setLoading] = useState(!cached);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    fetchDirectoryPage(
      `/api/politicians?${new URLSearchParams({ q: search, chamber, sort, offset: String(offset) })}`,
      controller.signal,
    )
      .then((data) => {
        if (!controller.signal.aborted) {
          setError("");
          if (offset === 0) storePublicPage(cacheKey, data);
          setMembers((old) =>
            offset === 0 ? data.members : [...old, ...data.members],
          );
          setNext(data.nextOffset);
        }
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [search, chamber, sort, offset, retry, cacheKey]);
  function reset() {
    setLoading(true);
    setError("");
    setMembers([]);
    setOffset(0);
    setRetry((n) => n + 1);
  }
  return (
    <>
      <div className="directory-tools">
        <form
          className="large-search"
          onSubmit={(e) => {
            e.preventDefault();
            reset();
            setSearch(q.trim());
          }}
        >
          <Icon name="search" />
          <input
            placeholder="Find a politician by name…"
            aria-label="Search politicians"
            maxLength={100}
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <button className="button primary small" type="submit">
            Search
          </button>
        </form>
        <div className="segmented">
          {["All", "House", "Senate"].map((c) => (
            <button
              key={c}
              className={chamber === c ? "selected" : ""}
              aria-pressed={chamber === c}
              onClick={() => {
                reset();
                setChamber(c);
              }}
            >
              {c === "All" ? "Both chambers" : c}
            </button>
          ))}
        </div>
      </div>
      <div className="ranking-controls">
        <div className="ranking-period"><strong>Last 12 months</strong><span>Rankings and totals for this period</span></div>
        <label>Rank by <select aria-label="Rank politicians by" value={sort} onChange={e => { reset(); updateFilter("sort", e.target.value); }}>
          <option value="volume">Disclosed volume</option>
          <option value="trades">Number of trades</option>
          <option value="name">Name A–Z</option>
        </select></label>
      </div>
      <p className="fine-print">Volume adds the minimum reported dollar amounts, not exact values. Includes available stock, ETF and option purchases and sales, plus source-verified public partnership units and option exercises. Includes household disclosures; known charitable contributions are excluded. Excludes unidentified tickers, annuities, bonds and other assets. Coverage is incomplete; annual returns are not available.</p>
      <div className="directory-grid">
        {members.map((m) => {
          const name = `${m.first_name} ${m.last_name}`;
          return (
            <Link
              key={m.id}
              href={`/politicians/${encodeURIComponent(m.id)}`}
              className="member-card"
            >
              <div className="member-card-top">
                <Avatar name={name} memberId={m.id} />
                <span className="member-chamber">{sort !== "name" ? `#${m.rank} · ` : ""}{m.chamber}</span>
              </div>
              <h2>{name}</h2>
              <p>
                <span className={`party-dot ${m.party?.toLowerCase()}`} />
                {partyLabel(m.party)} · {m.state || "State not listed"}
              </p>
              <div className="member-metrics">
                <strong>{m.volume ? `$${new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(m.volume)}+` : "—"}</strong>
                <span>Reported minimum · last 12 months</span>
                <span>{m.tradeCount} recorded transactions in this period</span>
                {m.unpricedCount > 0 && <small>{m.unpricedCount} with unavailable amounts</small>}
              </div>
              <span className="member-card-link">
                View disclosures
                <Icon name="arrow" size={17} />
              </span>
            </Link>
          );
        })}
      </div>
      {loading && (
        <div className="feed-loading" role="status">
          <span className="spinner" />
          Loading politicians…
        </div>
      )}
      {error && (
        <div className="empty-state" role="alert">
          <h2>We couldn’t load the directory.</h2>
          <button
            className="button secondary"
            onClick={() => {
              setLoading(true);
              setError("");
              setRetry((n) => n + 1);
            }}
          >
            Try again
          </button>
        </div>
      )}
      {!loading && !error && !members.length && (
        <div className="empty-state">
          <h2>No politicians found.</h2>
          <p>Try a shorter name or a different chamber.</p>
          <button
            className="button secondary"
            onClick={() => {
              reset();
              setQ("");
              window.history.replaceState(null, "", pathname);
            }}
          >
            Clear filters
          </button>
        </div>
      )}
      {!loading && !error && next !== null && (
        <div className="load-more">
          <button
            className="button secondary"
            onClick={() => {
              setLoading(true);
              setOffset(next);
            }}
          >
            Load more politicians
            <Icon name="plus" size={16} />
          </button>
        </div>
      )}
      <p className="fine-print">
        Available active-member records. Directory coverage may be incomplete.
      </p>
    </>
  );
}

export function PoliticianDirectory() {
  return (
    <Suspense
      fallback={
        <div className="feed-loading" role="status">
          Loading politicians…
        </div>
      }
    >
      <DirectoryContent />
    </Suspense>
  );
}

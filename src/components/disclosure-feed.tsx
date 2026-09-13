"use client";
import Link from "next/link";
import { Suspense, useEffect, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import {
  dateLabel,
  directionLabel,
  partyLabel,
  sourceLink,
  type Disclosure,
} from "@/lib/ui-format";
import { formatPoliticianAmountRange } from "@/lib/politician-amount-range";
import { Icon } from "./icon";
import { Avatar, CompanyLogo } from "./identity-images";
import { Modal } from "./modal";

export { Avatar } from "./identity-images";
export function TradeDetails({
  trade,
  onClose,
}: {
  trade: Disclosure;
  onClose: () => void;
}) {
  const source = sourceLink(trade.source_url);
  return (
    <Modal title="Inside the disclosure" onClose={onClose}>
      <div className="detail-person">
        <Avatar name={trade.politician_name || "Unknown"} memberId={trade.member_id} />
        <div>
          <h2>{trade.politician_name}</h2>
          <p className="muted">
            {trade.congress_members?.chamber || trade.chamber} ·{" "}
            {partyLabel(trade.congress_members?.party || trade.party)}
          </p>
        </div>
      </div>
      <div className="detail-asset">
        <span className={`badge ${trade.transaction_type}`}>
          {trade.activity_label || (trade.is_contribution ? "Contribution" : directionLabel(trade.transaction_type))}
        </span>
        <h3 className="asset-identity"><CompanyLogo ticker={trade.ticker} />{trade.ticker}</h3>
        <p>{trade.asset_name || "Disclosed security"}</p>
        {trade.activity_note && <p className="fine-print">{trade.activity_note}</p>}
      </div>
      <dl className="detail-facts">
        <div>
          <dt>Disclosed amount</dt>
          <dd>{formatPoliticianAmountRange(trade.amount_range)}</dd>
        </div>
        <div>
          <dt>Transaction date</dt>
          <dd>{dateLabel(trade.transaction_date)}</dd>
        </div>
        <div>
          <dt>Filing date</dt>
          <dd>{dateLabel(trade.published_date)}</dd>
        </div>
      </dl>
      <p className="fine-print">
        The filing date is when this activity was disclosed, not when the trade
        took place. Amounts are reported as ranges.
      </p>
      <div className="detail-actions">
        {source && (
          <a
            className="button primary"
            href={source}
            target="_blank"
            rel="noopener noreferrer"
          >
            Read original filing
            <Icon name="external" size={15} />
          </a>
        )}
        {trade.member_id && (
          <Link
            className="button secondary"
            href={`/politicians/${encodeURIComponent(trade.member_id)}`}
            onClick={onClose}
          >
            View politician
          </Link>
        )}
      </div>
    </Modal>
  );
}

function DisclosureFeedContent({
  memberId,
  ticker,
  trackedIds,
  showFilters = true,
  heading = "Latest disclosures",
}: {
  memberId?: string;
  ticker?: string;
  trackedIds?: string[];
  showFilters?: boolean;
  heading?: string;
}) {
  const searchParams = useSearchParams();
  const pathname = usePathname();
  const committedQuery = searchParams.get("q") || "";
  const direction = ["buy", "sell"].includes(
    searchParams.get("direction") || "",
  )
    ? searchParams.get("direction")!
    : "All";
  const chamber = ["House", "Senate"].includes(
    searchParams.get("chamber") || "",
  )
    ? searchParams.get("chamber")!
    : "All";
  const [query, setQuery] = useState(committedQuery);
  function updateFilter(key: string, value: string) {
    const params = new URLSearchParams(searchParams.toString());
    if (!value || value === "All") params.delete(key);
    else params.set(key, value);
    window.history.replaceState(
      null,
      "",
      pathname + (params.size ? "?" + params.toString() : ""),
    );
  }
  const setCommittedQuery = (value: string) => updateFilter("q", value);
  const setDirection = (value: string) => updateFilter("direction", value);
  const setChamber = (value: string) => updateFilter("chamber", value);
  const [rows, setRows] = useState<Disclosure[]>([]);
  const [next, setNext] = useState<number | null>(null);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  const [selected, setSelected] = useState<Disclosure | null>(null);
  const ids = trackedIds?.join(",");
  useEffect(() => {
    const controller = new AbortController();
    const params = new URLSearchParams({
      q: committedQuery,
      direction,
      chamber,
      limit: "20",
      offset: String(offset),
    });
    if (memberId) params.set("memberId", memberId);
    if (ticker) params.set("ticker", ticker);
    if (ids) params.set("memberIds", ids);
    async function loadDisclosures() {
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const response = await fetch(`/api/search-trades?${params}`, { signal: controller.signal });
          if (response.status >= 500 && attempt === 0) continue;
          return response;
        } catch (error) {
          if (controller.signal.aborted || attempt === 1) throw error;
        }
      }
      throw new Error("We couldn’t load these disclosures. Please try again.");
    }
    loadDisclosures()
      .then(async (r) => {
        if (!r.ok)
          throw new Error(
            "We couldn’t load these disclosures. Please try again.",
          );
        return r.json();
      })
      .then((data) => {
        if (controller.signal.aborted) return;
        setRows((old) =>
          offset === 0
            ? data.trades
            : [
                ...old,
                ...data.trades.filter(
                  (r: Disclosure) => !old.some((o) => o.id === r.id),
                ),
              ],
        );
        setNext(
          data.hasMore && data.nextOffset > offset ? data.nextOffset : null,
        );
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [
    committedQuery,
    direction,
    chamber,
    offset,
    memberId,
    ticker,
    ids,
    retry,
  ]);
  function reset() {
    setLoading(true);
    setError("");
    setOffset(0);
    setRows([]);
    setNext(null);
    setRetry((n) => n + 1);
  }
  return (
    <section className="feed-section" aria-labelledby="feed-heading">
      <div className="section-heading">
        <div>
          <div className="eyebrow">THE PUBLIC RECORD</div>
          <h2 id="feed-heading">{heading}</h2>
        </div>
        <span className="live-label">
          <span />
          Sorted by filing date
        </span>
      </div>
      {showFilters && (
        <div className="feed-toolbar">
          <div className="segmented" aria-label="Transaction type">
            {[
              ["All", "All activity"],
              ["buy", "Purchases"],
              ["sell", "Sales"],
            ].map(([value, label]) => (
              <button
                key={value}
                aria-pressed={direction === value}
                onClick={() => {
                  reset();
                  setDirection(value);
                }}
                className={direction === value ? "selected" : ""}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="toolbar-end">
            {!memberId && !ids && (
              <label className="select-wrap">
                <span className="sr-only">Chamber</span>
                <select
                  value={chamber}
                  onChange={(e) => {
                    reset();
                    setChamber(e.target.value);
                  }}
                >
                  <option value="All">All chambers</option>
                  <option>House</option>
                  <option>Senate</option>
                </select>
              </label>
            )}
            {!memberId && !ticker && !ids && (
              <form
                className="compact-search"
                onSubmit={(e) => {
                  e.preventDefault();
                  reset();
                  setCommittedQuery(query.trim());
                }}
              >
                <Icon name="search" size={16} />
                <input
                  aria-label="Search disclosures"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Politician or stock"
                  maxLength={100}
                />
                <button type="submit" aria-label="Run disclosure search">
                  <Icon name="arrow" size={15} />
                </button>
              </form>
            )}
          </div>
        </div>
      )}
      {committedQuery && (
        <div className="search-context">
          Results for “{committedQuery}”
          <button
            onClick={() => {
              reset();
              setQuery("");
              setCommittedQuery("");
            }}
          >
            Clear search ×
          </button>
        </div>
      )}
      <div className="table-scroll">
        <table className="trade-table">
          <thead>
            <tr>
              <th>Politician</th>
              <th>Asset</th>
              <th>Activity</th>
              <th>Disclosed amount</th>
              <th>Filed</th>
              <th>
                <span className="sr-only">Details</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((trade) => (
              <tr key={trade.id}>
                <td>
                  <div className="person-cell">
                    <Avatar name={trade.politician_name || "Unknown"} memberId={trade.member_id} />
                    <div>
                      {trade.member_id ? (
                        <Link
                          href={`/politicians/${encodeURIComponent(trade.member_id)}`}
                        >
                          {trade.politician_name}
                        </Link>
                      ) : (
                        <strong>{trade.politician_name}</strong>
                      )}
                      <small>
                        <span
                          className={`party-dot ${(trade.congress_members?.party || trade.party || "").toLowerCase()}`}
                        />
                        {trade.congress_members?.chamber || trade.chamber}
                        {trade.congress_members?.state
                          ? ` · ${trade.congress_members.state}`
                          : ""}
                      </small>
                    </div>
                  </div>
                </td>
                <td>
                  <Link
                    className="ticker-name asset-identity"
                    href={`/ticker/${encodeURIComponent(trade.ticker || "")}`}
                  >
                    <CompanyLogo ticker={trade.ticker} />
                    {trade.ticker}
                  </Link>
                  <small className="asset-description">
                    {trade.asset_name || "Disclosed security"}
                  </small>
                </td>
                <td>
                  <span className={`badge ${trade.transaction_type}`}>
                    {trade.activity_label || (trade.is_contribution ? "Contribution" : directionLabel(trade.transaction_type))}
                  </span>
                </td>
                <td className="amount">
                  {formatPoliticianAmountRange(trade.amount_range)}
                </td>
                <td className="date-cell">
                  {dateLabel(trade.published_date)}
                  <small>Traded {dateLabel(trade.transaction_date)}</small>
                </td>
                <td>
                  <button
                    className="icon-button row-open"
                    aria-label={`View ${trade.politician_name} ${trade.ticker} disclosure`}
                    onClick={() => setSelected(trade)}
                  >
                    <Icon name="chevron" size={17} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {loading && (
        <div className="feed-loading" role="status" aria-live="polite">
          <span className="spinner" />
          Loading disclosures…
          {!rows.length && (
            <div className="skeleton-rows">
              {[0, 1, 2, 3].map((n) => (
                <div key={n} />
              ))}
            </div>
          )}
        </div>
      )}
      {error && (
        <div className="empty-state" role="alert">
          <h3>Something didn’t load.</h3>
          <p>{error}</p>
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
      {!loading && !error && !rows.length && (
        <div className="empty-state">
          <Icon name="search" size={28} />
          <h3>No matching disclosures.</h3>
          <p>
            Try a different search or filter. An empty result doesn’t mean no
            trades took place.
          </p>
        </div>
      )}
      {!loading && !error && next !== null && (
        <div className="load-more">
          <button
            className="button secondary"
            onClick={() => {
              setLoading(true);
              setError("");
              setOffset(next);
            }}
          >
            Load more disclosures
            <Icon name="plus" size={16} />
          </button>
        </div>
      )}
      <div className="table-note">
        <span>Reported ranges · Original sources included</span>
        <Link href="/about">
          How to read this data <Icon name="arrow" size={13} />
        </Link>
      </div>
      {selected && (
        <TradeDetails trade={selected} onClose={() => setSelected(null)} />
      )}
    </section>
  );
}

export function DisclosureFeed(
  props: Parameters<typeof DisclosureFeedContent>[0],
) {
  return (
    <Suspense
      fallback={
        <div className="feed-loading" role="status">
          Loading disclosures…
        </div>
      }
    >
      <DisclosureFeedContent {...props} />
    </Suspense>
  );
}

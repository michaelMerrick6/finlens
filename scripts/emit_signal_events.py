import hashlib
import os
import re
from datetime import timedelta

from pipeline_support import emit_summary, get_supabase_client, utc_now


LOOKBACK_DAYS = int(os.environ.get("SIGNAL_EVENT_LOOKBACK_DAYS", "30"))
MAX_ROWS_PER_SOURCE = int(os.environ.get("SIGNAL_EVENT_MAX_ROWS", "1000"))


def stable_id(parts: list[str]) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def extract_sec_accession(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    match = re.search(r"(\d{10}-\d{2}-\d{6})", raw)
    if match:
        return match.group(1)
    match = re.search(r"/(\d{10}\d{6})/", raw)
    if match:
        compact = match.group(1)
        return f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"
    return None


def normalize_direction(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"buy", "purchase", "p"}:
        return "buy"
    if normalized in {"sell", "sale", "s"}:
        return "sell"
    if normalized in {"increase", "increased"}:
        return "increase"
    if normalized in {"decrease", "decreased", "reduce", "reduced"}:
        return "decrease"
    if normalized in {"hold", "held"}:
        return "hold"
    if normalized == "exchange":
        return "exchange"
    return normalized


def past_tense(direction: str | None) -> str:
    mapping = {
        "buy": "bought",
        "sell": "sold",
        "increase": "increased",
        "decrease": "decreased",
        "hold": "held",
        "exchange": "exchanged",
        "unknown": "reported",
    }
    return mapping.get(direction or "unknown", direction or "reported")


def politician_score(trade: dict) -> float:
    direction = normalize_direction(trade.get("transaction_type"))
    amount = (trade.get("amount_range") or "").upper()
    score = 0.72 if direction == "buy" else 0.56 if direction == "sell" else 0.45
    if "1,000,001" in amount or "5,000,001" in amount or "50,000,001" in amount:
        score += 0.18
    elif "500,001" in amount or "250,001" in amount:
        score += 0.10
    elif "50,001" in amount:
        score += 0.05
    return round(min(score, 0.99), 2)


def insider_score(trade: dict) -> float:
    direction = normalize_direction(trade.get("transaction_code"))
    value = float(trade.get("value") or 0)
    score = 0.68 if direction == "buy" else 0.48 if direction == "sell" else 0.4
    if value >= 10_000_000:
        score += 0.18
    elif value >= 1_000_000:
        score += 0.12
    elif value >= 250_000:
        score += 0.06
    return round(min(score, 0.99), 2)


def fund_score(holding: dict) -> float:
    qoq_change_percent = float(holding.get("qoq_change_percent") or 0)
    if qoq_change_percent > 0:
        direction = "increase"
    elif qoq_change_percent < 0:
        direction = "decrease"
    else:
        direction = "hold"
    change_pct = abs(qoq_change_percent)
    score = 0.58 if direction == "increase" else 0.46 if direction == "decrease" else 0.4
    if change_pct >= 50:
        score += 0.18
    elif change_pct >= 20:
        score += 0.12
    elif change_pct >= 10:
        score += 0.06
    return round(min(score, 0.99), 2)


def build_politician_events(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    events: list[dict] = []
    raw_filings: list[dict] = []
    for trade in rows:
        direction = normalize_direction(trade.get("transaction_type")) or "unknown"
        source_document_id = trade.get("doc_id") or stable_id(
            [
                trade.get("politician_name") or "",
                trade.get("ticker") or "",
                trade.get("transaction_date") or "",
                direction,
                trade.get("amount_range") or "",
            ]
        )
        ticker = (trade.get("ticker") or "").upper()
        actor_name = trade.get("politician_name") or "Unknown politician"
        title = f"Congress trade: {actor_name} {past_tense(direction)} {ticker}"
        summary = (
            f"{actor_name} reported a congressional trade in {ticker} and "
            f"{past_tense(direction)} "
            f"for {trade.get('amount_range') or 'an undisclosed range'}."
        )

        raw_filings.append(
            {
                "source": "congress",
                "filing_type": "politician_trade",
                "source_document_id": source_document_id,
                "source_url": trade.get("source_url"),
                "ticker": ticker,
                "filer_name": actor_name,
                "filed_at": trade.get("published_date"),
                "payload": trade,
            }
        )

        events.append(
            {
                "source": "congress",
                "signal_type": "politician_trade",
                "source_document_id": source_document_id,
                "ticker": ticker,
                "actor_name": actor_name,
                "actor_type": "politician",
                "direction": direction,
                "occurred_at": trade.get("transaction_date"),
                "published_at": trade.get("published_date"),
                "importance_score": politician_score(trade),
                "title": title,
                "summary": summary,
                "source_url": trade.get("source_url"),
                "payload": trade,
            }
        )
    return raw_filings, events


def build_insider_events(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    events: list[dict] = []
    raw_filings: list[dict] = []
    for trade in rows:
        direction = normalize_direction(trade.get("transaction_code")) or "unknown"
        accession = extract_sec_accession(trade.get("source_url"))
        source_document_id = stable_id(
            [
                accession or trade.get("source_url") or "",
                trade.get("ticker") or "",
                trade.get("transaction_date") or "",
                direction,
                str(trade.get("amount") or ""),
                str(trade.get("price") or ""),
                trade.get("filer_name") or "",
            ]
        )
        ticker = (trade.get("ticker") or "").upper()
        actor_name = trade.get("filer_name") or "Unknown insider"
        value = float(trade.get("value") or 0)
        formatted_value = f"${value:,.0f}" if value else "an undisclosed value"
        title = f"Insider trade: {actor_name} {past_tense(direction)} {ticker}"
        summary = f"{actor_name} reported an insider trade in {ticker} worth {formatted_value} and {past_tense(direction)}."

        raw_filings.append(
            {
                "source": "insider",
                "filing_type": "form_4_trade",
                "source_document_id": source_document_id,
                "source_url": trade.get("source_url"),
                "ticker": ticker,
                "filer_name": actor_name,
                "filed_at": trade.get("published_date"),
                "payload": trade,
            }
        )

        events.append(
            {
                "source": "insider",
                "signal_type": "insider_trade",
                "source_document_id": source_document_id,
                "ticker": ticker,
                "actor_name": actor_name,
                "actor_type": "insider",
                "direction": direction,
                "occurred_at": trade.get("transaction_date"),
                "published_at": trade.get("published_date"),
                "importance_score": insider_score(trade),
                "title": title,
                "summary": summary,
                "source_url": trade.get("source_url"),
                "payload": trade,
            }
        )
    return raw_filings, events


def build_fund_events(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    events: list[dict] = []
    raw_filings: list[dict] = []
    for holding in rows:
        qoq_change_percent = float(holding.get("qoq_change_percent") or 0)
        if qoq_change_percent > 0:
            direction = "increase"
        elif qoq_change_percent < 0:
            direction = "decrease"
        else:
            direction = "hold"
        source_document_id = stable_id(
            [
                holding.get("fund_name") or "",
                holding.get("ticker") or "",
                holding.get("report_period") or "",
            ]
        )
        ticker = (holding.get("ticker") or "").upper()
        actor_name = holding.get("fund_name") or "Unknown fund"
        change_pct = qoq_change_percent
        title = f"13F update: {actor_name} {past_tense(direction)} {ticker}"
        summary = (
            f"{actor_name} {past_tense(direction)} its {ticker} position by "
            f"{abs(change_pct):.2f}% for the {holding.get('report_period')} report period."
        )

        raw_filings.append(
            {
                "source": "hedge_fund",
                "filing_type": "13f_holding",
                "source_document_id": source_document_id,
                "source_url": holding.get("source_url"),
                "ticker": ticker,
                "filer_name": actor_name,
                "filed_at": holding.get("published_date"),
                "payload": holding,
            }
        )

        events.append(
            {
                "source": "hedge_fund",
                "signal_type": "fund_position_change",
                "source_document_id": source_document_id,
                "ticker": ticker,
                "actor_name": actor_name,
                "actor_type": "fund",
                "direction": direction,
                "occurred_at": holding.get("report_period"),
                "published_at": holding.get("published_date"),
                "importance_score": fund_score(holding),
                "title": title,
                "summary": summary,
                "source_url": holding.get("source_url"),
                "payload": holding,
            }
        )
    return raw_filings, events


def fetch_recent_rows(supabase, table: str) -> list[dict]:
    since_ts = (utc_now() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    response = (
        supabase.table(table)
        .select("*")
        .gte("created_at", since_ts)
        .order("created_at", desc=True)
        .limit(MAX_ROWS_PER_SOURCE)
        .execute()
    )
    return response.data or []


def dedupe_by_source_document_id(rows: list[dict]) -> list[dict]:
    deduped: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row["source"], row["source_document_id"])
        deduped[key] = row
    return list(deduped.values())


def main() -> None:
    print("Emitting canonical signal events...")
    supabase = get_supabase_client()

    pol_rows = fetch_recent_rows(supabase, "politician_trades")
    insider_rows = fetch_recent_rows(supabase, "insider_trades")
    fund_rows = fetch_recent_rows(supabase, "institutional_holdings")

    raw_filings: list[dict] = []
    signal_events: list[dict] = []

    for builder, rows in (
        (build_politician_events, pol_rows),
        (build_insider_events, insider_rows),
        (build_fund_events, fund_rows),
    ):
        raw_payloads, event_payloads = builder(rows)
        raw_filings.extend(raw_payloads)
        signal_events.extend(event_payloads)

    raw_filings = dedupe_by_source_document_id(raw_filings)
    signal_events = dedupe_by_source_document_id(signal_events)

    if raw_filings:
        supabase.table("raw_filings").upsert(
            raw_filings,
            on_conflict="source,source_document_id",
        ).execute()

    if signal_events:
        supabase.table("signal_events").upsert(
            signal_events,
            on_conflict="source,source_document_id",
        ).execute()

    summary = {
        "raw_filings_upserted": len(raw_filings),
        "signal_events_created": len(signal_events),
        "politician_rows_seen": len(pol_rows),
        "insider_rows_seen": len(insider_rows),
        "fund_rows_seen": len(fund_rows),
        "lookback_days": LOOKBACK_DAYS,
    }
    emit_summary(summary)
    print(
        "Signal event emission complete: "
        f"{summary['signal_events_created']} events across "
        f"{summary['politician_rows_seen']} politician trades, "
        f"{summary['insider_rows_seen']} insider trades, and "
        f"{summary['fund_rows_seen']} fund rows."
    )


if __name__ == "__main__":
    main()

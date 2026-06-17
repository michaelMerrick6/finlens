import os
import re
import time
import math
from datetime import timedelta

from pipeline_support import emit_summary, get_supabase_client, utc_now
from shared_utils import extract_sec_accession, stable_id
from signal_baseline_support import enrich_events_with_baseline_snapshots


LOOKBACK_DAYS = int(os.environ.get("SIGNAL_EVENT_LOOKBACK_DAYS", "30"))
MAX_ROWS_PER_SOURCE = int(os.environ.get("SIGNAL_EVENT_MAX_ROWS", "5000"))
DEFAULT_UPSERT_CHUNK_SIZE = int(os.environ.get("SIGNAL_EVENT_UPSERT_CHUNK_SIZE", "150"))
RAW_FILINGS_UPSERT_CHUNK_SIZE = int(
    os.environ.get("SIGNAL_EVENT_RAW_FILINGS_UPSERT_CHUNK_SIZE", str(min(DEFAULT_UPSERT_CHUNK_SIZE, 100)))
)
SIGNAL_EVENTS_UPSERT_CHUNK_SIZE = int(
    os.environ.get("SIGNAL_EVENT_SIGNAL_EVENTS_UPSERT_CHUNK_SIZE", str(DEFAULT_UPSERT_CHUNK_SIZE))
)
UPSERT_RETRY_DELAYS = (2, 5, 10)
ASSET_NAME_LOOKUP_CHUNK_SIZE = int(os.environ.get("SIGNAL_EVENT_ASSET_NAME_LOOKUP_CHUNK_SIZE", "50"))
ASSET_NAME_LOOKUP_RETRY_DELAYS = (2, 5)
ENABLE_CONGRESS_ASSET_NAME_PRESERVATION = os.environ.get("SIGNAL_EVENT_PRESERVE_CONGRESS_ASSET_NAMES", "0").strip() == "1"
ALLOW_RAW_FILINGS_UPSERT_FAILURE = os.environ.get("SIGNAL_EVENT_ALLOW_RAW_FILINGS_FAILURE", "1").strip() != "0"
ENABLE_SIGNAL_EVENT_BASELINE_ENRICHMENT = os.environ.get("SIGNAL_EVENT_ENRICH_BASELINES", "0").strip() == "1"
DE_MINIMIS_PREVIOUS_SHARE_COUNT = 100
DE_MINIMIS_PREVIOUS_SHARE_RATIO = 0.001

TABLE_DATE_COLUMN = {
    "politician_trades": "published_date",
    "insider_trades": "published_date",
    "institutional_holdings": "published_date",
}








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


def optional_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def optional_int(value) -> int | None:
    numeric = optional_float(value)
    return int(numeric) if numeric is not None else None


def fund_previous_shares(holding: dict) -> int | None:
    shares_held = optional_int(holding.get("shares_held"))
    qoq_change_shares = optional_int(holding.get("qoq_change_shares"))
    if shares_held is None or qoq_change_shares is None:
        return None
    return shares_held - qoq_change_shares


def is_material_new_fund_position(holding: dict) -> bool:
    shares_held = optional_int(holding.get("shares_held"))
    qoq_change_shares = optional_int(holding.get("qoq_change_shares"))
    previous_shares = fund_previous_shares(holding)
    if shares_held is None or qoq_change_shares is None or shares_held <= 0 or qoq_change_shares <= 0:
        return False
    if optional_float(holding.get("qoq_change_percent")) is None or previous_shares is None or previous_shares <= 0:
        return True
    return (
        previous_shares <= DE_MINIMIS_PREVIOUS_SHARE_COUNT
        and previous_shares / shares_held <= DE_MINIMIS_PREVIOUS_SHARE_RATIO
    )


def compact_share_quantity(value: int | float | None) -> str:
    absolute = abs(float(value or 0))

    def fmt(amount: float) -> str:
        return f"{amount:.0f}" if amount >= 10 else f"{amount:.1f}".rstrip("0").rstrip(".")

    if absolute >= 1_000_000_000:
        return f"{fmt(absolute / 1_000_000_000)}B shares"
    if absolute >= 1_000_000:
        return f"{fmt(absolute / 1_000_000)}M shares"
    if absolute >= 1_000:
        return f"{fmt(absolute / 1_000)}K shares"
    rounded = round(absolute)
    return f"{rounded:,} {'share' if rounded == 1 else 'shares'}"


def format_fund_change_label(holding: dict) -> str:
    change_type = fund_change_type(holding)
    qoq_change_shares = optional_int(holding.get("qoq_change_shares"))
    if change_type == "new":
        return "New position"
    if change_type == "exit":
        return "Exited"
    if qoq_change_shares is not None:
        if qoq_change_shares == 0:
            return "No share change"
        prefix = "+" if qoq_change_shares > 0 else "-"
        return f"{prefix}{compact_share_quantity(qoq_change_shares)}"
    return "Position changed"


def normalized_13f_value(value) -> float:
    amount = optional_float(value) or 0
    return amount / 1_000


def fund_change_type(holding: dict) -> str | None:
    if holding.get("qoq_change_percent") is None and holding.get("qoq_change_shares") is None:
        return None
    qoq_change_shares = optional_int(holding.get("qoq_change_shares"))
    shares_held = optional_int(holding.get("shares_held")) or 0
    if qoq_change_shares is not None:
        if shares_held <= 0 and qoq_change_shares < 0:
            return "exit"
        if is_material_new_fund_position(holding):
            return "new"
        if qoq_change_shares > 0:
            return "increase"
        if qoq_change_shares < 0:
            return "decrease"
        return "hold"

    qoq_change_percent = optional_float(holding.get("qoq_change_percent")) or 0
    if shares_held <= 0 and qoq_change_percent < 0:
        return "exit"
    if qoq_change_percent > 0:
        return "increase"
    if qoq_change_percent < 0:
        return "decrease"
    return "hold"


def fund_score(holding: dict) -> float:
    change_type = fund_change_type(holding)
    if change_type in {"new", "increase"}:
        direction = "increase"
    elif change_type in {"exit", "decrease"}:
        direction = "decrease"
    else:
        direction = "hold"
    score = 0.58 if direction == "increase" else 0.46 if direction == "decrease" else 0.4
    position_value = normalized_13f_value(holding.get("value_held"))
    changed_shares = abs(optional_int(holding.get("qoq_change_shares")) or 0)
    if change_type in {"new", "exit"}:
        score += 0.12
    if position_value >= 100_000_000:
        score += 0.12
    elif position_value >= 10_000_000:
        score += 0.08
    elif position_value >= 1_000_000:
        score += 0.04
    if changed_shares >= 1_000_000:
        score += 0.08
    elif changed_shares >= 100_000:
        score += 0.05
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
    filing_summaries: dict[tuple[str, str, str, str], dict] = {}
    for holding in rows:
        fund_name = str(holding.get("fund_name") or "").strip()
        report_period = str(holding.get("report_period") or "").strip()
        published_date = str(holding.get("published_date") or "").strip()
        source_url = str(holding.get("source_url") or "").strip()
        if fund_name and report_period:
            filing_key = (fund_name, report_period, published_date, source_url)
            filing_summary = filing_summaries.setdefault(
                filing_key,
                {
                    "fund_name": fund_name,
                    "report_period": report_period,
                    "published_date": published_date,
                    "source_url": source_url or None,
                    "holding_count": 0,
                    "total_value": 0.0,
                    "tickers": set(),
                },
            )
            filing_summary["holding_count"] += 1
            filing_summary["total_value"] += normalized_13f_value(holding.get("value_held"))
            ticker_for_summary = str(holding.get("ticker") or "").strip().upper()
            if ticker_for_summary:
                filing_summary["tickers"].add(ticker_for_summary)

        change_type = fund_change_type(holding)
        if not holding.get("ticker") or change_type is None:
            continue
        if change_type in {"new", "increase"}:
            direction = "increase"
        elif change_type in {"exit", "decrease"}:
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
        event_payload = {**holding, "change_type": change_type, "position_change_label": format_fund_change_label(holding)}
        position_value = normalized_13f_value(holding.get("value_held"))
        formatted_value = f"${position_value:,.0f}" if position_value else "an undisclosed value"
        if change_type == "new":
            title = f"13F update: {actor_name} initiated {ticker}"
            summary = (
                f"{actor_name} disclosed a new {ticker} position worth {formatted_value} "
                f"for the {holding.get('report_period')} report period."
            )
        elif change_type == "exit":
            title = f"13F update: {actor_name} exited {ticker}"
            summary = (
                f"{actor_name} fully exited its {ticker} position "
                f"for the {holding.get('report_period')} report period."
            )
        elif change_type == "hold":
            title = f"13F update: {actor_name} held {ticker} flat"
            summary = (
                f"{actor_name} reported no share-count change in its {ticker} position "
                f"for the {holding.get('report_period')} report period."
            )
        else:
            changed_shares = compact_share_quantity(optional_int(holding.get("qoq_change_shares")))
            title = f"13F update: {actor_name} {past_tense(direction)} {ticker}"
            summary = (
                f"{actor_name} {past_tense(direction)} its {ticker} position by "
                f"{changed_shares} for the {holding.get('report_period')} report period."
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
                "payload": event_payload,
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
                "payload": event_payload,
            }
        )

    for filing in filing_summaries.values():
        source_document_id = stable_id(
            [
                "fund_filing_received",
                filing["fund_name"],
                filing["report_period"],
                filing["published_date"],
                filing["source_url"] or "",
            ]
        )
        total_value = filing["total_value"]
        formatted_value = f"${total_value:,.0f}" if total_value else "an undisclosed value"
        holding_count = int(filing["holding_count"] or 0)
        tickers = sorted(filing["tickers"])
        payload = {
            "fund_name": filing["fund_name"],
            "report_period": filing["report_period"],
            "published_date": filing["published_date"],
            "filing_type": "13F-HR",
            "holding_count": holding_count,
            "total_value": total_value,
            "sample_tickers": tickers[:20],
        }
        events.append(
            {
                "source": "hedge_fund",
                "signal_type": "fund_filing_received",
                "source_document_id": source_document_id,
                "ticker": "13F",
                "actor_name": filing["fund_name"],
                "actor_type": "fund",
                "direction": "filed",
                "occurred_at": filing["report_period"],
                "published_at": filing["published_date"],
                "importance_score": 0.72,
                "title": f"13F filed: {filing['fund_name']} reported {holding_count:,} holdings",
                "summary": (
                    f"{filing['fund_name']} filed its 13F-HR for {filing['report_period']} with "
                    f"{holding_count:,} holdings worth {formatted_value}."
                ),
                "source_url": filing["source_url"],
                "payload": payload,
            }
        )

    return raw_filings, events


def fetch_recent_rows(supabase, table: str) -> list[dict]:
    since_date = (utc_now() - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    date_column = TABLE_DATE_COLUMN.get(table, "created_at")
    rows: list[dict] = []
    offset = 0
    while len(rows) < MAX_ROWS_PER_SOURCE:
        upper = min(offset + 999, MAX_ROWS_PER_SOURCE - 1)
        query = supabase.table(table).select("*").gte(date_column, since_date).order(date_column, desc=True).range(offset, upper)
        response = query.execute()
        batch = response.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < (upper - offset + 1):
            break
        offset += len(batch)
    return rows[:MAX_ROWS_PER_SOURCE]


def dedupe_by_source_document_id(rows: list[dict]) -> list[dict]:
    deduped: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row["source"], row["source_document_id"])
        deduped[key] = row
    return list(deduped.values())


def chunked_upsert(supabase, table: str, rows: list[dict], *, on_conflict: str, chunk_size: int) -> None:
    if not rows:
        return
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        last_exc = None
        for attempt in range(len(UPSERT_RETRY_DELAYS) + 1):
            try:
                supabase.table(table).upsert(chunk, on_conflict=on_conflict).execute()
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if "statement timeout" not in str(exc).lower() or attempt >= len(UPSERT_RETRY_DELAYS):
                    raise
                time.sleep(UPSERT_RETRY_DELAYS[attempt])
        if last_exc is not None:
            raise last_exc


def preserve_existing_congress_asset_names(supabase, raw_filings: list[dict]) -> None:
    congress_rows = [
        row
        for row in raw_filings
        if row.get("source") == "congress" and not str((row.get("payload") or {}).get("asset_name") or "").strip()
    ]
    if not congress_rows:
        return

    existing_by_doc_id: dict[str, str] = {}
    doc_ids = [str(row.get("source_document_id") or "").strip() for row in congress_rows if row.get("source_document_id")]

    for index in range(0, len(doc_ids), ASSET_NAME_LOOKUP_CHUNK_SIZE):
        chunk = doc_ids[index : index + ASSET_NAME_LOOKUP_CHUNK_SIZE]
        response = None
        last_exc = None
        for attempt in range(len(ASSET_NAME_LOOKUP_RETRY_DELAYS) + 1):
            try:
                response = (
                    supabase.table("raw_filings")
                    .select("source_document_id,payload")
                    .eq("source", "congress")
                    .in_("source_document_id", chunk)
                    .execute()
                )
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if "statement timeout" not in str(exc).lower() or attempt >= len(ASSET_NAME_LOOKUP_RETRY_DELAYS):
                    response = None
                    break
                time.sleep(ASSET_NAME_LOOKUP_RETRY_DELAYS[attempt])
        if response is None:
            if last_exc is not None:
                print(f"Skipping congress asset-name preservation chunk after timeout: {last_exc}")
            continue
        for existing in response.data or []:
            payload = existing.get("payload") or {}
            asset_name = str(payload.get("asset_name") or "").strip()
            if asset_name:
                existing_by_doc_id[str(existing.get("source_document_id") or "").strip()] = asset_name

    for row in congress_rows:
        payload = dict(row.get("payload") or {})
        if str(payload.get("asset_name") or "").strip():
            continue
        existing_asset_name = existing_by_doc_id.get(str(row.get("source_document_id") or "").strip(), "")
        if existing_asset_name:
            payload["asset_name"] = existing_asset_name
            row["payload"] = payload


def main() -> None:
    print("Emitting canonical signal events...")
    supabase = get_supabase_client()

    pol_rows = fetch_recent_rows(supabase, "politician_trades")
    insider_rows = fetch_recent_rows(supabase, "insider_trades")
    fund_rows = fetch_recent_rows(supabase, "institutional_holdings")
    print(
        "Fetched recent source rows: "
        f"{len(pol_rows)} congress, {len(insider_rows)} insider, {len(fund_rows)} fund.",
        flush=True,
    )

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
    print(
        f"Prepared {len(signal_events)} signal events and {len(raw_filings)} raw filing rows before enrichment.",
        flush=True,
    )
    if ENABLE_SIGNAL_EVENT_BASELINE_ENRICHMENT:
        signal_events = enrich_events_with_baseline_snapshots(signal_events)
        print(f"Signal-event baseline enrichment complete for {len(signal_events)} rows.", flush=True)
    else:
        print("Skipping signal-event baseline enrichment in the emitter critical path.", flush=True)

    print(
        f"Upserting {len(signal_events)} signal events in chunks of {SIGNAL_EVENTS_UPSERT_CHUNK_SIZE}...",
        flush=True,
    )
    chunked_upsert(
        supabase,
        "signal_events",
        signal_events,
        on_conflict="source,source_document_id",
        chunk_size=SIGNAL_EVENTS_UPSERT_CHUNK_SIZE,
    )

    raw_filings_upserted = 0
    raw_filings_upsert_status = "skipped"
    raw_filings_upsert_error: str | None = None

    if raw_filings:
        if ENABLE_CONGRESS_ASSET_NAME_PRESERVATION:
            preserve_existing_congress_asset_names(supabase, raw_filings)
        print(
            f"Upserting {len(raw_filings)} raw filings in chunks of {RAW_FILINGS_UPSERT_CHUNK_SIZE}...",
            flush=True,
        )
        try:
            chunked_upsert(
                supabase,
                "raw_filings",
                raw_filings,
                on_conflict="source,source_document_id",
                chunk_size=RAW_FILINGS_UPSERT_CHUNK_SIZE,
            )
            raw_filings_upserted = len(raw_filings)
            raw_filings_upsert_status = "success"
        except Exception as exc:
            raw_filings_upsert_error = str(exc)
            raw_filings_upsert_status = "failed"
            if not ALLOW_RAW_FILINGS_UPSERT_FAILURE:
                raise
            print(
                "Raw filing upsert failed after signal events were written; "
                f"continuing because signal_events are the critical path. Error: {exc}",
                flush=True,
            )

    summary = {
        "raw_filings_upserted": raw_filings_upserted,
        "raw_filings_upsert_status": raw_filings_upsert_status,
        "raw_filings_upsert_error": raw_filings_upsert_error,
        "signal_events_created": len(signal_events),
        "politician_rows_seen": len(pol_rows),
        "insider_rows_seen": len(insider_rows),
        "fund_rows_seen": len(fund_rows),
        "lookback_days": LOOKBACK_DAYS,
        "signal_events_upsert_chunk_size": SIGNAL_EVENTS_UPSERT_CHUNK_SIZE,
        "raw_filings_upsert_chunk_size": RAW_FILINGS_UPSERT_CHUNK_SIZE,
        "congress_asset_name_preservation_enabled": ENABLE_CONGRESS_ASSET_NAME_PRESERVATION,
        "signal_event_baseline_enrichment_enabled": ENABLE_SIGNAL_EVENT_BASELINE_ENRICHMENT,
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

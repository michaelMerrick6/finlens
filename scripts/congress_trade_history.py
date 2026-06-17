from __future__ import annotations

from typing import Iterable


def _clean_date(value: str | None) -> str:
    return str(value or "").strip()[:10]


def _event_reference_date(event: dict) -> str:
    payload = event.get("payload") or {}
    for value in (
        payload.get("trade_date"),
        payload.get("transaction_date"),
        event.get("occurred_at"),
        payload.get("published_date"),
        event.get("published_at"),
    ):
        cleaned = _clean_date(value)
        if cleaned:
            return cleaned
    return ""


def _row_reference_date(row: dict) -> str:
    for value in (row.get("transaction_date"), row.get("published_date")):
        cleaned = _clean_date(value)
        if cleaned:
            return cleaned
    return ""


def _event_actor_key(event: dict) -> str:
    payload = event.get("payload") or {}
    member_id = str(payload.get("member_id") or "").strip().lower()
    if member_id:
        return f"id:{member_id}"
    politician_name = str(payload.get("politician_name") or event.get("actor_name") or "").strip().lower()
    if politician_name:
        return f"name:{politician_name}"
    return ""


def _row_actor_key(row: dict) -> str:
    member_id = str(row.get("member_id") or "").strip().lower()
    if member_id:
        return f"id:{member_id}"
    politician_name = str(row.get("politician_name") or "").strip().lower()
    if politician_name:
        return f"name:{politician_name}"
    return ""


def is_raw_congress_buy_event(event: dict) -> bool:
    if str(event.get("source") or "").strip().lower() != "congress":
        return False
    if str(event.get("signal_type") or "").strip().lower() != "politician_trade":
        return False
    if str(event.get("direction") or "").strip().lower() != "buy":
        return False
    return bool(str(event.get("ticker") or "").strip().upper())


def annotate_events_with_congress_buy_history(events: list[dict], history_rows: Iterable[dict]) -> list[dict]:
    rows_by_ticker: dict[str, list[dict]] = {}
    for row in history_rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        rows_by_ticker.setdefault(ticker, []).append(row)

    enriched: list[dict] = []
    for event in events:
        if not is_raw_congress_buy_event(event):
            enriched.append(event)
            continue

        ticker = str(event.get("ticker") or "").strip().upper()
        reference_date = _event_reference_date(event)
        actor_key = _event_actor_key(event)
        if not ticker or not reference_date:
            enriched.append(event)
            continue

        prior_rows = []
        for row in rows_by_ticker.get(ticker, []):
            if str(row.get("transaction_type") or "").strip().lower() != "buy":
                continue
            row_date = _row_reference_date(row)
            if row_date and row_date < reference_date:
                prior_rows.append(row)

        prior_ticker_buy_count = len(prior_rows)
        prior_actor_ticker_buy_count = sum(1 for row in prior_rows if actor_key and _row_actor_key(row) == actor_key)

        payload = dict(event.get("payload") or {})
        payload["prior_congress_ticker_buy_count"] = prior_ticker_buy_count
        payload["prior_congress_actor_ticker_buy_count"] = prior_actor_ticker_buy_count
        payload["is_first_congress_ticker_buy"] = prior_ticker_buy_count == 0
        payload["is_first_congress_actor_ticker_buy"] = prior_actor_ticker_buy_count == 0

        enriched_event = dict(event)
        enriched_event["payload"] = payload
        enriched.append(enriched_event)

    return enriched


def fetch_congress_trade_history_rows(supabase, tickers: list[str]) -> list[dict]:
    rows: list[dict] = []
    clean_tickers = sorted({str(ticker or "").strip().upper() for ticker in tickers if str(ticker or "").strip()})
    for index in range(0, len(clean_tickers), 200):
        chunk = clean_tickers[index : index + 200]
        if not chunk:
            continue
        response = (
            supabase.table("politician_trades")
            .select("member_id,politician_name,ticker,transaction_type,transaction_date,published_date")
            .in_("ticker", chunk)
            .execute()
        )
        rows.extend(response.data or [])
    return rows


def enrich_events_with_congress_buy_history(events: list[dict], supabase) -> list[dict]:
    tickers = [str(event.get("ticker") or "").strip().upper() for event in events if is_raw_congress_buy_event(event)]
    if not tickers:
        return events
    history_rows = fetch_congress_trade_history_rows(supabase, tickers)
    return annotate_events_with_congress_buy_history(events, history_rows)

import os
import re
import xml.etree.ElementTree as ET

import requests


SEC_HEADERS = {
    "User-Agent": os.environ.get("SEC_USER_AGENT", "Vail/1.0 mikemerricka@gmail.com")
}
SEC_TIMEOUT_SECONDS = int(os.environ.get("SEC_DOC_TIMEOUT_SECONDS", "20"))

_FORM4_CACHE: dict[str, dict | None] = {}


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


def normalize_direction(code: str | None, acquired_disposed: str | None) -> str | None:
    normalized = str(code or "").strip().upper()
    if normalized == "P":
        return "buy"
    if normalized == "S":
        return "sell"
    acquired_disposed = str(acquired_disposed or "").strip().upper()
    if acquired_disposed == "A":
        return "buy"
    if acquired_disposed == "D":
        return "sell"
    return None


def canonical_form4_cache_key(source_url: str | None) -> str | None:
    accession = extract_sec_accession(source_url)
    if accession:
        return accession
    raw = str(source_url or "").strip()
    return raw or None


def event_source_url(event: dict) -> str | None:
    payload = event.get("payload") or {}
    return str(event.get("source_url") or payload.get("source_url") or "").strip() or None


def parse_form4_transactions(source_url: str | None) -> dict | None:
    cache_key = canonical_form4_cache_key(source_url)
    if not cache_key:
        return None
    if cache_key in _FORM4_CACHE:
        return _FORM4_CACHE[cache_key]

    try:
        response = requests.get(str(source_url), headers=SEC_HEADERS, timeout=SEC_TIMEOUT_SECONDS)
        response.raise_for_status()
        text = response.text
        xml_start = text.index("<XML>") + 5
        xml_end = text.index("</XML>")
        xml_body = text[xml_start:xml_end].strip()
        root = ET.fromstring(xml_body)
    except Exception:
        _FORM4_CACHE[cache_key] = None
        return None

    issuer_ticker = str(root.findtext(".//issuer/issuerTradingSymbol") or "").strip().upper()
    transactions: list[dict] = []
    for tx in root.findall(".//nonDerivativeTransaction"):
        code = tx.findtext(".//transactionCoding/transactionCode")
        acquired_disposed = tx.findtext(".//transactionAmounts/transactionAcquiredDisposedCode/value")
        direction = normalize_direction(code, acquired_disposed)
        if direction not in {"buy", "sell"}:
            continue
        try:
            shares = float(tx.findtext(".//transactionAmounts/transactionShares/value") or 0)
        except (TypeError, ValueError):
            shares = 0.0
        try:
            price = float(tx.findtext(".//transactionAmounts/transactionPricePerShare/value") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            following_shares = float(tx.findtext(".//postTransactionAmounts/sharesOwnedFollowingTransaction/value") or 0)
        except (TypeError, ValueError):
            following_shares = 0.0
        transactions.append(
            {
                "direction": direction,
                "transaction_date": str(tx.findtext(".//transactionDate/value") or "").strip()[:10],
                "shares": shares,
                "price": price,
                "value": shares * price,
                "following_shares": following_shares,
            }
        )

    result = {"ticker": issuer_ticker, "transactions": transactions}
    _FORM4_CACHE[cache_key] = result
    return result


def summarize_sell_transactions(transactions: list[dict]) -> dict | None:
    if not transactions:
        return None
    total_shares = sum(float(tx.get("shares") or 0) for tx in transactions)
    total_value = sum(float(tx.get("value") or 0) for tx in transactions)
    following_values = [
        float(tx.get("following_shares") or 0)
        for tx in transactions
        if tx.get("following_shares") is not None
    ]
    if total_shares <= 0 or not following_values:
        return None
    shares_after = min(following_values)
    shares_before = shares_after + total_shares
    if shares_before <= 0:
        return None
    reduction_pct = total_shares / shares_before
    return {
        "insider_total_sell_shares": round(total_shares, 4),
        "insider_total_sell_value": round(total_value, 2),
        "insider_shares_before_sale": round(shares_before, 4),
        "insider_shares_after_sale": round(shares_after, 4),
        "insider_holding_reduction_pct": round(reduction_pct, 6),
        "insider_sell_transaction_count": len(transactions),
    }


def matching_sell_transactions(event: dict, filing: dict) -> list[dict]:
    ticker = str(event.get("ticker") or "").strip().upper()
    if ticker and filing.get("ticker") and ticker != filing.get("ticker"):
        return []

    signal_type = str(event.get("signal_type") or "").strip().lower()
    occurred_at = str(event.get("occurred_at") or "").strip()[:10]
    payload = event.get("payload") or {}
    target_amount = float(payload.get("amount") or 0)

    transactions = [tx for tx in (filing.get("transactions") or []) if tx.get("direction") == "sell"]
    if signal_type == "insider_trade_grouped":
        if occurred_at:
            same_day = [tx for tx in transactions if tx.get("transaction_date") == occurred_at]
            if same_day:
                return same_day
        return transactions

    if occurred_at:
        transactions = [tx for tx in transactions if tx.get("transaction_date") == occurred_at] or transactions
    if target_amount > 0:
        exact = [tx for tx in transactions if abs(float(tx.get("shares") or 0) - target_amount) < 0.0001]
        if exact:
            return exact[:1]
    return transactions[:1]


def enrich_events_with_insider_sell_reductions(events: list[dict]) -> list[dict]:
    enriched: list[dict] = []
    for event in events:
        source = str(event.get("source") or "").strip().lower()
        direction = str(event.get("direction") or "").strip().lower()
        signal_type = str(event.get("signal_type") or "").strip().lower()
        if source != "insider" or direction != "sell" or signal_type not in {"insider_trade", "insider_trade_grouped"}:
            enriched.append(event)
            continue

        source_url = event_source_url(event)
        filing = parse_form4_transactions(source_url)
        if not filing:
            enriched.append(event)
            continue

        transactions = matching_sell_transactions(event, filing)
        metrics = summarize_sell_transactions(transactions)
        if not metrics:
            enriched.append(event)
            continue

        payload = dict(event.get("payload") or {})
        payload.update(metrics)
        payload["insider_sell_metrics_enriched"] = True

        enriched_event = dict(event)
        enriched_event["payload"] = payload
        if not enriched_event.get("source_url") and source_url:
            enriched_event["source_url"] = source_url
        enriched.append(enriched_event)

    return enriched

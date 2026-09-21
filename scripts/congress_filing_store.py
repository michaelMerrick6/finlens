"""Durable filing state and atomic publication. Requires phase14 migration."""
import re

from emit_signal_events import build_politician_events

PARSER_VERSION = "2026-09-16.1"


def filing_key(chamber, filing):
    if chamber == "House":
        return f"house-{filing['year']}-{filing['doc_id']}".lower()
    return f"senate-{filing['doc_key']}".lower()


def register_filings(client, chamber, filings):
    rows = [{"filing_id": filing_key(chamber, filing), "chamber": chamber,
             "filing": filing, "published_date": filing["published_date"]} for filing in filings]
    for offset in range(0, len(rows), 100):
        client.rpc("register_congress_filings", {"entries": rows[offset:offset + 100]}).execute()


def publish_filing(client, prefix, trades, *, filing=None, token=None, source_hash=None, verified_no_trades=False):
    """Never fall back to DELETE/INSERT over HTTP when the migration is missing."""
    prefix = prefix.lower()
    if not re.fullmatch(r"house-\d{4}-\d+|senate-[a-f0-9-]+", prefix):
        raise ValueError(f"Invalid official filing key: {prefix}")
    if not trades and not verified_no_trades:
        raise ValueError("Empty extraction is not a verified no-transactions filing")
    # Persist source-reviewed replacement relationships with the filing ledger.
    from reviewed_congress_filings import load_reviewed_filing
    reviewed = load_reviewed_filing(prefix)
    filing = dict(filing or {})
    if reviewed:
        for field in ('superseded_rows', 'replaces_rows', 'source_transaction_count'):
            if field in reviewed:
                filing[field] = reviewed[field]
    clean = []
    seen = set()
    for trade in trades:
        row = {key: value for key, value in trade.items() if not key.startswith("_") and key not in {"id", "created_at"}}
        row["doc_id"] = str(row["doc_id"]).lower()
        if not re.fullmatch(re.escape(prefix) + r"-\d+", row["doc_id"]) or row["doc_id"] in seen:
            raise ValueError("Duplicate or foreign transaction key; refusing publication")
        seen.add(row["doc_id"])
        if row["transaction_date"] > row["published_date"]:
            raise ValueError("Transaction date is after official filing date")
        clean.append(row)
    if reviewed and (reviewed.get('superseded_rows') or reviewed.get('replaces_rows')):
        from reviewed_congress_filings import reviewed_trades
        fields = ('doc_id', 'member_id', 'ticker', 'transaction_date', 'published_date',
                  'transaction_type', 'amount_range', 'asset_name', 'asset_type', 'source_url')
        signature = lambda row: tuple(row.get(field) for field in fields)
        if sorted(map(signature, clean)) != sorted(map(signature, reviewed_trades(prefix, reviewed))):
            raise ValueError('Publication does not match reviewed amendment rows')
    if token is None:
        metadata = dict(filing or {})
        published = (clean[0]["published_date"] if clean else metadata.get("published_date"))
        if not published and metadata.get("filing_date_raw"):
            from datetime import datetime
            published = datetime.strptime(metadata["filing_date_raw"], "%m/%d/%Y").date().isoformat()
        if not published:
            raise ValueError("Official filing date required for publication")
        client.rpc("register_congress_filings", {"entries": [{"filing_id": prefix,
            "chamber": "House" if prefix.startswith("house-") else "Senate",
            "published_date": published, "filing": metadata}]}).execute()
    raw, events = build_politician_events(clean)
    response = client.rpc("publish_congress_filing", {
        "target_filing_id": prefix, "trades": clean, "raw_rows": raw, "events": events,
        "claim_token": token, "parser_version": PARSER_VERSION,
        "source_hash": source_hash, "filing_metadata": filing or {}, "verified_no_trades": verified_no_trades,
    }).execute()
    result = response.data
    return int(result["previous_rows"]), int(result["stored_rows"])

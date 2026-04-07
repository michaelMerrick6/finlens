import argparse
import json

import requests
from bs4 import BeautifulSoup

from legacy_congress_guard import require_repair_write_opt_in
from ingest_senate_official import (
    SENATE_BASE_URL,
    SENATE_HOME_URL,
    SENATE_SEARCH_URL,
    CSRF_INPUT_RE,
    parse_filed_date,
    parse_senate_html_table,
    parse_senate_paper_report,
    prepare_senate_trades_for_insert,
    sanitize_politician_trade_for_insert,
    load_valid_tickers,
    resolve_member_id_from_full_name,
    supabase,
)
from politician_schema_support import politician_trades_has_asset_name_column


def create_senate_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )

    home = session.get(SENATE_HOME_URL, timeout=10)
    home.raise_for_status()
    match = CSRF_INPUT_RE.search(home.text)
    if not match:
        raise RuntimeError("Failed to find Senate CSRF token")
    csrf_token = match.group(1)

    response = session.post(
        SENATE_HOME_URL,
        data={"csrfmiddlewaretoken": csrf_token, "prohibition_agreement": "1"},
        headers={"Referer": SENATE_HOME_URL},
        timeout=10,
    )
    response.raise_for_status()

    cookie_csrf = session.cookies.get("csrftoken") or session.cookies.get("csrf")
    if not cookie_csrf:
        raise RuntimeError("Failed to obtain Senate csrf cookie")

    return session


def load_members_lookup() -> list[dict]:
    members_req = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute()
    return members_req.data if members_req else []


def load_existing_senate_filing(doc_key: str) -> dict:
    response = (
        supabase.table("politician_trades")
        .select("politician_name, source_url, published_date")
        .ilike("doc_id", f"senate-{doc_key}%")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise RuntimeError(f"Could not find existing Senate filing {doc_key} in politician_trades")

    row = rows[0]
    politician_name = row["politician_name"]
    if not politician_name or " " not in politician_name:
        raise RuntimeError(f"Could not resolve filer name for Senate filing {doc_key}")
    source_url = row["source_url"]
    if not source_url:
        raise RuntimeError(f"Could not resolve source_url for Senate filing {doc_key}")
    published_date = row.get("published_date")
    return {
        "politician_name": politician_name,
        "source_url": source_url,
        "published_date": published_date,
    }


def politician_trade_select_fields() -> str:
    fields = [
        "member_id",
        "politician_name",
        "chamber",
        "party",
        "ticker",
        "transaction_date",
        "published_date",
        "transaction_type",
        "asset_type",
        "amount_range",
        "source_url",
        "doc_id",
    ]
    if politician_trades_has_asset_name_column(supabase):
        fields.insert(5, "asset_name")
    return ",".join(fields)


def load_existing_senate_trade_rows(prefix: str) -> tuple[int, list[dict]]:
    supports_asset_name = politician_trades_has_asset_name_column(supabase)
    response = (
        supabase.table("politician_trades")
        .select(politician_trade_select_fields(), count="exact")
        .ilike("doc_id", f"{prefix}%")
        .limit(5000)
        .execute()
    )
    rows = [
        sanitize_politician_trade_for_insert(row, supports_asset_name=supports_asset_name)
        for row in (response.data or [])
    ]
    return response.count or 0, rows


def insert_politician_trade_rows(rows: list[dict]) -> int:
    inserted = 0
    row_failures: list[str] = []
    for index in range(0, len(rows), 50):
        chunk = rows[index : index + 50]
        try:
            supabase.table("politician_trades").insert(chunk).execute()
            inserted += len(chunk)
        except Exception as exc:
            for row in chunk:
                try:
                    supabase.table("politician_trades").insert(row).execute()
                    inserted += 1
                except Exception as inner_exc:
                    row_failures.append(f"{row.get('doc_id')}: {inner_exc}")
            if row_failures:
                break
            if not chunk:
                raise RuntimeError(f"Failed to insert politician trade chunk: {exc}") from exc

    if row_failures:
        preview = "; ".join(row_failures[:3])
        raise RuntimeError(f"Failed to insert {len(row_failures)} politician trade rows ({preview})")

    return inserted


def parse_senate_filing(
    session: requests.Session,
    doc_key: str,
    filing: dict,
    members_db: list[dict],
    valid_tickers: set[str],
) -> list[dict]:
    detail_url = filing["source_url"]
    detail_response = session.get(detail_url, headers={"Referer": SENATE_SEARCH_URL}, timeout=30)
    detail_response.raise_for_status()

    if "<title>eFD: Find Reports</title>" in detail_response.text:
        raise RuntimeError(f"Session redirect when fetching {detail_url}")

    soup = BeautifulSoup(detail_response.text, "html.parser")
    member_id = resolve_member_id_from_full_name(filing["politician_name"], members_db)
    filed_date = filing.get("published_date") or parse_filed_date("")
    first_name, last_name = filing["politician_name"].rsplit(" ", 1)

    if "/search/view/paper/" in detail_url:
        trades, _ = parse_senate_paper_report(
            session,
            soup,
            doc_key=doc_key,
            member_id=member_id,
            first_name=first_name,
            last_name=last_name,
            filed_date=filed_date,
            source_url=detail_url,
            valid_tickers=valid_tickers,
        )
    else:
        trades = parse_senate_html_table(
            soup,
            doc_key=doc_key,
            member_id=member_id,
            first_name=first_name,
            last_name=last_name,
            filed_date=filed_date,
            source_url=detail_url,
        )

    if not trades:
        raise RuntimeError(f"No Senate trades parsed for {doc_key}")
    return trades


def replace_senate_doc(
    session: requests.Session,
    doc_key: str,
    filing: dict,
    members_db: list[dict],
    valid_tickers: set[str],
) -> tuple[int, int]:
    trades = prepare_senate_trades_for_insert(parse_senate_filing(session, doc_key, filing, members_db, valid_tickers))
    if not trades:
        raise RuntimeError(f"No Senate trades parsed for {doc_key}; refusing to replace existing data")
    prefix = f"senate-{doc_key}"
    existing_count, existing_rows = load_existing_senate_trade_rows(prefix)

    if existing_count:
        supabase.table("politician_trades").delete().ilike("doc_id", f"{prefix}%").execute()

    try:
        inserted = insert_politician_trade_rows(trades)
    except Exception as exc:
        try:
            supabase.table("politician_trades").delete().ilike("doc_id", f"{prefix}%").execute()
        except Exception:
            pass

        if existing_rows:
            try:
                insert_politician_trade_rows(existing_rows)
            except Exception as restore_exc:
                raise RuntimeError(
                    f"Failed to replace Senate filing {doc_key}: {exc}; restore also failed: {restore_exc}"
                ) from exc

        raise RuntimeError(f"Failed to replace Senate filing {doc_key}: {exc}") from exc

    return existing_count, inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Reparse and replace specific Senate PTR filings.")
    parser.add_argument("targets", nargs="+", help="Senate filing UUIDs, with or without the senate- prefix")
    args = parser.parse_args()

    doc_keys = [target.removeprefix("senate-") for target in args.targets]
    session = create_senate_session()
    members_db = load_members_lookup()
    valid_tickers = load_valid_tickers()

    total_existing = 0
    total_inserted = 0
    for doc_key in doc_keys:
        filing = load_existing_senate_filing(doc_key)
        existing_count, inserted_count = replace_senate_doc(session, doc_key, filing, members_db, valid_tickers)
        total_existing += existing_count
        total_inserted += inserted_count
        print(f"Replaced senate:{doc_key} ({existing_count} old rows -> {inserted_count} reparsed rows)")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "targets": [f"senate:{doc_key}" for doc_key in doc_keys],
                "rows_replaced": total_existing,
                "rows_inserted": total_inserted,
            }
        )
    )


if __name__ == "__main__":
    require_repair_write_opt_in("repair_senate_filings.py")
    main()

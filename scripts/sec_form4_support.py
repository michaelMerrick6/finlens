import html
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import timedelta
from pathlib import Path

import requests

from time_utils import congress_today


SEC_HEADERS = {"User-Agent": os.environ.get("SEC_USER_AGENT", "Vail/1.0 mikemerricka@gmail.com")}
SEC_RSS_TIMEOUT_SECONDS = int(os.environ.get("SEC_RSS_TIMEOUT_SECONDS", "20"))
SEC_DOC_TIMEOUT_SECONDS = int(os.environ.get("SEC_DOC_TIMEOUT_SECONDS", "10"))
SEC_REQUEST_RETRIES = int(os.environ.get("SEC_REQUEST_RETRIES", "5"))
SEC_REQUEST_RETRY_SLEEP_SECONDS = float(os.environ.get("SEC_REQUEST_RETRY_SLEEP_SECONDS", "5"))
SEC_FORM4_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&owner=only&count=100&output=atom"
SEC_RECENT_FILINGS_CACHE_PATH = Path(os.environ.get("SEC_RECENT_FILINGS_CACHE_PATH", "/tmp/vail-sec-form4-recent-filings.json"))
SEC_DOC_CACHE_DIR = Path(os.environ.get("SEC_DOC_CACHE_DIR", "/tmp/vail-sec-form4-doc-cache"))
SEC_FEED_CACHE_MAX_AGE_SECONDS = int(os.environ.get("SEC_FEED_CACHE_MAX_AGE_SECONDS", "900"))


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(SEC_HEADERS)
    return session


def load_json_cache(path: Path, *, max_age_seconds: int) -> dict | None:
    try:
        if not path.exists():
            return None
        age_seconds = time.time() - path.stat().st_mtime
        if age_seconds > max_age_seconds:
            return None
        return json.loads(path.read_text())
    except Exception:
        return None


def write_json_cache(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True))
    except Exception:
        return


def fetch_with_retry(session: requests.Session, url: str, *, timeout: int, label: str) -> requests.Response:
    last_error = None
    for attempt in range(1, SEC_REQUEST_RETRIES + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else max(10.0, SEC_REQUEST_RETRY_SLEEP_SECONDS * (attempt + 1))
                except ValueError:
                    delay = max(10.0, SEC_REQUEST_RETRY_SLEEP_SECONDS * (attempt + 1))
                if attempt < SEC_REQUEST_RETRIES:
                    time.sleep(delay)
                    continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt < SEC_REQUEST_RETRIES:
                time.sleep(SEC_REQUEST_RETRY_SLEEP_SECONDS * attempt)
    raise last_error or RuntimeError(f"{label} failed without an exception")


def extract_sec_accession(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    dash_match = re.search(r"(\d{10}-\d{2}-\d{6})", raw)
    if dash_match:
        return dash_match.group(1)

    compact_match = re.search(r"/(\d{18})/", raw)
    if compact_match:
        compact = compact_match.group(1)
        return f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"

    return None


def accession_without_dashes(accession: str) -> str:
    return accession.replace("-", "")


def build_doc_url_from_entry_link(entry_link: str) -> str | None:
    accession = extract_sec_accession(entry_link)
    match = re.search(r"/data/(\d+)/", entry_link)
    if not accession or not match:
        return None
    cik = str(int(match.group(1)))
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_without_dashes(accession)}/{accession}.txt"


def canonical_form4_source_url(accession: str | None, issuer_cik: str | None, fallback_source_url: str | None = None) -> str:
    if accession:
        issuer_digits = re.sub(r"\D", "", str(issuer_cik or ""))
        if issuer_digits:
            return (
                f"https://www.sec.gov/Archives/edgar/data/{int(issuer_digits)}/"
                f"{accession_without_dashes(accession)}/{accession}.txt"
            )

    fallback = str(fallback_source_url or "").strip()
    if accession and fallback:
        match = re.search(r"/data/(\d+)/", fallback)
        if match:
            return (
                f"https://www.sec.gov/Archives/edgar/data/{int(match.group(1))}/"
                f"{accession_without_dashes(accession)}/{accession}.txt"
            )

    return fallback


def parse_feed_filed_date(summary_text: str | None, updated_at: str | None = None) -> str | None:
    summary = html.unescape(str(summary_text or ""))
    summary = re.sub(r"<[^>]+>", " ", summary)
    summary = re.sub(r"\s+", " ", summary).strip()
    match = re.search(r"Filed:\s*(\d{4}-\d{2}-\d{2})", summary)
    if match:
        return match.group(1)
    if updated_at:
        return str(updated_at).strip()[:10]
    return None


def role_from_relationship(relationship_node) -> str:
    if relationship_node is None:
        return "Insider"

    is_director = str(relationship_node.findtext("isDirector") or "").strip().lower() in {"1", "true"}
    is_officer = str(relationship_node.findtext("isOfficer") or "").strip().lower() in {"1", "true"}
    is_ten_percent = str(relationship_node.findtext("isTenPercentOwner") or "").strip().lower() in {"1", "true"}
    is_other = str(relationship_node.findtext("isOther") or "").strip().lower() in {"1", "true"}

    if is_officer:
        return str(relationship_node.findtext("officerTitle") or "Officer").strip() or "Officer"
    if is_director:
        return "Director"
    if is_ten_percent:
        return "10% Owner"
    if is_other:
        return str(relationship_node.findtext("otherText") or "Insider").strip() or "Insider"
    return "Insider"


def normalize_transaction_code(code: str | None, acquired_disposed: str | None = None) -> str | None:
    normalized = str(code or "").strip().upper()
    if normalized == "P":
        return "buy"
    if normalized == "S":
        return "sell"
    if normalized:
        return None

    acquired_disposed = str(acquired_disposed or "").strip().upper()
    if acquired_disposed == "A":
        return "buy"
    if acquired_disposed == "D":
        return "sell"
    return None


def parse_numeric(value: str | None) -> float:
    try:
        return float(str(value or "").strip())
    except (TypeError, ValueError):
        return 0.0


def normalize_numeric(value: float) -> int | float:
    return int(value) if float(value).is_integer() else round(value, 6)


def extract_ownership_xml(text: str) -> str | None:
    for match in re.finditer(r"<XML>(.*?)</XML>", text, flags=re.DOTALL | re.IGNORECASE):
        block = match.group(1).strip()
        if "ownershipDocument" in block:
            return block
    return None


def strip_default_namespace(xml_body: str) -> str:
    return re.sub(r'\sxmlns="[^"]+"', "", xml_body, count=1)


def recent_filings_cache_covers_request(payload: dict | None, *, days: int, limit: int, pages: int) -> bool:
    if not payload or not isinstance(payload.get("filings"), list):
        return False

    try:
        cached_days = int(payload.get("days") or 0)
        cached_limit = int(payload.get("limit") or 0)
        cached_pages = int(payload.get("pages") or 0)
    except (TypeError, ValueError):
        return False

    if cached_days < days or cached_limit < limit or cached_pages < pages:
        return False

    return True


def parse_form4_xml_text(text: str, *, fallback_source_url: str | None = None, filed_date: str | None = None) -> dict | None:
    xml_body = extract_ownership_xml(text)
    if not xml_body:
        return None

    try:
        root = ET.fromstring(strip_default_namespace(xml_body))
    except ET.ParseError:
        return None

    accession = extract_sec_accession(fallback_source_url)
    issuer_node = root.find(".//issuer")
    if issuer_node is None:
        return None

    issuer_cik = str(issuer_node.findtext("issuerCik") or "").strip()
    issuer_name = str(issuer_node.findtext("issuerName") or "").strip()
    ticker = str(issuer_node.findtext("issuerTradingSymbol") or "").strip().upper()
    if not ticker or ticker in {"NONE", "UNKNOWN"}:
        return None

    reporting_owner = root.find(".//reportingOwner")
    filer_name = str(reporting_owner.findtext(".//rptOwnerName") if reporting_owner is not None else "" or "").strip() or "Unknown"
    relationship = reporting_owner.find(".//reportingOwnerRelationship") if reporting_owner is not None else None
    filer_relation = role_from_relationship(relationship)

    canonical_source_url = canonical_form4_source_url(accession, issuer_cik, fallback_source_url)
    effective_filed_date = filed_date or str(root.findtext(".//periodOfReport") or "").strip()[:10] or None

    rows: list[dict] = []
    for index, tx in enumerate(root.findall(".//nonDerivativeTransaction")):
        direction = normalize_transaction_code(
            tx.findtext(".//transactionCoding/transactionCode"),
            tx.findtext(".//transactionAmounts/transactionAcquiredDisposedCode/value"),
        )
        if direction not in {"buy", "sell"}:
            continue

        transaction_date = str(tx.findtext(".//transactionDate/value") or "").strip()[:10]
        if not transaction_date:
            continue

        shares = parse_numeric(tx.findtext(".//transactionAmounts/transactionShares/value"))
        price = parse_numeric(tx.findtext(".//transactionAmounts/transactionPricePerShare/value"))

        rows.append(
            {
                "ticker": ticker[:10],
                "filer_name": filer_name[:255],
                "filer_relation": filer_relation[:255],
                "transaction_date": transaction_date,
                "published_date": effective_filed_date or transaction_date,
                "transaction_code": direction,
                "amount": normalize_numeric(shares),
                "price": round(price, 6),
                "value": round(shares * price, 2),
                "source_url": f"{canonical_source_url}#tx-{index}"[:500],
                "_row_key": f"{accession or canonical_source_url}:{index}",
                "_company_name": issuer_name or ticker,
                "_accession": accession,
            }
        )

    return {
        "accession": accession,
        "issuer_cik": issuer_cik,
        "ticker": ticker,
        "company_name": issuer_name or ticker,
        "filer_name": filer_name,
        "filed_date": effective_filed_date,
        "source_url": canonical_source_url,
        "rows": rows,
    }


def parse_form4_filing(session: requests.Session, source_url: str, *, filed_date: str | None = None) -> dict | None:
    accession = extract_sec_accession(source_url)
    cached_text = None
    if accession:
        cache_path = SEC_DOC_CACHE_DIR / f"{accession}.txt"
        try:
            if cache_path.exists():
                cached_text = cache_path.read_text()
        except Exception:
            cached_text = None

    if cached_text is None:
        response = fetch_with_retry(session, source_url, timeout=SEC_DOC_TIMEOUT_SECONDS, label=f"SEC Form 4 document {source_url}")
        cached_text = response.text
        if accession:
            try:
                SEC_DOC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                (SEC_DOC_CACHE_DIR / f"{accession}.txt").write_text(cached_text)
            except Exception:
                pass

    return parse_form4_xml_text(cached_text, fallback_source_url=source_url, filed_date=filed_date)


def load_recent_form4_filings(
    session: requests.Session,
    *,
    days: int,
    limit: int,
    pages: int,
    use_cache: bool = True,
) -> list[dict]:
    cached_payload = load_json_cache(SEC_RECENT_FILINGS_CACHE_PATH, max_age_seconds=SEC_FEED_CACHE_MAX_AGE_SECONDS) if use_cache else None
    if recent_filings_cache_covers_request(cached_payload, days=days, limit=limit, pages=pages):
        return list(cached_payload["filings"])[:limit]

    cutoff = congress_today() - timedelta(days=days)
    seen_accessions: set[str] = set()
    filings: list[dict] = []

    for page in range(pages):
        response = fetch_with_retry(
            session,
            f"{SEC_FORM4_RSS_URL}&start={page * 100}",
            timeout=SEC_RSS_TIMEOUT_SECONDS,
            label=f"SEC RSS page {page}",
        )
        root = ET.fromstring(response.text)
        namespace = {"atom": "http://www.w3.org/2005/Atom"}
        entries = root.findall("atom:entry", namespace)
        if not entries:
            break

        page_has_recent = False
        for entry in entries:
            title = str(entry.findtext("atom:title", default="", namespaces=namespace) or "")
            if not title.startswith("4 - "):
                continue

            entry_link = entry.find("atom:link", namespace)
            raw_href = entry_link.get("href") if entry_link is not None else None
            raw_source_url = build_doc_url_from_entry_link(str(raw_href or ""))
            accession = extract_sec_accession(raw_href) or extract_sec_accession(entry.findtext("atom:id", default="", namespaces=namespace))
            if not accession or not raw_source_url or accession in seen_accessions:
                continue

            updated_at = str(entry.findtext("atom:updated", default="", namespaces=namespace) or "").strip()
            filed_date = parse_feed_filed_date(
                entry.findtext("atom:summary", default="", namespaces=namespace),
                updated_at=updated_at,
            )
            if filed_date and filed_date >= cutoff.isoformat():
                page_has_recent = True
            elif filed_date and filed_date < cutoff.isoformat():
                continue

            seen_accessions.add(accession)
            filings.append(
                {
                    "accession": accession,
                    "filed_date": filed_date,
                    "updated_at": updated_at,
                    "source_url": raw_source_url,
                }
            )
            if len(filings) >= limit:
                write_json_cache(
                    SEC_RECENT_FILINGS_CACHE_PATH,
                    {
                        "days": days,
                        "limit": limit,
                        "pages": pages,
                        "filings": filings,
                    },
                )
                return filings

        if not page_has_recent:
            break

    write_json_cache(
        SEC_RECENT_FILINGS_CACHE_PATH,
        {
            "days": days,
            "limit": limit,
            "pages": pages,
            "filings": filings,
        },
    )
    return filings


def recent_trade_key(row: dict) -> tuple[str, str, str, str, str, str]:
    return (
        str(extract_sec_accession(row.get("source_url")) or row.get("source_url") or "").strip(),
        str(row.get("filer_name") or "").strip().upper(),
        str(row.get("ticker") or "").strip().upper(),
        str(row.get("transaction_date") or "").strip(),
        str(row.get("transaction_code") or "").strip().lower(),
        str(normalize_numeric(parse_numeric(str(row.get("amount") or 0)))),
    )

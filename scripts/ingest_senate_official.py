from congress_member_lookup import load_congress_members
import io
import json
from parser_write_policy import parser_writes_allowed
from reviewed_congress_filings import reviewed_senate_trades
import os
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image
from supabase import Client, create_client
from politician_schema_support import politician_trades_has_asset_name_column
from politician_trade_option_support import normalize_politician_asset_type
from time_utils import congress_now

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

SENATE_BASE_URL = "https://efdsearch.senate.gov"
SENATE_HOME_URL = f"{SENATE_BASE_URL}/search/home/"
SENATE_SEARCH_URL = f"{SENATE_BASE_URL}/search/"
SENATE_REPORT_DATA_URL = f"{SENATE_BASE_URL}/search/report/data/"

CSRF_INPUT_RE = re.compile(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"')
DOCUMENT_HREF_RE = re.compile(r'href="(.*?)"')
DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b")
TICKER_RE = re.compile(r"\(([A-Z]{1,6})\)")
COMMON_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
PRIVATE_ENTITY_MARKERS = (" LLC", " L.L.C", " L.P.", " LP", " PARTNERS", " FAMILY", " TRUST")
PUBLIC_COMPANY_HINTS = (" STOCK", " SHARES", " COMMON", " ETF", " ETN", " ADR", " ADS", " INC", " CORP", " PLC")
FIRST_NAME_ALIAS_GROUPS = (
    {"bill", "billy", "will", "william"},
    {"dan", "daniel", "danny"},
    {"dave", "david"},
    {"jim", "jimmy", "james"},
    {"rick", "richard"},
    {"ted", "rafael"},
    {"tom", "tommy", "thomas"},
)
FIRST_NAME_ALIAS_MAP = {
    token: group for group in FIRST_NAME_ALIAS_GROUPS for token in group
}
SENATE_DAILY_LOOKBACK_DAYS = int(os.environ.get("SENATE_DAILY_LOOKBACK_DAYS", "30"))
SENATE_DAILY_EXISTING_STOP = int(os.environ.get("SENATE_DAILY_EXISTING_STOP", "0"))
POLITICIAN_TRADE_INSERT_FIELDS = (
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
    "asset_name",
)


def normalize_name_part(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", normalized.lower())


def normalize_name_tokens(value: str) -> list[str]:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return [token for token in re.findall(r"[a-z]+", normalized.lower()) if token not in COMMON_NAME_SUFFIXES]


def alias_tokens(token: str) -> set[str]:
    return FIRST_NAME_ALIAS_MAP.get(token, {token})


def first_name_tokens_match(first_tokens: list[str], member_first_name: str) -> bool:
    member_tokens = normalize_name_tokens(member_first_name)
    if not first_tokens or not member_tokens:
        return False

    for filed_token in first_tokens:
        for member_token in member_tokens:
            if filed_token == member_token:
                return True
            if alias_tokens(filed_token) & alias_tokens(member_token):
                return True
            if len(filed_token) == 1 and member_token.startswith(filed_token):
                return True
            if len(member_token) == 1 and filed_token.startswith(member_token):
                return True
            if min(len(filed_token), len(member_token)) >= 4 and (
                filed_token.startswith(member_token) or member_token.startswith(filed_token)
            ):
                return True
    return False


def member_matches_chamber(member: dict, target_chamber: str) -> bool:
    chamber = (member.get("chamber") or "").strip().lower()
    if not chamber or chamber == "both":
        return True
    return chamber == target_chamber.lower()


def is_placeholder_member(member: dict) -> bool:
    return str(member.get("id") or "").startswith("unknown-")


def clean_text(value: str) -> str:
    value = value.replace("\x00", " ")
    value = value.replace("\u00a0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_ocr_date(raw_value: str) -> str | None:
    raw_value = raw_value.replace("o", "0").replace("O", "0").replace("l", "1").replace("I", "1")
    parts = re.findall(r"\d+", raw_value)
    if len(parts) < 3:
        return None
    month, day, year = parts[0], parts[1], parts[2]
    if len(year) == 2:
        year = "20" + year
    try:
        return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def resolve_member_id(first_name: str, last_name: str, members_db: list[dict], target_chamber: str = "Senate") -> str:
    first_tokens = normalize_name_tokens(first_name)
    last_key = "".join(normalize_name_tokens(last_name))
    matching_ids: set[str] = set()

    for member in members_db:
        if is_placeholder_member(member):
            continue
        if not member_matches_chamber(member, target_chamber):
            continue
        member_last_key = "".join(normalize_name_tokens(member["last_name"]))
        if not member_last_key or member_last_key != last_key:
            continue
        if first_name_tokens_match(first_tokens, member["first_name"]):
            matching_ids.add(member["id"])

    # A surname or active flag alone cannot establish the filing's identity.
    # Multiple compatible names need source evidence, not database row order.
    if len(matching_ids) == 1:
        return next(iter(matching_ids))

    first_norm = normalize_name_part(first_name)
    last_norm = normalize_name_part(last_name)
    member_id = f"unknown-{first_norm}-{last_norm}"[:50]
    if any(member["id"] == member_id for member in members_db):
        return member_id

    if not parser_writes_allowed():
        return member_id

    try:
        supabase.table("congress_members").upsert(
            {
                "id": member_id,
                "first_name": first_name,
                "last_name": last_name,
                "chamber": target_chamber,
            }
        ).execute()
        members_db.append(
            {"id": member_id, "first_name": first_name, "last_name": last_name, "chamber": target_chamber}
        )
    except Exception as exc:
        print(f"Warning: failed to upsert placeholder member {member_id}: {exc}")
    return member_id


def resolve_member_id_from_full_name(full_name: str, members_db: list[dict], target_chamber: str = "Senate") -> str:
    tokens = normalize_name_tokens(full_name)
    if len(tokens) < 2:
        return resolve_member_id(full_name, "", members_db, target_chamber=target_chamber)

    for member in members_db:
        if is_placeholder_member(member):
            continue
        if not member_matches_chamber(member, target_chamber):
            continue
        member_last_tokens = normalize_name_tokens(member["last_name"])
        if not member_last_tokens:
            continue
        if len(tokens) <= len(member_last_tokens):
            continue
        if tokens[-len(member_last_tokens) :] != member_last_tokens:
            continue
        if first_name_tokens_match(tokens[: -len(member_last_tokens)], member["first_name"]):
            return member["id"]

    return resolve_member_id(" ".join(tokens[:-1]), tokens[-1], members_db, target_chamber=target_chamber)


def load_valid_tickers() -> set[str]:
    tickers: set[str] = set()
    offset = 0
    while True:
        response = supabase.table("companies").select("ticker").range(offset, offset + 999).execute()
        rows = response.data or []
        if not rows:
            break
        tickers.update((row.get("ticker") or "").upper() for row in rows if row.get("ticker"))
        if len(rows) < 1000:
            break
        offset += 1000
    tickers.update({"US-TREAS", "N/A"})
    return tickers


def resolve_company_ticker(asset_text: str, valid_tickers: set[str]) -> str | None:
    asset_text = clean_paper_asset_text(asset_text)
    ticker_match = TICKER_RE.search(asset_text)
    if ticker_match:
        return ticker_match.group(1)[:10]

    asset_upper = asset_text.upper()
    if "UNITED STATES TREAS" in asset_upper or "U.S. TREASURY" in asset_upper:
        return "US-TREAS"

    is_private_entity = any(marker in f" {asset_upper}" for marker in PRIVATE_ENTITY_MARKERS)
    has_public_company_hint = any(hint in f" {asset_upper}" for hint in PUBLIC_COMPANY_HINTS)

    if not is_private_entity:
        for token in re.findall(r"\b[A-Z]{1,5}\b", asset_upper):
            if token in valid_tickers and token not in {"NYSE", "OTC", "LLC", "LP", "INC", "CORP", "LTD"}:
                return token[:10]

    if is_private_entity and not has_public_company_hint:
        return None

    lookup_candidates = [clean_text(asset_text)]
    stripped_candidate = clean_text(re.sub(r"\([^)]*\)", "", asset_text))
    if stripped_candidate and stripped_candidate not in lookup_candidates:
        lookup_candidates.append(stripped_candidate)

    for candidate in lookup_candidates:
        if not candidate:
            continue
        try:
            result = (
                supabase.table("companies")
                .select("ticker")
                .ilike("name", f"{candidate[:80]}%")
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["ticker"]
        except Exception:
            continue
    return None


def upsert_company(ticker: str, company_name: str):
    if not parser_writes_allowed():
        return
    try:
        supabase.table("companies").upsert(
            {
                "ticker": ticker[:10],
                "name": company_name[:255],
                "sector": "Unknown",
                "industry": "Unknown",
            }
        ).execute()
    except Exception as exc:
        print(f"Warning: company upsert failed for {ticker}: {exc}")


class PaperFilingReviewRequired(ValueError):
    """The scan cannot safely be published without source review."""


def validate_paper_extraction(trades: list[dict], expected_rows: int, filed_date: str) -> None:
    if not expected_rows or len(trades) != expected_rows:
        raise PaperFilingReviewRequired(
            f"Scan requires review: detected {expected_rows} table rows, parsed {len(trades)}")
    for index, trade in enumerate(trades, 1):
        if (trade.get("transaction_type") not in {"buy", "sell", "exchange"}
                or trade.get("amount_range") in {None, "", "Unknown"}
                or not trade.get("transaction_date")
                or trade["transaction_date"] > filed_date):
            raise PaperFilingReviewRequired(f"Scan requires review: ambiguous financial fields in row {index}")


def parse_filed_date(value: str) -> str:
    try:
        return datetime.strptime(str(value).strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        return congress_now().strftime("%Y-%m-%d")


def build_trade_record(
    *,
    doc_key: str,
    trade_index: int,
    member_id: str,
    first_name: str,
    last_name: str,
    chamber: str,
    ticker: str,
    transaction_date: str,
    published_date: str,
    transaction_type: str,
    amount_range: str,
    source_url: str,
    asset_name: str = "",
    asset_type: str = "Stock",
) -> dict:
    normalized_asset_type = normalize_politician_asset_type(asset_type, asset_name)
    return {
        "member_id": member_id,
        "politician_name": f"{first_name} {last_name}"[:100],
        "chamber": chamber,
        "party": "Unknown",
        "ticker": ticker[:10],
        "transaction_date": transaction_date,
        "published_date": published_date,
        "transaction_type": transaction_type[:10],
        "asset_type": normalized_asset_type,
        "amount_range": amount_range[:255],
        "source_url": source_url[:500],
        "doc_id": f"senate-{doc_key}-{trade_index}",
        "asset_name": asset_name[:255] if asset_name else "",
        "_asset_name": asset_name[:255] if asset_name else "",
    }


def sanitize_politician_trade_for_insert(trade: dict, *, supports_asset_name: bool) -> dict:
    prepared_trade = {key: trade.get(key) for key in POLITICIAN_TRADE_INSERT_FIELDS if key in trade}
    if not supports_asset_name:
        prepared_trade.pop("asset_name", None)
    elif not str(prepared_trade.get("asset_name") or "").strip():
        fallback_asset_name = str(trade.get("_asset_name") or "").strip()
        if fallback_asset_name:
            prepared_trade["asset_name"] = fallback_asset_name[:255]
    return prepared_trade


def prepare_senate_trades_for_insert(trades: list[dict]) -> list[dict]:
    supports_asset_name = politician_trades_has_asset_name_column(supabase)
    return [sanitize_politician_trade_for_insert(trade, supports_asset_name=supports_asset_name) for trade in trades]


def parse_senate_html_table(
    soup: BeautifulSoup,
    *,
    doc_key: str,
    member_id: str,
    first_name: str,
    last_name: str,
    filed_date: str,
    source_url: str,
) -> list[dict]:
    table = soup.select_one("table.table")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []

    trades: list[dict] = []
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        tx_date = normalize_ocr_date(cells[1].get_text(" ", strip=True))
        if not tx_date:
            continue

        ticker_text = clean_text(cells[3].get_text(" ", strip=True))
        issuer_text = clean_text(cells[4].get_text(" ", strip=True))
        tx_type_raw = clean_text(cells[6].get_text(" ", strip=True)).lower()
        amount_raw = clean_text(cells[7].get_text(" ", strip=True)) or "Unknown"

        ticker = ticker_text if ticker_text and ticker_text != "--" else "N/A"
        if ticker != "N/A":
            upsert_company(ticker, issuer_text or ticker)

        if "sale" in tx_type_raw:
            tx_type = "sell"
        elif "purchase" in tx_type_raw:
            tx_type = "buy"
        else:
            tx_type = "exchange"

        trades.append(
            build_trade_record(
                doc_key=doc_key,
                trade_index=len(trades),
                member_id=member_id,
                first_name=first_name,
                last_name=last_name,
                chamber="Senate",
                ticker=ticker,
                transaction_date=tx_date,
                published_date=filed_date,
                transaction_type=tx_type,
                amount_range=amount_raw,
                source_url=source_url,
                asset_name=issuer_text or ticker_text or ticker,
                asset_type=clean_text(cells[5].get_text(" ", strip=True)) or "Stock",
            )
        )
    return trades


def extract_paper_image_urls(soup: BeautifulSoup) -> list[str]:
    image_urls: list[str] = []
    for image in soup.select("img.filingImage"):
        src = image.get("src")
        if not src:
            continue
        image_urls.append(urljoin(SENATE_BASE_URL, src))
    return image_urls


def load_paper_images(session: requests.Session, image_urls: list[str]) -> list[Image.Image]:
    images: list[Image.Image] = []
    for image_url in image_urls:
        response = session.get(image_url, timeout=30)
        response.raise_for_status()
        images.append(Image.open(io.BytesIO(response.content)).convert("RGB"))
    return images


def clean_paper_asset_text(asset_text: str) -> str:
    asset_text = re.sub(r"^(?:[=»]\s*)*(?:[A-Za-z]\s+)?\((?:S|J|D|DC|SP|JT|PE)\)\s*", "", asset_text, flags=re.IGNORECASE)
    asset_text = re.sub(r"\s+x+\s*$", "", asset_text, flags=re.IGNORECASE)
    return clean_text(asset_text.rstrip(":"))


def parse_senate_paper_report(
    session: requests.Session,
    soup: BeautifulSoup,
    *,
    doc_key: str,
    member_id: str,
    first_name: str,
    last_name: str,
    filed_date: str,
    source_url: str,
    valid_tickers: set[str],
) -> tuple[list[dict], int]:
    image_urls = extract_paper_image_urls(soup)
    if not image_urls:
        return [], 0

    try:
        images = load_paper_images(session, image_urls)
    except Exception as exc:
        print(f"Failed to OCR Senate paper filing {doc_key}: {exc}")
        return [], 0

    reviewed = reviewed_senate_trades(f"senate-{doc_key}", images, member_id, filed_date)
    if reviewed is not None:
        return reviewed, len(reviewed)

    from senate_table_ocr import extract_document, TableReviewRequired
    try:
        extracted = extract_document(images, filed_date=filed_date)
    except (TableReviewRequired, RuntimeError) as exc:
        raise PaperFilingReviewRequired(f"{doc_key}: {exc}") from exc
    trades = []
    for row in extracted:
        asset = clean_paper_asset_text(row['asset_name'])
        ticker = resolve_company_ticker(asset, valid_tickers) or "N/A"
        trade = build_trade_record(
            doc_key=doc_key, trade_index=len(trades), member_id=member_id,
            first_name=first_name, last_name=last_name, chamber="Senate",
            ticker=ticker, transaction_date=row['transaction_date'],
            published_date=filed_date, transaction_type=row['transaction_type'],
            amount_range=row['amount_range'], source_url=source_url, asset_name=asset,
            asset_type="Stock" if re.search(r"\bstock\b", asset, re.I) else "Other",
        )
        trades.append(trade)
    validate_paper_extraction(trades, len(extracted), filed_date)
    for trade in trades:
        if trade["ticker"] != "N/A":
            upsert_company(trade["ticker"], trade.get("asset_name") or trade.get("_asset_name") or trade["ticker"])
    return trades, len(extracted)


def establish_senate_session(session: requests.Session) -> str:
    """Perform the CSRF handshake and return the cookie CSRF token."""
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
    return cookie_csrf


def fetch_senate_trades():
    print("Starting Official Senate eFD Scraper...")
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )

    print("1. Bypassing Senate eFD Terms of Service Gateway...")
    cookie_csrf = establish_senate_session(session)

    print("2. Fetching historical PTR reports via pagination...")
    daily_mode = os.environ.get("FINLENS_DAILY_MODE", "0") == "1"
    max_pagination = 500 if daily_mode else 10000

    submitted_start_date = "01/01/2012 00:00:00"
    if daily_mode:
        submitted_start_date = (congress_now() - timedelta(days=SENATE_DAILY_LOOKBACK_DAYS)).strftime("%m/%d/%Y 00:00:00")

    all_rows = []
    for start_offset in range(0, max_pagination, 100):
        print(f" -> Fetching offset {start_offset}...")
        payload = {
            "start": str(start_offset),
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": submitted_start_date,
            "submitted_end_date": "",
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
            "csrfmiddlewaretoken": cookie_csrf,
        }
        try:
            search_response = session.post(
                SENATE_REPORT_DATA_URL,
                data=payload,
                headers={"Referer": SENATE_SEARCH_URL},
                timeout=30,
            )
            search_response.raise_for_status()
            data = search_response.json()
        except Exception as exc:
            if start_offset == 0:
                raise RuntimeError(f"Failed to fetch Senate PTR feed: {exc}") from exc
            print(f"Pagination error at offset {start_offset}: {exc}")
            break

        chunk_rows = data.get("data", [])
        if not chunk_rows:
            break
        all_rows.extend(chunk_rows)
        time.sleep(1)

    print(f"Found {len(all_rows)} Senate PTR filings across pagination.")

    members_db = load_congress_members(supabase)

    valid_tickers = load_valid_tickers()
    formatted_trades: list[dict] = []
    inserted_count = 0
    paper_filings_seen = 0
    paper_transaction_rows_seen = 0
    paper_trades_parsed = 0
    paper_unmapped_filings = 0
    parse_failures = 0
    failed_doc_ids: list[str] = []
    existing_filings_seen = 0
    write_failures = 0
    prepared_trades: list[dict] = []

    consecutive_existing = 0
    for row in all_rows:
        first_name = clean_text(str(row[0]))
        last_name = clean_text(str(row[1]))
        link_str = str(row[3])
        href_match = DOCUMENT_HREF_RE.search(link_str)
        if not href_match:
            continue
        detail_path = href_match.group(1)
        doc_key = detail_path.rstrip("/").split("/")[-1]

        member_id = resolve_member_id(first_name, last_name, members_db)
        filed_date = parse_filed_date(row[4])
        anchor_doc_id = f"senate-{doc_key}-0"

        check = supabase.table("politician_trades").select("id").eq("doc_id", anchor_doc_id).limit(1).execute()
        if check.data:
            existing_filings_seen += 1
            if SENATE_DAILY_EXISTING_STOP > 0:
                consecutive_existing += 1
            if daily_mode and SENATE_DAILY_EXISTING_STOP > 0 and consecutive_existing >= SENATE_DAILY_EXISTING_STOP:
                print(f" -> Hit {SENATE_DAILY_EXISTING_STOP} consecutive existing Senate filings. Stopping.")
                break
            continue

        consecutive_existing = 0
        detail_url = f"{SENATE_BASE_URL}{detail_path}"
        print(f"Scraping eFD for {first_name} {last_name} ({detail_path})...")

        try:
            detail_response = session.get(detail_url, headers={"Referer": SENATE_SEARCH_URL}, timeout=30)
            detail_response.raise_for_status()
        except Exception as exc:
            parse_failures += 1
            failed_doc_ids.append(doc_key)
            print(f"Failed to fetch {detail_url}: {exc}")
            continue

        if "<title>eFD: Find Reports</title>" in detail_response.text:
            # Session expired — refresh and retry once
            print(f"Session redirect detected for {detail_url}. Refreshing session...")
            try:
                cookie_csrf = establish_senate_session(session)
                detail_response = session.get(detail_url, headers={"Referer": SENATE_SEARCH_URL}, timeout=30)
                detail_response.raise_for_status()
            except Exception as refresh_exc:
                parse_failures += 1
                failed_doc_ids.append(doc_key)
                print(f"Session refresh failed for {detail_url}: {refresh_exc}")
                continue

            if "<title>eFD: Find Reports</title>" in detail_response.text:
                parse_failures += 1
                failed_doc_ids.append(doc_key)
                print(f"Session redirect persists after refresh for {detail_url}")
                continue

        soup = BeautifulSoup(detail_response.text, "html.parser")
        paper_row_count = 0
        if "/search/view/paper/" in detail_path:
            paper_filings_seen += 1
            try:
                trades, paper_row_count = parse_senate_paper_report(
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
            except PaperFilingReviewRequired as exc:
                parse_failures += 1
                failed_doc_ids.append(doc_key)
                print(f" -> {doc_key}: {exc}")
                continue
            paper_transaction_rows_seen += paper_row_count
            paper_trades_parsed += len(trades)
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
            if "/search/view/paper/" in detail_path and paper_row_count > 0:
                paper_unmapped_filings += 1
                print(f" -> Paper filing contained {paper_row_count} transaction rows but no public ticker matches")
            else:
                parse_failures += 1
                failed_doc_ids.append(doc_key)
                print(" -> No Senate trades parsed from filing")
        else:
            formatted_trades.extend(trades)
            print(f" -> Extracted {len(trades)} Senate trades")

        time.sleep(0.5)

    print(f"Parsed {len(formatted_trades)} detailed Senate trades.")

    if formatted_trades:
        prepared_trades = prepare_senate_trades_for_insert(formatted_trades)
        print(f"Uploading {len(prepared_trades)} real Senate trades to Supabase...")
        for index in range(0, len(prepared_trades), 50):
            chunk = prepared_trades[index : index + 50]
            to_insert: list[dict] = []
            try:
                doc_ids = [trade["doc_id"] for trade in chunk]
                existing = supabase.table("politician_trades").select("doc_id").in_("doc_id", doc_ids).execute()
                existing_ids = {row["doc_id"] for row in existing.data}

                to_insert = [trade for trade in chunk if trade["doc_id"] not in existing_ids]
                if to_insert:
                    supabase.table("politician_trades").insert(to_insert).execute()
                    inserted_count += len(to_insert)
                    print(f" -> Inserted {len(to_insert)} new Senate trades.")
            except Exception as exc:
                print(f"Error manual-upserting chunk: {exc}")
                for trade in to_insert:
                    try:
                        supabase.table("politician_trades").insert(trade).execute()
                        inserted_count += 1
                    except Exception as inner_exc:
                        write_failures += 1
                        failed_doc_id = str(trade.get("doc_id") or "").rsplit("-", 1)[0].removeprefix("senate-")
                        if failed_doc_id:
                            failed_doc_ids.append(failed_doc_id)
                        print(f" -> Failed to insert Senate trade {trade.get('doc_id')}: {inner_exc}")

        print("Successfully seeded SENATE trades!")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "failed_doc_ids": failed_doc_ids[:20],
                "paper_filings_seen": paper_filings_seen,
                "paper_transaction_rows_seen": paper_transaction_rows_seen,
                "paper_trades_parsed": paper_trades_parsed,
                "paper_unmapped_filings": paper_unmapped_filings,
                "existing_filings_seen": existing_filings_seen,
                "write_failures": write_failures,
                "parse_failures": parse_failures + write_failures,
                "records_inserted": inserted_count,
                "records_seen": len(prepared_trades),
                "records_skipped": max(len(prepared_trades) - inserted_count, 0),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    fetch_senate_trades()

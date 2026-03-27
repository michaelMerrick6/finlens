import io
import json
import os
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin

import pytesseract
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image
from supabase import Client, create_client
from politician_schema_support import politician_trades_has_asset_name_column
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
PAPER_MARK_RE = re.compile(r"^[xX×]{1,3}$")
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
    exact_last_name_candidates: list[dict] = []

    for member in members_db:
        if is_placeholder_member(member):
            continue
        if not member_matches_chamber(member, target_chamber):
            continue
        member_last_key = "".join(normalize_name_tokens(member["last_name"]))
        if not member_last_key or member_last_key != last_key:
            continue
        exact_last_name_candidates.append(member)
        if first_name_tokens_match(first_tokens, member["first_name"]):
            return member["id"]

    active_candidates = [member for member in exact_last_name_candidates if member.get("active") is not False]
    if len(active_candidates) == 1:
        return active_candidates[0]["id"]
    if len(exact_last_name_candidates) == 1:
        return exact_last_name_candidates[0]["id"]

    first_norm = normalize_name_part(first_name)
    last_norm = normalize_name_part(last_name)
    member_id = f"unknown-{first_norm}-{last_norm}"[:50]
    if any(member["id"] == member_id for member in members_db):
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


def infer_paper_transaction_type(type_marks: list[dict], line_text: str) -> str:
    line_lower = line_text.lower()
    if "sale" in line_lower or "sold" in line_lower:
        return "sell"
    if "purchase" in line_lower or "buy" in line_lower:
        return "buy"
    if not type_marks:
        return "exchange"

    left = min(mark["left"] for mark in type_marks)
    if left < 1475:
        return "buy"
    if left < 1600:
        return "sell"
    return "exchange"


def infer_paper_amount(amount_marks: list[dict]) -> str:
    if not amount_marks:
        return "Unknown"

    left = min(mark["left"] for mark in amount_marks)
    if left < 2050:
        return "$1,001 - $15,000"
    if left < 2200:
        return "$15,001 - $50,000"
    if left < 2360:
        return "$50,001 - $100,000"
    if left < 2500:
        return "$100,001 - $250,000"
    if left < 2620:
        return "$250,001 - $500,000"
    if left < 2725:
        return "$500,001 - $1,000,000"
    if left < 2835:
        return "$1,000,001 - $5,000,000"
    return "Over $5,000,000"


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
) -> dict:
    return {
        "member_id": member_id,
        "politician_name": f"{first_name} {last_name}"[:100],
        "chamber": chamber,
        "party": "Unknown",
        "ticker": ticker[:10],
        "transaction_date": transaction_date,
        "published_date": published_date,
        "transaction_type": transaction_type[:10],
        "asset_type": "Stock",
        "amount_range": amount_range[:255],
        "source_url": source_url[:500],
        "doc_id": f"senate-{doc_key}-{trade_index}",
        "asset_name": asset_name[:255] if asset_name else "",
        "_asset_name": asset_name[:255] if asset_name else "",
    }


def prepare_senate_trades_for_insert(trades: list[dict]) -> list[dict]:
    supports_asset_name = politician_trades_has_asset_name_column(supabase)
    prepared: list[dict] = []
    for trade in trades:
        prepared_trade = {key: value for key, value in trade.items() if not key.startswith("_")}
        if not supports_asset_name:
            prepared_trade.pop("asset_name", None)
        elif not str(prepared_trade.get("asset_name") or "").strip():
            fallback_asset_name = str(trade.get("_asset_name") or "").strip()
            if fallback_asset_name:
                prepared_trade["asset_name"] = fallback_asset_name[:255]
        prepared.append(prepared_trade)
    return prepared


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


def detect_dark_line_groups(image: Image.Image, *, axis: str, threshold: int, min_fraction: float) -> list[tuple[int, int]]:
    grayscale = image.convert("L")
    pixels = grayscale.load()
    length = grayscale.height if axis == "horizontal" else grayscale.width
    breadth = grayscale.width if axis == "horizontal" else grayscale.height
    min_dark = int(breadth * min_fraction)
    positions: list[int] = []

    for primary in range(length):
        dark = 0
        for secondary in range(breadth):
            x, y = (secondary, primary) if axis == "horizontal" else (primary, secondary)
            if pixels[x, y] < threshold:
                dark += 1
        if dark >= min_dark:
            positions.append(primary)

    groups: list[tuple[int, int]] = []
    for position in positions:
        if not groups or position - groups[-1][1] > 4:
            groups.append((position, position))
        else:
            groups[-1] = (groups[-1][0], position)
    return groups


def load_paper_images(session: requests.Session, image_urls: list[str]) -> list[Image.Image]:
    images: list[Image.Image] = []
    for image_url in image_urls:
        response = session.get(image_url, timeout=30)
        response.raise_for_status()
        images.append(Image.open(io.BytesIO(response.content)).convert("RGB"))
    return images


def count_senate_paper_transaction_rows(images: list[Image.Image]) -> int:
    row_count = 0
    for image in images:
        horizontal_lines = detect_dark_line_groups(image, axis="horizontal", threshold=180, min_fraction=0.25)
        for current, nxt in zip(horizontal_lines, horizontal_lines[1:]):
            row_start = current[1]
            row_end = nxt[0]
            height = row_end - row_start
            if not (100 <= height <= 170):
                continue
            if row_start < int(image.height * 0.4) or row_end > int(image.height * 0.92):
                continue
            row_count += 1
    return row_count


def collect_paper_tokens(images: list[Image.Image]) -> list[dict]:
    tokens: list[dict] = []
    for page_index, image in enumerate(images):
        data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

        for index, raw_text in enumerate(data["text"]):
            text = clean_text(raw_text)
            if not text:
                continue
            try:
                confidence = float(data["conf"][index])
            except Exception:
                confidence = 0.0
            if confidence < 20:
                continue
            tokens.append(
                {
                    "page": page_index,
                    "text": text,
                    "left": int(data["left"][index]),
                    "top": int(data["top"][index]),
                }
            )
    return tokens


def group_tokens_by_line(tokens: list[dict]) -> list[list[dict]]:
    grouped: list[list[dict]] = []
    for token in sorted(tokens, key=lambda item: (item["page"], item["top"], item["left"])):
        if not grouped:
            grouped.append([token])
            continue
        last_line = grouped[-1]
        last_token = last_line[0]
        if token["page"] == last_token["page"] and abs(token["top"] - last_token["top"]) <= 18:
            last_line.append(token)
        else:
            grouped.append([token])
    for line in grouped:
        line.sort(key=lambda item: item["left"])
    return grouped


def clean_paper_asset_text(asset_text: str) -> str:
    asset_text = re.sub(r"^\((?:S|J|D|DC|SP|JT|PE)\)\s*", "", asset_text, flags=re.IGNORECASE)
    asset_text = re.sub(r"\s+x+\s*$", "", asset_text, flags=re.IGNORECASE)
    return clean_text(asset_text.rstrip(":"))


def parse_senate_paper_lines(
    line_tokens: list[list[dict]],
    *,
    doc_key: str,
    member_id: str,
    first_name: str,
    last_name: str,
    filed_date: str,
    source_url: str,
    valid_tickers: set[str],
) -> list[dict]:
    trades: list[dict] = []
    seen: set[str] = set()

    for tokens in line_tokens:
        date_tokens = [token for token in tokens if DATE_RE.fullmatch(token["text"])]
        if not date_tokens:
            continue

        asset_tokens = [token for token in tokens if token["left"] < 1400]
        if not asset_tokens:
            continue

        asset_text = clean_paper_asset_text(" ".join(token["text"] for token in asset_tokens))
        if not asset_text or asset_text.endswith(":"):
            continue

        tx_date = normalize_ocr_date(date_tokens[0]["text"])
        if not tx_date:
            continue

        ticker = resolve_company_ticker(asset_text, valid_tickers) or "N/A"

        type_marks = [
            token
            for token in tokens
            if 1350 <= token["left"] < 1650 and PAPER_MARK_RE.match(token["text"])
        ]
        amount_marks = [
            token
            for token in tokens
            if 1880 <= token["left"] < 2900 and PAPER_MARK_RE.match(token["text"])
        ]

        line_text = " ".join(token["text"] for token in tokens)
        transaction_type = infer_paper_transaction_type(type_marks, line_text)
        amount_range = infer_paper_amount(amount_marks)
        if ticker != "N/A":
            upsert_company(ticker, asset_text)

        unique_key = "|".join(
            (
                str(tokens[0].get("page") or 0),
                str(min(token.get("top") or 0 for token in tokens)),
                re.sub(r"\s+", " ", asset_text).upper(),
                tx_date,
                transaction_type,
                amount_range,
            )
        )
        if unique_key in seen:
            continue
        seen.add(unique_key)

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
                transaction_type=transaction_type,
                amount_range=amount_range,
                source_url=source_url,
                asset_name=asset_text,
            )
        )

    return trades


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

    paper_row_count = count_senate_paper_transaction_rows(images)

    try:
        tokens = collect_paper_tokens(images)
    except Exception as exc:
        print(f"Failed to OCR Senate paper filing {doc_key}: {exc}")
        return [], paper_row_count

    lines = group_tokens_by_line(tokens)
    return (
        parse_senate_paper_lines(
            lines,
            doc_key=doc_key,
            member_id=member_id,
            first_name=first_name,
            last_name=last_name,
            filed_date=filed_date,
            source_url=source_url,
            valid_tickers=valid_tickers,
        ),
        paper_row_count,
    )


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

    all_rows = []
    for start_offset in range(0, max_pagination, 100):
        print(f" -> Fetching offset {start_offset}...")
        payload = {
            "start": str(start_offset),
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": "01/01/2012 00:00:00",
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

    try:
        members_req = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute()
        members_db = members_req.data if members_req else []
    except Exception as exc:
        print(f"Warn: Could not fetch congress_members for mapping ({exc})")
        members_db = []

    valid_tickers = load_valid_tickers()
    formatted_trades: list[dict] = []
    inserted_count = 0
    paper_filings_seen = 0
    paper_transaction_rows_seen = 0
    paper_trades_parsed = 0
    paper_unmapped_filings = 0
    parse_failures = 0
    failed_doc_ids: list[str] = []

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
            consecutive_existing += 1
            if daily_mode and consecutive_existing >= 10:
                print(" -> Hit 10 consecutive existing Senate filings. Stopping.")
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
                "parse_failures": parse_failures,
                "records_inserted": inserted_count,
                "records_seen": len(prepared_trades),
                "records_skipped": max(len(prepared_trades) - inserted_count, 0),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    fetch_senate_trades()

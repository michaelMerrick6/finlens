import hashlib
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from time_utils import congress_now


CAPITOL_TRADES_BASE_URL = "https://www.capitoltrades.com"
CAPITOL_TRADES_LIST_URL = f"{CAPITOL_TRADES_BASE_URL}/trades"
CAPITOL_OFFICIAL_SOURCE_DOMAINS = ("disclosures-clerk.house.gov", "efdsearch.senate.gov")
HOUSE_OFFICIAL_SOURCE_RE = re.compile(r"/ptr-pdfs/(?P<year>\d{4})/(?P<doc_id>\d+)\.pdf(?:$|[?#])")
SENATE_OFFICIAL_SOURCE_RE = re.compile(r"/search/view/(?:paper|ptr|report|annual)/(?P<doc_key>[^/?#]+)")
SERIALIZED_OFFICIAL_SOURCE_RE = re.compile(
    r'filingUrl\\":\\"(?P<url>https://(?:disclosures-clerk\.house\.gov|efdsearch\.senate\.gov)[^"\\]+)\\"'
)
PLAIN_OFFICIAL_SOURCE_RE = re.compile(
    r'(?P<url>https://(?:disclosures-clerk\.house\.gov|efdsearch\.senate\.gov)[^"\\<]+)'
)
AMOUNT_MAP = {
    "1K–15K": "$1,001 - $15,000",
    "15K–50K": "$15,001 - $50,000",
    "50K–100K": "$50,001 - $100,000",
    "100K–250K": "$100,001 - $250,000",
    "250K–500K": "$250,001 - $500,000",
    "500K–1M": "$500,001 - $1,000,000",
    "1M–5M": "$1,000,001 - $5,000,000",
    "5M–25M": "$5,000,001 - $25,000,000",
    "25M–50M": "$25,000,001 - $50,000,000",
    "50M+": "Over $50,000,000",
}


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_actor_name(value: str) -> str:
    lowered = clean_text(value).lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def parse_trade_date_parts(top_text: str, bottom_text: str) -> str | None:
    top_text = clean_text(top_text)
    bottom_text = clean_text(bottom_text)
    if not top_text:
        return None

    full_text = f"{top_text} {bottom_text}".strip()
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(full_text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_published_cell(cell, *, now: datetime | None = None) -> tuple[str | None, str | None]:
    now = now or congress_now()
    top = clean_text(cell.select_one(".text-size-3") and cell.select_one(".text-size-3").get_text(" ", strip=True))
    bottom = clean_text(cell.select_one(".text-size-2") and cell.select_one(".text-size-2").get_text(" ", strip=True))
    combined = clean_text(cell.get_text(" ", strip=True))

    if bottom.isdigit() and len(bottom) == 4:
        return parse_trade_date_parts(top, bottom), None

    if bottom.lower() == "today":
        if re.fullmatch(r"\d{1,2}:\d{2}", top):
            return now.strftime("%Y-%m-%d"), f"{now.strftime('%Y-%m-%d')}T{top}:00"
        return now.strftime("%Y-%m-%d"), None

    if bottom.lower() == "yesterday":
        published = now - timedelta(days=1)
        if re.fullmatch(r"\d{1,2}:\d{2}", top):
            return published.strftime("%Y-%m-%d"), f"{published.strftime('%Y-%m-%d')}T{top}:00"
        return published.strftime("%Y-%m-%d"), None

    explicit_date = parse_trade_date_parts(top, bottom)
    if explicit_date:
        return explicit_date, None

    if re.fullmatch(r"\d{1,2}\s+[A-Za-z]{3}\s+\d{4}", combined):
        try:
            return datetime.strptime(combined, "%d %b %Y").strftime("%Y-%m-%d"), None
        except ValueError:
            pass

    return None, None


def parse_transaction_cell(cell) -> str | None:
    top = clean_text(cell.select_one(".text-size-3") and cell.select_one(".text-size-3").get_text(" ", strip=True))
    bottom = clean_text(cell.select_one(".text-size-2") and cell.select_one(".text-size-2").get_text(" ", strip=True))
    return parse_trade_date_parts(top, bottom)


def parse_reporting_gap_days(cell) -> int | None:
    digits = re.findall(r"\d+", clean_text(cell.get_text(" ", strip=True)))
    if not digits:
        return None
    try:
        return int(digits[-1])
    except ValueError:
        return None


def extract_official_source_url_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.select("a[href]"):
        href = clean_text(anchor.get("href"))
        if any(domain in href for domain in CAPITOL_OFFICIAL_SOURCE_DOMAINS):
            return href
    normalized_html = html.replace("\\/", "/")
    for pattern in (SERIALIZED_OFFICIAL_SOURCE_RE, PLAIN_OFFICIAL_SOURCE_RE):
        match = pattern.search(normalized_html)
        if match:
            return match.group("url")
    return None


def build_bridge_doc_id(official_source_url: str, capitol_trade_id: str) -> str | None:
    house_match = HOUSE_OFFICIAL_SOURCE_RE.search(official_source_url or "")
    if house_match:
        return f"house-{house_match.group('year')}-{house_match.group('doc_id')}-capitol-{capitol_trade_id}"

    senate_match = SENATE_OFFICIAL_SOURCE_RE.search(official_source_url or "")
    if senate_match:
        return f"senate-{senate_match.group('doc_key')}-capitol-{capitol_trade_id}"

    return None


def enrich_with_detail(session: requests.Session, detail_url: str) -> dict:
    response = session.get(detail_url, timeout=30)
    response.raise_for_status()
    html = response.text
    return {"official_source_url": extract_official_source_url_from_html(html)}


def parse_row(row, *, now: datetime | None = None) -> dict | None:
    now = now or congress_now()
    cells = row.find_all("td")
    if len(cells) < 8:
        return None

    politician_link = cells[0].find("a", href=re.compile(r"^/politicians/"))
    detail_link = row.find("a", href=re.compile(r"^/trades/\d+$"))
    issuer_link = cells[1].find("a", href=re.compile(r"^/issuers/"))
    if not politician_link or not detail_link or not issuer_link:
        return None

    politician_name = clean_text(politician_link.get_text(" ", strip=True))
    if not politician_name:
        return None

    ticker_span = cells[1].select_one(".issuer-ticker")
    ticker_raw = clean_text(ticker_span.get_text(" ", strip=True) if ticker_span else "")
    ticker = ticker_raw.split(":")[0].upper() if ticker_raw else "N/A"
    if not re.fullmatch(r"[A-Z0-9.-]{1,10}", ticker or ""):
        ticker = "N/A"

    published_date, published_at = parse_published_cell(cells[2], now=now)
    transaction_date = parse_transaction_cell(cells[3])
    if not transaction_date:
        return None

    detail_href = clean_text(detail_link.get("href"))
    trade_id = detail_href.rstrip("/").split("/")[-1]
    tx_type = clean_text(cells[6].get_text(" ", strip=True)).lower()
    if "buy" in tx_type:
        transaction_type = "buy"
    elif "sell" in tx_type:
        transaction_type = "sell"
    else:
        transaction_type = "exchange"

    amount_label = clean_text(cells[7].get_text(" ", strip=True))
    amount_range = AMOUNT_MAP.get(amount_label, amount_label or "Unknown")
    party = clean_text(cells[0].select_one("[class*='party--']") and cells[0].select_one("[class*='party--']").get_text(" ", strip=True)) or "Unknown"
    chamber = clean_text(cells[0].select_one("[class*='chamber--']") and cells[0].select_one("[class*='chamber--']").get_text(" ", strip=True)) or "Unknown"
    state = clean_text(cells[0].select_one("[class*='us-state-compact--']") and cells[0].select_one("[class*='us-state-compact--']").get_text(" ", strip=True)).upper()
    owner = clean_text(cells[5].get_text(" ", strip=True)) or "Unknown"
    asset_name = clean_text(issuer_link.get_text(" ", strip=True))
    reporting_gap_days = parse_reporting_gap_days(cells[4])

    return {
        "source_document_id": f"capitol-trade-{trade_id}",
        "capitol_trade_id": trade_id,
        "detail_url": urljoin(CAPITOL_TRADES_BASE_URL, detail_href),
        "official_source_url": None,
        "politician_name": politician_name,
        "politician_key": normalize_actor_name(politician_name),
        "politician_profile_url": urljoin(CAPITOL_TRADES_BASE_URL, clean_text(politician_link.get("href"))),
        "asset_name": asset_name,
        "issuer_url": urljoin(CAPITOL_TRADES_BASE_URL, clean_text(issuer_link.get("href"))),
        "ticker": ticker,
        "transaction_type": transaction_type,
        "transaction_date": transaction_date,
        "published_date": published_date,
        "published_at": published_at,
        "reporting_gap_days": reporting_gap_days,
        "party": party,
        "chamber": chamber,
        "state": state,
        "owner": owner,
        "amount_range": amount_range,
    }


def parse_trade_page(html: str, *, now: datetime | None = None) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    trades: list[dict] = []
    for row in soup.select("table tbody tr"):
        parsed = parse_row(row, now=now)
        if parsed:
            trades.append(parsed)
    return trades


def content_hash(lead: dict) -> str:
    payload = "|".join(
        [
            str(lead.get("source_document_id") or ""),
            str(lead.get("politician_name") or ""),
            str(lead.get("ticker") or ""),
            str(lead.get("transaction_date") or ""),
            str(lead.get("transaction_type") or ""),
            str(lead.get("published_date") or ""),
            str(lead.get("official_source_url") or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

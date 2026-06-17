import json
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from sec_form4_support import (
    create_session,
    extract_sec_accession,
    fetch_with_retry,
    load_json_cache,
    write_json_cache,
)


DEFAULT_TRACKED_13F_FUNDS = [
    {"cik": "0001067983", "name": "Berkshire Hathaway Inc"},
    {"cik": "0001336528", "name": "Pershing Square Capital Management, L.P."},
    {"cik": "0001649339", "name": "Scion Asset Management, LLC"},
    {"cik": "0001423053", "name": "Citadel Advisors LLC"},
]
TRACKED_13F_FUNDS_CONFIG_PATH = Path(
    os.environ.get("SEC_13F_FUNDS_PATH")
    or (Path(__file__).resolve().parents[1] / "config" / "tracked_13f_funds.json")
)

SEC_COMPANY_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_SUBMISSIONS_URL_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_SUBMISSION_HISTORY_URL_TEMPLATE = "https://data.sec.gov/submissions/{filename}"
SEC_13F_COMPANY_CACHE_PATH = Path("/tmp/vail-sec-13f-company-tickers-exchange.json")
SEC_13F_SUBMISSIONS_CACHE_DIR = Path("/tmp/vail-sec-13f-submissions")
SEC_13F_DOC_CACHE_DIR = Path("/tmp/vail-sec-13f-doc-cache")
SEC_13F_COMPANY_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
SEC_13F_SUBMISSIONS_CACHE_MAX_AGE_SECONDS = 6 * 60 * 60
SEC_13F_DOC_CACHE_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
SEC_13F_REQUEST_TIMEOUT_SECONDS = 20

NAME_TOKEN_REPLACEMENTS = {
    "FINL": ("FINANCIAL",),
    "HLDG": ("HOLDING",),
    "HLDGS": ("HOLDINGS",),
    "INFRAS": ("INFRASTRUCTURE",),
    "ENTMT": ("ENTERTAINMENT",),
    "MANUFAC": ("MANUFACTURING",),
    "PETE": ("PETROLEUM",),
    "PAC": ("PACIFIC",),
    "SVCS": ("SERVICES",),
    "INTL": ("INTERNATIONAL",),
    "BK": ("BANK",),
}
NAME_STOPWORDS = {
    "COM",
    "COMMON",
    "STK",
    "STOCK",
    "ORD",
    "ORDINARY",
    "SHS",
    "SHARES",
    "SPONSORED",
    "ADR",
    "ADS",
    "HOLDINGS",
    "HOLDING",
    "HLDGS",
    "CORP",
    "CORPORATION",
    "INC",
    "LTD",
    "LIMITED",
    "PLC",
    "PLLC",
    "CO",
    "COMPANY",
    "LT",
    "SA",
    "NV",
    "AG",
    "GROUP",
    "NEW",
    "DEL",
    "REGISTRY",
}
UNSUPPORTED_CLASS_TOKENS = {
    "PFD",
    "PREFERRED",
    "NOTE",
    "NOTES",
    "BOND",
    "WARRANT",
    "WT",
    "UNIT",
    "DEBT",
    "DEBENTURE",
}
PREFERRED_EXCHANGE_RANK = {
    "Nasdaq": 0,
    "NYSE": 1,
    "NYSE American": 2,
    "Cboe BZX": 3,
    "CboeBYX": 4,
}
SPECIAL_CLASS_TICKERS = {
    ("ALPHABET", "A"): "GOOGL",
    ("ALPHABET", "C"): "GOOG",
    ("HEICO", "A"): "HEI-A",
    ("HEICO", "COMMON"): "HEI",
    ("ATLANTA BRAVES", "A"): "BATRA",
    ("ATLANTA BRAVES", "B"): "BATRB",
    ("ATLANTA BRAVES", "C"): "BATRK",
    ("LIBERTY LATIN AMERICA", "A"): "LILA",
    ("LIBERTY LATIN AMERICA", "B"): "LILAB",
    ("LIBERTY LATIN AMERICA", "C"): "LILAK",
    ("LIBERTY LIVE", "A"): "LLYVA",
    ("LIBERTY LIVE", "B"): "LLYVB",
    ("LIBERTY LIVE", "C"): "LLYVK",
    ("LIBERTY MEDIA", "A"): "FWONA",
    ("LIBERTY MEDIA", "B"): "FWONB",
    ("LIBERTY MEDIA", "C"): "FWONK",
}
SPECIAL_CUSIP_TICKERS = {
    # Common 13F issuer abbreviations / ETF rows that do not resolve cleanly
    # through the SEC company_tickers_exchange name list.
    "09173B107": "BITF",
    "433921103": "HIVE",
    "74347M108": "PUMP",
    "83418M103": "SEI",
    "874039100": "TSM",
    "92189F676": "SMH",
    "N07059210": "ASML",
}
SPECIAL_ISSUER_TICKERS = {
    "BITFARMS": "BITF",
    "HIVE DIGITAL TECHNOLOGIES": "HIVE",
    "PROPETRO HOLDING": "PUMP",
    "SOLARIS ENERGY INFRASTRUCTURE": "SEI",
    "TAIWAN SEMICONDUCTOR MANUFACTURING": "TSM",
}


def normalize_tracked_fund(entry: Any) -> dict[str, str] | None:
    if not isinstance(entry, dict):
        return None
    cik_digits = re.sub(r"\D", "", str(entry.get("cik") or ""))
    name = str(entry.get("name") or "").strip()
    if not cik_digits or not name:
        return None
    return {"cik": f"{int(cik_digits):010d}", "name": name}


def load_tracked_13f_funds() -> list[dict[str, str]]:
    payload: Any = None
    inline_json = str(os.environ.get("SEC_13F_FUNDS_JSON") or "").strip()
    if inline_json:
        try:
            payload = json.loads(inline_json)
        except json.JSONDecodeError:
            payload = None
    elif TRACKED_13F_FUNDS_CONFIG_PATH.exists():
        try:
            payload = json.loads(TRACKED_13F_FUNDS_CONFIG_PATH.read_text())
        except (OSError, json.JSONDecodeError):
            payload = None

    entries = payload.get("funds") if isinstance(payload, dict) else payload
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    if isinstance(entries, list):
        for entry in entries:
            fund = normalize_tracked_fund(entry)
            if not fund or fund["cik"] in seen:
                continue
            seen.add(fund["cik"])
            normalized.append(fund)
    if normalized:
        return normalized
    return [fund.copy() for fund in DEFAULT_TRACKED_13F_FUNDS]


TRACKED_13F_FUNDS = load_tracked_13f_funds()


def normalize_issuer_name(value: str | None) -> str:
    text = str(value or "").upper().replace("&", " AND ")
    text = re.sub(r"/[A-Z]{2,4}/", " ", text)
    text = re.sub(r"/[A-Z]{2,4}\b", " ", text)
    text = text.replace("BANK AMERICA", "BANK OF AMERICA")
    text = text.replace("INCORPORATED", "INC")
    tokens: list[str] = []
    for raw_token in re.findall(r"[A-Z0-9]+", text):
        if raw_token in NAME_TOKEN_REPLACEMENTS:
            tokens.extend(
                replacement
                for replacement in NAME_TOKEN_REPLACEMENTS[raw_token]
                if replacement not in NAME_STOPWORDS
            )
            continue
        if raw_token in NAME_STOPWORDS:
            continue
        if len(raw_token) == 1 and raw_token not in {"A", "B", "C"}:
            continue
        tokens.append(raw_token)
    return " ".join(tokens).strip()


def normalize_share_class(value: str | None) -> str:
    text = str(value or "").upper()
    if "ADR" in text or "ADS" in text:
        return "ADR"
    for pattern in (r"\bCL(?:ASS)?\s+([A-Z])\b", r"\bSER(?:IES)?\s+([A-Z])\b", r"\bS\s+([A-Z])\b"):
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return "COMMON"


def is_supported_equity_row(title_of_class: str | None, put_call: str | None, share_amount_type: str | None) -> bool:
    if str(put_call or "").strip():
        return False
    share_type = str(share_amount_type or "").strip().upper()
    if share_type and share_type != "SH":
        return False
    title = str(title_of_class or "").upper()
    if any(token in title for token in UNSUPPORTED_CLASS_TOKENS):
        return False
    return True


def parse_int(value: str | None) -> int:
    raw = re.sub(r"[^\d-]", "", str(value or ""))
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def exchange_rank(exchange: str | None) -> int:
    return PREFERRED_EXCHANGE_RANK.get(str(exchange or "").strip(), 99)


def is_preferred_share_ticker(ticker: str | None) -> bool:
    return bool(re.search(r"-P[A-Z0-9]*$", str(ticker or "").upper()))


def class_ticker_candidates(candidates: list[dict[str, Any]], share_class: str) -> list[dict[str, Any]]:
    if share_class not in {"A", "B", "C"}:
        return []

    def matches(candidate_ticker: str) -> bool:
        ticker = candidate_ticker.upper()
        if share_class == "A":
            return ticker.endswith("-A") or (ticker.endswith("A") and not is_preferred_share_ticker(ticker))
        if share_class == "B":
            return ticker.endswith("-B") or (ticker.endswith("B") and not is_preferred_share_ticker(ticker))
        return ticker.endswith("-C") or ticker.endswith("C") or ticker.endswith("K")

    return [candidate for candidate in candidates if matches(candidate["ticker"])]


class SecTickerResolver:
    def __init__(self, companies: list[dict[str, Any]]):
        self.by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for company in companies:
            ticker = str(company.get("ticker") or "").strip().upper()
            name = str(company.get("name") or "").strip()
            if not ticker or not name:
                continue
            normalized = normalize_issuer_name(name)
            if not normalized:
                continue
            self.by_name[normalized].append(
                {
                    "ticker": ticker,
                    "name": name,
                    "exchange": str(company.get("exchange") or "").strip(),
                }
            )

    def resolve_ticker(self, issuer_name: str | None, title_of_class: str | None, cusip: str | None = None) -> str | None:
        normalized_cusip = str(cusip or "").strip().upper()
        if normalized_cusip in SPECIAL_CUSIP_TICKERS:
            return SPECIAL_CUSIP_TICKERS[normalized_cusip]

        normalized_name = normalize_issuer_name(issuer_name)
        if not normalized_name:
            return None

        if normalized_name in SPECIAL_ISSUER_TICKERS:
            return SPECIAL_ISSUER_TICKERS[normalized_name]

        candidates = list(self.by_name.get(normalized_name, []))
        if not candidates:
            return None

        major_exchange_candidates = [candidate for candidate in candidates if exchange_rank(candidate.get("exchange")) < 99]
        if major_exchange_candidates:
            candidates = major_exchange_candidates

        share_class = normalize_share_class(title_of_class)

        special_ticker = SPECIAL_CLASS_TICKERS.get((normalized_name, share_class))
        if special_ticker and any(candidate["ticker"] == special_ticker for candidate in candidates):
            return special_ticker

        if share_class in {"COMMON", "ADR", "A", "B", "C"}:
            non_preferred = [candidate for candidate in candidates if not is_preferred_share_ticker(candidate["ticker"])]
            if non_preferred:
                candidates = non_preferred

        if share_class in {"A", "B", "C"}:
            class_candidates = class_ticker_candidates(candidates, share_class)
            if class_candidates:
                candidates = class_candidates

        plain_tickers = [candidate for candidate in candidates if "-" not in candidate["ticker"]]
        if share_class in {"COMMON", "ADR"} and plain_tickers:
            candidates = plain_tickers

        candidates = sorted(
            candidates,
            key=lambda candidate: (
                exchange_rank(candidate.get("exchange")),
                1 if "-" in candidate["ticker"] else 0,
                len(candidate["ticker"]),
                candidate["ticker"],
            ),
        )
        return candidates[0]["ticker"] if candidates else None


def load_company_reference(session) -> list[dict[str, Any]]:
    cached = load_json_cache(SEC_13F_COMPANY_CACHE_PATH, max_age_seconds=SEC_13F_COMPANY_CACHE_MAX_AGE_SECONDS)
    if cached and isinstance(cached.get("companies"), list):
        return cached["companies"]

    response = fetch_with_retry(
        session,
        SEC_COMPANY_TICKERS_EXCHANGE_URL,
        timeout=SEC_13F_REQUEST_TIMEOUT_SECONDS,
        label="SEC company tickers exchange",
    )
    payload = response.json()
    fields = payload.get("fields") or []
    rows = [dict(zip(fields, values)) for values in payload.get("data") or []]
    companies = [
        {
            "ticker": str(row.get("ticker") or "").strip().upper(),
            "name": str(row.get("name") or "").strip(),
            "exchange": str(row.get("exchange") or "").strip(),
        }
        for row in rows
        if row.get("ticker") and row.get("name")
    ]
    write_json_cache(SEC_13F_COMPANY_CACHE_PATH, {"companies": companies})
    return companies


def load_fund_submissions(session, cik: str) -> dict[str, Any]:
    cache_path = SEC_13F_SUBMISSIONS_CACHE_DIR / f"{str(int(cik))}.json"
    cached = load_json_cache(cache_path, max_age_seconds=SEC_13F_SUBMISSIONS_CACHE_MAX_AGE_SECONDS)
    if cached:
        return cached

    response = fetch_with_retry(
        session,
        SEC_SUBMISSIONS_URL_TEMPLATE.format(cik=f"{int(cik):010d}"),
        timeout=SEC_13F_REQUEST_TIMEOUT_SECONDS,
        label=f"SEC submissions {cik}",
    )
    payload = response.json()
    write_json_cache(cache_path, payload)
    return payload


def load_submission_history_file(session, filename: str) -> dict[str, Any]:
    cache_path = SEC_13F_SUBMISSIONS_CACHE_DIR / filename
    cached = load_json_cache(cache_path, max_age_seconds=SEC_13F_SUBMISSIONS_CACHE_MAX_AGE_SECONDS)
    if cached:
        return cached

    response = fetch_with_retry(
        session,
        SEC_SUBMISSION_HISTORY_URL_TEMPLATE.format(filename=filename),
        timeout=SEC_13F_REQUEST_TIMEOUT_SECONDS,
        label=f"SEC submissions history {filename}",
    )
    payload = response.json()
    write_json_cache(cache_path, payload)
    return payload


def build_fund_filing_url(cik: str, accession: str) -> str:
    accession_compact = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_compact}/{accession}.txt"


def is_unbounded_limit(value: int | None) -> bool:
    return value is None or int(value) <= 0


def build_13f_filing_entries(
    fund: dict[str, str],
    submission_index: dict[str, Any],
    *,
    seen_accessions: set[str] | None = None,
) -> list[dict[str, str]]:
    filings: list[dict[str, str]] = []
    accessions_seen = seen_accessions if seen_accessions is not None else set()
    forms = submission_index.get("form") or []
    accessions = submission_index.get("accessionNumber") or []
    filed_dates = submission_index.get("filingDate") or []

    for form, accession, filed_date in zip(forms, accessions, filed_dates):
        if form not in {"13F-HR", "13F-HR/A"}:
            continue
        accession = str(accession or "").strip()
        if not accession:
            continue
        if accession in accessions_seen:
            continue
        accessions_seen.add(accession)
        filings.append(
            {
                "fund_name": fund["name"],
                "cik": fund["cik"],
                "form": form,
                "accession": accession,
                "filed_date": str(filed_date or "").strip()[:10],
                "source_url": build_fund_filing_url(fund["cik"], accession),
            }
        )
    return filings


def load_available_13f_filings(session, fund: dict[str, str], *, max_filings: int | None = None) -> list[dict[str, str]]:
    payload = load_fund_submissions(session, fund["cik"])
    filings_payload = payload.get("filings", {})
    seen_accessions: set[str] = set()
    filings = build_13f_filing_entries(
        fund,
        filings_payload.get("recent", {}),
        seen_accessions=seen_accessions,
    )

    for file_entry in filings_payload.get("files", []) or []:
        filename = str(file_entry.get("name") or "").strip()
        if not filename:
            continue
        history_payload = load_submission_history_file(session, filename)
        filings.extend(build_13f_filing_entries(fund, history_payload, seen_accessions=seen_accessions))

    filings.sort(key=lambda filing: (str(filing.get("filed_date") or ""), str(filing.get("accession") or "")), reverse=True)
    if not is_unbounded_limit(max_filings):
        return filings[: int(max_filings)]
    return filings


def load_recent_13f_filings(session, fund: dict[str, str], *, max_filings: int | None = None) -> list[dict[str, str]]:
    return load_available_13f_filings(session, fund, max_filings=max_filings)


def load_13f_filing_text(session, source_url: str) -> str:
    accession = extract_sec_accession(source_url) or re.sub(r"[^A-Za-z0-9]+", "-", source_url)[:120]
    cache_path = SEC_13F_DOC_CACHE_DIR / f"{accession}.txt"
    cached = load_json_cache(cache_path, max_age_seconds=SEC_13F_DOC_CACHE_MAX_AGE_SECONDS)
    if cached and isinstance(cached.get("text"), str):
        return cached["text"]

    response = fetch_with_retry(
        session,
        source_url,
        timeout=SEC_13F_REQUEST_TIMEOUT_SECONDS,
        label=f"SEC 13F document {accession}",
    )
    text = response.text
    write_json_cache(cache_path, {"text": text})
    return text


def strip_default_namespace(xml_body: str) -> str:
    # Some SEC XML documents include prefixed attributes such as
    # xsi:schemaLocation. Once xmlns declarations are removed, those attributes
    # become unbound and ElementTree rejects the document. Strip namespaced
    # attributes before normalizing tag prefixes.
    xml_body = re.sub(r'\s[A-Za-z_][\w.-]*:[A-Za-z_][\w.-]*="[^"]*"', "", xml_body)
    xml_body = re.sub(r'\sxmlns(?::[A-Za-z_][\w.-]*)?="[^"]+"', "", xml_body)
    return re.sub(r"<(/?)[A-Za-z_][\w.-]*:", r"<\1", xml_body)


def extract_xml_blocks(text: str) -> list[str]:
    return [match.group(1).strip() for match in re.finditer(r"<XML>(.*?)</XML>", text, flags=re.DOTALL | re.IGNORECASE)]


def extract_report_period(text: str, filed_date: str | None) -> str | None:
    match = re.search(r"<periodOfReport>\s*([^<]+)\s*</periodOfReport>", text, flags=re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        if re.fullmatch(r"\d{2}-\d{2}-\d{4}", raw):
            month, day, year = raw.split("-")
            return f"{year}-{month}-{day}"
        return raw[:10]
    return str(filed_date or "").strip()[:10] or None


def extract_information_table_root(text: str) -> ET.Element | None:
    for block in extract_xml_blocks(text):
        if "informationTable" not in block and "infoTable" not in block:
            continue
        try:
            return ET.fromstring(strip_default_namespace(block))
        except ET.ParseError:
            continue
    return None


def parse_13f_filing(session, filing: dict[str, str], resolver: SecTickerResolver) -> dict[str, Any] | None:
    text = load_13f_filing_text(session, filing["source_url"])
    report_period = extract_report_period(text, filing.get("filed_date"))
    root = extract_information_table_root(text)
    if root is None or not report_period:
        return None

    aggregated: dict[str, dict[str, Any]] = {}
    rows_seen = 0
    rows_supported = 0
    rows_skipped = 0
    rows_resolved = 0
    rows_unresolved = 0

    for node in root.findall(".//infoTable"):
        rows_seen += 1
        issuer_name = str(node.findtext("nameOfIssuer") or "").strip()
        title_of_class = str(node.findtext("titleOfClass") or "").strip()
        cusip = str(node.findtext("cusip") or "").strip().upper()
        share_amount_type = str(node.findtext(".//shrsOrPrnAmt/sshPrnamtType") or "").strip().upper()
        put_call = str(node.findtext("putCall") or "").strip().upper()
        shares_held = parse_int(node.findtext(".//shrsOrPrnAmt/sshPrnamt"))
        value_held = parse_int(node.findtext("value")) * 1000

        if not issuer_name or shares_held <= 0:
            rows_skipped += 1
            continue
        if not is_supported_equity_row(title_of_class, put_call, share_amount_type):
            rows_skipped += 1
            continue

        rows_supported += 1
        ticker = resolver.resolve_ticker(issuer_name, title_of_class, cusip)
        if ticker:
            rows_resolved += 1
        else:
            rows_unresolved += 1

        share_class = normalize_share_class(title_of_class)
        aggregate_key = ticker or f"{cusip or normalize_issuer_name(issuer_name)}::{share_class}"
        current = aggregated.setdefault(
            aggregate_key,
            {
                "fund_name": filing["fund_name"],
                "ticker": ticker,
                "report_period": report_period,
                "published_date": filing.get("filed_date"),
                "shares_held": 0,
                "value_held": 0,
                "source_url": filing["source_url"],
                "_issuer_name": issuer_name,
                "_title_of_class": title_of_class,
                "_cusip": cusip,
                "_share_class": share_class,
                "_asset_key": aggregate_key,
                "_accession": filing.get("accession"),
            },
        )
        current["shares_held"] += shares_held
        current["value_held"] += value_held

    holdings = list(aggregated.values())
    return {
        "fund_name": filing["fund_name"],
        "accession": filing.get("accession"),
        "report_period": report_period,
        "published_date": filing.get("filed_date"),
        "source_url": filing["source_url"],
        "holdings": holdings,
        "rows_seen": rows_seen,
        "rows_supported": rows_supported,
        "rows_skipped": rows_skipped,
        "rows_resolved": rows_resolved,
        "rows_unresolved": rows_unresolved,
    }


def create_13f_session():
    return create_session()

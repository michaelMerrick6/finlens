import csv
import difflib
import io
import json
import os
import re
import subprocess
import tempfile
import unicodedata
from datetime import datetime

import pytesseract
import requests
from dotenv import load_dotenv
from pdf2image import convert_from_bytes
from PIL import ImageOps
from pypdf import PdfReader, PdfWriter
from supabase import Client, create_client
from politician_schema_support import politician_trades_has_asset_name_column
from time_utils import congress_now

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

HOUSE_INDEX_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt"
HOUSE_PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

HOUSE_ASSET_TYPE_RE = re.compile(r"\[(?P<asset_type>[A-Z]{2})\]")
HOUSE_TX_RE = re.compile(
    r"(?P<tx_code>[PSE])(?:\s+\((?P<qualifier>[^)]+)\))?\s*"
    r"(?P<tx_date>\d{1,2}/\d{1,2}/\d{4})\s*"
    r"(?P<notif_date>\d{1,2}/\d{1,2}/\d{4})"
    r"(?P<tail>.*)$"
)
HOUSE_AMOUNT_RE = re.compile(
    r"(Over\s+\$[0-9,]+|Under\s+\$[0-9,]+|\$[0-9,]+\s*-\s*\$[0-9,]+|\$[0-9,]+)"
)
HOUSE_TICKER_RE = re.compile(r"\(([A-Z]{1,6})\)")
HOUSE_LAYOUT_TICKER_RE = re.compile(r"\(([A-Za-z]{1,10})\)(?:\s+\[(?P<asset_type>[A-Z]{2})\])?")
HOUSE_INLINE_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{1,4}\b")
HOUSE_FUZZY_DATE_RE = re.compile(r"(\d{1,2})\D+(\d{1,2})\D+(\d{1,4})")
HOUSE_LAYOUT_ROW_RE = re.compile(
    r"^(?P<asset_name>.+?)\s+"
    r"(?P<tx_code>[PSEpse])\s+"
    r"(?P<tx_date>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<notif_date>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<amount>Over\s+\$[0-9,]+|Under\s+\$[0-9,]+|\$[0-9,]+\s*-\s*\$[0-9,]+|\$[0-9,]+)"
)
HOUSE_OWNER_PREFIX_RE = re.compile(r"^(?:SP|DC|JT|C|D|S)\s+", re.IGNORECASE)
HOUSE_LAYOUT_OWNER_PREFIX_RE = re.compile(r"^(?:[A-Za-z]{1,3}\s+)+(?=[A-Za-z])")
FIRST_NAME_ALIAS_GROUPS = (
    {"bill", "billy", "will", "william"},
    {"dan", "daniel", "danny"},
    {"dave", "david"},
    {"jim", "jimmy", "james"},
    {"rick", "richard"},
    {"ted", "rafael"},
    {"tom", "tommy", "thomas"},
)
FIRST_NAME_ALIAS_MAP = {token: group for group in FIRST_NAME_ALIAS_GROUPS for token in group}
HOUSE_HEADER_FRAGMENTS = (
    "CLERK OF THE HOUSE OF REPRESENTATIVES",
    "LEGISLATIVE RESOURCE CENTER",
    "PERIODIC TRANSACTION REPORT",
    "ID OWNER ASSET TRANSACTION",
    "TYPE DATE NOTIFICATION",
    "DATE AMOUNT CAP",
    "GAINS >",
    "FILING ID #",
    "I CERTIFY",
    "STATUS: MEMBER",
    "STATE/DISTRICT:",
)
HOUSE_STOP_PREFIXES = ("* FOR THE COMPLETE LIST", "I V D", "INVESTMENTS AND TRUSTS", "CERTIFICATION AND SIGNATURE")
HOUSE_PREVIOUS_YEAR_DAILY_LIMIT = int(os.environ.get("HOUSE_PREVIOUS_YEAR_DAILY_LIMIT", "10"))
HOUSE_OCR_TIMEOUT_SECONDS = int(os.environ.get("HOUSE_OCR_TIMEOUT_SECONDS", "20"))
HOUSE_SCANNED_ROW_HEIGHT_RANGE = (18, 140)
HOUSE_SCANNED_MIN_ROW_START_RATIO = 0.35
HOUSE_SCANNED_MAX_ROW_END_RATIO = 0.92
HOUSE_SCANNED_LINE_CONFIGS = (
    (180, 0.4),
    (210, 0.25),
    (210, 0.2),
    (200, 0.25),
)
HOUSE_SCANNED_ASSET_BOUNDS = (0.121, 0.308)
HOUSE_SCANNED_TYPE_BOUNDS = (0.309, 0.423)
HOUSE_SCANNED_TYPE_COLUMNS = (0.309, 0.338, 0.363, 0.395, 0.423)
HOUSE_SCANNED_TX_DATE_BOUNDS = (0.423, 0.480)
HOUSE_SCANNED_NOTIFIED_BOUNDS = (0.480, 0.546)
HOUSE_SCANNED_AMOUNT_COLUMNS = (0.548, 0.582, 0.617, 0.652, 0.688, 0.723, 0.757, 0.792, 0.827, 0.863, 0.899, 0.932)
HOUSE_ATTACHMENT_ASSET_BOUNDS = (0.094, 0.255)
HOUSE_ATTACHMENT_TYPE_COLUMNS = (0.255, 0.285, 0.303, 0.332, 0.363)
HOUSE_ATTACHMENT_TX_DATE_BOUNDS = (0.363, 0.453)
HOUSE_ATTACHMENT_NOTIFIED_BOUNDS = (0.453, 0.505)
HOUSE_ATTACHMENT_AMOUNT_COLUMNS = (0.505, 0.538, 0.571, 0.600, 0.628, 0.656, 0.689, 0.720, 0.757, 0.825, 0.862, 0.941)
HOUSE_SCANNED_SKIP_TEXT = (
    "PROVIDE FULL NAME",
    "MEGA CORP",
    "EXAMPLE",
    "FULL ASSET NAME",
    "AMOUNT OF TRANSACTION",
    "NOTE NUMBER",
    "FILER NOTES",
)
HOUSE_ATTACHMENT_MARKERS = ("PLEASE SEE THE ATTACHED", "PLEASE SEE ATTACHED")
HOUSE_NO_TRADE_MARKERS = ("NOTHING TO REPORT", "NO TRANSACTIONS TO REPORT")
HOUSE_AMOUNT_RANGES = (
    "$1,001 - $15,000",
    "$15,001 - $50,000",
    "$50,001 - $100,000",
    "$100,001 - $250,000",
    "$250,001 - $500,000",
    "$500,001 - $1,000,000",
    "$1,000,001 - $5,000,000",
    "$5,000,001 - $25,000,000",
    "$25,000,001 - $50,000,000",
    "Over $50,000,000",
    "Transaction over $1,000,000 in a spouse or dependent child asset",
)
HOUSE_COMPANY_ALIASES = {
    "abbott laboratories": "ABT",
    "ally financial": "ALLY",
    "american intl group": "AIG",
    "american international group": "AIG",
    "american tower": "AMT",
    "avnet": "AVT",
    "arista networks": "ANET",
    "at t": "T",
    "att": "T",
    "axalta": "AXTA",
    "biogen": "BIIB",
    "bank of america": "BAC",
    "berkshire hathaway": "BRKB",
    "block inc": "XYZ",
    "broadcom": "AVGO",
    "canadian imperial bank": "CM",
    "capital one financial": "COF",
    "costco wholesale": "COST",
    "carrier global": "CARR",
    "colgate palmolive": "CL",
    "coca cola europacific partners": "CCEP",
    "estee laude": "EL",
    "estee lauder": "EL",
    "eaton": "ETN",
    "eversource energy": "ES",
    "extra space storage": "EXR",
    "first solar": "FSLR",
    "fortinet": "FTNT",
    "ge vernova": "GEV",
    "gilead sciences": "GILD",
    "hims hers health": "HIMS",
    "hubspot": "HUBS",
    "imperial bank": "CM",
    "international business machines": "IBM",
    "ibm": "IBM",
    "intel": "INTC",
    "kimberly clark": "KMB",
    "lam research": "LRCX",
    "lam resh": "LRCX",
    "mead johnson": "MJN",
    "michael kors": "KORS",
    "martin marietta": "MLM",
    "metlife": "MET",
    "micron technology": "MU",
    "occidental petro": "OXY",
    "paycom software": "PAYC",
    "qihu 360": "QIHU",
    "quanta services": "PWR",
    "rockwell automation": "ROK",
    "salesforce": "CRM",
    "servicenow": "NOW",
    "starbucks": "SBUX",
    "sysco": "SYY",
    "texas instruments": "TXN",
    "thermo fisher": "TMO",
    "trade desk": "TTD",
    "united rentals": "URI",
    "waste management": "WM",
    "western alliance": "WAL",
    "zoetis": "ZTS",
}
COMPANY_LOOKUP_STOPWORDS = {
    "and",
    "class",
    "cmn",
    "cmin",
    "co",
    "common",
    "corp",
    "corporation",
    "group",
    "inc",
    "ltd",
    "ordinary",
    "plc",
    "share",
    "shares",
    "stock",
}
HOUSE_SCANNED_LAYOUTS = {
    "default": {
        "asset_bounds": HOUSE_SCANNED_ASSET_BOUNDS,
        "type_columns": HOUSE_SCANNED_TYPE_COLUMNS,
        "tx_date_bounds": HOUSE_SCANNED_TX_DATE_BOUNDS,
        "notified_bounds": HOUSE_SCANNED_NOTIFIED_BOUNDS,
        "amount_columns": HOUSE_SCANNED_AMOUNT_COLUMNS,
    },
    "attachment": {
        "asset_bounds": HOUSE_ATTACHMENT_ASSET_BOUNDS,
        "type_columns": HOUSE_ATTACHMENT_TYPE_COLUMNS,
        "tx_date_bounds": HOUSE_ATTACHMENT_TX_DATE_BOUNDS,
        "notified_bounds": HOUSE_ATTACHMENT_NOTIFIED_BOUNDS,
        "amount_columns": HOUSE_ATTACHMENT_AMOUNT_COLUMNS,
    },
}


def normalize_line(text: str) -> str:
    text = text.replace("\x00", " ")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_name_part(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", normalized.lower())


def normalize_company_lookup_name(value: str) -> str:
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\bclass\s+[a-z]\b", " ", value)
    value = re.sub(r"\bcommon stock\b", " ", value)
    value = re.sub(r"\bst\b", " ", value)
    value = re.sub(r"\bbill\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def normalize_company_lookup_tokens(value: str) -> list[str]:
    return [token for token in normalize_company_lookup_name(value).split() if token not in COMPANY_LOOKUP_STOPWORDS]


def resolve_house_alias_ticker(*candidates: str) -> str | None:
    for candidate in candidates:
        if not candidate:
            continue

        ticker_match = HOUSE_TICKER_RE.search(candidate)
        if ticker_match:
            return ticker_match.group(1)[:10]

        candidate_upper = candidate.upper()
        if "UNITED STATES TREAS" in candidate_upper or "U.S. TREASURY" in candidate_upper:
            return "US-TREAS"
        if "US TREAS" in candidate_upper or "TREASURY BILL" in candidate_upper:
            return "US-TREAS"
        if "ALPHABET" in candidate_upper:
            return "GOOGL"
        if "CHUBB" in candidate_upper:
            return "CB"

        normalized_candidate = normalize_company_lookup_name(candidate)
        for alias, ticker in HOUSE_COMPANY_ALIASES.items():
            if alias in normalized_candidate:
                return ticker
    return None


def is_placeholder_company_record(ticker: str, name: str) -> bool:
    if not ticker or set(ticker) <= {"-"} or ticker == "N/A":
        return True
    normalized_name = normalize_company_lookup_name(name).replace(" ", "")
    normalized_ticker = re.sub(r"[^a-z0-9]", "", ticker.lower())
    return len(normalized_ticker) >= 5 and normalized_name.startswith(normalized_ticker)


def extract_pdf_lines(pdf_bytes: bytes) -> list[str]:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        lines: list[str] = []
        for page in reader.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
        return lines
    except Exception as exc:
        print(f"Error reading PDF text layer: {exc}")
        return []


def extract_pdftotext_layout_lines(pdf_bytes: bytes) -> list[str]:
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as handle:
            handle.write(pdf_bytes)
            handle.flush()
            temp_path = handle.name
        result = subprocess.run(
            ["pdftotext", "-layout", temp_path, "-"],
            capture_output=True,
            text=True,
            timeout=HOUSE_OCR_TIMEOUT_SECONDS,
            check=False,
        )
        if result.returncode != 0:
            return []
        return result.stdout.splitlines()
    except (FileNotFoundError, subprocess.SubprocessError, OSError):
        return []
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


def extract_ocr_lines(pdf_bytes: bytes) -> list[str]:
    try:
        images = convert_from_bytes(pdf_bytes, dpi=250)
    except Exception as exc:
        print(f"Error converting House PDF to images: {exc}")
        return []

    lines: list[str] = []
    for image in images:
        try:
            text = pytesseract.image_to_string(image, timeout=HOUSE_OCR_TIMEOUT_SECONDS)
        except Exception as exc:
            print(f"Error OCRing House PDF page: {exc}")
            continue
        lines.extend(text.splitlines())
    return lines


def extract_document_lines(pdf_bytes: bytes) -> tuple[list[str], bool]:
    pdf_lines = [normalize_line(line) for line in extract_pdf_lines(pdf_bytes)]
    pdf_lines = [line for line in pdf_lines if line]
    printable_len = sum(len(line) for line in pdf_lines)
    if printable_len >= 80:
        return pdf_lines, False

    ocr_lines = [normalize_line(line) for line in extract_ocr_lines(pdf_bytes)]
    ocr_lines = [line for line in ocr_lines if line]
    if ocr_lines:
        return ocr_lines, True
    return pdf_lines, False


def score_house_transactions(trades: list[dict]) -> tuple[int, int]:
    resolved = sum(1 for trade in trades if (trade.get("ticker") or "").strip().upper() not in {"", "N/A"})
    return len(trades), resolved


def select_best_house_transactions(*candidates: list[dict]) -> list[dict]:
    viable = [candidate for candidate in candidates if candidate]
    if not viable:
        return []
    return max(viable, key=score_house_transactions)


def should_skip_house_line(line: str) -> bool:
    upper = line.upper()
    if any(fragment in upper for fragment in HOUSE_HEADER_FRAGMENTS):
        return True
    if any(upper.startswith(prefix) for prefix in HOUSE_STOP_PREFIXES):
        return True
    if upper in {"T", "TYPE", "DATE", "DATE NOTIFICATION", "AMOUNT CAP.", "$200?"}:
        return True
    return False


def normalize_name_tokens(value: str) -> list[str]:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return [token for token in re.findall(r"[a-z]+", normalized.lower()) if token not in {"jr", "sr", "ii", "iii", "iv", "v"}]


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


def resolve_member_id(first_name: str, last_name: str, members_db: list[dict], target_chamber: str = "House") -> str:
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


def load_company_lookup() -> list[dict]:
    companies: list[dict] = []
    offset = 0
    while True:
        response = supabase.table("companies").select("ticker, name").range(offset, offset + 999).execute()
        rows = response.data or []
        if not rows:
            break
        for row in rows:
            ticker = (row.get("ticker") or "").strip()
            name = (row.get("name") or "").strip()
            if not ticker or not name:
                continue
            if is_placeholder_company_record(ticker, name):
                continue
            companies.append(
                {
                    "ticker": ticker[:10],
                    "name": name,
                    "normalized_name": normalize_company_lookup_name(name),
                    "tokens": normalize_company_lookup_tokens(name),
                }
            )
        if len(rows) < 1000:
            break
        offset += 1000
    return companies


def resolve_house_ticker(
    asset_name: str,
    company_lookup: list[dict] | None = None,
    fallback_texts: list[str] | None = None,
    *,
    allow_company_lookup: bool = True,
) -> str:
    fallback_texts = fallback_texts or []

    alias_ticker = resolve_house_alias_ticker(asset_name, *fallback_texts)
    if alias_ticker:
        return alias_ticker

    normalized_candidates = [normalize_company_lookup_name(asset_name)]
    normalized_candidates.extend(
        normalize_company_lookup_name(candidate)
        for candidate in fallback_texts
        if candidate
    )
    normalized_candidates = [candidate for candidate in normalized_candidates if candidate]

    if allow_company_lookup and company_lookup and normalized_candidates:
        for normalized_candidate in normalized_candidates:
            candidate_tokens = normalize_company_lookup_tokens(normalized_candidate)
            for company in company_lookup:
                if normalized_candidate == company["normalized_name"]:
                    return company["ticker"]
            for company in company_lookup:
                company_tokens = company.get("tokens") or []
                if len(candidate_tokens) < 2 or len(company_tokens) < 2:
                    continue
                shared_length = min(len(candidate_tokens), len(company_tokens), 3)
                if all(
                    candidate_token == company_token
                    or candidate_token.startswith(company_token)
                    or company_token.startswith(candidate_token)
                    for candidate_token, company_token in zip(candidate_tokens[:shared_length], company_tokens[:shared_length])
                ):
                    return company["ticker"]
            if candidate_tokens:
                best_company = None
                best_ratio = 0.0
                for company in company_lookup:
                    company_tokens = company.get("tokens") or []
                    if not company_tokens:
                        continue
                    if len(candidate_tokens) >= 2:
                        if len(company_tokens) < 2:
                            continue
                        prefix_matches = 0
                        for candidate_token, company_token in zip(candidate_tokens[:2], company_tokens[:2]):
                            if candidate_token == company_token:
                                prefix_matches += 1
                                continue
                            shorter = min(len(candidate_token), len(company_token), 5)
                            if shorter >= 3 and candidate_token[:shorter] == company_token[:shorter]:
                                prefix_matches += 1
                        if prefix_matches < 2:
                            continue
                    else:
                        candidate_token = candidate_tokens[0]
                        company_token = company_tokens[0]
                        shorter = min(len(candidate_token), len(company_token), 5)
                        if shorter < 3 or candidate_token[:shorter] != company_token[:shorter]:
                            continue

                    ratio = difflib.SequenceMatcher(None, normalized_candidate, company["normalized_name"]).ratio()
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_company = company
                if best_company and best_ratio >= 0.9:
                    return best_company["ticker"]
    return "N/A"


def clean_house_asset_name(asset_name: str) -> str:
    asset_name = HOUSE_OWNER_PREFIX_RE.sub("", asset_name).strip()
    asset_name = re.sub(r"\([^)]*\)", "", asset_name).strip()
    return asset_name


def house_trade_fingerprint(*parts: str) -> str:
    normalized_parts = [normalize_line(part).upper() for part in parts if part and normalize_line(part)]
    return "|".join(normalized_parts)


def upsert_company(ticker: str, company_name: str):
    try:
        supabase.table("companies").upsert(
            {
                "ticker": ticker,
                "name": company_name[:255] if company_name else ticker,
                "sector": "Unknown",
                "industry": "Unknown",
            }
        ).execute()
    except Exception as exc:
        print(f"Warning: company upsert failed for {ticker}: {exc}")


def prepare_house_trades_for_insert(trades: list[dict]) -> list[dict]:
    prepared: list[dict] = []
    seen_tickers: set[str] = set()
    supports_asset_name = politician_trades_has_asset_name_column(supabase)

    for trade in trades:
        ticker = (trade.get("ticker") or "").strip().upper()[:10]
        if ticker and ticker != "N/A" and ticker not in seen_tickers:
            upsert_company(ticker, trade.get("_company_name") or ticker)
            seen_tickers.add(ticker)

        prepared_trade = {key: value for key, value in trade.items() if not key.startswith("_")}
        asset_name = str(trade.get("asset_name") or trade.get("_company_name") or "").strip()
        if supports_asset_name and asset_name:
            prepared_trade["asset_name"] = asset_name[:255]

        prepared.append(prepared_trade)

    return prepared


def parse_house_date(raw_value: str) -> str | None:
    try:
        return datetime.strptime(raw_value, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(raw_value, "%m/%d/%y").strftime("%Y-%m-%d")
        except ValueError:
            return None


def parse_house_scanned_date(raw_value: str, tx_year: int) -> str | None:
    parsed = parse_house_date(raw_value)
    if parsed:
        return parsed

    match = HOUSE_FUZZY_DATE_RE.search(raw_value or "")
    if not match:
        return None

    month, day, year_token = match.groups()
    try:
        month_value = int(month)
        day_value = int(day)
    except ValueError:
        return None

    if not (1 <= month_value <= 12 and 1 <= day_value <= 31):
        return None

    candidate_year_suffixes: list[str] = []
    if len(year_token) == 1:
        expected_suffix = f"{tx_year % 100:02d}"
        candidate_year_suffixes.extend((year_token + expected_suffix[-1], expected_suffix[0] + year_token))
    elif len(year_token) == 3:
        expected_suffix = f"{tx_year:04d}"
        candidate_year_suffixes.extend((year_token[:2], year_token[1:], expected_suffix[2:]))

    seen_suffixes: set[str] = set()
    for suffix in candidate_year_suffixes:
        if suffix in seen_suffixes or not re.fullmatch(r"\d{2}", suffix):
            continue
        seen_suffixes.add(suffix)
        candidate = parse_house_date(f"{month_value:02d}/{day_value:02d}/{suffix}")
        if not candidate:
            continue
        candidate_year = int(candidate[:4])
        if tx_year - 1 <= candidate_year <= tx_year + 1:
            return candidate
    return None


def crop_ratio_box(image, bounds: tuple[float, float], row_start: int, row_end: int):
    left = int(image.width * bounds[0])
    right = int(image.width * bounds[1])
    return image.crop((left, row_start, right, row_end))


def clean_ocr_text(value: str) -> str:
    value = value.replace("\n", " ")
    value = value.replace("|", " ")
    value = value.replace("’", "'")
    value = re.sub(r"\s+", " ", value)
    return value.strip(" .,:;")


def ocr_house_scanned_cell(image, *, scale: int = 3, config: str = "--psm 7") -> str:
    cell = ImageOps.autocontrast(image.convert("L"))
    cell = cell.crop((4, 4, max(cell.width - 4, 4), max(cell.height - 4, 4)))
    if cell.width <= 0 or cell.height <= 0:
        return ""
    if scale > 1:
        cell = cell.resize((cell.width * scale, cell.height * scale))
    try:
        text = pytesseract.image_to_string(cell, config=config, timeout=HOUSE_OCR_TIMEOUT_SECONDS)
    except Exception:
        return ""
    return clean_ocr_text(text)


def detect_dark_line_groups(image, *, axis: str, threshold: int, min_fraction: float) -> list[tuple[int, int]]:
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


def build_house_checkbox_columns(image_width: int, ratios: tuple[float, ...]) -> list[tuple[int, int]]:
    columns: list[tuple[int, int]] = []
    for start, end in zip(ratios, ratios[1:]):
        columns.append((int(image_width * start), int(image_width * end)))
    return columns


def score_house_checkbox_row(image, row_start: int, row_end: int, columns: list[tuple[int, int]]) -> list[float]:
    grayscale = image.convert("L")
    pixels = grayscale.load()
    scores: list[float] = []

    margin_y = max(8, int((row_end - row_start) * 0.2))
    for column_start, column_end in columns:
        margin_x = max(6, int((column_end - column_start) * 0.18))
        x1 = min(column_start + margin_x, column_end - 1)
        x2 = max(column_end - margin_x, x1 + 1)
        y1 = min(row_start + margin_y, row_end - 1)
        y2 = max(row_end - margin_y, y1 + 1)

        dark = 0
        total = 0
        for y in range(y1, y2):
            for x in range(x1, x2):
                total += 1
                if pixels[x, y] < 180:
                    dark += 1
        scores.append(dark / total if total else 0.0)
    return scores


def extract_house_scanned_rows_for_config(
    image,
    *,
    asset_bounds: tuple[float, float],
    threshold: int,
    min_fraction: float,
) -> list[tuple[int, int]]:
    horizontal_lines = detect_dark_line_groups(image, axis="horizontal", threshold=threshold, min_fraction=min_fraction)
    rows: list[tuple[int, int]] = []
    min_height, max_height = HOUSE_SCANNED_ROW_HEIGHT_RANGE
    min_row_start = int(image.height * HOUSE_SCANNED_MIN_ROW_START_RATIO)
    max_row_end = int(image.height * HOUSE_SCANNED_MAX_ROW_END_RATIO)

    for current, nxt in zip(horizontal_lines, horizontal_lines[1:]):
        row_start = current[1]
        row_end = nxt[0]
        height = row_end - row_start
        if not (min_height <= height <= max_height):
            continue
        if row_start < min_row_start or row_end > max_row_end:
            continue
        asset_crop = crop_ratio_box(image, asset_bounds, row_start, row_end)
        asset_text = ocr_house_scanned_cell(asset_crop)
        asset_upper = asset_text.upper()
        if not asset_text or any(fragment in asset_upper for fragment in HOUSE_SCANNED_SKIP_TEXT):
            continue
        rows.append((row_start, row_end))
    return rows


def extract_house_scanned_rows(image, *, asset_bounds: tuple[float, float] = HOUSE_SCANNED_ASSET_BOUNDS) -> list[tuple[int, int]]:
    best_rows: list[tuple[int, int]] = []

    for threshold, min_fraction in HOUSE_SCANNED_LINE_CONFIGS:
        rows = extract_house_scanned_rows_for_config(
            image,
            asset_bounds=asset_bounds,
            threshold=threshold,
            min_fraction=min_fraction,
        )
        if len(rows) > len(best_rows):
            best_rows = rows
        if len(rows) >= 3:
            return rows

    return best_rows


def line_group_centers(groups: list[tuple[int, int]]) -> list[int]:
    return [int((start + end) / 2) for start, end in groups]


def has_line_near(centers: list[int], image_width: int, ratio: float, tolerance: float = 0.04) -> bool:
    target = image_width * ratio
    wiggle = image_width * tolerance
    return any(abs(center - target) <= wiggle for center in centers)


def is_house_attachment_continuation_page(image) -> bool:
    vertical_lines = detect_dark_line_groups(image, axis="vertical", threshold=180, min_fraction=0.3)
    centers = line_group_centers(vertical_lines)
    if len(centers) < 12:
        return False
    return all(
        has_line_near(centers, image.width, ratio)
        for ratio in (HOUSE_ATTACHMENT_ASSET_BOUNDS[1], HOUSE_ATTACHMENT_TX_DATE_BOUNDS[0], HOUSE_ATTACHMENT_NOTIFIED_BOUNDS[1])
    )


def detect_house_attachment_only_filing(pdf_bytes: bytes) -> bool:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.pages:
            return False
        writer = PdfWriter()
        writer.add_page(reader.pages[0])
        first_page_pdf = io.BytesIO()
        writer.write(first_page_pdf)
        images = convert_from_bytes(first_page_pdf.getvalue(), dpi=180)
    except Exception:
        return False
    if not images:
        return False

    image = images[0]
    if image.width < image.height:
        image = image.rotate(90, expand=True)

    row_bounds = extract_house_scanned_rows(image)
    if not row_bounds:
        return False

    saw_valid_tx_date = False
    for row_start, row_end in extract_house_scanned_rows(image):
        asset_text = ocr_house_scanned_cell(crop_ratio_box(image, HOUSE_SCANNED_ASSET_BOUNDS, row_start, row_end))
        asset_upper = asset_text.upper()
        if any(marker in asset_upper for marker in HOUSE_ATTACHMENT_MARKERS):
            return True
        tx_date_text = ocr_house_scanned_cell(
            crop_ratio_box(image, HOUSE_SCANNED_TX_DATE_BOUNDS, row_start, row_end),
            scale=4,
            config="--psm 7 -c tessedit_char_whitelist=0123456789/",
        )
        if parse_house_date(tx_date_text) or HOUSE_FUZZY_DATE_RE.search(tx_date_text):
            saw_valid_tx_date = True

    return len(reader.pages) >= 20 and not saw_valid_tx_date


def detect_house_no_trade_filing(pdf_bytes: bytes) -> bool:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.pages:
            return False
        writer = PdfWriter()
        writer.add_page(reader.pages[0])
        first_page_pdf = io.BytesIO()
        writer.write(first_page_pdf)
        images = convert_from_bytes(first_page_pdf.getvalue(), dpi=180)
    except Exception:
        return False
    if not images:
        return False

    image = images[0]
    if image.width < image.height:
        image = image.rotate(90, expand=True)

    for row_start, row_end in extract_house_scanned_rows(image):
        asset_text = ocr_house_scanned_cell(crop_ratio_box(image, HOUSE_SCANNED_ASSET_BOUNDS, row_start, row_end))
        asset_upper = asset_text.upper()
        if any(marker in asset_upper for marker in HOUSE_NO_TRADE_MARKERS):
            return True
    return False


def extract_transactions_from_scanned_house_pdf(
    pdf_bytes: bytes,
    doc_id: str,
    first_name: str,
    last_name: str,
    tx_year: int,
    members_db: list[dict],
    company_lookup: list[dict] | None = None,
    *,
    attachment_hint: bool = False,
) -> list[dict]:
    try:
        images = convert_from_bytes(pdf_bytes, dpi=250)
    except Exception as exc:
        print(f"Error converting scanned House PDF {doc_id}: {exc}")
        return []

    member_id = resolve_member_id(first_name, last_name, members_db)
    transactions: list[dict] = []
    seen_keys: set[str] = set()

    for image in images:
        if image.width < image.height:
            image = image.rotate(90, expand=True)

        layout_names = ["default"]
        if is_house_attachment_continuation_page(image):
            layout_names = ["attachment", "default"]
        elif attachment_hint:
            layout_names = ["default", "attachment"]

        for layout_name in layout_names:
            layout = HOUSE_SCANNED_LAYOUTS[layout_name]
            row_bounds = extract_house_scanned_rows(image, asset_bounds=layout["asset_bounds"])
            if not row_bounds:
                continue

            type_columns = build_house_checkbox_columns(image.width, layout["type_columns"])
            amount_columns = build_house_checkbox_columns(image.width, layout["amount_columns"])
            page_transactions_before = len(transactions)

            for row_start, row_end in row_bounds:
                asset_text = ocr_house_scanned_cell(crop_ratio_box(image, layout["asset_bounds"], row_start, row_end))
                asset_text = clean_house_asset_name(asset_text)
                if len(re.sub(r"[^A-Za-z0-9]", "", asset_text)) < 3:
                    continue

                row_text = ""
                if layout_name == "attachment":
                    row_text = ocr_house_scanned_cell(
                        image.crop((0, row_start, image.width, row_end)),
                        scale=3,
                        config="--psm 6",
                    )

                tx_date_text = ocr_house_scanned_cell(
                    crop_ratio_box(image, layout["tx_date_bounds"], row_start, row_end),
                    scale=4,
                    config="--psm 7 -c tessedit_char_whitelist=0123456789/",
                )
                tx_date = parse_house_scanned_date(tx_date_text, tx_year)
                if not tx_date:
                    if not row_text:
                        row_text = ocr_house_scanned_cell(
                            image.crop((0, row_start, image.width, row_end)),
                            scale=3,
                            config="--psm 6",
                        )
                    inline_dates = [parse_house_scanned_date(match, tx_year) for match in HOUSE_INLINE_DATE_RE.findall(row_text)]
                    inline_dates = [value for value in inline_dates if value]
                    if inline_dates:
                        tx_date = inline_dates[0]
                if not tx_date:
                    continue
                tx_year_value = int(tx_date[:4])
                if tx_year_value < tx_year - 1 or tx_year_value > tx_year + 1:
                    continue

                notified_text = ocr_house_scanned_cell(
                    crop_ratio_box(image, layout["notified_bounds"], row_start, row_end),
                    scale=4,
                    config="--psm 7 -c tessedit_char_whitelist=0123456789/",
                )
                published_date = parse_house_scanned_date(notified_text, tx_year) or tx_date
                if published_date == tx_date and row_text:
                    inline_dates = [parse_house_scanned_date(match, tx_year) for match in HOUSE_INLINE_DATE_RE.findall(row_text)]
                    inline_dates = [value for value in inline_dates if value]
                    if len(inline_dates) >= 2:
                        published_date = inline_dates[1]

                type_scores = score_house_checkbox_row(image, row_start, row_end, type_columns)
                amount_scores = score_house_checkbox_row(image, row_start, row_end, amount_columns)
                tx_type = ("buy", "sell", "sell", "exchange")[type_scores.index(max(type_scores))]
                amount_range = HOUSE_AMOUNT_RANGES[amount_scores.index(max(amount_scores))]

                layout_company_lookup = None if layout_name == "attachment" else company_lookup
                ticker = resolve_house_ticker(
                    asset_text,
                    layout_company_lookup,
                    fallback_texts=[row_text] if row_text else None,
                    allow_company_lookup=layout_name != "attachment",
                )

                if not row_text:
                    row_text = ocr_house_scanned_cell(
                        image.crop((0, row_start, image.width, row_end)),
                        scale=3,
                        config="--psm 6",
                    )

                key = "|".join(
                    (
                        ticker,
                        tx_date,
                        published_date,
                        amount_range,
                        tx_type,
                        house_trade_fingerprint(asset_text, row_text),
                    )
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                transactions.append(
                    {
                        "member_id": member_id,
                        "politician_name": f"{first_name} {last_name}"[:100],
                        "chamber": "House",
                        "party": "Unknown",
                        "ticker": ticker,
                        "transaction_date": tx_date,
                        "published_date": published_date,
                        "transaction_type": tx_type,
                        "asset_type": "Stock",
                        "amount_range": amount_range[:255],
                        "source_url": HOUSE_PTR_PDF_URL.format(year=tx_year, doc_id=doc_id),
                        "doc_id": f"house-{tx_year}-{doc_id}-{len(transactions)}",
                        "_company_name": asset_text[:255] if asset_text else ticker,
                    }
                )

            if len(transactions) > page_transactions_before:
                break

    return transactions


def extract_transactions_from_lines(
    lines: list[str],
    doc_id: str,
    first_name: str,
    last_name: str,
    tx_year: int,
    members_db: list[dict],
    company_lookup: list[dict] | None = None,
) -> list[dict]:
    filtered_lines = [line for line in lines if line and not should_skip_house_line(line)]
    member_id = resolve_member_id(first_name, last_name, members_db)

    transactions: list[dict] = []
    seen_keys: set[str] = set()

    for index, line in enumerate(filtered_lines):
        asset_match = HOUSE_ASSET_TYPE_RE.search(line)
        if not asset_match:
            continue

        asset_type = asset_match.group("asset_type")
        inline_asset_name = normalize_line(line[: asset_match.start()])
        if inline_asset_name:
            asset_name = inline_asset_name
        elif index > 0:
            asset_name = filtered_lines[index - 1]
        else:
            asset_name = ""
        if not asset_name:
            continue

        detail_parts = [normalize_line(line[asset_match.end() :])]
        lookahead = index + 1
        while lookahead < len(filtered_lines) and len(detail_parts) < 5:
            next_line = filtered_lines[lookahead]
            if HOUSE_ASSET_TYPE_RE.search(next_line):
                break
            if any(next_line.upper().startswith(prefix) for prefix in HOUSE_STOP_PREFIXES):
                break
            detail_parts.append(next_line)
            lookahead += 1

        detail_blob = normalize_line(" ".join(part for part in detail_parts if part))
        tx_match = HOUSE_TX_RE.search(detail_blob)
        if not tx_match:
            continue

        amount_match = HOUSE_AMOUNT_RE.search(detail_blob)
        if not amount_match:
            continue

        tx_date = parse_house_date(tx_match.group("tx_date"))
        if not tx_date:
            continue
        published_date = parse_house_date(tx_match.group("notif_date")) or tx_date

        tx_code = tx_match.group("tx_code")
        tx_type = {"P": "buy", "S": "sell", "E": "exchange"}.get(tx_code, "unknown")
        amount_range = amount_match.group(1).strip()
        ticker = resolve_house_ticker(asset_name, company_lookup)
        company_name = clean_house_asset_name(asset_name)

        key = "|".join(
            (
                ticker,
                tx_date,
                published_date,
                amount_range,
                tx_type,
                house_trade_fingerprint(asset_name, detail_blob),
            )
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)

        transactions.append(
            {
                "member_id": member_id,
                "politician_name": f"{first_name} {last_name}"[:100],
                "chamber": "House",
                "party": "Unknown",
                "ticker": ticker,
                "transaction_date": tx_date,
                "published_date": published_date,
                "transaction_type": tx_type,
                "asset_type": asset_type[:50] if asset_type else "Stock",
                "amount_range": amount_range[:255],
                "source_url": HOUSE_PTR_PDF_URL.format(year=tx_year, doc_id=doc_id),
                "doc_id": f"house-{tx_year}-{doc_id}-{len(transactions)}",
                "_company_name": company_name[:255] if company_name else ticker,
            }
        )

    return transactions


def extract_transactions_from_layout_lines(
    lines: list[str],
    doc_id: str,
    first_name: str,
    last_name: str,
    tx_year: int,
    members_db: list[dict],
    company_lookup: list[dict] | None = None,
) -> list[dict]:
    filtered_lines = [normalize_line(line) for line in lines if normalize_line(line)]
    member_id = resolve_member_id(first_name, last_name, members_db)

    transactions: list[dict] = []
    seen_keys: set[str] = set()

    for index, line in enumerate(filtered_lines):
        if should_skip_house_line(line):
            continue

        tx_match = HOUSE_LAYOUT_ROW_RE.search(line)
        if not tx_match:
            continue

        raw_asset_name = HOUSE_LAYOUT_OWNER_PREFIX_RE.sub("", tx_match.group("asset_name")).strip()
        asset_name = clean_house_asset_name(raw_asset_name)
        if len(re.sub(r"[^A-Za-z0-9]", "", asset_name)) < 3:
            continue

        asset_type = "Stock"
        ticker_match = HOUSE_LAYOUT_TICKER_RE.search(asset_name)
        fallback_texts = [line]
        for lookahead in range(index + 1, min(len(filtered_lines), index + 8)):
            next_line = filtered_lines[lookahead]
            if should_skip_house_line(next_line):
                break
            fallback_texts.append(next_line)
            if HOUSE_LAYOUT_ROW_RE.search(next_line):
                break
            if not ticker_match:
                ticker_match = HOUSE_LAYOUT_TICKER_RE.search(next_line)
            if ticker_match and ticker_match.group("asset_type"):
                asset_type = ticker_match.group("asset_type")[:50]
                break

        ticker = ticker_match.group(1).upper() if ticker_match else resolve_house_ticker(
            asset_name,
            company_lookup,
            fallback_texts=fallback_texts,
        )

        tx_date = parse_house_date(tx_match.group("tx_date"))
        if not tx_date:
            continue

        published_date = parse_house_date(tx_match.group("notif_date")) or tx_date
        tx_code = tx_match.group("tx_code").upper()
        tx_type = {"P": "buy", "S": "sell", "E": "exchange"}.get(tx_code, "unknown")
        amount_range = tx_match.group("amount").strip()
        detail_blob = " ".join(fallback_texts)

        key = "|".join(
            (
                ticker,
                tx_date,
                published_date,
                amount_range,
                tx_type,
                house_trade_fingerprint(asset_name, detail_blob),
            )
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)

        transactions.append(
            {
                "member_id": member_id,
                "politician_name": f"{first_name} {last_name}"[:100],
                "chamber": "House",
                "party": "Unknown",
                "ticker": ticker,
                "transaction_date": tx_date,
                "published_date": published_date,
                "transaction_type": tx_type,
                "asset_type": asset_type,
                "amount_range": amount_range[:255],
                "source_url": HOUSE_PTR_PDF_URL.format(year=tx_year, doc_id=doc_id),
                "doc_id": f"house-{tx_year}-{doc_id}-{len(transactions)}",
                "_company_name": asset_name[:255] if asset_name else ticker,
            }
        )

    return transactions


def extract_best_text_transactions(
    pdf_bytes: bytes,
    doc_id: str,
    first_name: str,
    last_name: str,
    tx_year: int,
    members_db: list[dict],
    company_lookup: list[dict] | None = None,
) -> tuple[list[dict], list[str]]:
    pdf_lines = [normalize_line(line) for line in extract_pdf_lines(pdf_bytes)]
    pdf_lines = [line for line in pdf_lines if line]

    standard_transactions: list[dict] = []
    if sum(len(line) for line in pdf_lines) >= 80:
        standard_transactions = extract_transactions_from_lines(
            pdf_lines,
            doc_id,
            first_name,
            last_name,
            tx_year,
            members_db,
            company_lookup,
        )

    layout_lines = [normalize_line(line) for line in extract_pdftotext_layout_lines(pdf_bytes)]
    layout_lines = [line for line in layout_lines if line]
    layout_transactions = extract_transactions_from_layout_lines(
        layout_lines,
        doc_id,
        first_name,
        last_name,
        tx_year,
        members_db,
        company_lookup,
    )

    return select_best_house_transactions(standard_transactions, layout_transactions), pdf_lines


def filing_sort_key(filing: dict) -> tuple[datetime, int]:
    try:
        filing_date = datetime.strptime(filing["filing_date"], "%m/%d/%Y")
    except Exception:
        filing_date = datetime.min
    try:
        numeric_doc_id = int(re.sub(r"\D", "", filing["doc_id"]) or "0")
    except ValueError:
        numeric_doc_id = 0
    return filing_date, numeric_doc_id


def fetch_house_trades():
    print("Starting Official House Clerk PTR Scraper...")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"})

    try:
        members_req = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute()
        members_db = members_req.data if members_req else []
    except Exception as exc:
        print(f"Warn: Could not fetch congress_members for mapping ({exc})")
        members_db = []
    try:
        company_lookup = load_company_lookup()
    except Exception as exc:
        print(f"Warn: Could not fetch companies for ticker resolution ({exc})")
        company_lookup = []

    all_transactions: list[dict] = []
    inserted_count = 0
    ocr_fallbacks = 0
    attachment_parsed_filings = 0
    attachment_parsed_doc_ids: list[str] = []
    attachment_only_filings = 0
    attachment_doc_ids: list[str] = []
    no_trade_filings = 0
    no_trade_doc_ids: list[str] = []
    carryover_parse_failures = 0
    carryover_failed_doc_ids: list[str] = []
    parse_failures = 0
    failed_doc_ids: list[str] = []

    daily_mode = os.environ.get("FINLENS_DAILY_MODE", "0") == "1"
    start_year = congress_now().year
    stop_year = start_year - 1 if daily_mode else 2012

    for year in range(start_year, stop_year - 1, -1):
        print(f"\n1. Fetching Bulk Index for {year}...")

        try:
            response = session.get(HOUSE_INDEX_URL.format(year=year), timeout=30)
        except Exception as exc:
            print(f"Skipping {year}, index request failed: {exc}")
            continue

        if response.status_code != 200:
            print(f"Skipping {year}, index not available.")
            continue

        payload = response.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(payload), delimiter="\t")

        bulk_filings = []
        for row in reader:
            if (row.get("FilingType") or "").strip().upper() != "P":
                continue
            doc_id = (row.get("DocID") or "").strip()
            if not doc_id:
                continue
            bulk_filings.append(
                {
                    "doc_id": doc_id,
                    "first_name": (row.get("First") or "").strip(),
                    "last_name": (row.get("Last") or "").strip(),
                    "filing_date": (row.get("FilingDate") or "").strip(),
                    "year": year,
                }
            )

        bulk_filings.sort(key=filing_sort_key, reverse=True)
        if daily_mode and year < start_year:
            bulk_filings = bulk_filings[:HOUSE_PREVIOUS_YEAR_DAILY_LIMIT]
        print(f"Found {len(bulk_filings)} House PTR filings in the {year} index.")

        consecutive_existing = 0
        for filing in bulk_filings:
            doc_id = filing["doc_id"]
            first_name = filing["first_name"]
            last_name = filing["last_name"]
            idx_filing_date_raw = filing["filing_date"]

            check = (
                supabase.table("politician_trades")
                .select("id")
                .eq("doc_id", f"house-{year}-{doc_id}-0")
                .limit(1)
                .execute()
            )
            if check.data:
                consecutive_existing += 1
                if daily_mode and year < start_year and consecutive_existing >= 25:
                    print("Reached 25 consecutive existing prior-year House filings. Stopping carryover scan.")
                    break
                continue

            consecutive_existing = 0

            try:
                idx_filing_date = datetime.strptime(idx_filing_date_raw, "%m/%d/%Y").strftime("%Y-%m-%d")
            except Exception:
                idx_filing_date = congress_now().strftime("%Y-%m-%d")

            pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
            print(f"Fetching NEW PDF {doc_id} for {first_name} {last_name}...")

            try:
                pdf_resp = session.get(pdf_url, timeout=(10, 60))
                pdf_resp.raise_for_status()

                used_ocr = False
                attachment_only = False
                no_trade_filing = False
                transactions, pdf_lines = extract_best_text_transactions(
                    pdf_resp.content,
                    doc_id,
                    first_name,
                    last_name,
                    year,
                    members_db,
                    company_lookup,
                )

                if not transactions:
                    no_trade_filing = detect_house_no_trade_filing(pdf_resp.content)
                    attachment_only = detect_house_attachment_only_filing(pdf_resp.content) if not no_trade_filing else False
                    if no_trade_filing or attachment_only:
                        used_ocr = True
                    if not no_trade_filing:
                        scanned_transactions = extract_transactions_from_scanned_house_pdf(
                            pdf_resp.content,
                            doc_id,
                            first_name,
                            last_name,
                            year,
                            members_db,
                            company_lookup,
                            attachment_hint=attachment_only,
                        )
                        if scanned_transactions:
                            used_ocr = True
                            transactions = scanned_transactions
                    if not transactions and not attachment_only and not pdf_lines:
                        ocr_lines = [normalize_line(line) for line in extract_ocr_lines(pdf_resp.content)]
                        ocr_lines = [line for line in ocr_lines if line]
                        if ocr_lines:
                            used_ocr = True
                            transactions = extract_transactions_from_lines(
                                ocr_lines, doc_id, first_name, last_name, year, members_db, company_lookup
                            )

                if used_ocr:
                    ocr_fallbacks += 1

                if transactions:
                    if attachment_only:
                        attachment_parsed_filings += 1
                        attachment_parsed_doc_ids.append(f"{year}-{doc_id}")
                    for transaction in transactions:
                        transaction["published_date"] = idx_filing_date
                    all_transactions.extend(transactions)
                    print(f" -> Extracted {len(transactions)} trades")
                elif no_trade_filing:
                    no_trade_filings += 1
                    no_trade_doc_ids.append(f"{year}-{doc_id}")
                    print(" -> Filing explicitly reports no transactions")
                elif attachment_only:
                    attachment_only_filings += 1
                    attachment_doc_ids.append(f"{year}-{doc_id}")
                    print(" -> Filing defers to attached statements; flagged for separate attachment parsing")
                else:
                    if daily_mode and year < start_year:
                        carryover_parse_failures += 1
                        carryover_failed_doc_ids.append(f"{year}-{doc_id}")
                        print(" -> No House trades parsed from prior-year carryover filing")
                    else:
                        parse_failures += 1
                        failed_doc_ids.append(f"{year}-{doc_id}")
                        print(" -> No House trades parsed from filing")
            except Exception as exc:
                if daily_mode and year < start_year:
                    carryover_parse_failures += 1
                    carryover_failed_doc_ids.append(f"{year}-{doc_id}")
                    print(f" -> Prior-year carryover PDF exception: {exc}")
                else:
                    parse_failures += 1
                    failed_doc_ids.append(f"{year}-{doc_id}")
                    print(f" -> PDF Exception: {exc}")

    print(f"\nFinished extracting {len(all_transactions)} standard House trades.")

    if all_transactions:
        print(f"Uploading {len(all_transactions)} real House trades to Supabase...")
        for index in range(0, len(all_transactions), 50):
            chunk = all_transactions[index : index + 50]
            try:
                doc_ids = [trade["doc_id"] for trade in chunk]
                existing = supabase.table("politician_trades").select("doc_id").in_("doc_id", doc_ids).execute()
                existing_ids = {row["doc_id"] for row in existing.data}

                to_insert = [trade for trade in chunk if trade["doc_id"] not in existing_ids]
                if to_insert:
                    prepared_trades = prepare_house_trades_for_insert(to_insert)
                    supabase.table("politician_trades").insert(prepared_trades).execute()
                    inserted_count += len(prepared_trades)
                    print(f" -> Inserted {len(to_insert)} new House trades.")
            except Exception as exc:
                print(f"Error manual-upserting chunk: {exc}")

        print("Successfully seeded HOUSE trades!")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "attachment_parsed_doc_ids": attachment_parsed_doc_ids[:20],
                "attachment_parsed_filings": attachment_parsed_filings,
                "attachment_doc_ids": attachment_doc_ids[:20],
                "attachment_only_filings": attachment_only_filings,
                "carryover_failed_doc_ids": carryover_failed_doc_ids[:20],
                "carryover_parse_failures": carryover_parse_failures,
                "no_trade_doc_ids": no_trade_doc_ids[:20],
                "no_trade_filings": no_trade_filings,
                "ocr_fallbacks": ocr_fallbacks,
                "parse_failures": parse_failures,
                "failed_doc_ids": failed_doc_ids[:20],
                "records_inserted": inserted_count,
                "records_seen": len(all_transactions),
                "records_skipped": max(len(all_transactions) - inserted_count, 0),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    fetch_house_trades()

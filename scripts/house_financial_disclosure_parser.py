from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable

from pypdf import PdfReader

CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
WHITESPACE_RE = re.compile(r"\s+")
ASSET_TYPE_RE = re.compile(r"\[(?P<asset_type>[A-Z]{2})\]")
PERIOD_COVERED_RE = re.compile(
    r"Period Covered:\s*(?P<start>\d{1,2}/\d{1,2}/\d{4})\s*[–-]\s*(?P<end>\d{1,2}/\d{1,2}/\d{4})",
    re.IGNORECASE,
)
TICKER_RE = re.compile(r"\(([A-Za-z][A-Za-z0-9.\-]{0,9})\)")
VALUE_RANGE_RE = re.compile(
    r"(Over\s+\$[0-9,]+|Under\s+\$[0-9,]+|\$[0-9,]+\s*-\s*\$[0-9,]+|None)",
    re.IGNORECASE,
)
OWNER_RE = re.compile(r"^(?P<owner>JT|SP|DC|S|D|C)\b", re.IGNORECASE)

SECTION_A_HEADER_MARKERS = (
    "ASSET OWNER VALUE OF ASSET",
    'ASSETS AND "UNEARNED" INCOME',
)
SECTION_STOP_MARKERS = (
    "SOURCE TYPE AMOUNT",
    "OWNER CREDITOR DATE INCURRED",
    "POSITION NAME OF ORGANIZATION",
    "AGREEMENTS",
    "COMPENSATION IN EXCESS OF $5,000",
    "SECTION B",
    "SECTION C",
    "SECTION D",
    "SECTION E",
    "SECTION F",
    "SECTION G",
    "SECTION H",
    "SECTION I",
    "SECTION J",
)
SECTION_SKIP_PREFIXES = (
    "CLERK OF THE HOUSE",
    "NAME:",
    "STATUS:",
    "STATE/DISTRICT:",
    "FILING TYPE:",
    "FILING YEAR:",
    "FILING DATE:",
    "ASSET OWNER VALUE OF ASSET",
    "CURRENT YEAR TO FILING",
    "CURRENT YEAR TO FILING INCOME PRECEDING YEAR",
    "INCOME PRECEDING YEAR",
    "LOCATION:",
    "DESCRIPTION:",
    "FILING ID #",
    "NONE DISCLOSED.",
    "* FOR THE COMPLETE LIST",
)
BLOCK_METADATA_HINTS = (
    "CAPITAL GAINS",
    "DIVIDENDS",
    "INTEREST",
    "NOT APPLICABLE",
    "SECONDARY CHECKING ACCOUNT",
    "CHECKING ACCOUNT",
    "MORTGAGE",
)


@dataclass
class HouseDisclosureHolding:
    asset_name: str
    asset_type_code: str | None
    ticker: str | None
    owner: str | None
    value_range: str


@dataclass
class HouseDisclosureDocument:
    period_covered_start: str | None
    period_covered_end: str | None
    holdings: list[HouseDisclosureHolding]


def clean_pdf_text(value: str) -> str:
    return CONTROL_CHARS_RE.sub("", value or "")


def normalize_line(value: str) -> str:
    return WHITESPACE_RE.sub(" ", clean_pdf_text(value)).strip()


def normalize_iso_date(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    parts = raw_value.strip().split("/")
    if len(parts) != 3:
        return None
    month, day, year = parts
    if len(year) == 2:
        year = f"20{year}"
    try:
        month_num = int(month)
        day_num = int(day)
        year_num = int(year)
    except ValueError:
        return None
    if not (1 <= month_num <= 12 and 1 <= day_num <= 31 and 1900 <= year_num <= 2100):
        return None
    return f"{year_num:04d}-{month_num:02d}-{day_num:02d}"


def extract_period_covered(text: str) -> tuple[str | None, str | None]:
    match = PERIOD_COVERED_RE.search(text)
    if not match:
        return None, None
    return (
        normalize_iso_date(match.group("start")),
        normalize_iso_date(match.group("end")),
    )


def is_section_header(line: str) -> bool:
    upper = line.upper()
    return any(marker in upper for marker in SECTION_A_HEADER_MARKERS)


def is_section_stop(line: str) -> bool:
    upper = line.upper()
    return any(marker in upper for marker in SECTION_STOP_MARKERS)


def should_skip_line(line: str) -> bool:
    if not line:
        return True
    upper = line.upper()
    if any(upper.startswith(prefix) for prefix in SECTION_SKIP_PREFIXES):
        return True
    return False


def iter_section_a_lines(pdf_bytes: bytes) -> tuple[list[str], str]:
    reader = PdfReader(BytesIO(pdf_bytes))
    section_lines: list[str] = []
    full_text_parts: list[str] = []
    in_section = False

    for page in reader.pages:
        text = clean_pdf_text(page.extract_text() or "")
        full_text_parts.append(text)
        for raw_line in text.splitlines():
            line = normalize_line(raw_line)
            if not line:
                continue
            if not in_section and is_section_header(line):
                in_section = True
                continue
            if not in_section:
                continue
            if is_section_stop(line):
                in_section = False
                continue
            if should_skip_line(line):
                continue
            section_lines.append(line)

    return section_lines, "\n".join(full_text_parts)


def build_asset_blocks(lines: Iterable[str]) -> list[str]:
    normalized_lines = [normalize_line(line) for line in lines if normalize_line(line)]
    blocks: list[str] = []
    pending_prefix: list[str] = []
    current_lines: list[str] = []

    for index, line in enumerate(normalized_lines):
        has_asset_type = bool(ASSET_TYPE_RE.search(line))
        next_line = normalized_lines[index + 1] if index + 1 < len(normalized_lines) else ""
        if has_asset_type:
            combined_line = " ".join([*pending_prefix, line]).strip()
            pending_prefix = []
            if current_lines:
                blocks.append(" ".join(current_lines))
            current_lines = [combined_line]
            continue

        if current_lines:
            if looks_like_asset_name_prefix(line) and next_line and ASSET_TYPE_RE.search(next_line):
                blocks.append(" ".join(current_lines))
                current_lines = []
                pending_prefix = [line]
                continue
            current_lines.append(line)
        else:
            if looks_like_asset_name_prefix(line):
                pending_prefix.append(line)

    if current_lines:
        blocks.append(" ".join(current_lines))

    return blocks


def looks_like_asset_name_prefix(line: str) -> bool:
    upper = line.upper()
    if not line or "[" in line:
        return False
    if VALUE_RANGE_RE.search(line):
        return False
    if any(hint in upper for hint in BLOCK_METADATA_HINTS):
        return False
    if upper.startswith(("LOCATION:", "DESCRIPTION:", "FILED", "SOURCE TYPE", "OWNER CREDITOR", "POSITION NAME")):
        return False
    return True


def extract_ticker(asset_name: str) -> str | None:
    matches = TICKER_RE.findall(asset_name)
    if not matches:
        return None
    candidate = matches[-1].upper()
    if candidate in {"US", "ADR", "PLC", "LLC"}:
        return None
    return candidate


def parse_asset_block(block: str) -> HouseDisclosureHolding | None:
    asset_match = ASSET_TYPE_RE.search(block)
    if not asset_match:
        return None

    asset_name = normalize_line(block[: asset_match.start()])
    if not asset_name:
        return None

    after = normalize_line(block[asset_match.end() :])
    value_match = VALUE_RANGE_RE.search(after)
    if not value_match:
        return None

    owner_match = OWNER_RE.match(after)
    owner = owner_match.group("owner").upper() if owner_match else None
    value_range = normalize_line(value_match.group(0)).replace(" - ", " - ")

    return HouseDisclosureHolding(
        asset_name=asset_name,
        asset_type_code=asset_match.group("asset_type").upper(),
        ticker=extract_ticker(asset_name),
        owner=owner,
        value_range=value_range,
    )


def parse_house_financial_disclosure(pdf_bytes: bytes) -> HouseDisclosureDocument:
    section_lines, full_text = iter_section_a_lines(pdf_bytes)
    blocks = build_asset_blocks(section_lines)
    holdings = [holding for block in blocks if (holding := parse_asset_block(block))]
    period_start, period_end = extract_period_covered(full_text)
    return HouseDisclosureDocument(
        period_covered_start=period_start,
        period_covered_end=period_end,
        holdings=holdings,
    )

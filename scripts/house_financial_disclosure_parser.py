from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable

from pypdf import PdfReader

CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
WHITESPACE_RE = re.compile(r"\s+")
ASSET_TYPE_RE = re.compile(r"\[(?P<asset_type>[A-Z0-9]{2})\]")
PERIOD_COVERED_RE = re.compile(
    r"Period Covered:\s*(?P<start>\d{1,2}/\d{1,2}/\d{4})\s*[–-]\s*(?P<end>\d{1,2}/\d{1,2}/\d{4})",
    re.IGNORECASE,
)
TICKER_RE = re.compile(r"\(([A-Za-z][A-Za-z0-9.\-]{0,9})\)")
STANDARD_VALUE_UPPER = {1001:15000,15001:50000,50001:100000,100001:250000,250001:500000,500001:1000000,1000001:5000000,5000001:25000000,25000001:50000000}
VALUE_RANGE_RE = re.compile(
    r"(Spouse/DC\s+Over\s+\$[0-9,]+|Over\s+\$[0-9,]+|Under\s+\$[0-9,]+|\$[0-9,]+\s*-\s*\$[0-9,]+|None|Undetermined)",
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
    # House PDFs often use a font whose missing glyphs leave "S B: T"
    # after control-character cleanup. This still marks Schedule B, not assets.
    return bool(re.match(r"^(?:S|SCHEDULE|SECTION)\s+[B-J]\s*:", upper)) or any(marker in upper for marker in SECTION_STOP_MARKERS)


def should_skip_line(line: str) -> bool:
    if not line or line in {'$1,000?', '$200?'}:
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
    source_lines = [normalize_line(line) for line in lines if normalize_line(line)]
    normalized_lines = []
    cursor = 0
    while cursor < len(source_lines):
        line = source_lines[cursor]
        typed = ASSET_TYPE_RE.search(line)
        if typed and cursor + 1 < len(source_lines):
            tail = line[typed.end():]
            partial = re.fullmatch(r"\s*(?:(SP|JT|DC|S|D|C)\s+)?\$([\d,]+)\s*-\s*(.*)", tail)
            upper = re.match(r"^\$([\d,]+)(?:\s|$)", source_lines[cursor + 1])
            if partial and not partial[3].startswith('$') and upper and int(upper[1].replace(',', '')) == STANDARD_VALUE_UPPER.get(int(partial[2].replace(',', ''))):
                continuation = source_lines[cursor + 1][upper.end():]
                normalized_lines.append(f"{line[:typed.end()]} {partial[1] or ''} ${partial[2]} - ${upper[1]} {partial[3]} {continuation}")
                cursor += 2
                continue
        normalized_lines.append(line)
        cursor += 1
    # An account row can end a page with its value, while its asset name/type
    # starts the next page. Rejoin only an explicit account-arrow + full value
    # followed immediately by a typed asset with no fields after its type.
    joined: list[str] = []
    cursor = 0
    while cursor < len(normalized_lines):
        line = normalized_lines[cursor]
        # A split value column is recoverable only with an account anchor and
        # an exact standard disclosure bracket. Never take an income-column
        # number as the asset value or combine different asset rows.
        partial = re.match(r"^(.*?)⇒\s*(?:(SP|JT|DC|S|D|C)\s+)?\$([\d,]+)\s*-\s*([^$]*)", line)
        standard_upper = STANDARD_VALUE_UPPER
        repaired = False
        if partial and not ASSET_TYPE_RE.search(line) and not VALUE_RANGE_RE.match(line[partial.start(3)-1:]):
            lower = int(partial[3].replace(',', ''))
            name_parts = []
            for offset in range(1, 4):
                at = cursor + offset
                if at >= len(normalized_lines): break
                part = normalized_lines[at]
                if '⇒' in part: break
                typed = ASSET_TYPE_RE.search(part)
                if not typed:
                    if '$' in part: break
                    name_parts.append(part)
                    continue
                name_parts.append(part[:typed.end()])
                tail = part[typed.end():].strip()
                end = at
                if not tail and at + 1 < len(normalized_lines):
                    end = at + 1
                    tail = normalized_lines[end]
                upper = re.match(r"^\$([\d,]+)(?![\d,])(?:\s|$)", tail)
                if upper and int(upper[1].replace(',', '')) == standard_upper.get(lower):
                    joined.append(f"{partial[1]}⇒ {' '.join(name_parts)} {partial[2] or ''} ${partial[3]} - ${upper[1]} {tail[upper.end():]}")
                    cursor = end + 1
                    repaired = True
                break
        if repaired:
            continue
        # Complete values can precede a wrapped name, too. Match the first
        # column after the account marker, never a later income bracket.
        full = re.search(r"⇒\s*(?:(SP|JT|DC|S|D|C)\s+)?(" + VALUE_RANGE_RE.pattern + r")", line, re.I)
        repaired = False
        if full and not ASSET_TYPE_RE.search(line):
            name_parts = []
            for offset in range(1, 4):
                at = cursor + offset
                if at >= len(normalized_lines): break
                part = normalized_lines[at]
                if '⇒' in part: break
                typed = ASSET_TYPE_RE.search(part)
                if not typed:
                    if '$' in part: break
                    name_parts.append(part)
                    continue
                tail = part[typed.end():].strip()
                if not VALUE_RANGE_RE.search(tail):
                    name_parts.append(part[:typed.end()])
                    prefix = line[:full.start()] + '⇒'
                    joined.append(f"{prefix} {' '.join(name_parts)} {full[1] or ''} {full[2]} {line[full.end():]} {tail}")
                    cursor = at + 1
                    repaired = True
                break
        if not repaired:
            joined.append(line)
            cursor += 1
    normalized_lines = joined
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
            if line.endswith('⇒'):
                blocks.append(' '.join(current_lines))
                current_lines = []
                pending_prefix = [line]
                continue
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
    # Dollar continuations belong to the current row, never the next asset name.
    if re.match(r"^(?:(?:SP|JT|DC|S|D|C)\s+)?\$", line, re.I):
        return False
    if VALUE_RANGE_RE.search(line):
        return False
    if any(hint in upper for hint in BLOCK_METADATA_HINTS):
        return False
    if upper.startswith(("LOCATION:", "DESCRIPTION:", "L:", "D:", "FILED", "SOURCE TYPE", "OWNER CREDITOR", "POSITION NAME")):
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
    owner_match = OWNER_RE.match(after)
    owner = owner_match.group("owner").upper() if owner_match else None
    value_column = after[owner_match.end():].strip() if owner_match else after
    value_match = VALUE_RANGE_RE.match(value_column)
    if not value_match:
        return None
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

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import csv
import io
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env.local")


def env_value(key: str) -> str:
    for line in Path(".env.local").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key == key:
            return value.strip().strip('"').strip("'")
    return ""


supabase = create_client(
    env_value("SUPABASE_URL") or env_value("NEXT_PUBLIC_SUPABASE_URL"),
    env_value("SUPABASE_SERVICE_KEY") or env_value("SUPABASE_SERVICE_ROLE_KEY"),
)

HOUSE_INDEX_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt"
HOUSE_FINANCIAL_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}/{doc_id}.pdf"
HOUSE_PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

FILING_TYPE_LABELS = {
    "P": "Periodic Transaction Report",
    "C": "Candidate Report",
    "H": "New Filer Report",
    "A": "Annual Report",
    "O": "Annual Report (Officeholder)",
    "W": "Annual Report (Amendment/Waiver)",
    "X": "Extension/Other",
    "D": "Termination Report",
    "T": "Termination Report (Amendment)",
    "E": "Extension",
    "G": "Gift/Travel",
    "B": "Blind Trust / Other",
}


@dataclass
class MemberIdentity:
    member_id: str
    first_name: str
    last_name: str


def load_member(member_id: str) -> MemberIdentity:
    rows = (
        supabase.table("congress_members")
        .select("id,first_name,last_name")
        .eq("id", member_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        raise SystemExit(f"Unknown congress member id: {member_id}")
    row = rows[0]
    return MemberIdentity(
        member_id=row["id"],
        first_name=str(row.get("first_name") or "").strip(),
        last_name=str(row.get("last_name") or "").strip(),
    )


def normalize_name(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def load_house_index(year: int) -> list[dict]:
    response = requests.get(HOUSE_INDEX_URL.format(year=year), timeout=30)
    response.raise_for_status()
    payload = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
    return list(reader)


def filing_pdf_url(year: int, doc_id: str, filing_type: str) -> str:
    if filing_type == "P":
        return HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
    return HOUSE_FINANCIAL_PDF_URL.format(year=year, doc_id=doc_id)


def find_member_filings(identity: MemberIdentity, years: list[int]) -> list[dict]:
    matches: list[dict] = []
    first = normalize_name(identity.first_name)
    last = normalize_name(identity.last_name)

    for year in years:
        for row in load_house_index(year):
            row_first = normalize_name(str(row.get("First") or ""))
            row_last = normalize_name(str(row.get("Last") or ""))
            if row_first != first or row_last != last:
                continue
            filing_type = str(row.get("FilingType") or "").strip().upper()
            doc_id = str(row.get("DocID") or "").strip()
            filing_date = str(row.get("FilingDate") or "").strip()
            matches.append(
                {
                    "member_id": identity.member_id,
                    "year": year,
                    "doc_id": doc_id,
                    "filing_type": filing_type,
                    "filing_type_label": FILING_TYPE_LABELS.get(filing_type, "Unknown"),
                    "filing_date": filing_date,
                    "state_district": str(row.get("StateDst") or "").strip(),
                    "pdf_url": filing_pdf_url(year, doc_id, filing_type),
                }
            )
    matches.sort(key=lambda row: (row["year"], row["filing_date"], row["doc_id"]))
    return matches


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit House disclosure coverage for a single member across all filing types.")
    parser.add_argument("--member-id", required=True, help="Bioguide / congress_members id, e.g. B001325")
    parser.add_argument("--start-year", type=int, default=2024)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument(
        "--artifact",
        type=Path,
        default=None,
        help="Optional path to write the full JSON audit result.",
    )
    args = parser.parse_args()

    identity = load_member(args.member_id)
    years = list(range(args.start_year, args.end_year + 1))
    filings = find_member_filings(identity, years)
    type_counts = Counter(filing["filing_type"] for filing in filings)

    result = {
        "member_id": identity.member_id,
        "first_name": identity.first_name,
        "last_name": identity.last_name,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "filing_count": len(filings),
        "filing_type_counts": dict(sorted(type_counts.items())),
        "filings": filings,
    }

    if args.artifact:
        args.artifact.parent.mkdir(parents=True, exist_ok=True)
        args.artifact.write_text(json.dumps(result, indent=2, sort_keys=True))
        print(f"ARTIFACT {args.artifact}")

    print("SUMMARY_JSON:" + json.dumps({k: v for k, v in result.items() if k != "filings"}, sort_keys=True))
    for filing in filings:
        print(
            f"{filing['year']} {filing['filing_type']} {filing['doc_id']} "
            f"{filing['filing_date']} {filing['filing_type_label']} {filing['pdf_url']}"
        )


if __name__ == "__main__":
    main()

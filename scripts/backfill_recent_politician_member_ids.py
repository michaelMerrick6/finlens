import json
import os
import re
import unicodedata
from datetime import timedelta

from pipeline_support import get_supabase_client
from time_utils import congress_today


LOOKBACK_DAYS = int(os.environ.get("POLITICIAN_MEMBER_BACKFILL_DAYS", "120"))
PAGE_SIZE = 500
COMMON_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
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


def ascii_fold(value: str) -> str:
    return unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")


def normalize_name_tokens(value: str) -> list[str]:
    normalized = ascii_fold(value).lower()
    return [token for token in re.findall(r"[a-z]+", normalized) if token not in COMMON_NAME_SUFFIXES]


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
            if min(len(filed_token), len(member_token)) >= 3 and (
                filed_token.startswith(member_token) or member_token.startswith(filed_token)
            ):
                return True
    return False


def chamber_matches(member: dict, chamber: str) -> bool:
    member_chamber = (member.get("chamber") or "").strip().lower()
    if not member_chamber or member_chamber == "both":
        return True
    return member_chamber == chamber.lower()


def is_placeholder_member(member: dict) -> bool:
    return str(member.get("id") or "").startswith("unknown-")


def suggest_member_id(politician_name: str, chamber: str, members: list[dict]) -> str | None:
    tokens = normalize_name_tokens(politician_name)
    if len(tokens) < 2:
        return None

    exact_last_name_candidates = []
    active_candidates = []
    for member in members:
        if is_placeholder_member(member):
            continue
        if not chamber_matches(member, chamber):
            continue
        member_last_tokens = normalize_name_tokens(member.get("last_name") or "")
        if not member_last_tokens:
            continue
        if len(tokens) <= len(member_last_tokens):
            continue
        if tokens[-len(member_last_tokens) :] != member_last_tokens:
            continue
        first_tokens = tokens[: -len(member_last_tokens)]
        exact_last_name_candidates.append(member)
        if member.get("active") is not False:
            active_candidates.append(member)
        if member.get("active") is not False and first_name_tokens_match(first_tokens, member.get("first_name") or ""):
            return member["id"]

    if len(active_candidates) == 1:
        return active_candidates[0]["id"]
    if len(exact_last_name_candidates) == 1:
        return exact_last_name_candidates[0]["id"]
    return None


def fetch_recent_unknown_rows(supabase) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    offset = 0
    rows: list[dict] = []
    while True:
        batch = (
            supabase.table("politician_trades")
            .select("id, politician_name, member_id, chamber, published_date, doc_id")
            .gte("published_date", cutoff)
            .like("member_id", "unknown-%")
            .order("published_date", desc=True)
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data
            or []
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def main() -> None:
    supabase = get_supabase_client()
    members = (
        supabase.table("congress_members")
        .select("id, first_name, last_name, chamber, active, state, party")
        .execute()
        .data
        or []
    )
    unknown_rows = fetch_recent_unknown_rows(supabase)

    updated = 0
    skipped = 0
    updated_doc_ids: list[str] = []
    for row in unknown_rows:
        suggested = suggest_member_id(row.get("politician_name") or "", row.get("chamber") or "", members)
        if not suggested or suggested == row.get("member_id"):
            skipped += 1
            continue

        supabase.table("politician_trades").update({"member_id": suggested}).eq("id", row["id"]).execute()
        updated += 1
        updated_doc_ids.append(row.get("doc_id") or row["id"])

    summary = {
        "lookback_days": LOOKBACK_DAYS,
        "rows_seen": len(unknown_rows),
        "rows_updated": updated,
        "rows_skipped": skipped,
        "updated_doc_ids": updated_doc_ids[:50],
    }
    print("SUMMARY_JSON:" + json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()

import argparse
import json

from pipeline_support import get_supabase_client


def main():
    parser = argparse.ArgumentParser(description="List tweet candidates from the review queue.")
    parser.add_argument("--status", default="pending_review", help="Candidate status filter.")
    parser.add_argument("--limit", type=int, default=20, help="Max rows to fetch.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    response = (
        supabase.table("tweet_candidates")
        .select(
            "id, channel, status, rule_key, score, title, draft_text, rationale, created_at, signal_events(ticker, actor_name, signal_type, source_url)"
        )
        .eq("status", args.status)
        .order("score", desc=True)
        .order("created_at", desc=True)
        .limit(args.limit)
        .execute()
    )
    rows = response.data or []
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()

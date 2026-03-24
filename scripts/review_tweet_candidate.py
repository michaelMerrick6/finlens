import argparse
from datetime import datetime, timezone

from pipeline_support import get_supabase_client


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Review a tweet candidate.")
    parser.add_argument("--id", required=True, help="Tweet candidate id.")
    parser.add_argument(
        "--status",
        required=True,
        choices=["pending_review", "approved", "rejected", "posted"],
        help="New candidate status.",
    )
    parser.add_argument("--reviewed-by", default="manual", help="Reviewer key.")
    parser.add_argument("--notes", default="", help="Optional review notes.")
    parser.add_argument("--external-post-id", default="", help="Optional platform post id when marking posted.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    payload = {
        "status": args.status,
        "reviewed_by": args.reviewed_by,
        "review_notes": args.notes,
        "reviewed_at": utc_now_iso(),
    }
    if args.status == "posted":
        payload["posted_at"] = utc_now_iso()
        if args.external_post_id:
            payload["external_post_id"] = args.external_post_id

    supabase.table("tweet_candidates").update(payload).eq("id", args.id).execute()
    print(
        {
            "id": args.id,
            "status": args.status,
            "reviewed_by": args.reviewed_by,
            "notes": args.notes,
            "external_post_id": args.external_post_id or None,
        }
    )


if __name__ == "__main__":
    main()

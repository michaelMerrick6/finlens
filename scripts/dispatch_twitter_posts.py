import argparse
import os
from datetime import datetime, timezone

from pipeline_support import emit_summary, get_supabase_client
from twitter_api_support import TwitterApiError, create_post, twitter_posting_config


DEFAULT_BATCH_SIZE = int(os.environ.get("TWITTER_POST_BATCH_SIZE", "5"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_approved_candidates(supabase, *, candidate_id: str | None, batch_size: int) -> list[dict]:
    query = (
        supabase.table("tweet_candidates")
        .select("id, channel, status, title, draft_text, review_notes, reviewed_by, signal_event_id, created_at")
        .eq("channel", "twitter")
        .eq("status", "approved")
        .order("created_at", desc=False)
    )
    if candidate_id:
        query = query.eq("id", candidate_id).limit(1)
    else:
        query = query.limit(batch_size)
    response = query.execute()
    return response.data or []


def append_post_error_note(existing_notes: str | None, message: str) -> str:
    clean_message = str(message or "").strip()
    if not clean_message:
        return existing_notes or ""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    failure_line = f"[{timestamp}] X post failed: {clean_message}"
    if not existing_notes:
        return failure_line
    if failure_line in existing_notes:
        return existing_notes
    return f"{existing_notes.rstrip()}\n{failure_line}"


def mark_candidate_posted(supabase, candidate_id: str, *, external_post_id: str) -> None:
    supabase.table("tweet_candidates").update(
        {
            "status": "posted",
            "posted_at": utc_now_iso(),
            "external_post_id": external_post_id,
            "reviewed_at": utc_now_iso(),
        }
    ).eq("id", candidate_id).execute()
    verify = supabase.table("tweet_candidates").select("id,status,external_post_id").eq("id", candidate_id).limit(1).execute()
    row = (verify.data or [None])[0]
    if not row or row.get("status") != "posted":
        raise RuntimeError(f"Failed to persist posted status for candidate {candidate_id}.")


def note_candidate_failure(supabase, candidate: dict, message: str) -> None:
    supabase.table("tweet_candidates").update(
        {
            "review_notes": append_post_error_note(candidate.get("review_notes"), message),
            "reviewed_at": utc_now_iso(),
        }
    ).eq("id", candidate["id"]).execute()
    verify = supabase.table("tweet_candidates").select("id").eq("id", candidate["id"]).limit(1).execute()
    if not (verify.data or []):
        raise RuntimeError(f"Failed to persist failure note for candidate {candidate['id']}.")


def main():
    parser = argparse.ArgumentParser(description="Dispatch approved X/Twitter post candidates.")
    parser.add_argument("--id", default="", help="Optional tweet candidate id to publish.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Maximum approved candidates to post.")
    parser.add_argument("--dry-run", action="store_true", help="Do not publish, just simulate successful posting.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    config = twitter_posting_config()
    candidates = fetch_approved_candidates(
        supabase,
        candidate_id=args.id.strip() or None,
        batch_size=max(1, int(args.batch_size or DEFAULT_BATCH_SIZE)),
    )

    summary = {
        "x_posting_enabled": bool(config["enabled"]),
        "x_configured": bool(config["configured"]),
        "x_auth_mode": config["auth_mode"],
        "candidates_seen": len(candidates),
        "candidates_posted": 0,
        "candidates_simulated": 0,
        "candidates_failed": 0,
        "failure_details": [],
        "dry_run": bool(args.dry_run),
    }

    if not candidates:
        emit_summary(summary)
        print("No approved X candidates to dispatch.")
        return

    if not config["enabled"]:
        summary["reason"] = "x_posting_disabled"
        emit_summary(summary)
        print("X dispatch skipped: TWITTER_POSTING_ENABLED is not enabled.")
        return

    if not config["configured"]:
        summary["reason"] = "x_posting_not_configured"
        emit_summary(summary)
        print("X dispatch skipped: credentials are not configured.")
        return

    for candidate in candidates:
        try:
            if args.dry_run:
                summary["candidates_simulated"] += 1
            else:
                response = create_post(candidate.get("draft_text") or "")
                post_id = str(response.get("id") or "").strip()
                if not post_id:
                    raise TwitterApiError("X API returned an empty post id.")
                mark_candidate_posted(supabase, candidate["id"], external_post_id=post_id)
                summary["candidates_posted"] += 1
        except Exception as exc:
            error_message = str(exc)
            note_candidate_failure(supabase, candidate, error_message)
            summary["candidates_failed"] += 1
            summary["failure_details"].append(
                {
                    "candidate_id": candidate["id"],
                    "title": candidate.get("title"),
                    "error": error_message,
                }
            )

    emit_summary(summary)
    print(
        f"X dispatch complete: {summary['candidates_posted']} posted, {summary['candidates_simulated']} simulated, {summary['candidates_failed']} failed."
    )


if __name__ == "__main__":
    main()

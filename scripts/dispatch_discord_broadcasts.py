import os
import argparse
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from alert_delivery_support import build_curated_discord_candidate_payload
from pipeline_support import emit_summary, get_supabase_client


DISCORD_BROADCAST_BATCH_SIZE = int(os.environ.get("DISCORD_BROADCAST_BATCH_SIZE", "10"))
DISCORD_BROADCAST_CHANNEL = "discord_premium"
GLOBAL_WEBHOOK_URL = os.environ.get("DISCORD_GLOBAL_WEBHOOK_URL", os.environ.get("DISCORD_WEBHOOK_URL", "")).strip()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def waitable_webhook_url(url: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["wait"] = "true"
    return urlunparse(parsed._replace(query=urlencode(query)))


def fetch_approved_candidates(supabase, *, candidate_id: str | None, batch_size: int) -> list[dict]:
    query = (
        supabase.table("tweet_candidates")
        .select(
            "id, channel, status, title, draft_text, rationale, review_notes, reviewed_by, score, signal_event_id, signal_events(ticker,actor_name,signal_type,source,source_url,direction)"
        )
        .eq("channel", DISCORD_BROADCAST_CHANNEL)
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
    failure_line = f"[{timestamp}] Discord send failed: {clean_message}"
    if not existing_notes:
        return failure_line
    if failure_line in existing_notes:
        return existing_notes
    return f"{existing_notes.rstrip()}\n{failure_line}"


def mark_candidate_posted(supabase, candidate_id: str, *, external_post_id: str | None = None) -> None:
    payload = {
        "status": "posted",
        "posted_at": utc_now_iso(),
        "reviewed_at": utc_now_iso(),
    }
    if external_post_id:
        payload["external_post_id"] = external_post_id
    supabase.table("tweet_candidates").update(payload).eq("id", candidate_id).execute()
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


def send_webhook(candidate: dict) -> str | None:
    if not GLOBAL_WEBHOOK_URL:
        raise RuntimeError("DISCORD_GLOBAL_WEBHOOK_URL is not configured.")

    payload = build_curated_discord_candidate_payload(candidate, candidate.get("signal_events") or {})
    response = requests.post(waitable_webhook_url(GLOBAL_WEBHOOK_URL), json=payload, timeout=15)
    response.raise_for_status()
    try:
        body = response.json()
    except ValueError:
        return None
    return str(body.get("id") or "").strip() or None


def main():
    parser = argparse.ArgumentParser(description="Dispatch approved premium Discord broadcast candidates.")
    parser.add_argument("--id", default="", help="Optional broadcast candidate id to send.")
    args = parser.parse_args()

    print("Dispatching curated Discord broadcasts...")
    supabase = get_supabase_client()
    candidate_id = str(args.id or "").strip() or str(os.environ.get("DISCORD_BROADCAST_TARGET_ID") or "").strip() or None
    candidates = fetch_approved_candidates(
        supabase,
        candidate_id=candidate_id,
        batch_size=DISCORD_BROADCAST_BATCH_SIZE,
    )
    summary = {
        "discord_broadcast_configured": bool(GLOBAL_WEBHOOK_URL),
        "candidates_seen": len(candidates),
        "candidates_posted": 0,
        "candidates_failed": 0,
        "failure_details": [],
    }

    if not candidates:
        emit_summary(summary)
        print("No approved Discord broadcast candidates to dispatch.")
        return

    if not GLOBAL_WEBHOOK_URL:
        summary["reason"] = "discord_global_webhook_missing"
        emit_summary(summary)
        print("Discord broadcast dispatch skipped: missing DISCORD_GLOBAL_WEBHOOK_URL.")
        return

    for candidate in candidates:
        try:
            external_post_id = send_webhook(candidate)
            mark_candidate_posted(supabase, candidate["id"], external_post_id=external_post_id)
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
    print(f"Curated Discord dispatch complete: {summary['candidates_posted']} posted, {summary['candidates_failed']} failed.")


if __name__ == "__main__":
    main()

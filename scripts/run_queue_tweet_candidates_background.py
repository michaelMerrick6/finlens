import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def update_running_state(path: Path, running_state: dict, **updates) -> None:
    next_state = dict(running_state)
    next_state.update(updates)
    next_state["updated_at"] = iso_now()
    write_state(path, next_state)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run tweet candidate queue in the background and persist status.")
    parser.add_argument("--state-path", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--repo-root", required=True)
    args = parser.parse_args()

    state_path = Path(args.state_path)
    log_path = Path(args.log_path)
    repo_root = Path(args.repo_root)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    running_state = {
        "status": "running",
        "started_at": iso_now(),
        "pid": os.getpid(),
        "log_path": str(log_path),
        "current_step": "Starting queue worker",
        "progress_percent": 0,
    }
    write_state(state_path, running_state)
    log_path.write_text(f"started_at={running_state['started_at']}\n\n")

    process = subprocess.Popen(
        ["python3", str(repo_root / "scripts" / "queue_tweet_candidates.py")],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    summary = None
    with log_path.open("a", encoding="utf-8") as log_file:
        assert process.stdout is not None
        for raw_line in process.stdout:
            log_file.write(raw_line)
            log_file.flush()
            line = raw_line.strip()
            if line.startswith("PROGRESS_JSON:"):
                try:
                    progress = json.loads(line.replace("PROGRESS_JSON:", "", 1).strip())
                except json.JSONDecodeError:
                    continue
                update_running_state(
                    state_path,
                    running_state,
                    current_step=progress.get("step") or running_state.get("current_step"),
                    progress_percent=progress.get("progress_percent"),
                    progress=progress,
                )
                continue
            if line.startswith("SUMMARY_JSON:"):
                try:
                    summary = json.loads(line.replace("SUMMARY_JSON:", "", 1).strip())
                except json.JSONDecodeError:
                    summary = None

        result_code = process.wait()
        log_file.write(f"\nfinished_at={iso_now()}\n")

    completed_state = {
        "status": "completed" if result_code == 0 else "failed",
        "started_at": running_state["started_at"],
        "finished_at": iso_now(),
        "pid": running_state["pid"],
        "log_path": str(log_path),
        "exit_code": result_code,
        "summary": summary,
        "stderr": None if result_code == 0 else "Queue runner failed. See log for details.",
        "current_step": "Completed" if result_code == 0 else "Failed",
        "progress_percent": 100 if result_code == 0 else running_state.get("progress_percent"),
    }
    write_state(state_path, completed_state)
    return result_code


if __name__ == "__main__":
    raise SystemExit(main())

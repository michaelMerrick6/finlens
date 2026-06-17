from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill all available 13F quarter history for tracked funds.")
    parser.add_argument("--max-filings-per-fund", type=int, default=0, help="0 means no filing cap.")
    parser.add_argument("--max-periods-per-fund", type=int, default=0, help="0 means no period cap.")
    parser.add_argument("--skip-audit", action="store_true", help="Skip the post-ingest audit pass.")
    args = parser.parse_args()

    ops_dir = Path(__file__).resolve().parent
    root_dir = ops_dir.parent
    scripts_dir = root_dir / "scripts"
    env = os.environ.copy()
    env["SEC_13F_MAX_FILINGS_PER_FUND"] = str(args.max_filings_per_fund)
    env["SEC_13F_MAX_PERIODS_PER_FUND"] = str(args.max_periods_per_fund)

    print(
        "Backfilling SEC 13F history with "
        f"max_filings_per_fund={args.max_filings_per_fund} "
        f"max_periods_per_fund={args.max_periods_per_fund}"
    )
    subprocess.run([sys.executable, str(scripts_dir / "ingest_sec_13f.py")], check=True, env=env)

    if not args.skip_audit:
        subprocess.run([sys.executable, str(ops_dir / "audit_recent_13f_coverage.py")], check=True, env=env)


if __name__ == "__main__":
    main()

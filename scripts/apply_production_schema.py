import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
PRODUCTION_SQL_FILES = [
    ROOT_DIR / "supabase_schema.sql",
    ROOT_DIR / "supabase_vail_phase1.sql",
    ROOT_DIR / "supabase_vail_phase2_notifications.sql",
    ROOT_DIR / "supabase_vail_phase4_follow_modes.sql",
    ROOT_DIR / "supabase_vail_phase5_user_accounts.sql",
    ROOT_DIR / "supabase_vail_phase6_billing.sql",
    ROOT_DIR / "supabase_vail_phase7_cluster_alerts.sql",
    ROOT_DIR / "supabase_vail_phase8_cluster_alert_channels.sql",
    ROOT_DIR / "ops" / "sql" / "supabase_pipeline_performance.sql",
]


def main() -> int:
    load_dotenv(ROOT_DIR / ".env.local")

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        print("DATABASE_URL is not set. Add it to .env.local or export it before running this script.")
        return 1

    for sql_file in PRODUCTION_SQL_FILES:
        if not sql_file.exists():
            print(f"Missing required SQL file: {sql_file}")
            return 1

        print(f"Applying {sql_file.relative_to(ROOT_DIR)}")
        result = subprocess.run(
            ["psql", database_url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_file)],
            text=True,
        )
        if result.returncode != 0:
            return result.returncode

    print("Production schema applied successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

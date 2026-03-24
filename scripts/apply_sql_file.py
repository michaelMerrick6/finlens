import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


def main() -> int:
    load_dotenv(".env.local")
    if len(sys.argv) != 2:
        print("Usage: python3 scripts/apply_sql_file.py <sql-file>")
        return 1

    sql_file = Path(sys.argv[1]).resolve()
    if not sql_file.exists():
        print(f"SQL file not found: {sql_file}")
        return 1

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        print("DATABASE_URL is not set. Add it to .env.local or export it before running this script.")
        return 1

    result = subprocess.run(
        ["psql", database_url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_file)],
        text=True,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

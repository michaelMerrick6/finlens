"""Run every self-contained pipeline test with the active Python runtime."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def main() -> int:
    scripts_dir = Path(__file__).resolve().parent
    repo_root = scripts_dir.parent
    test_files = sorted(scripts_dir.glob("test_*.py"))
    env = os.environ.copy()
    python_paths = [str(scripts_dir), str(repo_root / "ops")]
    if env.get("PYTHONPATH"):
        python_paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_paths)

    failures: list[str] = []
    for test_file in test_files:
        print(f"\n=== {test_file.name} ===", flush=True)
        result = subprocess.run(
            [sys.executable, str(test_file)],
            cwd=repo_root,
            env=env,
            check=False,
        )
        if result.returncode != 0:
            failures.append(test_file.name)

    if failures:
        print(f"\nFailed test files: {', '.join(failures)}", file=sys.stderr)
        return 1

    print(f"\nAll {len(test_files)} pipeline test files passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import os
import sys
import subprocess
from datetime import datetime

# Unified logging output
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def run_script(script_path: str, critical: bool = True):
    log(f"--- Executing: {script_path} ---")
    try:
        python_exe = os.environ.get("PYTHON_EXE", "python") if sys.platform == "linux" else sys.executable
        result = subprocess.run([python_exe, script_path], check=True, text=True, capture_output=True, timeout=600)
        print(result.stdout)
        log(f"--- Completed: {script_path} ---")
        return True
    except subprocess.CalledProcessError as e:
        log(f"!!! Error executing {script_path} !!!")
        print(e.stdout)
        print(e.stderr)
        if critical:
            sys.exit(1)
        return False
    except subprocess.TimeoutExpired:
        log(f"!!! Timeout executing {script_path} (10 min limit) !!!")
        return False

def main():
    log("Started Daily FinLens Scraper Orchestration Pipe")
    start = datetime.now()
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Daily mode: only check current year + last year for PDF scrapers
    os.environ["FINLENS_DAILY_MODE"] = "1"
    
    results = {}
    
    # 1) Official House PDF scraper (electronic filings)
    results["House PDFs"] = run_script(
        os.path.join(base_dir, "ingest_house_official.py"), critical=False
    )
    
    # 2) Official Senate scraper
    results["Senate"] = run_script(
        os.path.join(base_dir, "ingest_senate_official.py"), critical=False
    )
    
    # 3) Capitol Trades (SUPPLEMENTAL only — catches handwritten filers that PDFs can't parse)
    #    Official sources above are primary. This is a fallback for image-only PDFs.
    results["Capitol Trades (supplemental)"] = run_script(
        os.path.join(base_dir, "daily_capitol_trades.py"), critical=False
    )
    
    # 4) SEC Edgar (Form 4 insider trades - Daily Lightweight)
    results["SEC Edgar"] = run_script(
        os.path.join(base_dir, "ingest_sec_daily.py"), critical=False
    )
    
    elapsed = (datetime.now() - start).total_seconds()
    log(f"\n{'='*50}")
    log(f"DAILY PIPELINE COMPLETE ({elapsed:.0f}s)")
    for name, success in results.items():
        status = "✅" if success else "❌"
        log(f"  {status} {name}")
    log(f"{'='*50}")
    
    # Fail if ALL scrapers failed
    if not any(results.values()):
        log("All scrapers failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

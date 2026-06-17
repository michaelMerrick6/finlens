import argparse

from pipeline_support import emit_summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Disabled. Congress trade rows must now come only from official House and Senate filings."
    )
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--limit", type=int, default=500)
    parser.parse_args()

    emit_summary(
        {
            "status": "skipped",
            "reason": "official_source_only_policy",
            "sources_seen": 0,
            "sources_replaced": 0,
            "rows_deleted": 0,
            "rows_inserted": 0,
            "parse_failures": 0,
        }
    )

    print(
        "Capitol fallback reconciliation is disabled. "
        "Congress trade rows must be created from official filings only."
    )


if __name__ == "__main__":
    main()

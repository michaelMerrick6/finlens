import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


class SchemaPerformanceIndexesTests(unittest.TestCase):
    def test_base_schema_does_not_drop_trade_tables(self) -> None:
        schema = (ROOT_DIR / "supabase_schema.sql").read_text()

        self.assertNotIn("DROP TABLE IF EXISTS public.politician_trades", schema)
        self.assertNotIn("DROP TABLE IF EXISTS public.insider_trades", schema)
        self.assertNotIn("DROP TABLE IF EXISTS public.institutional_holdings", schema)

    def test_pipeline_performance_migration_covers_hot_queries(self) -> None:
        migration = (ROOT_DIR / "ops" / "sql" / "supabase_pipeline_performance.sql").read_text()
        expected_indexes = {
            "idx_politician_trades_published_date",
            "idx_politician_trades_transaction_date",
            "idx_insider_trades_published_date",
            "idx_insider_trades_transaction_date",
            "idx_institutional_holdings_published_date",
            "idx_institutional_holdings_report_period",
            "idx_institutional_holdings_fund_period",
            "idx_signal_events_created_at",
            "idx_signal_events_type_published_at",
            "idx_signal_events_type_occurred_at",
            "idx_tweet_candidates_cluster_feed",
            "idx_tweet_candidates_cluster_score_feed",
        }

        for index_name in expected_indexes:
            self.assertIn(index_name, migration)


if __name__ == "__main__":
    unittest.main()

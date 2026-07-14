import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


class Schema13FValueUnitTests(unittest.TestCase):
    def test_value_unit_migration_is_guarded_and_updates_every_stored_copy(self) -> None:
        migration = (ROOT_DIR / "supabase_vail_phase10_13f_value_units.sql").read_text()

        self.assertIn("2026-07-14-13f-dollar-values", migration)
        self.assertIn("UPDATE public.institutional_holdings", migration)
        self.assertIn("UPDATE public.signal_events", migration)
        self.assertIn("UPDATE public.raw_filings", migration)
        self.assertIn("value_held = value_held / 1000", migration)


if __name__ == "__main__":
    unittest.main()

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def normalized_sql(filename: str) -> str:
    return re.sub(r"\s+", " ", (ROOT / filename).read_text()).lower()


class SchemaSecurityTests(unittest.TestCase):
    def test_account_mutations_are_service_only(self) -> None:
        migration = normalized_sql("supabase_vail_phase5_user_accounts.sql")

        self.assertNotIn('create policy "profiles self insert"', migration)
        self.assertNotIn('create policy "profiles self update"', migration)
        for policy_name in (
            "auth users read own watchlists",
            "auth users read own ticker follows",
            "auth users read own actor follows",
            "auth users read own subscriptions",
        ):
            self.assertIn(f'create policy "{policy_name}"', migration)

        self.assertNotIn("for all to authenticated", migration)

    def test_stripe_webhook_payloads_are_not_public(self) -> None:
        migration = normalized_sql("supabase_vail_phase6_billing.sql")
        self.assertIn(
            "alter table public.stripe_webhook_events enable row level security",
            migration,
        )


if __name__ == "__main__":
    unittest.main()

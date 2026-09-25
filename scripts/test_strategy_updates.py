import json
import unittest
from unittest.mock import MagicMock, patch

from emit_strategy_updates import INVENTORY, build_events, save_events
from notification_targets import event_actor_match_keys
from alert_rules import classify_event_behavior
from daily_email_digest import digest_events, render_digest
import queue_alert_deliveries as queue

OLD = 'https://www.whitehouse.gov/old.pdf'
NEW = 'https://www.whitehouse.gov/new.pdf'
HTML = f'<a href="{OLD}">President Donald J. Trump Annual Report</a>' + f'<a href="{NEW}"><span>President Donald J. Trump</span> Periodic Transaction Report 09.23.26</a>'


class StrategyUpdatesTests(unittest.TestCase):
    def event(self):
        return dict(build_events(HTML, [OLD], '2026-09-24T10:00:00+00:00')[0], id='new-event')

    def test_historical_sources_are_a_baseline_not_a_notification_backfill(self):
        sources = json.loads(INVENTORY.read_text())['sources']
        baseline = [row['url'] for row in sources if row.get('notificationBaseline')]
        self.assertEqual(len(baseline), 18)
        html = ''.join(f'<a href="{url}">President Donald J. Trump report</a>' for url in baseline)
        self.assertEqual(build_events(html, baseline, 'now'), [])

    def test_only_new_official_trump_documents_and_no_invented_filing_date(self):
        extra = (f'<a href="{NEW}?download=1#page=2">President Donald J. Trump report</a>'
                 '<a href="https://other.example/new.pdf">President Donald J. Trump report</a>'
                 '<a href="/other.pdf">Someone else</a>'
                 '<a href="javascript:alert(1)">President Donald J. Trump report</a>')
        rows = build_events(HTML + extra, [OLD], '2026-09-24T10:00:00+00:00')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['source_document_id'], NEW)
        self.assertIsNone(rows[0]['published_at'])
        self.assertIsNone(rows[0]['occurred_at'])
        self.assertIn('still need review', rows[0]['summary'])

    def test_unrecognized_source_fails_without_emitting_empty_success(self):
        for html in ['', '<a href="/other.pdf">Someone else</a>', 'Service unavailable']:
            with self.assertRaises(ValueError):
                build_events(html, [OLD], 'now')

    def test_repeated_polls_preserve_existing_event_and_detection_time(self):
        db = MagicMock()
        save_events(db, [self.event()])
        args = db.table.return_value.upsert.call_args
        self.assertEqual(args.kwargs, {'on_conflict': 'source,source_document_id', 'ignore_duplicates': True})
        db.reset_mock()
        save_events(db, [])
        db.table.assert_not_called()

    def test_notice_matches_strategy_follow_only_and_uses_activity_mode(self):
        event = self.event()
        self.assertEqual(event_actor_match_keys(event), {'politician:strategy:trump'})
        behavior = classify_event_behavior(event)
        self.assertTrue(behavior['activity'])
        self.assertFalse(behavior['unusual'])
        self.assertFalse(behavior['suppressed'])

    def test_existing_queue_respects_strategy_follows_and_subscription_preferences(self):
        event = self.event()
        follow = dict(watchlist_id='mine', match_type='actor', alert_mode='activity', actor_match_key='politician:strategy:trump')
        follows = {'politician:strategy:trump': [follow, follow]}
        subscription = dict(id='email', watchlist_id='mine', channel='email', destination='test@example.invalid', minimum_importance=.9)
        other = dict(subscription, id='other', watchlist_id='other')
        global_subscription = dict(subscription, id='global', watchlist_id=None, minimum_importance=0)
        deliveries = queue.queue_subscription_deliveries([event, event], [subscription, other, global_subscription], {}, follows)
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]['subscription_id'], 'email')
        self.assertEqual(deliveries[0]['payload']['matched_actor_keys'], ['politician:strategy:trump'])
        self.assertEqual(queue.queue_subscription_deliveries([event], [], {}, follows), [])
        self.assertEqual(queue.queue_subscription_deliveries([event], [subscription], {}, {}), [])
        follow['alert_mode'] = 'unusual'
        self.assertEqual(queue.queue_subscription_deliveries([event], [subscription], {}, follows), [])
        with patch.object(queue, 'GLOBAL_WEBHOOK_URL', 'https://example.invalid'), patch.object(queue, 'GLOBAL_MIN_IMPORTANCE', 0):
            self.assertEqual(queue.queue_global_discord_deliveries([event], []), [])
        self.assertEqual(queue.queue_owner_sms_signal_deliveries([event]), [])

    def test_disabled_subscriptions_are_filtered_by_existing_loader(self):
        db = MagicMock()
        queue.fetch_subscriptions(db)
        db.table.return_value.select.return_value.eq.assert_called_once_with('active', True)

    def test_strategy_notice_joins_the_same_daily_digest_and_labels_detection_honestly(self):
        stock = dict(id='trade', source='congress', source_document_id='trade', actor_name='Nancy Pelosi',
                     signal_type='politician_trade', title='Purchase', ticker='NVDA', summary='Disclosed purchase', payload={})
        events = digest_events(None, [stock, self.event(), self.event()])
        content = render_digest({}, events)
        self.assertEqual(len(content['items']), 2)
        self.assertIn('Trump strategy', content['html'])
        self.assertIn('Nancy Pelosi', content['html'])
        self.assertIn('Detected 2026-09-24', content['text'])
        self.assertNotIn('Filed 2026-09-24', content['text'])
        self.assertIn(NEW, content['text'])


if __name__ == '__main__':
    unittest.main()

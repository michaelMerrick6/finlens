import unittest
from unittest.mock import Mock, patch
import sec_form4_support as sec

class RequestPacingTests(unittest.TestCase):
    def test_fast_requests_wait_before_fetch(self):
        session = Mock()
        session.get.return_value.status_code = 200
        with patch.object(sec, '_last_sec_request_started', 100.0), \
             patch.object(sec.time, 'monotonic', return_value=100.1), \
             patch.object(sec.time, 'sleep') as sleep:
            sec.fetch_with_retry(session, 'https://www.sec.gov/example', timeout=10, label='test')
            self.assertAlmostEqual(sleep.call_args.args[0], 0.15)
            session.get.assert_called_once()

    def test_spaced_requests_do_not_sleep(self):
        session = Mock()
        session.get.return_value.status_code = 200
        with patch.object(sec, '_last_sec_request_started', 100.0), \
             patch.object(sec.time, 'monotonic', return_value=101.0), \
             patch.object(sec.time, 'sleep') as sleep:
            sec.fetch_with_retry(session, 'https://www.sec.gov/example', timeout=10, label='test')
            sleep.assert_not_called()

if __name__ == '__main__':
    unittest.main()

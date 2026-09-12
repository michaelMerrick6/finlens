import unittest
from contextlib import ExitStack
from unittest.mock import patch
from PIL import Image

from ingest_house_official import (HouseScanReviewRequired, select_house_checkbox,
                                   extract_transactions_from_scanned_house_pdf)


class HouseScanSafetyTests(unittest.TestCase):
    def test_compound_surname_boundary_matches_complete_name(self):
        from ingest_house_official import resolve_member_id
        from parser_write_policy import read_only_parser_scope
        members = [dict(id='M001232', first_name='April', last_name='McClain Delaney', chamber='House'),
                   dict(id='D000620', first_name='John', last_name='Delaney', chamber='House')]
        with read_only_parser_scope():
            self.assertEqual(resolve_member_id('April McClain','Delaney',members),'M001232')
            self.assertTrue(resolve_member_id('April','Delaney',members).startswith('unknown-'))

    def extract(self, texts, scores=None):
        module = 'ingest_house_official.'
        with ExitStack() as stack:
            stack.enter_context(patch(module+'convert_from_bytes',return_value=[Image.new('RGB',(2200,1700),'white')]))
            stack.enter_context(patch(module+'resolve_member_id',return_value='test-member'))
            stack.enter_context(patch(module+'is_house_attachment_continuation_page',return_value=False))
            stack.enter_context(patch(module+'extract_house_scanned_rows',return_value=[(600,700),(700,800)]))
            ocr=stack.enter_context(patch(module+'ocr_house_scanned_cell',side_effect=texts))
            stack.enter_context(patch(module+'score_house_checkbox_row',side_effect=scores or [[.02,0,0,0],[.02]+[0]*10]*2))
            stack.enter_context(patch(module+'resolve_house_ticker',return_value='N/A'))
            result=extract_transactions_from_scanned_house_pdf(b'pdf','test','Test','Member',2026,[])
            return result, ocr.call_count

    def test_identical_physical_rows_are_preserved(self):
        trades,_=self.extract(['Private LLC','08/06/2026','08/07/2026','Private LLC']*2)
        self.assertEqual(len(trades),2)
        self.assertEqual(len({row['doc_id'] for row in trades}),2)
        self.assertEqual(trades[0]['transaction_date'],'2026-08-06')

    def test_blank_or_multiple_marks_require_review(self):
        for scores in ([0,0,0,0],[.02,.02,0,0]):
            with self.subTest(scores=scores), self.assertRaises(HouseScanReviewRequired):
                select_house_checkbox(scores,field='type')
        self.assertEqual(select_house_checkbox([0,.02,0,0],field='type'),1)

    def test_unreadable_date_does_not_use_option_expiration(self):
        with self.assertRaisesRegex(HouseScanReviewRequired,'transaction-date cell'):
            self.extract(['CALL/MSFT EXP 08/07/2026',''])

    def test_later_ambiguous_row_prevents_partial_return(self):
        with self.assertRaises(HouseScanReviewRequired):
            self.extract(['Private LLC','08/06/2026','08/07/2026','Private LLC',
                          'Private LLC','08/06/2026','08/07/2026'],
                         scores=[[.02,0,0,0],[.02]+[0]*10,[0,0,0,0],[.02]+[0]*10])

    def test_unknown_page_layout_stops_document(self):
        with patch('ingest_house_official.convert_from_bytes',return_value=[Image.new('RGB',(2200,1700),'white')]), \
             patch('ingest_house_official.resolve_member_id',return_value='test-member'), \
             patch('ingest_house_official.is_house_attachment_continuation_page',return_value=False), \
             patch('ingest_house_official.extract_house_scanned_rows',return_value=[]):
            with self.assertRaisesRegex(HouseScanReviewRequired,'unrecognized scan layout'):
                extract_transactions_from_scanned_house_pdf(b'pdf','test','Test','Member',2026,[])

    def test_future_transaction_year_requires_review(self):
        with self.assertRaisesRegex(HouseScanReviewRequired,'year'):
            self.extract(['Private LLC','08/06/2027'])


if __name__ == '__main__':
    unittest.main()

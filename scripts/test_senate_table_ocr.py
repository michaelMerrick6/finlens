"""Exercise image geometry and failure handling without external services."""
import unittest
from unittest.mock import patch

import cv2
import numpy as np
from PIL import Image

from senate_table_ocr import (AMOUNTS, TableReviewRequired, selected_mark,
                             mark_scores, extract_document, extract_page, align_page)


class SenateTableTests(unittest.TestCase):
    def test_grid_borders_do_not_count_as_marks(self):
        page = np.full((140, 300), 255, dtype=np.uint8)
        for x in (0, 100, 200, 299):
            cv2.line(page, (x,0), (x,139), 0, 4)
        for y in (0,139):
            cv2.line(page,(0,y),(299,y),0,4)
        xs = [0,100,200,299]
        self.assertIsNone(selected_mark(mark_scores(page,xs,0,139,range(3))))
        cv2.line(page,(140,55),(160,75),0,3)
        cv2.line(page,(160,55),(140,75),0,3)
        self.assertEqual(selected_mark(mark_scores(page,xs,0,139,range(3))),1)
        cv2.line(page,(240,55),(260,75),0,3)
        with self.assertRaises(TableReviewRequired):
            selected_mark(mark_scores(page,xs,0,139,range(3)))

    def test_all_eleven_amount_columns_are_distinct(self):
        self.assertEqual(len(set(AMOUNTS)),11)
        self.assertEqual(AMOUNTS[6], 'Over $1,000,000')
        self.assertEqual(AMOUNTS[7], '$1,000,001 - $5,000,000')
        for i in range(11):
            scores = [0.] * 11
            scores[i] = .02
            self.assertEqual(selected_mark(scores),i)

    def test_blank_page_is_not_a_successful_extraction(self):
        with self.assertRaises(TableReviewRequired):
            extract_page(Image.new('RGB',(850,1100),'white'),page=1,filed_date='2026-08-31')

    def test_later_failed_page_prevents_partial_document_result(self):
        row = dict(page=1,row=1,transaction_date='2026-07-01')
        with patch('senate_table_ocr.is_cover_letter',return_value=False), \
             patch('senate_table_ocr.extract_page',side_effect=[([row],'account'),TableReviewRequired('broken grid')]):
            with self.assertRaises(TableReviewRequired):
                extract_document([object(),object()],filed_date='2026-08-31')

    def test_source_rows_and_account_continue_across_pages(self):
        with patch('senate_table_ocr.is_cover_letter',return_value=True), \
             patch('senate_table_ocr.extract_page',side_effect=[([{'page':2,'row':1}],'account'),([{'page':3,'row':1}],'account')]) as extract:
            rows=extract_document([object(),object(),object()],filed_date='2026-08-31')
        self.assertEqual(rows,[{'page':2,'row':1},{'page':3,'row':1}])
        self.assertEqual(extract.call_args.kwargs['account'],'account')

    def test_alignment_corrects_small_rotation(self):
        gray=np.full((1100,850),255,dtype=np.uint8)
        for y in (300,500,700,900):
            cv2.line(gray,(100,y),(750,y),0,3)
        rotated=cv2.warpAffine(gray,cv2.getRotationMatrix2D((425,550),1.2,1),(850,1100),borderValue=255)
        aligned=align_page(Image.fromarray(rotated))
        # A corrected long rule occupies one narrow horizontal band.
        self.assertGreater(np.max(np.count_nonzero(aligned<100,axis=1)),600)


if __name__ == '__main__':
    unittest.main()

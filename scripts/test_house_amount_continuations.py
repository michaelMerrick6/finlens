import unittest
from ingest_house_official import extract_transactions_from_layout_lines, extract_transactions_from_lines, HouseScanReviewRequired, resolve_member_id
from parser_write_policy import read_only_parser_scope

class HouseAmountTests(unittest.TestCase):
    def parse(self, lines, layout=True):
        with read_only_parser_scope():
            return (extract_transactions_from_layout_lines if layout else extract_transactions_from_lines)(
                lines,'test','Test','Member',2026,[],[])

    def test_exact_cents_preserved(self):
        for value in ['$318.74','$707.21','$584.22']:
            row=f'Private Fund [HN] P 03/03/2026 03/30/2026 {value}'
            for layout in [True,False]:
                self.assertEqual(self.parse([row],layout)[0]['amount_range'],value)

    def test_wrapped_range_across_page_headers_preserves_asset_and_amount(self):
        rows=self.parse(['JT Exxon Mobil Corporation Common P 02/07/2025 05/29/2026 $15,001 -',
            'ID Owner Asset Transaction Date Notification Amount Cap.', 'Type Date Gains >','$200?',
            'Stock (XOM) [ST] $50,000'])
        self.assertEqual(rows[0]['amount_range'],'$15,001 - $50,000')
        self.assertEqual(rows[0]['ticker'],'XOM')
        self.assertNotIn('$50,000',rows[0]['asset_name'])

    def test_missing_bound_never_uses_next_transaction_or_description_amount(self):
        for rows in [
            ['Private Fund [HN] P 03/03/2026 03/30/2026 $1,001 -',
             'Second Fund [HN] P 03/03/2026 03/30/2026 $1,001 - $15,000'],
            ['Private Fund [HN] P 03/03/2026 03/30/2026', 'Description: capital call $318.74']]:
            with self.assertRaises(HouseScanReviewRequired): self.parse(rows,layout='Second Fund' in ' '.join(rows))

    def test_official_steube_middle_name_and_fletcher_alias(self):
        members=[dict(id='S001214',first_name='W.',last_name='Steube',chamber='House'),
                 dict(id='F000468',first_name='Lizzie',last_name='Fletcher',chamber='House')]
        with read_only_parser_scope():
            self.assertEqual(resolve_member_id('Greg','Steube',members),'S001214')
            self.assertEqual(resolve_member_id('Elizabeth','Fletcher',members),'F000468')

if __name__=='__main__': unittest.main()

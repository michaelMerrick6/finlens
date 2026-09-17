import unittest
from sync_pelosi_holdings import parse_filing

class SyncTests(unittest.TestCase):
    def page(self, description='Purchased 100 shares.', asset='[ST]', code='P', when='09/16/2026'):
        return f'Name: Hon. Nancy Pelosi SP Apple (AAPL) {asset} {code} {when} {when} $15,001 - $50,000 Filing Status: New Description: {description}'
    def parse(self, text):
        return parse_filing([text], 2026, '99999999', '09/16/2026')
    def test_purchase_sale_and_source(self):
        p = self.parse(self.page())[0]
        self.assertEqual(p['share_delta'], 100)
        self.assertEqual(p['doc_id'], 'house-2026-99999999-0')
        self.assertEqual(self.parse(self.page('Sold 50 shares.', code='S'))[0]['share_delta'], -50)
    def test_option_purchase(self):
        p = self.parse(self.page('Purchased 10 call options with a strike price of $100 and an expiration date of 6/17/27.', '[OP]'))[0]
        self.assertEqual(p['contract_delta'], 10)
        self.assertEqual(p['share_delta'], 0)
    def test_ambiguous_and_amended_fail_closed(self):
        for text in [self.page('Purchased stock.'), self.page().replace('New', 'Amended'), self.page().replace('SP Apple', 'JT Apple'), self.page(when='01/01/2026'), self.page('Exercised 100 options.'), self.page('Purchased 100 shares. Sold 50 shares.')]:
            with self.assertRaises(ValueError): self.parse(text)
    def test_multiple_rows_not_dropped(self):
        text = self.page() + ' SP Intel (INTC) [ST] S 09/16/2026 09/16/2026 $1,001 - $15,000 Description: Sold 20 shares.'
        self.assertEqual(len(self.parse(text)), 2)

if __name__ == '__main__': unittest.main()

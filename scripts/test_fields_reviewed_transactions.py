"""Independent expected rows transcribed during the Fields PDF visual review.

Scope: page 1 of each listed PTR, not confirmation of current ownership.
"""
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
# doc, row, ticker, date, action, amount bracket, account
REVIEWED = '''
20033732 1 GOOGL 2025-12-26 P C B
20033732 2 FIG 2025-12-26 S(partial) A B
20033732 3 FIG 2025-12-26 S B B
20033732 4 IREN 2025-12-26 S C B
20033732 5 OPEN 2025-12-26 S A B
20033858 1 GOOG 2026-01-12 P A I
20033858 2 GOOG 2026-01-20 P D B
20033858 3 IREN 2026-01-20 P C B
20033858 4 META 2026-01-20 P C B
20033858 5 NFLX 2026-01-20 P C B
20033858 6 TSM 2026-01-08 P A B
20034018 1 AMD 2026-02-03 P C B
20034018 2 GOOGL 2026-02-03 P D B
20034018 3 AAPL 2026-02-12 P A B
20034018 4 LRCX 2026-02-03 P B B
20034018 5 META 2026-02-03 P D B
20034018 6 MU 2026-02-03 P D B
20034179 1 MSFT 2026-03-12 P A L
20034179 2 MSFT 2026-03-12 P A B
20034295 1 GOOG 2026-03-16 P A B
20034357 1 TSM 2026-04-09 P A B
20034591 1 AAPL 2026-05-14 P A B
20034800 1 GOOG 2026-06-15 P A B
20034800 2 MSFT 2026-06-11 P A B
20034800 3 MSFT 2026-06-11 P A L
20034800 4 QNT 2026-06-04 P A B
20034894 1 NVDA 2026-06-26 P A B
20034894 2 NVDA 2026-06-26 P A I
20034894 3 NVDA 2026-06-26 P A L
20035023 1 TSM 2026-07-10 P A B
20035345 1 AAPL 2026-08-13 P A B
'''

class FieldsReviewTests(unittest.TestCase):
    def test_staged_rows_match_independently_read_sources(self):
        data = ROOT / 'docs/research'
        member = next(m for m in json.loads((data/'priority-holdings-ledger.json').read_text())['members'] if m['member_id']=='F000110')
        hashes = {r['source']:r['sha256'] for r in json.loads((data/'fields-source-check.json').read_text())['source_checks']}
        events = {e['id']:e for e in member['events']}
        accounts = dict(B='Morgan Stanley - E*TRADE #2', I='Morgan Stanley - E*TRADE IRA', L='Morgan Stanley - E*TRADE - Fields Law Firm 2, LLC')
        brackets = dict(A=[1001,15000], B=[15001,50000], C=[50001,100000], D=[100001,250000])
        expected_ids = set()
        for line in REVIEWED.strip().splitlines():
            doc,row,ticker,date,action,amount,account = line.split()
            source = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{doc}.pdf'
            identifier = f'{source}#row={row}'
            expected_ids.add(identifier)
            with self.subTest(row=identifier):
                e = events[identifier]
                self.assertEqual((e['ticker'],e['date'],e['action'].replace(' ',''),e['value_bounds'],e['account']), (ticker,date,action,brackets[amount],accounts[account]))
                self.assertEqual(e['source_sha256'],hashes[source])
                self.assertEqual(e['page'],1)
                self.assertEqual(e['owner'],'not-stated')
                self.assertEqual(e['filing_status'],'New')
                self.assertEqual(e['after_baseline'],date>'2025-12-31')
        self.assertEqual(set(events),expected_ids)

if __name__ == '__main__':
    unittest.main()

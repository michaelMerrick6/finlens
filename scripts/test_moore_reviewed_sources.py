"""Expected values independently read from Moore annual pages 1–6 and nine PTRs."""
import json
from pathlib import Path
import unittest
ROOT=Path(__file__).resolve().parents[1]
# A=1001–15000, B=15001–50000, C=50001–100000, D=100001–250000,
# E=250001–500000, F=500001–1000000, G=1000001–5000000, Z=explicit None.
ANNUAL='''
0 - E
1 - D
2 - C
3 - Z
4 - E
5 - U
6 - Z
7 UPS A
8 - Z
9 - A
10 - A
11 TEDMX A
12 - E
13 - C
14 - D
15 - E
16 - Z
17 - A
18 SMCY A
19 AAL Z
20 AAPL Z
21 AVIR A
22 BSOL D
23 - F
24 CNC Z
25 CBRL B
26 TZA E
27 TNA Z
28 F Z
29 GNPX A
30 QYLD Z
31 HOG B
32 HY Z
33 INTC Z
34 DNUT Z
35 LGIH Z
36 TQQQ Z
37 - G
38 SMCYY Z
39 SMCI Z
40 UNH Z
41 VZ B
42 WKHS Z
43 AMZY A
44 MSTY B
45 SMCY A
46 GOOG B
47 AMZN B
48 AAPL B
49 BKR B
50 BRK.B B
51 BLK B
52 COIN B
53 DE B
54 DASH B
55 HON B
56 ITW A
57 JPM B
58 JPST A
59 MSFT B
60 MS B
61 NVDA C
62 RSG B
63 USFD B
64 V B
65 WMT B
66 - A
67 WMPXX G
68 CL Z
69 GD Z
70 PG Z
71 ROK Z
'''
# Document, row, ticker, date, action, bracket. Blank owner/account in all PTRs.
TRADES='''
20033746 1 CBRL 2025-12-31 P B
20033746 2 GNPX 2025-12-18 P A
20033746 3 HOG 2025-12-18 P B
20033856 1 T 2026-01-09 P B
20033856 2 CBRL 2026-01-05 S C
20033856 3 HY 2025-12-31 P B
20033856 4 HY 2026-01-05 S B
20034034 1 T 2026-01-30 S B
20034034 2 COIN 2026-02-18 S A
20034034 3 GNPX 2026-02-05 P A
20034034 4 HOG 2026-01-23 P B
20034034 5 DNUT 2026-02-12 P A
20034034 6 SMPL 2026-02-11 P B
20034034 7 SMPL 2026-02-03 P B
20034034 8 VZ 2026-01-30 S B
20034217 1 CBRL 2026-03-23 P B
20034217 2 HOG 2026-03-12 P B
20034217 3 DNUT 2026-03-17 S B
20034217 4 LGIH 2026-03-18 P B
20034217 5 LGIH 2026-03-19 P C
20034217 6 LGIH 2026-03-20 P B
20034217 7 LGIH 2026-03-12 P B
20034217 8 SMPL 2026-03-12 P A
20034217 9 SMPL 2026-03-13 P A
20034424 1 CBRL 2026-04-01 S B
20034424 2 HOG 2026-04-07 S C
20034424 3 LGIH 2026-03-26 S D
20034424 4 NVDA 2026-03-24 S B
20034587 1 T 2026-05-18 P B
20034587 2 IHG 2026-05-07 P A
20034587 3 RYCEY 2026-05-07 P A
20034721 1 T 2026-05-21 S B
20034721 2 T 2026-06-04 P C
20034952 1 DASH 2026-06-09 S A
20035291 1 T 2026-08-14 S C
'''
BOUNDS=dict(A=[1001,15000],B=[15001,50000],C=[50001,100000],D=[100001,250000],E=[250001,500000],F=[500001,1000000],G=[1000001,5000000],Z=[0,0],U=None)

class MooreSourceTests(unittest.TestCase):
    def setUp(self):
        self.member=next(m for m in json.loads((ROOT/'docs/research/priority-holdings-ledger.json').read_text())['members'] if m['member_id']=='M001236')
    def test_all_annual_value_columns(self):
        rows={r['id']:r for r in self.member['positions']}
        self.assertEqual(len(rows),72)
        for line in ANNUAL.strip().splitlines():
            idx,ticker,code=line.split();row=rows['annual:'+idx]
            with self.subTest(row=idx):
                self.assertEqual(row['ticker'],None if ticker=='-' else ticker)
                self.assertEqual(row['value_bounds'],BOUNDS[code])
                if code=='Z':self.assertEqual(row['zero_basis'],'explicit-house-none')
        self.assertEqual(rows['annual:18']['account'],'Robinhood')
        self.assertEqual(rows['annual:45']['account'],'Schwab One')
        self.assertEqual(rows['annual:20']['value_bounds'],[0,0])
        self.assertEqual(rows['annual:48']['account'],'TradeWinds')
    def test_all_transaction_rows(self):
        events={e['id']:e for e in self.member['events']};expected=set()
        for line in TRADES.strip().splitlines():
            doc,row,ticker,date,action,code=line.split()
            eid=f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{doc}.pdf#row={row}'
            expected.add(eid);e=events[eid]
            with self.subTest(row=eid):
                self.assertEqual((e['ticker'],e['date'],e['action'],e['value_bounds']),(ticker,date,action,BOUNDS[code]))
                self.assertIsNone(e['account'])
                self.assertEqual(e['owner'],'not-stated')
                self.assertEqual(e['filing_status'],'New')
        self.assertEqual(set(events),expected)
    def test_reviewed_hashes_still_match(self):
        hashes={s['source']:s['sha256'] for s in json.loads((ROOT/'docs/research/moore-source-review.json').read_text())['sources']}
        for row in self.member['positions']+self.member['events']:
            self.assertEqual(row['source_sha256'],hashes[row['source']])
    def test_conflicting_hyster_yale_balance_is_not_published(self):
        m=next(m for m in json.loads((ROOT/'docs/research/priority-holdings-models.json').read_text())['members'] if m['member_id']=='M001236')
        hy=next(r for r in m['rows'] if r['ticker']=='HY')
        self.assertEqual(hy['model']['status'],'review-required')
        self.assertEqual(hy['model']['reason'],'sale-exceeds-modeled-balance')

if __name__=='__main__':unittest.main()

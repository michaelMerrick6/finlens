"""Research scenario only. Does not feed the public holdings ledger.
Run with python3 docs/research/pelosi-visa-range.py.
"""
import json
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

# Yahoo daily OHLC retrieved 2026-09-17 for 2008-11-24. Its quote prices are
# split-adjusted (not dividend-adjusted adjclose). Restore pre-2015 units.
quote = {'date': '2008-11-24', 'split_adjusted_low': '11.444999694824219',
         'split_adjusted_high': '12.5', 'subsequent_split_factor': 4,
         'source': 'https://query1.finance.yahoo.com/v8/finance/chart/V?period1=1227484800&period2=1227657600&interval=1d&events=splits'}
low = Decimal(quote['split_adjusted_low']) * 4
high = Decimal(quote['split_adjusted_high']) * 4
sold_min = int((Decimal(15001) / high).to_integral_value(rounding=ROUND_CEILING))
sold_max = int((Decimal(50000) / low).to_integral_value(rounding=ROUND_FLOOR))
# Primary quantity lead: archived 2011 Pelosi office statement supplies the
# 2008 acquisition quantities. This is NOT a certified complete balance.
purchases = 5000 + 10000 + 5000
before_split_sales = 1000 + 500  # 2014 annual, page 8
# Later annual schedules; 2019 is explicitly branched below, not deduplicated.
after_split_disposals = [
 ('2015-12-29', 5000), ('2015-12-30', 2000), ('2016-07-21', 1250),
 ('2017-12-21', 1750), ('2017-12-28', 5000),
 ('2020-05-08', 3000), ('2020-05-08', 3000),
 ('2022-06-21', 10000), ('2022-11-08', 20000), ('2024-07-01', 2000),
]
scenarios = []
for sales_2019 in [1000, 2000]:
 minimum = (purchases - sold_max - before_split_sales) * 4 - sum(n for _,n in after_split_disposals) - sales_2019
 maximum = (purchases - sold_min - before_split_sales) * 4 - sum(n for _,n in after_split_disposals) - sales_2019
 scenarios.append({'2019_shares_sold': sales_2019, 'remaining_min': minimum, 'remaining_max': maximum})
assert (sold_min, sold_max) == (301, 1092)
assert min(s['remaining_min'] for s in scenarios) == 14632
assert max(s['remaining_max'] for s in scenarios) == 18796
result = {'status': 'research_scenario_not_approved_for_public_holdings',
 'quote': quote, '2008_sale_shares_min': sold_min, '2008_sale_shares_max': sold_max,
 'scenarios': scenarios, 'after_split_disposals': after_split_disposals,
 'assumptions': ['2008 purchases exhaust opening shares; no other holdings or unreviewed changes',
 '2008 sale price within regular-session daily high/low, no fees and whole shares',
 'Daily OHLC is split-adjusted by four; raw prices restored before inferring sale shares',
 '2019 amended identity unresolved: one or two 1,000-share sales',
 'Both 2020 3,000-share rows are distinct as represented in PTR and annual',
 'Early annual amendment/inventory review and cross-year completeness remain to be completed'],
 'source_links': [
 'https://www.legistorm.com/stormfeed/view_rss/382260/office/1978/title/pelosi-spokesman-statement-on-60-minutes-report.html',
 'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2009/8142737.pdf#page=18',
 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2019/20012288.pdf',
 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2019/20012343.pdf',
 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2020/20016726.pdf',
 'https://www.sec.gov/Archives/edgar/data/1403161/000140316115000013/v093015.htm']}
Path(__file__).with_suffix('.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps({'sold_2008':[sold_min,sold_max], 'scenarios':scenarios},indent=2))

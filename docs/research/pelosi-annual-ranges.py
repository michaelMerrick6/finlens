"""Conditional whole-share ranges, assuming year-end closing-price valuation."""
import json, math
from pathlib import Path
prices=json.loads(Path(__file__).with_name('pelosi-baseline-prices.json').read_text())
# Annual values + net share changes AFTER each snapshot, through reviewed 2026 filings.
inputs={
 'AAPL':[(2022,5000001,25000000,12100-31600-73582),(2023,25000001,50000000,-31600-73582),(2024,25000001,50000000,-73582),(2025,5000001,25000000,0)],
 'CRM':[(2022,1000001,5000000,0),(2023,5000001,25000000,0),(2024,5000001,25000000,0),(2025,5000001,25000000,0)]}
baseline=json.loads(Path('artifacts/pelosi-audit/2025-holdings-baseline.json').read_text())
for ticker in ['T','CLNE','IBKR','MORN','QCOM','AMZN','CMCSA','GOOGL','V','WBD']:
 row=next(r for r in baseline['rows'] if r['ticker_as_filed']==ticker and r['asset_type_as_filed']=='ST')
 if ticker == 'CMCSA':
  assert prices[ticker]['prices']['2025']['close'] == 29.89
  assert prices[ticker]['corporate_action_review']
 else:
  assert not prices[ticker]['splits'], 'Corporate action requires explicit reconciliation'
 inputs[ticker]=[(2025,row['value_min'],row['value_max'],5000 if ticker in ('AMZN','GOOGL') else 0)]
result={}
for ticker,rows in inputs.items():
 constraints=[]
 for year,lo,hi,delta in rows:
  price=prices[ticker]['prices'][str(year)]
  constraints.append({'year':year,'value_min':lo,'value_max':hi,'price':price,'subsequent_net_shares':delta,'remaining_min':math.ceil(lo/price['close'])+delta,'remaining_max':math.floor(hi/price['close'])+delta})
 lower=max(0,max(r['remaining_min'] for r in constraints)); upper=min(r['remaining_max'] for r in constraints)
 assert lower<=upper, 'Inconsistent annual constraints: investigate, do not average'
 result[ticker]={'conditional_min':lower,'conditional_max':upper,'constraints':constraints,'assumptions':['Annual value brackets reflect the last trading-day closing price','Same stock class and owner across annuals','All intervening share changes captured; no omitted acquisitions, gifts, reinvestments or disposals','No subsequent stock splits during the modeled period','Range is model-dependent, not a statistical confidence interval']}
Path(__file__).with_suffix('.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:[v['conditional_min'],v['conditional_max']] for k,v in result.items()}))

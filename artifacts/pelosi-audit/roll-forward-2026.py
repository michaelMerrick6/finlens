"""Source-reviewed position changes, not certified current positions. No DB writes."""
import json,hashlib
from pathlib import Path
p=Path(__file__).resolve().parent
baseline=json.loads((p/'2025-holdings-baseline.json').read_text());events=[]
def add(doc,row,page,ticker,date,kind,shares=0,contracts=0,strike=None,expiration=None):
 source=p/'sources'/f'2026-{doc}.pdf'
 events.append(dict(doc_id=f'house-2026-{doc}-{row}',page=page,source=f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{doc}.pdf',sha256=hashlib.sha256(source.read_bytes()).hexdigest(),ticker=ticker,date=date,kind=kind,share_delta=shares,contract_delta=contracts,strike=strike,expiration=expiration,owner='SP',review='visually checked PDF and description'))
add('20033725',0,1,'AB','2026-01-16','public_partnership_units_purchase',25000)
for row,page,ticker,strike in [(1,1,'GOOGL',150),(6,2,'AMZN',150),(12,2,'NVDA',80),(14,3,'TEM',20),(16,3,'VST',50)]:
 add('20033725',row,page,ticker,'2026-01-16','exercise',5000,-50,strike,'2026-01-16')
add('20033725',15,3,'VSNT','2026-01-02','spinoff',776)
for row,ticker in [(0,'INTC'),(1,'UBER')]:add('20034836',row,1,ticker,'2026-05-29','call_purchase',0,200,50,'2027-03-19')
add('20035143',0,1,'BE','2026-07-24','stock_purchase',10000)
add('20035143',1,1,'BE','2026-07-24','call_purchase',0,100,100,'2027-06-17')
add('20035143',2,1,'BE','2026-07-28','stock_purchase',5000)
add('20035143',3,1,'BE','2026-07-28','call_purchase',0,100,100,'2027-06-17')
add('20035143',4,1,'INTC','2026-07-24','call_purchase',0,50,50,'2027-06-17')
add('20035143',5,1,'INTC','2026-07-24','stock_purchase',10000)
add('20035143',6,2,'REOF XXV LLC','2026-07-27','private_investment_quantity_unknown')
options={}
for r in baseline['rows']:
 if r['asset_type_as_filed']=='OP' and r['reported_value']!='None':
  for c in r['contracts_as_described']:options[(r['ticker_as_filed'],c['strike'],c['expiration'])]=c['contracts']
for e in events:
 if e['contract_delta']:
  k=(e['ticker'],e['strike'],e['expiration']);options[k]=options.get(k,0)+e['contract_delta'];assert options[k]>=0
assert sum(e['kind']=='exercise' for e in events)==5
assert len(events)==16
out={'status':'reviewed changes within three cached filings; fresh filing inventory and full completeness remain unverified','baseline_reporting_year':2025,'latest_filing_reviewed':'2026-08-21','latest_transaction_reviewed':'2026-07-28','events':events,'option_roll_forward':[{'ticker':k[0],'strike':k[1],'expiration':k[2],'contracts':v,'current_position_verified':False} for k,v in options.items()],'notes':['2025 transactions filed in January 2026 are excluded from changes to avoid counting them twice.','Five exercises consume their 50-contract 2026 series and add 5,000 shares each.','Unknown baseline share counts remain unknown; deltas are not final positions.','VSNT spinoff adds 776 shares; filing says no Comcast shares surrendered. $15.00 field is not interpreted as purchase price or total position value.','REOF private investment has no disclosed quantity.','No current market value or return has been calculated.']}
(p/'2026-position-changes.json').write_text(json.dumps(out,indent=2)+'\n')
print('Validated',len(events),'events; option series:',len(options),'remaining positive:',sum(v>0 for v in options.values()))

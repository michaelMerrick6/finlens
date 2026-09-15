"""Persist PDF-checked rows and supporting-letter classifications; read-only DB snapshot."""
import json,re
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent;reports=json.loads((r/'supplemental-annual-reconciliation.json').read_text());db=json.loads((r/'stored-transactions.json').read_text())
for year,doc in [(2013,'10008188'),(2014,'10008189')]:
 report=next(x for x in reports if x['doc_id']==doc);rows=[]
 names=(['The Active Network, Inc. (ACTV)']+['United Football League Sacramento Mountain Lions Team']*10+['Vizu Corporation Preferred Stock']) if year==2013 else ['Alcoa Inc. (AA)','Blucora, Inc. (BCOR)','Broadcom Corporation - Class A (BRCM)','Dow Chemical Company (DOW)','Emulex Corporation (ELX)','Entropic Communications, Inc. (ENTR)']+['Hertz Global Holdings, Inc (HTZ)']*4+['McGrath RentCorp (MGRC)','Robert Half International Inc. (RHI)','Shutterfly, Inc. (SFLY)','SunEdison, Inc. (SUNE)','Visa Inc. (V)','Visa Inc. (V)','Walt Disney Company (DIS)','Walt Disney Company (DIS)']
 for i,(date,d,lo,hi) in enumerate(report['parsed_date_direction_ranges']):
  iso=datetime.strptime(date,'%m/%d/%Y').date().isoformat();low=int(lo.replace(',',''));high=int(hi.replace(',',''));page=7 if i<(4 if year==2013 else 7) else 8
  x=dict(page=page,source_row=i+1,asset=names[i],owner='SP' if year==2013 or i not in [6,7,8,9,14,15,16,17] else None,dates=[iso],activity='buy' if d=='P' else 'sell',amount_min=low,amount_max=high,amount_scope='reported source row',visual_review='checked against PDF pages 7–8',candidate_db_ids=[])
  for row in db:
   amount=re.search(r'\d[\d,]*',row.get('amount_range') or '')
   if row.get('transaction_date')==iso and row.get('transaction_type')==x['activity'] and amount and int(amount[0].replace(',',''))==low:x['candidate_db_ids'].append(row['id'])
  if year==2013 and 1<=i<=10:x.update(economic_type='business_operating_cost',exclude_from_equity_purchase_totals=True,classification_source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2013/9106227.pdf',classification_page=1)
  if year==2013 and i==0:x.update(economic_type='acquisition_cash_distribution',corroborating_ptr='8214491',source_note='PTR notes acquisition by Vista Equity Partners and cash distributed in lieu of shares; do not double count annual/PTR.')
  if year==2013 and i in [9,10]:x.update(aggregation_hold=True,review_note='Two identical December 30 source rows. Letter refers to ten operating-cost items; preserve both without inventing equity purchases.')
  if year==2014 and i in [6,7,8,9,16,17]:
   x.update(economic_type='call_option_purchase',contracts={6:50,7:150,8:200,9:100,16:8,17:92}[i],strike_price=22 if i<10 else 90,expiration_date='2016-01-15')
  if year==2014 and i in [10,11]:x.update(economic_type='charitable_contribution',exclude_from_sale_totals=True,source_note='Contribution to Paul & Nancy Pelosi Charitable Foundation; transfer between brokerage accounts.')
  if year==2014 and i in [14,15]:x.update(activity='partial_sale',shares=1000 if i==14 else 500)
  rows.append(x)
 assert len(rows)==(12 if year==2013 else 18)
 (r/f'verified-{year}-schedule.json').write_text(json.dumps(dict(source=report['url'],sha256=report['sha256'],reporting_year=year,status='schedule_visually_checked_pending_entity_and_full_filing_reconciliation',row_count=len(rows),rows=rows),indent=2))
 print(year,'rows',len(rows),'without candidate',sum(not x['candidate_db_ids'] for x in rows))

import json,hashlib
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent;rows=[];counts={}
def add(p,a,d,date,lo,hi,g='N/A',**extra):
 counts[p]=counts.get(p,0)+1
 rows.append(dict(page=p,source_row=counts[p],asset=a,owner='SP',activity=d,dates=[datetime.strptime('2012-'+date,'%Y-%m-%d').date().isoformat()],amount_min=lo,amount_max=hi,capital_gain_over_200=g,amount_scope='reported source row',database_status='missing_pre_2014',**extra))
add(12,'820 Sir Francis Drake Blvd.','sell','8-29',5000001,25000000,'Yes')
add(12,'Apple Inc. Common Stock - Additional Investment','buy','5-23',250001,500000)
add(12,'Facebook Inc. Common Stock','buy','5-23',100001,250000)
add(12,'J. Crew Group Inc. Common Stock (Sale in Prior Year - Residual Distribution)','sell','1-19',1001,15000,'Yes',economic_type='residual_distribution')
add(12,'Nine Hundred One Battery','sell','12-28',50001,100000,'Yes')
add(12,'Oakwood Homes Corp. Bonds (Sale in Prior Year - Residual Distribution)','sell','1-27',1001,15000,'Yes',economic_type='residual_distribution')
for date,lo,hi in [('9-7',500001,1000000),('9-14',500001,1000000),('9-20',500001,1000000),('10-9',250001,500000),('10-17',250001,500000)]:add(13,'United Football League - Additional Investment','buy',date,lo,hi)
for p,date,lo,hi in [(13,'5-8',15001,50000),(13,'5-16',15001,50000),(13,'6-20',50001,100000),(13,'7-18',50001,100000),(14,'7-27',50001,100000),(14,'8-8',50001,100000),(14,'8-15',100001,250000),(14,'8-30',250001,500000),(14,'8-30',100001,250000),(14,'9-18',100001,250000),(14,'9-20',100001,250000),(14,'9-24',100001,250000),(14,'9-25',100001,250000),(14,'9-28',100001,250000),(14,'10-2',100001,250000),(15,'10-2',100001,250000),(15,'10-2',250001,500000),(15,'10-10',100001,250000),(15,'10-17',250001,500000),(15,'11-1',15001,50000),(15,'11-1',100001,250000),(15,'11-23',15001,50000),(15,'12-31',250001,500000)]:add(p,'United Football League Sacramento Team - Additional Investment','buy',date,lo,hi)
add(15,'Vizu Corporation Preferred Stock','sell','7-13',50001,100000,'Yes')
add(16,'Vizu LLC','sell','7-13',15001,50000,'Yes')
for x in rows:
 if x['asset'].startswith('United Football League Sacramento') and x['dates']==['2012-10-02'] and x['amount_min']==100001:x['review_note']='Same date/range appears at page 14 final row and page 15 first row. Preserve source rows; possible page-boundary duplicate, do not aggregate until resolved.';x['aggregation_hold']=True
assert len(rows)==36
result=dict(source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2013/9102968.pdf',sha256=hashlib.sha256((r/'sources/2013-9102968.pdf').read_bytes()).hexdigest(),reporting_year=2012,status='schedule_visually_transcribed_pending_duplicate_and_full_filing_review',row_count=len(rows),rows=rows)
(r/'verified-2012-schedule.json').write_text(json.dumps(result,indent=2));print(len(rows),counts)

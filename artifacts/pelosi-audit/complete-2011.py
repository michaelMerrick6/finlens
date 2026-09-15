"""Manual visual transcription; exchange legs remain linked, not cash trades."""
import json,hashlib
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent;rows=[];counts={}
def add(p,a,d,date,lo,hi,g='N/A',**extra):
 counts[p]=counts.get(p,0)+1
 rows.append(dict(page=p,source_row=counts[p],asset=a,owner='SP',activity=d,dates=[datetime.strptime('2011-'+date,'%Y-%m-%d').date().isoformat()],amount_min=lo,amount_max=hi,capital_gain_over_200=g,amount_scope='reported source row',database_status='missing_pre_2014',**extra))
add(13,'Active LLC (Ltd. Partnership Investment Exchanged for Active Network Inc. Common Stock)','exchange','12-5',15001,50000,exchange_group='active-2011-12-05',exchange_leg='out')
add(13,'Active Network Inc. Common Stock (Received in Exchange for Ltd. Partnership Investment in Active LLC)','exchange','12-5',15001,50000,exchange_group='active-2011-12-05',exchange_leg='in')
add(13,'Command Audio Preferred Stock','sell','3-24',1001,15000,'Yes')
add(13,'Digital Fountain Preferred Stock','sell','3-14',1,1000,'No')
add(13,'Entropic Communications Inc. Common Stock (Received in Exchange for Portion of Ltd. Partnership Interest in Granite Ventures LP)','exchange','1-25',50001,100000,exchange_group='granite-entropic-2011-01-25',exchange_leg='in')
add(14,'Fastnet Common Stock','sell','9-26',1,1000,'No')
add(14,'Granite Ventures LP (Portion of Ltd. Partnership Interest Exchanged for Entropic Communications Inc. Common Stock)','exchange','1-25',50001,100000,exchange_group='granite-entropic-2011-01-25',exchange_leg='out')
add(14,'J. Crew Group Common Stock','sell','3-8',250001,500000,'Yes')
for date,d,lo,hi in [('1-10','partial_sale',500001,1000000),('4-1','partial_sale',500001,1000000),('4-29','sell',1000001,5000000)]:add(14,'Matthews International Capital Management, LLC',d,date,lo,hi,'Yes')
add(14,'Oakwood Homes Corp. Bonds','sell','10-18',1,1000,'No')
add(15,'Sierra Vista Baseline Investors LP','buy','10-28',100001,250000)
for p,date,lo,hi in [(15,'4-4',250001,500000),(15,'4-29',50001,100000),(15,'5-5',100001,250000),(15,'5-20',50001,100000),(15,'6-3',50001,100000),(15,'6-13',100001,250000),(15,'6-20',250001,500000),(15,'6-28',100001,250000),(16,'7-12',50001,100000),(16,'7-20',15001,50000)]:add(p,'United Football League - Additional Investment','buy',date,lo,hi)
for p,date,lo,hi in [(16,'3-17',100001,250000),(16,'4-8',100001,250000),(16,'6-21',50001,100000),(16,'6-24',100001,250000),(16,'7-11',100001,250000),(16,'7-20',100001,250000),(16,'8-9',250001,500000),(16,'9-1',250001,500000),(17,'9-13',100001,250000),(17,'9-15',50001,100000),(17,'10-4',50001,100000),(17,'10-12',250001,500000),(17,'11-22',15001,50000),(17,'12-20',15001,50000),(17,'12-27',250001,500000),(17,'12-29',100001,250000),(17,'12-30',15001,50000)]:add(p,'United Football League Sacramento Team - Additional Investment','buy',date,lo,hi)
add(17,'Vizu Corporation Preferred Stock','buy','3-2',15001,50000)
assert len(rows)==41
result=dict(source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2012/8205631.pdf',sha256=hashlib.sha256((r/'sources/2012-8205631.pdf').read_bytes()).hexdigest(),reporting_year=2011,status='schedule_visually_transcribed_pending_full_filing_and_amendment_reconciliation',row_count=len(rows),rows=rows)
(r/'verified-2011-schedule.json').write_text(json.dumps(result,indent=2));print(len(rows),counts)

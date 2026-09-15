import json
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent
raw='''2|Apple Inc. Common Stock|partial_sale|4-20,5-28,10-12,10-20,10-23,10-26,12-28|5000001|25000000|Yes
2|Briazz Common Stock|sell|12-23|1|1000|No
2|Ebay Inc. Common Stock|partial_sale|11-6|250001|500000|No
2|Genitope Corp. Common Stock|sell|12-23|1|1000|No
2|Granite Ventures LP - Additional Ltd. Partnership Interest in Investment Fund|buy|4-27|15001|50000|N/A
2|Hennessy Advisors Inc. Common Stock|buy|1-23,1-26,1-30|1001|15000|N/A
2|Intraop Medical Corp. Common Stock|sell|12-23|1|1000|No
2|Inverness Med. Innovations Inc. Common Stock|sell|12-23|1001|15000|Yes
2|Matthews International Capital Management, LLC|buy|11-18|1000001|5000000|N/A
3|Motorola Inc. Common Stock|buy|11-6|500001|1000000|N/A
3|Niman Ranch LLC|sell|2-25|1001|15000|No
3|Odyssey LLC - Additional Ltd. Partnership Interest in Investment Firm|buy|4-20|1001|15000|N/A
3|Potomac Investment Assoc.|sell|1-2|1001|15000|No
3|Redenvelope Common Stock|sell|12-23|1|1000|No
3|Sun Microsystems Common Stock|sell|12-23|1|1000|No
3|TeraOp LLC|sell|12-28|1|1000|No
3|Unicru LLC|sell|10-20|1|1000|No
3|United Football League Team San Francisco, LLC|buy|4-24,7-28,10-15|1000001|5000000|N/A
3|Yahoo Inc. Common Stock|partial_sale|10-23|100001|250000|No'''
rows=[];counts={}
for line in raw.splitlines():
 p,a,d,dates,lo,hi,gain=line.split('|');p=int(p);counts[p]=counts.get(p,0)+1
 rows.append(dict(page=p,printed_page=p+12,source_row=counts[p],asset=a,owner='SP',activity=d,dates=[datetime.strptime('2009-'+x,'%Y-%m-%d').date().isoformat() for x in dates.split(',')],amount_min=int(lo),amount_max=int(hi),capital_gain_over_200=gain,amount_scope='reported source row; do not assign full range separately to each date',database_status='missing_pre_2014'))
assert len(rows)==19
assert sum(x['capital_gain_over_200']=='Yes' for x in rows)==2
assert sum(x['capital_gain_over_200']=='No' for x in rows)==11
result=dict(source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2010/8147383.pdf',original_doc_id='8147382',reporting_year=2009,status='schedule_visually_transcribed_amendment_applied_pending_full_filing_review',amendment_note='Cover letter states only capital gain over $200 column changed: 11 losses and 2 gains among 13 sale rows. Do not derive dollar returns from flags.',row_count=len(rows),rows=rows)
(r/'verified-2009-schedule.json').write_text(json.dumps(result,indent=2))
print(len(rows),'source rows;',sum(len(x['dates']) for x in rows),'listed dates')

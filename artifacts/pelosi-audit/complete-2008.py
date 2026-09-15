"""Source page transcription, with 2009 AIG amendment applied; no DB writes."""
import json
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent
raw='''16|Akamai Technologies Inc. Common Stock|sell|11-19|15001|50000
16|American International Group Inc. Common Stock|sell|12-31|1001|15000
16|Americas Doctors.com Preferred Stock|sell|12-29|1001|15000
16|Apple Inc. Common Stock|buy|1-15,1-16|5000001|25000000
16|Apple Inc. Common Stock|partial_sale|11-17,11-24|500001|1000000
16|AT&T Inc. Common Stock|partial_sale|11-21|15001|50000
16|Auberge du Soleil - Purchase of Additional Ltd. Partnership Interest|buy|4-4|250001|500000
16|Avalon Pharmaceuticals Inc. Common Stock|sell|11-3|1001|15000
16|Biz360 Inc. Preferred Stock|buy|4-10|1001|15000
16|Bullhorn LLC|sell|12-29|1001|15000
16|Cardiac Science Corp. Common Stock|sell|10-14|50001|100000
17|Clean Energy Fuels Corp. Common Stock|partial_sale|10-14|15001|50000
17|Critical Path Inc. Common Stock|sell|5-29|1001|15000
17|Cryocor Inc. Common Stock|sell|6-4|1001|15000
17|Digital Fountain Preferred Stock|buy|8-5|1001|15000
17|General Electric Co. Common Stock|sell|11-19|15001|50000
17|Getty Images Inc. Common Stock|sell|7-3|15001|50000
17|Granite Ventures LP - Additional Ltd. Partnership Interest in Investment Fund|buy|6-3|15001|50000
17|Internap Network Services Common Stock|sell|12-31|15001|50000
17|Johnson & Johnson Common Stock|sell|10-27,11-21|100001|250000
17|Morningstar Inc. Common Stock|partial_sale|10-21|15001|50000
17|Navteq Corp. Common Stock|sell|7-10|50001|100000
18|Odyssey LLC|buy|2-8|1001|15000
18|Overstock.com Common Stock|sell|10-21,11-3|15001|50000
18|Quest Energy Partners LP|sell|12-31|15001|50000
18|Rackspace Hosting Inc. Common Stock|buy|8-7|100001|250000
18|Rackspace Hosting Inc. Common Stock|sell|11-19|50001|100000
18|Research in Motion Ltd. Common Stock|buy|6-25|100001|250000
18|Research in Motion Ltd. Common Stock|sell|11-19|15001|50000
18|Starbucks Corporation Common Stock|sell|1-2|1001|15000
18|Visa Inc. Common Stock|buy|3-18,3-25,6-4|1000001|5000000
18|Visa Inc. Common Stock|partial_sale|11-24|15001|50000
18|Yahoo Inc. Common Stock|buy|2-8|500001|1000000'''
rows=[];counts={}
for line in raw.splitlines():
 p,a,d,dates,lo,hi=line.split('|');p=int(p);counts[p]=counts.get(p,0)+1
 rows.append(dict(page=p,printed_page=p-1,source_row=counts[p],asset=a,owner='SP',activity=d,dates=[datetime.strptime('2008-'+x,'%Y-%m-%d').date().isoformat() for x in dates.split(',')],amount_min=int(lo),amount_max=int(hi),amount_scope='reported source row; do not assign full range separately to each date',database_status='missing_pre_2014'))
rows[1]['amendment_note']='June 11, 2009 cover letter explicitly corrects partial sale to complete sale; no remaining AIG interest.'
result=dict(source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2009/8142737.pdf',original_doc_id='8140536',reporting_year=2008,status='schedule_visually_transcribed_amendment_applied_pending_full_filing_review',row_count=len(rows),rows=rows)
assert len(rows)==33
(r/'verified-2008-schedule.json').write_text(json.dumps(result,indent=2))
print(len(rows),'rows;',sum(len(x['dates']) for x in rows),'listed dates')

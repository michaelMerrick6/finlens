"""Visually transcribed pages 13–18; no DB writes."""
import json,hashlib
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent
raw='''13|Apple Inc. Common Stock|partial_sale|5-28|500001|1000000|Yes
13|Apple Inc. Common Stock|partial_sale|6-8|500001|1000000|Yes
13|Apple Inc. Common Stock|partial_sale|6-29|1000001|5000000|Yes
13|AT&T Inc. Common Stock|sell|5-24|100001|250000|No
13|Cisco Systems Inc. Common Stock - Additional Investment|buy|3-8|250001|500000|N/A
13|Cisco Systems Inc. Common Stock|partial_sale|5-14|250001|500000|No
13|Cisco Systems Inc. Common Stock|partial_sale|5-19|100001|250000|No
13|Cisco Systems Inc. Common Stock|partial_sale|5-20|15001|50000|No
13|Cisco Systems Inc. Common Stock|partial_sale|5-21|100001|250000|No
14|Cisco Systems Inc. Common Stock|sell|5-24|50001|100000|No
14|Dover Saddlery Inc. Common Stock|sell|6-25|1001|15000|No
14|Ebay Inc. Common Stock|sell|5-21|100001|250000|No
14|Fortunet Inc. Common Stock|sell|4-19|1001|15000|No
14|Golub Capital Partners - Additional Investment|buy|4-1|1|1000|N/A
14|Hennessy Advisors Inc. Common Stock|sell|6-4|1001|15000|No
15|J. Crew Group Inc. Common Stock|partial_sale|5-25|50001|100000|Yes
15|J. Crew Group Inc. Common Stock|partial_sale|5-28|15001|50000|Yes
15|Microsoft Corp. Common Stock|sell|6-7|100001|250000|Yes
15|MontaVista LLC|sell|1-25|1|1000|No
15|Morningstar Inc. Common Stock|partial_sale|6-25|1001|15000|Yes
15|Motorola Inc. Common Stock|partial_sale|5-21|250001|500000|No
16|Motorola Inc. Common Stock|sell|5-24|50001|100000|No
16|Oakwood Homes Corp. Common Stock|sell|6-30|1|1000|No
16|Sonus Networks Inc. Common Stock|sell|7-14|1|1000|No
16|Support Soft Inc. Common Stock|sell|7-14|1|1000|Yes
16|United Football League - Investment in Football League|buy|3-30|50001|100000|N/A
16|United Football League - Additional Investment in Football League|buy|8-26|250001|500000|N/A
16|United Football League - Additional Investment in Football League|buy|9-20|100001|250000|N/A
17|United Football League - Additional Investment in Football League|buy|10-14|100001|250000|N/A
17|United Football League - Additional Investment in Football League|buy|11-22|100001|250000|N/A
17|United Football League - Additional Investment in Football League|buy|11-29|50001|100000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|1-13|500001|1000000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|3-10|100001|250000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|5-13|250001|500000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|6-2|250001|500000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|6-10|50001|100000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|6-16|50001|100000|N/A
17|United Football League - Additional Investment in Sacramento Team|buy|6-18|50001|100000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|7-6|250001|500000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|7-9|50001|100000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|7-16|50001|100000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|7-30|50001|100000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|8-26|100001|250000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|12-16|50001|100000|N/A
18|United Football League - Additional Investment in Sacramento Team|buy|12-24|15001|50000|N/A
18|Yahoo Inc. Common Stock|sell|6-8|100001|250000|No'''
rows=[];counts={}
for line in raw.splitlines():
 p,a,d,date,lo,hi,g=line.split('|');p=int(p);counts[p]=counts.get(p,0)+1
 rows.append(dict(page=p,source_row=counts[p],asset=a,owner='SP',activity=d,dates=[datetime.strptime('2010-'+date,'%Y-%m-%d').date().isoformat()],amount_min=int(lo),amount_max=int(hi),capital_gain_over_200=g,amount_scope='reported source row',database_status='missing_pre_2014'))
assert len(rows)==46
result=dict(source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2011/8202691.pdf',sha256=hashlib.sha256((r/'sources/2011-8202691.pdf').read_bytes()).hexdigest(),reporting_year=2010,status='schedule_visually_transcribed_pending_full_filing_and_amendment_reconciliation',row_count=len(rows),rows=rows)
(r/'verified-2010-schedule.json').write_text(json.dumps(result,indent=2))
print(len(rows),'rows; pages',counts)

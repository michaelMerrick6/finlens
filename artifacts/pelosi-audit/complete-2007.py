"""Persist manually visually transcribed Schedule IV rows; no database writes."""
import json
from pathlib import Path
from datetime import datetime
r=Path(__file__).parent
base=json.loads((r/'verified-2007-page15.json').read_text())
for x in base['rows']:x['page']=15
# page|asset|activity|dates (2007)|minimum|maximum
raw='''16|Cisco Systems, Inc. - Common Stock|buy|11-7,12-27,12-28,12-31|500001|1000000
16|Clean Energy Fuels Corp. - Public Common Stock|buy|5-25|50001|100000
16|CMGI, Inc. - Common Stock|partial_sale|11-2|1001|15000
16|Co-Nect - Common Stock|sell|6-22|1001|15000
16|Collaborative Group Investors, LP|sell|12-28|1001|15000
16|Currenex, LLC|sell|3-29|1001|15000
16|Dow Chemical Company - Public Common Stock|buy|3-14|15001|50000
16|Ebay Inc - Public Common Stock|buy|7-24|250001|500000
16|EMC Corp. Mass - Public Common Stock|buy|8-13,8-14|250001|500000
16|EMC Corp. Mass - Public Common Stock|sell|10-12|250001|500000
16|Evident - Common Stock|sell|12-28|1001|15000
17|Global Ambassador Concierge, LLC|buy|12-28|15001|50000
17|Golub Capital Partners|buy|4-10,7-25,8-29|1001|15000
17|Granite Ventures, LP|buy|1-24|15001|50000
17|Interactive Brokers Group Inc. - Public Common Stock|buy|5-4|100001|250000
17|Japan Partners, LP|sell|12-28|1001|15000
17|Learning Technologies, Inc. - Preferred Stock|sell|12-28|1001|15000
17|Liberate Technologies - Public Common Stock|sell|12-28|1001|15000
17|Lions Gate Limited Partnership (Cordevalle) - Golf Development Partnership|sell|4-18|250001|500000
17|Matthews International Fund - Mutual Fund|buy|6-12|15001|50000
17|MTI Cephus Partners|sell|12-28|1001|15000
17|Navis Investors, LP|sell|12-28|1001|15000
18|New River Pharmaceuticals - Public Common Stock|sell|6-7|500001|1000000
18|Niman Ranch, LLC|buy|3-28|1001|15000
18|Odyssey, LLC|buy|8-22|1001|15000
18|Oracle Corp. - Public Common Stock|sell|9-26|100001|250000
18|Pacific Island Resources, LLC|sell|12-28|1001|15000
18|Picture IQ Corp - Common Stock|sell|12-28|1001|15000
18|Plato Learning, Inc. - Common Stock|sell|12-28|1001|15000
18|Powerwave Tech Inc. - Public Common Stock|sell|12-27|50001|100000
18|Quest Energy Partners, LP|buy|11-8|250001|500000
18|Sandisk Corp. - Public Common Stock|buy|10-2,10-11|1000001|5000000
18|Sandisk Corp. - Public Common Stock|sell|12-27|500001|1000000
18|Starbucks Corporation - Public Common Stock|buy|7-24|250001|500000
19|Starbucks Corporation - Public Common Stock|partial_sale|12-27|250001|500000
19|Tom's of Maine, LLC|sell|11-14|1001|15000
19|Vanguard Airlines, Inc. - Public Common Stock|sell|12-28|1001|15000'''
counts={}
for line in raw.splitlines():
 page,asset,activity,dates,lo,hi=line.split('|');page=int(page);counts[page]=counts.get(page,0)+1
 base['rows'].append(dict(page=page,source_row=counts[page],asset=asset,owner='SP',activity=activity,dates=[datetime.strptime('2007-'+d,'%Y-%m-%d').date().isoformat() for d in dates.split(',')],amount_min=int(lo),amount_max=int(hi),amount_scope='reported source row; do not assign full range separately to each date',database_status='missing_pre_2014'))
base.pop('page',None);base['pages']=[15,16,17,18,19];base['row_count']=len(base['rows']);base['status']='schedule_visually_transcribed_pending_full_filing_and_amendment_reconciliation'
assert base['row_count']==49
assert len({(x['page'],x['source_row']) for x in base['rows']})==49
(r/'verified-2007-schedule.json').write_text(json.dumps(base,indent=2))
print('Saved',base['row_count'],'source rows;',sum(len(x['dates']) for x in base['rows']),'listed dates')

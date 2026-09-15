"""Read-only annual Schedule B reconciliation; candidate matches are not certification."""
import json,re,collections
from pathlib import Path
from datetime import datetime
from pypdf import PdfReader
root=Path(__file__).parent
db=json.loads((root/'stored-transactions.json').read_text())
manifest=json.loads((root/'manifest.json').read_text())
pattern=re.compile(r'(?P<date>\d{1,2}/\d{1,2}/\d{4})\s*(?P<direction>P|S|E)(?:\s*\(partial\))?\s*\$(?P<low>[\d,]+)\s*-\s*\$(?P<high>[\d,]+)')
reports=[]
for m in manifest:
 if m['FilingType'] not in ('O','A'):continue
 p=root/'sources'/f"{m['index_year']}-{m['DocID']}.pdf"
 pages=[x.extract_text().replace('\x00','') for x in PdfReader(p).pages]
 text='\n'.join(pages)
 start=re.search(r'(?:s\s*chedule\s*b:|S B:)',text,re.I)
 report={'doc_id':m['DocID'],'index_year':m['index_year'],'filing_type':m['FilingType'],'source':m['url'],'sha256':m['sha256'],'pages':len(pages),'rows':[]}
 if not start:
  report['status']='needs_ocr' if len(text)<100 else 'schedule_not_located';reports.append(report);continue
 end=re.search(r'(?:s\s*chedule\s*c:|S C:)',text[start.end():],re.I)
 section=text[start.end():start.end()+end.start() if end else len(text)]
 matches=list(pattern.finditer(section));used=set()
 for i,match in enumerate(matches):
  x=match.groupdict();iso=datetime.strptime(x['date'],'%m/%d/%Y').date().isoformat();low=int(x['low'].replace(',',''))
  candidates=[]
  for r in db:
   amount=re.search(r'\d[\d,]*',r.get('amount_range') or '')
   if r.get('transaction_date')==iso and r.get('transaction_type')=={'P':'buy','S':'sell','E':'exchange'}[x['direction']] and amount and int(amount[0].replace(',',''))==low:candidates.append(r)
  available=[r for r in candidates if r['id'] not in used]
  if available:used.add(available[0]['id'])
  offset=start.end()+match.start();page=next((j+1 for j in range(len(pages)) if offset < sum(len(s)+1 for s in pages[:j+1])),len(pages))
  x.update(transaction_date=iso,page=page,context=section[max(0,match.start()-160):matches[i+1].start() if i+1<len(matches) else len(section)],candidate_db_ids=[r['id'] for r in candidates],status='candidate_match' if available else 'unmatched_or_duplicate')
  report['rows'].append(x)
 report['status']='text_extracted_pending_visual_review';reports.append(report)
(root/'annual-reconciliation.json').write_text(json.dumps(reports,indent=2))
for r in reports:
 print(r['index_year'],r['doc_id'],r['status'],len(r['rows']),'unmatched',sum(x['status']!='candidate_match' for x in r['rows']))
print('total',sum(len(r['rows']) for r in reports))

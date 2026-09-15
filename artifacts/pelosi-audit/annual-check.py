import re,json,collections
from pathlib import Path
root=Path('artifacts/pelosi-audit');db=json.loads((root/'stored-transactions.json').read_text());text=(root/'sources/2025-10075701.txt').read_text().replace('\x00','');section=text.split('S B: T\n')[1].split('S C:')[0]
pattern=re.compile(r'(?P<asset>[^\n]+?\[(?P<type>[A-Z]+)\])\s+(?P<owner>SP|JT)\s+(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<direction>P|S)(?:\s*\(partial\))?\s+\$(?P<low>[\d,]+)\s*-\s*\$(?P<high>[\d,]+)')
ledger=[];matches=list(pattern.finditer(section))
for i,m in enumerate(matches):
 x=m.groupdict();tail=section[m.end():matches[i+1].start() if i+1<len(matches) else len(section)];x['description']=tail.strip();x['source']='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075701.pdf';date=x['date'];iso=f'{date[6:]}-{date[:2]}-{date[3:5]}';tick=re.search(r'\(([A-Z]+)\)',x['asset']);ticker=tick.group(1) if tick else None
 x['matched_db_ids']=[r['id'] for r in db if r.get('transaction_date')==iso and r.get('transaction_type')==('buy' if x['direction']=='P' else 'sell') and r.get('ticker')==ticker and (r.get('amount_range') or '').startswith('$'+x['low'])]
 x['economic_type']='contribution' if 'Contribution' in tail else 'option_exercise' if 'Exercised' in tail else 'option_purchase' if x['type']=='OP' and x['direction']=='P' else 'sale' if x['direction']=='S' else 'purchase'
 ledger.append(x)
(root/'annual-2025-ledger.json').write_text(json.dumps(ledger,indent=2));print('Extracted',len(ledger),'unmatched',[(x['asset'],x['date']) for x in ledger if not x['matched_db_ids']]);print(collections.Counter(x['economic_type'] for x in ledger))

"""Explicit source-reviewed supersession proposal; no DB writes."""
import json,hashlib
from pathlib import Path
p=Path(__file__).resolve().parent
rows=json.loads((p/'stored-transactions.json').read_text()); bydoc={r['doc_id']:r for r in rows}
pairs=[(f'house-2022-20020515-{i}',f'house-2022-20020561-{i}') for i in range(5)]
pairs += [('house-2021-20018011-0','house-2021-20018539-0'),('house-2021-20018355-0','house-2021-20018539-1'),('house-2021-20018355-1','house-2021-20018539-2')]
pairs += [('house-2020-20015042-1','house-2020-20016961-0'),('house-2020-20015042-2','house-2020-20016961-1')]
out=[]
for old,new in pairs:
 a,b=bydoc[old],bydoc[new]
 for key in ['ticker','transaction_date','transaction_type','amount_range']: assert a[key]==b[key],(key,a,b)
 n='-'.join(new.split('-')[1:3]); source=p/'sources'/f'{n}.pdf'
 out.append({'superseded_db_id':a['id'],'canonical_db_id':b['id'],'original_doc_id':old,'amended_doc_id':new,'source':b['source_url'],'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'page':1,'ticker':b['ticker'],'transaction_date':b['transaction_date'],'evidence':'Explicit Amended status; visual PDF review and original text comparison; matching asset/date/direction/range.','economic_type':'call_option_sale' if b['ticker']=='AMZN' else ('option_exercise_into_shares' if b['ticker']!='AB' else 'public_partnership_units'),'owner':'SP'})
(p/'modern-amendment-reconciliation.json').write_text(json.dumps({'status':'reviewed proposal, not applied','superseded_count':len(out),'rows':out},indent=2)+'\n')
print('Verified supersession pairs:',len(out))

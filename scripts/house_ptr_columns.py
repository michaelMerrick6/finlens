"""Extract PTR columns by dated row anchors, retaining account metadata and continuations."""
import re
import pdfplumber
from holdings_pdf_columns import header_geometry
from house_financial_disclosure_parser import normalize_line, clean_pdf_text, ASSET_TYPE_RE

NAMES=['ID','Owner','Asset','Transaction Type','Date','Notification Date','Amount','Cap. Gains > $200?']
DATE_RE=re.compile(r'^\d{2}/\d{2}/\d{4}$')

def parse_ptr_columns(path):
    events=[];issues=[];expected=0
    with pdfplumber.open(path) as pdf:
        for number,page in enumerate(pdf.pages,1):
            page_text=clean_pdf_text(page.extract_text() or '')
            expected+=len(ASSET_TYPE_RE.findall(page_text))
            geometry=header_geometry(page,'ptr')
            if not geometry:
                if ASSET_TYPE_RE.search(page_text):issues.append(dict(page=number,reason='transaction-header-unresolved'))
                continue
            top,xs=geometry;bottom=page.height-25
            words=page.extract_words()
            # A transaction-date cell anchors every row even when the PDF omits horizontal lines.
            anchors=sorted(w['top'] for w in words if xs[4]<=w['x0']<xs[5] and DATE_RE.fullmatch(w['text']))
            header_bottom=max((w['bottom'] for w in words if top<=w['top']<top+40 and w['text'] in ['Type','Date','$200?']),default=top+35)
            bounds=[header_bottom]+anchors+[bottom]
            for start,end in zip(bounds,bounds[1:]):
                start=max(top,start-.5);end-=.5
                if end<=start:continue
                block=page.crop((xs[0],start,xs[-1],end))
                lines=block.extract_text_lines()
                # Metadata spans columns; keep it whole, and out of financial cells.
                data_end=end
                for line in lines:
                    if re.match(r'^(?:F\s*S:|Filing Status:|S\s*O:|Subholding)',normalize_line(line['text']),re.I):
                        data_end=line['top']-.5;break
                if data_end<=start: cells=['']*8
                else:cells=[normalize_line(page.crop((a,start,b,data_end)).extract_text() or '') for a,b in zip(xs,xs[1:])]
                row=dict(zip(NAMES,cells))
                text=normalize_line(block.extract_text() or '')
                # First block is continuation of the preceding page, never a new transaction.
                is_new=bool(DATE_RE.fullmatch(row['Date']))
                if is_new:
                    if not re.fullmatch(r'[PSE](?:\s*\((?:partial|full)\))?',row['Transaction Type'],re.I):
                        issues.append(dict(page=number,reason='transaction-type-unresolved',cells=row))
                    events.append(dict(page=number,end_page=number,cells=row,metadata=[text]))
                elif events:
                    # Only the pre-metadata text belongs to the financial columns.
                    if ASSET_TYPE_RE.search(row['Asset']) and not ASSET_TYPE_RE.search(events[-1]['cells']['Asset']):
                        for key in NAMES:events[-1]['cells'][key]=normalize_line(events[-1]['cells'][key]+' '+row[key])
                        events[-1]['end_page']=number
                    elif row['Amount'] and not row['Asset'] and re.fullmatch(r'\$[\d,]+',row['Amount']):
                        events[-1]['cells']['Amount']+=' '+row['Amount']
                    events[-1]['metadata'].append(text)
    for event in events:
        if len(ASSET_TYPE_RE.findall(event['cells']['Asset']))!=1:
            issues.append(dict(page=event['page'],reason='transaction-asset-unresolved',cells=event['cells']))
    if len(events)!=expected:issues.append(dict(reason='transaction-count-mismatch',expected=expected,parsed=len(events)))
    return dict(events=events,issues=issues,asset_markers=expected,current_holdings_eligible=False)

if __name__=='__main__':
    import json,sys
    print(json.dumps(parse_ptr_columns(sys.argv[1]),indent=2))

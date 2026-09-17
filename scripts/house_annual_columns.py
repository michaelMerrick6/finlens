"""Stage annual asset cells using PDF column geometry; retain unresolved rows."""
import re
from pathlib import Path
import pdfplumber
from holdings_pdf_columns import header_geometry
from house_financial_disclosure_parser import clean_pdf_text, normalize_line, ASSET_TYPE_RE, VALUE_RANGE_RE, extract_ticker, iter_section_a_lines


def parse_columns(path):
    results=[];issues=[];pending=None;active=False
    lines,_=iter_section_a_lines(Path(path).read_bytes())
    expected=sum(len(ASSET_TYPE_RE.findall(line)) for line in lines)
    with pdfplumber.open(path) as pdf:
        for number,page in enumerate(pdf.pages,1):
            geometry=header_geometry(page,'annual')
            if not geometry:
                if active and re.search(r'Asset\s+Owner\s+Date\s+Tx\.', clean_pdf_text(page.extract_text() or '')):break
                if active and not re.search(r'(?:S|Schedule)\s+B:', clean_pdf_text(page.extract_text() or ''), re.I) and ASSET_TYPE_RE.search(clean_pdf_text(page.extract_text() or '')):
                    issues.append(dict(page=number,reason='asset-header-unresolved'))
                if active:break
                continue
            active=True
            top,xs=geometry;bottom=page.height-25
            for line in page.extract_text_lines():
                text=normalize_line(line['text'])
                if line['top']>top and re.match(r'^(?:S|Schedule)\s+B:',text,re.I):bottom=line['top']-1;break
            crop=page.crop((xs[0],top,xs[-1],bottom))
            tables=crop.find_tables({'vertical_strategy':'explicit','explicit_vertical_lines':xs,
                                     'horizontal_strategy':'lines','explicit_horizontal_lines':[top,bottom]})
            for table in tables:
                for row_number,row in enumerate(table.extract()):
                    original_cells=[normalize_line(c or '') for c in row]
                    boxes=[cell for cell in table.rows[row_number].cells if cell]
                    lo=min(c[1] for c in boxes);hi=max(c[3] for c in boxes)
                    metadata=[]
                    for line in page.crop((xs[0],lo,xs[-1],hi)).extract_text_lines():
                        if re.match(r'^(?:L|D|C)\s*:',normalize_line(line['text'])):
                            metadata=original_cells
                            stop=line['top']-.5
                            if stop>lo:row=[page.crop((a,lo,b,stop)).extract_text() or '' for a,b in zip(xs,xs[1:])]
                            break
                    cells=[normalize_line(c or '') for c in row]
                    if len(cells)!=6 or cells[:3]==['Asset','Owner','Value of Asset'] or not any(cells):continue
                    asset=cells[0]
                    if not asset or re.match(r'^(?:L|D|C)\s*:|^\*|^Filing ID',asset):continue
                    types=list(ASSET_TYPE_RE.finditer(asset))
                    if not types:
                        if pending: issues.append(dict(**pending,reason='untyped-row-before-next-row'))
                        pending=dict(cells=cells,page=number)
                        continue
                    if pending:
                        cells=[' '.join([a,b]).strip() for a,b in zip(pending['cells'],cells)]
                        source_page=pending['page'];pending=None
                    else:source_page=number
                    asset=cells[0];types=list(ASSET_TYPE_RE.finditer(asset))
                    value=VALUE_RANGE_RE.fullmatch(cells[2])
                    if len(types)!=1 or not value:
                        issues.append(dict(page=source_page,cells=cells,reason='asset-or-value-unresolved'));continue
                    name=asset[:types[0].start()].strip()
                    if not name:
                        issues.append(dict(page=source_page,cells=cells,reason='asset-name-unresolved'));continue
                    results.append(dict(row_index=len(results),asset_name=name,asset_type_code=types[0][1],ticker=extract_ticker(name),
                                        owner=cells[1] or None,value_range=value[0],income_type=cells[3],income_range=cells[4],
                                        source_page=source_page,end_page=number,source_cells=cells,source_metadata=metadata))
    if pending:issues.append(dict(**pending,reason='unfinished-asset-row'))
    if expected!=len(results):issues.append(dict(reason='asset-marker-count-mismatch',expected=expected,parsed=len(results)))
    return dict(holdings=results,issues=issues,asset_markers=expected,current_holdings_eligible=False)

if __name__=='__main__':
    import json,sys
    print(json.dumps(parse_columns(Path(sys.argv[1])),indent=2))

"""Regression coverage for column extraction and account matching safeguards."""
import tempfile
import unittest
from pathlib import Path
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
from house_annual_columns import parse_columns
from house_ptr_columns import parse_ptr_columns
from reconcile_priority_holdings import match_position, dollar_range, finish, charitable

# Small PDF fixtures reproduce missing row borders and columns, independent of live data.
def pdf(path, annual=False):
    w=PdfWriter();p=w.add_blank_page(width=780,height=800)
    f=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    p[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):w._add_object(f)})})
    ops=[]
    def text(x,y,s):
        s=s.replace('\\','\\\\').replace('(','\\(').replace(')','\\)')
        ops.append(f'BT /F1 8 Tf {x} {800-y} Td ({s}) Tj ET')
    xs=[20,320,365,480,575,665,750] if annual else [20,55,95,370,445,520,595,710,760]
    for a,b in zip(xs,xs[1:]):ops.append(f'{a} 730 {b-a} 30 re S')
    headers=['Asset','Owner','Value of Asset','Income Type','Income','Current Value'] if annual else ['ID','Owner','Asset','Transaction Type','Date','Notification Date','Amount','Cap. Gains']
    for x,s in zip(xs,headers):text(x+2,55,s)
    if annual:
        for y,cells in [(100,['Savings [5F]','SP','$15,001 - $50,000','Tax-Deferred','','']), (140,['Apple (AAPL) [ST]','JT','$1,001 - $15,000','Dividends','$201 - $1,000',''])]:
            for x,s in zip(xs,cells):text(x+2,y,s)
            if y==140:text(22,151,'D: This description spans the asset value and income columns; it is not an asset value.')
            ops.append(f'20 {800-y-15} m 750 {800-y-15} l S')
        text(20,195,'Schedule B: Transactions')
    else:
        for y,cells in [(100,['','JT','Apple (AAPL) [ST]','S (partial)','01/02/2026','01/03/2026','$1,001 - $15,000','']), (160,['','JT','Microsoft (MSFT) [ST]','P','01/04/2026','01/05/2026','$15,001 - $50,000',''])]:
            for x,s in zip(xs,cells):text(x+2,y,s)
            text(97,y+18,'F S: New')
            text(97,y+30,'S O: Joint brokerage')
        # Deliberately no transaction row borders.
    stream=DecodedStreamObject();stream.set_data('\n'.join(ops).encode());p[NameObject('/Contents')]=w._add_object(stream)
    with open(path,'wb') as out:w.write(out)

class ColumnTests(unittest.TestCase):
    def test_borderless_ptr_rows_and_sale_type(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'ptr.pdf';pdf(p);r=parse_ptr_columns(p)
        self.assertEqual(r['issues'],[])
        self.assertEqual(len(r['events']),2)
        self.assertEqual(r['events'][0]['cells']['Transaction Type'],'S (partial)')
        self.assertEqual(r['events'][1]['cells']['Amount'],'$15,001 - $50,000')
        self.assertIn('Joint brokerage',r['events'][0]['metadata'][0])
    def test_annual_5f_is_not_merged_into_next_stock(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'annual.pdf';pdf(p,True);r=parse_columns(p)
        self.assertEqual(len(r['holdings']),2)
        self.assertEqual(r['holdings'][0]['asset_type_code'],'5F')
        self.assertEqual(r['holdings'][1]['value_range'],'$1,001 - $15,000')
        self.assertEqual(r['holdings'][1]['income_range'],'$201 - $1,000')
    def test_account_and_owner_must_match(self):
        ps=[dict(id='one',ticker='AAPL',owner='DC',account='Child1'),dict(id='two',ticker='AAPL',owner='DC',account='Child2')]
        self.assertIsNone(match_position(dict(ticker='AAPL',owner='DC',account=None),ps))
        self.assertEqual(match_position(dict(ticker='AAPL',owner='DC',account='Child1'),ps),'one')
        self.assertIsNone(match_position(dict(ticker='AAPL',owner='not-stated',account='Child1'),ps))
    def test_ranges_do_not_treat_none_as_zero(self):
        self.assertIsNone(dollar_range('None'))
        self.assertIsNone(dollar_range('Over $50,000,000'))
        self.assertEqual(dollar_range('$1,001 - $15,000'),[1001,15000])
    def test_amendments_and_prebaseline_events_not_applied(self):
        base=dict(date='2026-01-01',owner='JT',account='joint',name='Stock',action='S',range='$1,001 - $15,000',position_id='one',source='a',amendment=False)
        events=[dict(base,id='old',after_baseline=False),dict(base,id='amended',after_baseline=True,amendment=True),dict(base,id='x',after_baseline=True),dict(base,id='y',after_baseline=True,source='b')]
        r=finish({},'2025-12-31',[dict(id='one',events=[],charitable=False)],events,[])
        self.assertEqual(r['counts'],{'already-in-baseline-period':1,'amendment-requires-original-row':1,'possible-cross-filing-duplicate':2})
        self.assertFalse(r['current_holdings_eligible'])
    def test_deleted_sale_and_its_possible_original_are_quarantined(self):
        base=dict(date='2026-08-05',owner='JT',account='foundation',ticker='VSNT',name='Stock',action='S',range='$1,001 - $15,000',position_id='one',amendment=False,after_baseline=True)
        original=dict(base,id='old',source='a',filing_status='New')
        deletion=dict(base,id='deleted',source='b',filing_status='Deleted')
        r=finish({},'2025-12-31',[dict(id='one',events=[],charitable=True)],[original,deletion],[])
        self.assertEqual(original['review_status'],'possible-original-of-correction')
        self.assertEqual(deletion['review_status'],'deletion-requires-original-row')
        self.assertEqual(deletion['candidate_original_ids'],['old'])

    def test_charitable_assets_are_not_personal_portfolio(self):
        self.assertTrue(charitable('Hern Family Foundation'))
        self.assertTrue(charitable('David McCormick Charitable Gift Fund'))
        self.assertFalse(charitable('Family Revocable Trust'))

if __name__=='__main__':unittest.main()

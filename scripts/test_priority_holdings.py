import unittest
from types import SimpleNamespace
from audit_priority_holdings import matches_house, senate_reports
from senate_annual_holdings import parse_senate_annual

HEAD = '<h1>Annual Report for Calendar 2025</h1><h2>Jane Doe (Doe, Jane)</h2>'
TABLE = '<table><tr>'+''.join('<th>'+x+'</th>' for x in ['', 'Asset','Asset Type','Owner','Value','Income Type','Income'])+'</tr>'
def row(*cells): return '<tr>'+''.join('<td>'+x+'</td>' for x in cells)+'</tr>'
CONTAINER = row('1','Joint brokerage','Brokerage/Managed Account','Joint','--','','')
STOCK = row('1.1','AAPL - Apple','Corporate Securities Stock','Spouse','$15,001 - $50,000','Dividends','$201 - $1,000')

class PriorityTests(unittest.TestCase):
    def test_explicit_name_and_district_mapping(self):
        member=dict(first='Rohit',last='Khanna',district='CA17')
        self.assertTrue(matches_house(dict(First='Rohit',Last='Khanna',StateDst='CA17'),member))
        self.assertFalse(matches_house(dict(First='Lisa Vedernikova',Last='Khanna',StateDst='VA01'),member))
        self.assertFalse(matches_house(dict(First='Rohit',Last='Khanna',StateDst='CA01'),member))
    def test_senate_asset_columns_and_accounts_are_preserved(self):
        result=parse_senate_annual(HEAD+TABLE+CONTAINER+STOCK+'</table>','Jane','Doe')
        self.assertEqual(result['valuation_date'],'2025-12-31')
        self.assertTrue(result['holdings'][0]['is_container'])
        asset=result['holdings'][1]
        self.assertEqual(asset['account_path'],['Joint brokerage'])
        self.assertEqual(asset['owner'],'Spouse')
        self.assertEqual(asset['value_range'],'$15,001 - $50,000')
        self.assertEqual(asset['income_range'],'$201 - $1,000')
        self.assertFalse(result['current_holdings_eligible'])
    def test_low_value_bank_identifier_is_not_stock_or_zero(self):
        bank=row('1','QIWSQ - IRA Cash','Bank Deposit','Self','None (or less than $1,001)','None','None')
        result=parse_senate_annual(HEAD+TABLE+bank+'</table>','Jane','Doe')
        self.assertIsNone(result['holdings'][0]['ticker'])
        self.assertEqual(result['holdings'][0]['value_range'],'None (or less than $1,001)')
    def test_identity_and_missing_or_duplicate_rows_fail(self):
        for html in [HEAD.replace('(Doe, Jane)','(Doe, Janet)')+TABLE+CONTAINER+'</table>', HEAD+TABLE+STOCK+'</table>', HEAD+TABLE+CONTAINER+CONTAINER+'</table>',HEAD+'<table><tr><th>Transactions</th></tr></table>']:
            with self.assertRaises(ValueError):parse_senate_annual(html,'Jane','Doe')
    def test_new_filer_date_not_assumed_and_exempt_assets_flagged(self):
        html=HEAD.replace('Annual Report for Calendar 2025','New Filer Report for 03/24/2026')+TABLE+CONTAINER+'</table><input name="filing_omitted_assets" checked><p>I omitted assets because they meet the three-part test for exemption.</p>'
        result=parse_senate_annual(html,'Jane','Doe')
        self.assertIsNone(result['valuation_date'])
        self.assertTrue(result['declared_exempt_assets'])
    def test_unchecked_exemption_is_not_a_declaration(self):
        html=HEAD+TABLE+CONTAINER+'</table><input name="filing_omitted_assets"><p>I omitted assets because they meet the three-part test for exemption.</p>'
        result=parse_senate_annual(html,'Jane','Doe')
        self.assertFalse(result['declared_exempt_assets'])
        self.assertNotIn('filer-declared-exempt-assets', result['issues'])

    def test_incomplete_senate_pagination_fails(self):
        pages=iter([{'recordsFiltered':1,'data':[]}])
        session=SimpleNamespace(cookies={},post=lambda *a,**k:SimpleNamespace(raise_for_status=lambda:None,json=lambda:next(pages)))
        with self.assertRaises(ValueError):senate_reports(session,dict(first='Jane',last='Doe'))

if __name__=='__main__':unittest.main()

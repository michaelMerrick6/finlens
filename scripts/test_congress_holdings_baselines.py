import unittest
from unittest.mock import patch
from types import SimpleNamespace
from prepare_congress_holdings_baselines import validate_document, name_match_basis, explicitly_no_assets, select_latest_annual
from house_financial_disclosure_parser import is_section_stop, iter_section_a_lines, build_asset_blocks, parse_asset_block

class BaselineTests(unittest.TestCase):
    def validate(self, name='Jane Doe', year='2025', owner='SP', rows=None):
        text=f'Name: Hon. {name}\nStatus: Member\nFiling Type: Annual Report\nFiling Year: {year}\nFiling Date: 05/15/2026\nFiling ID #12345'
        reader=SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda:text)])
        lines=rows or [f'Apple (AAPL) [ST] {owner} $1,001 - $15,000 Dividends']
        with patch('prepare_congress_holdings_baselines.PdfReader',return_value=reader), patch('prepare_congress_holdings_baselines.iter_section_a_lines',return_value=(lines,text)):
            return validate_document(b'fixture',{'name':'Jane Doe','member_id':'D000001'}, {'year':2025,'filed':'5/15/2026','doc_id':'12345','source':'https://disclosures-clerk.house.gov/test.pdf'})
    def test_newer_reporting_year_wins_over_later_old_filing(self):
        old = dict(year=2024, doc_id='older', type='A', filed='5/15/2026')
        new = dict(year=2025, doc_id='newer', type='O', filed='5/14/2026')
        self.assertEqual(select_latest_annual([old,new]), new)
        old['filed'] = new['filed']
        self.assertEqual(select_latest_annual([old,new]), new)

    def test_same_year_conflicts_and_amendments_stay_blocked(self):
        base = dict(year=2025, doc_id='original', type='O', filed='5/14/2026')
        with self.assertRaises(ValueError):
            select_latest_annual([base,dict(base, doc_id='other', filed='05/14/2026')])
        with self.assertRaises(ValueError):
            select_latest_annual([base,dict(base, doc_id='amended', type='W', filed='5/15/2026')])

    def test_spouse_dependent_value_category_preserved(self):
        holding = parse_asset_block('Fund [MF] SP Spouse/DC Over $1,000,000 Dividends $201 - $1,000')
        self.assertEqual(holding.owner, 'SP')
        self.assertEqual(holding.value_range, 'Spouse/DC Over $1,000,000')
        self.assertIsNone(parse_asset_block('Fund [MF] SP Spouse/DC OverDividends $201 - $1,000 $1,000,000'))

    def test_rejected_blocks_preserve_source_row_identity(self):
        row = self.validate(rows=['Unknown Fund [MF] Dividends $1,001 - $2,500', 'Apple (AAPL) [ST] SP $15,001 - $50,000'])
        self.assertEqual(row['holdings'][0]['row_index'], 1)
        self.assertEqual(row['rejected_blocks'][0]['row_index'], 0)
        self.assertIn('Unknown Fund', row['rejected_blocks'][0]['extracted_text'])
        self.assertEqual(row['typed_asset_markers'], 2)
        self.assertEqual(row['status'], 'review-required')

    def test_complete_owned_value_not_repaired_from_next_line(self):
        blocks = build_asset_blocks(['Fund [MF] SP $100,001 - $250,000 Dividends', '$250,000'])
        self.assertEqual(parse_asset_block(blocks[0]).value_range, '$100,001 - $250,000')

    def test_typed_row_with_split_value_and_income_columns(self):
        lines = ['Vanguard Long Term Treasury [EF] $100,001 - Dividends $5,001 -', '$250,000 $15,000', 'Vanguard Target 2015 [MF] $100,001 -', '$250,000', 'None']
        holdings = [parse_asset_block(b) for b in build_asset_blocks(lines)]
        self.assertEqual(len(holdings), 2)
        self.assertEqual([h.value_range for h in holdings], ['$100,001 - $250,000'] * 2)
        self.assertEqual(holdings[1].asset_name, 'Vanguard Target 2015')

    def test_missing_asset_value_cannot_fall_through_to_income(self):
        for block in ['Fund [MF] Dividends $1,001 - $2,500', 'Fund [MF] SP $100,001 - Dividends $5,001 - $15,000', 'Fund [MF] $250,000 None']:
            self.assertIsNone(parse_asset_block(block))

    def test_mismatched_typed_value_continuation_stays_unparsed(self):
        blocks = build_asset_blocks(['Fund [MF] $100,001 - Dividends $5,001 -', '$15,000 $15,000'])
        self.assertIsNone(parse_asset_block(blocks[0]))

    def test_suffix_normalization_is_narrow(self):
        self.assertEqual(name_match_basis('Jane Doe Jr.', 'Jane Doe'), 'generational-suffix-normalized')
        self.assertEqual(name_match_basis('Jane Doe III', 'Jane Doe'), 'generational-suffix-normalized')
        for actual in ['Janet Doe Jr.', 'Jane A. Doe Jr.', 'Jane Roe III', 'Jane Doe Sr.']:
            self.assertIsNone(name_match_basis(actual, 'Jane Doe Jr.'))
        row = self.validate(name='Jane Doe Jr.')
        self.assertEqual(row['source_member_name'], 'Jane Doe Jr.')
        self.assertNotIn('member-name-mismatch', row['issues'])

    def test_explicit_empty_schedule_is_bounded(self):
        self.assertTrue(explicitly_no_assets('S A: A "U" I\nNone disclosed.\nS B: T'))
        for text in ['None disclosed.', 'S B: T\nNone disclosed.\nS C: E',
                     'S A: Assets\nNone disclosed.',
                     'S A: Assets\nNone disclosed.\nApple [ST] $1,001 - $15,000\nS B: T']:
            self.assertFalse(explicitly_no_assets(text))

    def test_explicit_empty_filing_stays_unpublished(self):
        text = 'Name: Hon. Jane Doe\nStatus: Member\nFiling Type: Annual Report\nFiling Year: 2025\nFiling Date: 05/15/2026\nFiling ID #12345\nS A: Assets\nNone disclosed.\nS B: Transactions'
        reader = SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda: text)])
        with patch('prepare_congress_holdings_baselines.PdfReader', return_value=reader), patch('prepare_congress_holdings_baselines.iter_section_a_lines', return_value=([], text)):
            row = validate_document(b'fixture', {'name':'Jane Doe','member_id':'D000001'}, {'year':2025,'filed':'5/15/2026','doc_id':'12345','source':'fixture'})
        self.assertEqual(row['status'], 'machine-checked-awaiting-review')
        self.assertTrue(row['explicit_no_assets'])
        self.assertFalse(row['current_holdings_eligible'])
        self.assertEqual(row['holdings'], [])

    def test_garbled_schedule_headers_stop_transaction_contamination(self):
        self.assertTrue(is_section_stop('S B: T'))
        self.assertFalse(is_section_stop('S A: A "U" I'))
        page=SimpleNamespace(extract_text=lambda:'Asset Owner Value of Asset\nApple (AAPL) [ST] SP $1,001 - $15,000\nS B: T\nTesla (TSLA) [ST] P $50,001 - $100,000')
        with patch('house_financial_disclosure_parser.PdfReader',return_value=SimpleNamespace(pages=[page])):
            lines,_=iter_section_a_lines(b'fixture')
        self.assertTrue(any('Apple' in line for line in lines))
        self.assertFalse(any('Tesla' in line for line in lines))

    def test_account_value_before_asset_across_page_boundary(self):
        lines=['Apple (AAPL) [ST] SP $50,001 - $100,000 Dividends', 'Brokerage IRA ⇒ JT $1,001 - $15,000 Tax-Deferred', 'Municipal ETF (FMB) [EF]']
        parsed=[parse_asset_block(b) for b in build_asset_blocks(lines)]
        self.assertEqual(len(parsed),2)
        self.assertEqual(parsed[0].value_range,'$50,001 - $100,000')
        self.assertEqual(parsed[1].value_range,'$1,001 - $15,000')
        self.assertEqual(parsed[1].owner,'JT')
        self.assertIn('Brokerage IRA',parsed[1].asset_name)
    def test_explicit_next_asset_value_is_not_reassigned(self):
        blocks=build_asset_blocks(['Brokerage ⇒ $1,001 - $15,000', 'Apple (AAPL) [ST] SP $50,001 - $100,000'])
        self.assertEqual(parse_asset_block(blocks[-1]).value_range,'$50,001 - $100,000')

    def test_split_value_does_not_use_income_range(self):
        lines=['Brokerage ⇒ JT $100,001 - Capital Gains, $1,001 -', 'Value ETF (EFV) [EF] $250,000 Dividends $2,500']
        p=parse_asset_block(build_asset_blocks(lines)[0])
        self.assertEqual(p.value_range,'$100,001 - $250,000')
        self.assertEqual(p.owner,'JT')
        self.assertEqual(p.ticker,'EFV')
    def test_wrapped_asset_between_value_halves(self):
        lines=['Merrill Lynch ⇒ $50,001 - Dividends $5,001 -','PIMCO Active Bond Exchange-Traded Fund Exchange-','Traded Fund (BOND) [EF]','$100,000 $15,000']
        p=parse_asset_block(build_asset_blocks(lines)[0])
        self.assertEqual(p.value_range,'$50,001 - $100,000')
        self.assertEqual(p.ticker,'BOND')
    def test_mismatched_upper_bound_not_repaired(self):
        lines=['Brokerage ⇒ JT $100,001 - Capital Gains, $1,001 -','Value ETF (EFV) [EF] $2,500']
        self.assertIsNone(parse_asset_block(build_asset_blocks(lines)[0]))

    def test_damaged_description_label_does_not_rename_next_asset(self):
        lines=['Apple (AAPL) [ST] SP $1,001 - $15,000','D: New in 2024','Brokerage ⇒ JT $100,001 - Dividends $1,001 -','Value ETF (EFV) [EF] $250,000 $2,500']
        p=parse_asset_block(build_asset_blocks(lines)[1])
        self.assertEqual(p.asset_name,'Brokerage ⇒ Value ETF (EFV)')

    def test_undetermined_value_is_preserved_not_zero(self):
        p=parse_asset_block('State Pension [PE] JT Undetermined Tax-Deferred')
        self.assertEqual(p.value_range,'Undetermined')
        self.assertEqual(p.asset_type_code,'PE')
    def test_full_value_before_wrapped_asset_with_income_tail(self):
        lines=['Merrill Lynch ⇒ SP $15,001 - $50,000 Dividends $1,001 -','BlackRock Inflation Protected Bond Portfolio -','Institutional (BPRIX) [MF]','$2,500']
        p=parse_asset_block(build_asset_blocks(lines)[0])
        self.assertEqual(p.ticker,'BPRIX')
        self.assertEqual(p.value_range,'$15,001 - $50,000')
        self.assertEqual(p.owner,'SP')
    def test_partial_asset_value_does_not_use_complete_income_bracket(self):
        lines=['Brokerage ⇒ $15,001 - Dividends $201 - $1,000','Fund (ABC) [MF]']
        self.assertIsNone(parse_asset_block(build_asset_blocks(lines)[0]))

    def test_account_heading_survives_multiline_asset_name(self):
        lines=['Apple (AAPL) [ST] SP $1,001 - $15,000','IRA ⇒','BlackRock Inflation Protected Bond Portfolio -','Institutional (BPRIX) [MF]','$15,001 - $50,000 Dividends']
        parsed=[parse_asset_block(b) for b in build_asset_blocks(lines)]
        self.assertEqual(len(parsed),2)
        self.assertTrue(parsed[1].asset_name.startswith('IRA ⇒ BlackRock'))
        self.assertNotIn('IRA',parsed[0].asset_name)

    def test_date_and_not_auto_published(self):
        row=self.validate()
        self.assertEqual(row['valuation_date'],'2025-12-31')
        self.assertEqual(row['status'],'machine-checked-awaiting-review')
        self.assertFalse(row['current_holdings_eligible'])
    def test_identity_and_year_fail_closed(self):
        self.assertIn('member-name-mismatch',self.validate(name='Other Person')['issues'])
        self.assertIn('reporting-year-mismatch',self.validate(year='2024')['issues'])
    def test_missing_quantity_and_repeated_owners_preserved(self):
        row=self.validate(rows=['Apple (AAPL) [ST] SP $1,001 - $15,000 Dividends','Apple (AAPL) [ST] JT $15,001 - $50,000 Dividends'])
        self.assertEqual(len(row['holdings']),2)
        self.assertEqual([h['owner'] for h in row['holdings']],['SP','JT'])
        self.assertIsNone(self.validate(owner='')['holdings'][0]['owner'])
    def test_unparsed_asset_blocks_review(self):
        row=self.validate(rows=['Apple (AAPL) [ST] SP unknown value'])
        self.assertIn('incomplete-asset-blocks',row['issues'])

if __name__=='__main__':unittest.main()

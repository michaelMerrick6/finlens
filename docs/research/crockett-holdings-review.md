# Jasmine Crockett holdings pilot

Reviewed September 17, 2026. Member C001130 / TX30.

## Evidence

- Official annual: https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10074813.pdf
- SHA-256: 70a9effc3b2ee33c48d5ea2c5db707bda38273d60ca7de15ab88cabc637196b7
- Both pages rendered and visually inspected. Filing year 2025, filed August 12, 2026. Schedule A on page 1 has exactly three stock entries: DVN, MGM, MRNA, each $1,001–$15,000. Owner columns are blank; do not assume spouse ownership.
- Schedule B says None disclosed for the reporting year. Schedule E lists organization roles, not valued assets. Liabilities are not subtracted from the stock total.
- Live 2025 and 2026 House indexes checked on September 17: annual 10074813 and extension 30026765 are the only Jasmine Crockett rows. Douglas Crockett in VA09 is a different person. No subsequent PTR found in those indexes. This does not establish that no unreported transaction occurred.

## Model

Use December 31, 2025 as the annual valuation date. Divide the disclosed bracket by the last available daily close on or before that date, within seven days; multiply the resulting quantity range by the latest completed daily close, no older than five days. Yahoo quote.close is split-adjusted, not dividend-adjusted; both endpoints use the same series, so do not apply another split multiplier. Fractional modeled shares are allowed; exact quantities are unknown. No dividend reinvestment or unreported transaction is assumed.

Rank and illustrative allocation use midpoint values, explicitly marked as modeling assumptions; broad intervals overlap. A partial pricing failure suppresses the aggregate and allocation, while retaining independently priced rows. Missing prices never become zero balances.

## Maintenance and limits

The server caches a source-verified snapshot keyed by UTC date, refreshing on the first visit of a new day. It checks annual PDF hash and all House index years from 2025 through the current year. New filings, identity conflicts, a missing baseline, changed PDF, malformed index, or source outage suppress current estimates. This is demand-driven daily checking, not a scheduled background job. New transaction interpretation is not automated in this pilot: the user sees a review-needed state until evidence is incorporated.

Only C001130 was added to the public route allowlist beside Pelosi. No broad machine-parsed staging rows were promoted, no database writes, and no production deployment.

## Validation

Seven focused tests cover source identity, new/backdated filings, valuation, split basis, stale prices, source changes and outages. TypeScript and focused ESLint checks pass. Local route returns 200 and browser inspection shows all three stocks, value ranges, allocation, and source details.

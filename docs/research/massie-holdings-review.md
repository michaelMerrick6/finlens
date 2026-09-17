# Thomas Massie holdings pilot

Reviewed September 17, 2026. Member M001184 / KY04.

## Source review

Official annual: https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10077444.pdf

SHA-256: eea9811015030f89f0da5512ad43bd227e13062080e32a38d724ac4770843fb1

Both pages were rendered and visually inspected. Filing year 2025; filed May 15, 2026. The annual baseline is December 31, 2025. All owner fields are blank. Schedule A contains exactly five entries:

| Entry | Reported annual value | Treatment |
| --- | --- | --- |
| Tesla (TSLA), common stock | $15,001–$50,000 | Model current shares/value as a range |
| Federal Credit Union | $15,001–$50,000 | Preserve historical bank balance range |
| Howard Massie Farms, LLC, 50% interest | $1,000,001–$5,000,000 | Preserve reported interest and historical range; no additional 50% scaling |
| People's Bank | $15,001–$50,000 | Preserve historical bank balance range |
| Shireware, LLC, 33% interest | $1,001–$15,000 | Preserve reported interest and historical range; no additional 33% scaling |

Schedule B says None disclosed for 2025. Schedule D lists two mortgages; the stock estimate is not net worth and does not subtract them. Page 2 declares no excluded trust or spouse/dependent information. This does not establish completeness beyond the public report.

The official 2025 House index contains annual 10077444, matching Thomas Massie / KY04 / O / May 15, 2026. The 2026 index contains no Massie rows at review time. No subsequent PTR or amended annual was found in these indexes. Annual source hash is rechecked by the live page.

## Valuation and interface

Use the same conditional unchanged-position price-ratio method as Crockett: annual dollar bracket divided by the baseline daily close, multiplied by the latest completed close. This does not establish exact shares. It assumes no unreported changes or dividend reinvestment. Both endpoints use the same split-adjusted, non-dividend-adjusted series.

All four non-stock entries remain visible in a separate section with historical dates. The graphic explicitly describes the public-stock portion: Tesla at 100% is not 100% of his entire portfolio. No farm, business or bank values are revalued or included in the stock estimate.

## Implementation and validation

Reviewed baselines now share an explicit member registry, member-specific index checks, dated cache keys including member ID and source hash, and a reusable page. Unreviewed IDs remain inaccessible. New filings and source failures suppress modeled values pending review. Daily refresh remains demand-driven; new trades are not automatically parsed.

71 backend tests, TypeScript and focused ESLint checks pass. Both Massie and Crockett local routes return 200; browser inspection confirms Massie's stock graphic, value range, other-assets section and source details. No production deployment or database writes.

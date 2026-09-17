import otherAssets from './pelosi-other-assets.json';
import changes from '../../artifacts/pelosi-audit/2026-position-changes.json';
import baseline from '../../artifacts/pelosi-audit/2025-holdings-baseline.json';

export function getPelosiOtherAssets() {
  return otherAssets.rows.map(row => ({ ...row,
    source: `${otherAssets.source}#page=${row.page}`,
    // Exact reviewed entity mapping: punctuation differs between annual and PTR.
    changes: row.name === 'REOF XXV, LLC' ? changes.events.filter(e => e.ticker === 'REOF XXV LLC') : [],
  }));
}
export function getPelosiExcludedAssets() {
  return [
    ...baseline.rows.filter(r => r.reported_value === 'None').map(r => ({ name: `${r.ticker_as_filed} ${r.asset_type_as_filed === 'OP' ? 'options' : 'stock'}`, source: `${baseline.source}#page=${r.page}` })),
    ...getPelosiOtherAssets().filter(r => r.reported_value === 'None').map(r => ({ name: r.name, source: r.source })),
  ];
}

'use client';

import { useState } from 'react';
import { analysisNumber as number, type AnalysisData } from '@/lib/analysis-overview';
import { Icon } from './icon';
import styles from './analysis.module.css';

export function AnalysisCharts({ data, options, sector, onSector }: {
  data: AnalysisData; options: boolean; sector: string; onSector: (sector: string) => void;
}) {
  const [showAllSectors, setShowAllSectors] = useState(false);
  const overview = data.overview;
  const visibleSectors = showAllSectors ? overview.sectors : overview.sectors.slice(0, 5);
  const buyPercent = overview.total ? overview.buys / overview.total * 100 : 0;
  const directions = [
    { name: 'Purchases', count: overview.buys, percent: buyPercent, color: '#315846' },
    { name: 'Sales', count: overview.sells, percent: overview.total ? 100 - buyPercent : 0, color: '#b79b77' },
  ];

  return <div className={styles.charts}>
    <section className={styles.card} aria-labelledby="activity-mix-title">
      <div className={styles.cardHeading}><h2 id="activity-mix-title">The period at a glance</h2></div>
      {overview.total ? <>
        <div className={styles.activityTotal}><strong>{number(overview.total)}</strong><span>reported {options ? 'option ' : ''}trades</span></div>
        <p className={styles.activityContext}>Across <strong>{number(overview.stocks)}</strong> {options ? 'underlying stocks' : 'stocks & ETFs'} and <strong>{number(overview.members)}</strong> politicians.</p>
        <div className={styles.activityBar} role="img" aria-label={`${number(overview.buys)} purchases and ${number(overview.sells)} sales`}>
          {directions.filter(row => row.count > 0).map(row => <span key={row.name}
            style={{ width: `${row.percent}%`, background: row.color }} title={`${row.name}: ${number(row.count)} (${row.percent.toFixed(1)}%)`}/>) }
        </div>
        <div className={styles.mixLegend}>{directions.map(row => <div key={row.name}>
          <span className={styles.legendLabel}><i style={{ background: row.color }}/>{row.name}</span>
          <strong>{number(row.count)}<small>{row.percent.toFixed(1)}%</small></strong>
        </div>)}</div>
        <p className={styles.chartNote}>Transaction counts, including household disclosures.</p>
      </> : <div className={styles.chartEmpty}>No recorded {options ? 'option ' : ''}purchases or sales in this period.</div>}
    </section>
    <section className={styles.card} aria-labelledby="activity-sectors-title">
      <div className={styles.cardHeading}><div><h2 id="activity-sectors-title">Sector activity</h2><p>{!showAllSectors && overview.sectors.length > 5 ? 'Top 5 · ' : ''}{options ? 'Underlying company sectors' : 'Share of transactions'}</p></div></div>
      {overview.total ? <>
        <div id="activity-sector-list" className={styles.sectors}>{visibleSectors.map(row => <button key={row.name} className={styles.sectorRow}
          aria-pressed={sector === row.name} onClick={() => onSector(sector === row.name ? '' : row.name)}
          aria-label={`${row.name}: ${number(row.count)} transactions, ${row.percent.toFixed(1)} percent. View stocks.`}>
          <span className={styles.sectorLabel}><span>{row.name}</span><small>{row.percent.toFixed(1)}%</small></span>
          <span className={styles.sectorTrack}><span style={{ width: `${row.percent}%`, background: row.color }}/></span>
        </button>)}</div>
        <div className={styles.sectorFooter}>
          <p className={styles.chartNote}>Percentages use all {number(overview.total)} transactions.</p>
          {overview.sectors.length > 5 && <button className={styles.sectorToggle} aria-expanded={showAllSectors} aria-controls="activity-sector-list"
            onClick={() => setShowAllSectors(value => !value)}>{showAllSectors ? 'Show top 5' : `View all sectors (${overview.sectors.length})`}<Icon name="chevron" size={12}/></button>}
        </div>
      </> : <div className={styles.chartEmpty}>Sector activity will appear when trades are available.</div>}
    </section>
  </div>;
}

'use client';

import { useState, type ReactNode } from 'react';
import { Icon } from '@/components/icon';
import { compactMoney, strategyMoney } from '@/lib/strategies/strategy-format';
import styles from './strategy-dashboard.module.css';

export type StrategyMix = {
  positions: number;
  segments: { ticker: string; name: string; value: number; percent: number; color: string }[];
};
export type StrategySector = { name: string; count: number; percent: number; color: string };

export function StrategyOverview({ mix, sectors, onSelect, mixDescription, mixLabel, mixNote, sectorDescription, highlight }: {
  mix: StrategyMix; sectors: StrategySector[];
  onSelect: (ticker?: string, sector?: string) => void;
  mixDescription: string; mixLabel: string; mixNote: string; sectorDescription: string;
  highlight?: ReactNode;
}) {
  const [hovered, setHovered] = useState<string | null>(null);
  const selected = mix.segments.find(row => row.ticker === hovered);
  const starts = mix.segments.map((_, i) => mix.segments.slice(0, i).reduce((sum, row) => sum + row.percent, 0));
  return <div className={styles.overview}>
    <section className={styles.card} aria-labelledby="mix-title">
      <div className={styles.cardHeading}><div><h2 id="mix-title">Holdings mix</h2><p>{mixDescription}</p></div><span className={styles.badge}>Estimated</span></div>
      <div className={styles.mixBody}>
        <div className={styles.donut}>
          <svg viewBox="0 0 240 240" aria-label={mixLabel} role="img">
            <circle cx="120" cy="120" r="94" fill="none" stroke="#f0f2ec" strokeWidth="27"/>
            {mix.segments.map((row, i) => <circle key={row.ticker} cx="120" cy="120" r="94" fill="none" stroke={row.color}
              strokeWidth={hovered === row.ticker ? 33 : 27} pathLength="100"
              strokeDasharray={`${Math.max(0, row.percent - .7)} ${100 - Math.max(0, row.percent - .7)}`}
              strokeDashoffset={-starts[i]} transform="rotate(-90 120 120)" className={styles.donutSlice}
              onMouseEnter={() => setHovered(row.ticker)} onMouseLeave={() => setHovered(null)} onClick={() => onSelect(row.ticker)}>
              <title>{`${row.name}: ${row.percent.toFixed(1)}% of shown estimates, ${strategyMoney(row.value)}`}</title>
            </circle>)}
          </svg>
          <div className={styles.donutCenter} aria-live="polite"><strong>{selected ? `${selected.percent.toFixed(1)}%` : mix.positions}</strong><span>{selected ? selected.name : 'priced positions'}</span></div>
        </div>
        <div className={styles.legend}>{mix.segments.map(row => <button key={row.ticker} className={styles.legendRow}
          onMouseEnter={() => setHovered(row.ticker)} onMouseLeave={() => setHovered(null)} onFocus={() => setHovered(row.ticker)} onBlur={() => setHovered(null)}
          onClick={() => onSelect(row.ticker)} aria-label={`View ${row.name}; ${row.percent.toFixed(1)} percent of displayed estimates`}>
          <span className={styles.dot} style={{ background: row.color }}/><span className={styles.legendName}><strong>{row.ticker === 'Other' ? row.name : row.ticker}</strong>{row.ticker !== 'Other' && <small>{row.name}</small>}</span>
          <span className={styles.legendValue}>{compactMoney(row.value)}<small>{row.percent.toFixed(1)}%</small></span>
        </button>)}{!mix.segments.length && <p className={styles.chartNote}>Recent prices are unavailable. Holdings and their source records are listed below.</p>}</div>
      </div>
      {highlight}
      <p className={styles.chartNote}>{mixNote}</p>
    </section>
    <section className={styles.card} aria-labelledby="sectors-title">
      <div className={styles.cardHeading}><div><h2 id="sectors-title">Sector breakdown</h2><p>{sectorDescription}</p></div><Icon name="grid" size={18}/></div>
      <div className={styles.sectors}>{sectors.map(row => <button key={row.name} className={styles.sectorRow}
        aria-label={`View ${row.name}: ${row.count} positions, ${row.percent.toFixed(1)} percent of the reviewed list`} onClick={() => onSelect('', row.name)}>
        <span className={styles.sectorLabel}><span>{row.name}</span><span>{row.count}<small> · {row.percent.toFixed(1)}%</small></span></span>
        <span className={styles.sectorTrack}><span style={{ width: `${row.percent}%`, background: row.color }}/></span>
      </button>)}</div>
      <p className={styles.chartNote}>Select a sector to explore its stocks. Percentages count positions, not invested dollars.</p>
    </section>
  </div>;
}

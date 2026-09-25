import type { EventSummary, Stats } from '../api/types'
import { histogram, percent } from '../format'
import type { Dict } from '../i18n'
import { Card } from './ui'

const SPAN = 36 * 3600 * 1000
const BUCKETS = 36

export function Kpis({ stats, t }: { stats: Stats; t: Dict }) {
  const s = stats.events_by_status
  const d = stats.deliveries_by_status
  const finished = (d.delivered ?? 0) + (d.dead ?? 0)
  const items = [
    { label: t.total, value: stats.events_total, tone: '' },
    { label: t.delivered, value: s.delivered ?? 0, tone: 'ok' },
    { label: t.inProgress, value: s.processing ?? 0, tone: 'info' },
    { label: t.failed, value: (s.failed ?? 0) + (s.partial ?? 0), tone: 'bad' },
    { label: t.successRate, value: percent(d.delivered ?? 0, finished), tone: '' },
    { label: t.queue, value: stats.queue.ready + stats.queue.scheduled, tone: 'warn', hint: t.queueHint(stats.queue.ready, stats.queue.scheduled) },
  ]
  return (
    <div className="kpis">
      {items.map((item) => (
        <div key={item.label} className={`kpi kpi-${item.tone}`}>
          <span className="kpi-label">{item.label}</span>
          <strong className="kpi-value">{item.value}</strong>
          {item.hint && <span className="kpi-hint">{item.hint}</span>}
        </div>
      ))}
    </div>
  )
}

export function ActivityChart({ events, now, t }: { events: EventSummary[]; now: number; t: Dict }) {
  const ok = histogram(events.filter((e) => e.status !== 'failed' && e.status !== 'partial').map((e) => Date.parse(e.received_at)), now, SPAN, BUCKETS)
  const bad = histogram(events.filter((e) => e.status === 'failed' || e.status === 'partial').map((e) => Date.parse(e.received_at)), now, SPAN, BUCKETS)
  const max = Math.max(1, ...ok.map((v, i) => v + (bad[i] ?? 0)))
  const w = 100 / BUCKETS
  return (
    <Card title={t.activity} className="activity">
      <svg viewBox="0 0 100 40" preserveAspectRatio="none" className="bars" role="img" aria-label={t.activity}>
        {ok.map((v, i) => {
          const b = bad[i] ?? 0
          const hOk = (v / max) * 38
          const hBad = (b / max) * 38
          return (
            <g key={i}>
              <rect x={i * w + 0.25} width={w - 0.5} y={40 - hOk - hBad} height={hOk} className="bar-ok" rx="0.4" />
              {b > 0 && <rect x={i * w + 0.25} width={w - 0.5} y={40 - hBad} height={hBad} className="bar-bad" rx="0.4" />}
            </g>
          )
        })}
      </svg>
      <div className="axis">
        {t.axis.map((label) => (
          <span key={label}>{label}</span>
        ))}
      </div>
    </Card>
  )
}

export function Breakdown({ stats, t }: { stats: Stats; t: Dict }) {
  const sources = Object.entries(stats.events_by_source).sort((a, b) => b[1] - a[1])
  const maxSource = Math.max(1, ...sources.map(([, v]) => v))
  const destinations = Object.entries(stats.deliveries_by_destination).sort()
  return (
    <>
      <Card title={t.bySource} className="breakdown">
        <ul className="hbars">
          {sources.map(([source, value]) => (
            <li key={source}>
              <span className="hbar-label">{source}</span>
              <span className="hbar-track">
                <span className="hbar-fill" style={{ width: `${(value / maxSource) * 100}%` }} />
              </span>
              <span className="hbar-value">{value}</span>
            </li>
          ))}
        </ul>
      </Card>
      <Card title={t.byDestination} className="breakdown">
        <ul className="stack-list">
          {destinations.map(([name, counts]) => {
            const total = Object.values(counts).reduce((a, b) => a + (b ?? 0), 0)
            return (
              <li key={name}>
                <span className="hbar-label">{name}</span>
                <span className="stack">
                  {(['delivered', 'retrying', 'pending', 'dead'] as const).map((status) =>
                    counts[status] ? (
                      <span key={status} className={`seg seg-${status}`} style={{ width: `${(counts[status] / total) * 100}%` }} title={`${t.statuses[status]}: ${counts[status]}`} />
                    ) : null,
                  )}
                </span>
                <span className="hbar-value">{total}</span>
              </li>
            )
          })}
        </ul>
      </Card>
    </>
  )
}

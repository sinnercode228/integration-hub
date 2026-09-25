import type { ConnectorsInfo } from '../api/types'
import { formatSeconds } from '../format'
import type { Dict } from '../i18n'
import { Card, ConnectorIcon } from './ui'

export function ConnectorsView({ info, t }: { info: ConnectorsInfo | null; t: Dict }) {
  if (!info) return null
  return (
    <div className="connectors">
      <div className="grid-2">
        <Card title={t.sources}>
          <ul className="conn-list">
            {info.sources.map((s) => (
              <li key={s.id}>
                <ConnectorIcon kind={s.connector} />
                <div>
                  <strong>{s.id}</strong> <span className="muted">· {s.connector}</span>
                  <small>{s.description}</small>
                  <code className="endpoint">POST /webhooks/{s.id}</code>
                </div>
                <span className={`tag ${s.signed ? 'tag-ok' : 'tag-warn'}`}>{s.signed ? t.signed : t.unsigned}</span>
              </li>
            ))}
            {info.stock && (
              <li>
                <ConnectorIcon kind="moysklad" />
                <div>
                  <strong>stock</strong> <span className="muted">· {info.stock.connector}</span>
                  <small>MoySklad stock proxy with TTL cache, single-flight and stale-if-error</small>
                  <code className="endpoint">GET /api/stock?sku=…</code>
                </div>
              </li>
            )}
          </ul>
        </Card>
        <Card title={t.destinations}>
          <ul className="conn-list">
            {info.destinations.map((d) => (
              <li key={d.id}>
                <ConnectorIcon kind={d.connector} />
                <div>
                  <strong>{d.id}</strong> <span className="muted">· {d.connector}</span>
                  <small>{d.description}</small>
                </div>
              </li>
            ))}
          </ul>
        </Card>
      </div>
      <Card title={t.routes}>
        <ul className="routes">
          {info.routes.map((r) => (
            <li key={r.name}>
              <div className="route-head">
                <strong className="mono">{r.name}</strong>
                <span className="muted">{r.description}</span>
              </div>
              <div className="route-flow">
                <span className="chip">{(r.match.source ?? ['*']).join(', ')}</span>
                <span className="chip chip-type">{(r.match.type ?? ['*']).join(', ')}</span>
                {r.match.where.map((c) => (
                  <span key={c.path} className="chip chip-cond">
                    {t.when}: {c.path} {c.op} {JSON.stringify(c.value)}
                  </span>
                ))}
                <span className="arrow">→</span>
                {r.deliver.map((d) => (
                  <span key={d.to} className="chip chip-dest">
                    {d.to}
                  </span>
                ))}
              </div>
            </li>
          ))}
        </ul>
      </Card>
      <Card title={t.retrySchedule}>
        <div className="schedule">
          {info.retry_schedule_seconds.map((s, i) => (
            <span key={i} className="step">
              <small>#{i + 2}</small>
              {formatSeconds(s)}
            </span>
          ))}
          <span className="step step-dead">DLQ</span>
        </div>
      </Card>
    </div>
  )
}

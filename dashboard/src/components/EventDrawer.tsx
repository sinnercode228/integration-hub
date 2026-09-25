import { useEffect } from 'react'
import type { EventRecord } from '../api/types'
import { formatDuration, formatTime } from '../format'
import type { Dict, Lang } from '../i18n'
import { ConnectorIcon, Json, StatusBadge } from './ui'

interface Props {
  event: EventRecord
  onClose: () => void
  onReplayEvent: () => void
  onReplayDelivery: (id: string) => void
  lang: Lang
  now: number
  t: Dict
}

export function EventDrawer({ event, onClose, onReplayEvent, onReplayDelivery, lang, now, t }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const hasDead = event.deliveries.some((d) => d.status === 'dead')
  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-modal="true" aria-label={event.id} onClick={(e) => e.stopPropagation()}>
        <header className="drawer-head">
          <div>
            <div className="drawer-kicker">
              <span className="source">{event.source}</span> <code>{event.type}</code>
            </div>
            <h2 className="mono">{event.id}</h2>
            <div className="drawer-meta">
              <StatusBadge status={event.status} t={t} />
              <span className="muted">{formatTime(event.received_at, lang, now)}</span>
            </div>
          </div>
          <div className="drawer-actions">
            {hasDead && (
              <button type="button" className="btn btn-primary" onClick={onReplayEvent}>
                ↻ {t.replayAll}
              </button>
            )}
            <button type="button" className="btn btn-ghost" onClick={onClose} aria-label={t.close}>
              ✕
            </button>
          </div>
        </header>

        <div className="drawer-body">
          <dl className="facts">
            <div>
              <dt>{t.contact}</dt>
              <dd>
                {[event.contact.name, event.contact.phone, event.contact.email].filter(Boolean).join(' · ') || '—'}
              </dd>
            </div>
            <div>
              <dt>{t.externalId}</dt>
              <dd className="mono">{event.external_id ?? '—'}</dd>
            </div>
            <div>
              <dt>{t.idempotency}</dt>
              <dd className="mono small">{event.idempotency_key}</dd>
            </div>
          </dl>

          <h3 className="section-title">{t.deliveries}</h3>
          {event.deliveries.length === 0 && <p className="muted">{t.statuses.no_route}</p>}
          {event.deliveries.map((d) => (
            <article key={d.id} className={`delivery delivery-${d.status}`}>
              <header className="delivery-head">
                <ConnectorIcon kind={d.connector} />
                <div className="delivery-title">
                  <strong>{d.destination}</strong>
                  <small className="muted">
                    {d.route} · {d.connector}
                    {d.replays > 0 && ` · replays: ${d.replays}`}
                  </small>
                </div>
                <StatusBadge status={d.status} t={t} />
                {(d.status === 'dead' || d.status === 'delivered') && (
                  <button type="button" className="btn btn-small" onClick={() => onReplayDelivery(d.id)}>
                    ↻ {t.replay}
                  </button>
                )}
              </header>
              <ol className="timeline">
                {d.attempts.map((a) => (
                  <li key={a.number} className={a.ok ? 'ok' : 'fail'}>
                    <span className="tl-dot" />
                    <span className="tl-main">
                      {t.attempt} #{a.number} · {a.status_code ?? (a.ok ? 'OK' : 'ERR')} · {formatDuration(a.duration_ms)}
                      {a.retry_in_seconds != null && <em> · {t.retryIn(a.retry_in_seconds)}</em>}
                    </span>
                    <span className="tl-time muted">{new Date(a.started_at).toLocaleTimeString(lang === 'ru' ? 'ru-RU' : 'en-GB')}</span>
                    {a.error && <code className="tl-error">{a.error}</code>}
                  </li>
                ))}
                {(d.status === 'pending' || d.status === 'retrying') && d.next_attempt_at && (
                  <li className="next">
                    <span className="tl-dot" />
                    <span className="tl-main">
                      {t.nextAttempt}: {Math.max(0, Math.ceil((Date.parse(d.next_attempt_at) - now) / 1000))} s
                    </span>
                  </li>
                )}
              </ol>
              <details>
                <summary>{t.payload}</summary>
                <Json value={d.payload} />
              </details>
              {d.result && (
                <details>
                  <summary>{t.result}</summary>
                  <Json value={d.result} />
                </details>
              )}
            </article>
          ))}

          <details className="block" open>
            <summary>{t.normalized}</summary>
            <Json value={event.fields} />
          </details>
          <details className="block">
            <summary>{t.raw}</summary>
            <Json value={event.raw} />
          </details>
        </div>
      </aside>
    </div>
  )
}

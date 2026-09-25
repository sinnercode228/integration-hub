import type { DeadLetterItem } from '../api/types'
import { formatTime } from '../format'
import type { Dict, Lang } from '../i18n'
import { ConnectorIcon } from './ui'

interface Props {
  items: DeadLetterItem[] | null
  onReplay: (deliveryId: string) => void
  onOpen: (eventId: string) => void
  lang: Lang
  now: number
  t: Dict
}

export function DeadLettersView({ items, onReplay, onOpen, lang, now, t }: Props) {
  return (
    <div className="dead">
      <p className="muted hint">{t.deadHint}</p>
      {items && items.length === 0 && <div className="empty-state">✓ {t.noDead}</div>}
      <ul className="dead-list">
        {items?.map(({ delivery, event }) => (
          <li key={delivery.id} className="dead-item">
            <ConnectorIcon kind={delivery.connector} />
            <div className="dead-main">
              <div className="dead-title">
                <strong>{delivery.destination}</strong>
                <span className="muted"> ← </span>
                {event ? (
                  <button type="button" className="link" onClick={() => onOpen(event.id)}>
                    {event.source} · {event.type} · {event.contact.name ?? event.external_id ?? event.id}
                  </button>
                ) : (
                  <span className="mono">{delivery.event_id}</span>
                )}
              </div>
              <code className="tl-error">{delivery.last_error}</code>
              <small className="muted">
                {t.attempts}: {delivery.attempts.length} · {formatTime(delivery.updated_at, lang, now)}
              </small>
            </div>
            <button type="button" className="btn btn-primary btn-small" onClick={() => onReplay(delivery.id)}>
              ↻ {t.replay}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

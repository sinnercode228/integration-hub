import type { ReactNode } from 'react'
import type { DeliveryStatus, EventStatus } from '../api/types'
import type { Dict } from '../i18n'

export type AnyStatus = EventStatus | DeliveryStatus

const TONE: Record<AnyStatus, string> = {
  delivered: 'ok',
  processing: 'info',
  pending: 'info',
  retrying: 'warn',
  partial: 'warn',
  failed: 'bad',
  dead: 'bad',
  no_route: 'muted',
}

export function StatusBadge({ status, t }: { status: AnyStatus; t: Dict }) {
  return (
    <span className={`badge badge-${TONE[status]}`} data-status={status}>
      <span className="dot" aria-hidden />
      {t.statuses[status]}
    </span>
  )
}

export function DeliveryDots({ statuses }: { statuses: { id: string; destination: string; status: DeliveryStatus }[] }) {
  return (
    <span className="dots">
      {statuses.map((d) => (
        <span key={d.id} className={`pip pip-${TONE[d.status]}`} title={`${d.destination}: ${d.status}`} />
      ))}
    </span>
  )
}

export function Card({ title, children, className = '', actions }: { title?: ReactNode; children: ReactNode; className?: string; actions?: ReactNode }) {
  return (
    <section className={`card ${className}`}>
      {(title || actions) && (
        <header className="card-head">
          {title && <h3>{title}</h3>}
          {actions}
        </header>
      )}
      {children}
    </section>
  )
}

export function Json({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>
}

export function ConnectorIcon({ kind }: { kind: string }) {
  const letters: Record<string, string> = {
    tilda: 'Ti', amocrm: 'am', amocrm_lead: 'am', bitrix24: 'B24', generic: '{ }', telegram: 'TG', google_sheets: 'GS', smtp: '@', webhook: '↗', moysklad: 'MS',
  }
  return <span className={`cicon cicon-${kind}`}>{letters[kind] ?? kind.slice(0, 2)}</span>
}

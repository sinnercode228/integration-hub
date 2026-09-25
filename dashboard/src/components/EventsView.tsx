import type { EventPage, EventStatus } from '../api/types'
import { formatTime } from '../format'
import type { Dict, Lang } from '../i18n'
import { DeliveryDots, StatusBadge } from './ui'

export interface EventFilters {
  status: EventStatus | ''
  source: string
  q: string
}

const STATUSES: EventStatus[] = ['processing', 'delivered', 'partial', 'failed', 'no_route']

interface Props {
  page: EventPage | null
  filters: EventFilters
  sources: string[]
  onFilters: (filters: EventFilters) => void
  onOpen: (id: string) => void
  onMore: () => void
  lang: Lang
  now: number
  t: Dict
}

export function EventsView({ page, filters, sources, onFilters, onOpen, onMore, lang, now, t }: Props) {
  return (
    <div className="events">
      <div className="filters">
        <input
          type="search"
          value={filters.q}
          placeholder={t.search}
          aria-label={t.search}
          onChange={(e) => onFilters({ ...filters, q: e.target.value })}
        />
        <select aria-label={t.status} value={filters.status} onChange={(e) => onFilters({ ...filters, status: e.target.value as EventStatus | '' })}>
          <option value="">{t.allStatuses}</option>
          {STATUSES.map((s) => (
            <option key={s} value={s}>
              {t.statuses[s]}
            </option>
          ))}
        </select>
        <select aria-label={t.source} value={filters.source} onChange={(e) => onFilters({ ...filters, source: e.target.value })}>
          <option value="">{t.allSources}</option>
          {sources.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>
      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              <th>{t.received}</th>
              <th>{t.source}</th>
              <th>{t.type}</th>
              <th>{t.contact}</th>
              <th>{t.deliveries}</th>
              <th>{t.status}</th>
            </tr>
          </thead>
          <tbody>
            {page?.items.map((e) => (
              <tr key={e.id} onClick={() => onOpen(e.id)} tabIndex={0} onKeyDown={(k) => k.key === 'Enter' && onOpen(e.id)}>
                <td className="nowrap muted" title={e.received_at}>
                  {formatTime(e.received_at, lang, now)}
                </td>
                <td>
                  <span className="source">{e.source}</span>
                </td>
                <td>
                  <code>{e.type}</code>
                </td>
                <td className="contact-cell">
                  <span>{e.contact.name ?? '—'}</span>
                  <small>{e.contact.phone ?? e.contact.email ?? e.external_id ?? ''}</small>
                </td>
                <td>
                  <DeliveryDots statuses={e.deliveries} />
                </td>
                <td>
                  <StatusBadge status={e.status} t={t} />
                </td>
              </tr>
            ))}
            {page && page.items.length === 0 && (
              <tr className="empty-row">
                <td colSpan={6}>{t.empty}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {page && (
        <div className="table-foot">
          <span className="muted">{t.shown(page.items.length, page.total)}</span>
          {page.items.length < page.total && (
            <button type="button" className="btn btn-ghost" onClick={onMore}>
              {t.loadMore}
            </button>
          )}
        </div>
      )}
    </div>
  )
}

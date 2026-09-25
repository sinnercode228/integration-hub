import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createApi, readToken, writeToken } from './api'
import type { ConnectorsInfo, DeadLetterItem, EventPage, EventRecord, EventSummary, RelayApi, SimulationKind, Stats } from './api/types'
import { ConnectorsView } from './components/ConnectorsView'
import { DeadLettersView } from './components/DeadLettersView'
import { EventDrawer } from './components/EventDrawer'
import { EventsView, type EventFilters } from './components/EventsView'
import { ActivityChart, Breakdown, Kpis } from './components/Overview'
import { DICTS, type Lang } from './i18n'

type Tab = 'events' | 'dead' | 'connectors'
const KINDS: SimulationKind[] = ['tilda-form', 'tilda-order', 'amocrm-won', 'bitrix-lead', 'partner']
const POLL_MS = 2000
const PAGE = 25

function initialLang(): Lang {
  try {
    const saved = localStorage.getItem('relay.lang')
    if (saved === 'ru' || saved === 'en') return saved
  } catch {
    /* ignore */
  }
  return navigator.language.toLowerCase().startsWith('ru') ? 'ru' : 'en'
}

export default function App({ api: injected }: { api?: RelayApi }) {
  const api = useMemo(() => injected ?? createApi(), [injected])
  const [lang, setLang] = useState<Lang>(initialLang)
  const t = DICTS[lang]
  const [tab, setTab] = useState<Tab>('events')
  const [now, setNow] = useState(() => Date.now())
  const [stats, setStats] = useState<Stats | null>(null)
  const [recent, setRecent] = useState<EventSummary[]>([])
  const [page, setPage] = useState<EventPage | null>(null)
  const [limit, setLimit] = useState(PAGE)
  const [filters, setFilters] = useState<EventFilters>({ status: '', source: '', q: '' })
  const [dead, setDead] = useState<DeadLetterItem[] | null>(null)
  const [connectors, setConnectors] = useState<ConnectorsInfo | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [detail, setDetail] = useState<EventRecord | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [toast, setToast] = useState<string | null>(null)
  const [token, setToken] = useState(() => readToken() ?? '')
  const [version, setVersion] = useState(0)
  const toastTimer = useRef<number | undefined>(undefined)

  const refresh = useCallback(() => setVersion((v) => v + 1), [])

  const notify = useCallback((message: string) => {
    setToast(message)
    window.clearTimeout(toastTimer.current)
    toastTimer.current = window.setTimeout(() => setToast(null), 2600)
  }, [])

  useEffect(() => {
    try {
      localStorage.setItem('relay.lang', lang)
    } catch {
      /* ignore */
    }
    document.documentElement.lang = lang
  }, [lang])

  // Poll: the mock advances its simulated worker on every read, the live API is just re-read.
  useEffect(() => {
    const id = window.setInterval(() => {
      setNow(Date.now())
      refresh()
    }, POLL_MS)
    return () => window.clearInterval(id)
  }, [refresh])

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const [s, r] = await Promise.all([api.stats(), api.events({ limit: 200 })])
        const query = { status: filters.status || undefined, source: filters.source || undefined, q: filters.q.trim() || undefined, limit }
        const main = tab === 'events' ? await api.events(query) : null
        const d = tab === 'dead' ? await api.deadLetters() : null
        const ev = selected ? await api.event(selected) : null
        if (cancelled) return
        setStats(s)
        setRecent(r.items)
        if (main) setPage(main)
        if (d) setDead(d)
        setDetail(ev)
        setError(null)
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err))
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [api, tab, filters, limit, selected, version])

  // Config rarely changes: load it once (retried on every poll until it succeeds, e.g. after a token is set).
  const connectorsLoaded = connectors !== null
  useEffect(() => {
    if (!connectorsLoaded) api.connectors().then(setConnectors, () => undefined)
  }, [api, connectorsLoaded, version])

  const act = async (fn: () => Promise<unknown>, message: string) => {
    try {
      await fn()
      notify(message)
      refresh()
    } catch (err) {
      notify(`${t.apiError}: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  const simulate = async (kind: SimulationKind) => {
    if (!api.simulate) return
    const id = await api.simulate(kind)
    setTab('events')
    setFilters({ status: '', source: '', q: '' })
    notify(`${t.kinds[kind]} → ${id}`)
    refresh()
  }

  const sources = connectors?.sources.map((s) => s.id) ?? Object.keys(stats?.events_by_source ?? {})
  const deadCount = stats?.queue.dead ?? 0

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden>
            <svg viewBox="0 0 32 32">
              <path d="M5 16h8l3-7 4 14 3-7h4" />
            </svg>
          </span>
          <div>
            <h1>Relay</h1>
            <p>{t.subtitle}</p>
          </div>
          <span className={`mode mode-${api.mode}`}>{api.mode === 'mock' ? t.demoMode : t.liveMode}</span>
        </div>
        <div className="top-actions">
          {api.mode === 'live' && (
            <form
              className="token"
              onSubmit={(e) => {
                e.preventDefault()
                writeToken(token || null)
                refresh()
              }}
            >
              <input type="password" placeholder={t.token} value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" />
              <button className="btn btn-ghost" type="submit">
                {t.tokenSave}
              </button>
            </form>
          )}
          <div className="lang" role="group" aria-label="Language">
            {(['ru', 'en'] as const).map((l) => (
              <button key={l} type="button" className={l === lang ? 'active' : ''} onClick={() => setLang(l)}>
                {l.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
      </header>

      {api.simulate && (
        <section className="simulate" aria-label={t.simulate}>
          <div>
            <strong>{t.simulate}</strong>
            <span className="muted">{t.simulateHint}</span>
          </div>
          <div className="sim-buttons">
            {KINDS.map((kind) => (
              <button key={kind} type="button" className="btn btn-sim" onClick={() => void simulate(kind)}>
                + {t.kinds[kind]}
              </button>
            ))}
          </div>
        </section>
      )}

      {error && (
        <div className="alert" role="alert">
          {t.apiError}: {error}
        </div>
      )}

      <main>
        {stats && <Kpis stats={stats} t={t} />}
        {stats && (
          <div className="overview">
            <ActivityChart events={recent} now={now} t={t} />
            <Breakdown stats={stats} t={t} />
          </div>
        )}

        <nav className="tabs" role="tablist">
          <button role="tab" aria-selected={tab === 'events'} className={tab === 'events' ? 'active' : ''} onClick={() => setTab('events')}>
            {t.events} {stats && <span className="count">{stats.events_total}</span>}
          </button>
          <button role="tab" aria-selected={tab === 'dead'} className={tab === 'dead' ? 'active' : ''} onClick={() => setTab('dead')}>
            {t.deadLetters} {deadCount > 0 && <span className="count count-bad">{deadCount}</span>}
          </button>
          <button role="tab" aria-selected={tab === 'connectors'} className={tab === 'connectors' ? 'active' : ''} onClick={() => setTab('connectors')}>
            {t.connectors}
          </button>
        </nav>

        <div className="tab-panel">
          {tab === 'events' && (
            <EventsView
              page={page}
              filters={filters}
              sources={sources}
              onFilters={(f) => {
                setFilters(f)
                setLimit(PAGE)
              }}
              onOpen={setSelected}
              onMore={() => setLimit((l) => l + PAGE)}
              lang={lang}
              now={now}
              t={t}
            />
          )}
          {tab === 'dead' && (
            <DeadLettersView
              items={dead}
              onReplay={(id) => void act(() => api.replayDelivery(id), t.replayDone(1))}
              onOpen={setSelected}
              lang={lang}
              now={now}
              t={t}
            />
          )}
          {tab === 'connectors' && <ConnectorsView info={connectors} t={t} />}
        </div>
      </main>

      {selected && detail && detail.id === selected && (
        <EventDrawer
          event={detail}
          onClose={() => setSelected(null)}
          onReplayEvent={() => void act(async () => api.replayEvent(detail.id), t.replayDone(detail.deliveries.filter((d) => d.status === 'dead').length))}
          onReplayDelivery={(id) => void act(() => api.replayDelivery(id), t.replayDone(1))}
          lang={lang}
          now={now}
          t={t}
        />
      )}

      {toast && (
        <div className="toast" role="status">
          {toast}
        </div>
      )}

      <footer className="footer">
        <span>{t.footer}</span>
        <span>
          <a href="https://github.com/sinnercode228/integration-hub" target="_blank" rel="noreferrer">
            GitHub
          </a>
          {' · '}
          <a href="https://t.me/sinnercode" target="_blank" rel="noreferrer">
            Telegram @sinnercode
          </a>
        </span>
      </footer>
    </div>
  )
}

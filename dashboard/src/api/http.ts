import type {
  ConnectorsInfo,
  DeadLetterItem,
  DeliveryRecord,
  EventPage,
  EventQuery,
  EventRecord,
  RelayApi,
  ReplayResponse,
  Stats,
} from './types'

export class ApiError extends Error {
  readonly status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

/** Client for the real Relay admin API (``/admin/api/*``, Bearer token). */
export class HttpRelayApi implements RelayApi {
  readonly mode = 'live' as const
  private readonly base: string
  private readonly token: () => string | null
  private readonly fetcher: typeof fetch

  constructor(base: string, token: () => string | null, fetcher: typeof fetch = fetch.bind(globalThis)) {
    this.base = base.replace(/\/$/, '')
    this.token = token
    this.fetcher = fetcher
  }

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    const token = this.token()
    if (token) headers.set('Authorization', `Bearer ${token}`)
    const response = await this.fetcher(`${this.base}/admin/api${path}`, { ...init, headers })
    if (!response.ok) {
      let detail = response.statusText
      try {
        const body = (await response.json()) as { detail?: unknown }
        if (typeof body.detail === 'string') detail = body.detail
      } catch {
        /* non-JSON error body */
      }
      throw new ApiError(response.status, detail)
    }
    return (await response.json()) as T
  }

  stats(): Promise<Stats> {
    return this.request('/stats')
  }

  events(query: EventQuery): Promise<EventPage> {
    const params = new URLSearchParams()
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined && value !== '') params.set(key, String(value))
    }
    const qs = params.toString()
    return this.request(`/events${qs ? `?${qs}` : ''}`)
  }

  event(id: string): Promise<EventRecord> {
    return this.request(`/events/${encodeURIComponent(id)}`)
  }

  replayEvent(id: string, includeDelivered = false): Promise<ReplayResponse> {
    const qs = includeDelivered ? '?include_delivered=true' : ''
    return this.request(`/events/${encodeURIComponent(id)}/replay${qs}`, { method: 'POST' })
  }

  replayDelivery(id: string): Promise<DeliveryRecord> {
    return this.request(`/deliveries/${encodeURIComponent(id)}/replay`, { method: 'POST' })
  }

  deadLetters(): Promise<DeadLetterItem[]> {
    return this.request('/dead-letters')
  }

  connectors(): Promise<ConnectorsInfo> {
    return this.request('/connectors')
  }
}

// Mirrors backend/src/relay/api/schemas.py and relay/domain.py.

export type DeliveryStatus = 'pending' | 'retrying' | 'delivered' | 'dead'
export type EventStatus = 'processing' | 'delivered' | 'partial' | 'failed' | 'no_route'

export interface Contact {
  name: string | null
  phone: string | null
  email: string | null
}

export interface Attempt {
  number: number
  started_at: string
  duration_ms: number
  ok: boolean
  status_code: number | null
  error: string | null
  retry_in_seconds: number | null
}

export interface DeliveryRecord {
  id: string
  event_id: string
  route: string
  destination: string
  connector: string
  status: DeliveryStatus
  payload: unknown
  result: Record<string, unknown> | null
  attempts: Attempt[]
  attempt_base: number
  replays: number
  next_attempt_at: string | null
  last_error: string | null
  created_at: string
  updated_at: string
}

export interface EventRecord {
  id: string
  source: string
  connector: string
  type: string
  external_id: string | null
  idempotency_key: string
  received_at: string
  status: EventStatus
  contact: Contact
  fields: Record<string, unknown>
  raw: Record<string, unknown>
  deliveries: DeliveryRecord[]
}

export interface DeliveryBrief {
  id: string
  route: string
  destination: string
  connector: string
  status: DeliveryStatus
  attempts: number
  last_error: string | null
  next_attempt_at: string | null
  updated_at: string
}

export interface EventSummary {
  id: string
  source: string
  connector: string
  type: string
  status: EventStatus
  external_id: string | null
  received_at: string
  contact: Contact
  deliveries: DeliveryBrief[]
}

export interface EventPage {
  items: EventSummary[]
  total: number
  limit: number
  offset: number
}

export interface QueueInfo {
  ready: number
  scheduled: number
  in_flight: number
  dead: number
}

export interface Stats {
  events_total: number
  events_by_status: Partial<Record<EventStatus, number>>
  events_by_source: Record<string, number>
  deliveries_by_status: Partial<Record<DeliveryStatus, number>>
  deliveries_by_destination: Record<string, Partial<Record<DeliveryStatus, number>>>
  queue: QueueInfo
}

export interface DeadLetterItem {
  delivery: DeliveryRecord
  event: EventSummary | null
}

export interface ReplayResponse {
  replayed: string[]
  skipped: string[]
}

export interface RouteInfo {
  name: string
  description: string | null
  enabled: boolean
  match: { source: string[] | null; type: string[] | null; where: { path: string; op: string; value: unknown }[] }
  deliver: { to: string; template: unknown }[]
}

export interface ConnectorsInfo {
  sources: { id: string; connector: string; enabled: boolean; signed: boolean; description: string | null }[]
  destinations: { id: string; connector: string; enabled: boolean; description: string | null }[]
  routes: RouteInfo[]
  stock: { connector: string } | null
  available: { inbound: { kind: string; label: string }[]; outbound: { kind: string; label: string }[] }
  retry_schedule_seconds: number[]
}

export interface EventQuery {
  status?: EventStatus
  source?: string
  q?: string
  limit?: number
  offset?: number
}

export type SimulationKind = 'tilda-form' | 'tilda-order' | 'amocrm-won' | 'bitrix-lead' | 'partner'

/** The data layer used by the UI: implemented by the live HTTP client and the in-browser mock. */
export interface RelayApi {
  readonly mode: 'mock' | 'live'
  stats(): Promise<Stats>
  events(query: EventQuery): Promise<EventPage>
  event(id: string): Promise<EventRecord>
  replayEvent(id: string, includeDelivered?: boolean): Promise<ReplayResponse>
  replayDelivery(id: string): Promise<DeliveryRecord>
  deadLetters(): Promise<DeadLetterItem[]>
  connectors(): Promise<ConnectorsInfo>
  /** Demo only: push a synthetic webhook through the pipeline. */
  simulate?(kind: SimulationKind): Promise<string>
}

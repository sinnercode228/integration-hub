// In-browser simulation of the Relay pipeline for the GitHub Pages demo.
//
// It reproduces the backend semantics closely enough to exercise every screen: routing,
// exponential backoff with Retry-After hints, dead letters, aggregate event statuses and
// manual replay. Nothing leaves the browser; state resets on reload.
import type {
  Attempt,
  ConnectorsInfo,
  DeadLetterItem,
  DeliveryBrief,
  DeliveryRecord,
  DeliveryStatus,
  EventPage,
  EventQuery,
  EventRecord,
  EventStatus,
  EventSummary,
  RelayApi,
  ReplayResponse,
  SimulationKind,
  Stats,
} from '../types'
import {
  CONNECTORS,
  CONNECTOR_OF,
  DEFAULT_MAX_ATTEMPTS,
  FAILURES,
  FORMS,
  MAX_ATTEMPTS,
  PEOPLE,
  PRODUCTS,
  RETRY_SCHEDULE,
  UTM,
  type FailureSpec,
} from './fixtures'

/** Deterministic PRNG (mulberry32) so the seeded history is stable across reloads. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

type Outcome = { ok: true } | ({ ok: false } & FailureSpec)

interface Draft {
  source: string
  connector: string
  type: string
  externalId: string | null
  contact: EventRecord['contact']
  fields: Record<string, unknown>
  raw: Record<string, unknown>
}

export interface MockOptions {
  now?: () => number
  seed?: number
  /** Number of historical events to generate. */
  history?: number
}

const iso = (ms: number) => new Date(ms).toISOString()

export function aggregateStatus(statuses: DeliveryStatus[]): EventStatus {
  if (statuses.length === 0) return 'no_route'
  if (statuses.some((s) => s === 'pending' || s === 'retrying')) return 'processing'
  const dead = statuses.filter((s) => s === 'dead').length
  if (dead === 0) return 'delivered'
  return dead === statuses.length ? 'failed' : 'partial'
}

export function retryDelay(attempt: number): number {
  return RETRY_SCHEDULE[Math.min(attempt, RETRY_SCHEDULE.length) - 1] ?? 3600
}

export class MockRelay implements RelayApi {
  readonly mode = 'mock' as const
  private readonly now: () => number
  private readonly rnd: () => number
  private readonly items: EventRecord[] = []
  private readonly scripts = new Map<string, Outcome[]>()
  private seq = 0

  constructor(options: MockOptions = {}) {
    this.now = options.now ?? Date.now
    this.rnd = mulberry32(options.seed ?? 20250101)
    this.seedHistory(options.history ?? 64)
  }

  // -- helpers ------------------------------------------------------------------------------

  private pick<T>(items: readonly T[]): T {
    return items[Math.floor(this.rnd() * items.length)] as T
  }

  private id(prefix: string, at: number): string {
    this.seq += 1
    const rand = Math.floor(this.rnd() * 0xffffff).toString(16).padStart(6, '0')
    return `${prefix}_${at.toString(16).padStart(12, '0')}${rand}${(this.seq % 4096).toString(16).padStart(3, '0')}`
  }

  private phone(): string {
    const n = () => Math.floor(this.rnd() * 10)
    return `+7 900 ${n()}${n()}${n()}-${n()}${n()}-${n()}${n()}`
  }

  private draft(kind: SimulationKind): Draft {
    const [name, email] = this.pick(PEOPLE)
    const phone = this.phone()
    const utm = this.pick(UTM)
    switch (kind) {
      case 'tilda-form': {
        const form = this.pick(FORMS)
        const tranid = `${Math.floor(this.rnd() * 9e6) + 1e6}:${Math.floor(this.rnd() * 9e8)}`
        return {
          source: 'tilda-site',
          connector: 'tilda',
          type: 'form.submitted',
          externalId: tranid,
          contact: { name, phone, email },
          fields: { form_id: 'form' + Math.floor(this.rnd() * 900 + 100), form_name: form, comment: this.rnd() > 0.5 ? 'Перезвоните после 18:00' : null, utm: { utm_source: utm } },
          raw: { Name: name, Phone: phone, Email: email, formname: form, tranid },
        }
      }
      case 'tilda-order': {
        const count = 1 + Math.floor(this.rnd() * 3)
        const products = Array.from({ length: count }, () => {
          const [title, sku, price] = this.pick(PRODUCTS)
          const quantity = 1 + Math.floor(this.rnd() * 2)
          return { name: title, sku, price, quantity, amount: price * quantity }
        })
        const amount = products.reduce((sum, p) => sum + p.amount, 0)
        const orderid = String(Math.floor(this.rnd() * 9e8) + 1e8)
        return {
          source: 'tilda-site',
          connector: 'tilda',
          type: 'order.created',
          externalId: orderid,
          contact: { name, phone, email },
          fields: { form_name: 'Корзина', order: { id: orderid, amount, products }, utm: { utm_source: utm } },
          raw: { name, phone, email, payment: { orderid, amount, products } },
        }
      }
      case 'amocrm-won': {
        const lead = Math.floor(this.rnd() * 90000 + 10000)
        const price = Math.floor(this.rnd() * 90 + 10) * 1000
        const won = this.rnd() > 0.25
        return {
          source: 'amocrm',
          connector: 'amocrm',
          type: 'lead.status_changed',
          externalId: `lead:${lead}:status:${Math.floor(this.now() / 1000)}`,
          contact: { name: null, phone: null, email: null },
          fields: { id: String(lead), name: `Сделка #${lead}`, status_id: won ? 142 : 143, price, entity: 'lead' },
          raw: { id: String(lead), status_id: won ? '142' : '143', price: String(price) },
        }
      }
      case 'bitrix-lead': {
        const entity = Math.floor(this.rnd() * 9000 + 1000)
        const type = this.rnd() > 0.3 ? 'lead.created' : 'deal.created'
        return {
          source: 'bitrix',
          connector: 'bitrix24',
          type,
          externalId: `ONCRM${type === 'lead.created' ? 'LEAD' : 'DEAL'}ADD:${entity}`,
          contact: { name, phone, email: null },
          fields: { entity: type.split('.')[0], entity_id: String(entity), domain: 'demo.bitrix24.ru' },
          raw: { event: type === 'lead.created' ? 'ONCRMLEADADD' : 'ONCRMDEALADD', data: { FIELDS: { ID: String(entity) } } },
        }
      }
      case 'partner': {
        const type = this.pick(['shipment.created', 'invoice.paid', 'return.requested'])
        const ref = `P-${Math.floor(this.rnd() * 90000 + 10000)}`
        return {
          source: 'partner-api',
          connector: 'generic',
          type,
          externalId: ref,
          contact: { name, phone: null, email },
          fields: { reference: ref, total: Math.floor(this.rnd() * 20000) + 500 },
          raw: { type, id: ref, contact: { name, email } },
        }
      }
    }
  }

  private plan(draft: Draft): { route: string; to: string }[] {
    const targets: { route: string; to: string }[] = []
    for (const route of CONNECTORS.routes) {
      const m = route.match
      if (m.source && !m.source.includes(draft.source)) continue
      if (m.type && !m.type.some((t) => t === '*' || t === draft.type)) continue
      if (m.where.some((c) => String(draft.fields[c.path.replace('fields.', '')]) !== String(c.value))) continue
      for (const target of route.deliver) targets.push({ route: route.name, to: target.to })
    }
    return targets
  }

  private payload(draft: Draft, destination: string, eventId: string, at: number): unknown {
    const c = draft.contact
    const f = draft.fields as { form_name?: string; order?: { id: string; amount: number }; entity_id?: string; name?: string; price?: number }
    switch (CONNECTOR_OF[destination]) {
      case 'telegram':
        return {
          text: f.order
            ? `<b>Заказ ${f.order.id}</b> на ${f.order.amount} ₽ · ${c.name}`
            : draft.source === 'bitrix'
              ? `<b>Bitrix24: ${draft.type}</b> #${f.entity_id} · ${c.name}`
              : `<b>Новая заявка с сайта</b>\nФорма: ${f.form_name}\nИмя: ${c.name}\nТелефон: ${c.phone?.replace(/[^\d+]/g, '')}`,
        }
      case 'google_sheets':
        return draft.source === 'amocrm'
          ? { values: [new Date(at).toLocaleString('ru-RU'), 'amoCRM', f.name, f.price, 'won'] }
          : { values: [new Date(at).toLocaleString('ru-RU'), c.name, c.phone?.replace(/[^\d+]/g, ''), c.email, f.form_name, eventId] }
      case 'amocrm_lead':
        return { name: `Сайт: ${f.form_name}`, contact: { name: c.name, phone: c.phone, email: c.email }, tags: ['tilda'] }
      case 'smtp':
        return { subject: `Заказ ${f.order?.id} на ${f.order?.amount} ₽`, text: `Покупатель: ${c.name} (${c.phone})` }
      default:
        return draft.source === 'tilda-site' && f.order
          ? { order_id: f.order.id, amount: f.order.amount, customer: { name: c.name, phone: c.phone } }
          : { id: eventId, type: draft.type, source: draft.source, fields: draft.fields }
    }
  }

  /** Decide the future of a delivery up front: mostly success, some retries, a few dead letters. */
  private script(destination: string, flaky: number): Outcome[] {
    const failures = FAILURES[CONNECTOR_OF[destination] ?? 'webhook'] ?? []
    const roll = this.rnd()
    if (roll < 1 - flaky) return [{ ok: true }]
    const failure = this.pick(failures)
    if (!failure.retryable) return [{ ok: false, ...failure }]
    const max = MAX_ATTEMPTS[destination] ?? DEFAULT_MAX_ATTEMPTS
    const fails = roll > 1 - flaky * 0.25 ? max : 1 + Math.floor(this.rnd() * 3)
    return [...Array.from({ length: fails }, () => ({ ok: false as const, ...failure })), { ok: true }]
  }

  private create(draft: Draft, at: number, flaky: number): EventRecord {
    const id = this.id('evt', at)
    const deliveries: DeliveryRecord[] = this.plan(draft).map(({ route, to }) => {
      const delivery: DeliveryRecord = {
        id: this.id('dlv', at),
        event_id: id,
        route,
        destination: to,
        connector: CONNECTOR_OF[to] ?? 'webhook',
        status: 'pending',
        payload: null,
        result: null,
        attempts: [],
        attempt_base: 0,
        replays: 0,
        next_attempt_at: iso(at + 150),
        last_error: null,
        created_at: iso(at),
        updated_at: iso(at),
      }
      this.scripts.set(delivery.id, this.script(to, flaky))
      return delivery
    })
    const event: EventRecord = {
      id,
      source: draft.source,
      connector: draft.connector,
      type: draft.type,
      external_id: draft.externalId,
      idempotency_key: draft.externalId ? `${draft.source}:ext:${draft.type}:${draft.externalId}` : `${draft.source}:body:${id}`,
      received_at: iso(at),
      status: aggregateStatus(deliveries.map((d) => d.status)),
      contact: draft.contact,
      fields: draft.fields,
      raw: draft.raw,
      deliveries,
    }
    for (const d of deliveries) d.payload = this.payload(draft, d.destination, id, at)
    this.items.push(event)
    return event
  }

  private attempt(event: EventRecord, delivery: DeliveryRecord, at: number): void {
    const script = this.scripts.get(delivery.id) ?? [{ ok: true }]
    const outcome = script.shift() ?? { ok: true }
    const cycle = delivery.attempts.length - delivery.attempt_base + 1
    const max = MAX_ATTEMPTS[delivery.destination] ?? DEFAULT_MAX_ATTEMPTS
    const duration = Math.round((80 + this.rnd() * 420) * 100) / 100
    const attempt: Attempt = {
      number: delivery.attempts.length + 1,
      started_at: iso(at),
      duration_ms: duration,
      ok: outcome.ok,
      status_code: outcome.ok ? (delivery.connector === 'smtp' ? null : 200) : outcome.status,
      error: outcome.ok ? null : outcome.error,
      retry_in_seconds: null,
    }
    if (outcome.ok) {
      delivery.status = 'delivered'
      delivery.last_error = null
      delivery.next_attempt_at = null
      delivery.result = resultFor(delivery.connector, this.rnd)
    } else if (outcome.retryable && cycle < max) {
      const delay = Math.max(retryDelay(cycle), outcome.retryAfter ?? 0)
      attempt.retry_in_seconds = delay
      delivery.status = 'retrying'
      delivery.last_error = outcome.error
      delivery.next_attempt_at = iso(at + duration + delay * 1000)
    } else {
      delivery.status = 'dead'
      delivery.last_error = outcome.error
      delivery.next_attempt_at = null
    }
    delivery.attempts.push(attempt)
    delivery.updated_at = iso(at + duration)
    event.status = aggregateStatus(event.deliveries.map((d) => d.status))
  }

  /** Run every due attempt up to ``until`` (virtual time for history, wall clock later). */
  private advance(until: number): number {
    let processed = 0
    for (let guard = 0; guard < 10_000; guard += 1) {
      let next: { e: EventRecord; d: DeliveryRecord; at: number } | null = null
      for (const e of this.items) {
        for (const d of e.deliveries) {
          if (d.status !== 'pending' && d.status !== 'retrying') continue
          const at = Date.parse(d.next_attempt_at ?? d.created_at)
          if (at <= until && (!next || at < next.at)) next = { e, d, at }
        }
      }
      if (!next) break
      this.attempt(next.e, next.d, next.at)
      processed += 1
    }
    return processed
  }

  private seedHistory(count: number): void {
    const now = this.now()
    const kinds: SimulationKind[] = ['tilda-form', 'tilda-form', 'tilda-form', 'tilda-order', 'tilda-order', 'amocrm-won', 'bitrix-lead', 'partner']
    const span = 36 * 3600 * 1000
    const times = Array.from({ length: count }, () => now - Math.pow(this.rnd(), 1.6) * span).sort((a, b) => a - b)
    for (const at of times) {
      this.create(this.draft(this.pick(kinds)), Math.round(at), 0.22)
      this.advance(Math.round(at))
    }
    // A short warehouse outage a few minutes ago: deliveries are still retrying right now,
    // so the demo shows the backoff progressing live and then recovering.
    const recent = now - 25_000
    for (let i = 0; i < 3; i += 1) {
      const event = this.create(this.draft(i === 1 ? 'partner' : 'tilda-order'), recent + i * 5000, 0)
      for (const d of event.deliveries) {
        if (d.connector === 'webhook') this.scripts.set(d.id, [FAILURE_503, FAILURE_503, FAILURE_503, FAILURE_503, FAILURE_503, { ok: true }])
      }
    }
    this.advance(now)
  }

  /** Process due deliveries using the wall clock; the UI calls this on a timer. */
  tick(): number {
    return this.advance(this.now())
  }

  // -- RelayApi -----------------------------------------------------------------------------

  async stats(): Promise<Stats> {
    this.tick()
    const count = <K extends string>(values: K[]) =>
      values.reduce<Partial<Record<K, number>>>((acc, v) => ({ ...acc, [v]: (acc[v] ?? 0) + 1 }), {})
    const deliveries = this.items.flatMap((e) => e.deliveries)
    const perDestination: Stats['deliveries_by_destination'] = {}
    for (const d of deliveries) {
      const bucket = (perDestination[d.destination] ??= {})
      bucket[d.status] = (bucket[d.status] ?? 0) + 1
    }
    const now = this.now()
    const open = deliveries.filter((d) => d.status === 'pending' || d.status === 'retrying')
    const ready = open.filter((d) => Date.parse(d.next_attempt_at ?? d.created_at) <= now).length
    return {
      events_total: this.items.length,
      events_by_status: count(this.items.map((e) => e.status)),
      events_by_source: count(this.items.map((e) => e.source)) as Record<string, number>,
      deliveries_by_status: count(deliveries.map((d) => d.status)),
      deliveries_by_destination: perDestination,
      queue: { ready, scheduled: open.length - ready, in_flight: 0, dead: deliveries.filter((d) => d.status === 'dead').length },
    }
  }

  async events(query: EventQuery): Promise<EventPage> {
    this.tick()
    const needle = query.q?.trim().toLowerCase()
    const items = this.items
      .filter((e) => !query.status || e.status === query.status)
      .filter((e) => !query.source || e.source === query.source)
      .filter((e) => {
        if (!needle) return true
        return [e.id, e.external_id ?? '', e.type, e.contact.name ?? '', e.contact.phone ?? '', e.contact.email ?? '']
          .some((v) => v.toLowerCase().includes(needle))
      })
      .sort((a, b) => b.received_at.localeCompare(a.received_at) || b.id.localeCompare(a.id))
    const limit = query.limit ?? 50
    const offset = query.offset ?? 0
    return { items: items.slice(offset, offset + limit).map(summary), total: items.length, limit, offset }
  }

  async event(id: string): Promise<EventRecord> {
    this.tick()
    const event = this.items.find((e) => e.id === id)
    if (!event) throw new Error('event not found')
    return structuredClone(event)
  }

  async replayEvent(id: string, includeDelivered = false): Promise<ReplayResponse> {
    const event = this.items.find((e) => e.id === id)
    if (!event) throw new Error('event not found')
    const response: ReplayResponse = { replayed: [], skipped: [] }
    for (const d of event.deliveries) {
      if (d.status === 'dead' || (includeDelivered && d.status === 'delivered')) {
        this.replay(event, d)
        response.replayed.push(d.id)
      } else {
        response.skipped.push(d.id)
      }
    }
    return response
  }

  async replayDelivery(id: string): Promise<DeliveryRecord> {
    for (const event of this.items) {
      const delivery = event.deliveries.find((d) => d.id === id)
      if (!delivery) continue
      if (delivery.status === 'pending' || delivery.status === 'retrying') throw new Error('delivery is still in progress')
      this.replay(event, delivery)
      return structuredClone(delivery)
    }
    throw new Error('delivery not found')
  }

  private replay(event: EventRecord, delivery: DeliveryRecord): void {
    const now = this.now()
    delivery.status = 'pending'
    delivery.attempt_base = delivery.attempts.length
    delivery.replays += 1
    delivery.last_error = null
    delivery.next_attempt_at = iso(now + 800)
    delivery.updated_at = iso(now)
    // In the demo the upstream "has been fixed" by the time someone presses replay.
    this.scripts.set(delivery.id, [{ ok: true }])
    event.status = aggregateStatus(event.deliveries.map((d) => d.status))
  }

  async deadLetters(): Promise<DeadLetterItem[]> {
    this.tick()
    return this.items
      .flatMap((e) => e.deliveries.filter((d) => d.status === 'dead').map((d) => ({ delivery: structuredClone(d), event: summary(e) })))
      .sort((a, b) => b.delivery.updated_at.localeCompare(a.delivery.updated_at))
  }

  async connectors(): Promise<ConnectorsInfo> {
    return structuredClone(CONNECTORS)
  }

  async simulate(kind: SimulationKind): Promise<string> {
    const event = this.create(this.draft(kind), this.now(), 0.3)
    return event.id
  }
}

const FAILURE_503: Outcome = { ok: false, status: 503, error: 'webhook: HTTP 503 - {"error":"maintenance"}', retryable: true }

function resultFor(connector: string, rnd: () => number): Record<string, unknown> {
  const n = () => Math.floor(rnd() * 90000 + 10000)
  switch (connector) {
    case 'telegram':
      return { message_id: n() }
    case 'google_sheets':
      return { updated_range: `Leads!A${n() % 900}:H${n() % 900}`, rows: 1 }
    case 'amocrm_lead':
      return { lead_id: n() * 100, contact_id: n() * 100 }
    case 'smtp':
      return { message_id: `<${n()}.dlv@shop.example>` }
    default:
      return { status: 200 }
  }
}

function brief(d: DeliveryRecord): DeliveryBrief {
  return {
    id: d.id,
    route: d.route,
    destination: d.destination,
    connector: d.connector,
    status: d.status,
    attempts: d.attempts.length,
    last_error: d.last_error,
    next_attempt_at: d.next_attempt_at,
    updated_at: d.updated_at,
  }
}

function summary(e: EventRecord): EventSummary {
  return {
    id: e.id,
    source: e.source,
    connector: e.connector,
    type: e.type,
    status: e.status,
    external_id: e.external_id,
    received_at: e.received_at,
    contact: { ...e.contact },
    deliveries: e.deliveries.map(brief),
  }
}

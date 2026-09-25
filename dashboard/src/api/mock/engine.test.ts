import { describe, expect, it } from 'vitest'
import { MockRelay, aggregateStatus, mulberry32, retryDelay } from './engine'

function clock(start = Date.parse('2026-09-01T12:00:00Z')) {
  const state = { now: start }
  return { now: () => state.now, advance: (ms: number) => (state.now += ms), state }
}

describe('helpers', () => {
  it('aggregates delivery statuses like the backend', () => {
    expect(aggregateStatus([])).toBe('no_route')
    expect(aggregateStatus(['delivered', 'retrying'])).toBe('processing')
    expect(aggregateStatus(['delivered', 'delivered'])).toBe('delivered')
    expect(aggregateStatus(['delivered', 'dead'])).toBe('partial')
    expect(aggregateStatus(['dead'])).toBe('failed')
  })

  it('uses exponential backoff', () => {
    expect([1, 2, 3, 4].map(retryDelay)).toEqual([2, 4, 8, 16])
  })

  it('has a deterministic PRNG', () => {
    const a = mulberry32(1)
    const b = mulberry32(1)
    expect([a(), a(), a()]).toEqual([b(), b(), b()])
  })
})

describe('MockRelay', () => {
  it('seeds a deterministic, consistent history', async () => {
    const c = clock()
    const one = new MockRelay({ now: c.now, seed: 7, history: 30 })
    const two = new MockRelay({ now: c.now, seed: 7, history: 30 })
    const [p1, p2] = await Promise.all([one.events({ limit: 100 }), two.events({ limit: 100 })])
    expect(p1.items.map((e) => e.id)).toEqual(p2.items.map((e) => e.id))
    expect(p1.total).toBe(33) // history + 3 events of the ongoing "outage"

    const stats = await one.stats()
    expect(stats.events_total).toBe(33)
    const byStatus = Object.values(stats.events_by_status).reduce((a, b) => a + (b ?? 0), 0)
    expect(byStatus).toBe(33)
    for (const item of p1.items) {
      const expected = aggregateStatus(item.deliveries.map((d) => d.status))
      expect(item.status).toBe(expected)
    }
  })

  it('routes events like config/relay.yaml', async () => {
    const c = clock()
    const relay = new MockRelay({ now: c.now, history: 0 })
    const form = await relay.event(await relay.simulate('tilda-form'))
    expect(form.deliveries.map((d) => d.destination)).toEqual(['sales-telegram', 'leads-sheet', 'amocrm-leads'])
    const order = await relay.event(await relay.simulate('tilda-order'))
    expect(order.deliveries.map((d) => d.destination)).toEqual(['manager-email', 'warehouse-webhook', 'sales-telegram'])
    const partner = await relay.event(await relay.simulate('partner'))
    expect(partner.deliveries.map((d) => d.connector)).toEqual(['webhook'])
  })

  it('retries with backoff, recovers from the outage and filters/searches', async () => {
    const c = clock()
    const relay = new MockRelay({ now: c.now, history: 0 })
    const before = await relay.events({ status: 'processing' })
    expect(before.total).toBe(3)
    const retrying = (await relay.event(before.items[0]!.id)).deliveries.find((d) => d.connector === 'webhook')!
    expect(retrying.status).toBe('retrying')
    expect(retrying.attempts.every((a) => !a.ok)).toBe(true)
    expect(retrying.attempts.map((a) => a.retry_in_seconds)).toEqual([2, 4, 8, 16].slice(0, retrying.attempts.length))

    c.advance(5 * 60_000)
    relay.tick()
    const after = await relay.events({ status: 'processing' })
    expect(after.total).toBe(0)
    const healed = await relay.event(before.items[0]!.id)
    expect(healed.status).toBe('delivered')

    const name = healed.contact.name!
    const found = await relay.events({ q: name.slice(0, 4).toLowerCase() })
    expect(found.items.some((e) => e.id === healed.id)).toBe(true)
    const bySource = await relay.events({ source: 'partner-api' })
    expect(bySource.items.every((e) => e.source === 'partner-api')).toBe(true)
  })

  it('dead-letters and replays deliveries', async () => {
    const c = clock()
    const relay = new MockRelay({ now: c.now, seed: 3, history: 80 })
    c.advance(3600_000)
    relay.tick()
    const dead = await relay.deadLetters()
    expect(dead.length).toBeGreaterThan(0)
    const target = dead[0]!
    expect(target.delivery.last_error).toBeTruthy()

    const replayed = await relay.replayEvent(target.delivery.event_id)
    expect(replayed.replayed).toContain(target.delivery.id)
    c.advance(1000)
    relay.tick()
    const event = await relay.event(target.delivery.event_id)
    const delivery = event.deliveries.find((d) => d.id === target.delivery.id)!
    expect(delivery.status).toBe('delivered')
    expect(delivery.replays).toBe(1)
    expect(delivery.attempt_base).toBe(delivery.attempts.length - 1)

    await expect(relay.replayDelivery('nope')).rejects.toThrow('not found')
    const again = await relay.replayDelivery(delivery.id)
    expect(again.status).toBe('pending')
    await expect(relay.replayDelivery(delivery.id)).rejects.toThrow('in progress')
  })

  it('exposes the connector configuration without secrets', async () => {
    const info = await new MockRelay({ history: 0 }).connectors()
    expect(info.routes).toHaveLength(5)
    expect(info.retry_schedule_seconds).toEqual([2, 4, 8, 16, 32, 64, 128])
    expect(JSON.stringify(info)).not.toMatch(/bot_token|access_token/)
  })
})

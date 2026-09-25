import { describe, expect, it, vi } from 'vitest'
import { ApiError, HttpRelayApi } from './http'

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
}

describe('HttpRelayApi', () => {
  it('sends the bearer token and query params', async () => {
    const fetcher = vi.fn(async () => json({ items: [], total: 0, limit: 10, offset: 0 }))
    const api = new HttpRelayApi('https://relay.test/', () => 'tok', fetcher as unknown as typeof fetch)
    await api.events({ status: 'failed', q: 'анна', limit: 10, source: '' })
    const [url, init] = fetcher.mock.calls[0] as unknown as [string, RequestInit]
    expect(url).toBe('https://relay.test/admin/api/events?status=failed&q=%D0%B0%D0%BD%D0%BD%D0%B0&limit=10')
    expect(new Headers(init.headers).get('authorization')).toBe('Bearer tok')
  })

  it('posts replays and surfaces API errors', async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(json({ replayed: ['d1'], skipped: [] }))
      .mockResolvedValueOnce(json({ detail: 'invalid admin token' }, 401))
    const api = new HttpRelayApi('', () => null, fetcher as unknown as typeof fetch)
    expect(await api.replayEvent('evt_1', true)).toEqual({ replayed: ['d1'], skipped: [] })
    expect(fetcher.mock.calls[0]?.[0]).toBe('/admin/api/events/evt_1/replay?include_delivered=true')
    expect((fetcher.mock.calls[0]?.[1] as RequestInit).method).toBe('POST')
    const error = await api.stats().catch((e: unknown) => e)
    expect(error).toBeInstanceOf(ApiError)
    expect((error as ApiError).status).toBe(401)
    expect((error as ApiError).message).toBe('invalid admin token')
  })
})

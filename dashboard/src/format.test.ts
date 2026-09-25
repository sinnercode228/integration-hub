import { describe, expect, it } from 'vitest'
import { formatDuration, formatSeconds, formatTime, histogram, percent } from './format'

describe('format', () => {
  const now = Date.parse('2026-09-01T12:00:00Z')

  it('formats relative times', () => {
    expect(formatTime(new Date(now - 5000).toISOString(), 'en', now)).toBe('5s ago')
    expect(formatTime(new Date(now - 5 * 60_000).toISOString(), 'ru', now)).toBe('5 мин назад')
  })

  it('formats numbers', () => {
    expect(formatDuration(120.4)).toBe('120 ms')
    expect(formatDuration(2500)).toBe('2.50 s')
    expect([30, 120, 7200].map(formatSeconds)).toEqual(['30s', '2m', '2h'])
    expect(percent(1, 3)).toBe('33.3%')
    expect(percent(0, 0)).toBe('—')
  })

  it('builds a histogram', () => {
    const hour = 3600_000
    expect(histogram([now - 10, now - hour - 10, now - 5 * hour, now + 5], now, 3 * hour, 3)).toEqual([0, 1, 1])
  })
})

export function formatTime(value: string, lang: 'ru' | 'en', now: number = Date.now()): string {
  const date = new Date(value)
  const diff = (now - date.getTime()) / 1000
  const locale = lang === 'ru' ? 'ru-RU' : 'en-GB'
  if (diff >= 0 && diff < 60) return lang === 'ru' ? `${Math.max(1, Math.floor(diff))} с назад` : `${Math.max(1, Math.floor(diff))}s ago`
  if (diff >= 0 && diff < 3600) return lang === 'ru' ? `${Math.floor(diff / 60)} мин назад` : `${Math.floor(diff / 60)} min ago`
  const sameDay = new Date(now).toDateString() === date.toDateString()
  return date.toLocaleString(locale, sameDay ? { hour: '2-digit', minute: '2-digit' } : { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })
}

export function formatDuration(ms: number): string {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`
}

export function formatSeconds(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${Math.round(seconds / 360) / 10}h`
}

export function percent(part: number, total: number): string {
  if (total === 0) return '—'
  return `${Math.round((part / total) * 1000) / 10}%`
}

/** Bucket timestamps into ``buckets`` equal slots ending at ``now``. */
export function histogram(times: number[], now: number, spanMs: number, buckets: number): number[] {
  const result = new Array<number>(buckets).fill(0)
  const size = spanMs / buckets
  for (const t of times) {
    const age = now - t
    if (age < 0 || age >= spanMs) continue
    const index = buckets - 1 - Math.floor(age / size)
    result[index] = (result[index] ?? 0) + 1
  }
  return result
}

import { HttpRelayApi } from './http'
import { MockRelay } from './mock/engine'
import type { RelayApi } from './types'

export const TOKEN_KEY = 'relay.adminToken'

export function readToken(): string | null {
  try {
    return sessionStorage.getItem(TOKEN_KEY)
  } catch {
    return null
  }
}

export function writeToken(token: string | null): void {
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token)
    else sessionStorage.removeItem(TOKEN_KEY)
  } catch {
    /* storage unavailable (private mode) - token lives only in memory */
  }
}

/**
 * Pick the data layer. Build-time ``VITE_API_MODE=live`` talks to the real admin API;
 * anything else (the GitHub Pages build) runs the in-browser mock pipeline.
 * ``?api=live`` / ``?api=mock`` overrides the build default at runtime.
 */
export function createApi(search: string = window.location.search): RelayApi {
  const override = new URLSearchParams(search).get('api')
  const mode = override === 'live' || override === 'mock' ? override : (import.meta.env.VITE_API_MODE ?? 'mock')
  if (mode === 'live') return new HttpRelayApi(import.meta.env.VITE_API_BASE ?? '', readToken)
  return new MockRelay()
}

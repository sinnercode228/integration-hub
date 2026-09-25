/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** 'mock' (default, in-browser demo data) or 'live' (real Relay admin API). */
  readonly VITE_API_MODE?: 'mock' | 'live'
  /** Base URL of the Relay API in live mode; empty = same origin. */
  readonly VITE_API_BASE?: string
}

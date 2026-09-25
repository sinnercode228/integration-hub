/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// BASE_PATH: '/integration-hub/' for GitHub Pages, '/admin/' when served by the FastAPI app.
export default defineConfig({
  base: process.env.BASE_PATH || '/',
  plugins: [react()],
  build: { outDir: 'dist', sourcemap: false },
  server: {
    port: 5173,
    proxy: { '/admin/api': 'http://127.0.0.1:8000' },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test-setup.ts'],
  },
})

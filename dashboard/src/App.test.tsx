import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import App from './App'
import { MockRelay } from './api/mock/engine'

describe('App (demo mode)', () => {
  it('renders KPIs, events and opens an event with its delivery timeline', async () => {
    localStorage.setItem('relay.lang', 'en')
    const api = new MockRelay({ seed: 11, history: 20 })
    render(<App api={api} />)

    expect(await screen.findByText('Total events')).toBeInTheDocument()
    expect(screen.getByText('Demo data')).toBeInTheDocument()
    expect(screen.getByText(/Demo project · Демо-проект/)).toBeInTheDocument()

    const rows = await screen.findAllByRole('row')
    expect(rows.length).toBeGreaterThan(5)
    fireEvent.click(rows[1]!)
    const dialog = await screen.findByRole('dialog')
    expect(within(dialog).getAllByText(/Attempt #1/).length).toBeGreaterThan(0)
    fireEvent.keyDown(window, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('simulates a webhook and shows connectors', async () => {
    localStorage.setItem('relay.lang', 'ru')
    const api = new MockRelay({ seed: 5, history: 5 })
    render(<App api={api} />)
    await screen.findByText('Всего событий')
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: '+ Tilda: форма' }))
    })
    expect(await screen.findByRole('status')).toHaveTextContent('Tilda: форма → evt_')
    fireEvent.click(screen.getByRole('tab', { name: /Коннекторы/ }))
    expect(await screen.findByText('site-leads')).toBeInTheDocument()
    expect(screen.getByText('POST /webhooks/tilda-site')).toBeInTheDocument()
  })
})

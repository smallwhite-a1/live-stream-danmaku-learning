import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from './App'

const insight = (overrides: Record<string, unknown> = {}) => ({
  room_id: 'room-alpha',
  window_start: '2026-08-04T12:00:00Z',
  window_end: '2026-08-04T12:01:00Z',
  status: 'normal',
  rules: {
    message_count: 128,
    unique_users: 42,
    question_count: 13,
    repeated_message_ratio: 0.25,
    peak_messages_per_second: 19,
    top_repeated_count: 0,
  },
  semantic: {
    summary: 'Inventory questions are rising while the room remains constructive.',
    topics: [{ name: 'Restock timing', confidence: 0.91, evidence_event_ids: ['evt-101', 'evt-102'] }],
    sentiment: { label: 'positive', confidence: 0.79, evidence_event_ids: ['evt-103'] },
    questions: [{ text: 'When will the next batch arrive?', evidence_event_ids: ['evt-104'] }],
    alerts: [{ type: 'demand spike', severity: 'medium', description: 'Question volume is elevated.', evidence_event_ids: ['evt-105'] }],
  },
  model: { provider: 'local', model: 'rules', prompt_version: 'v1', input_tokens: 0, output_tokens: 0, latency_millis: 11 },
  generated_at: '2026-08-04T12:01:04Z',
  ...overrides,
})

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('App', () => {
  it('loads the default room automatically', async () => {
    const fetchMock = vi.fn(() => jsonResponse(insight()))
    vi.stubGlobal('fetch', fetchMock)

    render(<App />)

    expect(await screen.findByText('Inventory questions are rising while the room remains constructive.')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/rooms/room-alpha/insights/latest', expect.objectContaining({ signal: expect.any(AbortSignal) }))
  })

  it('loads a different room from the room search', async () => {
    const fetchMock = vi.fn(() => jsonResponse(insight({ room_id: 'room beta' })))
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)
    await screen.findByText('Inventory questions are rising while the room remains constructive.')

    fireEvent.change(screen.getByLabelText('Room ID'), { target: { value: 'room beta' } })
    fireEvent.click(screen.getByRole('button', { name: 'Load room' }))

    await waitFor(() => expect(fetchMock).toHaveBeenLastCalledWith('/api/v1/rooms/room%20beta/insights/latest', expect.anything()))
  })

  it('renders normal and degraded status as distinct signals', async () => {
    const fetchMock = vi.fn(() => jsonResponse(insight()))
    vi.stubGlobal('fetch', fetchMock)
    const { rerender } = render(<App />)
    expect(await screen.findByText('Normal')).toHaveClass('status-normal')

    fetchMock.mockImplementationOnce(() => jsonResponse(insight({ status: 'degraded', degraded_reason: 'semantic model unavailable' })))
    fireEvent.click(screen.getByRole('button', { name: 'Refresh insight' }))
    expect(await screen.findByText('Degraded')).toHaveClass('status-degraded')
    expect(screen.getByText('semantic model unavailable')).toBeInTheDocument()
    rerender(<App />)
  })

  it('renders the window summary and exact rule metrics', async () => {
    vi.stubGlobal('fetch', vi.fn(() => jsonResponse(insight())))
    render(<App />)

    expect(await screen.findByText('128')).toBeInTheDocument()
    const metrics = within(screen.getByLabelText('Rule metrics'))
    expect(metrics.getByText('Messages')).toBeInTheDocument()
    expect(metrics.getByText('Unique users')).toBeInTheDocument()
    expect(metrics.getByText('Questions')).toBeInTheDocument()
    expect(metrics.getByText('Repeated ratio')).toBeInTheDocument()
    expect(metrics.getByText('Peak msg/s')).toBeInTheDocument()
    expect(screen.getByText('25.0%')).toBeInTheDocument()
    expect(screen.getByText(/12:00:00/)).toBeInTheDocument()
  })

  it('renders topics, sentiment, questions, and alerts', async () => {
    vi.stubGlobal('fetch', vi.fn(() => jsonResponse(insight())))
    render(<App />)

    expect(await screen.findByText('Restock timing')).toBeInTheDocument()
    expect(screen.getByText('Positive')).toBeInTheDocument()
    expect(screen.getByText('When will the next batch arrive?')).toBeInTheDocument()
    expect(screen.getByText('demand spike')).toBeInTheDocument()
  })

  it('reveals evidence event IDs for a selected semantic claim', async () => {
    vi.stubGlobal('fetch', vi.fn(() => jsonResponse(insight())))
    render(<App />)
    await screen.findByText('Restock timing')

    fireEvent.click(screen.getByRole('button', { name: 'View evidence for Restock timing' }))

    expect(screen.getByRole('complementary', { name: 'Evidence' })).toHaveTextContent('evt-101')
    expect(screen.getByRole('complementary', { name: 'Evidence' })).toHaveTextContent('evt-102')
  })

  it('shows a missing-room error without deleting the last successful insight', async () => {
    const fetchMock = vi.fn(() => jsonResponse(insight()))
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)
    expect(await screen.findByText('Restock timing')).toBeInTheDocument()

    fetchMock.mockImplementationOnce(() => jsonResponse({ error: 'insight not found' }, 404))
    fireEvent.click(screen.getByRole('button', { name: 'Refresh insight' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('No insight was found for room-alpha.')
    expect(screen.getByText('Restock timing')).toBeInTheDocument()
    expect(screen.getByText('Stale')).toBeInTheDocument()
  })

  it('shows a network error without deleting the last successful insight', async () => {
    const fetchMock = vi.fn(() => jsonResponse(insight()))
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)
    expect(await screen.findByText('Restock timing')).toBeInTheDocument()

    fetchMock.mockImplementationOnce(() => Promise.reject(new TypeError('network down')))
    fireEvent.click(screen.getByRole('button', { name: 'Refresh insight' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to reach the insight service.')
    expect(screen.getByText('Restock timing')).toBeInTheDocument()
  })

  it('shows an incomplete-response error for an empty semantic object', async () => {
    vi.stubGlobal('fetch', vi.fn(() => jsonResponse(insight({ semantic: {} }))))
    render(<App />)

    expect(await screen.findByRole('alert')).toHaveTextContent('The insight response was incomplete.')
    expect(screen.getByText('No insight is available for this room.')).toBeInTheDocument()
  })

  it('shows an incomplete-response error for malformed rule fields', async () => {
    vi.stubGlobal('fetch', vi.fn(() => jsonResponse(insight({ rules: { message_count: '128' } }))))
    render(<App />)

    expect(await screen.findByRole('alert')).toHaveTextContent('The insight response was incomplete.')
    expect(screen.getByText('No insight is available for this room.')).toBeInTheDocument()
  })
})

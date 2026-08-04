import { RefreshCw, Search } from 'lucide-react'

interface InsightHeaderProps {
  roomID: string
  loading: boolean
  onRoomIDChange: (roomID: string) => void
  onSearch: () => void
  onRefresh: () => void
}

export function InsightHeader({ roomID, loading, onRoomIDChange, onSearch, onRefresh }: InsightHeaderProps) {
  return (
    <header className="app-header">
      <div className="header-content">
        <p className="product-name">Live insight</p>
        <form className="room-search" onSubmit={(event) => { event.preventDefault(); onSearch() }}>
          <label className="sr-only" htmlFor="room-id">Room ID</label>
          <input
            id="room-id"
            value={roomID}
            onChange={(event) => onRoomIDChange(event.target.value)}
            placeholder="Room ID"
            autoComplete="off"
          />
          <button className="icon-button" type="submit" aria-label="Load room" title="Load room" disabled={loading}>
            <Search aria-hidden="true" size={17} />
          </button>
        </form>
        <button className="icon-button" type="button" aria-label="Refresh insight" title="Refresh insight" onClick={onRefresh} disabled={loading}>
          <RefreshCw aria-hidden="true" size={17} className={loading ? 'spin' : undefined} />
        </button>
      </div>
    </header>
  )
}

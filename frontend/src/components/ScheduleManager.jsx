import { useState, useEffect } from 'react'

function ScheduleManager() {
  const [currentTime, setCurrentTime] = useState('')
  const [newTime, setNewTime] = useState('')
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState(null)

  useEffect(() => {
    fetchSchedule()
  }, [])

  const fetchSchedule = async () => {
    try {
      const res = await fetch('/api/schedule')
      if (res.ok) {
        const data = await res.json()
        setCurrentTime(data.time)
        setNewTime(data.time)
      }
    } catch {
      /* 忽略網路錯誤 */
    }
  }

  const handleSave = async () => {
    if (newTime === currentTime) {
      setMessage({ type: 'error', text: '排程時間未變更' })
      return
    }

    setSaving(true)
    setMessage(null)

    try {
      const res = await fetch('/api/schedule', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ time: newTime }),
      })

      const data = await res.json()

      if (res.ok) {
        setCurrentTime(data.time)
        setMessage({ type: 'success', text: data.message })
      } else {
        setMessage({ type: 'error', text: data.detail || '更新失敗' })
      }
    } catch {
      setMessage({ type: 'error', text: '無法連線至伺服器' })
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#9200;</span>
        每日排程設定
      </h2>

      <div className="current-schedule">
        <div>
          <div className="time-label">目前排程時間</div>
          <div className="time-display">{currentTime || '--:--'}</div>
        </div>
      </div>

      <div className="form-group">
        <label>新排程時間</label>
        <input
          type="time"
          value={newTime}
          onChange={(e) => setNewTime(e.target.value)}
        />
      </div>

      <button
        className="btn btn-primary"
        onClick={handleSave}
        disabled={saving}
      >
        {saving ? '儲存中...' : '儲存排程時間'}
      </button>

      {message && (
        <div className={`message message-${message.type}`}>{message.text}</div>
      )}

      <div style={{ marginTop: '16px', fontSize: '0.8125rem', color: 'var(--color-text-secondary)' }}>
        <p>排程會在每日指定時間自動檢查過去 30 天的資料，補抓缺漏日期。</p>
        <p style={{ marginTop: '4px' }}>涵蓋資料庫：TWSE、TPEX、TAIFEX、FAOI、MGTS</p>
      </div>
    </div>
  )
}

export default ScheduleManager

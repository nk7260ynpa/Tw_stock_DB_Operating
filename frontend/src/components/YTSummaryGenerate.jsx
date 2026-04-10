import { useState, useEffect, useCallback } from 'react'

function getToday() {
  return new Date().toISOString().slice(0, 10)
}

function YTSummaryGenerate() {
  const [date, setDate] = useState(getToday())
  const [generated, setGenerated] = useState([])
  const [jobs, setJobs] = useState([])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const [scheduleTime, setScheduleTime] = useState('19:15')
  const [saving, setSaving] = useState(false)
  const [scheduleMsg, setScheduleMsg] = useState('')

  const hasRunningJob = jobs.some(
    (j) => j.status === 'running' || j.status === 'pending' || j.status === 'queued'
  )

  const fetchGenerated = useCallback(async () => {
    try {
      const res = await fetch('/api/yt-summary/generated')
      if (res.ok) {
        const data = await res.json()
        setGenerated(data.generated || [])
      }
    } catch { /* ignore */ }
  }, [])

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/api/upload/jobs')
      if (res.ok) {
        const data = await res.json()
        const summaryJobs = data
          .filter((j) => j.type === 'yt_summary')
          .reverse()
        setJobs(summaryJobs)
      }
    } catch { /* ignore */ }
  }, [])

  const fetchSchedule = useCallback(async () => {
    try {
      const res = await fetch('/api/yt-summary/schedule')
      if (res.ok) {
        const data = await res.json()
        setScheduleTime(data.time || '19:15')
      }
    } catch { /* ignore */ }
  }, [])

  useEffect(() => {
    fetchGenerated()
    fetchJobs()
    fetchSchedule()
    const interval = setInterval(() => {
      fetchGenerated()
      fetchJobs()
    }, 5000)
    return () => clearInterval(interval)
  }, [fetchGenerated, fetchJobs, fetchSchedule])

  const handleGenerate = async () => {
    setError('')
    setSubmitting(true)
    try {
      const res = await fetch('/api/yt-summary/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date }),
      })

      if (!res.ok) {
        const data = await res.json()
        setError(data.detail || '產生摘要請求失敗')
        return
      }

      await fetchJobs()
    } catch {
      setError('無法連線至伺服器')
    } finally {
      setSubmitting(false)
    }
  }

  const handleSaveSchedule = async () => {
    setSaving(true)
    setScheduleMsg('')
    try {
      const res = await fetch('/api/yt-summary/schedule', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ time: scheduleTime }),
      })

      if (res.ok) {
        const data = await res.json()
        setScheduleMsg(data.message || '排程已更新')
      } else {
        const data = await res.json()
        setScheduleMsg(data.detail || '更新失敗')
      }
    } catch {
      setScheduleMsg('無法連線至伺服器')
    } finally {
      setSaving(false)
    }
  }

  const statusLabel = {
    pending: '等待中',
    queued: '排隊中',
    running: '處理中',
    completed: '已完成',
    failed: '失敗',
  }

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#128221;</span>
        YT 精華摘要
      </h2>

      <div className="form-group">
        <label>日期</label>
        <input
          type="date"
          className="form-input"
          value={date}
          onChange={(e) => setDate(e.target.value)}
        />
      </div>

      <button
        className="btn btn-primary"
        onClick={handleGenerate}
        disabled={submitting || hasRunningJob}
        style={{ marginBottom: '12px' }}
      >
        {hasRunningJob ? (
          <>
            <span className="spinner" />
            任務排隊/執行中...
          </>
        ) : (
          '產生摘要'
        )}
      </button>

      {error && <div className="message message-error">{error}</div>}

      <div className="form-group" style={{ marginTop: '16px' }}>
        <label>每日排程設定（自動產生當日 YT 精華摘要）</label>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <input
            type="time"
            className="form-input"
            value={scheduleTime}
            onChange={(e) => setScheduleTime(e.target.value)}
            style={{ flex: 1 }}
          />
          <button
            className="btn btn-primary"
            onClick={handleSaveSchedule}
            disabled={saving}
            style={{ whiteSpace: 'nowrap' }}
          >
            {saving ? '儲存中...' : '儲存'}
          </button>
        </div>
        {scheduleMsg && (
          <div className="message" style={{ marginTop: '8px' }}>
            {scheduleMsg}
          </div>
        )}
      </div>

      {generated.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">已產生日期</h3>
          <div className="uploaded-grid">
            {generated.map((d) => (
              <div key={d} className="uploaded-item">
                <span>{d}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {jobs.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">任務紀錄</h3>
          {jobs.map((job) => (
            <div key={job.job_id} className="job-item">
              <div className="job-header">
                <span className="job-info">
                  YT 摘要 {job.date}
                  {job.scheduled && ' (排程)'}
                </span>
                <span className={`badge badge-${job.status}`}>
                  {job.status === 'running' && <span className="spinner" />}
                  {statusLabel[job.status] || job.status}
                  {job.queue_position !== undefined && job.status === 'queued' &&
                    ` (第 ${job.queue_position} 位)`}
                </span>
              </div>
              {job.error && (
                <div className="job-errors">{job.error}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default YTSummaryGenerate

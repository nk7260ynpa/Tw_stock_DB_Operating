import { useState, useEffect, useCallback } from 'react'

function getToday() {
  return new Date().toISOString().slice(0, 10)
}

function CTEENewsUpload() {
  const [startDate, setStartDate] = useState(getToday())
  const [endDate, setEndDate] = useState(getToday())
  const [uploaded, setUploaded] = useState([])
  const [jobs, setJobs] = useState([])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const [scheduleTime, setScheduleTime] = useState('21:00')
  const [saving, setSaving] = useState(false)
  const [scheduleMsg, setScheduleMsg] = useState('')

  const hasRunningJob = jobs.some(
    (j) => j.status === 'running' || j.status === 'pending'
  )

  const fetchUploaded = useCallback(async () => {
    try {
      const res = await fetch('/api/ctee-news/uploaded')
      if (res.ok) {
        const data = await res.json()
        setUploaded(data.uploaded || [])
      }
    } catch {
      /* ignore */
    }
  }, [])

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/api/upload/jobs')
      if (res.ok) {
        const data = await res.json()
        const cteeJobs = data
          .filter((j) => j.type === 'ctee_news')
          .reverse()
        setJobs(cteeJobs)
      }
    } catch {
      /* ignore */
    }
  }, [])

  const fetchSchedule = useCallback(async () => {
    try {
      const res = await fetch('/api/ctee-news/schedule')
      if (res.ok) {
        const data = await res.json()
        setScheduleTime(data.time || '21:00')
      }
    } catch {
      /* ignore */
    }
  }, [])

  useEffect(() => {
    fetchUploaded()
    fetchJobs()
    fetchSchedule()
    const interval = setInterval(() => {
      fetchUploaded()
      fetchJobs()
    }, 3000)
    return () => clearInterval(interval)
  }, [fetchUploaded, fetchJobs, fetchSchedule])

  const handleUpload = async () => {
    setError('')
    setSubmitting(true)
    try {
      const res = await fetch('/api/ctee-news/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start_date: startDate,
          end_date: endDate,
        }),
      })

      if (!res.ok) {
        const data = await res.json()
        setError(data.detail || '上傳請求失敗')
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
      const res = await fetch('/api/ctee-news/schedule', {
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
    running: '上傳中',
    completed: '已完成',
    failed: '失敗',
  }

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#128240;</span>
        CTEE 新聞
      </h2>

      <div className="form-group">
        <label>起始日期</label>
        <input
          type="date"
          className="form-input"
          value={startDate}
          onChange={(e) => setStartDate(e.target.value)}
        />
      </div>

      <div className="form-group">
        <label>結束日期</label>
        <input
          type="date"
          className="form-input"
          value={endDate}
          onChange={(e) => setEndDate(e.target.value)}
        />
      </div>

      <button
        className="btn btn-primary"
        onClick={handleUpload}
        disabled={submitting || hasRunningJob}
        style={{ marginBottom: '12px' }}
      >
        {hasRunningJob ? (
          <>
            <span className="spinner" />
            上傳中...
          </>
        ) : (
          '開始上傳'
        )}
      </button>

      {error && <div className="message message-error">{error}</div>}

      <div className="form-group" style={{ marginTop: '16px' }}>
        <label>每日排程設定（每天自動抓取當日新聞）</label>
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

      {uploaded.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">已上傳日期</h3>
          <div className="uploaded-grid">
            {uploaded.map((date) => (
              <div key={date} className="uploaded-item">
                <span>{date}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {jobs.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">上傳任務紀錄</h3>
          {jobs.map((job) => (
            <div key={job.job_id} className="job-item">
              <div className="job-header">
                <span className="job-info">
                  CTEE {job.start_date || job.date}
                  {job.end_date && job.end_date !== job.start_date
                    ? ` ~ ${job.end_date}`
                    : ''}
                  {job.scheduled && ' (排程)'}
                </span>
                <span className={`badge badge-${job.status}`}>
                  {job.status === 'running' && <span className="spinner" />}
                  {statusLabel[job.status] || job.status}
                </span>
              </div>
              {job.status === 'completed' && job.record_count > 0 && (
                <div className="job-info" style={{ marginTop: '4px' }}>
                  共 {job.record_count.toLocaleString()} 筆 metadata，
                  {job.file_count || 0} 個全文檔案
                </div>
              )}
              {job.status === 'completed' && job.record_count === 0 && (
                <div className="job-info" style={{ marginTop: '4px' }}>
                  無新資料或資料已存在
                </div>
              )}
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

export default CTEENewsUpload

import { useState, useEffect, useCallback } from 'react'

const DAY_OPTIONS = [
  { value: 'monday', label: '星期一' },
  { value: 'tuesday', label: '星期二' },
  { value: 'wednesday', label: '星期三' },
  { value: 'thursday', label: '星期四' },
  { value: 'friday', label: '星期五' },
  { value: 'saturday', label: '星期六' },
  { value: 'sunday', label: '星期日' },
]

function TDCCUpload() {
  const [uploaded, setUploaded] = useState([])
  const [jobs, setJobs] = useState([])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const [scheduleDay, setScheduleDay] = useState('saturday')
  const [scheduleTime, setScheduleTime] = useState('10:00')
  const [saving, setSaving] = useState(false)
  const [scheduleMsg, setScheduleMsg] = useState('')

  const hasRunningJob = jobs.some(
    (j) => j.status === 'running' || j.status === 'pending'
  )

  const fetchUploaded = useCallback(async () => {
    try {
      const res = await fetch('/api/tdcc/uploaded')
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
        const tdccJobs = data
          .filter((j) => j.type === 'tdcc')
          .reverse()
        setJobs(tdccJobs)
      }
    } catch {
      /* ignore */
    }
  }, [])

  const fetchSchedule = useCallback(async () => {
    try {
      const res = await fetch('/api/tdcc/schedule')
      if (res.ok) {
        const data = await res.json()
        setScheduleDay(data.day || 'saturday')
        setScheduleTime(data.time || '10:00')
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
      const res = await fetch('/api/tdcc/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
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
      const res = await fetch('/api/tdcc/schedule', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ day: scheduleDay, time: scheduleTime }),
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

  const dayLabel = DAY_OPTIONS.reduce((acc, d) => {
    acc[d.value] = d.label
    return acc
  }, {})

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#128202;</span>
        TDCC 集保庫存
      </h2>

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
          '取得最新資料'
        )}
      </button>

      {error && <div className="message message-error">{error}</div>}

      <div className="form-group" style={{ marginTop: '16px' }}>
        <label>週排程設定</label>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <select
            className="form-select"
            value={scheduleDay}
            onChange={(e) => setScheduleDay(e.target.value)}
            style={{ flex: 1 }}
          >
            {DAY_OPTIONS.map((d) => (
              <option key={d.value} value={d.value}>
                {d.label}
              </option>
            ))}
          </select>
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
                  {job.date ? `TDCC ${job.date}` : 'TDCC'}
                  {job.scheduled && ' (排程)'}
                </span>
                <span className={`badge badge-${job.status}`}>
                  {job.status === 'running' && <span className="spinner" />}
                  {statusLabel[job.status] || job.status}
                </span>
              </div>
              {job.status === 'completed' && job.record_count > 0 && (
                <div className="job-info" style={{ marginTop: '4px' }}>
                  共 {job.record_count.toLocaleString()} 筆資料
                </div>
              )}
              {job.status === 'completed' && job.record_count === 0 && (
                <div className="job-info" style={{ marginTop: '4px' }}>
                  資料已存在或無新資料
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

export default TDCCUpload

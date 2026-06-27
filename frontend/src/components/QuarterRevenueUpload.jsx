import { useState, useEffect, useCallback } from 'react'
import { apiFetch } from '../api'
import { MAX_UPLOADED_SHOWN } from '../constants'

function getCurrentROCYear() {
  return new Date().getFullYear() - 1911
}

function QuarterRevenueUpload() {
  const currentYear = getCurrentROCYear()
  const years = Array.from({ length: 10 }, (_, i) => currentYear - i)

  const [year, setYear] = useState(currentYear)
  const [quarter, setQuarter] = useState(1)
  const [uploaded, setUploaded] = useState([])
  const [jobs, setJobs] = useState([])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const hasRunningJob = jobs.some(
    (j) => j.status === 'running' || j.status === 'pending' || j.status === 'queued'
  )

  const isUploaded = (y, q) =>
    uploaded.some((u) => u.year === y && String(u.quarter) === String(q))

  const fetchUploaded = useCallback(async () => {
    try {
      const res = await apiFetch('/api/quarter-revenue/uploaded')
      if (res.ok) {
        const data = await res.json()
        setUploaded(data.uploaded || [])
      }
    } catch {
      /* 忽略網路錯誤 */
    }
  }, [])

  const fetchJobs = useCallback(async () => {
    try {
      const res = await apiFetch('/api/upload/jobs')
      if (res.ok) {
        const data = await res.json()
        const revenueJobs = data
          .filter((j) => j.type === 'quarter_revenue')
          .reverse()
        setJobs(revenueJobs)
      }
    } catch {
      /* 忽略網路錯誤 */
    }
  }, [])

  useEffect(() => {
    fetchUploaded()
    fetchJobs()
    const interval = setInterval(() => {
      fetchUploaded()
      fetchJobs()
    }, 3000)
    return () => clearInterval(interval)
  }, [fetchUploaded, fetchJobs])

  const handleSubmit = async () => {
    setError('')
    setSubmitting(true)
    try {
      const res = await apiFetch('/api/quarter-revenue/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ year, quarter }),
      })

      if (!res.ok) {
        const data = await res.json()
        setError(data.detail || '抓取請求失敗')
        return
      }

      await fetchJobs()
    } catch {
      setError('無法連線至伺服器')
    } finally {
      setSubmitting(false)
    }
  }

  const statusLabel = {
    pending: '等待中',
    queued: '排隊中',
    running: '抓取中',
    completed: '已完成',
    failed: '失敗',
  }

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#128200;</span>
        季度營業收入
      </h2>

      <div className="form-group">
        <label>年度（民國年）</label>
        <select
          className="form-select"
          value={year}
          onChange={(e) => setYear(Number(e.target.value))}
        >
          {years.map((y) => (
            <option key={y} value={y}>
              民國 {y} 年
            </option>
          ))}
        </select>
      </div>

      <div className="form-group">
        <label>季度</label>
        <div className="checkbox-group">
          {[1, 2, 3, 4].map((q) => {
            const done = isUploaded(year, q)
            return (
              <label
                key={q}
                className={`checkbox-label${quarter === q ? ' checked' : ''}${done ? ' uploaded' : ''}`}
              >
                <input
                  type="radio"
                  name="quarter"
                  checked={quarter === q}
                  onChange={() => setQuarter(q)}
                />
                Q{q}
                {done && <span className="uploaded-mark">&#10003;</span>}
              </label>
            )
          })}
        </div>
      </div>

      <button
        className="btn btn-primary"
        onClick={handleSubmit}
        disabled={submitting || hasRunningJob}
      >
        {hasRunningJob ? (
          <>
            <span className="spinner" />
            任務排隊/執行中...
          </>
        ) : (
          '開始抓取'
        )}
      </button>

      {error && <div className="message message-error">{error}</div>}

      {uploaded.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">已上傳季度</h3>
          <div className="uploaded-grid">
            {uploaded.slice(0, MAX_UPLOADED_SHOWN).map((u) => (
              <div key={`${u.year}-${u.quarter}`} className="uploaded-item">
                <span>
                  民國 {u.year} 年 Q{u.quarter}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {jobs.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">抓取任務紀錄</h3>
          {jobs.map((job) => (
            <div key={job.job_id} className="job-item">
              <div className="job-header">
                <span className="job-info">
                  民國 {job.year} 年 Q{job.quarter}
                </span>
                <span className={`badge badge-${job.status}`}>
                  {job.status === 'running' && <span className="spinner" />}
                  {statusLabel[job.status] || job.status}
                  {job.queue_position !== undefined && job.status === 'queued' &&
                    ` (第 ${job.queue_position} 位)`}
                </span>
              </div>
              {job.status === 'completed' && job.record_count > 0 && (
                <div className="job-info" style={{ marginTop: '4px' }}>
                  共 {job.record_count.toLocaleString()} 筆資料
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

export default QuarterRevenueUpload

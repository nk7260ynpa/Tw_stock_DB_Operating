import { useState, useEffect, useCallback } from 'react'

const DB_LIST = ['TWSE', 'TPEX', 'TAIFEX', 'FAOI', 'MGTS']

function getToday() {
  return new Date().toISOString().split('T')[0]
}

function ManualUpload() {
  const [startDate, setStartDate] = useState(getToday)
  const [endDate, setEndDate] = useState(getToday)
  const [selectedDbs, setSelectedDbs] = useState([])
  const [jobs, setJobs] = useState([])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const hasRunningJob = jobs.some(
    (j) => j.status === 'running' || j.status === 'pending'
  )

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/api/upload/jobs')
      if (res.ok) {
        const data = await res.json()
        setJobs(data.filter((j) => !j.type).reverse())
      }
    } catch {
      /* 忽略網路錯誤 */
    }
  }, [])

  useEffect(() => {
    fetchJobs()
    const interval = setInterval(fetchJobs, 2000)
    return () => clearInterval(interval)
  }, [fetchJobs])

  const toggleDb = (db) => {
    setSelectedDbs((prev) =>
      prev.includes(db) ? prev.filter((d) => d !== db) : [...prev, db]
    )
  }

  const toggleAll = () => {
    setSelectedDbs((prev) =>
      prev.length === DB_LIST.length ? [] : [...DB_LIST]
    )
  }

  const handleUpload = async () => {
    setError('')

    if (!selectedDbs.length) {
      setError('請至少選擇一個資料庫')
      return
    }

    setSubmitting(true)
    try {
      const res = await fetch('/api/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start_date: startDate,
          end_date: endDate,
          databases: selectedDbs,
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

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#128228;</span>
        手動上傳
      </h2>

      <div className="form-group">
        <label>起始日期</label>
        <input
          type="date"
          value={startDate}
          onChange={(e) => setStartDate(e.target.value)}
        />
      </div>

      <div className="form-group">
        <label>結束日期</label>
        <input
          type="date"
          value={endDate}
          onChange={(e) => setEndDate(e.target.value)}
        />
      </div>

      <div className="form-group">
        <label>資料庫</label>
        <div className="checkbox-group">
          <label
            className={`checkbox-label select-all ${
              selectedDbs.length === DB_LIST.length ? 'checked' : ''
            }`}
          >
            <input
              type="checkbox"
              checked={selectedDbs.length === DB_LIST.length}
              onChange={toggleAll}
            />
            全選
          </label>
          {DB_LIST.map((db) => (
            <label
              key={db}
              className={`checkbox-label ${
                selectedDbs.includes(db) ? 'checked' : ''
              }`}
            >
              <input
                type="checkbox"
                checked={selectedDbs.includes(db)}
                onChange={() => toggleDb(db)}
              />
              {db}
            </label>
          ))}
        </div>
      </div>

      <button
        className="btn btn-primary"
        onClick={handleUpload}
        disabled={submitting || hasRunningJob}
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

      {jobs.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">上傳任務紀錄</h3>
          {jobs.map((job) => (
            <JobItem key={job.job_id} job={job} />
          ))}
        </div>
      )}
    </div>
  )
}

function JobItem({ job }) {
  const percent =
    job.total > 0 ? Math.round((job.completed / job.total) * 100) : 0

  const statusLabel = {
    pending: '等待中',
    running: '上傳中',
    completed: '已完成',
    failed: '失敗',
  }

  return (
    <div className="job-item">
      <div className="job-header">
        <span className="job-info">
          {job.start_date}
          {job.start_date !== job.end_date && ` ~ ${job.end_date}`}
          {' | '}
          {job.databases.join(', ')}
        </span>
        <span className={`badge badge-${job.status}`}>
          {job.status === 'running' && <span className="spinner" />}
          {statusLabel[job.status] || job.status}
        </span>
      </div>

      {(job.status === 'running' || job.status === 'completed') && (
        <div className="job-progress">
          <div className="progress-bar">
            <div
              className="progress-fill"
              style={{ width: `${percent}%` }}
            />
          </div>
          <div className="progress-text">
            {job.completed} / {job.total}
            {job.status === 'running' &&
              job.current_db &&
              ` - ${job.current_db} ${job.current_date}`}
          </div>
        </div>
      )}

      {job.errors && job.errors.length > 0 && (
        <div className="job-errors">
          {job.errors.map((err, i) => (
            <div key={i}>{err}</div>
          ))}
        </div>
      )}
    </div>
  )
}

export default ManualUpload

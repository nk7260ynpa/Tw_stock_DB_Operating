import { useState, useEffect, useCallback } from 'react'

function CompanyInfoUpload() {
  const [status, setStatus] = useState({
    company_info_count: 0,
    industry_map_count: 0,
  })
  const [jobs, setJobs] = useState([])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const hasRunningJob = jobs.some(
    (j) => j.status === 'running' || j.status === 'pending' || j.status === 'queued'
  )

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/company-info/status')
      if (res.ok) {
        const data = await res.json()
        setStatus(data)
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
        const ciJobs = data
          .filter((j) => j.type === 'company_info')
          .reverse()
        setJobs(ciJobs)
      }
    } catch {
      /* ignore */
    }
  }, [])

  useEffect(() => {
    fetchStatus()
    fetchJobs()
    const interval = setInterval(() => {
      fetchStatus()
      fetchJobs()
    }, 3000)
    return () => clearInterval(interval)
  }, [fetchStatus, fetchJobs])

  const handleUpload = async () => {
    setError('')
    setSubmitting(true)
    try {
      const res = await fetch('/api/company-info/upload', {
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

  const statusLabel = {
    pending: '等待中',
    queued: '排隊中',
    running: '上傳中',
    completed: '已完成',
    failed: '失敗',
  }

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#127970;</span>
        公司產業對照
      </h2>

      <div style={{ marginBottom: '12px', fontSize: '14px', color: '#666' }}>
        <div>CompanyInfo：{status.company_info_count.toLocaleString()} 筆</div>
        <div>IndustryMap：{status.industry_map_count.toLocaleString()} 筆</div>
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
            任務排隊/執行中...
          </>
        ) : (
          '更新公司產業對照'
        )}
      </button>

      {error && <div className="message message-error">{error}</div>}

      {jobs.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">上傳任務紀錄</h3>
          {jobs.map((job) => (
            <div key={job.job_id} className="job-item">
              <div className="job-header">
                <span className="job-info">公司產業對照</span>
                <span className={`badge badge-${job.status}`}>
                  {job.status === 'running' && <span className="spinner" />}
                  {statusLabel[job.status] || job.status}
                  {job.queue_position !== undefined && job.status === 'queued' &&
                    ` (第 ${job.queue_position} 位)`}
                </span>
              </div>
              {job.status === 'completed' && (
                <div className="job-info" style={{ marginTop: '4px' }}>
                  CompanyInfo {(job.company_info_count || 0).toLocaleString()} 筆，IndustryMap {(job.industry_map_count || 0).toLocaleString()} 筆
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

export default CompanyInfoUpload

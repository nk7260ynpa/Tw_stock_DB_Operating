import { useState, useEffect, useCallback } from 'react'
import { apiFetch } from '../api'

const STATUS_LABEL = {
  pending: '等待中',
  retrying: '重試中',
  success: '成功',
  exhausted: '已耗盡',
}

const TASK_TYPE_LABEL = {
  daily_upload: '每日資料',
  ctee_news: 'CTEE 新聞',
  cnyes_news: 'CNYES 新聞',
  ptt_news: 'PTT 新聞',
  moneyudn_news: 'MoneyUDN 新聞',
  tdcc: 'TDCC 集保',
}

function formatParams(taskType, params) {
  if (taskType === 'daily_upload') {
    const dates = params.dates || []
    return `${params.db_name} (${dates.length} 日)`
  }
  if (params.hours) {
    return `${params.hours} 小時`
  }
  return JSON.stringify(params)
}

function RetryQueue() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [actionMsg, setActionMsg] = useState(null)

  const fetchData = useCallback(async () => {
    try {
      const res = await apiFetch('/api/retry-queue')
      if (res.ok) {
        const json = await res.json()
        setData(json)
      }
    } catch {
      /* ignore */
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 10000)
    return () => clearInterval(interval)
  }, [fetchData])

  const handleRetryAll = async () => {
    setActionMsg(null)
    try {
      const res = await apiFetch('/api/retry-queue/retry-all', { method: 'POST' })
      if (res.ok) {
        const json = await res.json()
        // started 為 false 代表上一輪仍在跑而略過，不可顯示成綠色成功。
        setActionMsg({
          type: json.started === false ? 'warning' : 'success',
          text: json.message,
        })
        setTimeout(fetchData, 2000)
      }
    } catch {
      setActionMsg({ type: 'error', text: '無法連線至伺服器' })
    }
  }

  const handleResetExhausted = async () => {
    setActionMsg(null)
    try {
      const res = await apiFetch('/api/retry-queue/reset-exhausted', { method: 'POST' })
      if (res.ok) {
        const json = await res.json()
        setActionMsg({ type: 'success', text: json.message })
        fetchData()
      }
    } catch {
      setActionMsg({ type: 'error', text: '無法連線至伺服器' })
    }
  }

  const handleClearCompleted = async () => {
    setActionMsg(null)
    try {
      const res = await apiFetch('/api/retry-queue/clear', { method: 'DELETE' })
      if (res.ok) {
        const json = await res.json()
        setActionMsg({ type: 'success', text: json.message })
        fetchData()
      }
    } catch {
      setActionMsg({ type: 'error', text: '無法連線至伺服器' })
    }
  }

  const handleRemoveTask = async (taskId) => {
    try {
      const res = await apiFetch(`/api/retry-queue/${taskId}`, { method: 'DELETE' })
      if (res.ok) {
        fetchData()
      }
    } catch {
      /* ignore */
    }
  }

  if (loading || !data) {
    return (
      <div className="card">
        <h2 className="card-title">
          <span className="icon">&#128260;</span>
          重試佇列
        </h2>
        <div className="job-info">載入中...</div>
      </div>
    )
  }

  const { tasks, network_available, summary } = data
  const hasPending = summary.pending > 0
  const hasExhausted = summary.exhausted > 0
  const hasSuccess = summary.success > 0

  return (
    <div className="card">
      <h2 className="card-title">
        <span className="icon">&#128260;</span>
        重試佇列
      </h2>

      {/* 網路狀態 */}
      <div
        className="current-schedule"
        style={{
          background: network_available
            ? 'var(--color-success-bg)'
            : 'var(--color-error-bg)',
        }}
      >
        <div>
          <div className="time-label">爬蟲服務連線狀態</div>
          <div
            style={{
              fontSize: '1rem',
              fontWeight: 700,
              color: network_available
                ? 'var(--color-success)'
                : 'var(--color-error)',
            }}
          >
            {network_available ? '正常' : '中斷'}
          </div>
        </div>
      </div>

      {/* 統計摘要 */}
      {tasks.length > 0 && (
        <div
          style={{
            display: 'flex',
            gap: '8px',
            flexWrap: 'wrap',
            marginBottom: '16px',
          }}
        >
          {summary.pending > 0 && (
            <span className="badge badge-pending">
              等待中 {summary.pending}
            </span>
          )}
          {summary.retrying > 0 && (
            <span className="badge badge-running">
              重試中 {summary.retrying}
            </span>
          )}
          {summary.success > 0 && (
            <span className="badge badge-completed">
              成功 {summary.success}
            </span>
          )}
          {summary.exhausted > 0 && (
            <span className="badge badge-failed">
              已耗盡 {summary.exhausted}
            </span>
          )}
        </div>
      )}

      {/* 操作按鈕 */}
      <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
        {hasPending && (
          <button
            className="btn btn-primary"
            onClick={handleRetryAll}
            style={{ flex: 1 }}
          >
            立即重試
          </button>
        )}
        {hasExhausted && (
          <button
            className="btn btn-primary"
            onClick={handleResetExhausted}
            style={{
              flex: 1,
              background: 'var(--color-warning)',
            }}
          >
            重新執行失敗任務
          </button>
        )}
        {hasSuccess && (
          <button
            className="btn btn-primary"
            onClick={handleClearCompleted}
            style={{
              flex: 1,
              background: 'var(--color-success)',
            }}
          >
            清除已完成
          </button>
        )}
      </div>

      {actionMsg && (
        <div className={`message message-${actionMsg.type}`}>
          {actionMsg.text}
        </div>
      )}

      {/* 任務清單 */}
      {tasks.length === 0 && (
        <div
          className="job-info"
          style={{ marginTop: '16px', textAlign: 'center' }}
        >
          目前沒有重試任務
        </div>
      )}

      {tasks.length > 0 && (
        <div className="jobs-section">
          <h3 className="jobs-title">任務清單</h3>
          {tasks.map((task) => (
            <div key={task.task_id} className="job-item">
              <div className="job-header">
                <span className="job-info">
                  {TASK_TYPE_LABEL[task.task_type] || task.task_type}
                  {' - '}
                  {formatParams(task.task_type, task.params)}
                </span>
                <span className={`badge badge-${task.status === 'exhausted' ? 'failed' : task.status}`}>
                  {task.status === 'retrying' && <span className="spinner" />}
                  {STATUS_LABEL[task.status] || task.status}
                </span>
              </div>
              <div className="job-info" style={{ marginTop: '4px' }}>
                重試 {task.retry_count}/{task.max_retries} 次
                {task.failed_at && ` | 失敗時間: ${task.failed_at.replace('T', ' ').slice(0, 19)}`}
              </div>
              {task.error_message && (
                <div className="job-errors">{task.error_message}</div>
              )}
              <div style={{ marginTop: '8px', textAlign: 'right' }}>
                <button
                  onClick={() => handleRemoveTask(task.task_id)}
                  style={{
                    background: 'none',
                    border: '1px solid var(--color-border)',
                    borderRadius: '6px',
                    padding: '4px 12px',
                    fontSize: '0.75rem',
                    color: 'var(--color-error)',
                    cursor: 'pointer',
                  }}
                >
                  移除
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      <div
        style={{
          marginTop: '16px',
          fontSize: '0.8125rem',
          color: 'var(--color-text-secondary)',
        }}
      >
        <p>排程每小時自動檢查網路並重試失敗任務，最多重試 5 次。</p>
        <p style={{ marginTop: '4px' }}>
          超過上限的任務可透過「重新執行失敗任務」重設。
        </p>
      </div>
    </div>
  )
}

export default RetryQueue

/**
 * API 呼叫工具：自動套用 Vite base URL 前綴。
 *
 * 直接存取（port 8080）時 BASE_URL 為 "/"，呼叫 /api/xxx 不變。
 * 透過 Dashboard 反向代理（port 8002）存取時 BASE_URL 為 "/app/db-operating/"，
 * 呼叫會被重寫成 /app/db-operating/api/xxx，再由 Dashboard 代理 strip 前綴後轉發。
 */

const BASE = import.meta.env.BASE_URL.replace(/\/$/, '')

/**
 * 將相對 API 路徑組合成帶 base 前綴的完整路徑。
 *
 * @param {string} path - 原本以 "/" 開頭的 API 路徑
 * @returns {string} 帶 base 前綴的完整路徑
 */
export function apiUrl(path) {
  if (!path.startsWith('/')) {
    path = '/' + path
  }
  return `${BASE}${path}`
}

/**
 * fetch 的 wrapper，自動套用 base 前綴。
 *
 * @param {string} path - API 路徑
 * @param {RequestInit} [options] - fetch options
 * @returns {Promise<Response>}
 */
export function apiFetch(path, options) {
  return fetch(apiUrl(path), options)
}

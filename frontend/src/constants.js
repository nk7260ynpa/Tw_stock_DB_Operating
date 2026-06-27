/**
 * 前端共用常數。
 */

/**
 * 各上傳卡片「已上傳日期 / 已上傳季度」清單最多顯示的筆數。
 *
 * 後端 /api/<source>/uploaded 已依日期由新到舊（DESC）回傳，故前端僅取前
 * MAX_UPLOADED_SHOWN 筆即為最新數筆，藉以縮短卡片高度。此為純前端顯示限制，
 * 不影響後端資料或防重複上傳的 *Uploaded 紀錄。
 */
export const MAX_UPLOADED_SHOWN = 5

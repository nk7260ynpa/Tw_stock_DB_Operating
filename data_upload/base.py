"""資料上傳抽象基類模組。"""

import os
import logging
import requests
from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

# 爬蟲請求逾時秒數（可用環境變數 CRAW_TIMEOUT 覆寫）。
#
# 行情資料單日筆數較多（上萬筆），故預設值較其他上傳器寬鬆。此處**必須**設定
# timeout：未設 timeout 時 requests 會無限期等待，一旦爬蟲服務 hang 住，單一請求
# 即可卡住整個 daily_craw，連帶延後當日後續所有排程（實測曾延後逾 20 小時）。
# 逾時會拋出 requests.Timeout，於 craw_data 中歸類為可重試的 NetworkError。
_DEFAULT_CRAW_TIMEOUT = 120

try:
    CRAW_TIMEOUT = int(os.getenv("CRAW_TIMEOUT", str(_DEFAULT_CRAW_TIMEOUT)))
    if CRAW_TIMEOUT <= 0:
        raise ValueError("CRAW_TIMEOUT 需為正整數")
except ValueError:
    # 環境變數設錯不應讓整個服務在 import 期就起不來，退回預設值並記錄警告。
    logger.warning(
        "CRAW_TIMEOUT 環境變數值無效（%r），改用預設 %d 秒。",
        os.getenv("CRAW_TIMEOUT"), _DEFAULT_CRAW_TIMEOUT,
    )
    CRAW_TIMEOUT = _DEFAULT_CRAW_TIMEOUT


class CrawlError(Exception):
    """爬取資料失敗時拋出的異常。"""


class NetworkError(CrawlError):
    """網路連線失敗時拋出的異常。

    用於區分可重試的網路問題（ConnectionError、Timeout）
    與其他不可重試的爬取錯誤。
    """


class SourceError(NetworkError):
    """爬蟲可達、但該次抓取於來源端失敗時拋出（可重試）。

    刻意繼承 `NetworkError` 以維持「可重試」語意（仍會進 retry queue），
    但與傳輸層失敗區分開來，因為兩者該有的**批次策略相反**：

    * `NetworkError`（連不上爬蟲）：其後的日期／任務必然同樣失敗，
      應整批排入重試並**中止本輪**。
    * `SourceError`（爬蟲活著、只是這天抓不到）：僅該筆失敗，
      應**繼續處理後續**日期／任務。

    若不區分，`daily_craw` 會在昇冪排序的缺漏清單第一個「毒日期」上
    每天重複中斷，其後日期永遠不會被嘗試，直到滑出 30 天視窗即永久遺失。

    Attributes:
        partial_result (dict | None): 拋出前已成功落地的統計（如
            `record_count`、`file_count`）。`partial` 狀態下已取得的資料
            會先寫入才拋例外，呼叫端若不讀取此欄位就只能回報 0 筆，
            使用者會誤判成「完全沒抓到」。無資料落地時為 None。
    """

    def __init__(self, message, partial_result=None):
        """初始化來源端抓取失敗例外。

        Args:
            message (str): 錯誤說明文字。
            partial_result (dict | None): 已落地的統計結果，預設 None。
        """
        super().__init__(message)
        self.partial_result = partial_result


class OutOfRangeError(CrawlError):
    """查詢日期超出來源可回溯範圍時拋出（重試無用）。

    刻意繼承 `CrawlError` 而非 `NetworkError`：本專案以「是否為
    `NetworkError`」作為可重試判準，故此例外不會進入 retry queue，
    避免對來源根本不再提供的日期反覆重抓。

    Attributes:
        oldest_available (str | None): 來源目前最舊可取得的日期／時間。
    """

    def __init__(self, message, oldest_available=None):
        """初始化超出回溯範圍例外。

        Args:
            message (str): 錯誤說明文字。
            oldest_available (str | None): 來源最舊可取得的日期／時間。
        """
        super().__init__(message)
        self.oldest_available = oldest_available


# --- 爬蟲回應狀態（Tw_stock_crawer v2.13.0 起提供） ---
#
# 舊契約下，爬取失敗的回應「沒有 data 鍵」，本專案靠 `response.json()["data"]`
# 拋出的 KeyError 得知失敗（→ CrawlError → 不寫帳本 → 次日重抓）。新契約
# **保證 data 鍵永遠存在**（失敗時為 []／{}），KeyError 不再發生；若沿用
# 「有 data 就當成功」的邏輯，失敗日會被誤判為「當日無資料」而寫入
# `Open=False` 帳本 → `check_date` 之後永久跳過 → 真實資料永久遮蔽。
# 因此改以 status 欄位作為成敗的唯一判準。
STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_PARTIAL = "partial"
STATUS_OUT_OF_RANGE = "out_of_range"
STATUS_ERROR = "error"

# 代表「抓取成功」、可直接依 data 筆數繼續處理的狀態。
_PASSTHROUGH_STATUSES = frozenset({STATUS_OK, STATUS_EMPTY})

# --- 舊契約（爬蟲 v2.14.0 之前）的 meta 標記 ---
#
# 只在回應**不帶 `retryable`** 時使用。新契約已把判斷收斂成單一布林值，
# 下方兩組常數僅為向後相容而保留：舊版爬蟲的回應仍須維持既有行為，
# 不可因為「沒有 retryable」就當成不重抓——那正是「把失敗誤記成空」的老毛病。

# `partial` 時明確代表「重試有機會補齊」的 meta 標記。
# `list_failed` 在舊契約下靠預設重抓兜底，行為正確但重試原因說明模糊，
# 故一併列入以取得明確的原因字串。
_TRANSIENT_PARTIAL_META = (
    "list_failed", "detail_failed", "skipped_by_deadline",
)

# `partial` 時明確代表「重試也拿不到」的 meta 標記（來源硬上限）。
_PERMANENT_PARTIAL_META = "source_truncated"

# --- 新契約（爬蟲 v2.14.0 起）的不完整成因代碼 ---
#
# 與爬蟲 `tw_crawler/status.py` 的 `REASON_*` 對齊。爬蟲已用單一的
# `meta.retryable` 表達「重抓有沒有機會補回來」，下游不必再自行維護
# 成因黑白名單；`retryable_reasons` 僅用於**在可重試的前提下**再細分
# 值不值得付出重跑成本。
REASON_LIST_FAILED = "list_failed"      # 列表／分頁抓取中途失敗
REASON_DETAIL_FAILED = "detail_failed"  # 部分文章全文抓取失敗
REASON_DEADLINE = "deadline"            # 達單次時間上限而提前收工
REASON_CRAWL_FAILED = "crawl_failed"    # 爬蟲整體拋例外

# 「連有哪些文章都不知道」的成因：損失範圍無上限，一律重抓。
_UNBOUNDED_LOSS_REASONS = frozenset({
    REASON_LIST_FAILED, REASON_DEADLINE, REASON_CRAWL_FAILED,
})

# 僅有全文抓取失敗時的重抓門檻（失敗率達此值才值得整批重跑）。
#
# 取 0.2 的理由：
#
# 1. **重跑成本高**：retry queue 是同步重跑整個 48 小時窗（CTEE 正常
#    210~230 秒），且每小時觸發一次。若沿用舊的「1 篇失敗就重抓」，
#    PTT／MoneyUDN 在新契約下天天都會排重試（舊契約這些情形只回 `ok`）。
# 2. **detail-only 的損失多半會自癒**：全文抓失敗的文章被爬蟲**整篇排除**
#    在 `data` 之外（不是留空白內容），因此不會寫進 MySQL；隔天早上的排程
#    以 48 小時窗重抓時，這些 URL 仍是「新記錄」而會被正常補上——等於
#    每篇文章本來就有第二次免費機會，不需付出額外重跑成本。
#    界線：每日視窗只往前推 48 小時，故「今天視窗中較舊的那 24 小時」不會
#    被明天的視窗涵蓋，每篇文章總共只有兩次機會。連兩天同一篇都失敗時
#    才會真的漏掉，而那種系統性失敗即使立刻重抓通常也救不回來。
# 3. **門檻要抓的是系統性異常**：失敗率高到五分之一，多半代表來源端正在
#    擋人或全文頁改版，明天大概也修不好，值得立刻再試一次；零星幾篇失敗
#    交給隔日的自然重抓即可。
#
# 對照之下 `list_failed`／`deadline`／`crawl_failed` 一律重抓，因為那時
# 連「有哪些文章」都不知道，無從估計損失，且 CTEE 來源只保留約 3 天，
# 等不起。
DETAIL_FAILED_RETRY_RATIO = 0.2


def check_crawl_status(result, source, context="", allow_partial=False):
    """判讀爬蟲回應的 status 欄位，失敗時拋出對應例外。

    須在取用 `data` 之前呼叫，確保「抓取失敗」不會被當成「當日無資料」。

    狀態對應行為：

    | status         | 行為                                   | 可重試 |
    |----------------|----------------------------------------|--------|
    | `ok`/`empty`   | 放行，由呼叫端依 data 筆數處理         | —      |
    | `partial`      | 視 `allow_partial` 而定                | 是     |
    | `error`        | 拋 `SourceError`（0 筆不代表沒有）     | 是     |
    | `out_of_range` | 拋 `OutOfRangeError`（重試也沒用）     | 否     |

    **相容性**：`status` 缺席時（舊版爬蟲或非制式回應）一律放行，維持既有
    行為。未知狀態則保守視為可重試失敗——寧可多重試，也不可把失敗誤記成
    「當日無資料」而永久遮蔽。

    Args:
        result (dict): 爬蟲回應的 JSON 物件。
        source (str): 來源名稱，供錯誤訊息使用（如「CTEE 新聞」）。
        context (str): 補充情境，如「（2026-08-16）」。
        allow_partial (bool): 為 True 時 `partial` 不拋例外而回傳狀態值，
            供「資料照存、之後再補」的來源（新聞類）使用；為 False 時
            `partial` 拋 `SourceError`（行情類：`DailyPrice` 為 append
            寫入且無去重，存入部分資料會在重抓時產生重複列）。

    Returns:
        str: 判定後的狀態值（`ok`／`empty`／`partial`）。

    Raises:
        SourceError: `error`、未知狀態，或 `allow_partial` 為 False 的
            `partial`。皆可重試，但屬來源端失敗（爬蟲仍可達），呼叫端
            應僅跳過該筆、繼續處理後續日期。
        OutOfRangeError: `out_of_range`（不可重試）。
    """
    if not isinstance(result, dict):
        return STATUS_OK

    status = result.get("status")
    if status is None or status in _PASSTHROUGH_STATUSES:
        return status or STATUS_OK

    label = f"{source}{context}"
    detail = result.get("message") or result.get("error") or "（未提供說明）"

    if status == STATUS_OUT_OF_RANGE:
        meta = result.get("meta") or {}
        oldest = meta.get("oldest_available")
        suffix = f"（來源最舊可得：{oldest}）" if oldest else ""
        raise OutOfRangeError(
            f"{label} 超出來源可回溯範圍，重試無用：{detail}{suffix}",
            oldest_available=oldest,
        )

    if status == STATUS_PARTIAL:
        if allow_partial:
            return STATUS_PARTIAL
        raise SourceError(f"{label} 爬取結果不完整，需重抓：{detail}")

    if status == STATUS_ERROR:
        raise SourceError(f"{label} 爬取失敗，0 筆不代表無資料：{detail}")

    # 未知狀態：保守視為可重試失敗，絕不當成「當日無資料」寫入帳本。
    raise SourceError(f"{label} 回傳未知狀態 {status!r}：{detail}")


def partial_retry_reason(result):
    """判斷 `partial` 回應是否值得重抓，回傳原因說明。

    判讀順序：

    1. `meta.retryable` 存在（爬蟲 v2.14.0 起）→ 以它為**主判準**。
       爬蟲已彙整過所有成因，`retryable=False` 代表重抓結果不會改變
       （但需有 `non_retryable_reasons`／`source_truncated` 佐證，
       兩者皆空的退化回應仍保守重抓）。
    2. `retryable=True` 時再以 `retryable_reasons` 決定「值不值得」：
       只有全文抓取失敗（`detail_failed`）時看 `detail_failed_ratio`
       是否達 `DETAIL_FAILED_RETRY_RATIO`；其餘成因一律重抓。
    3. `meta` 不帶 `retryable`（舊版爬蟲或非制式回應）→ 走舊契約邏輯，
       維持**預設重抓**。絕不可因為「沒有 retryable」就當成不重抓，
       那會把失敗誤記成空而永久漏抓。

    Args:
        result (dict): 爬蟲回應的 JSON 物件。

    Returns:
        str | None: 值得重抓時回傳原因說明，不值得重抓時回傳 None
            （呼叫端應只告警，不排入 retry queue）。
    """
    if not isinstance(result, dict):
        return "未提供狀態細節"

    meta = result.get("meta") or {}
    if "retryable" in meta:
        return _retry_reason_from_contract(meta)
    return _legacy_retry_reason(result, meta)


def _retry_reason_from_contract(meta):
    """依 v2.14.0 契約的 `retryable` 系列欄位判斷是否值得重抓。

    Args:
        meta (dict): 爬蟲回應的 meta 物件（已確認含 `retryable`）。

    Returns:
        str | None: 值得重抓時回傳原因說明，否則 None。
    """
    if not meta.get("retryable"):
        # 第二道防線：`retryable=False` 必須有硬限制佐證才採信。
        #
        # 爬蟲 `classify_meta` 是以 `retryable = bool(retryable_reasons)`
        # 推導的，`partial` 必定至少帶一個成因碼，故正常情況不會走到這裡。
        # 但「不重抓」是本 repo 唯一會把失敗永久遮蔽的方向，若日後爬蟲回了
        # 退化的 `retryable: false`（兩個成因清單都空），寧可多跑一次也不能
        # 誤判成「重抓拿不到」。此分支只會把「不重抓」翻成「重抓」，
        # 不會反向削弱 `retryable` 作為單一判準的地位。
        blocked = [
            r for r in (meta.get("non_retryable_reasons") or []) if r
        ]
        if blocked or meta.get(_PERMANENT_PARTIAL_META):
            return None
        return "爬蟲標記不可重試但未提供硬限制佐證"

    reasons = [str(r) for r in (meta.get("retryable_reasons") or []) if r]
    if not reasons:
        # 標記可重試卻沒說明成因：保守重抓，不臆測。
        return "爬蟲標記可重試但未提供成因"

    # 只要有一項「連缺什麼都不知道」的成因，就不做門檻判斷。
    unbounded = [r for r in reasons if r in _UNBOUNDED_LOSS_REASONS]
    if unbounded:
        return "、".join(unbounded)

    # 未知的成因代碼（爬蟲日後新增）同樣保守重抓。
    unknown = [r for r in reasons if r != REASON_DETAIL_FAILED]
    if unknown:
        return "、".join(unknown)

    ratio = meta.get("detail_failed_ratio")
    failed = meta.get("detail_failed")
    if not isinstance(ratio, (int, float)):
        # 沒給失敗率就無從套門檻，保守重抓。
        return f"detail_failed={failed}（未提供失敗率）"
    if ratio >= DETAIL_FAILED_RETRY_RATIO:
        return (
            f"detail_failed_ratio={ratio}"
            f"（達重抓門檻 {DETAIL_FAILED_RETRY_RATIO}）"
        )
    return None


def _legacy_retry_reason(result, meta):
    """舊契約（無 `retryable`）的判讀邏輯，維持預設重抓。

    刻意不採白名單（只認特定 meta key 才重抓）：舊版各爬蟲對 `partial`
    的 meta 標註並不一致——例如 CNYES 的「翻頁中途失敗」只帶
    `fetched`／`pages`。白名單會把這些**該重試**的情形誤判成「重試無用」
    而永久漏抓，與 `check_crawl_status` 對未知狀態的保守原則自相矛盾。

    Args:
        result (dict): 爬蟲回應的 JSON 物件。
        meta (dict): 回應的 meta 物件。

    Returns:
        str | None: 值得重抓時回傳原因說明，重抓無用時回傳 None。
    """
    transient = [
        f"{key}={meta[key]}"
        for key in _TRANSIENT_PARTIAL_META
        if meta.get(key)
    ]
    if transient:
        # 即使同時有來源硬上限，暫時性失敗仍值得重抓補齊。
        return "、".join(transient)
    if meta.get(_PERMANENT_PARTIAL_META):
        return None
    return result.get("message") or "爬蟲未說明不完整的原因"


def partial_skip_note(result):
    """說明「判定不重抓」的理由，供只告警時寫進 log。

    `partial_retry_reason` 回 None 有兩種截然不同的成因，log 訊息若不加
    區分會誤導：來源硬限制是「重抓也拿不到」，低於門檻則是「重抓拿得到，
    但不值得為這幾篇重跑整批，隔日 48 小時窗會再抓一次」。

    Args:
        result (dict): 爬蟲回應的 JSON 物件。

    Returns:
        str | None: 不重抓的理由說明；判定為值得重抓時回傳 None。
    """
    if partial_retry_reason(result) is not None:
        return None
    if not isinstance(result, dict):
        return None

    meta = result.get("meta") or {}
    blocked = [str(r) for r in (meta.get("non_retryable_reasons") or []) if r]
    if blocked:
        return f"來源硬限制（{'、'.join(blocked)}），重抓也拿不到"
    if meta.get(_PERMANENT_PARTIAL_META):
        return "來源硬限制（source_truncated），重抓也拿不到"

    ratio = meta.get("detail_failed_ratio")
    if isinstance(ratio, (int, float)):
        return (
            f"僅全文抓取失敗且失敗率 {ratio} 低於門檻 "
            f"{DETAIL_FAILED_RETRY_RATIO}，缺的部分留待隔日 48 小時窗補回"
        )
    return "爬蟲判定重抓結果不會改變"


class DataUploadBase(ABC):
    """資料上傳抽象基類。

    定義爬蟲資料的預處理、schema 驗證與上傳流程。
    """

    stock_code_col = None
    stock_name_col = None
    daily_price_table = "DailyPrice"
    upload_date_table = "UploadDate"

    @abstractmethod
    def __init__(self, conn):
        """初始化資料上傳基類。

        Args:
            conn (sqlalchemy.engine.Connection): 資料庫連線物件。
        """
        self.name = os.path.basename(type(self).__module__.split('.')[-1])
        self.conn = conn

    @abstractmethod
    def preprocess(self, df):
        """預處理 DataFrame，上傳前進行資料轉換。"""
        pass

    def craw_data(self, date):
        """根據日期從爬蟲服務取得資料。

        若爬蟲服務回傳異常則拋出 CrawlError。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。

        Returns:
            pd.DataFrame: 包含每日資料的 DataFrame。

        Raises:
            NetworkError: 網路連線失敗或請求逾時（爬蟲不可達，可重試）。
            SourceError: 爬蟲回報 `error`／`partial`／未知狀態
                （爬蟲可達但該日抓取失敗，可重試）。
            OutOfRangeError: 爬蟲回報 `out_of_range` 時拋出（不可重試）。
            CrawlError: 其他爬取失敗時拋出（不可重試）。
        """
        url = f"{self.url}/{self.name}"
        payload = {"date": date}
        try:
            response = requests.get(url, params=payload, timeout=CRAW_TIMEOUT)
            response.raise_for_status()
            result = response.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            raise NetworkError(f"日期 {date} 網路連線失敗：{e}") from e
        except (requests.RequestException, ValueError) as e:
            raise CrawlError(f"日期 {date} 爬取失敗：{e}") from e

        # 先判讀 status 再取用 data：爬蟲新契約下失敗也會回 data: []，
        # 若不先判讀，失敗會被當成「當日無資料」而寫入 Open=False 帳本，
        # 使該日永久跳過、真實行情再也補不回。
        check_crawl_status(result, self.name.upper(), f"（{date}）")

        try:
            df = pd.DataFrame(result["data"])
        except (KeyError, ValueError) as e:
            raise CrawlError(f"日期 {date} 爬取失敗：{e}") from e
        return df

    def check_schema(self, df):
        """檢查 DataFrame 的 schema 是否符合 UploadType 模型。

        Args:
            df (pd.DataFrame): 待檢查的 DataFrame。

        Returns:
            pd.DataFrame: 經過 schema 驗證與轉換後的 DataFrame。
        """
        df_dict = df.to_dict(orient='records')
        df_schema = [self.UploadType(**record).__dict__ for record in df_dict]
        df = pd.DataFrame(df_schema)
        return df

    def check_date(self, date):
        """檢查該日期是否已存在於 UploadDate 資料表中。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。

        Returns:
            bool: 若日期已存在回傳 True，否則回傳 False。
        """
        if self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.upload_date_table} "
                f"WHERE Date = '{date}'"
            )
        ).scalar():
            return True
        return False

    def register_stock_names(self, df):
        """檢查並註冊新的股票代碼至 StockName 資料表。

        若 stock_code_col 或 stock_name_col 未設定則跳過（如 TAIFEX 無 StockName 表）。
        比對 DataFrame 中的股票代碼與資料庫現有記錄，將新代碼插入 StockName 表。

        Args:
            df (pd.DataFrame): 包含股票代碼與名稱的 DataFrame。
        """
        if self.stock_code_col is None or self.stock_name_col is None:
            return

        new_stocks = df[[self.stock_code_col, self.stock_name_col]].drop_duplicates(
            subset=[self.stock_code_col]
        )

        existing = self.conn.execute(
            text(f"SELECT {self.stock_code_col} FROM StockName")
        ).fetchall()
        existing_codes = {row[0] for row in existing}

        new_stocks = new_stocks[
            ~new_stocks[self.stock_code_col].isin(existing_codes)
        ]

        if new_stocks.empty:
            return

        new_stocks.to_sql(
            "StockName", self.conn,
            if_exists='append', index=False
        )
        self.conn.commit()
        logger.info(
            f"新增 {len(new_stocks)} 筆股票代碼至 StockName："
            f"{new_stocks[self.stock_code_col].tolist()}"
        )

    def upload_df(self, df):
        """上傳每日資料至 DailyPrice 資料表。

        Args:
            df (pd.DataFrame): 包含每日資料的 DataFrame。
        """
        df_copy = self.preprocess(df.copy())
        df_copy = self.check_schema(df_copy)
        df_copy.to_sql(
            self.daily_price_table, self.conn,
            if_exists='append', index=False, chunksize=1000
        )
        self.conn.commit()

    def upload_date(self, date, df):
        """上傳日期記錄至 UploadDate 資料表。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。
            df (pd.DataFrame): 該日期的資料 DataFrame，用於判斷是否為交易日。
        """
        if df.shape[0] != 0:
            update = text(
                f"INSERT INTO {self.upload_date_table} "
                f"(Date, Open) VALUES ('{date}', True);"
            )
            self.conn.execute(update)
            self.conn.commit()
        else:
            update = text(
                f"INSERT INTO {self.upload_date_table} "
                f"(Date, Open) VALUES ('{date}', False);"
            )
            self.conn.execute(update)
            self.conn.commit()

    def upload(self, date):
        """執行上傳流程。

        若該日期資料已存在則跳過，否則爬取資料並上傳至資料庫。
        爬取失敗時不會寫入 UploadDate，以確保後續可重新上傳。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。
        """
        if self.check_date(date):
            logger.info(
                f"日期 {date} 的資料已存在於資料庫中，跳過上傳。"
            )
        else:
            try:
                df = self.craw_data(date)
            except NetworkError:
                raise
            except CrawlError as e:
                logger.error(str(e))
                return
            if df.shape[0] > 0:
                self.register_stock_names(df)
                self.upload_df(df)
            self.upload_date(date, df)
            logger.info(f"日期 {date} 的資料已成功上傳至資料庫。")

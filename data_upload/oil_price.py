"""原油價格資料上傳模組。

從爬蟲服務取得國際原油價格資料（WTI、Brent），
並上傳至 SPECIAL_INFO 資料庫的 OilPrice 表。
使用 REPLACE INTO 避免重複寫入，並記錄已上傳日期至 OilPriceUploaded 表。
"""

import logging
from decimal import Decimal

import pandas as pd
import requests
from pydantic import BaseModel
from sqlalchemy import text

from data_upload import special_info_common
from data_upload.base import CrawlError, NetworkError

logger = logging.getLogger(__name__)


class OilPriceType(BaseModel):
    """原油價格資料 schema。

    欄位名稱對應 Tw_stock_DB 的 OilPrice 表結構。
    """

    Date: str
    Product: str
    Open: Decimal
    High: Decimal
    Low: Decimal
    Close: Decimal
    Volume: int


class OilPriceUploader:
    """原油價格資料上傳器。

    從爬蟲取得 WTI/Brent 原油價格，
    使用 REPLACE INTO 寫入 SPECIAL_INFO 資料庫。
    資料表結構由 Tw_stock_DB 專案負責建立與管理。

    原油為非 24/7 市場（is_continuous_market=False）。

    排程一律只請求「昨日」（web_server.settled_end_date），確保請求日的日 K
    已定案；此前提成立時，爬蟲 fallback 到更早日期即代表請求日為非交易日。
    但記帳前仍以 special_info_common._is_settled 再守一次（人工／回填可能
    請求今日），且只在爬蟲 status 為 empty／fallback 且
    meta.target_date_available 非真時才標記；status 為 partial／error／未知
    一律拋 SourceError 進重試佇列，絕不寫帳本（詳見 special_info_common）。
    """

    # 非 24/7 市場，供 special_info_common 判斷帳本語意與缺漏偵測行為。
    is_continuous_market = False
    price_table = "OilPrice"
    uploaded_table = "OilPriceUploaded"
    asset_label = "原油價格"

    def __init__(self, conn, crawler_host):
        """初始化原油價格上傳器。

        Args:
            conn: SQLAlchemy 連線物件（SPECIAL_INFO 資料庫）。
            crawler_host (str): 爬蟲服務主機位址（含 port）。
        """
        self.conn = conn
        self.crawler_host = crawler_host

    def check_uploaded(self, date):
        """檢查指定日期是否已上傳。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            bool: 若已上傳回傳 True，否則回傳 False。
        """
        result = self.conn.execute(
            text("SELECT COUNT(*) FROM OilPriceUploaded WHERE Date = :date"),
            {"date": date},
        ).scalar()
        return result > 0

    def crawl_data(self, date):
        """從爬蟲服務取得指定日期的原油價格資料。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            pd.DataFrame: 原油價格 DataFrame。

        Raises:
            NetworkError: 無法連線爬蟲（可重試，整批中止）。
            SourceError: 來源端抓取失敗、不完整或狀態未知（可重試，
                逐日隔離，一律不得寫入帳本）。
            OutOfRangeError: 早於來源可回溯範圍（不重試）。
            CrawlError: 爬蟲呼叫失敗或回傳資料缺少必要欄位。
        """
        url = f"http://{self.crawler_host}/oil_price"
        try:
            resp = requests.get(url, params={"date": date}, timeout=30)
            resp.raise_for_status()
        except (requests.ConnectionError, requests.Timeout) as e:
            raise NetworkError(
                f"原油價格爬蟲網路連線失敗（{date}）：{e}"
            ) from e
        except requests.RequestException as e:
            raise CrawlError(
                f"原油價格爬蟲呼叫失敗（{date}）：{e}"
            ) from e

        return special_info_common.parse_price_response(
            self, resp.json(), date
        )

    def check_schema(self, df):
        """使用 Pydantic 驗證 DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame。
        """
        records = df.to_dict(orient="records")
        validated = [
            OilPriceType(**record).model_dump()
            for record in records
        ]
        return pd.DataFrame(validated)

    def _replace_into(self, df):
        """使用 REPLACE INTO 批次寫入 OilPrice 資料。

        Args:
            df (pd.DataFrame): 待寫入的 DataFrame。
        """
        if df.empty:
            return

        columns = df.columns.tolist()
        col_str = ", ".join(columns)
        placeholder_str = ", ".join([f":{col}" for col in columns])
        sql = f"REPLACE INTO OilPrice ({col_str}) VALUES ({placeholder_str})"

        records = df.to_dict(orient="records")
        # Decimal 轉為字串避免浮點精度問題
        for record in records:
            for key in ("Open", "High", "Low", "Close"):
                if key in record and isinstance(record[key], Decimal):
                    record[key] = str(record[key])
            self.conn.execute(text(sql), record)
        self.conn.commit()

    def _record_uploaded_date(self, date):
        """記錄已上傳日期至 OilPriceUploaded 表。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。
        """
        self.conn.execute(
            text(
                "INSERT IGNORE INTO OilPriceUploaded (Date) "
                "VALUES (:date)"
            ),
            {"date": date},
        )
        self.conn.commit()

    def upload(self, date):
        """執行原油價格資料上傳流程。

        從爬蟲取得指定日期資料，檢查帳本是否已標記，若未標記則依帳本語意
        寫入資料庫並記帳（實際交易日；fallback／空時額外標記請求日為非交易日）。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            dict: 包含 date 和 record_count 的結果字典。

        Raises:
            NetworkError: 網路連線失敗（供排程重試機制使用）。
        """
        if self.check_uploaded(date):
            logger.info("原油價格 %s 資料已存在，跳過上傳。", date)
            return {"date": date, "record_count": 0}

        result = special_info_common.fetch_and_store(self, date)
        return {"date": date, "record_count": result["record_count"]}

    def backfill_date(self, date):
        """回補單一日期（缺漏偵測用；不檢查帳本，套用新帳本語意）。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            dict: 包含 date、record_count 與 filled 的結果字典。
        """
        return special_info_common.fetch_and_store(self, date)

    def find_missing_dates(self, days=30):
        """找出近 N 天在價格表缺漏、需補抓的候選日期。

        Args:
            days (int): 掃描天數，預設 30。

        Returns:
            list[str]: 由舊到新排序的候選缺漏日期字串。
        """
        return special_info_common.find_missing_dates(self, days=days)

    def backfill_missing(self, days=30, today=None, deep=False,
                         reverify_days=0):
        """掃描近 N 天缺漏並補抓（冪等、可重跑）。

        Args:
            days (int): 掃描天數，預設 30。
            today (str | datetime.date | None): 掃描基準日（含），預設當日。
                排程呼叫時固定傳「昨日」，只重驗已定案的日 K。
            deep (bool): 是否先清除整個窗的孤兒帳本再重驗，預設 False。
            reverify_days (int): deep=False 時要清除孤兒帳本的天數，
                預設 0（不清）。日常排程傳入小窗即可自我修復誤標。

        Returns:
            dict: 補抓摘要。
        """
        return special_info_common.backfill_missing(
            self, days=days, today=today, deep=deep,
            reverify_days=reverify_days,
        )

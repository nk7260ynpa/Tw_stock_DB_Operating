"""黃金價格資料上傳模組。

從爬蟲服務取得國際黃金期貨價格資料，
並上傳至 SPECIAL_INFO 資料庫的 GoldPrice 表。
使用 REPLACE INTO 避免重複寫入，並記錄已上傳日期至 GoldPriceUploaded 表。
"""

import logging
from decimal import Decimal

import pandas as pd
import requests
from pydantic import BaseModel
from sqlalchemy import text

from data_upload.base import CrawlError, NetworkError

logger = logging.getLogger(__name__)


class GoldPriceType(BaseModel):
    """黃金價格資料 schema。

    欄位名稱對應 Tw_stock_DB 的 GoldPrice 表結構。
    """

    Date: str
    Product: str
    Open: Decimal
    High: Decimal
    Low: Decimal
    Close: Decimal
    Volume: int


class GoldPriceUploader:
    """黃金價格資料上傳器。

    從爬蟲取得黃金期貨價格，
    使用 REPLACE INTO 寫入 SPECIAL_INFO 資料庫。
    資料表結構由 Tw_stock_DB 專案負責建立與管理。
    """

    def __init__(self, conn, crawler_host):
        """初始化黃金價格上傳器。

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
            text("SELECT COUNT(*) FROM GoldPriceUploaded WHERE Date = :date"),
            {"date": date},
        ).scalar()
        return result > 0

    def crawl_data(self, date):
        """從爬蟲服務取得指定日期的黃金價格資料。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            pd.DataFrame: 黃金價格 DataFrame。

        Raises:
            NetworkError: 網路連線失敗。
            CrawlError: 爬取失敗或資料格式異常。
        """
        url = f"http://{self.crawler_host}/gold_price"
        try:
            resp = requests.get(url, params={"date": date}, timeout=30)
            resp.raise_for_status()
        except (requests.ConnectionError, requests.Timeout) as e:
            raise NetworkError(
                f"黃金價格爬蟲網路連線失敗（{date}）：{e}"
            ) from e
        except requests.RequestException as e:
            raise CrawlError(
                f"黃金價格爬蟲呼叫失敗（{date}）：{e}"
            ) from e

        result = resp.json()
        if "error" in result:
            raise CrawlError(
                f"黃金價格爬蟲回傳錯誤（{date}）：{result['error']}"
            )
        data = result.get("data")
        if not data:
            logger.info("黃金價格 %s 無資料（可能非交易日）。", date)
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # 欄位名稱標準化：爬蟲回傳小寫，需映射至資料庫大寫
        column_mapping = {
            "product": "Product",
            "date": "Date",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
        df = df.rename(columns=column_mapping)

        # 確保必要欄位存在
        required = {"Date", "Product", "Open", "High", "Low", "Close", "Volume"}
        if not required.issubset(set(df.columns)):
            missing = required - set(df.columns)
            raise CrawlError(
                f"黃金價格爬蟲回傳資料缺少欄位：{missing}"
            )

        return df

    def check_schema(self, df):
        """使用 Pydantic 驗證 DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame。
        """
        records = df.to_dict(orient="records")
        validated = [
            GoldPriceType(**record).model_dump()
            for record in records
        ]
        return pd.DataFrame(validated)

    def _replace_into(self, df):
        """使用 REPLACE INTO 批次寫入 GoldPrice 資料。

        Args:
            df (pd.DataFrame): 待寫入的 DataFrame。
        """
        if df.empty:
            return

        columns = df.columns.tolist()
        col_str = ", ".join(columns)
        placeholder_str = ", ".join([f":{col}" for col in columns])
        sql = f"REPLACE INTO GoldPrice ({col_str}) VALUES ({placeholder_str})"

        records = df.to_dict(orient="records")
        # Decimal 轉為字串避免浮點精度問題
        for record in records:
            for key in ("Open", "High", "Low", "Close"):
                if key in record and isinstance(record[key], Decimal):
                    record[key] = str(record[key])
            self.conn.execute(text(sql), record)
        self.conn.commit()

    def _record_uploaded_date(self, date):
        """記錄已上傳日期至 GoldPriceUploaded 表。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。
        """
        self.conn.execute(
            text(
                "INSERT IGNORE INTO GoldPriceUploaded (Date) "
                "VALUES (:date)"
            ),
            {"date": date},
        )
        self.conn.commit()

    def upload(self, date):
        """執行黃金價格資料上傳流程。

        從爬蟲取得指定日期資料，檢查是否已上傳，
        若未上傳則驗證後寫入資料庫並記錄上傳日期。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            dict: 包含 date 和 record_count 的結果字典。

        Raises:
            NetworkError: 網路連線失敗（供排程重試機制使用）。
        """
        if self.check_uploaded(date):
            logger.info("黃金價格 %s 資料已存在，跳過上傳。", date)
            return {"date": date, "record_count": 0}

        df = self.crawl_data(date)

        if df.empty:
            # 非交易日，記錄已處理以避免重複檢查
            self._record_uploaded_date(date)
            logger.info("黃金價格 %s 無資料（非交易日），已記錄。", date)
            return {"date": date, "record_count": 0}

        df = self.check_schema(df)
        record_count = len(df)

        self._replace_into(df)
        self._record_uploaded_date(date)

        logger.info(
            "黃金價格 %s 資料已上傳，共 %d 筆。",
            date, record_count,
        )
        return {"date": date, "record_count": record_count}

"""TDCC 集保庫存分級資料上傳模組。

從爬蟲服務取得 TDCC（臺灣集中保管結算所）集保庫存分級資料，
並上傳至 TWSE 資料庫的 TDCC 表。
資料為每週更新，API 固定回傳最新一期。
"""

import logging
from decimal import Decimal

import pandas as pd
import requests
from pydantic import BaseModel
from sqlalchemy import text

logger = logging.getLogger(__name__)

# 爬蟲欄位 → 資料庫欄位對應
COLUMN_MAPPING = {
    "HoldingLevel": "Level",
    "Shares": "HoldingShares",
    "Percentage": "HoldingRatio",
}


class TDCCType(BaseModel):
    """TDCC 集保庫存分級資料 schema。

    欄位名稱對應 Tw_stock_DB 的 TDCC 表結構。
    """

    Date: str
    SecurityCode: str
    Level: str
    Holders: int
    HoldingShares: int
    HoldingRatio: Decimal


class TDCCUploader:
    """TDCC 集保庫存分級資料上傳器。

    資料表結構由 Tw_stock_DB 專案負責建立與管理。
    """

    def __init__(self, conn, crawler_host):
        """初始化 TDCC 上傳器。

        Args:
            conn: SQLAlchemy 連線物件（TWSE 資料庫）。
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
            text("SELECT COUNT(*) FROM TDCC WHERE Date = :date"),
            {"date": date},
        ).scalar()
        return result > 0

    def get_latest_uploaded_date(self):
        """取得最新已上傳日期。

        Returns:
            str | None: 最新日期字串，無資料時回傳 None。
        """
        result = self.conn.execute(
            text("SELECT MAX(Date) FROM TDCC")
        ).scalar()
        if result is None:
            return None
        return str(result)

    def crawl_data(self):
        """從爬蟲服務取得 TDCC 資料。

        爬蟲 API 回傳格式為 {"date": "...", "data": [...]}，
        其中 data 陣列內的 Date 欄位帶有時間戳（如 2026-02-13T00:00:00），
        需截取日期部分。HoldingLevel 為數字（1-17），需轉為字串。

        Returns:
            tuple[str, pd.DataFrame]: (日期, 資料 DataFrame)。
                爬取失敗時回傳 (None, 空 DataFrame)。
        """
        url = f"http://{self.crawler_host}/tdcc"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error("TDCC 爬蟲呼叫失敗：%s", e)
            return None, pd.DataFrame()

        result = resp.json()
        if not result:
            logger.warning("TDCC 爬蟲回傳空資料")
            return None, pd.DataFrame()

        # API 回傳巢狀結構：{"date": "...", "data": [...]}
        records = result.get("data", result)
        if isinstance(records, dict):
            # 若無 data key 且本身是 dict，視為異常
            logger.warning("TDCC 爬蟲回傳資料格式異常")
            return None, pd.DataFrame()

        if not records:
            logger.warning("TDCC 爬蟲回傳空資料")
            return None, pd.DataFrame()

        df = pd.DataFrame(records)
        if df.empty or "Date" not in df.columns:
            logger.warning("TDCC 爬蟲回傳資料格式異常")
            return None, pd.DataFrame()

        # Date 欄位截取日期部分（移除 T00:00:00）
        df["Date"] = df["Date"].astype(str).str[:10]

        # HoldingLevel 轉為字串
        if "HoldingLevel" in df.columns:
            df["HoldingLevel"] = df["HoldingLevel"].astype(str)

        date = df["Date"].iloc[0]
        return date, df

    def _rename_columns(self, df):
        """套用欄位名稱對應。

        Args:
            df (pd.DataFrame): 原始 DataFrame。

        Returns:
            pd.DataFrame: 重新命名後的 DataFrame。
        """
        return df.rename(columns=COLUMN_MAPPING)

    def check_schema(self, df):
        """使用 Pydantic 驗證 DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame。
        """
        records = df.to_dict(orient="records")
        validated = [
            TDCCType(**record).model_dump()
            for record in records
        ]
        return pd.DataFrame(validated)

    def upload(self):
        """執行 TDCC 資料上傳流程。

        從爬蟲取得最新一期資料，檢查是否已上傳，
        若未上傳則驗證後寫入資料庫。

        Returns:
            dict: 包含 date 和 record_count 的結果字典。
        """
        date, df = self.crawl_data()

        if df.empty:
            logger.info("TDCC 無資料可上傳。")
            return {"date": None, "record_count": 0}

        if self.check_uploaded(date):
            logger.info("TDCC %s 資料已存在，跳過上傳。", date)
            return {"date": date, "record_count": 0}

        df = self._rename_columns(df)
        df = self.check_schema(df)
        record_count = len(df)

        # HoldingRatio 轉為字串避免浮點精度問題（Decimal → str → DB DECIMAL）
        df["HoldingRatio"] = df["HoldingRatio"].astype(str)

        df.to_sql(
            "TDCC", self.conn,
            if_exists="append", index=False, chunksize=1000,
        )
        self.conn.commit()

        logger.info(
            "TDCC %s 資料已上傳，共 %d 筆。",
            date, record_count,
        )
        return {"date": date, "record_count": record_count}

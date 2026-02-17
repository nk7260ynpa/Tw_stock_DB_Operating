"""TAIFEX 資料上傳模組。"""

import logging
from typing import Optional

from pydantic import BaseModel
from datetime import datetime

from data_upload.base import DataUploadBase

logger = logging.getLogger(__name__)


class UploadType(BaseModel):
    """TAIFEX 期貨每日行情資料 schema。"""

    Date: datetime
    Contract: str
    ContractMonth: str
    Open: Optional[float] = None
    High: Optional[float] = None
    Low: Optional[float] = None
    Last: Optional[float] = None
    Change: Optional[float] = None
    ChangePercent: Optional[float] = None
    Volume: int
    SettlementPrice: Optional[float] = None
    OpenInterest: Optional[float] = None
    BestBid: Optional[float] = None
    BestAsk: Optional[float] = None
    HistoricalHigh: Optional[float] = None
    HistoricalLow: Optional[float] = None
    TradingHalt: Optional[float] = None
    TradingSession: str
    SpreadOrderVolume: Optional[float] = None


class Uploader(DataUploadBase):
    """TAIFEX 資料上傳器。"""

    def __init__(self, conn, host):
        """初始化 TAIFEX 上傳器。

        Args:
            conn (sqlalchemy.engine.Connection): 資料庫連線物件。
            host (str): 爬蟲服務主機位址。
        """
        super().__init__(conn)
        self.UploadType = UploadType
        self.url = f"http://{host}"

    def preprocess(self, df):
        """預處理 DataFrame（TAIFEX 無需額外處理）。

        Args:
            df (pd.DataFrame): 待預處理的 DataFrame。

        Returns:
            pd.DataFrame: 原始 DataFrame。
        """
        return df

import logging

from pydantic import BaseModel
from datetime import datetime

from data_upload.base import DataUploadBase

logger = logging.getLogger(__name__)


class UploadType(BaseModel):
    """TAIFEX 期貨每日行情資料 schema。"""

    Date: datetime
    Contract: str
    ContractMonth: str
    Open: float
    High: float
    Low: float
    Last: float
    Change: float
    ChangePercent: float
    Volume: int
    SettlementPrice: float
    OpenInterest: float
    BestBid: float
    BestAsk: float
    HistoricalHigh: float
    HistoricalLow: float
    TradingHalt: float
    TradingSession: str
    SpreadOrderVolume: float


class Uploader(DataUploadBase):
    """TAIFEX 資料上傳器。"""

    def __init__(self, conn, host):
        """初始化 TAIFEX 上傳器。

        Args:
            conn: 資料庫連線物件。
            host: 爬蟲服務主機位址。
        """
        super().__init__(conn)
        self.UploadType = UploadType
        self.url = f"http://{host}"

    def preprocess(self, df):
        """預處理 DataFrame（TAIFEX 無需額外處理）。

        Args:
            df: 待預處理的 DataFrame。

        Returns:
            原始 DataFrame。
        """
        return df

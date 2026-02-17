import logging

from pydantic import BaseModel
from datetime import datetime

from data_upload.base import DataUploadBase

logger = logging.getLogger(__name__)


class UploadType(BaseModel):
    """TPEX 每日行情資料 schema。"""

    Date: datetime
    Code: str
    Close: float
    Change: float
    Open: float
    High: float
    Low: float
    TradeVolume: float
    TradeAmount: float
    NumberOfTransactions: int
    LastBestBidPrice: float
    LastBidVolume: float
    LastBestAskPrice: float
    LastBestAskVolume: float
    IssuedShares: int
    NextDayUpLimitPrice: float
    NextDayDownLimitPrice: float


class Uploader(DataUploadBase):
    """TPEX 資料上傳器。"""

    def __init__(self, conn, host):
        """初始化 TPEX 上傳器。

        Args:
            conn: 資料庫連線物件。
            host: 爬蟲服務主機位址。
        """
        super().__init__(conn)
        self.UploadType = UploadType
        self.url = f"http://{host}"

    def preprocess(self, df):
        """預處理 DataFrame，移除 Name 欄位。

        Args:
            df: 待預處理的 DataFrame。

        Returns:
            移除 Name 欄位後的 DataFrame。
        """
        df = df.drop(columns=['Name'])
        return df

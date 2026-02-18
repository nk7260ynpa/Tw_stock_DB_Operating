"""TWSE 資料上傳模組。"""

import logging

from pydantic import BaseModel
from datetime import datetime

from data_upload.base import DataUploadBase

logger = logging.getLogger(__name__)


class UploadType(BaseModel):
    """TWSE 每日行情資料 schema。"""

    Date: datetime
    SecurityCode: str
    TradeVolume: int
    Transaction: int
    TradeValue: int
    OpeningPrice: float
    HighestPrice: float
    LowestPrice: float
    ClosingPrice: float
    Change: float
    LastBestBidPrice: float
    LastBestBidVolume: int
    LastBestAskPrice: float
    LastBestAskVolume: int
    PriceEarningratio: float


class Uploader(DataUploadBase):
    """TWSE 資料上傳器。"""

    def __init__(self, conn, host):
        """初始化 TWSE 上傳器。

        Args:
            conn (sqlalchemy.engine.Connection): 資料庫連線物件。
            host (str): 爬蟲服務主機位址。
        """
        super().__init__(conn)
        self.UploadType = UploadType
        self.url = f"http://{host}"
        self.stock_code_col = "SecurityCode"
        self.stock_name_col = "StockName"

    def preprocess(self, df):
        """預處理 DataFrame，移除 StockName 欄位。

        Args:
            df (pd.DataFrame): 待預處理的 DataFrame。

        Returns:
            pd.DataFrame: 移除 StockName 欄位後的 DataFrame。
        """
        df = df.drop(columns=['StockName'])
        return df

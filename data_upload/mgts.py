import logging

from pydantic import BaseModel
from datetime import datetime

from data_upload.base import DataUploadBase

logger = logging.getLogger(__name__)


class UploadType(BaseModel):
    """MGTS 融資融券資料 schema。"""

    Date: datetime
    SecurityCode: str
    MarginPurchase: int
    MarginSales: int
    CashRedemption: int
    MarginPurchaseBalanceOfPreviousDay: int
    MarginPurchaseBalanceOfTheDay: int
    MarginPurchaseQuotaForTheNextDay: int
    ShortCovering: int
    ShortSale: int
    StockRedemption: int
    ShortSaleBalanceOfPreviousDay: int
    ShortSaleBalanceOfTheDay: int
    ShortSaleQuotaForTheNextDay: int
    OffsettingOfMarginPurchasesAndShortSales: int
    Note: str


class Uploader(DataUploadBase):
    """MGTS 資料上傳器。"""

    def __init__(self, conn, host):
        """初始化 MGTS 上傳器。

        Args:
            conn: 資料庫連線物件。
            host: 爬蟲服務主機位址。
        """
        super().__init__(conn)
        self.UploadType = UploadType
        self.url = f"http://{host}"

    def preprocess(self, df):
        """預處理 DataFrame，移除 StockName 欄位。

        Args:
            df: 待預處理的 DataFrame。

        Returns:
            移除 StockName 欄位後的 DataFrame。
        """
        df = df.drop(columns=['StockName'])
        return df

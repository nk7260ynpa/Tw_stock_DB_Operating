"""FAOI 資料上傳模組。"""

import logging

from pydantic import BaseModel
from datetime import datetime

from data_upload.base import DataUploadBase

logger = logging.getLogger(__name__)


class UploadType(BaseModel):
    """FAOI 三大法人買賣超資料 schema。"""

    Date: datetime
    SecurityCode: str
    ForeignInvestorsTotalBuy: int
    ForeignInvestorsTotalSell: int
    ForeignInvestorsDifference: int
    ForeignDealersTotalBuy: int
    ForeignDealersTotalSell: int
    ForeignDealersDifference: int
    SecuritiesInvestmentTotalBuy: int
    SecuritiesInvestmentTotalSell: int
    SecuritiesInvestmentDifference: int
    DealersDifference: int
    DealersProprietaryTotalBuy: int
    DealersProprietaryTotalSell: int
    DealersProprietaryDifference: int
    DealersHedgeTotalBuy: int
    DealersHedgeTotalSell: int
    DealersHedgeDifference: int
    TotalDifference: int


class Uploader(DataUploadBase):
    """FAOI 資料上傳器。"""

    def __init__(self, conn, host):
        """初始化 FAOI 上傳器。

        Args:
            conn (sqlalchemy.engine.Connection): 資料庫連線物件。
            host (str): 爬蟲服務主機位址。
        """
        super().__init__(conn)
        self.UploadType = UploadType
        self.url = f"http://{host}"

    def preprocess(self, df):
        """預處理 DataFrame，移除 StockName 欄位。

        Args:
            df (pd.DataFrame): 待預處理的 DataFrame。

        Returns:
            pd.DataFrame: 移除 StockName 欄位後的 DataFrame。
        """
        df = df.drop(columns=['StockName'])
        return df

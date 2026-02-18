"""MGTS 資料上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock

import pandas as pd
from pydantic import ValidationError

from data_upload.mgts import Uploader, UploadType


class TestUploadType(unittest.TestCase):
    """測試 MGTS UploadType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = UploadType(
            Date="2026-01-02",
            SecurityCode="2330",
            MarginPurchase=1000,
            MarginSales=800,
            CashRedemption=100,
            MarginPurchaseBalanceOfPreviousDay=5000,
            MarginPurchaseBalanceOfTheDay=5100,
            MarginPurchaseQuotaForTheNextDay=10000,
            ShortCovering=200,
            ShortSale=300,
            StockRedemption=50,
            ShortSaleBalanceOfPreviousDay=2000,
            ShortSaleBalanceOfTheDay=2050,
            ShortSaleQuotaForTheNextDay=5000,
            OffsettingOfMarginPurchasesAndShortSales=0,
            Note="",
        )

        self.assertEqual(data.SecurityCode, "2330")
        self.assertEqual(data.MarginPurchase, 1000)

    def test_invalid_missing_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            UploadType(
                Date="2026-01-02",
                SecurityCode="2330",
            )


class TestUploader(unittest.TestCase):
    """測試 MGTS Uploader。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = Uploader(self.mock_conn, "localhost:6738")

    def test_init_attributes(self):
        """測試初始化屬性設定。"""
        self.assertEqual(self.uploader.url, "http://localhost:6738")
        self.assertEqual(self.uploader.stock_code_col, "SecurityCode")
        self.assertEqual(self.uploader.stock_name_col, "StockName")
        self.assertEqual(self.uploader.name, "mgts")

    def test_preprocess_drops_stock_name(self):
        """測試預處理移除 StockName 欄位。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
            "MarginPurchase": [1000],
        })

        result = self.uploader.preprocess(df)

        self.assertNotIn("StockName", result.columns)
        self.assertIn("SecurityCode", result.columns)


if __name__ == "__main__":
    unittest.main()

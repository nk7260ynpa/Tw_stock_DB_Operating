"""TWSE 資料上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock

import pandas as pd
from pydantic import ValidationError

from data_upload.twse import Uploader, UploadType


class TestUploadType(unittest.TestCase):
    """測試 TWSE UploadType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = UploadType(
            Date="2026-01-02",
            SecurityCode="2330",
            TradeVolume=10000,
            Transaction=500,
            TradeValue=5000000,
            OpeningPrice=100.0,
            HighestPrice=105.0,
            LowestPrice=99.0,
            ClosingPrice=103.0,
            Change=3.0,
            LastBestBidPrice=102.5,
            LastBestBidVolume=100,
            LastBestAskPrice=103.5,
            LastBestAskVolume=200,
            PriceEarningratio=15.5,
        )

        self.assertEqual(data.SecurityCode, "2330")
        self.assertAlmostEqual(data.ClosingPrice, 103.0)

    def test_invalid_missing_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            UploadType(
                Date="2026-01-02",
                SecurityCode="2330",
            )


class TestUploader(unittest.TestCase):
    """測試 TWSE Uploader。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = Uploader(self.mock_conn, "localhost:6738")

    def test_init_attributes(self):
        """測試初始化屬性設定。"""
        self.assertEqual(self.uploader.url, "http://localhost:6738")
        self.assertEqual(self.uploader.stock_code_col, "SecurityCode")
        self.assertEqual(self.uploader.stock_name_col, "StockName")
        self.assertEqual(self.uploader.name, "twse")

    def test_preprocess_drops_stock_name(self):
        """測試預處理移除 StockName 欄位。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
            "Value": [100.0],
        })

        result = self.uploader.preprocess(df)

        self.assertNotIn("StockName", result.columns)
        self.assertIn("SecurityCode", result.columns)
        self.assertIn("Value", result.columns)

    def test_preprocess_does_not_modify_original(self):
        """測試預處理不修改原始 DataFrame。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
            "Value": [100.0],
        })

        self.uploader.preprocess(df.copy())

        self.assertIn("StockName", df.columns)


if __name__ == "__main__":
    unittest.main()

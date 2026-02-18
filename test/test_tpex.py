"""TPEX 資料上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock

import pandas as pd
from pydantic import ValidationError

from data_upload.tpex import Uploader, UploadType


class TestUploadType(unittest.TestCase):
    """測試 TPEX UploadType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = UploadType(
            Date="2026-01-02",
            Code="6547",
            Close=100.0,
            Change=2.0,
            Open=98.0,
            High=101.0,
            Low=97.0,
            TradeVolume=5000.0,
            TradeAmount=500000.0,
            NumberOfTransactions=100,
            LastBestBidPrice=99.5,
            LastBidVolume=50.0,
            LastBestAskPrice=100.5,
            LastBestAskVolume=60.0,
            IssuedShares=1000000,
            NextDayUpLimitPrice=110.0,
            NextDayDownLimitPrice=90.0,
        )

        self.assertEqual(data.Code, "6547")
        self.assertAlmostEqual(data.Close, 100.0)

    def test_invalid_missing_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            UploadType(
                Date="2026-01-02",
                Code="6547",
            )


class TestUploader(unittest.TestCase):
    """測試 TPEX Uploader。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = Uploader(self.mock_conn, "localhost:6738")

    def test_init_attributes(self):
        """測試初始化屬性設定。"""
        self.assertEqual(self.uploader.url, "http://localhost:6738")
        self.assertEqual(self.uploader.stock_code_col, "Code")
        self.assertEqual(self.uploader.stock_name_col, "Name")
        self.assertEqual(self.uploader.name, "tpex")

    def test_preprocess_drops_name(self):
        """測試預處理移除 Name 欄位。"""
        df = pd.DataFrame({
            "Code": ["6547"],
            "Name": ["高端疫苗"],
            "Close": [100.0],
        })

        result = self.uploader.preprocess(df)

        self.assertNotIn("Name", result.columns)
        self.assertIn("Code", result.columns)
        self.assertIn("Close", result.columns)


if __name__ == "__main__":
    unittest.main()

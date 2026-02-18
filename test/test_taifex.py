"""TAIFEX 資料上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock

import pandas as pd
from pydantic import ValidationError

from data_upload.taifex import Uploader, UploadType


class TestUploadType(unittest.TestCase):
    """測試 TAIFEX UploadType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = UploadType(
            Date="2026-01-02",
            Contract="TX",
            ContractMonth="202601",
            Open=18000.0,
            High=18100.0,
            Low=17900.0,
            Last=18050.0,
            Change=50.0,
            ChangePercent=0.28,
            Volume=100000,
            SettlementPrice=18050.0,
            OpenInterest=50000.0,
            BestBid=18045.0,
            BestAsk=18055.0,
            HistoricalHigh=19000.0,
            HistoricalLow=15000.0,
            TradingHalt=None,
            TradingSession="Regular",
            SpreadOrderVolume=1000.0,
        )

        self.assertEqual(data.Contract, "TX")
        self.assertAlmostEqual(data.Last, 18050.0)

    def test_optional_fields_accept_none(self):
        """測試 Optional 欄位接受 None。"""
        data = UploadType(
            Date="2026-01-02",
            Contract="TX",
            ContractMonth="202601",
            Open=None,
            High=None,
            Low=None,
            Last=None,
            Change=None,
            ChangePercent=None,
            Volume=100,
            SettlementPrice=None,
            OpenInterest=None,
            BestBid=None,
            BestAsk=None,
            HistoricalHigh=None,
            HistoricalLow=None,
            TradingHalt=None,
            TradingSession="Regular",
            SpreadOrderVolume=None,
        )

        self.assertIsNone(data.Open)
        self.assertIsNone(data.TradingHalt)

    def test_invalid_missing_required(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            UploadType(
                Date="2026-01-02",
                Contract="TX",
            )


class TestUploader(unittest.TestCase):
    """測試 TAIFEX Uploader。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = Uploader(self.mock_conn, "localhost:6738")

    def test_init_attributes(self):
        """測試初始化屬性設定。"""
        self.assertEqual(self.uploader.url, "http://localhost:6738")
        self.assertIsNone(self.uploader.stock_code_col)
        self.assertIsNone(self.uploader.stock_name_col)
        self.assertEqual(self.uploader.name, "taifex")

    def test_preprocess_returns_unchanged(self):
        """測試預處理回傳原始 DataFrame。"""
        df = pd.DataFrame({
            "Contract": ["TX"],
            "Volume": [100000],
        })

        result = self.uploader.preprocess(df)

        pd.testing.assert_frame_equal(result, df)


if __name__ == "__main__":
    unittest.main()

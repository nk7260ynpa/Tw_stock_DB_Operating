"""TDCC 集保庫存模組單元測試。"""

import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError

from data_upload.tdcc import (
    COLUMN_MAPPING,
    TDCCType,
    TDCCUploader,
)


class TestTDCCType(unittest.TestCase):
    """測試 TDCCType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = TDCCType(
            Date="2024-01-05",
            SecurityCode="2330",
            Level="1-999",
            Holders=150000,
            HoldingShares=5000000,
            HoldingRatio=Decimal("3.14"),
        )

        self.assertEqual(data.Date, "2024-01-05")
        self.assertEqual(data.SecurityCode, "2330")
        self.assertEqual(data.Level, "1-999")
        self.assertEqual(data.Holders, 150000)
        self.assertEqual(data.HoldingShares, 5000000)
        self.assertEqual(data.HoldingRatio, Decimal("3.14"))

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            TDCCType(
                Date="2024-01-05",
                SecurityCode="2330",
            )

    def test_holding_ratio_decimal(self):
        """測試 HoldingRatio 使用 Decimal 型別。"""
        data = TDCCType(
            Date="2024-01-05",
            SecurityCode="2330",
            Level="1-999",
            Holders=100,
            HoldingShares=500,
            HoldingRatio=Decimal("0.01"),
        )

        self.assertIsInstance(data.HoldingRatio, Decimal)


class TestCheckUploaded(unittest.TestCase):
    """測試 check_uploaded 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = TDCCUploader(self.mock_conn, "localhost:6738")

    def test_uploaded_exists(self):
        """測試已上傳時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1

        result = self.uploader.check_uploaded("2024-01-05")

        self.assertTrue(result)

    def test_not_exists(self):
        """測試未上傳時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        result = self.uploader.check_uploaded("2024-01-05")

        self.assertFalse(result)


class TestGetLatestUploadedDate(unittest.TestCase):
    """測試 get_latest_uploaded_date 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = TDCCUploader(self.mock_conn, "localhost:6738")

    def test_has_data(self):
        """測試有資料時回傳日期字串。"""
        self.mock_conn.execute.return_value.scalar.return_value = "2024-01-05"

        result = self.uploader.get_latest_uploaded_date()

        self.assertEqual(result, "2024-01-05")

    def test_no_data(self):
        """測試無資料時回傳 None。"""
        self.mock_conn.execute.return_value.scalar.return_value = None

        result = self.uploader.get_latest_uploaded_date()

        self.assertIsNone(result)


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = TDCCUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.tdcc.requests.get")
    def test_success_nested_format(self, mock_get):
        """測試成功取得巢狀格式資料（實際 API 格式）。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2024-01-05",
            "data": [
                {
                    "Date": "2024-01-05T00:00:00",
                    "SecurityCode": "2330",
                    "HoldingLevel": 1,
                    "Holders": 150000,
                    "Shares": 5000000,
                    "Percentage": 3.14,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        date, df = self.uploader.crawl_data()

        self.assertEqual(date, "2024-01-05")
        self.assertEqual(len(df), 1)
        # 驗證 Date 截取日期部分
        self.assertEqual(df["Date"].iloc[0], "2024-01-05")
        # 驗證 HoldingLevel 轉為字串
        self.assertEqual(df["HoldingLevel"].iloc[0], "1")

    @patch("data_upload.tdcc.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳空資料。"""
        mock_get.side_effect = Exception("連線失敗")

        date, df = self.uploader.crawl_data()

        self.assertIsNone(date)
        self.assertTrue(df.empty)

    @patch("data_upload.tdcc.requests.get")
    def test_empty_response(self, mock_get):
        """測試空回應回傳空資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"date": "2024-01-05", "data": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        date, df = self.uploader.crawl_data()

        self.assertIsNone(date)
        self.assertTrue(df.empty)

    @patch("data_upload.tdcc.requests.get")
    def test_empty_dict_response(self, mock_get):
        """測試完全空回應回傳空資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        date, df = self.uploader.crawl_data()

        self.assertIsNone(date)
        self.assertTrue(df.empty)


class TestRenameColumns(unittest.TestCase):
    """測試 _rename_columns 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = TDCCUploader(self.mock_conn, "localhost:6738")

    def test_rename(self):
        """測試欄位重新命名。"""
        df = pd.DataFrame({
            "Date": ["2024-01-05"],
            "SecurityCode": ["2330"],
            "HoldingLevel": ["1-999"],
            "Holders": [150000],
            "Shares": [5000000],
            "Percentage": [3.14],
        })

        result = self.uploader._rename_columns(df)

        self.assertIn("Level", result.columns)
        self.assertIn("HoldingShares", result.columns)
        self.assertIn("HoldingRatio", result.columns)
        self.assertNotIn("HoldingLevel", result.columns)
        self.assertNotIn("Shares", result.columns)
        self.assertNotIn("Percentage", result.columns)

    def test_column_mapping_values(self):
        """測試 COLUMN_MAPPING 定義正確。"""
        self.assertEqual(COLUMN_MAPPING["HoldingLevel"], "Level")
        self.assertEqual(COLUMN_MAPPING["Shares"], "HoldingShares")
        self.assertEqual(COLUMN_MAPPING["Percentage"], "HoldingRatio")


class TestCheckSchema(unittest.TestCase):
    """測試 check_schema 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = TDCCUploader(self.mock_conn, "localhost:6738")

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "Date": ["2024-01-05"],
            "SecurityCode": ["2330"],
            "Level": ["1-999"],
            "Holders": [150000],
            "HoldingShares": [5000000],
            "HoldingRatio": [Decimal("3.14")],
        })

        result = self.uploader.check_schema(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["SecurityCode"].iloc[0], "2330")

    def test_invalid_schema(self):
        """測試不合法資料拋出例外。"""
        df = pd.DataFrame({
            "Date": ["2024-01-05"],
            "SecurityCode": ["2330"],
        })

        with self.assertRaises(Exception):
            self.uploader.check_schema(df)


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = TDCCUploader(self.mock_conn, "localhost:6738")

    def test_empty_data(self):
        """測試無資料時回傳 record_count=0。"""
        self.uploader.crawl_data = MagicMock(
            return_value=(None, pd.DataFrame())
        )

        result = self.uploader.upload()

        self.assertEqual(result["record_count"], 0)
        self.assertIsNone(result["date"])

    def test_already_uploaded(self):
        """測試已上傳時跳過並回傳 record_count=0。"""
        df = pd.DataFrame({
            "Date": ["2024-01-05"],
            "SecurityCode": ["2330"],
            "HoldingLevel": ["1-999"],
            "Holders": [150000],
            "Shares": [5000000],
            "Percentage": [3.14],
        })
        self.uploader.crawl_data = MagicMock(
            return_value=("2024-01-05", df)
        )
        self.uploader.check_uploaded = MagicMock(return_value=True)

        result = self.uploader.upload()

        self.assertEqual(result["date"], "2024-01-05")
        self.assertEqual(result["record_count"], 0)

    def test_successful_upload(self):
        """測試成功上傳回傳正確筆數。"""
        df = pd.DataFrame({
            "Date": ["2024-01-05"],
            "SecurityCode": ["2330"],
            "HoldingLevel": ["1-999"],
            "Holders": [150000],
            "Shares": [5000000],
            "Percentage": [3.14],
        })
        self.uploader.crawl_data = MagicMock(
            return_value=("2024-01-05", df)
        )
        self.uploader.check_uploaded = MagicMock(return_value=False)

        result = self.uploader.upload()

        self.assertEqual(result["date"], "2024-01-05")
        self.assertEqual(result["record_count"], 1)
        self.mock_conn.commit.assert_called()


if __name__ == "__main__":
    unittest.main()

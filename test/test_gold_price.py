"""黃金價格上傳模組單元測試。"""

import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError

from data_upload.gold_price import (
    GoldPriceType,
    GoldPriceUploader,
)
from data_upload.base import CrawlError, NetworkError


class TestGoldPriceType(unittest.TestCase):
    """測試 GoldPriceType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = GoldPriceType(
            Date="2026-03-19",
            Product="GC=F",
            Open=Decimal("2180.50"),
            High=Decimal("2195.20"),
            Low=Decimal("2175.10"),
            Close=Decimal("2190.85"),
            Volume=150000,
        )

        self.assertEqual(data.Date, "2026-03-19")
        self.assertEqual(data.Product, "GC=F")
        self.assertEqual(data.Open, Decimal("2180.50"))
        self.assertEqual(data.Volume, 150000)

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            GoldPriceType(
                Date="2026-03-19",
                Product="GC=F",
            )

    def test_decimal_fields(self):
        """測試價格欄位使用 Decimal 型別。"""
        data = GoldPriceType(
            Date="2026-03-19",
            Product="GC=F",
            Open=Decimal("2180.50"),
            High=Decimal("2195.20"),
            Low=Decimal("2175.10"),
            Close=Decimal("2190.85"),
            Volume=150000,
        )

        self.assertIsInstance(data.Open, Decimal)
        self.assertIsInstance(data.High, Decimal)
        self.assertIsInstance(data.Low, Decimal)
        self.assertIsInstance(data.Close, Decimal)


class TestCheckUploaded(unittest.TestCase):
    """測試 check_uploaded 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = GoldPriceUploader(self.mock_conn, "localhost:6738")

    def test_uploaded_exists(self):
        """測試已上傳時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1

        result = self.uploader.check_uploaded("2026-03-19")

        self.assertTrue(result)

    def test_not_exists(self):
        """測試未上傳時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        result = self.uploader.check_uploaded("2026-03-19")

        self.assertFalse(result)


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = GoldPriceUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.gold_price.requests.get")
    def test_success(self, mock_get):
        """測試成功取得資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "data": [
                {
                    "product": "GC=F",
                    "date": "2026-03-19",
                    "open": 2180.50,
                    "high": 2195.20,
                    "low": 2175.10,
                    "close": 2190.85,
                    "volume": 150000,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-19")

        self.assertEqual(len(df), 1)
        self.assertIn("Product", df.columns)
        self.assertIn("Open", df.columns)
        self.assertEqual(df["Product"].iloc[0], "GC=F")

    @patch("data_upload.gold_price.requests.get")
    def test_empty_data(self, mock_get):
        """測試無資料時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "data": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-19")

        self.assertTrue(df.empty)

    @patch("data_upload.gold_price.requests.get")
    def test_none_data(self, mock_get):
        """測試 data 為 None 時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "data": None,
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-19")

        self.assertTrue(df.empty)

    @patch("data_upload.gold_price.requests.get")
    def test_connection_error(self, mock_get):
        """測試連線失敗拋出 NetworkError。"""
        import requests as req_lib
        mock_get.side_effect = req_lib.ConnectionError("連線失敗")

        with self.assertRaises(NetworkError):
            self.uploader.crawl_data("2026-03-19")

    @patch("data_upload.gold_price.requests.get")
    def test_timeout_error(self, mock_get):
        """測試超時拋出 NetworkError。"""
        import requests as req_lib
        mock_get.side_effect = req_lib.Timeout("超時")

        with self.assertRaises(NetworkError):
            self.uploader.crawl_data("2026-03-19")

    @patch("data_upload.gold_price.requests.get")
    def test_http_error(self, mock_get):
        """測試 HTTP 錯誤拋出 CrawlError。"""
        import requests as req_lib
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req_lib.HTTPError("500")
        mock_get.return_value = mock_resp

        with self.assertRaises(CrawlError):
            self.uploader.crawl_data("2026-03-19")

    @patch("data_upload.gold_price.requests.get")
    def test_crawler_no_data_treated_as_non_trading_day(self, mock_get):
        """爬蟲回傳「無法取得任何...資料」視為非交易日，回空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "error": "無法取得任何黃金價格資料（查詢日期：2026-03-19）",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-19")
        self.assertTrue(df.empty)

    @patch("data_upload.gold_price.requests.get")
    def test_crawler_other_error_raises(self, mock_get):
        """其他 error 訊息（非「無法取得任何」）仍拋出 CrawlError。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "error": "Yahoo Finance API 連線逾時",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(CrawlError):
            self.uploader.crawl_data("2026-03-19")

    @patch("data_upload.gold_price.requests.get")
    def test_missing_columns(self, mock_get):
        """測試缺少必要欄位拋出 CrawlError。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "data": [
                {
                    "product": "GC=F",
                    "date": "2026-03-19",
                    "open": 2180.50,
                    # 缺少其他欄位
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(CrawlError):
            self.uploader.crawl_data("2026-03-19")


class TestCheckSchema(unittest.TestCase):
    """測試 check_schema 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = GoldPriceUploader(self.mock_conn, "localhost:6738")

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "Date": ["2026-03-19"],
            "Product": ["GC=F"],
            "Open": [Decimal("2180.50")],
            "High": [Decimal("2195.20")],
            "Low": [Decimal("2175.10")],
            "Close": [Decimal("2190.85")],
            "Volume": [150000],
        })

        result = self.uploader.check_schema(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["Product"].iloc[0], "GC=F")

    def test_invalid_schema(self):
        """測試不合法資料拋出例外。"""
        df = pd.DataFrame({
            "Date": ["2026-03-19"],
            "Product": ["GC=F"],
        })

        with self.assertRaises(Exception):
            self.uploader.check_schema(df)


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = GoldPriceUploader(self.mock_conn, "localhost:6738")

    def test_already_uploaded(self):
        """測試已上傳時跳過並回傳 record_count=0。"""
        self.uploader.check_uploaded = MagicMock(return_value=True)

        result = self.uploader.upload("2026-03-19")

        self.assertEqual(result["date"], "2026-03-19")
        self.assertEqual(result["record_count"], 0)

    @patch("data_upload.gold_price.requests.get")
    def test_empty_data_records_date(self, mock_get):
        """測試無資料時仍記錄已處理日期。"""
        self.uploader.check_uploaded = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "data": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.upload("2026-03-19")

        self.assertEqual(result["date"], "2026-03-19")
        self.assertEqual(result["record_count"], 0)
        # 應該有寫入 GoldPriceUploaded
        self.mock_conn.execute.assert_called()
        self.mock_conn.commit.assert_called()

    @patch("data_upload.gold_price.requests.get")
    def test_successful_upload(self, mock_get):
        """測試成功上傳回傳正確筆數。"""
        self.uploader.check_uploaded = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-19",
            "data": [
                {
                    "product": "GC=F",
                    "date": "2026-03-19",
                    "open": 2180.50,
                    "high": 2195.20,
                    "low": 2175.10,
                    "close": 2190.85,
                    "volume": 150000,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.upload("2026-03-19")

        self.assertEqual(result["date"], "2026-03-19")
        self.assertEqual(result["record_count"], 1)
        self.mock_conn.commit.assert_called()

    @patch("data_upload.gold_price.requests.get")
    def test_network_error_propagates(self, mock_get):
        """測試網路錯誤向上傳播。"""
        import requests as req_lib
        self.uploader.check_uploaded = MagicMock(return_value=False)
        mock_get.side_effect = req_lib.ConnectionError("連線失敗")

        with self.assertRaises(NetworkError):
            self.uploader.upload("2026-03-19")


if __name__ == "__main__":
    unittest.main()

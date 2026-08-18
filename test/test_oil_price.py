"""原油價格上傳模組單元測試。"""

import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError

from data_upload.oil_price import (
    OilPriceType,
    OilPriceUploader,
)
from data_upload.base import (
    CrawlError,
    NetworkError,
    OutOfRangeError,
    SourceError,
)


def _recorded_uploaded_dates(mock_conn):
    """從 mock 連線的 execute 呼叫中收集寫入帳本表的日期。

    Args:
        mock_conn: 被 mock 的 SQLAlchemy 連線物件。

    Returns:
        list[str]: 被寫入帳本的日期字串清單。
    """
    dates = []
    for call in mock_conn.execute.call_args_list:
        if not call.args:
            continue
        if "INSERT IGNORE INTO" not in str(call.args[0]):
            continue
        if len(call.args) >= 2 and isinstance(call.args[1], dict):
            date = call.args[1].get("date")
            if date is not None:
                dates.append(date)
    return dates


class TestOilPriceType(unittest.TestCase):
    """測試 OilPriceType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = OilPriceType(
            Date="2026-03-18",
            Product="WTI",
            Open=Decimal("68.50"),
            High=Decimal("69.20"),
            Low=Decimal("68.10"),
            Close=Decimal("68.85"),
            Volume=250000,
        )

        self.assertEqual(data.Date, "2026-03-18")
        self.assertEqual(data.Product, "WTI")
        self.assertEqual(data.Open, Decimal("68.50"))
        self.assertEqual(data.Volume, 250000)

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            OilPriceType(
                Date="2026-03-18",
                Product="WTI",
            )

    def test_decimal_fields(self):
        """測試價格欄位使用 Decimal 型別。"""
        data = OilPriceType(
            Date="2026-03-18",
            Product="Brent",
            Open=Decimal("72.30"),
            High=Decimal("73.00"),
            Low=Decimal("72.00"),
            Close=Decimal("72.65"),
            Volume=180000,
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
        self.uploader = OilPriceUploader(self.mock_conn, "localhost:6738")

    def test_uploaded_exists(self):
        """測試已上傳時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1

        result = self.uploader.check_uploaded("2026-03-18")

        self.assertTrue(result)

    def test_not_exists(self):
        """測試未上傳時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        result = self.uploader.check_uploaded("2026-03-18")

        self.assertFalse(result)


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = OilPriceUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.oil_price.requests.get")
    def test_success(self, mock_get):
        """測試成功取得資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": [
                {
                    "product": "WTI",
                    "date": "2026-03-18",
                    "open": 68.50,
                    "high": 69.20,
                    "low": 68.10,
                    "close": 68.85,
                    "volume": 250000,
                },
                {
                    "product": "Brent",
                    "date": "2026-03-18",
                    "open": 72.30,
                    "high": 73.00,
                    "low": 72.00,
                    "close": 72.65,
                    "volume": 180000,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-18")

        self.assertEqual(len(df), 2)
        self.assertIn("Product", df.columns)
        self.assertIn("Open", df.columns)
        self.assertEqual(df["Product"].iloc[0], "WTI")
        self.assertEqual(df["Product"].iloc[1], "Brent")

    @patch("data_upload.oil_price.requests.get")
    def test_empty_data(self, mock_get):
        """測試無資料時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-18")

        self.assertTrue(df.empty)

    @patch("data_upload.oil_price.requests.get")
    def test_none_data(self, mock_get):
        """測試 data 為 None 時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": None,
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-18")

        self.assertTrue(df.empty)

    @patch("data_upload.oil_price.requests.get")
    def test_connection_error(self, mock_get):
        """測試連線失敗拋出 NetworkError。"""
        import requests as req_lib
        mock_get.side_effect = req_lib.ConnectionError("連線失敗")

        with self.assertRaises(NetworkError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_timeout_error(self, mock_get):
        """測試超時拋出 NetworkError。"""
        import requests as req_lib
        mock_get.side_effect = req_lib.Timeout("超時")

        with self.assertRaises(NetworkError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_http_error(self, mock_get):
        """測試 HTTP 錯誤拋出 CrawlError。"""
        import requests as req_lib
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req_lib.HTTPError("500")
        mock_get.return_value = mock_resp

        with self.assertRaises(CrawlError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_status_empty_returns_empty_dataframe(self, mock_get):
        """status=empty（探測確認無報價）回空 DataFrame，交由記帳判定。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "status": "empty",
            "data": [],
            "meta": {"retryable": False},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-03-18")
        self.assertTrue(df.empty)
        self.assertEqual(self.uploader.last_crawl_status, "empty")

    @patch("data_upload.oil_price.requests.get")
    def test_status_error_raises_source_error(self, mock_get):
        """status=error 一律視為可重試失敗，絕不可當成當日無資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "status": "error",
            "data": [],
            "error": "來源探測失敗",
            "meta": {"retryable": True},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(SourceError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_status_partial_raises_source_error(self, mock_get):
        """行情類 partial 整批丟棄重抓（不接受只存一半的一天）。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "status": "partial",
            "data": [{"date": "2026-03-18", "product": "X"}],
            "meta": {"retryable": True, "retryable_reasons": ["fetch_failed"]},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(SourceError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_null_ohlc_raises_source_error(self, mock_get):
        """status=ok 但 OHLC 全為 null：殘缺資料視為抓取失敗、不得記帳。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "status": "ok",
            "data": [{
                "date": "2026-03-18",
                "product": "X",
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": 436708050,
            }],
            "meta": {"target_date_available": True},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(SourceError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_status_out_of_range_raises(self, mock_get):
        """status=out_of_range 拋 OutOfRangeError（不重試，由呼叫端記帳）。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "1990-01-02",
            "status": "out_of_range",
            "data": [],
            "meta": {"retryable": False, "oldest_available": "2000-01-03"},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(OutOfRangeError):
            self.uploader.crawl_data("1990-01-02")

    @patch("data_upload.oil_price.requests.get")
    def test_status_unknown_raises_source_error(self, mock_get):
        """未知狀態保守視為可重試失敗，不得寫入帳本。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "status": "who_knows",
            "data": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(SourceError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_legacy_error_without_status_raises(self, mock_get):
        """舊版格式（無 status 只有 error）一律視為失敗，不再靠訊息字串判非交易日。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "error": "無法取得任何原油價格資料（查詢日期：2026-03-18）",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(CrawlError):
            self.uploader.crawl_data("2026-03-18")

    @patch("data_upload.oil_price.requests.get")
    def test_missing_columns(self, mock_get):
        """測試缺少必要欄位拋出 CrawlError。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": [
                {
                    "product": "WTI",
                    "date": "2026-03-18",
                    "open": 68.50,
                    # 缺少其他欄位
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with self.assertRaises(CrawlError):
            self.uploader.crawl_data("2026-03-18")


class TestCheckSchema(unittest.TestCase):
    """測試 check_schema 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = OilPriceUploader(self.mock_conn, "localhost:6738")

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "Date": ["2026-03-18"],
            "Product": ["WTI"],
            "Open": [Decimal("68.50")],
            "High": [Decimal("69.20")],
            "Low": [Decimal("68.10")],
            "Close": [Decimal("68.85")],
            "Volume": [250000],
        })

        result = self.uploader.check_schema(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["Product"].iloc[0], "WTI")

    def test_invalid_schema(self):
        """測試不合法資料拋出例外。"""
        df = pd.DataFrame({
            "Date": ["2026-03-18"],
            "Product": ["WTI"],
        })

        with self.assertRaises(Exception):
            self.uploader.check_schema(df)


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = OilPriceUploader(self.mock_conn, "localhost:6738")

    def test_already_uploaded(self):
        """測試已上傳時跳過並回傳 record_count=0。"""
        self.uploader.check_uploaded = MagicMock(return_value=True)

        result = self.uploader.upload("2026-03-18")

        self.assertEqual(result["date"], "2026-03-18")
        self.assertEqual(result["record_count"], 0)

    @patch("data_upload.oil_price.requests.get")
    def test_empty_data_records_date(self, mock_get):
        """測試無資料時仍記錄已處理日期。"""
        self.uploader.check_uploaded = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.upload("2026-03-18")

        self.assertEqual(result["date"], "2026-03-18")
        self.assertEqual(result["record_count"], 0)
        # 應該有寫入 OilPriceUploaded
        self.mock_conn.execute.assert_called()
        self.mock_conn.commit.assert_called()

    @patch("data_upload.oil_price.requests.get")
    def test_successful_upload(self, mock_get):
        """測試成功上傳回傳正確筆數。"""
        self.uploader.check_uploaded = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": [
                {
                    "product": "WTI",
                    "date": "2026-03-18",
                    "open": 68.50,
                    "high": 69.20,
                    "low": 68.10,
                    "close": 68.85,
                    "volume": 250000,
                },
                {
                    "product": "Brent",
                    "date": "2026-03-18",
                    "open": 72.30,
                    "high": 73.00,
                    "low": 72.00,
                    "close": 72.65,
                    "volume": 180000,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.upload("2026-03-18")

        self.assertEqual(result["date"], "2026-03-18")
        self.assertEqual(result["record_count"], 2)
        self.mock_conn.commit.assert_called()

    @patch("data_upload.oil_price.requests.get")
    def test_successful_upload_records_actual_date(self, mock_get):
        """測試實際==請求日時，帳本記錄實際交易日。"""
        self.uploader.check_uploaded = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-18",
            "data": [
                {
                    "product": "WTI", "date": "2026-03-18",
                    "open": 68.50, "high": 69.20, "low": 68.10,
                    "close": 68.85, "volume": 250000,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        self.uploader.upload("2026-03-18")

        recorded = _recorded_uploaded_dates(self.mock_conn)
        self.assertIn("2026-03-18", recorded)

    @patch("data_upload.oil_price.requests.get")
    def test_fallback_records_both_actual_and_request_date(self, mock_get):
        """測試非 24/7 商品 fallback 時，實際日與請求日皆記帳。

        請求 2026-03-16（週日），爬蟲 fallback 回 2026-03-13（週五）：
        帳本應同時寫入實際日 2026-03-13（真資料）與請求日 2026-03-16
        （確定為非交易日，記帳避免反覆檢查）。
        """
        self.uploader.check_uploaded = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-03-16",
            "data": [
                {
                    "product": "WTI", "date": "2026-03-13",
                    "open": 68.50, "high": 69.20, "low": 68.10,
                    "close": 68.85, "volume": 250000,
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.upload("2026-03-16")

        self.assertEqual(result["record_count"], 1)
        recorded = _recorded_uploaded_dates(self.mock_conn)
        self.assertIn("2026-03-13", recorded)
        self.assertIn("2026-03-16", recorded)

    @patch("data_upload.oil_price.requests.get")
    def test_network_error_propagates(self, mock_get):
        """測試網路錯誤向上傳播。"""
        import requests as req_lib
        self.uploader.check_uploaded = MagicMock(return_value=False)
        mock_get.side_effect = req_lib.ConnectionError("連線失敗")

        with self.assertRaises(NetworkError):
            self.uploader.upload("2026-03-18")


if __name__ == "__main__":
    unittest.main()

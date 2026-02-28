"""CTEE 新聞上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock, patch, call

import pandas as pd
from pydantic import ValidationError

from data_upload.ctee_news import (
    CTEENewsType,
    CTEENewsUploader,
)


class TestCTEENewsType(unittest.TestCase):
    """測試 CTEENewsType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = CTEENewsType(
            Date="2026-02-27",
            Time="14:30:00",
            Author="記者A",
            Head="測試標題",
            SubHead="測試副標",
            HashTag="科技,半導體",
            url="https://www.ctee.com.tw/news/123",
        )

        self.assertEqual(data.Date, "2026-02-27")
        self.assertEqual(data.Head, "測試標題")
        self.assertEqual(data.url, "https://www.ctee.com.tw/news/123")

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            CTEENewsType(
                Date="2026-02-27",
                Time="14:30:00",
            )

    def test_empty_strings_allowed(self):
        """測試允許空字串。"""
        data = CTEENewsType(
            Date="2026-02-27",
            Time="",
            Author="",
            Head="",
            SubHead="",
            HashTag="",
            url="https://www.ctee.com.tw/news/123",
        )

        self.assertEqual(data.Author, "")


class TestUrlHash(unittest.TestCase):
    """測試 url_hash 靜態方法。"""

    def test_hash_length(self):
        """測試雜湊值長度為 12。"""
        result = CTEENewsUploader.url_hash("https://example.com/news/1")

        self.assertEqual(len(result), 12)

    def test_deterministic(self):
        """測試相同 URL 產生相同雜湊值。"""
        url = "https://www.ctee.com.tw/news/123"

        hash1 = CTEENewsUploader.url_hash(url)
        hash2 = CTEENewsUploader.url_hash(url)

        self.assertEqual(hash1, hash2)

    def test_different_urls_different_hashes(self):
        """測試不同 URL 產生不同雜湊值。"""
        hash1 = CTEENewsUploader.url_hash("https://example.com/1")
        hash2 = CTEENewsUploader.url_hash("https://example.com/2")

        self.assertNotEqual(hash1, hash2)


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.ctee_news.requests.get")
    def test_success(self, mock_get):
        """測試成功取得新聞資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-02-27",
            "data": [
                {
                    "Date": "2026-02-27",
                    "Time": "14:30:00",
                    "Author": "記者A",
                    "Head": "標題",
                    "SubHead": "副標",
                    "HashTag": "科技",
                    "url": "https://www.ctee.com.tw/news/1",
                    "Content": "全文內容",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-02-27")

        self.assertEqual(len(df), 1)
        self.assertEqual(df["Head"].iloc[0], "標題")

    @patch("data_upload.ctee_news.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳空 DataFrame。"""
        mock_get.side_effect = Exception("連線失敗")

        df = self.uploader.crawl_data("2026-02-27")

        self.assertTrue(df.empty)

    @patch("data_upload.ctee_news.requests.get")
    def test_empty_data(self, mock_get):
        """測試無資料時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"date": "2026-02-27", "data": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-02-27")

        self.assertTrue(df.empty)


class TestGetExistingUrls(unittest.TestCase):
    """測試 get_existing_urls 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    def test_has_existing_urls(self):
        """測試有已存在 URL 時回傳正確集合。"""
        self.mock_conn.execute.return_value.fetchall.return_value = [
            ("https://www.ctee.com.tw/news/1",),
            ("https://www.ctee.com.tw/news/2",),
        ]

        result = self.uploader.get_existing_urls("2026-02-27")

        self.assertEqual(len(result), 2)
        self.assertIn("https://www.ctee.com.tw/news/1", result)

    def test_no_existing_urls(self):
        """測試無已存在 URL 時回傳空集合。"""
        self.mock_conn.execute.return_value.fetchall.return_value = []

        result = self.uploader.get_existing_urls("2026-02-27")

        self.assertEqual(len(result), 0)


class TestFilterNewRecords(unittest.TestCase):
    """測試 filter_new_records 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    def test_all_new(self):
        """測試所有記錄都是新的。"""
        df = pd.DataFrame({
            "url": ["https://a.com/1", "https://a.com/2"],
            "Head": ["標題1", "標題2"],
        })
        self.uploader.get_existing_urls = MagicMock(return_value=set())

        result = self.uploader.filter_new_records(df, "2026-02-27")

        self.assertEqual(len(result), 2)

    def test_some_existing(self):
        """測試部分記錄已存在。"""
        df = pd.DataFrame({
            "url": ["https://a.com/1", "https://a.com/2"],
            "Head": ["標題1", "標題2"],
        })
        self.uploader.get_existing_urls = MagicMock(
            return_value={"https://a.com/1"}
        )

        result = self.uploader.filter_new_records(df, "2026-02-27")

        self.assertEqual(len(result), 1)
        self.assertEqual(result["url"].iloc[0], "https://a.com/2")

    def test_all_existing(self):
        """測試所有記錄都已存在。"""
        df = pd.DataFrame({
            "url": ["https://a.com/1"],
            "Head": ["標題1"],
        })
        self.uploader.get_existing_urls = MagicMock(
            return_value={"https://a.com/1"}
        )

        result = self.uploader.filter_new_records(df, "2026-02-27")

        self.assertTrue(result.empty)

    def test_empty_dataframe(self):
        """測試空 DataFrame。"""
        df = pd.DataFrame()

        result = self.uploader.filter_new_records(df, "2026-02-27")

        self.assertTrue(result.empty)


class TestCheckSchema(unittest.TestCase):
    """測試 check_schema 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "Date": ["2026-02-27"],
            "Time": ["14:30:00"],
            "Author": ["記者A"],
            "Head": ["標題"],
            "SubHead": ["副標"],
            "HashTag": ["科技"],
            "url": ["https://a.com/1"],
            "Content": ["全文"],
        })

        result = self.uploader.check_schema(df)

        self.assertEqual(len(result), 1)
        # Content 不應出現在驗證後的 DataFrame
        self.assertNotIn("Content", result.columns)
        self.assertIn("url", result.columns)

    def test_missing_field_raises_error(self):
        """測試缺少欄位時拋出例外。"""
        df = pd.DataFrame({
            "Date": ["2026-02-27"],
            "Time": ["14:30:00"],
        })

        with self.assertRaises(Exception):
            self.uploader.check_schema(df)


class TestSaveContents(unittest.TestCase):
    """測試 save_contents 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.ctee_news.NEWS_CONTENT_BASE")
    def test_save_files(self, mock_base):
        """測試成功儲存全文檔案。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)
        mock_dir.mkdir = MagicMock()

        df = pd.DataFrame({
            "url": ["https://a.com/1", "https://a.com/2"],
            "Content": ["全文1", "全文2"],
        })

        saved = self.uploader.save_contents(df, "2026-02-27")

        self.assertEqual(saved, 2)
        self.assertEqual(mock_file.write_text.call_count, 2)

    @patch("data_upload.ctee_news.NEWS_CONTENT_BASE")
    def test_empty_url_skipped(self, mock_base):
        """測試空 URL 被跳過。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)
        mock_dir.mkdir = MagicMock()

        df = pd.DataFrame({
            "url": ["", "https://a.com/1"],
            "Content": ["全文1", "全文2"],
        })

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)

        saved = self.uploader.save_contents(df, "2026-02-27")

        self.assertEqual(saved, 1)


class TestUploadMetadata(unittest.TestCase):
    """測試 upload_metadata 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    def test_upload_records(self):
        """測試成功上傳 metadata。"""
        df = pd.DataFrame({
            "Date": ["2026-02-27"],
            "Time": ["14:30:00"],
            "Author": ["記者A"],
            "Head": ["標題"],
            "SubHead": ["副標"],
            "HashTag": ["科技"],
            "url": ["https://a.com/1"],
        })

        count = self.uploader.upload_metadata(df)

        self.assertEqual(count, 1)
        self.assertTrue(self.mock_conn.commit.called)

    def test_empty_dataframe_returns_zero(self):
        """測試空 DataFrame 回傳 0。"""
        df = pd.DataFrame()

        count = self.uploader.upload_metadata(df)

        self.assertEqual(count, 0)
        self.mock_conn.commit.assert_not_called()


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    def test_no_data(self):
        """測試無資料時回傳 record_count=0。"""
        self.uploader.crawl_data = MagicMock(return_value=pd.DataFrame())

        result = self.uploader.upload("2026-02-27")

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)
        self.assertEqual(result["date"], "2026-02-27")

    def test_all_existing(self):
        """測試所有記錄都已存在時跳過上傳。"""
        df = pd.DataFrame({
            "Date": ["2026-02-27"],
            "Time": ["14:30:00"],
            "Author": ["記者A"],
            "Head": ["標題"],
            "SubHead": ["副標"],
            "HashTag": ["科技"],
            "url": ["https://a.com/1"],
            "Content": ["全文"],
        })
        self.uploader.crawl_data = MagicMock(return_value=df)
        self.uploader.filter_new_records = MagicMock(
            return_value=pd.DataFrame()
        )

        result = self.uploader.upload("2026-02-27")

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)

    def test_successful_upload(self):
        """測試成功上傳回傳正確數量。"""
        raw_df = pd.DataFrame({
            "Date": ["2026-02-27"],
            "Time": ["14:30:00"],
            "Author": ["記者A"],
            "Head": ["標題"],
            "SubHead": ["副標"],
            "HashTag": ["科技"],
            "url": ["https://a.com/1"],
            "Content": ["全文"],
        })
        self.uploader.crawl_data = MagicMock(return_value=raw_df)
        self.uploader.filter_new_records = MagicMock(return_value=raw_df)
        self.uploader.upload_metadata = MagicMock(return_value=1)
        self.uploader.save_contents = MagicMock(return_value=1)

        result = self.uploader.upload("2026-02-27")

        self.assertEqual(result["record_count"], 1)
        self.assertEqual(result["file_count"], 1)
        self.uploader.upload_metadata.assert_called_once()
        self.uploader.save_contents.assert_called_once()


class TestCrawlDataByHours(unittest.TestCase):
    """測試 crawl_data_by_hours 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.ctee_news.requests.get")
    def test_success(self, mock_get):
        """測試成功取得過去 N 小時的新聞資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [
                {
                    "Date": "2026-02-27",
                    "Time": "14:30:00",
                    "Author": "記者A",
                    "Head": "標題1",
                    "SubHead": "副標",
                    "HashTag": "科技",
                    "url": "https://www.ctee.com.tw/news/1",
                    "Content": "全文內容1",
                },
                {
                    "Date": "2026-02-26",
                    "Time": "23:30:00",
                    "Author": "記者B",
                    "Head": "標題2",
                    "SubHead": "副標2",
                    "HashTag": "金融",
                    "url": "https://www.ctee.com.tw/news/2",
                    "Content": "全文內容2",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data_by_hours(24)

        self.assertEqual(len(df), 2)
        mock_get.assert_called_once_with(
            "http://localhost:6738/ctee_news",
            params={"hours": 24},
            timeout=600,
        )

    @patch("data_upload.ctee_news.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳空 DataFrame。"""
        mock_get.side_effect = Exception("連線失敗")

        df = self.uploader.crawl_data_by_hours(24)

        self.assertTrue(df.empty)

    @patch("data_upload.ctee_news.requests.get")
    def test_empty_data(self, mock_get):
        """測試無資料時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data_by_hours(24)

        self.assertTrue(df.empty)


class TestUploadByHours(unittest.TestCase):
    """測試 upload_by_hours 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CTEENewsUploader(self.mock_conn, "localhost:6738")

    def test_no_data(self):
        """測試無資料時回傳空結果。"""
        self.uploader.crawl_data_by_hours = MagicMock(
            return_value=pd.DataFrame()
        )

        result = self.uploader.upload_by_hours(24)

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)
        self.assertEqual(result["dates"], [])
        self.assertEqual(result["hours"], 24)

    def test_cross_day_data(self):
        """測試跨日資料正確分組處理。"""
        raw_df = pd.DataFrame({
            "Date": ["2026-02-27", "2026-02-26", "2026-02-27"],
            "Time": ["14:30:00", "23:30:00", "15:00:00"],
            "Author": ["記者A", "記者B", "記者C"],
            "Head": ["標題1", "標題2", "標題3"],
            "SubHead": ["副標", "副標2", "副標3"],
            "HashTag": ["科技", "金融", "半導體"],
            "url": [
                "https://a.com/1", "https://a.com/2", "https://a.com/3"
            ],
            "Content": ["全文1", "全文2", "全文3"],
        })
        self.uploader.crawl_data_by_hours = MagicMock(return_value=raw_df)
        self.uploader.filter_new_records = MagicMock(
            side_effect=lambda df, d: df
        )
        self.uploader.upload_metadata = MagicMock(
            side_effect=lambda df: len(df)
        )
        self.uploader.save_contents = MagicMock(
            side_effect=lambda df, d: len(df)
        )
        self.uploader.record_uploaded_date = MagicMock()

        result = self.uploader.upload_by_hours(24)

        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["file_count"], 3)
        self.assertEqual(len(result["dates"]), 2)
        self.assertIn("2026-02-26", result["dates"])
        self.assertIn("2026-02-27", result["dates"])
        # record_uploaded_date 應針對兩個不同日期各呼叫一次
        self.assertEqual(
            self.uploader.record_uploaded_date.call_count, 2
        )

    def test_all_existing_skipped(self):
        """測試所有記錄都已存在時跳過上傳。"""
        raw_df = pd.DataFrame({
            "Date": ["2026-02-27"],
            "Time": ["14:30:00"],
            "Author": ["記者A"],
            "Head": ["標題"],
            "SubHead": ["副標"],
            "HashTag": ["科技"],
            "url": ["https://a.com/1"],
            "Content": ["全文"],
        })
        self.uploader.crawl_data_by_hours = MagicMock(return_value=raw_df)
        self.uploader.filter_new_records = MagicMock(
            return_value=pd.DataFrame()
        )

        result = self.uploader.upload_by_hours(24)

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)
        self.assertEqual(result["dates"], [])


if __name__ == "__main__":
    unittest.main()

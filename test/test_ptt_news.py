"""PTT 新聞上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock, patch, call

import pandas as pd
from pydantic import ValidationError

from data_upload.ptt_news import (
    PTTNewsType,
    PTTNewsUploader,
)


class TestPTTNewsType(unittest.TestCase):
    """測試 PTTNewsType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = PTTNewsType(
            Date="2026-02-28",
            Time="14:30:00",
            Author="test_user",
            Head="[標的] 2330 台積電",
            url="https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html",
        )

        self.assertEqual(data.Date, "2026-02-28")
        self.assertEqual(data.Head, "[標的] 2330 台積電")
        self.assertEqual(
            data.url,
            "https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html",
        )

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            PTTNewsType(
                Date="2026-02-28",
                Time="14:30:00",
            )

    def test_empty_strings_allowed(self):
        """測試允許空字串。"""
        data = PTTNewsType(
            Date="2026-02-28",
            Time="",
            Author="",
            Head="",
            url="https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html",
        )

        self.assertEqual(data.Author, "")

    def test_no_hashtag_field(self):
        """測試 PTTNewsType 沒有 HashTag 欄位。"""
        data = PTTNewsType(
            Date="2026-02-28",
            Head="標題",
            url="https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html",
        )

        self.assertFalse(hasattr(data, "HashTag"))

    def test_no_subhead_field(self):
        """測試 PTTNewsType 沒有 SubHead 欄位。"""
        data = PTTNewsType(
            Date="2026-02-28",
            Head="標題",
            url="https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html",
        )

        self.assertFalse(hasattr(data, "SubHead"))


class TestUrlHash(unittest.TestCase):
    """測試 url_hash 靜態方法。"""

    def test_hash_length(self):
        """測試雜湊值長度為 12。"""
        result = PTTNewsUploader.url_hash(
            "https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html"
        )

        self.assertEqual(len(result), 12)

    def test_deterministic(self):
        """測試相同 URL 產生相同雜湊值。"""
        url = "https://www.ptt.cc/bbs/Stock/M.1234567890.A.123.html"

        hash1 = PTTNewsUploader.url_hash(url)
        hash2 = PTTNewsUploader.url_hash(url)

        self.assertEqual(hash1, hash2)

    def test_different_urls_different_hashes(self):
        """測試不同 URL 產生不同雜湊值。"""
        hash1 = PTTNewsUploader.url_hash("https://www.ptt.cc/1")
        hash2 = PTTNewsUploader.url_hash("https://www.ptt.cc/2")

        self.assertNotEqual(hash1, hash2)


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.ptt_news.requests.get")
    def test_success(self, mock_get):
        """測試成功取得新聞資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-02-28",
            "data": [
                {
                    "Date": "2026-02-28",
                    "Time": "14:30:00",
                    "Author": "test_user",
                    "Head": "[標的] 測試",
                    "url": "https://www.ptt.cc/bbs/Stock/M.123.A.html",
                    "Content": "全文內容",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-02-28")

        self.assertEqual(len(df), 1)
        self.assertEqual(df["Head"].iloc[0], "[標的] 測試")

    @patch("data_upload.ptt_news.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳空 DataFrame。"""
        mock_get.side_effect = Exception("連線失敗")

        df = self.uploader.crawl_data("2026-02-28")

        self.assertTrue(df.empty)

    @patch("data_upload.ptt_news.requests.get")
    def test_empty_data(self, mock_get):
        """測試無資料時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"date": "2026-02-28", "data": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-02-28")

        self.assertTrue(df.empty)


class TestGetExistingUrls(unittest.TestCase):
    """測試 get_existing_urls 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    def test_has_existing_urls(self):
        """測試有已存在 URL 時回傳正確集合。"""
        self.mock_conn.execute.return_value.fetchall.return_value = [
            ("https://www.ptt.cc/bbs/Stock/M.1.A.html",),
            ("https://www.ptt.cc/bbs/Stock/M.2.A.html",),
        ]

        result = self.uploader.get_existing_urls("2026-02-28")

        self.assertEqual(len(result), 2)
        self.assertIn(
            "https://www.ptt.cc/bbs/Stock/M.1.A.html", result
        )

    def test_no_existing_urls(self):
        """測試無已存在 URL 時回傳空集合。"""
        self.mock_conn.execute.return_value.fetchall.return_value = []

        result = self.uploader.get_existing_urls("2026-02-28")

        self.assertEqual(len(result), 0)


class TestFilterNewRecords(unittest.TestCase):
    """測試 filter_new_records 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    def test_all_new(self):
        """測試所有記錄都是新的。"""
        df = pd.DataFrame({
            "url": ["https://ptt.cc/1", "https://ptt.cc/2"],
            "Head": ["標題1", "標題2"],
        })
        self.uploader.get_existing_urls = MagicMock(return_value=set())

        result = self.uploader.filter_new_records(df, "2026-02-28")

        self.assertEqual(len(result), 2)

    def test_some_existing(self):
        """測試部分記錄已存在。"""
        df = pd.DataFrame({
            "url": ["https://ptt.cc/1", "https://ptt.cc/2"],
            "Head": ["標題1", "標題2"],
        })
        self.uploader.get_existing_urls = MagicMock(
            return_value={"https://ptt.cc/1"}
        )

        result = self.uploader.filter_new_records(df, "2026-02-28")

        self.assertEqual(len(result), 1)
        self.assertEqual(result["url"].iloc[0], "https://ptt.cc/2")

    def test_all_existing(self):
        """測試所有記錄都已存在。"""
        df = pd.DataFrame({
            "url": ["https://ptt.cc/1"],
            "Head": ["標題1"],
        })
        self.uploader.get_existing_urls = MagicMock(
            return_value={"https://ptt.cc/1"}
        )

        result = self.uploader.filter_new_records(df, "2026-02-28")

        self.assertTrue(result.empty)

    def test_empty_dataframe(self):
        """測試空 DataFrame。"""
        df = pd.DataFrame()

        result = self.uploader.filter_new_records(df, "2026-02-28")

        self.assertTrue(result.empty)


class TestCheckSchema(unittest.TestCase):
    """測試 check_schema 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["test_user"],
            "Head": ["[標的] 測試"],
            "url": ["https://ptt.cc/1"],
            "Content": ["全文"],
        })

        result = self.uploader.check_schema(df)

        self.assertEqual(len(result), 1)
        # Content 不應出現在驗證後的 DataFrame
        self.assertNotIn("Content", result.columns)
        self.assertIn("url", result.columns)
        # HashTag 和 SubHead 不應出現
        self.assertNotIn("HashTag", result.columns)
        self.assertNotIn("SubHead", result.columns)

    def test_content_file_is_md(self):
        """測試 ContentFile 副檔名為 .md。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["test_user"],
            "Head": ["標題"],
            "url": ["https://ptt.cc/1"],
            "Content": ["全文"],
        })

        result = self.uploader.check_schema(df)

        content_file = result["ContentFile"].iloc[0]
        self.assertTrue(content_file.endswith(".md"))

    def test_missing_field_raises_error(self):
        """測試缺少欄位時拋出例外。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
        })

        with self.assertRaises(Exception):
            self.uploader.check_schema(df)


class TestSaveContents(unittest.TestCase):
    """測試 save_contents 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.ptt_news.NEWS_CONTENT_BASE")
    def test_save_files(self, mock_base):
        """測試成功儲存全文檔案。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)
        mock_dir.mkdir = MagicMock()

        df = pd.DataFrame({
            "url": ["https://ptt.cc/1", "https://ptt.cc/2"],
            "Content": ["全文1", "全文2"],
        })

        saved = self.uploader.save_contents(df, "2026-02-28")

        self.assertEqual(saved, 2)
        self.assertEqual(mock_file.write_text.call_count, 2)

    @patch("data_upload.ptt_news.NEWS_CONTENT_BASE")
    def test_empty_url_skipped(self, mock_base):
        """測試空 URL 被跳過。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)
        mock_dir.mkdir = MagicMock()

        df = pd.DataFrame({
            "url": ["", "https://ptt.cc/1"],
            "Content": ["全文1", "全文2"],
        })

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)

        saved = self.uploader.save_contents(df, "2026-02-28")

        self.assertEqual(saved, 1)

    @patch("data_upload.ptt_news.NEWS_CONTENT_BASE")
    def test_file_extension_is_md(self, mock_base):
        """測試儲存的檔案副檔名為 .md。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)
        mock_dir.mkdir = MagicMock()

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)

        df = pd.DataFrame({
            "url": ["https://ptt.cc/1"],
            "Content": ["全文"],
        })

        self.uploader.save_contents(df, "2026-02-28")

        # 確認檔案名稱以 .md 結尾
        call_args = mock_dir.__truediv__.call_args_list
        for c in call_args:
            file_name = c[0][0]
            self.assertTrue(file_name.endswith(".md"))


class TestUploadMetadata(unittest.TestCase):
    """測試 upload_metadata 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    def test_upload_records(self):
        """測試成功上傳 metadata。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["test_user"],
            "Head": ["標題"],
            "url": ["https://ptt.cc/1"],
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


class TestRecordUploadedDate(unittest.TestCase):
    """測試 record_uploaded_date 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    def test_record_date(self):
        """測試成功記錄已上傳日期。"""
        self.uploader.record_uploaded_date("2026-02-28")

        self.mock_conn.execute.assert_called_once()
        self.mock_conn.commit.assert_called_once()

    def test_record_date_failure(self):
        """測試記錄失敗時不拋出例外。"""
        self.mock_conn.execute.side_effect = Exception("資料庫錯誤")

        # 不應拋出例外
        self.uploader.record_uploaded_date("2026-02-28")


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    def test_no_data(self):
        """測試無資料時回傳 record_count=0。"""
        self.uploader.crawl_data = MagicMock(return_value=pd.DataFrame())

        result = self.uploader.upload("2026-02-28")

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)
        self.assertEqual(result["date"], "2026-02-28")

    def test_all_existing(self):
        """測試所有記錄都已存在時跳過上傳。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["test_user"],
            "Head": ["標題"],
            "url": ["https://ptt.cc/1"],
            "Content": ["全文"],
        })
        self.uploader.crawl_data = MagicMock(return_value=df)
        self.uploader.filter_new_records = MagicMock(
            return_value=pd.DataFrame()
        )

        result = self.uploader.upload("2026-02-28")

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)

    def test_successful_upload(self):
        """測試成功上傳回傳正確數量。"""
        raw_df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["test_user"],
            "Head": ["標題"],
            "url": ["https://ptt.cc/1"],
            "Content": ["全文"],
        })
        self.uploader.crawl_data = MagicMock(return_value=raw_df)
        self.uploader.filter_new_records = MagicMock(return_value=raw_df)
        self.uploader.upload_metadata = MagicMock(return_value=1)
        self.uploader.save_contents = MagicMock(return_value=1)

        result = self.uploader.upload("2026-02-28")

        self.assertEqual(result["record_count"], 1)
        self.assertEqual(result["file_count"], 1)
        self.uploader.upload_metadata.assert_called_once()
        self.uploader.save_contents.assert_called_once()


class TestCrawlDataByHours(unittest.TestCase):
    """測試 crawl_data_by_hours 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

    @patch("data_upload.ptt_news.requests.get")
    def test_success(self, mock_get):
        """測試成功取得過去 N 小時的新聞資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [
                {
                    "Date": "2026-02-28",
                    "Time": "14:30:00",
                    "Author": "test_user",
                    "Head": "標題1",
                    "url": "https://ptt.cc/bbs/stock/1",
                    "Content": "全文內容1",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data_by_hours(24)

        self.assertEqual(len(df), 1)
        mock_get.assert_called_once_with(
            "http://localhost:6738/ptt_news",
            params={"hours": 24},
            timeout=600,
        )

    @patch("data_upload.ptt_news.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳空 DataFrame。"""
        mock_get.side_effect = Exception("連線失敗")

        df = self.uploader.crawl_data_by_hours(24)

        self.assertTrue(df.empty)

    @patch("data_upload.ptt_news.requests.get")
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
        self.uploader = PTTNewsUploader(self.mock_conn, "localhost:6738")

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
            "Date": ["2026-02-28", "2026-02-27", "2026-02-28"],
            "Time": ["14:30:00", "23:30:00", "15:00:00"],
            "Author": ["user1", "user2", "user3"],
            "Head": ["標題1", "標題2", "標題3"],
            "url": [
                "https://ptt.cc/1", "https://ptt.cc/2", "https://ptt.cc/3"
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
        self.assertEqual(
            self.uploader.record_uploaded_date.call_count, 2
        )

    def test_all_existing_skipped(self):
        """測試所有記錄都已存在時跳過上傳。"""
        raw_df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["test_user"],
            "Head": ["標題"],
            "url": ["https://ptt.cc/1"],
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

"""MoneyUDN 新聞上傳模組單元測試。"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError

from data_upload.moneyudn_news import (
    MoneyUDNNewsType,
    MoneyUDNNewsUploader,
)


class TestMoneyUDNNewsType(unittest.TestCase):
    """測試 MoneyUDNNewsType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = MoneyUDNNewsType(
            Date="2026-02-28",
            Time="14:30:00",
            Author="記者名",
            Head="台積電法說會重點整理",
            url="https://money.udn.com/money/story/1234/5678",
        )

        self.assertEqual(data.Date, "2026-02-28")
        self.assertEqual(data.Head, "台積電法說會重點整理")
        self.assertEqual(
            data.url,
            "https://money.udn.com/money/story/1234/5678",
        )

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            MoneyUDNNewsType(
                Date="2026-02-28",
                Time="14:30:00",
            )

    def test_empty_strings_allowed(self):
        """測試允許空字串。"""
        data = MoneyUDNNewsType(
            Date="2026-02-28",
            Time="",
            Author="",
            Head="",
            url="https://money.udn.com/money/story/1234/5678",
        )

        self.assertEqual(data.Author, "")

    def test_no_hashtag_field(self):
        """測試 MoneyUDNNewsType 沒有 HashTag 欄位。"""
        data = MoneyUDNNewsType(
            Date="2026-02-28",
            Head="標題",
            url="https://money.udn.com/money/story/1234/5678",
        )

        self.assertFalse(hasattr(data, "HashTag"))

    def test_no_subhead_field(self):
        """測試 MoneyUDNNewsType 沒有 SubHead 欄位。"""
        data = MoneyUDNNewsType(
            Date="2026-02-28",
            Head="標題",
            url="https://money.udn.com/money/story/1234/5678",
        )

        self.assertFalse(hasattr(data, "SubHead"))


class TestUrlHash(unittest.TestCase):
    """測試 url_hash 靜態方法。"""

    def test_hash_length(self):
        """測試雜湊值長度為 12。"""
        result = MoneyUDNNewsUploader.url_hash(
            "https://money.udn.com/money/story/1234/5678"
        )

        self.assertEqual(len(result), 12)

    def test_deterministic(self):
        """測試相同 URL 產生相同雜湊值。"""
        url = "https://money.udn.com/money/story/1234/5678"

        hash1 = MoneyUDNNewsUploader.url_hash(url)
        hash2 = MoneyUDNNewsUploader.url_hash(url)

        self.assertEqual(hash1, hash2)

    def test_different_urls_different_hashes(self):
        """測試不同 URL 產生不同雜湊值。"""
        hash1 = MoneyUDNNewsUploader.url_hash(
            "https://money.udn.com/1"
        )
        hash2 = MoneyUDNNewsUploader.url_hash(
            "https://money.udn.com/2"
        )

        self.assertNotEqual(hash1, hash2)


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    @patch("data_upload.moneyudn_news.requests.get")
    def test_success(self, mock_get):
        """測試成功取得新聞資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-02-28",
            "data": [
                {
                    "Date": "2026-02-28",
                    "Time": "14:30:00",
                    "Author": "記者名",
                    "Head": "經濟日報測試新聞",
                    "url": "https://money.udn.com/money/story/1/2",
                    "Content": "全文內容",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-02-28")

        self.assertEqual(len(df), 1)
        self.assertEqual(df["Head"].iloc[0], "經濟日報測試新聞")

    @patch("data_upload.moneyudn_news.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳空 DataFrame。"""
        mock_get.side_effect = Exception("連線失敗")

        df = self.uploader.crawl_data("2026-02-28")

        self.assertTrue(df.empty)

    @patch("data_upload.moneyudn_news.requests.get")
    def test_empty_data(self, mock_get):
        """測試無資料時回傳空 DataFrame。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "date": "2026-02-28", "data": []
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = self.uploader.crawl_data("2026-02-28")

        self.assertTrue(df.empty)


class TestGetExistingUrls(unittest.TestCase):
    """測試 get_existing_urls 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_has_existing_urls(self):
        """測試有已存在 URL 時回傳正確集合。"""
        self.mock_conn.execute.return_value.fetchall.return_value = [
            ("https://money.udn.com/1",),
            ("https://money.udn.com/2",),
        ]

        result = self.uploader.get_existing_urls("2026-02-28")

        self.assertEqual(len(result), 2)
        self.assertIn("https://money.udn.com/1", result)

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
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_all_new(self):
        """測試所有記錄都是新的。"""
        df = pd.DataFrame({
            "url": ["https://money.udn.com/1", "https://money.udn.com/2"],
            "Head": ["標題1", "標題2"],
        })
        self.uploader.get_existing_urls = MagicMock(return_value=set())

        result = self.uploader.filter_new_records(df, "2026-02-28")

        self.assertEqual(len(result), 2)

    def test_some_existing(self):
        """測試部分記錄已存在。"""
        df = pd.DataFrame({
            "url": ["https://money.udn.com/1", "https://money.udn.com/2"],
            "Head": ["標題1", "標題2"],
        })
        self.uploader.get_existing_urls = MagicMock(
            return_value={"https://money.udn.com/1"}
        )

        result = self.uploader.filter_new_records(df, "2026-02-28")

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result["url"].iloc[0], "https://money.udn.com/2"
        )

    def test_all_existing(self):
        """測試所有記錄都已存在。"""
        df = pd.DataFrame({
            "url": ["https://money.udn.com/1"],
            "Head": ["標題1"],
        })
        self.uploader.get_existing_urls = MagicMock(
            return_value={"https://money.udn.com/1"}
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
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["記者名"],
            "Head": ["經濟日報測試"],
            "url": ["https://money.udn.com/1"],
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
            "Author": ["記者名"],
            "Head": ["標題"],
            "url": ["https://money.udn.com/1"],
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
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    def test_save_files(self, mock_base):
        """測試成功儲存全文檔案。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)
        mock_dir.mkdir = MagicMock()

        df = pd.DataFrame({
            "url": ["https://money.udn.com/1", "https://money.udn.com/2"],
            "Content": ["全文1", "全文2"],
        })

        saved = self.uploader.save_contents(df, "2026-02-28")

        self.assertEqual(saved, 2)
        self.assertEqual(mock_file.write_text.call_count, 2)

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    def test_empty_url_skipped(self, mock_base):
        """測試空 URL 被跳過。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)
        mock_dir.mkdir = MagicMock()

        df = pd.DataFrame({
            "url": ["", "https://money.udn.com/1"],
            "Content": ["全文1", "全文2"],
        })

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)

        saved = self.uploader.save_contents(df, "2026-02-28")

        self.assertEqual(saved, 1)

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    def test_file_extension_is_md(self, mock_base):
        """測試儲存的檔案副檔名為 .md。"""
        mock_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_dir)
        mock_dir.mkdir = MagicMock()

        mock_file = MagicMock()
        mock_dir.__truediv__ = MagicMock(return_value=mock_file)

        df = pd.DataFrame({
            "url": ["https://money.udn.com/1"],
            "Content": ["全文"],
        })

        self.uploader.save_contents(df, "2026-02-28")

        # 確認檔案名稱以 .md 結尾
        call_args = mock_dir.__truediv__.call_args_list
        for c in call_args:
            file_name = c[0][0]
            self.assertTrue(file_name.endswith(".md"))


class TestGuessExtension(unittest.TestCase):
    """測試 _guess_extension 靜態方法。"""

    def test_content_type_jpeg(self):
        """測試 Content-Type 為 image/jpeg 時回傳 jpg。"""
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "image/jpeg"}

        result = MoneyUDNNewsUploader._guess_extension(
            "https://example.com/photo", mock_resp
        )

        self.assertEqual(result, "jpg")

    def test_content_type_png(self):
        """測試 Content-Type 為 image/png 時回傳 png。"""
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "image/png; charset=utf-8"}

        result = MoneyUDNNewsUploader._guess_extension(
            "https://example.com/photo", mock_resp
        )

        self.assertEqual(result, "png")

    def test_content_type_webp(self):
        """測試 Content-Type 為 image/webp 時回傳 webp。"""
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "image/webp"}

        result = MoneyUDNNewsUploader._guess_extension(
            "https://example.com/photo", mock_resp
        )

        self.assertEqual(result, "webp")

    def test_fallback_to_url_extension(self):
        """測試 Content-Type 不明確時從 URL 推斷副檔名。"""
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "application/octet-stream"}

        result = MoneyUDNNewsUploader._guess_extension(
            "https://example.com/photo.png?w=800", mock_resp
        )

        self.assertEqual(result, "png")

    def test_jpeg_normalized_to_jpg(self):
        """測試 URL 中的 jpeg 副檔名正規化為 jpg。"""
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "application/octet-stream"}

        result = MoneyUDNNewsUploader._guess_extension(
            "https://example.com/photo.jpeg", mock_resp
        )

        self.assertEqual(result, "jpg")

    def test_default_jpg(self):
        """測試無法判斷時預設回傳 jpg。"""
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "application/octet-stream"}

        result = MoneyUDNNewsUploader._guess_extension(
            "https://example.com/photo", mock_resp
        )

        self.assertEqual(result, "jpg")


class TestDownloadImages(unittest.TestCase):
    """測試 _download_images 靜態方法。"""

    def test_empty_content(self):
        """測試空內容直接回傳。"""
        result = MoneyUDNNewsUploader._download_images("", "2026-02-28")

        self.assertEqual(result, "")

    def test_none_content(self):
        """測試 None 內容直接回傳。"""
        result = MoneyUDNNewsUploader._download_images(None, "2026-02-28")

        self.assertIsNone(result)

    def test_no_images(self):
        """測試不含圖片的 Markdown 原樣回傳。"""
        content = "# 標題\n\n這是純文字內容。"

        result = MoneyUDNNewsUploader._download_images(
            content, "2026-02-28"
        )

        self.assertEqual(result, content)

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    @patch("data_upload.moneyudn_news.requests.get")
    def test_successful_download(self, mock_get, mock_base):
        """測試成功下載圖片並替換 URL。"""
        # 設定 mock 目錄
        mock_images_dir = MagicMock()
        mock_date_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_date_dir)
        mock_date_dir.__truediv__ = MagicMock(return_value=mock_images_dir)
        mock_images_dir.__truediv__ = MagicMock(return_value=MagicMock())
        mock_images_dir.mkdir = MagicMock()

        # 設定 mock HTTP 回應
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "image/jpeg"}
        mock_resp.content = b"fake_image_data"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        content = "![圖片](https://example.com/photo.jpg)"

        result = MoneyUDNNewsUploader._download_images(
            content, "2026-02-28"
        )

        # 確認 URL 已被替換為本地路徑
        self.assertIn("images/", result)
        self.assertNotIn("https://example.com/photo.jpg", result)

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    @patch("data_upload.moneyudn_news.requests.get")
    def test_download_failure_keeps_original_url(self, mock_get, mock_base):
        """測試下載失敗時保留原始 URL。"""
        mock_images_dir = MagicMock()
        mock_date_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_date_dir)
        mock_date_dir.__truediv__ = MagicMock(return_value=mock_images_dir)
        mock_images_dir.mkdir = MagicMock()

        # 模擬下載失敗
        mock_get.side_effect = Exception("連線逾時")

        content = "![圖片](https://example.com/photo.jpg)"

        result = MoneyUDNNewsUploader._download_images(
            content, "2026-02-28"
        )

        # 原始 URL 應保留
        self.assertIn("https://example.com/photo.jpg", result)

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    @patch("data_upload.moneyudn_news.requests.get")
    def test_duplicate_urls_downloaded_once(self, mock_get, mock_base):
        """測試重複的圖片 URL 只下載一次。"""
        mock_images_dir = MagicMock()
        mock_date_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_date_dir)
        mock_date_dir.__truediv__ = MagicMock(return_value=mock_images_dir)
        mock_images_dir.__truediv__ = MagicMock(return_value=MagicMock())
        mock_images_dir.mkdir = MagicMock()

        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": "image/png"}
        mock_resp.content = b"fake_image"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        content = (
            "![圖1](https://example.com/same.png)\n"
            "![圖2](https://example.com/same.png)"
        )

        MoneyUDNNewsUploader._download_images(content, "2026-02-28")

        # requests.get 只應被呼叫一次（同一 URL 不重複下載）
        mock_get.assert_called_once()

    @patch("data_upload.moneyudn_news.NEWS_CONTENT_BASE")
    @patch("data_upload.moneyudn_news.requests.get")
    def test_mixed_success_and_failure(self, mock_get, mock_base):
        """測試部分下載成功、部分失敗的情境。"""
        mock_images_dir = MagicMock()
        mock_date_dir = MagicMock()
        mock_base.__truediv__ = MagicMock(return_value=mock_date_dir)
        mock_date_dir.__truediv__ = MagicMock(return_value=mock_images_dir)
        mock_images_dir.__truediv__ = MagicMock(return_value=MagicMock())
        mock_images_dir.mkdir = MagicMock()

        # 第一次成功，第二次失敗
        mock_resp_ok = MagicMock()
        mock_resp_ok.headers = {"Content-Type": "image/jpeg"}
        mock_resp_ok.content = b"ok"
        mock_resp_ok.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_resp_ok, Exception("失敗")]

        content = (
            "![圖1](https://example.com/ok.jpg)\n"
            "![圖2](https://example.com/fail.jpg)"
        )

        result = MoneyUDNNewsUploader._download_images(
            content, "2026-02-28"
        )

        # 成功的圖片 URL 應被替換
        self.assertNotIn("https://example.com/ok.jpg", result)
        self.assertIn("images/", result)
        # 失敗的圖片 URL 應保留
        self.assertIn("https://example.com/fail.jpg", result)


class TestUploadMetadata(unittest.TestCase):
    """測試 upload_metadata 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_upload_records(self):
        """測試成功上傳 metadata。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["記者名"],
            "Head": ["標題"],
            "url": ["https://money.udn.com/1"],
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
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

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
        self.uploader = MoneyUDNNewsUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_no_data(self):
        """測試無資料時回傳 record_count=0。"""
        self.uploader.crawl_data = MagicMock(
            return_value=pd.DataFrame()
        )

        result = self.uploader.upload("2026-02-28")

        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["file_count"], 0)
        self.assertEqual(result["date"], "2026-02-28")

    def test_all_existing(self):
        """測試所有記錄都已存在時跳過上傳。"""
        df = pd.DataFrame({
            "Date": ["2026-02-28"],
            "Time": ["14:30:00"],
            "Author": ["記者名"],
            "Head": ["標題"],
            "url": ["https://money.udn.com/1"],
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
            "Author": ["記者名"],
            "Head": ["標題"],
            "url": ["https://money.udn.com/1"],
            "Content": ["全文"],
        })
        self.uploader.crawl_data = MagicMock(return_value=raw_df)
        self.uploader.filter_new_records = MagicMock(
            return_value=raw_df
        )
        self.uploader.upload_metadata = MagicMock(return_value=1)
        self.uploader.save_contents = MagicMock(return_value=1)

        result = self.uploader.upload("2026-02-28")

        self.assertEqual(result["record_count"], 1)
        self.assertEqual(result["file_count"], 1)
        self.uploader.upload_metadata.assert_called_once()
        self.uploader.save_contents.assert_called_once()


if __name__ == "__main__":
    unittest.main()

"""四支新聞上傳器的爬蟲 status 契約單元測試。

CTEE／CNYES／PTT／MoneyUDN 四支上傳器結構相同，狀態處理邏輯亦相同，
故以同一組測試涵蓋四者，確保未來新增來源時行為不致分歧。

本模組守住的核心不變量：**爬取失敗絕不被當成「當日無新聞」靜默略過**。
舊行為在爬蟲回傳 `data: []` 時只是記錄一行「無資料」並回報成功，
任務不會進 retry queue，缺漏就此永久遺失（新聞來源回溯窗有限）。
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from data_upload.base import NetworkError, OutOfRangeError
from data_upload.cnyes_news import CNYESNewsUploader
from data_upload.ctee_news import CTEENewsUploader
from data_upload.moneyudn_news import MoneyUDNNewsUploader
from data_upload.ptt_news import PTTNewsUploader

# (上傳器類別, 該模組的 requests.get patch 目標)
UPLOADERS = [
    (CTEENewsUploader, "data_upload.ctee_news.requests.get"),
    (CNYESNewsUploader, "data_upload.cnyes_news.requests.get"),
    (PTTNewsUploader, "data_upload.ptt_news.requests.get"),
    (MoneyUDNNewsUploader, "data_upload.moneyudn_news.requests.get"),
]


def _response(payload):
    """建立回傳指定 JSON 的 mock response。

    Args:
        payload (dict): 要回傳的 JSON 物件。

    Returns:
        MagicMock: mock 的 requests response。
    """
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


class TestNewsCrawlStatus(unittest.TestCase):
    """測試四支新聞上傳器對各爬蟲狀態的處理。"""

    def test_error_status_raises_on_crawl_data(self):
        """測試 status=error 時拋 NetworkError（而非回報無新聞）。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(
                        {"date": "2026-08-16", "status": "error",
                         "data": [], "message": "來源逾時"}
                    )
                    with self.assertRaises(NetworkError):
                        uploader.crawl_data("2026-08-16")

    def test_error_status_raises_on_crawl_data_by_hours(self):
        """測試時數模式下 status=error 同樣拋 NetworkError。

        每日排程實際走的是時數模式，這條路徑漏掉就等於防呆失效。
        """
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(
                        {"hours": 48, "status": "error",
                         "data": [], "message": "來源逾時"}
                    )
                    with self.assertRaises(NetworkError):
                        uploader.crawl_data_by_hours(48)

    def test_out_of_range_raises_non_retryable(self):
        """測試 status=out_of_range 拋出不可重試的 OutOfRangeError。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(
                        {"date": "2026-08-05", "status": "out_of_range",
                         "data": [], "message": "超出可回溯範圍",
                         "meta": {"oldest_available": "2026-08-13"}}
                    )
                    with self.assertRaises(OutOfRangeError) as ctx:
                        uploader.crawl_data("2026-08-05")
                    self.assertEqual(
                        ctx.exception.oldest_available, "2026-08-13"
                    )
                    self.assertNotIsInstance(ctx.exception, NetworkError)

    def test_empty_status_returns_empty_df_without_raising(self):
        """測試 status=empty 時安靜回空表（該日確實沒有新聞）。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(
                        {"date": "2026-08-16", "status": "empty", "data": []}
                    )
                    df = uploader.crawl_data("2026-08-16")
                    self.assertTrue(df.empty)

    def test_legacy_response_without_status_still_works(self):
        """測試舊版爬蟲回應（無 status）維持既有行為。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response({"data": []})
                    df = uploader.crawl_data("2026-08-16")
                    self.assertTrue(df.empty)

    def test_partial_does_not_raise_during_crawl(self):
        """測試 partial 於爬取階段不拋例外，讓已取得的資料得以落地。

        新聞以 URL 去重，先存已取得的部分再重試是冪等且不損失資料的；
        若在爬取階段就拋例外，這些資料會被整批丟棄。
        """
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(
                        {"date": "2026-08-16", "status": "partial",
                         "data": [{"url": "u1", "Date": "2026-08-16"}],
                         "meta": {"detail_failed": 2}}
                    )
                    df = uploader.crawl_data("2026-08-16")
                    self.assertEqual(len(df), 1)


class TestNewsPartialRetryDecision(unittest.TestCase):
    """測試 partial 於資料落地後的重試決策。"""

    def _uploader_with_status(self, cls, status, reason):
        """建立已設定爬取狀態的上傳器。

        Args:
            cls (type): 上傳器類別。
            status (str | None): 最近一次爬取狀態。
            reason (str | None): partial 的可重抓原因。

        Returns:
            上傳器實例。
        """
        uploader = cls(MagicMock(), "crawler:6738")
        uploader._last_status = status
        uploader._last_partial_reason = reason
        return uploader

    def test_transient_partial_raises_for_retry(self):
        """測試暫時性不完整時拋 NetworkError 以排入重試補齊。"""
        for cls, _ in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = self._uploader_with_status(
                    cls, "partial", "detail_failed=2"
                )
                with self.assertRaises(NetworkError) as ctx:
                    uploader._check_incomplete("（hours=48）")
                self.assertIn("detail_failed=2", str(ctx.exception))

    def test_source_truncated_partial_does_not_raise(self):
        """測試來源硬上限造成的不完整不重試（重抓也拿不到）。"""
        for cls, _ in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = self._uploader_with_status(cls, "partial", None)
                uploader._check_incomplete("（hours=48）")

    def test_ok_status_does_not_raise(self):
        """測試正常狀態不觸發重試。"""
        for cls, _ in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = self._uploader_with_status(cls, "ok", None)
                uploader._check_incomplete("（hours=48）")


class TestNewsUploadPersistsBeforeRetry(unittest.TestCase):
    """測試 partial 時資料先落地、再拋例外要求重試。"""

    def test_upload_by_hours_uploads_then_raises(self):
        """測試時數模式 partial：先寫入 metadata，之後才拋 NetworkError。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                records = [{
                    "Date": "2026-08-16", "Time": "10:00:00",
                    "Author": "記者", "Head": "標題", "SubHead": "副標",
                    "HashTag": "tag", "url": "https://example.com/1",
                    "Content": "內文", "Board": "Stock", "PushCount": 1,
                }]
                with patch(target) as mock_get:
                    mock_get.return_value = _response(
                        {"hours": 48, "status": "partial", "data": records,
                         "meta": {"detail_failed": 1}}
                    )
                    with patch.object(
                        uploader, "filter_new_records",
                        side_effect=lambda df, d: df,
                    ), patch.object(
                        uploader, "upload_metadata", return_value=1,
                    ) as mock_upload, patch.object(
                        uploader, "save_contents", return_value=1,
                    ), patch.object(
                        uploader, "record_uploaded_date",
                    ):
                        with self.assertRaises(NetworkError):
                            uploader.upload_by_hours(48)
                        # 關鍵：例外必須發生在資料寫入「之後」。
                        mock_upload.assert_called_once()


if __name__ == "__main__":
    unittest.main()

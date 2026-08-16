"""爬蟲 status 契約判讀單元測試模組。

`Tw_stock_crawer` v2.13.0 起保證 `data` 鍵永遠存在（失敗時為 `[]`），
本專案原本正是靠 `KeyError` 得知爬取失敗。本模組確保新契約下：

1. 失敗（`error`／`partial`／未知狀態）一律拋可重試的 `NetworkError`；
2. `out_of_range` 拋不可重試的 `OutOfRangeError`；
3. **失敗絕不寫入 `UploadDate` 帳本**——這是本次要防的回歸：帳本一旦被
   誤標成「當日無資料」，該日就永久跳過、真實行情再也補不回。
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import BaseModel

from data_upload.base import (
    STATUS_EMPTY,
    STATUS_OK,
    STATUS_PARTIAL,
    CrawlError,
    DataUploadBase,
    NetworkError,
    OutOfRangeError,
    check_crawl_status,
    partial_retry_reason,
)


class SimpleUploadType(BaseModel):
    """測試用 schema。"""

    SecurityCode: str
    Value: float


class ConcreteUploader(DataUploadBase):
    """測試用具體上傳器。"""

    def __init__(self, conn):
        """初始化測試用上傳器。

        Args:
            conn (MagicMock): Mock 的資料庫連線物件。
        """
        super().__init__(conn)
        self.name = "test"
        self.url = "http://localhost:6738"
        self.UploadType = SimpleUploadType
        self.stock_code_col = None
        self.stock_name_col = None

    def preprocess(self, df):
        """預處理 DataFrame（測試用，原樣回傳）。

        Args:
            df (pd.DataFrame): 待預處理的 DataFrame。

        Returns:
            pd.DataFrame: 未經修改的 DataFrame。
        """
        return df


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


class TestCheckCrawlStatus(unittest.TestCase):
    """測試 check_crawl_status 對各狀態的判讀。"""

    def test_ok_passes_through(self):
        """測試 ok 狀態放行並回傳狀態值。"""
        result = check_crawl_status({"status": "ok", "data": [1]}, "測試")
        self.assertEqual(result, STATUS_OK)

    def test_empty_passes_through(self):
        """測試 empty 狀態放行（該日確實無資料）。"""
        result = check_crawl_status({"status": "empty", "data": []}, "測試")
        self.assertEqual(result, STATUS_EMPTY)

    def test_missing_status_passes_through(self):
        """測試舊版爬蟲（無 status 欄位）維持既有行為。"""
        result = check_crawl_status({"data": []}, "測試")
        self.assertEqual(result, STATUS_OK)

    def test_non_dict_passes_through(self):
        """測試非字典回應不誤判為失敗。"""
        self.assertEqual(check_crawl_status([1, 2], "測試"), STATUS_OK)

    def test_error_raises_network_error(self):
        """測試 error 狀態拋出可重試的 NetworkError。"""
        with self.assertRaises(NetworkError) as ctx:
            check_crawl_status(
                {"status": "error", "data": [], "message": "來源逾時"},
                "測試", "（2026-08-16）",
            )
        self.assertIn("來源逾時", str(ctx.exception))

    def test_error_falls_back_to_error_key(self):
        """測試無 message 時改用既有 error 鍵作為說明。"""
        with self.assertRaises(NetworkError) as ctx:
            check_crawl_status(
                {"status": "error", "data": [], "error": "boom"}, "測試"
            )
        self.assertIn("boom", str(ctx.exception))

    def test_partial_raises_network_error_by_default(self):
        """測試行情類（不允許 partial）時 partial 拋 NetworkError。"""
        with self.assertRaises(NetworkError):
            check_crawl_status({"status": "partial", "data": [1]}, "測試")

    def test_partial_returned_when_allowed(self):
        """測試新聞類允許 partial 時回傳狀態值而不拋例外。"""
        result = check_crawl_status(
            {"status": "partial", "data": [1]}, "測試", allow_partial=True
        )
        self.assertEqual(result, STATUS_PARTIAL)

    def test_out_of_range_raises_out_of_range_error(self):
        """測試 out_of_range 拋出不可重試的 OutOfRangeError。"""
        with self.assertRaises(OutOfRangeError) as ctx:
            check_crawl_status(
                {
                    "status": "out_of_range",
                    "data": [],
                    "message": "超出範圍",
                    "meta": {"oldest_available": "2026-08-13"},
                },
                "CTEE 新聞", "（2026-08-05）",
            )
        self.assertEqual(ctx.exception.oldest_available, "2026-08-13")
        self.assertIn("2026-08-13", str(ctx.exception))

    def test_out_of_range_is_not_retryable(self):
        """測試 OutOfRangeError 不屬於 NetworkError（不進 retry queue）。"""
        self.assertTrue(issubclass(OutOfRangeError, CrawlError))
        self.assertFalse(issubclass(OutOfRangeError, NetworkError))

    def test_unknown_status_raises_network_error(self):
        """測試未知狀態保守視為可重試失敗，而非當日無資料。"""
        with self.assertRaises(NetworkError):
            check_crawl_status({"status": "weird", "data": []}, "測試")

    def test_out_of_range_without_meta(self):
        """測試 out_of_range 缺 meta 時仍可正常拋出。"""
        with self.assertRaises(OutOfRangeError) as ctx:
            check_crawl_status({"status": "out_of_range", "data": []}, "測試")
        self.assertIsNone(ctx.exception.oldest_available)


class TestPartialRetryReason(unittest.TestCase):
    """測試 partial_retry_reason 對 meta 的判讀。"""

    def test_detail_failed_is_retryable(self):
        """測試部分全文抓取失敗時判為值得重抓。"""
        reason = partial_retry_reason(
            {"status": "partial", "meta": {"detail_failed": 3}}
        )
        self.assertIsNotNone(reason)
        self.assertIn("detail_failed=3", reason)

    def test_skipped_by_deadline_is_retryable(self):
        """測試因逾時提前收工時判為值得重抓。"""
        reason = partial_retry_reason(
            {"status": "partial", "meta": {"skipped_by_deadline": 5}}
        )
        self.assertIn("skipped_by_deadline=5", reason)

    def test_source_truncated_is_not_retryable(self):
        """測試來源硬上限造成的不完整判為重抓無用。"""
        reason = partial_retry_reason(
            {"status": "partial", "meta": {"source_truncated": True}}
        )
        self.assertIsNone(reason)

    def test_no_meta_is_not_retryable(self):
        """測試無 meta 時判為重抓無用。"""
        self.assertIsNone(partial_retry_reason({"status": "partial"}))

    def test_mixed_meta_is_retryable(self):
        """測試同時有硬上限與暫時性失敗時仍判為值得重抓。"""
        reason = partial_retry_reason(
            {
                "status": "partial",
                "meta": {"source_truncated": True, "detail_failed": 2},
            }
        )
        self.assertIn("detail_failed=2", reason)


class TestCrawDataStatusContract(unittest.TestCase):
    """測試行情類 craw_data 在新契約下的行為。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    @patch("data_upload.base.requests.get")
    def test_error_with_empty_data_raises(self, mock_get):
        """測試 status=error 且 data 為空時拋出 NetworkError。

        這是新契約下最關鍵的分支：舊契約靠 KeyError 得知失敗，新契約
        失敗也會回 data: []，若不判讀 status 會被當成「當日無資料」。
        """
        mock_get.return_value = _response(
            {"date": "2026-08-16", "status": "error", "data": [],
             "message": "來源連線失敗", "error": "來源連線失敗"}
        )

        with self.assertRaises(NetworkError):
            self.uploader.craw_data("2026-08-16")

    @patch("data_upload.base.requests.get")
    def test_empty_status_returns_empty_df(self, mock_get):
        """測試 status=empty 時回傳空 DataFrame（非交易日的正常情形）。"""
        mock_get.return_value = _response(
            {"date": "2026-08-16", "status": "empty", "data": []}
        )

        df = self.uploader.craw_data("2026-08-16")

        self.assertTrue(df.empty)

    @patch("data_upload.base.requests.get")
    def test_partial_raises_network_error(self, mock_get):
        """測試 status=partial 時拋出 NetworkError 且不回傳部分資料。

        `DailyPrice` 為 append 寫入且無去重，存入部分資料會在重抓時
        產生重複列，故行情類一律丟棄重抓。
        """
        mock_get.return_value = _response(
            {"date": "2026-08-14", "status": "partial",
             "data": [{"SecurityCode": "2330", "Value": 1.0}]}
        )

        with self.assertRaises(NetworkError):
            self.uploader.craw_data("2026-08-14")

    @patch("data_upload.base.requests.get")
    def test_out_of_range_raises_out_of_range_error(self, mock_get):
        """測試 status=out_of_range 時拋出 OutOfRangeError。"""
        mock_get.return_value = _response(
            {"date": "1990-01-01", "status": "out_of_range", "data": [],
             "message": "超出可回溯範圍"}
        )

        with self.assertRaises(OutOfRangeError):
            self.uploader.craw_data("1990-01-01")

    @patch("data_upload.base.requests.get")
    def test_ok_returns_dataframe(self, mock_get):
        """測試 status=ok 時正常回傳 DataFrame。"""
        mock_get.return_value = _response(
            {"date": "2026-08-14", "status": "ok",
             "data": [{"SecurityCode": "2330", "Value": 1.0}]}
        )

        df = self.uploader.craw_data("2026-08-14")

        self.assertEqual(len(df), 1)

    @patch("data_upload.base.requests.get")
    def test_legacy_response_without_status(self, mock_get):
        """測試舊版爬蟲回應（無 status）仍可正常運作。"""
        mock_get.return_value = _response(
            {"date": "2026-08-14",
             "data": [{"SecurityCode": "2330", "Value": 1.0}]}
        )

        df = self.uploader.craw_data("2026-08-14")

        self.assertEqual(len(df), 1)

    @patch("data_upload.base.requests.get")
    def test_legacy_missing_data_key_still_raises(self, mock_get):
        """測試舊版失敗回應（無 data 鍵）仍拋出 CrawlError。"""
        mock_get.return_value = _response({"error": "not found"})

        with self.assertRaises(CrawlError):
            self.uploader.craw_data("2026-08-16")


class TestUploadNeverMarksLedgerOnFailure(unittest.TestCase):
    """測試失敗時絕不寫入 UploadDate 帳本（本次要防的回歸）。"""

    def setUp(self):
        """初始化測試環境（該日期尚未上傳）。"""
        self.mock_conn = MagicMock()
        self.mock_conn.execute.return_value.scalar.return_value = 0
        self.uploader = ConcreteUploader(self.mock_conn)

    def _ledger_writes(self):
        """取出所有「寫入」帳本表的 SQL（排除 check_date 的 SELECT）。

        Returns:
            list[str]: 對 UploadDate 做 INSERT／UPDATE／DELETE 的 SQL 清單。
        """
        return [
            sql
            for call in self.mock_conn.execute.call_args_list
            if call.args
            for sql in [str(call.args[0])]
            if "UploadDate" in sql
            and any(verb in sql.upper() for verb in ("INSERT", "UPDATE", "DELETE"))
        ]

    @patch("data_upload.base.requests.get")
    def test_error_status_does_not_write_upload_date(self, mock_get):
        """測試 status=error 時不寫入 UploadDate（否則該日永久遮蔽）。"""
        mock_get.return_value = _response(
            {"date": "2026-08-16", "status": "error", "data": [],
             "message": "來源連線失敗"}
        )

        with patch.object(self.uploader, "upload_date") as mock_upload_date:
            with self.assertRaises(NetworkError):
                self.uploader.upload("2026-08-16")
            mock_upload_date.assert_not_called()

        self.assertEqual(self._ledger_writes(), [])

    @patch("data_upload.base.requests.get")
    def test_partial_status_does_not_write_upload_date(self, mock_get):
        """測試 status=partial 時不寫入 UploadDate。"""
        mock_get.return_value = _response(
            {"date": "2026-08-14", "status": "partial",
             "data": [{"SecurityCode": "2330", "Value": 1.0}]}
        )

        with patch.object(self.uploader, "upload_date") as mock_upload_date:
            with self.assertRaises(NetworkError):
                self.uploader.upload("2026-08-14")
            mock_upload_date.assert_not_called()

        self.assertEqual(self._ledger_writes(), [])

    @patch("data_upload.base.requests.get")
    def test_out_of_range_does_not_write_upload_date(self, mock_get):
        """測試 status=out_of_range 時不寫帳本且不向外拋例外。

        `OutOfRangeError` 繼承 `CrawlError`，由 `upload` 攔截後直接返回，
        既不會被誤記為非交易日，也不會進入 retry queue。
        """
        mock_get.return_value = _response(
            {"date": "1990-01-01", "status": "out_of_range", "data": [],
             "message": "超出可回溯範圍"}
        )

        with patch.object(self.uploader, "upload_date") as mock_upload_date:
            self.uploader.upload("1990-01-01")
            mock_upload_date.assert_not_called()

        self.assertEqual(self._ledger_writes(), [])

    @patch("data_upload.base.requests.get")
    def test_empty_status_still_marks_non_trading_day(self, mock_get):
        """測試 status=empty 時仍照舊標記非交易日（既有正確行為不可破壞）。"""
        mock_get.return_value = _response(
            {"date": "2026-08-16", "status": "empty", "data": []}
        )

        with patch.object(self.uploader, "upload_date") as mock_upload_date:
            self.uploader.upload("2026-08-16")
            mock_upload_date.assert_called_once()
            self.assertEqual(mock_upload_date.call_args.args[0], "2026-08-16")
            self.assertTrue(mock_upload_date.call_args.args[1].empty)

    @patch("data_upload.base.requests.get")
    def test_empty_status_writes_open_false(self, mock_get):
        """測試 status=empty 實際寫入的 SQL 為 Open=False。"""
        mock_get.return_value = _response(
            {"date": "2026-08-16", "status": "empty", "data": []}
        )

        self.uploader.upload("2026-08-16")

        writes = self._ledger_writes()
        self.assertEqual(len(writes), 1)
        self.assertIn("False", writes[0])


if __name__ == "__main__":
    unittest.main()

"""partial 已落地筆數回報單元測試模組。

`partial`（抓取不完整）時，已取得的新聞**早已寫入 MySQL 與 NewsContents**，
之後才拋 `SourceError` 排入重試。舊版呼叫端只讀例外訊息、不讀已落地統計，
任務一律回報 `record_count=0`，介面上看起來像「完全沒抓到」，實際上資料在庫裡。
這會讓人誤判成爬蟲全掛而去做多餘的人工補抓。

本模組守住的不變量：**已落地多少就回報多少**。涵蓋三層：

1. `SourceError.partial_result` 能承載統計，且未帶時維持 None（向後相容）。
2. 四支新聞上傳器拋 `SourceError` 時附上真實落地筆數。
3. `web_server` 的時數模式與日期範圍任務把該筆數寫進 `upload_jobs`。

注意：本模組**不涉及**「是否該重試」的契約判讀（`check_crawl_status`、
`_check_incomplete` 的 meta 黑名單），那是另一條軸線，只驗證顯示用的筆數。
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from data_upload.base import SourceError
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

# (來源代號, web_server 內的上傳器類別名稱)
NEWS_SOURCES = [
    ("ctee", "CTEENewsUploader"),
    ("cnyes", "CNYESNewsUploader"),
    ("ptt", "PTTNewsUploader"),
    ("moneyudn", "MoneyUDNNewsUploader"),
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


def _records(count, date="2026-08-16"):
    """建立指定筆數的新聞記錄（四來源欄位聯集，供各上傳器共用）。

    Args:
        count (int): 記錄筆數。
        date (str): 新聞日期。

    Returns:
        list[dict]: 新聞記錄清單。
    """
    return [
        {
            "Date": date, "Time": "10:00:00", "Author": "記者",
            "Head": f"標題{i}", "SubHead": "副標", "HashTag": "tag",
            "url": f"https://example.com/{i}", "Content": "內文",
            "Board": "Stock", "PushCount": 1,
        }
        for i in range(count)
    ]


class TestSourceErrorPartialResult(unittest.TestCase):
    """測試 `SourceError` 承載已落地統計的能力。"""

    def test_defaults_to_none(self):
        """測試未帶統計時為 None（既有拋出點無須逐一改寫）。"""
        error = SourceError("抓取失敗")

        self.assertIsNone(error.partial_result)
        self.assertEqual(str(error), "抓取失敗")

    def test_carries_partial_result(self):
        """測試帶入的統計原樣保留。"""
        error = SourceError("不完整", partial_result={
            "record_count": 7, "file_count": 5,
        })

        self.assertEqual(error.partial_result["record_count"], 7)
        self.assertEqual(error.partial_result["file_count"], 5)

    def test_still_retryable(self):
        """測試新增欄位未動搖「可重試」的繼承語意。"""
        from data_upload.base import NetworkError

        self.assertIsInstance(SourceError("x", {"record_count": 1}),
                              NetworkError)


class TestUploaderAttachesLandedCounts(unittest.TestCase):
    """測試四支上傳器拋 `SourceError` 時附上真實落地筆數。"""

    def test_upload_by_hours_attaches_counts(self):
        """測試時數模式：例外帶回實際寫入的 metadata 與檔案數。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response({
                        "hours": 48, "status": "partial",
                        "data": _records(3),
                        "meta": {"detail_failed": 1},
                    })
                    with patch.object(
                        uploader, "filter_new_records",
                        side_effect=lambda df, d: df,
                    ), patch.object(
                        uploader, "upload_metadata", return_value=3,
                    ), patch.object(
                        uploader, "save_contents", return_value=2,
                    ), patch.object(uploader, "record_uploaded_date"):
                        with self.assertRaises(SourceError) as ctx:
                            uploader.upload_by_hours(48)

                partial = ctx.exception.partial_result
                self.assertIsNotNone(
                    partial, "已落地的筆數未附在例外上，呼叫端只能顯示 0 筆",
                )
                self.assertEqual(partial["record_count"], 3)
                self.assertEqual(partial["file_count"], 2)
                self.assertEqual(partial["hours"], 48)

    def test_upload_attaches_counts(self):
        """測試日期模式：例外帶回實際寫入的 metadata 與檔案數。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response({
                        "date": "2026-08-16", "status": "partial",
                        "data": _records(4),
                        "meta": {"skipped_by_deadline": 2},
                    })
                    with patch.object(
                        uploader, "filter_new_records",
                        side_effect=lambda df, d: df,
                    ), patch.object(
                        uploader, "upload_metadata", return_value=4,
                    ), patch.object(
                        uploader, "save_contents", return_value=4,
                    ), patch.object(uploader, "record_uploaded_date"):
                        with self.assertRaises(SourceError) as ctx:
                            uploader.upload("2026-08-16")

                partial = ctx.exception.partial_result
                self.assertIsNotNone(partial)
                self.assertEqual(partial["record_count"], 4)
                self.assertEqual(partial["file_count"], 4)
                self.assertEqual(partial["date"], "2026-08-16")

    def test_zero_landed_reports_zero(self):
        """測試真的 0 筆落地時如實回報 0（不可反過來虛報）。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response({
                        "hours": 48, "status": "partial", "data": [],
                        "meta": {"skipped_by_deadline": 3},
                    })
                    with self.assertRaises(SourceError) as ctx:
                        uploader.upload_by_hours(48)

                self.assertEqual(
                    ctx.exception.partial_result["record_count"], 0
                )

    def test_all_records_exist_reports_zero_new(self):
        """測試記錄皆已存在時回報 0 筆新增（本輪確實沒新寫入）。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response({
                        "date": "2026-08-16", "status": "partial",
                        "data": _records(2),
                        "meta": {"detail_failed": 1},
                    })
                    with patch.object(
                        uploader, "filter_new_records",
                        return_value=pd.DataFrame(),
                    ):
                        with self.assertRaises(SourceError) as ctx:
                            uploader.upload("2026-08-16")

                self.assertEqual(
                    ctx.exception.partial_result["record_count"], 0
                )


class TestHoursJobReportsPartialCounts(unittest.TestCase):
    """測試時數模式任務把已落地筆數寫進 `upload_jobs`。"""

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server

        self.web_server = web_server
        web_server.upload_jobs.clear()

    def _run_hours_job(self, source, cls_name, error):
        """以指定例外執行某來源的時數模式任務。

        Args:
            source (str): 來源代號（ctee／cnyes／ptt／moneyudn）。
            cls_name (str): web_server 內的上傳器類別名稱。
            error (Exception): 上傳器要拋出的例外。

        Returns:
            dict: 任務執行後的 `upload_jobs` 項目。
        """
        job_id = f"job-{source}"
        self.web_server.upload_jobs[job_id] = {
            "job_id": job_id, "type": f"{source}_news", "status": "queued",
            "record_count": 0, "file_count": 0,
        }
        job_func = getattr(self.web_server, f"run_{source}_news_hours_job")

        with patch("routers.MySQLRouter"), \
                patch.object(self.web_server, cls_name) as mock_cls, \
                patch.object(self.web_server, "retry_queue") as mock_retry:
            mock_cls.return_value.upload_by_hours.side_effect = error
            job_func(job_id, 48)

        self.assertTrue(
            mock_retry.add.called, "partial 仍須排入重試以補齊剩餘資料",
        )
        return self.web_server.upload_jobs[job_id]

    def test_reports_landed_counts(self):
        """測試 partial 時回報實際落地筆數，而非固定 0。"""
        for source, cls_name in NEWS_SOURCES:
            with self.subTest(source=source):
                job = self._run_hours_job(
                    source, cls_name,
                    SourceError("抓取不完整", partial_result={
                        "hours": 48, "record_count": 12, "file_count": 9,
                    }),
                )

                self.assertEqual(job["status"], "failed")
                self.assertEqual(
                    job["record_count"], 12,
                    "已落地 12 筆卻回報 0，會被誤判成完全沒抓到",
                )
                self.assertEqual(job["file_count"], 9)

    def test_handles_missing_partial_result(self):
        """測試未帶統計的 SourceError 不致炸開，維持 0 筆。"""
        for source, cls_name in NEWS_SOURCES:
            with self.subTest(source=source):
                job = self._run_hours_job(
                    source, cls_name, SourceError("抓取不完整"),
                )

                self.assertEqual(job["status"], "failed")
                self.assertEqual(job["record_count"], 0)
                self.assertEqual(job["file_count"], 0)

    def test_pure_network_error_reports_zero(self):
        """測試純網路失敗（連不上爬蟲）維持 0 筆，未誤植筆數。"""
        from data_upload.base import NetworkError

        for source, cls_name in NEWS_SOURCES:
            with self.subTest(source=source):
                job = self._run_hours_job(
                    source, cls_name, NetworkError("連線逾時"),
                )

                self.assertEqual(job["status"], "failed")
                self.assertEqual(job["record_count"], 0)


class TestDateRangeJobAccumulatesPartialCounts(unittest.TestCase):
    """測試日期範圍任務把各日已落地筆數累加進總數。"""

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server

        self.web_server = web_server
        web_server.upload_jobs.clear()

    def _run_range_job(self, source, cls_name, side_effect,
                       start="2026-08-15", end="2026-08-16"):
        """以指定逐日結果執行某來源的日期範圍任務。

        Args:
            source (str): 來源代號。
            cls_name (str): web_server 內的上傳器類別名稱。
            side_effect (list): `upload` 逐日的回傳值或例外。
            start (str): 起始日期。
            end (str): 結束日期。

        Returns:
            dict: 任務執行後的 `upload_jobs` 項目。
        """
        job_id = f"range-{source}"
        self.web_server.upload_jobs[job_id] = {
            "job_id": job_id, "type": f"{source}_news", "status": "queued",
            "record_count": 0, "file_count": 0,
        }
        job_func = getattr(self.web_server, f"run_{source}_news_upload_job")

        with patch("routers.MySQLRouter"), \
                patch.object(self.web_server, cls_name) as mock_cls:
            mock_cls.return_value.upload.side_effect = side_effect
            job_func(job_id, start, end)

        return self.web_server.upload_jobs[job_id]

    def test_partial_day_counts_into_total(self):
        """測試不完整的那一天已落地筆數計入總數，且仍標記為失敗待重抓。"""
        for source, cls_name in NEWS_SOURCES:
            with self.subTest(source=source):
                job = self._run_range_job(source, cls_name, [
                    {"date": "2026-08-15", "record_count": 10,
                     "file_count": 10},
                    SourceError("抓取不完整", partial_result={
                        "date": "2026-08-16", "record_count": 4,
                        "file_count": 3,
                    }),
                ])

                self.assertEqual(
                    job["record_count"], 14,
                    "第二天已落地 4 筆未計入，總數會少報",
                )
                self.assertEqual(job["file_count"], 13)
                # 仍須列為失敗日，之後重抓補齊剩餘資料。
                self.assertEqual(job["status"], "failed")
                self.assertIn("2026-08-16", job["error"])

    def test_partial_without_result_keeps_total(self):
        """測試未帶統計時總數不變且不拋例外（向後相容）。"""
        for source, cls_name in NEWS_SOURCES:
            with self.subTest(source=source):
                job = self._run_range_job(source, cls_name, [
                    {"date": "2026-08-15", "record_count": 10,
                     "file_count": 10},
                    SourceError("抓取不完整"),
                ])

                self.assertEqual(job["record_count"], 10)
                self.assertEqual(job["status"], "failed")


if __name__ == "__main__":
    unittest.main()

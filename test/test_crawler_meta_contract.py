"""爬蟲 v2.14.0 `meta` 契約適配的單元測試模組。

爬蟲自 v2.14.0 起以 `meta.retryable` 作為「重抓有沒有機會補回來」的**單一判準**，
並附上 `retryable_reasons`／`non_retryable_reasons`／`detail_failed_ratio` 等細節。
本模組守住四條不變量：

1. `retryable` 存在時以它為主判準，並以 `detail_failed_ratio` 門檻取代舊的
   「1 篇全文失敗就重抓」——否則 PTT／MoneyUDN 會天天排一次同步重跑 48 小時窗。
2. 舊版爬蟲回應（不帶 `retryable`）維持**預設重抓**，不可因為「沒有 retryable」
   就當成不重抓，那會把失敗誤記成空而永久漏抓。
3. CNYES 翻頁上限（`source_truncated`）只告警、不重抓。
4. `partial` 但 `data` 為空時：既不當成有資料上傳，也**絕不**寫入已上傳帳本
   （寫了該日就永久遮蔽，補不回來）。
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from data_upload.base import (
    DETAIL_FAILED_RETRY_RATIO,
    SourceError,
    partial_retry_reason,
    partial_skip_note,
)
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

# 四支上傳器共用的單筆新聞樣本（各來源欄位取聯集，多餘欄位不影響驗證）。
SAMPLE_RECORD = {
    "Date": "2026-08-16", "Time": "10:00:00", "Author": "記者",
    "Head": "標題", "SubHead": "副標", "HashTag": "tag",
    "url": "https://example.com/1", "Content": "內文",
    "Board": "Stock", "PushCount": 1,
}


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


def _partial(meta, data=None, date="2026-08-16"):
    """建立 `status=partial` 的爬蟲回應。

    Args:
        meta (dict | None): 回應的 meta 物件。
        data (list | None): 回應的 data 陣列，預設空陣列。
        date (str): 回應標示的日期。

    Returns:
        dict: 爬蟲回應 JSON。
    """
    payload = {"date": date, "hours": 48, "status": "partial",
               "data": data if data is not None else []}
    if meta is not None:
        payload["meta"] = meta
    return payload


class TestRetryableIsPrimaryCriterion(unittest.TestCase):
    """測試 `meta.retryable` 為重抓與否的單一判準。"""

    def test_retryable_false_never_retries(self):
        """測試 `retryable=False` 一律不重抓，即使 meta 帶有暫時性欄位。

        這是「單一判準」的關鍵：舊邏輯只要看到 `detail_failed`／`list_failed`
        就重抓，會與爬蟲已彙整好的結論打架。爬蟲說重抓沒用就是沒用。
        """
        metas = {
            "純硬限制": {
                "retryable": False,
                "non_retryable_reasons": ["source_truncated"],
                "source_truncated": True,
            },
            "帶暫時性欄位但已判定不可重抓": {
                "retryable": False,
                "non_retryable_reasons": ["out_of_range"],
                "detail_failed": 5,
                "list_failed": True,
            },
            # 舊欄位 `source_truncated` 單獨出現時同樣算硬限制佐證，
            # 不因為少了 `non_retryable_reasons` 就退回保守重抓。
            "只有 source_truncated 佐證": {
                "retryable": False,
                "source_truncated": True,
            },
        }
        for label, meta in metas.items():
            with self.subTest(meta=label):
                self.assertIsNone(partial_retry_reason(_partial(meta)))

    def test_retryable_true_without_reasons_still_retries(self):
        """測試標記可重抓但沒給成因時保守重抓（不臆測）。"""
        reason = partial_retry_reason(_partial({"retryable": True}))
        self.assertIsNotNone(reason)

    def test_retryable_false_without_evidence_still_retries(self):
        """測試 `retryable=False` 但無硬限制佐證時仍保守重抓。

        爬蟲現行邏輯（`retryable = bool(retryable_reasons)`）不會產生這種
        退化回應，但「不重抓」是唯一會把失敗永久遮蔽的方向，缺乏佐證時
        寧可多跑一次。此防線只把「不重抓」翻成「重抓」，不會反向削弱
        `retryable` 的單一判準地位。
        """
        metas = {
            "兩個成因清單皆空": {"retryable": False},
            "只有空清單": {
                "retryable": False,
                "retryable_reasons": [],
                "non_retryable_reasons": [],
            },
            # 空字串不算佐證：拿掉過濾會讓這筆從「重抓」翻成「不重抓」，
            # 正是本 repo 最怕的危險方向，故明確釘住。
            "清單只有空字串": {
                "retryable": False,
                "non_retryable_reasons": [""],
            },
        }
        for label, meta in metas.items():
            with self.subTest(meta=label):
                self.assertIsNotNone(partial_retry_reason(_partial(meta)))

    def test_unknown_reason_code_still_retries(self):
        """測試遇到未知成因代碼（爬蟲日後新增）時保守重抓。

        白名單式判讀會把未知成因當成「不必重抓」而靜默漏抓，
        與 `check_crawl_status` 對未知狀態的保守原則相衝突。
        """
        reason = partial_retry_reason(_partial({
            "retryable": True,
            "retryable_reasons": ["rate_limited"],
        }))
        self.assertIsNotNone(reason)
        self.assertIn("rate_limited", reason)

    def test_skip_note_absent_when_retrying(self):
        """測試判定要重抓時不產生「不重抓理由」，避免 log 自相矛盾。"""
        self.assertIsNone(partial_skip_note(_partial({
            "retryable": True,
            "retryable_reasons": ["list_failed"],
        })))


class TestDetailFailedThreshold(unittest.TestCase):
    """測試「僅全文抓取失敗」時以失敗率門檻決定是否重抓。"""

    def _detail_meta(self, ratio, failed=None, total=50):
        """建立僅含 `detail_failed` 成因的 meta。

        Args:
            ratio (float | None): 失敗率；None 代表爬蟲未提供。
            failed (int | None): 失敗篇數，預設由 ratio 推算。
            total (int): 嘗試抓全文的總篇數。

        Returns:
            dict: meta 物件。
        """
        meta = {
            "retryable": True,
            "retryable_reasons": ["detail_failed"],
            "detail_failed": (
                failed if failed is not None else int((ratio or 0) * total)
            ),
            "detail_total": total,
        }
        if ratio is not None:
            meta["detail_failed_ratio"] = ratio
        return meta

    def test_below_threshold_does_not_retry(self):
        """測試零星全文失敗不排重抓（隔日 48 小時窗會自然補上）。

        這是本次最主要的回歸風險：舊契約下 PTT／MoneyUDN 抓漏也回 `ok`，
        新契約改回 `partial`，若沿用「1 篇失敗就重抓」，等於天天排一次
        同步重跑整個 48 小時窗，會把當晚的排程窗整批往後推。
        """
        self.assertIsNone(partial_retry_reason(_partial(
            self._detail_meta(0.04, failed=2)
        )))

    def test_below_threshold_skip_note_explains_why(self):
        """測試不重抓時的 log 理由據實說明是「低於門檻」而非「來源硬限制」。"""
        note = partial_skip_note(_partial(self._detail_meta(0.04, failed=2)))
        self.assertIsNotNone(note)
        self.assertIn("0.04", note)
        self.assertIn(str(DETAIL_FAILED_RETRY_RATIO), note)

    def test_at_threshold_retries(self):
        """測試失敗率剛好等於門檻即重抓（門檻採 >=，邊界不漏接）。"""
        reason = partial_retry_reason(_partial(
            self._detail_meta(DETAIL_FAILED_RETRY_RATIO)
        ))
        self.assertIsNotNone(reason)

    def test_above_threshold_retries(self):
        """測試失敗率高於門檻時重抓（多半是來源改版或擋人）。"""
        reason = partial_retry_reason(_partial(self._detail_meta(0.24)))
        self.assertIsNotNone(reason)
        self.assertIn("0.24", reason)

    def test_missing_ratio_retries(self):
        """測試爬蟲未提供失敗率時保守重抓（無從套門檻）。"""
        reason = partial_retry_reason(_partial(
            self._detail_meta(None, failed=1)
        ))
        self.assertIsNotNone(reason)

    def test_non_numeric_ratio_retries(self):
        """測試失敗率型別異常時保守重抓，而非靜默當成不必重抓。"""
        meta = self._detail_meta(None, failed=1)
        meta["detail_failed_ratio"] = "0.5"
        self.assertIsNotNone(partial_retry_reason(_partial(meta)))

    def test_unbounded_reason_bypasses_threshold(self):
        """測試同時有列表失敗時無視門檻直接重抓。

        列表失敗代表「連有哪些文章都不知道」，損失無上限，
        不能拿 `detail_failed_ratio` 這種只涵蓋已知文章的比率去衡量。
        """
        reason = partial_retry_reason(_partial({
            "retryable": True,
            "retryable_reasons": ["list_failed", "detail_failed"],
            "list_failed": True,
            "detail_failed": 1,
            "detail_total": 50,
            "detail_failed_ratio": 0.02,
        }))
        self.assertIsNotNone(reason)
        self.assertIn("list_failed", reason)

    def test_deadline_and_crawl_failed_always_retry(self):
        """測試逾時收工與爬蟲整體失敗一律重抓（損失範圍未知）。"""
        for code in ("deadline", "crawl_failed"):
            with self.subTest(reason=code):
                reason = partial_retry_reason(_partial({
                    "retryable": True, "retryable_reasons": [code],
                }))
                self.assertIsNotNone(reason)
                self.assertIn(code, reason)


class TestListFailedHasExplicitReason(unittest.TestCase):
    """測試 `list_failed` 有明確的重試原因字串（原工作項目 5）。"""

    def test_reason_names_list_failed(self):
        """測試重試原因直接寫出 `list_failed`，而非籠統的預設訊息。"""
        reason = partial_retry_reason(_partial({
            "retryable": True,
            "retryable_reasons": ["list_failed"],
            "list_failed": True,
        }))
        self.assertEqual(reason, "list_failed")

    def test_source_error_message_carries_reason(self):
        """測試四支上傳器拋出的例外訊息含 `list_failed`，便於排查。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(_partial({
                        "retryable": True,
                        "retryable_reasons": ["list_failed"],
                        "list_failed": True,
                    }))
                    with self.assertRaises(SourceError) as ctx:
                        uploader.upload_by_hours(48)
                self.assertIn("list_failed", str(ctx.exception))


class TestLegacyResponsesStillDefaultToRetry(unittest.TestCase):
    """測試舊版爬蟲回應（不帶 `retryable`）維持既有預設重抓行為。

    向後相容的底線：新增判準只能讓「該重抓的更明確」，不能讓「沒講清楚的」
    變成不重抓——那正是把失敗誤記成空、使該日永久遮蔽的老毛病。
    """

    def test_meta_absent_defaults_to_retry(self):
        """測試完全沒有 meta 時仍判定重抓。"""
        self.assertIsNotNone(partial_retry_reason(_partial(None)))

    def test_empty_meta_defaults_to_retry(self):
        """測試 meta 為空物件時仍判定重抓。"""
        self.assertIsNotNone(partial_retry_reason(_partial({})))

    def test_legacy_cnyes_page_capped_defaults_to_retry(self):
        """測試舊版 CNYES 只帶 `fetched`／`pages` 的回應仍判定重抓。"""
        self.assertIsNotNone(partial_retry_reason(_partial({
            "fetched": 50, "pages": 51,
        })))

    def test_legacy_detail_failed_not_subject_to_threshold(self):
        """測試舊版 `detail_failed` 不套用新門檻（舊版沒有失敗率可判）。

        舊契約沒有 `detail_total`／`detail_failed_ratio`，1 篇失敗與 100 篇
        失敗長得一樣；貿然套門檻會把大量失敗誤判成可容忍。
        """
        self.assertIsNotNone(partial_retry_reason(_partial({
            "detail_failed": 1,
        })))

    def test_legacy_source_truncated_still_warn_only(self):
        """測試舊版 `source_truncated` 仍只告警不重抓（既有黑名單保留）。"""
        self.assertIsNone(partial_retry_reason(_partial({
            "source_truncated": True,
        })))

    def test_non_dict_response_defaults_to_retry(self):
        """測試非制式回應（非 dict）保守判定重抓。"""
        self.assertIsNotNone(partial_retry_reason(["unexpected"]))


class TestCnyesPageCapIsWarnOnly(unittest.TestCase):
    """測試 CNYES 翻頁上限在新契約下自動變成只告警（原工作項目 1）。

    舊版 `page_capped` 只帶 `fetched`／`pages`，落到預設重抓；新版改帶
    `source_truncated`＋`retryable: False`，應直接命中既有黑名單。本測試
    鎖住此行為，避免日後把它改回「每小時重抓一次拿不到的資料」。
    """

    def test_new_contract_page_cap_does_not_retry(self):
        """測試新契約的翻頁上限回應不拋例外、資料照樣落地。"""
        uploader = CNYESNewsUploader(MagicMock(), "crawler:6738")
        with patch("data_upload.cnyes_news.requests.get") as mock_get:
            mock_get.return_value = _response(_partial(
                {
                    "retryable": False,
                    "non_retryable_reasons": ["source_truncated"],
                    "source_truncated": True,
                    "fetched": 50,
                    "pages": 51,
                },
                data=[SAMPLE_RECORD],
            ))
            with patch.object(
                uploader, "filter_new_records", side_effect=lambda df, d: df,
            ), patch.object(
                uploader, "upload_metadata", return_value=1,
            ), patch.object(
                uploader, "save_contents", return_value=1,
            ), patch.object(
                uploader, "record_uploaded_date",
            ):
                result = uploader.upload_by_hours(48)

        self.assertEqual(result["record_count"], 1)

    def test_legacy_page_cap_still_retries(self):
        """測試舊版翻頁上限回應仍會重抓（證明差異來自新欄位而非改壞判讀）。"""
        uploader = CNYESNewsUploader(MagicMock(), "crawler:6738")
        with patch("data_upload.cnyes_news.requests.get") as mock_get:
            mock_get.return_value = _response(_partial(
                {"fetched": 50, "pages": 51}, data=[SAMPLE_RECORD],
            ))
            with patch.object(
                uploader, "filter_new_records", side_effect=lambda df, d: df,
            ), patch.object(
                uploader, "upload_metadata", return_value=1,
            ), patch.object(
                uploader, "save_contents", return_value=1,
            ), patch.object(
                uploader, "record_uploaded_date",
            ):
                with self.assertRaises(SourceError):
                    uploader.upload_by_hours(48)


class TestPartialWithEmptyDataNeverWritesLedger(unittest.TestCase):
    """測試「`partial` 但 `data` 為空」的處理（原工作項目 4）。

    爬蟲把三種 0 筆情境收斂進 `partial`／`error`（CNYES／MoneyUDN 翻頁截斷
    且 0 筆、PTT 列表中途失敗且 0 筆）。下游必須：既不把空 data 當成有資料
    去上傳，也**絕不**寫入 `*Uploaded` 帳本——寫了之後該日就再也不會被檢查，
    真實新聞永久遺失。
    """

    RETRYABLE_META = {
        "retryable": True,
        "retryable_reasons": ["list_failed"],
        "list_failed": True,
    }
    NON_RETRYABLE_META = {
        "retryable": False,
        "non_retryable_reasons": ["source_truncated"],
        "source_truncated": True,
    }

    def _run(self, uploader, target, meta, by_hours):
        """以指定 meta 執行一次上傳，回傳落地相關的 mock。

        Args:
            uploader: 上傳器實例。
            target (str): requests.get 的 patch 目標。
            meta (dict): 回應的 meta 物件。
            by_hours (bool): True 走時數模式，False 走日期模式。

        Returns:
            tuple: (執行結果或 None, upload_metadata mock,
                record_uploaded_date mock, 例外或 None)。
        """
        with patch(target) as mock_get:
            mock_get.return_value = _response(_partial(meta))
            with patch.object(
                uploader, "upload_metadata", return_value=0,
            ) as mock_upload, patch.object(
                uploader, "record_uploaded_date",
            ) as mock_ledger:
                try:
                    if by_hours:
                        result = uploader.upload_by_hours(48)
                    else:
                        result = uploader.upload("2026-08-16")
                except SourceError as e:
                    return None, mock_upload, mock_ledger, e
                return result, mock_upload, mock_ledger, None

    def test_retryable_empty_partial_retries_without_ledger(self):
        """測試可重抓的 0 筆 `partial`：拋例外排重試，且不寫帳本、不上傳。"""
        for cls, target in UPLOADERS:
            for by_hours in (False, True):
                with self.subTest(uploader=cls.__name__, hours=by_hours):
                    uploader = cls(MagicMock(), "crawler:6738")
                    result, mock_upload, mock_ledger, exc = self._run(
                        uploader, target, self.RETRYABLE_META, by_hours,
                    )
                    self.assertIsNotNone(exc, "應拋 SourceError 排入重試")
                    self.assertIsNone(result)
                    mock_upload.assert_not_called()
                    mock_ledger.assert_not_called()

    def test_non_retryable_empty_partial_writes_no_ledger(self):
        """測試不可重抓的 0 筆 `partial`：只告警，但同樣不得寫入帳本。

        「重抓也拿不到」不等於「當日確實沒有新聞」——來源硬上限截斷時，
        被截掉的部分是真實存在的資料。寫進帳本等於永久宣告該日已處理完畢。
        """
        for cls, target in UPLOADERS:
            for by_hours in (False, True):
                with self.subTest(uploader=cls.__name__, hours=by_hours):
                    uploader = cls(MagicMock(), "crawler:6738")
                    result, mock_upload, mock_ledger, exc = self._run(
                        uploader, target, self.NON_RETRYABLE_META, by_hours,
                    )
                    self.assertIsNone(exc, "來源硬限制不應排入重試")
                    self.assertEqual(result["record_count"], 0)
                    mock_upload.assert_not_called()
                    mock_ledger.assert_not_called()

    def test_error_status_with_empty_data_writes_no_ledger(self):
        """測試 0 筆的 `error`（PTT 列表中途失敗）拋例外且不寫帳本。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response({
                        "hours": 48, "status": "error", "data": [],
                        "message": "列表抓取失敗",
                        "meta": {"retryable": True,
                                 "retryable_reasons": ["list_failed"]},
                    })
                    with patch.object(
                        uploader, "record_uploaded_date",
                    ) as mock_ledger:
                        with self.assertRaises(SourceError):
                            uploader.upload_by_hours(48)
                mock_ledger.assert_not_called()


class TestDetailFailedThresholdEndToEnd(unittest.TestCase):
    """測試門檻在四支上傳器上的實際效果（原工作項目 2 的核心）。"""

    def _upload_with_ratio(self, cls, target, ratio, failed):
        """以指定失敗率跑一次時數模式上傳。

        Args:
            cls (type): 上傳器類別。
            target (str): requests.get 的 patch 目標。
            ratio (float): `detail_failed_ratio`。
            failed (int): 失敗篇數。

        Returns:
            tuple: (結果或 None, record_uploaded_date mock, 例外或 None)。
        """
        uploader = cls(MagicMock(), "crawler:6738")
        meta = {
            "retryable": True,
            "retryable_reasons": ["detail_failed"],
            "detail_failed": failed,
            "detail_total": 50,
            "detail_failed_ratio": ratio,
        }
        with patch(target) as mock_get:
            mock_get.return_value = _response(
                _partial(meta, data=[SAMPLE_RECORD])
            )
            with patch.object(
                uploader, "filter_new_records", side_effect=lambda df, d: df,
            ), patch.object(
                uploader, "upload_metadata", return_value=1,
            ), patch.object(
                uploader, "save_contents", return_value=1,
            ), patch.object(
                uploader, "record_uploaded_date",
            ) as mock_ledger:
                try:
                    return uploader.upload_by_hours(48), mock_ledger, None
                except SourceError as e:
                    return None, mock_ledger, e

    def test_low_ratio_lands_data_without_queueing_retry(self):
        """測試零星全文失敗時資料照樣落地，且不排入重試佇列。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                result, mock_ledger, exc = self._upload_with_ratio(
                    cls, target, 0.02, 1,
                )
                self.assertIsNone(exc, "低於門檻不應排重試")
                self.assertEqual(result["record_count"], 1)
                mock_ledger.assert_called_once()

    def test_high_ratio_queues_retry_with_landed_counts(self):
        """測試失敗率達門檻時排重試，且例外帶著已落地筆數。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                result, _, exc = self._upload_with_ratio(
                    cls, target, 0.3, 15,
                )
                self.assertIsNotNone(exc, "達門檻應排重試")
                self.assertIsNone(result)
                self.assertEqual(exc.partial_result["record_count"], 1)


class TestPartialDoesNotShortCircuitDataHandling(unittest.TestCase):
    """測試 `partial` 不影響既有的資料處理流程（防呆）。"""

    def test_partial_with_records_still_uploads(self):
        """測試 `partial` 且有資料時仍完整走完上傳流程。

        重抓決策只影響「要不要排重試」，不可讓已取得的資料被丟掉——
        新聞來源回溯窗有限，丟掉就補不回。
        """
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                uploader = cls(MagicMock(), "crawler:6738")
                with patch(target) as mock_get:
                    mock_get.return_value = _response(_partial(
                        {"retryable": True,
                         "retryable_reasons": ["deadline"]},
                        data=[SAMPLE_RECORD],
                    ))
                    with patch.object(
                        uploader, "filter_new_records",
                        side_effect=lambda df, d: df,
                    ), patch.object(
                        uploader, "upload_metadata", return_value=1,
                    ) as mock_upload, patch.object(
                        uploader, "save_contents", return_value=1,
                    ) as mock_save, patch.object(
                        uploader, "record_uploaded_date",
                    ):
                        with self.assertRaises(SourceError):
                            uploader.upload("2026-08-16")
                # 例外必須發生在資料寫入「之後」。
                mock_upload.assert_called_once()
                mock_save.assert_called_once()

    def test_filter_new_records_receives_dataframe(self):
        """測試 `partial` 的 data 仍被正常轉成 DataFrame 交給去重流程。"""
        uploader = CTEENewsUploader(MagicMock(), "crawler:6738")
        with patch("data_upload.ctee_news.requests.get") as mock_get:
            mock_get.return_value = _response(_partial(
                {"retryable": False,
                 "non_retryable_reasons": ["source_truncated"]},
                data=[SAMPLE_RECORD],
            ))
            with patch.object(
                uploader, "filter_new_records",
                return_value=pd.DataFrame(),
            ) as mock_filter, patch.object(
                uploader, "record_uploaded_date",
            ) as mock_ledger:
                uploader.upload("2026-08-16")

        passed_df = mock_filter.call_args[0][0]
        self.assertIsInstance(passed_df, pd.DataFrame)
        self.assertEqual(len(passed_df), 1)
        # 去重後全數已存在 → 什麼都沒寫，帳本也不可寫。
        mock_ledger.assert_not_called()


class TestSkipNoteReachesWarningLog(unittest.TestCase):
    """測試「不重抓」的理由確實寫進 warning log（守住接線）。

    `partial_skip_note()` 本身有單元測試，但四支上傳器把它接到
    `logger.warning` 的那條線若被改回固定字串，不會有任何測試轉紅。
    不重抓有兩種成因——「來源硬限制，重抓也拿不到」與「低於門檻，缺的留待
    隔日補回」——語意天差地遠，log 分不出來就等於排查時被誤導。
    """

    TRUNCATED_META = {
        "retryable": False,
        "non_retryable_reasons": ["source_truncated"],
        "source_truncated": True,
    }
    BELOW_THRESHOLD_META = {
        "retryable": True,
        "retryable_reasons": ["detail_failed"],
        "detail_failed": 1,
        "detail_total": 100,
        "detail_failed_ratio": 0.01,
    }

    def _warning_text(self, cls, target, meta):
        """跑一次上傳並取回 warning log 全文。

        Args:
            cls: 上傳器類別。
            target (str): requests.get 的 patch 目標。
            meta (dict): 回應的 meta 物件。

        Returns:
            str: 該模組 logger 輸出的 warning 訊息全文。
        """
        uploader = cls(MagicMock(), "crawler:6738")
        with patch(target) as mock_get:
            mock_get.return_value = _response(_partial(meta))
            with patch.object(uploader, "record_uploaded_date"):
                with self.assertLogs(cls.__module__, level="WARNING") as cm:
                    uploader.upload_by_hours(48)
        return "\n".join(cm.output)

    def test_source_truncated_warning_says_hard_limit(self):
        """測試來源硬限制的告警寫明「重抓也拿不到」。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                text = self._warning_text(cls, target, self.TRUNCATED_META)
                self.assertIn("來源硬限制", text)
                self.assertIn("source_truncated", text)

    def test_below_threshold_warning_says_threshold(self):
        """測試低於門檻的告警寫明失敗率與門檻，而非硬限制。"""
        for cls, target in UPLOADERS:
            with self.subTest(uploader=cls.__name__):
                text = self._warning_text(
                    cls, target, self.BELOW_THRESHOLD_META,
                )
                self.assertIn("低於門檻", text)
                self.assertIn(str(DETAIL_FAILED_RETRY_RATIO), text)
                self.assertNotIn("來源硬限制", text)


if __name__ == "__main__":
    unittest.main()

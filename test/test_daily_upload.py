"""每日排程上傳模組單元測試。"""

import unittest
from unittest.mock import patch, MagicMock, call
import datetime


class TestGetMissingDates(unittest.TestCase):
    """測試 get_missing_dates 函式。"""

    @patch("routers.MySQLRouter")
    def test_no_missing_dates(self, mock_router_cls):
        """測試所有日期皆已上傳時回傳空清單。"""
        import DailyUpload

        today = datetime.datetime.now()
        date_list = [
            (today - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(5)
        ]
        uploaded = [
            (datetime.datetime.strptime(d, "%Y-%m-%d"),) for d in date_list
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = uploaded
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.get_missing_dates("TWSE", days=5)

        self.assertEqual(result, [])
        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_some_missing_dates(self, mock_router_cls):
        """測試部分日期未上傳時回傳缺漏日期。"""
        import DailyUpload

        today = datetime.datetime.now()
        # 只有今天和昨天已上傳
        uploaded = [
            (today,),
            (today - datetime.timedelta(days=1),),
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = uploaded
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.get_missing_dates("TWSE", days=5)

        # 應有 3 個缺漏日期 (days 2, 3, 4)
        self.assertEqual(len(result), 3)
        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_all_missing_dates(self, mock_router_cls):
        """測試完全沒有上傳紀錄時回傳全部日期。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.get_missing_dates("TWSE", days=3)

        self.assertEqual(len(result), 3)
        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_uses_correct_db_name(self, mock_router_cls):
        """測試使用正確的資料庫名稱建立連線。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.get_missing_dates("TPEX", days=1)

        mock_router_cls.assert_called_once_with(
            DailyUpload.HOST,
            DailyUpload.USER,
            DailyUpload.PASSWORD,
            "TPEX",
        )

    @patch("routers.MySQLRouter")
    def test_faoi_maps_to_twse(self, mock_router_cls):
        """測試 FAOI 連線至 TWSE 資料庫並查詢 FAOIUploadDate。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.get_missing_dates("FAOI", days=1)

        mock_router_cls.assert_called_once_with(
            DailyUpload.HOST,
            DailyUpload.USER,
            DailyUpload.PASSWORD,
            "TWSE",
        )
        call_args = mock_conn.execute.call_args[0][0]
        self.assertIn("FAOIUploadDate", str(call_args))

    @patch("routers.MySQLRouter")
    def test_mgts_maps_to_twse(self, mock_router_cls):
        """測試 MGTS 連線至 TWSE 資料庫並查詢 MGTSUploadDate。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.get_missing_dates("MGTS", days=1)

        mock_router_cls.assert_called_once_with(
            DailyUpload.HOST,
            DailyUpload.USER,
            DailyUpload.PASSWORD,
            "TWSE",
        )
        call_args = mock_conn.execute.call_args[0][0]
        self.assertIn("MGTSUploadDate", str(call_args))


class TestDailyCraw(unittest.TestCase):
    """測試 daily_craw 函式。"""

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_no_missing(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試所有資料皆已上傳時不進行爬取。"""
        import DailyUpload

        mock_get_missing.return_value = []
        mock_clear.return_value = []

        DailyUpload.daily_craw()

        mock_day_upload.assert_not_called()

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_with_missing(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試有缺漏日期時進行爬取上傳。"""
        import DailyUpload

        mock_clear.return_value = []
        mock_get_missing.side_effect = [
            ["2026-01-03", "2026-01-02"],  # TWSE
            [],  # TPEX
            [],  # TAIFEX
            [],  # FAOI
            [],  # MGTS
        ]

        DailyUpload.daily_craw()

        # TWSE 有 2 個缺漏日期，應排序後依序爬取
        self.assertEqual(mock_day_upload.call_count, 2)
        dates = [call.args[0] for call in mock_day_upload.call_args_list]
        self.assertEqual(dates, ["2026-01-02", "2026-01-03"])

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_excludes_today(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試早上排程執行時排除今日，只補抓昨日（含）以前的缺漏。

        排程改於早上（07:30）執行，此時當日台股尚未收盤、行情尚未發布，
        若爬取今日會取得空資料並被誤標為非交易日而永久跳過，故應排除今日。
        """
        import DailyUpload

        mock_clear.return_value = []
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        yesterday = (
            datetime.datetime.now() - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

        mock_get_missing.side_effect = [
            [today, yesterday],  # TWSE：含今日與昨日
            [],  # TPEX
            [],  # TAIFEX
            [],  # FAOI
            [],  # MGTS
        ]

        DailyUpload.daily_craw()

        # 只應爬取昨日，今日被排除
        dates = [call.args[0] for call in mock_day_upload.call_args_list]
        self.assertEqual(dates, [yesterday])
        self.assertNotIn(today, dates)

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_pauses_between_dates(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試每次爬取之間有隨機暫停。"""
        import DailyUpload

        mock_clear.return_value = []
        mock_get_missing.side_effect = [
            ["2026-01-02", "2026-01-03"],  # TWSE
            [],  # TPEX
            [],  # TAIFEX
            [],  # FAOI
            [],  # MGTS
        ]

        DailyUpload.daily_craw()

        self.assertEqual(mock_sleep.call_count, 2)

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_iterates_all_db_names(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試遍歷所有資料來源。"""
        import DailyUpload

        mock_get_missing.return_value = []
        mock_clear.return_value = []

        DailyUpload.daily_craw()

        self.assertEqual(mock_get_missing.call_count, len(DailyUpload.DB_NAMES))
        db_names = [call.args[0] for call in mock_get_missing.call_args_list]
        self.assertEqual(db_names, DailyUpload.DB_NAMES)

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_clears_orphans_each_source(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試每個資料來源都先清除近期平日孤兒帳本再補抓。"""
        import DailyUpload

        mock_clear.return_value = []
        mock_get_missing.return_value = []

        DailyUpload.daily_craw()

        # 每個來源都應呼叫一次 clear_price_orphans（以 REVERIFY_DAYS 為視窗）
        self.assertEqual(mock_clear.call_count, len(DailyUpload.DB_NAMES))
        for call_obj in mock_clear.call_args_list:
            self.assertEqual(
                call_obj.kwargs.get("days"), DailyUpload.REVERIFY_DAYS
            )

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_requeries_cleared_orphans(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試清除的孤兒帳本會透過缺漏補抓重新查詢（根因修復核心行為）。

        孤兒帳本被清除後應重新成為缺漏候選並被重抓；本測試以 get_missing_dates
        於清除後回傳該日期模擬此串接，確認 day_upload 會針對該日重新上傳。
        """
        import DailyUpload

        yesterday = (
            datetime.datetime.now() - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

        # TWSE 清出一筆平日孤兒帳本，清除後該日重新成為缺漏候選。
        mock_clear.side_effect = [[yesterday], [], [], [], []]
        mock_get_missing.side_effect = [
            [yesterday],  # TWSE：清除後重新變成缺漏
            [],  # TPEX
            [],  # TAIFEX
            [],  # FAOI
            [],  # MGTS
        ]

        DailyUpload.daily_craw()

        dates = [call.args[0] for call in mock_day_upload.call_args_list]
        self.assertEqual(dates, [yesterday])

    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_survives_clear_failure(
        self, mock_get_missing, mock_sleep, mock_day_upload, mock_clear
    ):
        """測試清孤兒帳本失敗時不中斷當日補抓（防禦性容錯）。"""
        import DailyUpload

        mock_clear.side_effect = Exception("db down")
        mock_get_missing.return_value = ["2026-01-02"]

        # 不應拋出例外，且仍會嘗試補抓缺漏。
        DailyUpload.daily_craw()

        self.assertTrue(mock_day_upload.called)


class TestClearPriceOrphans(unittest.TestCase):
    """測試 clear_price_orphans 函式。"""

    def _make_rows(self, dates):
        """將日期字串清單轉為模擬 SELECT 結果（每列為 (date,)）。"""
        return [
            (datetime.datetime.strptime(d, "%Y-%m-%d").date(),)
            for d in dates
        ]

    @patch("routers.MySQLRouter")
    def test_clears_weekday_orphans(self, mock_router_cls):
        """測試清除平日 Open=False 孤兒帳本並回傳其日期。"""
        import DailyUpload

        # 基準日 2026-01-09（週五）；視窗內含平日孤兒。
        today = datetime.date(2026, 1, 9)
        # 2026-01-05 週一、2026-01-06 週二（皆平日孤兒）
        rows = self._make_rows(["2026-01-05", "2026-01-06"])

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = rows
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.clear_price_orphans("TWSE", days=7, today=today)

        self.assertEqual(result, ["2026-01-05", "2026-01-06"])
        # 一次 SELECT + 兩次 DELETE
        self.assertEqual(mock_conn.execute.call_count, 3)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_skips_weekend_orphans(self, mock_router_cls):
        """測試週末孤兒帳本不清除（確定非交易日、不反覆重試）。"""
        import DailyUpload

        today = datetime.date(2026, 1, 12)  # 週一
        # 2026-01-10 週六、2026-01-11 週日 → 週末，應略過
        rows = self._make_rows(["2026-01-10", "2026-01-11"])

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = rows
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.clear_price_orphans("TWSE", days=7, today=today)

        self.assertEqual(result, [])
        # 只有 SELECT，無 DELETE、無 commit
        self.assertEqual(mock_conn.execute.call_count, 1)
        mock_conn.commit.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_mixed_weekday_weekend(self, mock_router_cls):
        """測試混合平日與週末時只清除平日。"""
        import DailyUpload

        today = datetime.date(2026, 1, 12)  # 週一
        # 2026-01-09 週五（平日）、2026-01-10 週六（週末）
        rows = self._make_rows(["2026-01-09", "2026-01-10"])

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = rows
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.clear_price_orphans("TWSE", days=7, today=today)

        self.assertEqual(result, ["2026-01-09"])
        # SELECT + 1 次 DELETE
        self.assertEqual(mock_conn.execute.call_count, 2)

    @patch("routers.MySQLRouter")
    def test_no_orphans(self, mock_router_cls):
        """測試無孤兒帳本時回傳空清單且不 DELETE、不 commit。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.clear_price_orphans("TWSE", days=7)

        self.assertEqual(result, [])
        mock_conn.execute.assert_called_once()  # 只有 SELECT
        mock_conn.commit.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_faoi_uses_mapped_db_and_table(self, mock_router_cls):
        """測試 FAOI 連線至 TWSE 資料庫並操作 FAOIUploadDate 表。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.clear_price_orphans("FAOI", days=7)

        mock_router_cls.assert_called_once_with(
            DailyUpload.HOST,
            DailyUpload.USER,
            DailyUpload.PASSWORD,
            "TWSE",
        )
        select_sql = str(mock_conn.execute.call_args_list[0].args[0])
        self.assertIn("FAOIUploadDate", select_sql)
        # 僅針對 Open=False 的孤兒帳本
        self.assertIn("`Open` = 0", select_sql)

    @patch("routers.MySQLRouter")
    def test_only_selects_open_false(self, mock_router_cls):
        """測試 SELECT 僅挑出 Open=False（不動 Open=True 已上傳日）。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.clear_price_orphans("MGTS", days=7)

        select_sql = str(mock_conn.execute.call_args_list[0].args[0])
        self.assertIn("MGTSUploadDate", select_sql)
        self.assertIn("`Open` = 0", select_sql)

    @patch("routers.MySQLRouter")
    def test_delete_also_guards_open_false(self, mock_router_cls):
        """測試 DELETE 亦帶 Open=0 條件（防 SELECT 後被改為已上傳的競態）。"""
        import DailyUpload

        today = datetime.date(2026, 1, 9)  # 週五
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = self._make_rows(
            ["2026-01-05"]
        )
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.clear_price_orphans("TWSE", days=7, today=today)

        delete_sql = str(mock_conn.execute.call_args_list[1].args[0])
        self.assertIn("DELETE", delete_sql)
        self.assertIn("`Open` = 0", delete_sql)

    @patch("routers.MySQLRouter")
    def test_window_excludes_today(self, mock_router_cls):
        """測試查詢視窗為 today-days+1 ～ today-1（排除今日）。"""
        import DailyUpload

        today = datetime.date(2026, 1, 12)
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.clear_price_orphans("TWSE", days=7, today=today)

        params = mock_conn.execute.call_args_list[0].args[1]
        self.assertEqual(params["start"], "2026-01-06")
        self.assertEqual(params["today"], "2026-01-12")

    @patch("routers.MySQLRouter")
    def test_uses_custom_connection_args(self, mock_router_cls):
        """測試可注入自訂 MySQL 連線參數（供一次性修復入口使用）。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        DailyUpload.clear_price_orphans(
            "TWSE", days=7, host="h", user="u", password="p",
        )

        mock_router_cls.assert_called_once_with("h", "u", "p", "TWSE")


class TestToDate(unittest.TestCase):
    """測試 _to_date 帳本日期正規化。"""

    def test_accepts_date(self):
        """測試 datetime.date 原樣回傳。"""
        import DailyUpload

        value = datetime.date(2026, 1, 5)
        self.assertEqual(DailyUpload._to_date(value), value)

    def test_accepts_datetime(self):
        """測試 datetime.datetime 取其日期部分。"""
        import DailyUpload

        value = datetime.datetime(2026, 1, 5, 13, 30)
        self.assertEqual(DailyUpload._to_date(value), datetime.date(2026, 1, 5))

    def test_accepts_string(self):
        """測試 YYYY-MM-DD 字串轉為 date。"""
        import DailyUpload

        self.assertEqual(
            DailyUpload._to_date("2026-01-05"), datetime.date(2026, 1, 5)
        )

    def test_rejects_unknown_type(self):
        """測試無法辨識的型別拋出 TypeError。"""
        import DailyUpload

        with self.assertRaises(TypeError):
            DailyUpload._to_date(20260105)


if __name__ == "__main__":
    unittest.main()

"""每日排程上傳模組單元測試。"""

import unittest
from unittest.mock import patch, MagicMock, call
import datetime


class TestGetMissingDates(unittest.TestCase):
    """測試 get_missing_dates 函式。"""

    @patch("DailyUpload.MySQLRouter")
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

    @patch("DailyUpload.MySQLRouter")
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

    @patch("DailyUpload.MySQLRouter")
    def test_all_missing_dates(self, mock_router_cls):
        """測試完全沒有上傳紀錄時回傳全部日期。"""
        import DailyUpload

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_router_cls.return_value.mysql_conn = mock_conn

        result = DailyUpload.get_missing_dates("TWSE", days=3)

        self.assertEqual(len(result), 3)
        mock_conn.close.assert_called_once()

    @patch("DailyUpload.MySQLRouter")
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


class TestDailyCraw(unittest.TestCase):
    """測試 daily_craw 函式。"""

    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_no_missing(
        self, mock_get_missing, mock_sleep, mock_day_upload
    ):
        """測試所有資料皆已上傳時不進行爬取。"""
        import DailyUpload

        mock_get_missing.return_value = []

        DailyUpload.daily_craw()

        mock_day_upload.assert_not_called()

    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_with_missing(
        self, mock_get_missing, mock_sleep, mock_day_upload
    ):
        """測試有缺漏日期時進行爬取上傳。"""
        import DailyUpload

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

    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_pauses_between_dates(
        self, mock_get_missing, mock_sleep, mock_day_upload
    ):
        """測試每次爬取之間有隨機暫停。"""
        import DailyUpload

        mock_get_missing.side_effect = [
            ["2026-01-02", "2026-01-03"],  # TWSE
            [],  # TPEX
            [],  # TAIFEX
            [],  # FAOI
            [],  # MGTS
        ]

        DailyUpload.daily_craw()

        self.assertEqual(mock_sleep.call_count, 2)

    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_daily_craw_iterates_all_db_names(
        self, mock_get_missing, mock_sleep, mock_day_upload
    ):
        """測試遍歷所有資料來源。"""
        import DailyUpload

        mock_get_missing.return_value = []

        DailyUpload.daily_craw()

        self.assertEqual(mock_get_missing.call_count, len(DailyUpload.DB_NAMES))
        db_names = [call.args[0] for call in mock_get_missing.call_args_list]
        self.assertEqual(db_names, DailyUpload.DB_NAMES)


if __name__ == "__main__":
    unittest.main()

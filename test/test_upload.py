"""批次上傳入口模組單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from easydict import EasyDict


class TestDayUpload(unittest.TestCase):
    """測試 day_upload 函式。"""

    @patch("upload.data_upload")
    @patch("upload.MySQLRouter")
    def test_day_upload_calls_uploader(self, mock_router_cls, mock_data_upload):
        """測試 day_upload 正確呼叫對應上傳器。"""
        import upload

        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        mock_uploader = MagicMock()
        mock_module = MagicMock()
        mock_module.Uploader.return_value = mock_uploader
        mock_data_upload.__dict__ = {"twse": mock_module}

        opt = EasyDict({
            "host": "localhost:3306",
            "user": "root",
            "password": "stock",
            "dbname": "TWSE",
            "crawlerhost": "127.0.0.1:6738",
        })

        upload.day_upload("2026-01-02", opt)

        mock_router_cls.assert_called_once_with(
            "localhost:3306", "root", "stock", "TWSE"
        )
        mock_module.Uploader.assert_called_once_with(mock_conn, "127.0.0.1:6738")
        mock_uploader.upload.assert_called_once_with("2026-01-02")
        mock_conn.close.assert_called_once()

    @patch("upload.data_upload")
    @patch("upload.MySQLRouter")
    def test_day_upload_uses_correct_package_name(
        self, mock_router_cls, mock_data_upload
    ):
        """測試 day_upload 將 dbname 轉換為小寫模組名稱。"""
        import upload

        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        mock_uploader = MagicMock()
        mock_module = MagicMock()
        mock_module.Uploader.return_value = mock_uploader
        mock_data_upload.__dict__ = {"tpex": mock_module}

        opt = EasyDict({
            "host": "localhost:3306",
            "user": "root",
            "password": "stock",
            "dbname": "TPEX",
            "crawlerhost": "127.0.0.1:6738",
        })

        upload.day_upload("2026-01-02", opt)

        mock_module.Uploader.assert_called_once()


class TestMain(unittest.TestCase):
    """測試 main 函式。"""

    @patch("upload.time.sleep")
    @patch("upload.day_upload")
    def test_main_single_date(self, mock_day_upload, mock_sleep):
        """測試單日上傳。"""
        import upload

        opt = EasyDict({
            "start_date": "2026-01-02",
            "end_date": "",
            "host": "localhost:3306",
            "user": "root",
            "password": "stock",
            "dbname": "TWSE",
            "crawlerhost": "127.0.0.1:6738",
        })

        upload.main(opt)

        mock_day_upload.assert_called_once_with("2026-01-02", opt)

    @patch("upload.time.sleep")
    @patch("upload.day_upload")
    def test_main_date_range(self, mock_day_upload, mock_sleep):
        """測試日期範圍批次上傳。"""
        import upload

        opt = EasyDict({
            "start_date": "2026-01-02",
            "end_date": "2026-01-04",
            "host": "localhost:3306",
            "user": "root",
            "password": "stock",
            "dbname": "TWSE",
            "crawlerhost": "127.0.0.1:6738",
        })

        upload.main(opt)

        self.assertEqual(mock_day_upload.call_count, 3)
        dates = [call.args[0] for call in mock_day_upload.call_args_list]
        self.assertEqual(dates, ["2026-01-02", "2026-01-03", "2026-01-04"])

    @patch("upload.time.sleep")
    @patch("upload.day_upload")
    def test_main_sets_end_date_if_empty(self, mock_day_upload, mock_sleep):
        """測試未指定 end_date 時自動設為 start_date。"""
        import upload

        opt = EasyDict({
            "start_date": "2026-01-05",
            "end_date": "",
            "host": "localhost:3306",
            "user": "root",
            "password": "stock",
            "dbname": "TWSE",
            "crawlerhost": "127.0.0.1:6738",
        })

        upload.main(opt)

        self.assertEqual(opt.end_date, "2026-01-05")
        mock_day_upload.assert_called_once()


if __name__ == "__main__":
    unittest.main()

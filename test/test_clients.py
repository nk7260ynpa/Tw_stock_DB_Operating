"""MySQL 連線函式單元測試模組。"""

import unittest
from unittest.mock import patch, MagicMock

from clients import mysql_conn, mysql_conn_db


class TestMysqlConn(unittest.TestCase):
    """測試 mysql_conn 函式。"""

    @patch("clients.create_engine")
    def test_mysql_conn_creates_correct_url(self, mock_create_engine):
        """測試不指定資料庫時產生正確的連線字串。"""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_conn

        result = mysql_conn("localhost:3306", "root", "password")

        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://root:password@localhost:3306"
        )
        mock_engine.connect.assert_called_once()
        self.assertEqual(result, mock_conn)

    @patch("clients.create_engine")
    def test_mysql_conn_with_different_host(self, mock_create_engine):
        """測試不同主機位址的連線字串。"""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_conn

        result = mysql_conn("192.168.1.100:3307", "admin", "secret")

        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://admin:secret@192.168.1.100:3307"
        )
        self.assertEqual(result, mock_conn)


class TestMysqlConnDb(unittest.TestCase):
    """測試 mysql_conn_db 函式。"""

    @patch("clients.create_engine")
    def test_mysql_conn_db_creates_correct_url(self, mock_create_engine):
        """測試指定資料庫時產生正確的連線字串。"""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_conn

        result = mysql_conn_db("localhost:3306", "root", "password", "TWSE")

        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://root:password@localhost:3306/TWSE"
        )
        mock_engine.connect.assert_called_once()
        self.assertEqual(result, mock_conn)

    @patch("clients.create_engine")
    def test_mysql_conn_db_with_different_db(self, mock_create_engine):
        """測試不同資料庫名稱的連線字串。"""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_conn

        result = mysql_conn_db("localhost:3306", "root", "stock", "TPEX")

        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://root:stock@localhost:3306/TPEX"
        )
        self.assertEqual(result, mock_conn)


if __name__ == "__main__":
    unittest.main()

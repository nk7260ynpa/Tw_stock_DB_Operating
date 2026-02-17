"""MySQLRouter 單元測試模組。"""

import unittest
from unittest.mock import patch, MagicMock

from routers import MySQLRouter


class TestMySQLRouter(unittest.TestCase):
    """測試 MySQLRouter 類別。"""

    @patch("routers.mysql_conn_db")
    def test_init_with_db_name(self, mock_mysql_conn_db):
        """測試指定資料庫名稱時的初始化。"""
        mock_conn = MagicMock()
        mock_mysql_conn_db.return_value = mock_conn

        router = MySQLRouter("localhost:3306", "root", "password", "TWSE")

        mock_mysql_conn_db.assert_called_once_with(
            "localhost:3306", "root", "password", "TWSE"
        )
        self.assertEqual(router.host, "localhost:3306")
        self.assertEqual(router.user, "root")
        self.assertEqual(router.password, "password")
        self.assertEqual(router.db_name, "TWSE")
        self.assertEqual(router.mysql_conn, mock_conn)

    @patch("routers.mysql_conn")
    def test_init_without_db_name(self, mock_mysql_conn):
        """測試不指定資料庫名稱時的初始化。"""
        mock_conn = MagicMock()
        mock_mysql_conn.return_value = mock_conn

        router = MySQLRouter("localhost:3306", "root", "password")

        mock_mysql_conn.assert_called_once_with(
            "localhost:3306", "root", "password"
        )
        self.assertIsNone(router.db_name)
        self.assertEqual(router.mysql_conn, mock_conn)

    @patch("routers.mysql_conn_db")
    def test_mysql_conn_property(self, mock_mysql_conn_db):
        """測試 mysql_conn 屬性回傳連線物件。"""
        mock_conn = MagicMock()
        mock_mysql_conn_db.return_value = mock_conn

        router = MySQLRouter("localhost:3306", "root", "password", "TWSE")

        self.assertIs(router.mysql_conn, mock_conn)


if __name__ == "__main__":
    unittest.main()

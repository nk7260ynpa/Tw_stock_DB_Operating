"""DataUploadBase 單元測試模組。"""

import unittest
from unittest.mock import MagicMock, patch, call

import pandas as pd

from data_upload.base import DataUploadBase


class ConcreteUploader(DataUploadBase):
    """測試用具體上傳器。"""

    def __init__(self, conn):
        """初始化測試用上傳器。

        Args:
            conn (MagicMock): Mock 的資料庫連線物件。
        """
        super().__init__(conn)
        self.url = "http://localhost:6738"
        self.stock_code_col = "SecurityCode"
        self.stock_name_col = "StockName"

    def preprocess(self, df):
        """預處理 DataFrame，移除 StockName 欄位。

        Args:
            df (pd.DataFrame): 待預處理的 DataFrame。

        Returns:
            pd.DataFrame: 移除 StockName 欄位後的 DataFrame。
        """
        return df.drop(columns=["StockName"])


class TestRegisterStockNames(unittest.TestCase):
    """測試 register_stock_names 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    def test_skip_when_cols_not_set(self):
        """測試未設定欄位時跳過註冊。"""
        self.uploader.stock_code_col = None
        self.uploader.stock_name_col = None

        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
        })
        self.uploader.register_stock_names(df)

        self.mock_conn.execute.assert_not_called()

    def test_no_new_stocks(self):
        """測試所有股票代碼皆已存在時不新增。"""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("2330",), ("2317",)]
        self.mock_conn.execute.return_value = mock_result

        df = pd.DataFrame({
            "SecurityCode": ["2330", "2317"],
            "StockName": ["台積電", "鴻海"],
        })
        self.uploader.register_stock_names(df)

        self.mock_conn.execute.assert_called_once()
        self.mock_conn.commit.assert_not_called()

    @patch("data_upload.base.pd.DataFrame.to_sql")
    def test_register_new_stocks(self, mock_to_sql):
        """測試新增不存在的股票代碼。"""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("2330",)]
        self.mock_conn.execute.return_value = mock_result

        df = pd.DataFrame({
            "SecurityCode": ["2330", "2317", "2454"],
            "StockName": ["台積電", "鴻海", "聯發科"],
        })
        self.uploader.register_stock_names(df)

        mock_to_sql.assert_called_once_with(
            "StockName", self.mock_conn,
            if_exists='append', index=False
        )
        self.mock_conn.commit.assert_called_once()

    @patch("data_upload.base.pd.DataFrame.to_sql")
    def test_register_deduplicates_by_code(self, mock_to_sql):
        """測試同一股票代碼出現多次時只註冊一次。"""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        self.mock_conn.execute.return_value = mock_result

        df = pd.DataFrame({
            "SecurityCode": ["2330", "2330", "2317"],
            "StockName": ["台積電", "台積電", "鴻海"],
        })
        self.uploader.register_stock_names(df)

        mock_to_sql.assert_called_once()
        self.mock_conn.commit.assert_called_once()


class TestRegisterStockNamesTpex(unittest.TestCase):
    """測試 TPEX 風格欄位（Code/Name）的註冊。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)
        self.uploader.stock_code_col = "Code"
        self.uploader.stock_name_col = "Name"

    @patch("data_upload.base.pd.DataFrame.to_sql")
    def test_register_with_code_name_cols(self, mock_to_sql):
        """測試使用 Code/Name 欄位的股票代碼註冊。"""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        self.mock_conn.execute.return_value = mock_result

        df = pd.DataFrame({
            "Code": ["6547", "4966"],
            "Name": ["高端疫苗", "譜瑞-KY"],
            "Close": [100.0, 200.0],
        })
        self.uploader.register_stock_names(df)

        mock_to_sql.assert_called_once_with(
            "StockName", self.mock_conn,
            if_exists='append', index=False
        )
        self.mock_conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()

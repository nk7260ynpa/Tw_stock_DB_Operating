"""DataUploadBase 單元測試模組。"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from pydantic import BaseModel

from data_upload.base import DataUploadBase


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


class TestCrawData(unittest.TestCase):
    """測試 craw_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    @patch("data_upload.base.requests.get")
    def test_craw_data_success(self, mock_get):
        """測試成功取得爬蟲資料。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"SecurityCode": "2330", "Value": 100.0},
                {"SecurityCode": "2317", "Value": 200.0},
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = self.uploader.craw_data("2026-01-02")

        mock_get.assert_called_once_with(
            "http://localhost:6738/test", params={"date": "2026-01-02"}
        )
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["SecurityCode"], "2330")

    @patch("data_upload.base.requests.get")
    def test_craw_data_request_exception(self, mock_get):
        """測試爬蟲服務連線失敗時回傳空 DataFrame。"""
        mock_get.side_effect = requests.RequestException("Connection refused")

        df = self.uploader.craw_data("2026-01-02")

        self.assertTrue(df.empty)

    @patch("data_upload.base.requests.get")
    def test_craw_data_missing_data_key(self, mock_get):
        """測試爬蟲回應缺少 data 欄位時回傳空 DataFrame。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": "not found"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = self.uploader.craw_data("2026-01-02")

        self.assertTrue(df.empty)

    @patch("data_upload.base.requests.get")
    def test_craw_data_empty_data(self, mock_get):
        """測試爬蟲回應 data 為空列表時回傳空 DataFrame。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = self.uploader.craw_data("2026-01-02")

        self.assertEqual(len(df), 0)


class TestCheckSchema(unittest.TestCase):
    """測試 check_schema 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    def test_check_schema_valid(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "Value": [100.0],
        })

        result = self.uploader.check_schema(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["SecurityCode"], "2330")
        self.assertAlmostEqual(result.iloc[0]["Value"], 100.0)

    def test_check_schema_type_coercion(self):
        """測試 schema 驗證時自動型別轉換。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "Value": ["99.5"],
        })

        result = self.uploader.check_schema(df)

        self.assertAlmostEqual(result.iloc[0]["Value"], 99.5)


class TestCheckDate(unittest.TestCase):
    """測試 check_date 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    def test_check_date_exists(self):
        """測試日期已存在時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1

        result = self.uploader.check_date("2026-01-02")

        self.assertTrue(result)

    def test_check_date_not_exists(self):
        """測試日期不存在時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        result = self.uploader.check_date("2026-01-02")

        self.assertFalse(result)


class TestUploadDf(unittest.TestCase):
    """測試 upload_df 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    @patch("data_upload.base.pd.DataFrame.to_sql")
    def test_upload_df_calls_to_sql(self, mock_to_sql):
        """測試 upload_df 呼叫 to_sql 上傳資料。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
            "Value": [100.0],
        })

        self.uploader.upload_df(df)

        mock_to_sql.assert_called_once_with(
            "DailyPrice", self.mock_conn,
            if_exists='append', index=False, chunksize=1000
        )
        self.mock_conn.commit.assert_called_once()

    @patch("data_upload.base.pd.DataFrame.to_sql")
    def test_upload_df_preprocesses_data(self, mock_to_sql):
        """測試 upload_df 會先進行預處理（移除 StockName）。"""
        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
            "Value": [100.0],
        })

        self.uploader.upload_df(df)

        # 原始 df 不應被修改
        self.assertIn("StockName", df.columns)


class TestUploadDate(unittest.TestCase):
    """測試 upload_date 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    def test_upload_date_with_data(self):
        """測試有交易資料時記錄 Open=True。"""
        df = pd.DataFrame({"SecurityCode": ["2330"]})

        self.uploader.upload_date("2026-01-02", df)

        call_args = self.mock_conn.execute.call_args[0][0]
        self.assertIn("True", str(call_args))
        self.mock_conn.commit.assert_called_once()

    def test_upload_date_without_data(self):
        """測試無交易資料時記錄 Open=False。"""
        df = pd.DataFrame()

        self.uploader.upload_date("2026-01-02", df)

        call_args = self.mock_conn.execute.call_args[0][0]
        self.assertIn("False", str(call_args))
        self.mock_conn.commit.assert_called_once()


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = ConcreteUploader(self.mock_conn)

    def test_upload_skips_existing_date(self):
        """測試日期已存在時跳過上傳。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1

        with patch.object(self.uploader, "craw_data") as mock_craw:
            self.uploader.upload("2026-01-02")
            mock_craw.assert_not_called()

    @patch("data_upload.base.pd.DataFrame.to_sql")
    def test_upload_with_data(self, mock_to_sql):
        """測試有資料時執行完整上傳流程。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        df = pd.DataFrame({
            "SecurityCode": ["2330"],
            "StockName": ["台積電"],
            "Value": [100.0],
        })

        mock_register_result = MagicMock()
        mock_register_result.fetchall.return_value = [("2330",)]
        self.mock_conn.execute.side_effect = [
            MagicMock(scalar=MagicMock(return_value=0)),  # check_date
            mock_register_result,  # register_stock_names SELECT
            MagicMock(),  # upload_date INSERT
        ]

        with patch.object(self.uploader, "craw_data", return_value=df):
            self.uploader.upload("2026-01-02")

    def test_upload_without_data(self):
        """測試無資料時只記錄日期不上傳。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        empty_df = pd.DataFrame()

        with patch.object(self.uploader, "craw_data", return_value=empty_df):
            with patch.object(self.uploader, "upload_df") as mock_upload_df:
                self.uploader.upload("2026-01-02")
                mock_upload_df.assert_not_called()


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

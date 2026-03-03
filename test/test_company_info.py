"""公司產業對照模組單元測試。"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError

from data_upload.company_info import (
    CompanyInfoType,
    IndustryMapType,
    CompanyInfoUploader,
)


class TestCompanyInfoType(unittest.TestCase):
    """測試 CompanyInfoType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = CompanyInfoType(
            SecurityCode="1101",
            IndustryCode="01",
            CompanyName="臺灣水泥股份有限公司",
            SpecialShares=200000000,
            NormalShares=7523181742,
            PrivateShares=0,
        )

        self.assertEqual(data.SecurityCode, "1101")
        self.assertEqual(data.IndustryCode, "01")
        self.assertEqual(data.CompanyName, "臺灣水泥股份有限公司")
        self.assertEqual(data.SpecialShares, 200000000)
        self.assertEqual(data.NormalShares, 7523181742)
        self.assertEqual(data.PrivateShares, 0)

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            CompanyInfoType(
                SecurityCode="1101",
                IndustryCode="01",
            )


class TestIndustryMapType(unittest.TestCase):
    """測試 IndustryMapType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = IndustryMapType(
            IndustryCode="01",
            Industry="水泥工業",
        )

        self.assertEqual(data.IndustryCode, "01")
        self.assertEqual(data.Industry, "水泥工業")

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            IndustryMapType(IndustryCode="01")


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CompanyInfoUploader(
            self.mock_conn, "localhost:6738"
        )

    @patch("data_upload.company_info.requests.get")
    def test_success(self, mock_get):
        """測試成功取得資料。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {
                "company_info": [
                    {
                        "SecurityCode": "1101",
                        "IndustryCode": "01",
                        "CompanyName": "臺灣水泥",
                        "SpecialShares": 200000000,
                        "NormalShares": 7523181742,
                        "PrivateShares": 0,
                    },
                ],
                "industry_map": [
                    {
                        "IndustryCode": "01",
                        "Industry": "水泥工業",
                        "Market": "TWSE",
                    },
                ],
                "twse_count": 1,
                "tpex_count": 0,
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.crawl_data()

        self.assertIsNotNone(result)
        self.assertIn("company_info", result)
        self.assertIn("industry_map", result)
        self.assertEqual(len(result["company_info"]), 1)

    @patch("data_upload.company_info.requests.get")
    def test_connection_failure(self, mock_get):
        """測試連線失敗回傳 None。"""
        mock_get.side_effect = Exception("連線失敗")

        result = self.uploader.crawl_data()

        self.assertIsNone(result)

    @patch("data_upload.company_info.requests.get")
    def test_empty_data(self, mock_get):
        """測試空資料回傳 None。"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": None}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.uploader.crawl_data()

        self.assertIsNone(result)


class TestCheckSchemaCompanyInfo(unittest.TestCase):
    """測試 check_schema_company_info 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CompanyInfoUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "SecurityCode": ["1101"],
            "IndustryCode": ["01"],
            "CompanyName": ["臺灣水泥"],
            "SpecialShares": [200000000],
            "NormalShares": [7523181742],
            "PrivateShares": [0],
        })

        result = self.uploader.check_schema_company_info(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["SecurityCode"].iloc[0], "1101")

    def test_invalid_schema(self):
        """測試不合法資料拋出例外。"""
        df = pd.DataFrame({
            "SecurityCode": ["1101"],
        })

        with self.assertRaises(Exception):
            self.uploader.check_schema_company_info(df)


class TestCheckSchemaIndustryMap(unittest.TestCase):
    """測試 check_schema_industry_map 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CompanyInfoUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_valid_schema(self):
        """測試合法資料通過 schema 驗證。"""
        df = pd.DataFrame({
            "IndustryCode": ["01"],
            "Industry": ["水泥工業"],
        })

        result = self.uploader.check_schema_industry_map(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["IndustryCode"].iloc[0], "01")


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CompanyInfoUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_no_data(self):
        """測試無資料時回傳 count=0。"""
        self.uploader.crawl_data = MagicMock(return_value=None)

        result = self.uploader.upload()

        self.assertEqual(result["company_info_count"], 0)
        self.assertEqual(result["industry_map_count"], 0)

    def test_successful_upload(self):
        """測試成功上傳回傳正確筆數。"""
        self.uploader.crawl_data = MagicMock(return_value={
            "company_info": [
                {
                    "SecurityCode": "1101",
                    "IndustryCode": "01",
                    "CompanyName": "臺灣水泥",
                    "SpecialShares": 200000000,
                    "NormalShares": 7523181742,
                    "PrivateShares": 0,
                },
                {
                    "SecurityCode": "1102",
                    "IndustryCode": "01",
                    "CompanyName": "亞洲水泥",
                    "SpecialShares": 100000000,
                    "NormalShares": 3000000000,
                    "PrivateShares": 0,
                },
            ],
            "industry_map": [
                {
                    "IndustryCode": "01",
                    "Industry": "水泥工業",
                    "Market": "TWSE",
                },
                {
                    "IndustryCode": "99",
                    "Industry": "其他",
                    "Market": "TPEX",
                },
            ],
        })

        result = self.uploader.upload()

        self.assertEqual(result["company_info_count"], 2)
        # 僅 TWSE 的 IndustryMap 會被上傳
        self.assertEqual(result["industry_map_count"], 1)
        self.mock_conn.commit.assert_called()

    def test_empty_company_info(self):
        """測試 company_info 為空清單時 count=0。"""
        self.uploader.crawl_data = MagicMock(return_value={
            "company_info": [],
            "industry_map": [
                {
                    "IndustryCode": "01",
                    "Industry": "水泥工業",
                    "Market": "TWSE",
                },
            ],
        })

        result = self.uploader.upload()

        self.assertEqual(result["company_info_count"], 0)
        self.assertEqual(result["industry_map_count"], 1)

    def test_empty_industry_map(self):
        """測試 industry_map 為空清單時 count=0。"""
        self.uploader.crawl_data = MagicMock(return_value={
            "company_info": [
                {
                    "SecurityCode": "1101",
                    "IndustryCode": "01",
                    "CompanyName": "臺灣水泥",
                    "SpecialShares": 200000000,
                    "NormalShares": 7523181742,
                    "PrivateShares": 0,
                },
            ],
            "industry_map": [],
        })

        result = self.uploader.upload()

        self.assertEqual(result["company_info_count"], 1)
        self.assertEqual(result["industry_map_count"], 0)

    def test_filter_twse_only(self):
        """測試僅保留 Market=TWSE 的 IndustryMap。"""
        self.uploader.crawl_data = MagicMock(return_value={
            "company_info": [],
            "industry_map": [
                {
                    "IndustryCode": "01",
                    "Industry": "水泥工業",
                    "Market": "TWSE",
                },
                {
                    "IndustryCode": "02",
                    "Industry": "食品工業",
                    "Market": "TWSE",
                },
                {
                    "IndustryCode": "90",
                    "Industry": "電子類",
                    "Market": "TPEX",
                },
            ],
        })

        result = self.uploader.upload()

        # 只有 2 筆 TWSE 的 IndustryMap
        self.assertEqual(result["industry_map_count"], 2)


class TestReplaceInto(unittest.TestCase):
    """測試 _replace_into 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        self.uploader = CompanyInfoUploader(
            self.mock_conn, "localhost:6738"
        )

    def test_empty_dataframe(self):
        """測試空 DataFrame 不執行任何操作。"""
        df = pd.DataFrame()

        self.uploader._replace_into("CompanyInfo", df)

        self.mock_conn.execute.assert_not_called()
        self.mock_conn.commit.assert_not_called()

    def test_non_empty_dataframe(self):
        """測試非空 DataFrame 執行 REPLACE INTO。"""
        df = pd.DataFrame({
            "SecurityCode": ["1101"],
            "IndustryCode": ["01"],
            "CompanyName": ["臺灣水泥"],
            "SpecialShares": [200000000],
            "NormalShares": [7523181742],
            "PrivateShares": [0],
        })

        self.uploader._replace_into("CompanyInfo", df)

        self.mock_conn.execute.assert_called_once()
        self.mock_conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()

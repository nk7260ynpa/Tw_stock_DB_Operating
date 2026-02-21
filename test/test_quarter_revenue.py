"""季度營業收入模組單元測試。"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError

from data_upload.quarter_revenue import (
    QuarterRevenueType,
    QuarterRevenueUploader,
    COLUMN_KEYWORD_MAPPING,
)


class TestQuarterRevenueType(unittest.TestCase):
    """測試 QuarterRevenueType schema。"""

    def test_valid_data(self):
        """測試合法資料通過驗證。"""
        data = QuarterRevenueType(
            Year=113,
            Season=1,
            CompanyCode="2330",
            TYPEK="sii",
            CompanyName="台積電",
            Industry="半導體業",
            EPS=10.53,
            Revenue=592559000,
        )

        self.assertEqual(data.CompanyCode, "2330")
        self.assertEqual(data.Year, 113)
        self.assertEqual(data.Season, 1)
        self.assertEqual(data.Industry, "半導體業")
        self.assertEqual(data.EPS, 10.53)

    def test_optional_fields(self):
        """測試選填欄位可為 None。"""
        data = QuarterRevenueType(
            Year=113,
            Season=1,
            CompanyCode="2330",
            TYPEK="sii",
        )

        self.assertIsNone(data.CompanyName)
        self.assertIsNone(data.Industry)
        self.assertIsNone(data.EPS)
        self.assertIsNone(data.Revenue)
        self.assertIsNone(data.NetIncome)

    def test_missing_required_field(self):
        """測試缺少必要欄位時拋出 ValidationError。"""
        with self.assertRaises(ValidationError):
            QuarterRevenueType(
                Year=113,
                Season=1,
            )


class TestCheckUploaded(unittest.TestCase):
    """測試 check_uploaded 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        with patch.object(
            QuarterRevenueUploader, "_ensure_tables"
        ):
            self.uploader = QuarterRevenueUploader(self.mock_conn)

    def test_uploaded_exists(self):
        """測試已上傳時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1

        result = self.uploader.check_uploaded(113, 1)

        self.assertTrue(result)

    def test_not_exists(self):
        """測試未上傳時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0

        result = self.uploader.check_uploaded(113, 1)

        self.assertFalse(result)


class TestCleanDataframe(unittest.TestCase):
    """測試 _clean_dataframe 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        with patch.object(
            QuarterRevenueUploader, "_ensure_tables"
        ):
            self.uploader = QuarterRevenueUploader(self.mock_conn)

    def test_removes_non_numeric_code(self):
        """測試移除 CompanyCode 非數字開頭的列。"""
        df = pd.DataFrame({
            "公司代號": ["2330", "合計", "8069"],
            "公司名稱": ["台積電", "合計", "元太"],
            "營業收入": [100, 200, 300],
        })

        result = self.uploader._clean_dataframe(df, 113, 1, "sii")

        self.assertEqual(len(result), 2)
        self.assertIn("2330", result["CompanyCode"].values)
        self.assertIn("8069", result["CompanyCode"].values)

    def test_replaces_dashes(self):
        """測試 '--' 被替換為 NaN。"""
        df = pd.DataFrame({
            "公司代號": ["2330"],
            "公司名稱": ["台積電"],
            "營業收入": ["--"],
        })

        result = self.uploader._clean_dataframe(df, 113, 1, "sii")

        self.assertTrue(
            pd.isna(result["Revenue"].iloc[0])
        )

    def test_adds_metadata(self):
        """測試加入 Year、Season、TYPEK 欄位。"""
        df = pd.DataFrame({
            "公司代號": ["2330"],
            "公司名稱": ["台積電"],
            "產業別": ["半導體業"],
            "營業收入": [100000],
        })

        result = self.uploader._clean_dataframe(df, 113, 2, "sii")

        self.assertEqual(result["Year"].iloc[0], 113)
        self.assertEqual(result["Season"].iloc[0], 2)
        self.assertEqual(result["TYPEK"].iloc[0], "sii")
        self.assertEqual(result["Industry"].iloc[0], "半導體業")

    def test_drops_total_rows(self):
        """測試移除合計列。"""
        df = pd.DataFrame({
            "公司代號": ["2330", "合計"],
            "公司名稱": ["台積電", ""],
            "營業收入": [100000, 200000],
        })

        result = self.uploader._clean_dataframe(df, 113, 1, "sii")

        self.assertEqual(len(result), 1)

    def test_missing_company_code_returns_empty(self):
        """測試找不到 CompanyCode 欄位時回傳空 DataFrame。"""
        df = pd.DataFrame({
            "未知欄位": ["2330"],
            "另一個欄位": ["台積電"],
        })

        result = self.uploader._clean_dataframe(df, 113, 1, "sii")

        self.assertTrue(result.empty)

    def test_converts_numeric_columns(self):
        """測試數值欄位轉換（含千分位逗號）。"""
        df = pd.DataFrame({
            "公司代號": ["2330"],
            "公司名稱": ["台積電"],
            "基本每股盈餘(元)": ["10.53"],
            "營業收入": ["592,559,000"],
            "營業利益": ["200,000,000"],
            "稅後淨利": ["180,000,000"],
        })

        result = self.uploader._clean_dataframe(df, 113, 1, "sii")

        self.assertAlmostEqual(result["EPS"].iloc[0], 10.53)
        self.assertEqual(result["Revenue"].iloc[0], 592559000)
        self.assertEqual(result["OperatingIncome"].iloc[0], 200000000)
        self.assertEqual(result["NetIncome"].iloc[0], 180000000)


class TestBuildColumnMapping(unittest.TestCase):
    """測試 _build_column_mapping 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        with patch.object(
            QuarterRevenueUploader, "_ensure_tables"
        ):
            self.uploader = QuarterRevenueUploader(self.mock_conn)

    def test_mapping_standard_columns(self):
        """測試標準欄位名稱對應。"""
        columns = [
            "公司代號", "公司名稱", "產業別",
            "基本每股盈餘(元)", "營業收入",
        ]

        result = self.uploader._build_column_mapping(columns)

        self.assertEqual(result["公司代號"], "CompanyCode")
        self.assertEqual(result["公司名稱"], "CompanyName")
        self.assertEqual(result["產業別"], "Industry")
        self.assertEqual(result["基本每股盈餘(元)"], "EPS")
        self.assertEqual(result["營業收入"], "Revenue")

    def test_mapping_with_extra_text(self):
        """測試包含額外文字的欄位名稱也能對應。"""
        columns = ["公司代號 Code", "基本每股盈餘（元）"]

        result = self.uploader._build_column_mapping(columns)

        self.assertEqual(result["公司代號 Code"], "CompanyCode")
        self.assertEqual(
            result["基本每股盈餘（元）"], "EPS"
        )

    def test_unmatched_columns_not_in_mapping(self):
        """測試無法對應的欄位不在結果中。"""
        columns = ["未知欄位", "公司代號"]

        result = self.uploader._build_column_mapping(columns)

        self.assertNotIn("未知欄位", result)
        self.assertIn("公司代號", result)

    def test_distinguishes_revenue_columns(self):
        """測試正確區分營業收入、營業利益、營業外收入及支出。"""
        columns = [
            "營業收入", "營業利益", "營業外收入及支出",
        ]

        result = self.uploader._build_column_mapping(columns)

        self.assertEqual(result["營業收入"], "Revenue")
        self.assertEqual(result["營業利益"], "OperatingIncome")
        self.assertEqual(
            result["營業外收入及支出"], "NonOperatingIncome"
        )


class TestCrawlData(unittest.TestCase):
    """測試 crawl_data 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        with patch.object(
            QuarterRevenueUploader, "_ensure_tables"
        ):
            self.uploader = QuarterRevenueUploader(self.mock_conn)

    def test_fetch_failure_returns_empty(self):
        """測試 _fetch_html 失敗時回傳空 DataFrame。"""
        self.uploader._fetch_html = MagicMock(
            side_effect=RuntimeError("連線失敗")
        )

        result = self.uploader.crawl_data(113, 1)

        self.assertTrue(result.empty)

    def test_empty_tables_returns_empty(self):
        """測試無表格資料時回傳空 DataFrame。"""
        self.uploader._fetch_html = MagicMock(
            return_value="<html><body>No tables</body></html>"
        )

        result = self.uploader.crawl_data(113, 1)

        self.assertTrue(result.empty)

    def test_merges_multiple_tables(self):
        """測試合併多個產業別表格。"""
        html = """
        <html><body>
        <table>
            <tr><th>公司代號</th><th>公司名稱</th>
                <th>產業別</th><th>營業收入</th></tr>
            <tr><td>2330</td><td>台積電</td>
                <td>半導體業</td><td>100</td></tr>
        </table>
        <table>
            <tr><th>公司代號</th><th>公司名稱</th>
                <th>產業別</th><th>營業收入</th></tr>
            <tr><td>1102</td><td>亞泥</td>
                <td>水泥工業</td><td>200</td></tr>
        </table>
        </body></html>
        """
        self.uploader._fetch_html = MagicMock(return_value=html)

        result = self.uploader.crawl_data(113, 1)

        self.assertEqual(len(result), 2)
        self.assertIn("2330", result["CompanyCode"].values)
        self.assertIn("1102", result["CompanyCode"].values)


class TestUpload(unittest.TestCase):
    """測試 upload 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.mock_conn = MagicMock()
        with patch.object(
            QuarterRevenueUploader, "_ensure_tables"
        ):
            self.uploader = QuarterRevenueUploader(self.mock_conn)

    def test_skips_existing(self):
        """測試已上傳時跳過並回傳 0。"""
        self.uploader.check_uploaded = MagicMock(return_value=True)

        result = self.uploader.upload(113, 1)

        self.assertEqual(result, 0)

    def test_no_data_returns_zero(self):
        """測試無資料時回傳 0。"""
        self.uploader.check_uploaded = MagicMock(return_value=False)
        self.uploader.crawl_data = MagicMock(
            return_value=pd.DataFrame()
        )

        result = self.uploader.upload(113, 1)

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()

"""季度營業收入資料爬取與上傳模組。

從公開資訊觀測站 (MOPS) 抓取上市公司各產業 EPS 統計資訊，
並上傳至 TWSE 資料庫的 QuarterRevenue 表。
使用 Playwright 瀏覽器自動化繞過 MOPS WAF 防護。
"""

import logging
from datetime import datetime
from io import StringIO
from typing import Optional

import pandas as pd
from playwright.sync_api import sync_playwright
from pydantic import BaseModel
from sqlalchemy import text

logger = logging.getLogger(__name__)

# MOPS 頁面 URL（SPA 路由）
MOPS_PAGE_URL = "https://mops.twse.com.tw/mops/#/web/t163sb19"

# 中文欄位模糊對應英文欄位（包含關鍵字即對應）
# 注意：「營業外收入及支出」須在「營業利益」和「營業收入」之前，
# 避免「營業」子字串被提早匹配。
COLUMN_KEYWORD_MAPPING = {
    "公司代號": "CompanyCode",
    "公司名稱": "CompanyName",
    "產業別": "Industry",
    "基本每股盈餘": "EPS",
    "普通股每股面額": "ParValue",
    "營業外收入及支出": "NonOperatingIncome",
    "營業利益": "OperatingIncome",
    "營業收入": "Revenue",
    "稅後淨利": "NetIncome",
}


class QuarterRevenueType(BaseModel):
    """季度營業收入資料 schema。"""

    Year: int
    Season: int
    CompanyCode: str
    TYPEK: str
    CompanyName: Optional[str] = None
    Industry: Optional[str] = None
    EPS: Optional[float] = None
    ParValue: Optional[str] = None
    Revenue: Optional[int] = None
    OperatingIncome: Optional[int] = None
    NonOperatingIncome: Optional[int] = None
    NetIncome: Optional[int] = None


class QuarterRevenueUploader:
    """季度營業收入爬取與上傳器。"""

    def __init__(self, conn):
        """初始化季度營業收入上傳器。

        Args:
            conn: SQLAlchemy 連線物件。
        """
        self.conn = conn
        self._ensure_tables()

    def _ensure_tables(self):
        """確保 QuarterRevenue 與 QuarterRevenueUploaded 資料表存在。

        若偵測到舊版結構（缺少 Season 或 Industry 欄位），
        會先 DROP 再重建。
        """
        rebuild_revenue = False

        # 檢查 QuarterRevenue 表
        try:
            cols = self.conn.execute(
                text("DESCRIBE QuarterRevenue")
            ).fetchall()
            col_names = {row[0] for row in cols}
            if "Season" not in col_names or "Industry" not in col_names:
                logger.info(
                    "偵測到舊版 QuarterRevenue 表結構，重建中..."
                )
                self.conn.execute(text("DROP TABLE QuarterRevenue"))
                self.conn.commit()
                rebuild_revenue = True
        except Exception:
            pass

        # 若 QuarterRevenue 需要重建，也清除上傳記錄
        if rebuild_revenue:
            try:
                self.conn.execute(
                    text("DROP TABLE IF EXISTS QuarterRevenueUploaded")
                )
                self.conn.commit()
            except Exception:
                pass
        else:
            # 單獨檢查 QuarterRevenueUploaded
            try:
                cols = self.conn.execute(
                    text("DESCRIBE QuarterRevenueUploaded")
                ).fetchall()
                col_names = {row[0] for row in cols}
                if "Season" not in col_names:
                    logger.info(
                        "偵測到舊版 QuarterRevenueUploaded "
                        "表結構，重建中..."
                    )
                    self.conn.execute(
                        text("DROP TABLE QuarterRevenueUploaded")
                    )
                    self.conn.commit()
            except Exception:
                pass

        self.conn.execute(text("""
            CREATE TABLE IF NOT EXISTS QuarterRevenue (
                Year INT,
                Season INT,
                CompanyCode VARCHAR(10),
                CompanyName VARCHAR(100),
                Industry VARCHAR(50),
                EPS FLOAT,
                ParValue VARCHAR(50),
                Revenue BIGINT,
                OperatingIncome BIGINT,
                NonOperatingIncome BIGINT,
                NetIncome BIGINT,
                TYPEK VARCHAR(10),
                UNIQUE KEY uq_quarter_revenue (Year, Season, CompanyCode)
            )
        """))
        self.conn.execute(text("""
            CREATE TABLE IF NOT EXISTS QuarterRevenueUploaded (
                Year INT,
                Season INT,
                UploadedAt DATETIME,
                RecordCount INT,
                UNIQUE KEY uq_quarter_uploaded (Year, Season)
            )
        """))
        self.conn.commit()

    def check_uploaded(self, year, season):
        """檢查指定年度季度是否已上傳。

        Args:
            year (int): 民國年。
            season (int): 季度（1-4）。

        Returns:
            bool: 若已上傳回傳 True，否則回傳 False。
        """
        result = self.conn.execute(
            text(
                "SELECT COUNT(*) FROM QuarterRevenueUploaded "
                "WHERE Year = :year AND Season = :season"
            ),
            {"year": year, "season": season},
        ).scalar()
        return result > 0

    def _fetch_html(self, year, season):
        """使用 Playwright 從 MOPS 取得查詢結果 HTML。

        透過瀏覽器自動化繞過 MOPS 的 JavaScript WAF 防護，
        填寫查詢表單後擷取彈出視窗的內容。

        Args:
            year (int): 民國年。
            season (int): 季度（1-4）。

        Returns:
            str: 查詢結果頁面的 HTML 內容。

        Raises:
            RuntimeError: 當無法取得查詢結果時拋出。
        """
        season_str = f"{season:02d}"

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            page.goto(MOPS_PAGE_URL)
            page.wait_for_timeout(5000)

            # 填寫查詢表單
            page.select_option("select#TYPEK", "sii")
            page.fill("input#year", str(year))
            page.select_option("select#season", season_str)

            # 點擊查詢並擷取彈出視窗
            with context.expect_page(timeout=60000) as popup_info:
                page.click("button#searchBtn")

            popup = popup_info.value
            popup.wait_for_load_state("networkidle")
            html = popup.content()

            browser.close()

        return html

    def crawl_data(self, year, season):
        """從 MOPS 爬取季度營業收入資料。

        使用 Playwright 瀏覽器自動化取得資料，
        合併所有產業別表格後回傳。

        Args:
            year (int): 民國年。
            season (int): 季度（1-4）。

        Returns:
            pd.DataFrame: 清理後的營業收入 DataFrame，
                爬取失敗或無資料時回傳空 DataFrame。
        """
        try:
            html = self._fetch_html(year, season)
        except Exception as e:
            logger.error(
                "MOPS 爬取失敗（民國 %d 年第 %d 季）：%s",
                year, season, e,
            )
            return pd.DataFrame()

        try:
            tables = pd.read_html(
                StringIO(html), flavor="lxml"
            )
        except ValueError:
            logger.warning(
                "MOPS 回傳無表格資料（民國 %d 年第 %d 季）",
                year, season,
            )
            return pd.DataFrame()

        if not tables:
            return pd.DataFrame()

        # 合併所有表格（每個表格對應一個產業別）
        all_dfs = []
        for table in tables:
            cleaned = self._clean_dataframe(
                table, year, season, "sii"
            )
            if not cleaned.empty:
                all_dfs.append(cleaned)

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)

    def _clean_dataframe(self, df, year, season, typek):
        """清理從 MOPS 取得的 DataFrame。

        處理多層欄位名稱、移除合計列、轉換欄位名稱、
        替換 '--' 為 NaN、加入年度季度與市場類別欄位。

        Args:
            df (pd.DataFrame): 原始 DataFrame。
            year (int): 民國年。
            season (int): 季度。
            typek (str): 市場類別（sii）。

        Returns:
            pd.DataFrame: 清理後的 DataFrame。
        """
        # 處理多層欄位：將 MultiIndex 合併為單一字串
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(c) for c in col).strip()
                for col in df.columns
            ]

        # 建立欄位對應
        column_mapping = self._build_column_mapping(df.columns.tolist())
        df = df.rename(columns=column_mapping)

        # 僅保留成功對應的欄位
        mapped_cols = [
            c for c in df.columns
            if c in QuarterRevenueType.model_fields
        ]
        df = df[mapped_cols]

        # CompanyCode 必須存在
        if "CompanyCode" not in df.columns:
            logger.warning("找不到 CompanyCode 欄位，無法處理")
            return pd.DataFrame()

        # 將 CompanyCode 轉為字串（read_html 可能解析為整數）
        df["CompanyCode"] = df["CompanyCode"].astype(str)

        # 移除 CompanyCode 非數字開頭的列（合計列、產業別標題列）
        df = df[
            df["CompanyCode"]
            .str.match(r"^\d")
        ].copy()

        # 替換 '--' 為 NaN
        df = df.replace(["--", "－", "―", "—"], pd.NA)

        # 轉換數值欄位
        numeric_cols = [
            "EPS", "Revenue", "OperatingIncome",
            "NonOperatingIncome", "NetIncome",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce",
                )

        # 加入年度、季度、市場類別
        df["Year"] = year
        df["Season"] = season
        df["TYPEK"] = typek

        df = df.reset_index(drop=True)
        return df

    def _build_column_mapping(self, columns):
        """根據模糊匹配建立中文欄位到英文欄位的對應。

        Args:
            columns (list[str]): 原始欄位名稱清單。

        Returns:
            dict: 欄位名稱對應字典（原始名稱 → 英文名稱）。
        """
        mapping = {}
        for col in columns:
            col_str = str(col)
            for keyword, eng_name in COLUMN_KEYWORD_MAPPING.items():
                if keyword in col_str:
                    mapping[col] = eng_name
                    break
        return mapping

    def check_schema(self, df):
        """使用 Pydantic 驗證 DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame。
        """
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
        validated = [
            QuarterRevenueType(**record).model_dump()
            for record in records
        ]
        return pd.DataFrame(validated)

    def upload(self, year, season):
        """執行季度營業收入上傳流程。

        若該年度季度已上傳則跳過，否則爬取資料並上傳至資料庫。

        Args:
            year (int): 民國年。
            season (int): 季度（1-4）。

        Returns:
            int: 上傳的資料筆數，已上傳或無資料時回傳 0。
        """
        if self.check_uploaded(year, season):
            logger.info(
                "民國 %d 年第 %d 季資料已存在，跳過上傳。",
                year, season,
            )
            return 0

        df = self.crawl_data(year, season)

        if df.empty:
            logger.info(
                "民國 %d 年第 %d 季無資料可上傳。",
                year, season,
            )
            return 0

        df = self.check_schema(df)
        record_count = len(df)

        df.to_sql(
            "QuarterRevenue", self.conn,
            if_exists="append", index=False, chunksize=1000,
        )
        self.conn.commit()

        # 記錄已上傳
        self.conn.execute(
            text(
                "INSERT INTO QuarterRevenueUploaded "
                "(Year, Season, UploadedAt, RecordCount) "
                "VALUES (:year, :season, :uploaded_at, :count)"
            ),
            {
                "year": year,
                "season": season,
                "uploaded_at": datetime.now(),
                "count": record_count,
            },
        )
        self.conn.commit()

        logger.info(
            "民國 %d 年第 %d 季資料已上傳，共 %d 筆。",
            year, season, record_count,
        )
        return record_count

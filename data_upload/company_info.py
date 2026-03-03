"""公司產業對照資料上傳模組。

從爬蟲服務取得 TWSE/TPEX 公司基本資料與產業對照表，
並上傳至 TWSE 資料庫的 CompanyInfo 和 IndustryMap 表。
使用 REPLACE INTO 確保資料為最新版本。
"""

import logging

import pandas as pd
import requests
from pydantic import BaseModel
from sqlalchemy import text

logger = logging.getLogger(__name__)


class CompanyInfoType(BaseModel):
    """公司基本資料 schema。

    欄位名稱對應 Tw_stock_DB 的 CompanyInfo 表結構。
    """

    SecurityCode: str
    IndustryCode: str
    CompanyName: str
    SpecialShares: int
    NormalShares: int
    PrivateShares: int


class IndustryMapType(BaseModel):
    """產業對照 schema。

    欄位名稱對應 Tw_stock_DB 的 IndustryMap 表結構。
    """

    IndustryCode: str
    Industry: str


class CompanyInfoUploader:
    """公司產業對照資料上傳器。

    從爬蟲取得公司基本資料與產業對照表，
    使用 REPLACE INTO 寫入 TWSE 資料庫。
    資料表結構由 Tw_stock_DB 專案負責建立與管理。
    """

    def __init__(self, conn, crawler_host):
        """初始化公司產業對照上傳器。

        Args:
            conn: SQLAlchemy 連線物件（TWSE 資料庫）。
            crawler_host (str): 爬蟲服務主機位址（含 port）。
        """
        self.conn = conn
        self.crawler_host = crawler_host

    def crawl_data(self):
        """從爬蟲服務取得公司產業對照資料。

        Returns:
            dict | None: 包含 company_info 和 industry_map 的字典，
                爬取失敗時回傳 None。
        """
        url = f"http://{self.crawler_host}/company_info"
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.error("公司產業對照爬蟲呼叫失敗：%s", e)
            return None

        result = resp.json()
        data = result.get("data")
        if not data:
            logger.warning("公司產業對照爬蟲回傳空資料")
            return None

        return data

    def check_schema_company_info(self, df):
        """使用 Pydantic 驗證 CompanyInfo DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame。
        """
        records = df.to_dict(orient="records")
        validated = [
            CompanyInfoType(**record).model_dump()
            for record in records
        ]
        return pd.DataFrame(validated)

    def check_schema_industry_map(self, df):
        """使用 Pydantic 驗證 IndustryMap DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame。
        """
        records = df.to_dict(orient="records")
        validated = [
            IndustryMapType(**record).model_dump()
            for record in records
        ]
        return pd.DataFrame(validated)

    def _replace_into(self, table_name, df):
        """使用 REPLACE INTO 批次寫入資料。

        Args:
            table_name (str): 資料表名稱。
            df (pd.DataFrame): 待寫入的 DataFrame。
        """
        if df.empty:
            return

        columns = df.columns.tolist()
        col_str = ", ".join(columns)
        placeholder_str = ", ".join([f":{col}" for col in columns])
        sql = f"REPLACE INTO {table_name} ({col_str}) VALUES ({placeholder_str})"

        records = df.to_dict(orient="records")
        for record in records:
            self.conn.execute(text(sql), record)
        self.conn.commit()

    def upload(self):
        """執行公司產業對照資料上傳流程。

        從爬蟲取得資料，驗證 schema 後以 REPLACE INTO 寫入資料庫。

        Returns:
            dict: 包含 company_info_count 和 industry_map_count 的結果字典。
        """
        data = self.crawl_data()

        if data is None:
            logger.info("公司產業對照無資料可上傳。")
            return {"company_info_count": 0, "industry_map_count": 0}

        # 處理 CompanyInfo
        company_info_raw = data.get("company_info", [])
        if company_info_raw:
            df_company = pd.DataFrame(company_info_raw)
            df_company = self.check_schema_company_info(df_company)
            self._replace_into("CompanyInfo", df_company)
            company_info_count = len(df_company)
            logger.info(
                "CompanyInfo 資料已上傳，共 %d 筆。",
                company_info_count,
            )
        else:
            company_info_count = 0
            logger.warning("爬蟲回傳 company_info 為空。")

        # 處理 IndustryMap（僅取 Market="TWSE" 的部分）
        industry_map_raw = data.get("industry_map", [])
        if industry_map_raw:
            df_industry = pd.DataFrame(industry_map_raw)
            # 僅保留 TWSE 市場的產業對照
            if "Market" in df_industry.columns:
                df_industry = df_industry[
                    df_industry["Market"] == "TWSE"
                ].copy()
                df_industry = df_industry.drop(columns=["Market"])
            df_industry = self.check_schema_industry_map(df_industry)
            self._replace_into("IndustryMap", df_industry)
            industry_map_count = len(df_industry)
            logger.info(
                "IndustryMap 資料已上傳，共 %d 筆。",
                industry_map_count,
            )
        else:
            industry_map_count = 0
            logger.warning("爬蟲回傳 industry_map 為空。")

        return {
            "company_info_count": company_info_count,
            "industry_map_count": industry_map_count,
        }

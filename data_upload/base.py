"""資料上傳抽象基類模組。"""

import os
import logging
import requests
from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


class DataUploadBase(ABC):
    """資料上傳抽象基類。

    定義爬蟲資料的預處理、schema 驗證與上傳流程。
    """

    stock_code_col = None
    stock_name_col = None

    @abstractmethod
    def __init__(self, conn):
        """初始化資料上傳基類。

        Args:
            conn (sqlalchemy.engine.Connection): 資料庫連線物件。
        """
        self.name = os.path.basename(type(self).__module__.split('.')[-1])
        self.conn = conn

    @abstractmethod
    def preprocess(self, df):
        """預處理 DataFrame，上傳前進行資料轉換。"""
        pass

    def craw_data(self, date):
        """根據日期從爬蟲服務取得資料。

        若爬蟲服務回傳異常則回傳空 DataFrame。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。

        Returns:
            pd.DataFrame: 包含每日資料的 DataFrame。
        """
        url = f"{self.url}/{self.name}"
        payload = {"date": date}
        try:
            response = requests.get(url, params=payload)
            response.raise_for_status()
            json_data = response.json()["data"]
            df = pd.DataFrame(json_data)
        except (requests.RequestException, KeyError, ValueError) as e:
            logger.error(f"日期 {date} 爬取失敗：{e}")
            df = pd.DataFrame()
        return df

    def check_schema(self, df):
        """檢查 DataFrame 的 schema 是否符合 UploadType 模型。

        Args:
            df (pd.DataFrame): 待檢查的 DataFrame。

        Returns:
            pd.DataFrame: 經過 schema 驗證與轉換後的 DataFrame。
        """
        df_dict = df.to_dict(orient='records')
        df_schema = [self.UploadType(**record).__dict__ for record in df_dict]
        df = pd.DataFrame(df_schema)
        return df

    def check_date(self, date):
        """檢查該日期是否已存在於 UploadDate 資料表中。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。

        Returns:
            bool: 若日期已存在回傳 True，否則回傳 False。
        """
        if self.conn.execute(
            text(f"SELECT COUNT(*) FROM UploadDate WHERE Date = '{date}'")
        ).scalar():
            return True
        return False

    def register_stock_names(self, df):
        """檢查並註冊新的股票代碼至 StockName 資料表。

        若 stock_code_col 或 stock_name_col 未設定則跳過（如 TAIFEX 無 StockName 表）。
        比對 DataFrame 中的股票代碼與資料庫現有記錄，將新代碼插入 StockName 表。

        Args:
            df (pd.DataFrame): 包含股票代碼與名稱的 DataFrame。
        """
        if self.stock_code_col is None or self.stock_name_col is None:
            return

        new_stocks = df[[self.stock_code_col, self.stock_name_col]].drop_duplicates(
            subset=[self.stock_code_col]
        )

        existing = self.conn.execute(
            text(f"SELECT {self.stock_code_col} FROM StockName")
        ).fetchall()
        existing_codes = {row[0] for row in existing}

        new_stocks = new_stocks[
            ~new_stocks[self.stock_code_col].isin(existing_codes)
        ]

        if new_stocks.empty:
            return

        new_stocks.to_sql(
            "StockName", self.conn,
            if_exists='append', index=False
        )
        self.conn.commit()
        logger.info(
            f"新增 {len(new_stocks)} 筆股票代碼至 StockName："
            f"{new_stocks[self.stock_code_col].tolist()}"
        )

    def upload_df(self, df):
        """上傳每日資料至 DailyPrice 資料表。

        Args:
            df (pd.DataFrame): 包含每日資料的 DataFrame。
        """
        df_copy = self.preprocess(df.copy())
        df_copy = self.check_schema(df_copy)
        df_copy.to_sql(
            "DailyPrice", self.conn,
            if_exists='append', index=False, chunksize=1000
        )
        self.conn.commit()

    def upload_date(self, date, df):
        """上傳日期記錄至 UploadDate 資料表。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。
            df (pd.DataFrame): 該日期的資料 DataFrame，用於判斷是否為交易日。
        """
        if df.shape[0] != 0:
            update = text(
                f"INSERT INTO UploadDate (Date, Open) VALUES ('{date}', True);"
            )
            self.conn.execute(update)
            self.conn.commit()
        else:
            update = text(
                f"INSERT INTO UploadDate (Date, Open) VALUES ('{date}', False);"
            )
            self.conn.execute(update)
            self.conn.commit()

    def upload(self, date):
        """執行上傳流程。

        若該日期資料已存在則跳過，否則爬取資料並上傳至資料庫。

        Args:
            date (str): 日期字串，格式為 YYYY-MM-DD。
        """
        if self.check_date(date):
            logger.info(
                f"日期 {date} 的資料已存在於資料庫中，跳過上傳。"
            )
        else:
            df = self.craw_data(date)
            if df.shape[0] > 0:
                self.register_stock_names(df)
                self.upload_df(df)
            self.upload_date(date, df)
            logger.info(f"日期 {date} 的資料已成功上傳至資料庫。")

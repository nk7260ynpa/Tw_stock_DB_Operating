"""MySQL 連線路由模組。"""

from contextlib import contextmanager

from clients import mysql_conn, mysql_conn_db


class MySQLRouter:
    """MySQL 連線路由類別，封裝資料庫連線邏輯。"""

    def __init__(self, host, user, password, db_name=None):
        """初始化 MySQLRouter。

        Args:
            host (str): MySQL 主機位址。
            user (str): MySQL 使用者名稱。
            password (str): MySQL 密碼。
            db_name (str | None): 資料庫名稱，預設為 None。
        """
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.conn = self._build_mysql_conn()

    def _build_mysql_conn(self):
        """根據參數建立 MySQL 連線。

        若指定 db_name 則連線至該資料庫，否則僅連線至 MySQL 伺服器。

        Returns:
            sqlalchemy.engine.Connection: MySQL 連線物件。
        """
        if self.db_name:
            conn = mysql_conn_db(self.host, self.user, self.password, self.db_name)
        else:
            conn = mysql_conn(self.host, self.user, self.password)
        return conn

    @property
    def mysql_conn(self):
        """取得 MySQL 連線物件。

        Returns:
            sqlalchemy.engine.Connection: MySQL 連線物件。

        Note:
            正式程式碼請改用 `db_conn` context manager，勿自行取用本屬性後
            手動 `close()`——例外路徑會漏關而洩漏連線。

        Example:
            >>> with db_conn(host, user, password, db_name) as conn:
            ...     conn.execute(text("SELECT 1"))
        """
        return self.conn


@contextmanager
def db_conn(host, user, password, db_name=None):
    """以 context manager 取得 MySQL 連線，離開時保證關閉。

    這是本專案取得連線的**唯一**建議方式。直接呼叫 `MySQLRouter(...).mysql_conn`
    的寫法只在成功路徑執行 `conn.close()`，任何例外（爬蟲失敗、SQL 錯誤、
    KeyError 等）都會讓連線一路洩漏到垃圾回收為止；重試佇列一輪可能執行數十個
    任務，累積下來足以耗盡 MySQL 的 `max_connections`。

    Args:
        host (str): MySQL 主機位址。
        user (str): MySQL 使用者名稱。
        password (str): MySQL 密碼。
        db_name (str | None): 資料庫名稱，預設為 None（不指定資料庫）。

    Yields:
        sqlalchemy.engine.Connection: MySQL 連線物件。

    Example:
        >>> with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
        ...     conn.execute(text("SELECT 1"))
    """
    conn = MySQLRouter(host, user, password, db_name).mysql_conn
    try:
        yield conn
    finally:
        conn.close()

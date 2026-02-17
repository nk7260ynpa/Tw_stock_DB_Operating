from clients import mysql_conn, mysql_conn_db


class MySQLRouter:
    """MySQL 連線路由類別，封裝資料庫連線邏輯。"""

    def __init__(self, host, user, password, db_name=None):
        """初始化 MySQLRouter。

        Args:
            host: MySQL 主機位址。
            user: MySQL 使用者名稱。
            password: MySQL 密碼。
            db_name: 資料庫名稱，預設為 None。
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
            MySQL 連線物件。
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
            MySQL 連線物件。

        Example:
            >>> router = MySQLRouter(host, user, password, db_name)
            >>> conn = router.mysql_conn
            >>> conn.execute("SELECT 1")
            >>> conn.close()
        """
        return self.conn

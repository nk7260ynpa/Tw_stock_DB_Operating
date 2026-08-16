"""MySQL 連線函式模組。

本模組每次呼叫都會建立**全新的 Engine**，該 Engine 用完即棄、不會被重複使用，
故一律指定 `NullPool`：預設的 `QueuePool` 會在 `conn.close()` 後把實體連線留在
池中「備用」，但這個池永遠不會有第二個使用者，等於每呼叫一次就長期佔住一條
MySQL 連線，直到 Engine 被垃圾回收才釋放。改用 `NullPool` 後 `conn.close()`
即真正關閉實體連線。
"""

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


def mysql_conn(host, user, password):
    """建立不指定資料庫的 MySQL 連線。

    Args:
        host (str): MySQL 主機位址。
        user (str): MySQL 使用者名稱。
        password (str): MySQL 密碼。

    Returns:
        sqlalchemy.engine.Connection: MySQL 連線物件。

    Example:
        >>> conn = mysql_conn("localhost:3306", "root", "password")
        >>> conn.execute("SELECT 1")
        >>> conn.close()
    """
    address = f"mysql+pymysql://{user}:{password}@{host}"
    engine = create_engine(address, poolclass=NullPool)
    conn = engine.connect()
    return conn


def mysql_conn_db(host, user, password, db_name):
    """建立指定資料庫的 MySQL 連線。

    Args:
        host (str): MySQL 主機位址。
        user (str): MySQL 使用者名稱。
        password (str): MySQL 密碼。
        db_name (str): 資料庫名稱。

    Returns:
        sqlalchemy.engine.Connection: MySQL 連線物件。

    Example:
        >>> conn = mysql_conn_db("localhost:3306", "root", "password", "TWSE")
        >>> conn.execute("SELECT 1")
        >>> conn.close()
    """
    address = f"mysql+pymysql://{user}:{password}@{host}/{db_name}"
    engine = create_engine(address, poolclass=NullPool)
    conn = engine.connect()
    return conn

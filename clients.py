"""MySQL 連線函式模組。"""

from sqlalchemy import create_engine


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
    engine = create_engine(address)
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
    engine = create_engine(address)
    conn = engine.connect()
    return conn

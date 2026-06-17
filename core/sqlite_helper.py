import sqlite3
import os
from typing import Optional, List, Dict, Any


DB_DIR = os.path.join(os.getcwd(), "data")
DB_FILE = os.path.join(os.getcwd(), "device_sqlite.db")

CREATE_TABLE_SQL_DEVICE_CACHE = """
CREATE TABLE IF NOT EXISTS device_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dev_id TEXT NOT NULL,
    temp REAL,
    hum REAL,
    collect_time TEXT,
    UNIQUE(dev_id, collect_time)
);
"""
CREATE_TABLE_SQL_MODBUS_OFFLINE = """
CREATE TABLE IF NOT EXISTS modbus_offline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_time INT NOT NULL,
    json_content TEXT NOT NULL
);
"""

def init_sqlite():
    """
    初始化SQLite：目录不存在则创建，表不存在则创建
    """
    os.makedirs(DB_DIR, exist_ok=True)

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL_DEVICE_CACHE)
        cursor.execute(CREATE_TABLE_SQL_MODBUS_OFFLINE)
        conn.commit()
    finally:
        if conn:
            conn.close()

def get_db_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def modbus_to_sqlite(record_time: int, json_str: str):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
        INSERT INTO modbus_offline (record_time, json_content)
        VALUES (?, ?)
        """
        cur.execute(sql, (record_time, json_str))
        conn.commit()
        conn.close()
        print(f"✅ 数据写入 SQLite | 时间: {record_time}")
    except Exception as e:
        print(f"❌ 写入 modbus_offline 失败: {e}")

def query_modbus_offline() -> List[Dict[str, Any]]:
    """
    查询离线Modbus缓存表最新100条
    :return: 列表，每条为字典行数据
    """
    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
        SELECT * FROM modbus_offline ORDER BY id DESC LIMIT 100
        """
        # SQL无?/%s占位符，不需要传参，删除多余元组
        cur.execute(sql)
        # 读取全部结果
        rows = cur.fetchall()
        # 把字段名和值组装成字典（方便业务读取）
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        return result

    except Exception as e:
        print(f"❌ 读取 modbus_offline 失败: {e}")
        return []
    finally:
        # 无论正常/异常，强制释放游标、关闭连接，防止连接泄露
        if cur:
            cur.close()
        if conn:
            conn.close()

def delete_modbus_offline(record_id: int) -> bool:
    """根据id删除单条离线缓存记录"""
    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = "DELETE FROM modbus_offline WHERE id = ?"
        cur.execute(sql, (record_id,))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ 删除离线记录失败 id={record_id}: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
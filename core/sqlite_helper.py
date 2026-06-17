import sqlite3
import os
from typing import Optional


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
    record_time TEXT NOT NULL,
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

def modbus_to_sqlite(record_time: str, json_str: str):
    print(DB_DIR)
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
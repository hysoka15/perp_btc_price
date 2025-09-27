import sqlite3
import logging
import threading
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PriceDatabase:
    """价格数据库管理器 - 存储多个交易所的BTC价格数据"""
    
    def __init__(self, db_path: str = "price_data.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 创建价格数据表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL DEFAULT 'BTCUSDT',
                    price REAL NOT NULL,
                    price_diff REAL,
                    binance_base_price REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, exchange, symbol)
                )
                """)
                
                # 创建交易所状态表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS exchange_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL UNIQUE,
                    last_update DATETIME,
                    last_price REAL,
                    status TEXT DEFAULT 'active',
                    error_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 创建索引优化查询
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_timestamp 
                ON price_data(timestamp DESC)
                """)
                
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_exchange_timestamp 
                ON price_data(exchange, timestamp DESC)
                """)
                
                conn.commit()
                logger.info("数据库初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库连接错误: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def insert_price_data(self, exchange: str, price: float, binance_price: Optional[float] = None, 
                         timestamp: Optional[datetime] = None, symbol: str = "BTCUSDT") -> bool:
        """插入价格数据"""
        try:
            if timestamp is None:
                timestamp = datetime.now()
            
            price_diff = None
            if binance_price is not None:
                price_diff = price - binance_price
            
            with self.lock:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                    INSERT OR REPLACE INTO price_data 
                    (timestamp, exchange, symbol, price, price_diff, binance_base_price)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, (timestamp, exchange, symbol, price, price_diff, binance_price))
                    
                    # 更新交易所状态
                    cursor.execute("""
                    INSERT OR REPLACE INTO exchange_status 
                    (exchange, last_update, last_price, status, error_count)
                    VALUES (?, ?, ?, 'active', 0)
                    """, (exchange, timestamp, price))
                    
                    conn.commit()
                    
            logger.debug(f"插入价格数据: {exchange} {price} (基准: {binance_price}, 价差: {price_diff})")
            return True
            
        except Exception as e:
            logger.error(f"插入价格数据失败 {exchange}: {str(e)}")
            self.update_exchange_error(exchange, str(e))
            return False
    
    def update_exchange_error(self, exchange: str, error_msg: str):
        """更新交易所错误状态"""
        try:
            with self.lock:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                    INSERT OR REPLACE INTO exchange_status 
                    (exchange, last_error, error_count, status)
                    VALUES (?, ?, 
                        COALESCE((SELECT error_count FROM exchange_status WHERE exchange = ?), 0) + 1,
                        'error')
                    """, (exchange, error_msg, exchange))
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"更新交易所错误状态失败 {exchange}: {str(e)}")
    
    def get_latest_prices(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取最新的价格数据"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT timestamp, exchange, symbol, price, price_diff, binance_base_price
                FROM price_data
                ORDER BY timestamp DESC
                LIMIT ?
                """, (limit,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"获取最新价格数据失败: {str(e)}")
            return []
    
    def get_price_history(self, exchange: str, hours: int = 24) -> List[Dict[str, Any]]:
        """获取指定交易所的价格历史"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT timestamp, exchange, price, price_diff, binance_base_price
                FROM price_data
                WHERE exchange = ? 
                  AND timestamp >= datetime('now', '-{} hours')
                ORDER BY timestamp DESC
                """.format(hours), (exchange,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"获取价格历史失败 {exchange}: {str(e)}")
            return []
    
    def get_price_comparison_data(self, hours: int = 24) -> Dict[str, List[Dict[str, Any]]]:
        """获取价格对比数据，按交易所分组"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT timestamp, exchange, price, price_diff, binance_base_price
                FROM price_data
                WHERE timestamp >= datetime('now', '-{} hours')
                ORDER BY timestamp DESC, exchange
                """.format(hours))
                
                results = cursor.fetchall()
                
                # 按交易所分组
                grouped_data = {}
                for row in results:
                    exchange = row['exchange']
                    if exchange not in grouped_data:
                        grouped_data[exchange] = []
                    grouped_data[exchange].append(dict(row))
                
                return grouped_data
                
        except Exception as e:
            logger.error(f"获取价格对比数据失败: {str(e)}")
            return {}
    
    def get_exchange_status(self) -> List[Dict[str, Any]]:
        """获取所有交易所状态"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT exchange, last_update, last_price, status, error_count, last_error
                FROM exchange_status
                ORDER BY exchange
                """)
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"获取交易所状态失败: {str(e)}")
            return []
    
    def cleanup_old_data(self, days: int = 7):
        """清理旧数据，保留指定天数的数据"""
        try:
            with self.lock:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                    DELETE FROM price_data 
                    WHERE timestamp < datetime('now', '-{} days')
                    """.format(days))
                    
                    deleted_rows = cursor.rowcount
                    conn.commit()
                    
                    if deleted_rows > 0:
                        logger.info(f"清理了 {deleted_rows} 条旧数据")
                    
        except Exception as e:
            logger.error(f"清理旧数据失败: {str(e)}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 总记录数
                cursor.execute("SELECT COUNT(*) as total_records FROM price_data")
                total_records = cursor.fetchone()['total_records']
                
                # 各交易所记录数
                cursor.execute("""
                SELECT exchange, COUNT(*) as count 
                FROM price_data 
                GROUP BY exchange
                """)
                exchange_counts = {row['exchange']: row['count'] for row in cursor.fetchall()}
                
                # 最新记录时间
                cursor.execute("SELECT MAX(timestamp) as latest_time FROM price_data")
                latest_time = cursor.fetchone()['latest_time']
                
                # 时间范围
                cursor.execute("SELECT MIN(timestamp) as earliest_time FROM price_data")
                earliest_time = cursor.fetchone()['earliest_time']
                
                return {
                    'total_records': total_records,
                    'exchange_counts': exchange_counts,
                    'latest_time': latest_time,
                    'earliest_time': earliest_time,
                    'active_exchanges': len(exchange_counts)
                }
                
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return {}

# 全局数据库实例
_db_instance = None
_db_lock = threading.Lock()

def get_database(db_path: str = "price_data.db") -> PriceDatabase:
    """获取数据库单例实例"""
    global _db_instance
    
    with _db_lock:
        if _db_instance is None:
            _db_instance = PriceDatabase(db_path)
        return _db_instance
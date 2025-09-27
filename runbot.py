#!/usr/bin/env python3
"""
EdgeX Futures Trading Bot - Using Official EdgeX Python SDK
"""

import sys
import os
import time
import csv
import logging
import asyncio
import json
import traceback
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import pytz

# Import EdgeX SDK from local folder
from edgex_sdk import Client, OrderSide, WebSocketManager, CancelOrderParams, GetOrderBookDepthParams, GetActiveOrderParams, GetKLineParams


def load_config(config_file: str = 'config.json') -> Dict[str, Any]:
    """Load configuration from JSON file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def round_price_to_step(price: float, step_size: float) -> float:
    """Round price to the nearest step size."""
    if step_size <= 0:
        return price
    
    # Simple and effective
    if step_size == 0.1:
        return round(round(price / step_size) * step_size, 1)
    elif step_size == 0.01:
        return round(round(price / step_size) * step_size, 2)
    else:
        return round(round(price / step_size) * step_size, 8)


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    contract_id: str
    quantity: float
    take_profit_percentage: float
    direction: str
    max_orders: int
    wait_time: int

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[float] = None
    filled_qty: float = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class LowCostInventoryManager:
    """低成本刷量的净敞口管理器 (适用于单向持仓模式)"""
    
    def __init__(self, base_quantity: float):
        self.base_quantity = base_quantity
        # 净敞口分层管理阈值 (单向持仓模式)
        self.inventory_layers = {
            'safe': 2 * base_quantity,      # 安全层：正常交易
            'caution': 4 * base_quantity,   # 警告层：减少同方向开仓
            'danger': 6 * base_quantity,    # 危险层：只做反向开仓
            'emergency': 8 * base_quantity # 紧急层：暂停所有开仓
        }
    
    def get_trading_strategy(self, current_inventory: float) -> str:
        """根据当前库存获取交易策略"""
        abs_inventory = abs(current_inventory)
        
        if abs_inventory < self.inventory_layers['safe']:
            return 'normal'  # 正常双向开仓
        elif abs_inventory < self.inventory_layers['caution']:
            return 'reduce_same_side'  # 减少同向开仓概率50%
        elif abs_inventory < self.inventory_layers['danger']:
            return 'opposite_only'  # 只做反向开仓
        else:
            return 'pause'  # 暂停开仓，等待自然对冲
    
    def get_inventory_direction_bias(self, current_inventory: float) -> str:
        """基于净敞口获取方向偏好 (单向持仓模式)"""
        # 净敞口为正（多头净仓位），倾向于开空减少净敞口
        # 净敞口为负（空头净仓位），倾向于开多减少净敞口
        if current_inventory > self.base_quantity * 2:
            return 'sell'  # 开空减少多头净敞口
        elif current_inventory < -self.base_quantity * 2:
            return 'buy'   # 开多减少空头净敞口
        else:
            return None    # 无偏好，使用原方向
    
    def should_emergency_hedge(self, current_inventory: float) -> bool:
        """判断是否需要紧急对冲"""
        return abs(current_inventory) > self.inventory_layers['emergency']
    
    def get_hedge_batch_size(self, current_inventory: float) -> float:
        """获取对冲批次大小"""
        abs_inventory = abs(current_inventory)
        # 分批对冲，每批3倍基础数量
        return min(self.base_quantity * 3, abs_inventory)


class TradingLogger:
    """Enhanced logging with structured output and error handling."""

    def __init__(self, contract_id: str, log_to_console: bool = False, timezone: str = 'Asia/Shanghai'):
        self.contract_id = contract_id
        self.log_file = f"{contract_id}_transactions_log.csv"
        self.debug_log_file = f"{contract_id}_bot_activity.log"
        self.inventory_log_file = f"{contract_id}_inventory_decisions.log"
        # Use configured timezone for logging timestamps
        self.timezone = pytz.timezone(timezone)
        self.logger = self._setup_logger(log_to_console)
        self._init_inventory_log()

    def _setup_logger(self, log_to_console: bool) -> logging.Logger:
        """Setup the logger with proper configuration."""
        logger = logging.getLogger(f"trading_bot_{self.contract_id}")
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        class TimeZoneFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None, tz=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.tz = tz

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()

        formatter = TimeZoneFormatter(
            "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            tz=self.timezone
        )

        # File handler
        file_handler = logging.FileHandler(self.debug_log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler if requested
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def log(self, message: str, level: str = "INFO"):
        """Log a message with the specified level."""
        if level.upper() == "DEBUG":
            self.logger.debug(message)
        elif level.upper() == "INFO":
            self.logger.info(message)
        elif level.upper() == "WARNING":
            self.logger.warning(message)
        elif level.upper() == "ERROR":
            self.logger.error(message)
        else:
            self.logger.info(message)

    def log_transaction(self, order_id: str, side: str, quantity: float, price: float, status: str):
        """Log a transaction to CSV file."""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, order_id, side, quantity, price, status]

            # Check if file exists to write headers
            file_exists = os.path.isfile(self.log_file)

            with open(self.log_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['Timestamp', 'OrderID', 'Side', 'Quantity', 'Price', 'Status'])
                writer.writerow(row)

        except Exception as e:
            self.log(f"Failed to log transaction: {e}", "ERROR")
    
    def _init_inventory_log(self):
        """初始化库存决策日志文件"""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            if not os.path.isfile(self.inventory_log_file):
                with open(self.inventory_log_file, 'w', encoding='utf-8') as f:
                    f.write("="*100 + "\n")
                    f.write(f"库存管理决策日志 - {self.contract_id} - {timestamp}\n")
                    f.write("="*100 + "\n\n")
        except Exception as e:
            self.log(f"Failed to initialize inventory log: {e}", "ERROR")
    
    def log_inventory_decision(self, decision_data: dict):
        """记录库存管理决策到专门的日志文件"""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            # 格式化决策数据
            log_entry = f"""
[{timestamp}] 库存管理决策
----------------------------------------
当前库存: {decision_data.get('current_inventory', 'N/A'):.2f}
库存策略: {decision_data.get('inventory_strategy', 'N/A')}
配置方向: {decision_data.get('config_direction', 'N/A')}
执行方向: {decision_data.get('trade_direction', 'N/A')}
决策类型: {decision_data.get('decision_type', 'N/A')}
库存利用率: {decision_data.get('inventory_usage', 0):.1f}%
反向原因: {decision_data.get('reverse_reason', 'N/A')}
交易数量: {decision_data.get('quantity', 'N/A')}
----------------------------------------

"""
            
            with open(self.inventory_log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
                
        except Exception as e:
            self.log(f"Failed to log inventory decision: {e}", "ERROR")


class EdgeXTradingBot:
    """EdgeX Futures Trading Bot - Main trading logic using official SDK."""

    def __init__(self, config: TradingConfig, app_config: Dict[str, Any]):
        self.config = config
        self.app_config = app_config
        # Use timezone from config
        timezone = app_config.get('logging', {}).get('timezone', 'Asia/Shanghai')
        self.logger = TradingLogger(config.contract_id, log_to_console=True, timezone=timezone)

        # EdgeX credentials from configuration
        edgex_config = app_config['edgex']
        self.account_id = edgex_config['account_id']
        self.stark_private_key = edgex_config['stark_private_key']
        self.base_url = edgex_config['base_url']
        self.ws_url = edgex_config['ws_url']

        if not self.account_id or not self.stark_private_key:
            raise ValueError("EdgeX account_id and stark_private_key must be set in config.json")

        # Initialize EdgeX client using official SDK
        self.client = Client(
            base_url=self.base_url,
            account_id=int(self.account_id),
            stark_private_key=self.stark_private_key
        )

        # Initialize WebSocket manager using official SDK
        self.ws_manager = WebSocketManager(
            base_url=self.ws_url,
            account_id=int(self.account_id),
            stark_pri_key=self.stark_private_key
        )

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = "PENDING"
        self.order_filled_event = asyncio.Event()
        self.shutdown_requested = False

        # BTC price tracking for volatility check
        self.btc_price_history = []
        self.btc_price_lock = asyncio.Lock()
        
        # BTC price logging configuration
        self.enable_btc_price_logging = app_config.get('logging', {}).get('enable_btc_price_logging', True)
        
        # 初始化库存管理器
        self.inventory_manager = LowCostInventoryManager(config.quantity)
        
        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True
        
        try:
            # Close HTTP client session first
            if hasattr(self, 'client') and self.client:
                self.logger.log("Closing HTTP client session...", "INFO")
                await self.client.close()
            
            # Disconnect WebSocket connections
            if hasattr(self, 'ws_manager'):
                self.logger.log("Disconnecting WebSocket connections...", "INFO")
                self.ws_manager.disconnect_all()
            
            self.logger.log("Graceful shutdown completed", "INFO")
            
        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Parse the message structure
                if isinstance(message, str):
                    message = json.loads(message)
                
                content = message.get("content", {})
                event = content.get("event", "")
                if event == "ORDER_UPDATE":
                    # Extract order data from the nested structure
                    data = content.get('data', {})
                    orders = data.get('order', [])

                    if orders and len(orders) > 0:
                        order = orders[0]  # Get the first order
                        if order.get('contractId') != self.config.contract_id:
                            return
                        order_id = order.get('id')
                        status = order.get('status')
                        if order.get('side') == self.config.close_order_side.upper():
                            order_type = "CLOSE"
                        else:
                            order_type = "OPEN"

                        if status == 'FILLED':
                            if order_type == "OPEN":
                                self.order_filled_event.set()

                            collateral = data.get('collateral', [])
                            if collateral and len(collateral):
                                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                                f"{order.get('size')} @ {order.get('price')}", "INFO")

                                # Log the filled transaction to CSV using log_transaction function
                                order_side = order.get('side', '').lower()  # Convert to lowercase for consistency
                                order_size = float(order.get('size', 0))
                                order_price = float(order.get('price', 0))

                                # Use log_transaction to log the filled order
                                self.logger.log_transaction(
                                    order_id=order_id,
                                    side=order_side,
                                    quantity=order_size,
                                    price=order_price,
                                    status=status
                                )
                        else:
                            self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                            f"{order.get('size')} @ {order.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] No order data found in message", "WARNING")
                else:
                    self.logger.log(f"Unexpected message format: {message.get('type')}", "DEBUG")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Subscribe to order updates
        try:
            private_client = self.ws_manager.get_private_client()
            private_client.on_message("trade-event", order_update_handler)
        except Exception as e:
            self.logger.log(f"Could not add trade-event handler: {e}", "WARNING")
        
        # Set up BTC price tracking
        self._setup_btc_price_tracking()
    
    def _setup_btc_price_tracking(self):
        """Setup BTC price tracking via WebSocket."""
        def btc_price_handler(message: str):
            """Handle BTC price updates for volatility tracking."""
            try:
                import json
                data = json.loads(message)
                
                # Extract ticker data
                content = data.get("content", {})
                ticker_data_list = content.get("data", [])
                
                # Handle both single ticker and list of tickers
                if isinstance(ticker_data_list, list) and ticker_data_list:
                    ticker_data = ticker_data_list[0]  # Take the first ticker
                else:
                    ticker_data = ticker_data_list
                
                if isinstance(ticker_data, dict):
                    contract_id = ticker_data.get("contractId")
                    last_price = ticker_data.get("lastPrice")
                    
                    if contract_id == "10000001" and last_price:  # BTC contract
                        # Use asyncio to schedule the coroutine
                        asyncio.create_task(self._record_btc_price(float(last_price)))
                        
            except Exception as e:
                self.logger.log(f"Error handling BTC price update: {e}", "WARNING")
        
        # 不需要在这里设置handler，在run()方法中通过subscribe_ticker设置
        
        # 初始化价格日志文件
        self._init_btc_price_log()
    
    def _init_btc_price_log(self):
        """初始化BTC价格详细日志文件"""
        from datetime import datetime, timezone
        import os
        
        # 创建logs目录
        os.makedirs('logs', exist_ok=True)
        
        # 初始化BTC价格日志文件（仅当启用时）
        if self.enable_btc_price_logging:
            # 生成日志文件名（包含日期）
            today = datetime.now().strftime('%Y-%m-%d')
            self.btc_price_log_file = f'logs/btc_price_{today}.log'
            
            # 写入日志文件头部信息
            try:
                with open(self.btc_price_log_file, 'a', encoding='utf-8') as f:
                    if os.path.getsize(self.btc_price_log_file) == 0:  # 新文件
                        f.write("="*100 + "\n")
                        f.write(f"BTC价格详细日志 - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                        f.write("="*100 + "\n")
                        f.write("时间戳(UTC) | 本地时间 | Unix时间戳 | BTC价格 | 24h涨跌 | 24h涨跌% | 交易量 | 备注\n")
                        f.write("-"*100 + "\n")
            except Exception as e:
                self.logger.log(f"初始化BTC价格日志文件失败: {e}", "WARNING")
        else:
            self.btc_price_log_file = None
    
    def _log_btc_price_detail(self, price_record, ticker_data):
        """详细记录BTC价格数据到专门的日志文件"""
        # 检查是否启用BTC价格日志
        if not self.enable_btc_price_logging:
            return
            
        try:
            from datetime import datetime, timezone
            
            # 解析时间
            timestamp = price_record['timestamp']
            utc_time = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            local_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # 提取ticker数据
            price = price_record['price']
            price_change = ticker_data.get('priceChange', 'N/A')
            price_change_percent = ticker_data.get('priceChangePercent', 'N/A')
            volume = ticker_data.get('size', 'N/A')
            trades = ticker_data.get('trades', 'N/A')
            
            # 计算当前分钟的开始时间（用于验证）
            minute_start = int(timestamp // 60) * 60
            minute_start_utc = datetime.fromtimestamp(minute_start, timezone.utc).strftime('%Y-%m-%d %H:%M:00')
            
            # 格式化日志条目
            log_entry = (
                f"{utc_time} | {local_time} | {timestamp:.3f} | {price:>8.1f} | "
                f"{price_change:>8s} | {price_change_percent:>8s} | {volume:>10s} | "
                f"分钟:{minute_start_utc} 交易:{trades}\n"
            )
            
            # 写入日志文件
            with open(self.btc_price_log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
                
        except Exception as e:
            self.logger.log(f"记录BTC价格详细日志失败: {e}", "WARNING")
    
    def _log_amplitude_check(self, recent_prices, high_price, low_price, amplitude_1m, price_change_1m, threshold):
        """记录振幅检查的详细信息到日志文件"""
        # 检查是否启用BTC价格日志
        if not self.enable_btc_price_logging:
            return
            
        try:
            from datetime import datetime, timezone
            
            # 写入振幅检查分割线
            with open(self.btc_price_log_file, 'a', encoding='utf-8') as f:
                f.write("-"*50 + " 振幅检查 " + "-"*50 + "\n")
                
                # 检查时间
                check_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                f.write(f"检查时间: {check_time} UTC\n")
                
                # 数据范围
                start_time = datetime.fromtimestamp(recent_prices[0]['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                end_time = datetime.fromtimestamp(recent_prices[-1]['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                f.write(f"数据时间范围: {start_time} ~ {end_time} UTC\n")
                
                # 分钟线边界
                minute_start = int(recent_prices[0]['timestamp'] // 60) * 60
                minute_end = int(recent_prices[-1]['timestamp'] // 60) * 60
                minute_start_str = datetime.fromtimestamp(minute_start, timezone.utc).strftime('%Y-%m-%d %H:%M:00')
                minute_end_str = datetime.fromtimestamp(minute_end, timezone.utc).strftime('%Y-%m-%d %H:%M:00')
                
                if minute_start == minute_end:
                    f.write(f"分钟线: {minute_start_str} UTC (单一分钟)\n")
                else:
                    f.write(f"分钟线跨度: {minute_start_str} ~ {minute_end_str} UTC\n")
                
                # 价格统计
                f.write(f"价格数据点: {len(recent_prices)} 个\n")
                f.write(f"最高价: {high_price:.1f}\n")
                f.write(f"最低价: {low_price:.1f}\n")
                f.write(f"振幅: {amplitude_1m:.1f}u ({price_change_1m:.3f}%)\n")
                f.write(f"阈值: {threshold:.1f}u\n")
                
                # 判断结果
                is_safe = amplitude_1m <= threshold and price_change_1m <= 0.5
                result = "✅ 安全" if is_safe else "❌ 过大"
                f.write(f"结果: {result}\n")
                
                # 详细价格列表
                f.write("详细价格序列:\n")
                for i, record in enumerate(recent_prices):
                    timestamp = record['timestamp']
                    price = record['price']
                    time_str = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%H:%M:%S.%f')[:-3]
                    f.write(f"  {i+1:2d}. {time_str} | {price:8.1f}\n")
                
                f.write("="*110 + "\n\n")
                
        except Exception as e:
            self.logger.log(f"记录振幅检查日志失败: {e}", "WARNING")
    
    async def _record_btc_price(self, price: float):
        """Record BTC price with timestamp for volatility analysis."""
        try:
            async with self.btc_price_lock:
                current_time = time.time()
                
                # Add new price record
                self.btc_price_history.append({
                    'price': price,
                    'timestamp': current_time
                })
                
                # Keep only recent 5 minutes of data (300 seconds)
                cutoff_time = current_time - 300
                self.btc_price_history = [
                    record for record in self.btc_price_history 
                    if record['timestamp'] > cutoff_time
                ]
                
                # Log price tracking (debug level to avoid spam)
                if len(self.btc_price_history) % 50 == 0:  # Log every 50th update
                    self.logger.log(f"BTC价格跟踪: {price}, 历史记录{len(self.btc_price_history)}条", "DEBUG")
                    
        except Exception as e:
            self.logger.log(f"Error recording BTC price: {e}", "ERROR")

    async def place_open_order(self, contract_id: str, quantity: float, direction: str) -> Dict[str, Any]:
        """Place an open order with EdgeX using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                depth_params = GetOrderBookDepthParams(contract_id=contract_id, limit=15)
                order_book = await self.client.quote.get_order_book_depth(depth_params)

                # Debug: log the full response structure
                self.logger.log(f"Order book response structure: {type(order_book)}", "DEBUG")
                if isinstance(order_book, dict):
                    self.logger.log(f"Order book keys: {list(order_book.keys())}", "DEBUG")
                    if 'data' in order_book:
                        self.logger.log(f"Data type: {type(order_book['data'])}", "DEBUG")
                        if isinstance(order_book['data'], list):
                            self.logger.log(f"Data list length: {len(order_book['data'])}", "DEBUG")
                            if order_book['data']:
                                self.logger.log(f"First data item: {order_book['data'][0]}", "DEBUG")

                # Handle the response format: {"code": "SUCCESS", "data": [{"asks": [...], "bids": [...]}]}
                if not isinstance(order_book, dict) or 'data' not in order_book:
                    self.logger.log(f"Unexpected order book response format: {type(order_book)}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Unexpected order book response format'}

                order_book_data = order_book['data']
                if not isinstance(order_book_data, list) or len(order_book_data) == 0:
                    self.logger.log(f"Order book data is not a valid list: {type(order_book_data)}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Order book data is not a valid list'}

                # Get the first (and should be only) order book entry
                order_book_entry = order_book_data[0]
                if not isinstance(order_book_entry, dict):
                    self.logger.log(f"Order book entry is not a dict: {type(order_book_entry)}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Order book entry is not a dict'}

                # Extract bids and asks from the entry
                bids = order_book_entry.get('bids', [])
                asks = order_book_entry.get('asks', [])

                if not bids or not asks:
                    self.logger.log("[OPEN] No bid/ask data available in order book", "ERROR")
                    return {'status': 'error', 'err_msg': 'No bid/ask data available'}

                # Best bid is the highest price someone is willing to buy at
                best_bid = float(bids[0]['price']) if bids and len(bids) > 0 else 0
                # Best ask is the lowest price someone is willing to sell at
                best_ask = float(asks[0]['price']) if asks and len(asks) > 0 else 0

                if best_bid <= 0 or best_ask <= 0:
                    return {'status': 'error', 'err_msg': 'Invalid bid/ask prices'}

                # Calculate order price based on direction using config
                contract_config = self.app_config['contracts'].get(contract_id, {})
                price_delta = contract_config.get('price_delta', 0.01)
                step_size = contract_config.get('step_size', 0.01)

                if direction == 'buy':
                    # For buy orders, place slightly below best ask to ensure execution
                    order_price = best_ask - price_delta
                    side = OrderSide.BUY
                else:
                    # For sell orders, place slightly above best bid to ensure execution
                    order_price = best_bid + price_delta
                    side = OrderSide.SELL

                # Round price to correct step size
                order_price = round_price_to_step(order_price, step_size)

                # Place the order using official SDK (post-only to ensure maker order)
                order_result = await self.client.create_limit_order(
                    contract_id=contract_id,
                    size=str(quantity),
                    price=str(order_price),
                    side=side,
                    post_only=True
                )

                if not order_result or 'data' not in order_result:
                    self.logger.log("[OPEN] Failed to place open order", "ERROR")
                    return {'status': 'error', 'err_msg': 'Failed to place order'}

                # Extract order ID from response
                order_id = order_result['data'].get('orderId')
                if not order_id:
                    return {'status': 'error', 'err_msg': 'No order ID in response'}

                # Check order status after a short delay to see if it was rejected
                await asyncio.sleep(0.01)
                order_info = await self.get_order_info(order_id)

                if order_info and 'data' in order_info:
                    order_data = order_info['data']
                    status = order_data.get('status')

                    if status == 'CANCELED':
                        cancel_reason = order_data.get('cancelReason', 'UNKNOWN')
                        self.logger.log(
                            f"Order {order_id} was canceled. Reason: {cancel_reason}. "
                            f"Retrying... (attempt {retry_count + 1}/{max_retries})",
                            "WARNING"
                        )

                        if retry_count < max_retries - 1:
                            retry_count += 1
                            continue
                        else:
                            self.logger.log("[OPEN] Max retries reached for order placement", "ERROR")
                            return {'status': 'error', 'err_msg': f'Order rejected after {max_retries} attempts'}
                    elif status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                        # Order successfully placed
                        return {
                            'status': 'ok',
                            'data': {
                                'order_id': order_id,
                                'side': side.value,
                                'size': quantity,
                                'price': order_price,
                                'status': status
                            }
                        }
                    else:
                        self.logger.log(f"[OPEN] Order {order_id} has unexpected status: {status}", "WARNING")
                        return {'status': 'error', 'err_msg': f'Unexpected order status: {status}'}
                else:
                    self.logger.log(f"[OPEN] Could not retrieve order info for {order_id}", "WARNING")
                    # Assume order is successful if we can't get info
                    return {
                        'status': 'ok',
                        'data': {
                            'order_id': order_id,
                            'side': side.value,
                            'size': quantity,
                            'price': order_price
                        }
                    }

            except Exception as e:
                self.logger.log(f"[OPEN] Error placing open order (attempt {retry_count + 1}): {e}", "ERROR")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)  # Wait before retry
                    continue
                else:
                    return {'status': 'error', 'err_msg': str(e)}

        return {'status': 'error', 'err_msg': 'Max retries exceeded'}

    async def place_close_order(self, contract_id: str, quantity: float, price: float, side: str) -> Dict[str, Any]:
        """Place a close order with EdgeX using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Get current market prices to adjust order price if needed
                depth_params = GetOrderBookDepthParams(contract_id=contract_id, limit=15)
                order_book = await self.client.quote.get_order_book_depth(depth_params)

                if not isinstance(order_book, dict) or 'data' not in order_book:
                    self.logger.log("[CLOSE] Failed to get order book for close order price adjustment", "ERROR")
                    return {'status': 'error', 'err_msg': 'Failed to get order book'}

                order_book_data = order_book['data']
                if not isinstance(order_book_data, list) or len(order_book_data) == 0:
                    self.logger.log("[CLOSE] Order book data is not valid for close order", "ERROR")
                    return {'status': 'error', 'err_msg': 'Invalid order book data'}

                # Get the first order book entry
                order_book_entry = order_book_data[0]
                bids = order_book_entry.get('bids', [])
                asks = order_book_entry.get('asks', [])

                if not bids or not asks:
                    self.logger.log("[CLOSE] No bid/ask data available for close order", "ERROR")
                    return {'status': 'error', 'err_msg': 'No bid/ask data available'}

                # Get best bid and ask prices
                best_bid = float(bids[0]['price']) if bids and len(bids) > 0 else 0
                best_ask = float(asks[0]['price']) if asks and len(asks) > 0 else 0

                if best_bid <= 0 or best_ask <= 0:
                    self.logger.log(f"[CLOSE] Invalid bid/ask prices for close order: bid={best_bid}, ask={best_ask}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Invalid bid/ask prices'}

                # Convert side string to OrderSide enum
                order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL

                # Adjust order price based on market conditions and side
                contract_config = self.app_config['contracts'].get(contract_id, {})
                price_delta = contract_config.get('price_delta', 0.01)
                step_size = contract_config.get('step_size', 0.01)
                
                adjusted_price = price
                if side.lower() == 'sell':
                    # For sell orders, ensure price is above best bid to be a maker order
                    if price <= best_bid:
                        adjusted_price = best_bid + price_delta
                elif side.lower() == 'buy':
                    # For buy orders, ensure price is below best ask to be a maker order
                    if price >= best_ask:
                        adjusted_price = best_ask - price_delta

                # Round price to correct step size
                adjusted_price = round_price_to_step(adjusted_price, step_size)

                # Place the order using official SDK (post-only to avoid taker fees)
                order_result = await self.client.create_limit_order(
                    contract_id=contract_id,
                    size=str(quantity),
                    price=str(adjusted_price),
                    side=order_side,
                    post_only=True
                )

                if not order_result or 'data' not in order_result:
                    self.logger.log("[CLOSE] Failed to place close order", "ERROR")
                    return {'status': 'error', 'err_msg': 'Failed to place order'}

                # Extract order ID from response
                order_id = order_result['data'].get('orderId')
                if not order_id:
                    return {'status': 'error', 'err_msg': 'No order ID in response'}

                # Check order status after a short delay to see if it was rejected
                await asyncio.sleep(0.01)
                order_info = await self.get_order_info(order_id)

                if order_info and 'data' in order_info:
                    order_data = order_info['data']
                    status = order_data.get('status')

                    if status == 'CANCELED':
                        cancel_reason = order_data.get('cancelReason', 'UNKNOWN')
                        self.logger.log(
                            f"[CLOSE] Close order {order_id} was canceled. Reason: {cancel_reason}. "
                            f"Retrying... (attempt {retry_count + 1}/{max_retries})",
                            "WARNING"
                        )

                        if retry_count < max_retries - 1:
                            retry_count += 1
                            continue
                        else:
                            self.logger.log("[CLOSE] Max retries reached for close order placement", "ERROR")
                            return {'status': 'error', 'err_msg': f'Close order rejected after {max_retries} attempts'}
                    elif status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                        self.logger.log(f"[CLOSE] [{order_id}] Order placed: {quantity} @ {price}", "INFO")
                        # Order successfully placed
                        return {
                            'status': 'ok',
                            'data': {
                                'order_id': order_id,
                                'side': side,
                                'size': quantity,
                                'price': adjusted_price,
                                'status': status
                            }
                        }
                    else:
                        self.logger.log(f"[CLOSE] Close order {order_id} has unexpected status: {status}", "WARNING")
                        return {'status': 'error', 'err_msg': f'Unexpected close order status: {status}'}
                else:
                    self.logger.log(f"[CLOSE] Could not retrieve close order info for {order_id}", "WARNING")
                    # Assume order is successful if we can't get info
                    return {
                        'status': 'ok',
                        'data': {
                            'order_id': order_id,
                            'side': side,
                            'size': quantity,
                            'price': adjusted_price
                        }
                    }

            except Exception as e:
                self.logger.log(f"[CLOSE] Error placing close order (attempt {retry_count + 1}): {e}", "ERROR")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)  # Wait before retry
                    continue
                else:
                    return {'status': 'error', 'err_msg': str(e)}

        return {'status': 'error', 'err_msg': 'Max retries exceeded for close order'}

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order with EdgeX using official SDK."""
        try:
            # Create cancel parameters using official SDK
            cancel_params = CancelOrderParams(order_id=order_id)

            # Cancel the order using official SDK
            cancel_result = await self.client.cancel_order(cancel_params)

            if not cancel_result or 'data' not in cancel_result:
                self.logger.log(f"Failed to cancel order {order_id}", "ERROR")
                return {'status': 'error', 'err_msg': 'Failed to cancel order'}

            return {'status': 'ok', 'data': cancel_result}

        except Exception as e:
            self.logger.log(f"Error canceling order {order_id}: {e}", "ERROR")
            return {'status': 'error', 'err_msg': str(e)}

    async def get_order_info(self, order_id: str) -> Dict[str, Any]:
        """Get order information from EdgeX using official SDK."""
        try:
            # Use the newly created get_order_by_id method
            order_result = await self.client.order.get_order_by_id(order_id_list=[order_id])

            if not order_result or 'data' not in order_result:
                return {}

            # The API returns a list of orders, get the first (and should be only) one
            order_list = order_result['data']
            if order_list and len(order_list) > 0:
                return {'data': order_list[0]}

            return {}

        except Exception as e:
            self.logger.log(f"Error getting order info for {order_id}: {e}", "ERROR")
            return {}

    async def get_active_orders(self, contract_id: str) -> List[Dict[str, Any]]:
        """Get active orders for a contract using official SDK."""
        try:
            # Get active orders using official SDK
            params = GetActiveOrderParams(size="100", offset_data="")
            active_orders = await self.client.get_active_orders(params)

            if not active_orders or 'data' not in active_orders:
                return []

            # Filter orders for the specific contract and ensure they are dictionaries
            # The API returns orders under 'dataList' key, not 'orderList'
            order_list = active_orders['data'].get('dataList', [])
            contract_orders = []

            for order in order_list:
                if isinstance(order, dict) and order.get('contractId') == contract_id:
                    contract_orders.append(order)

            return contract_orders

        except Exception as e:
            self.logger.log(f"Error getting active orders: {e}", "ERROR")
            return []

    async def get_account_positions(self) -> Dict[str, Any]:
        """Get account positions using official SDK."""
        try:
            positions = await self.client.get_account_positions()
            return positions
        except Exception as e:
            self.logger.log(f"Error getting positions: {e}", "ERROR")
            return {}
    
    async def get_current_inventory(self) -> float:
        """获取当前合约的净仓位（库存）"""
        try:
            positions_data = await self.get_account_positions()
            
            if not positions_data or 'data' not in positions_data:
                return 0.0
            
            positions = positions_data.get('data', {}).get('positionList', [])
            if not positions:
                return 0.0
            
            # 找到当前合约的仓位
            for position in positions:
                if isinstance(position, dict) and position.get('contractId') == self.config.contract_id:
                    open_size = float(position.get('openSize', 0))
                    # 返回带符号的仓位：多头为正，空头为负
                    side = position.get('side', 'LONG')
                    if side == 'SHORT':
                        open_size = -open_size
                    return open_size
            
            return 0.0
            
        except Exception as e:
            self.logger.log(f"Error getting current inventory: {e}", "ERROR")
            return 0.0
    
    async def place_hedge_order(self, direction: str, quantity: float, aggressive: bool = False) -> Dict[str, Any]:
        """使用maker单进行库存对冲"""
        try:
            # 获取市场深度
            depth_params = GetOrderBookDepthParams(contract_id=self.config.contract_id, limit=15)
            order_book = await self.client.quote.get_order_book_depth(depth_params)
            
            if not isinstance(order_book, dict) or 'data' not in order_book:
                return {'status': 'error', 'err_msg': 'Failed to get order book for hedge'}
            
            order_book_data = order_book['data']
            if not isinstance(order_book_data, list) or len(order_book_data) == 0:
                return {'status': 'error', 'err_msg': 'Invalid order book data for hedge'}
            
            order_book_entry = order_book_data[0]
            bids = order_book_entry.get('bids', [])
            asks = order_book_entry.get('asks', [])
            
            if not bids or not asks:
                return {'status': 'error', 'err_msg': 'No bid/ask data for hedge'}
            
            best_bid = float(bids[0]['price'])
            best_ask = float(asks[0]['price'])
            
            # 获取合约配置
            contract_config = self.app_config['contracts'].get(self.config.contract_id, {})
            step_size = contract_config.get('step_size', 0.01)
            
            # 设置对冲价格
            if direction == 'sell':
                if aggressive:
                    # 激进对冲：在买二价或买三价
                    if len(bids) >= 3:
                        hedge_price = float(bids[2]['price'])  # 买三价
                    elif len(bids) >= 2:
                        hedge_price = float(bids[1]['price'])  # 买二价
                    else:
                        hedge_price = best_bid - step_size     # 买一下方一档
                else:
                    # 保守对冲：在买一价上方
                    hedge_price = best_bid + step_size
                side = OrderSide.SELL
            else:  # buy
                if aggressive:
                    # 激进对冲：在卖二价或卖三价
                    if len(asks) >= 3:
                        hedge_price = float(asks[2]['price'])  # 卖三价
                    elif len(asks) >= 2:
                        hedge_price = float(asks[1]['price'])  # 卖二价
                    else:
                        hedge_price = best_ask + step_size     # 卖一上方一档
                else:
                    # 保守对冲：在卖一价下方
                    hedge_price = best_ask - step_size
                side = OrderSide.BUY
            
            # 调整价格到步长
            hedge_price = round_price_to_step(hedge_price, step_size)
            
            self.logger.log(f"[HEDGE] 下对冲单: {direction} {quantity} @ {hedge_price} ({'激进' if aggressive else '保守'})", "INFO")
            
            # 下单
            order_result = await self.client.create_limit_order(
                contract_id=self.config.contract_id,
                size=str(quantity),
                price=str(hedge_price),
                side=side,
                post_only=True
            )
            
            if not order_result or 'data' not in order_result:
                return {'status': 'error', 'err_msg': 'Failed to place hedge order'}
            
            order_id = order_result['data'].get('orderId')
            if not order_id:
                return {'status': 'error', 'err_msg': 'No order ID in hedge response'}
            
            return {
                'status': 'ok',
                'data': {
                    'order_id': order_id,
                    'side': direction,
                    'size': quantity,
                    'price': hedge_price,
                    'type': 'hedge'
                }
            }
            
        except Exception as e:
            self.logger.log(f"Error placing hedge order: {e}", "ERROR")
            return {'status': 'error', 'err_msg': str(e)}
    
    async def emergency_inventory_management(self) -> bool:
        """紧急库存管理 - 分批用maker单处理大库存"""
        try:
            current_inventory = await self.get_current_inventory()
            
            if not self.inventory_manager.should_emergency_hedge(current_inventory):
                return False
            
            self.logger.log(f"触发紧急库存管理：当前库存 {current_inventory:.2f}", "WARNING")
            
            # 确定对冲方向
            hedge_direction = 'sell' if current_inventory > 0 else 'buy'
            
            # 分批处理
            batch_size = self.inventory_manager.get_hedge_batch_size(current_inventory)
            self.logger.log(f"开始分批对冲，批次大小: {batch_size}", "INFO")
            
            # 下激进的对冲单
            hedge_result = await self.place_hedge_order(hedge_direction, batch_size, aggressive=True)
            
            if hedge_result.get('status') == 'ok':
                self.logger.log(f"紧急对冲订单已下达: {hedge_result['data']}", "INFO")
                return True
            else:
                self.logger.log(f"紧急对冲失败: {hedge_result}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log(f"紧急库存管理异常: {e}", "ERROR")
            return False

    async def check_btc_volatility_safe(self, max_amplitude_usdt: float = 105.0) -> bool:
        """
        检查BTC 1分钟内的实时价格振幅是否在安全范围内。
        使用WebSocket收集的实时价格数据计算最近1分钟的振幅。
        
        Args:
            max_amplitude_usdt: 最大允许振幅（USDT），默认50
            
        Returns:
            bool: True表示振幅安全可以下单，False表示振幅过大应暂停下单
        """
        try:
            current_time = time.time()
            
            # 如果没有价格数据，回退到24小时数据检查
            if not self.btc_price_history:
                self.logger.log("无BTC实时价格数据，使用24小时数据检查", "WARNING")
                return await self._check_volatility_fallback(max_amplitude_usdt)
            
            # 过滤出最近1分钟的价格数据
            one_minute_ago = current_time - 60
            recent_prices = [
                record for record in self.btc_price_history 
                if record['timestamp'] > one_minute_ago
            ]
                
            if len(recent_prices) < 2:
                self.logger.log(f"BTC 1分钟内价格数据不足({len(recent_prices)}条)，使用24小时数据检查", "WARNING")
                return await self._check_volatility_fallback(max_amplitude_usdt)
            
            # 计算1分钟内的最高价和最低价
            prices = [record['price'] for record in recent_prices]
            high_price = max(prices)
            low_price = min(prices)
            latest_price = prices[-1]
            
            # 计算绝对振幅
            amplitude_1m = abs(high_price - low_price)
            
            # 计算价格变化百分比
            price_change_1m = abs(high_price - low_price) / latest_price * 100
            
            # 计算时间范围用于日志显示
            from datetime import datetime, timezone
            start_time = datetime.fromtimestamp(recent_prices[0]['timestamp'], timezone.utc).strftime('%H:%M:%S')
            end_time = datetime.fromtimestamp(recent_prices[-1]['timestamp'], timezone.utc).strftime('%H:%M:%S')
            minute_range = f"{start_time}-{end_time}"
            
            self.logger.log(
                f"BTC 1m振幅检查: 时间范围={minute_range}UTC, 最高={high_price:.1f}, 最低={low_price:.1f}, "
                f"振幅={amplitude_1m:.1f}u({price_change_1m:.3f}%), 数据点={len(recent_prices)}条", 
                "INFO"
            )
            
            # 同时写入价格日志文件
            self._log_amplitude_check(recent_prices, high_price, low_price, amplitude_1m, price_change_1m, max_amplitude_usdt)
            
            # 判断振幅是否安全
            volatility_high = (
                amplitude_1m > max_amplitude_usdt or  # 绝对振幅超过阈值
                price_change_1m > 0.5  # 1分钟价格变化超过0.5%
            )
            
            if volatility_high:
                self.logger.log(
                    f"BTC 1分钟振幅过大({amplitude_1m:.1f}u > {max_amplitude_usdt}u 或 {price_change_1m:.3f}% > 0.5%)，暂停下单", 
                    "WARNING"
                )
                return False
            else:
                self.logger.log("BTC 1分钟振幅安全，可以下单", "INFO")
                return True
                    
        except Exception as e:
            self.logger.log(f"BTC实时振幅检查异常: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            # 异常时回退到24小时数据检查
            return await self._check_volatility_fallback(max_amplitude_usdt)
    
    async def _check_volatility_fallback(self, max_amplitude_usdt: float) -> bool:
        """回退的24小时数据波动检查（简化版本）。"""
        try:
            # 获取24小时行情数据
            quote_result = await self.client.quote.get_24_hour_quote("10000001")
            
            if quote_result and 'data' in quote_result and quote_result.get('code') == 'SUCCESS':
                quote_data = quote_result['data']
                if quote_data and isinstance(quote_data, list) and len(quote_data) > 0:
                    ticker = quote_data[0]
                    
                    price_change_percent = float(ticker.get('priceChangePercent', 0))
                    
                    # 简单检查：如果24小时涨跌幅超过3%，认为波动过大
                    if abs(price_change_percent) > 0.03:
                        self.logger.log(f"BTC 24小时涨跌幅过大({price_change_percent*100:.2f}%)，暂停下单", "WARNING")
                        return False
                    else:
                        self.logger.log("BTC 24小时涨跌幅正常，可以下单", "INFO")
                        return True
            
            # 如果无法获取数据，默认允许下单
            self.logger.log("无法获取BTC波动数据，默认允许下单", "WARNING")
            return True
            
        except Exception as e:
            self.logger.log(f"回退波动检查异常: {e}", "ERROR")
            return True

    def _calculate_wait_time(self) -> float:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _place_and_monitor_open_order(self, direction: Optional[str] = None) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()

            # Use provided direction or default
            trade_direction = direction or self.config.direction

            # Place the order
            order = await self.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                trade_direction
            )

            if order.get('status') != 'ok':
                self.logger.log(f"Failed to place order: {order}", "ERROR")
                return False

            self.last_open_order_time = time.time()

            # Wait for fill or timeout
            if order.get('data').get('status') != 'FILLED':
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            return False

    async def _handle_order_result(self, order: Dict[str, Any]) -> bool:
        """Handle the result of an order placement."""
        order_id = order['data']['order_id']

        # Get current order status
        order_info = await self.get_order_info(order_id)

        # Extract status from EdgeX response
        status = order_info.get('data', {}).get('status', 'UNKNOWN')

        if status == 'FILLED':
            self.current_order_status = "FILLED"

            # Place close order with percentage-based take profit
            filled_price = float(order_info['data'].get('price', 0))
            if filled_price > 0:
                # 根据实际开仓方向确定止盈方向
                actual_open_side = order_info['data'].get('side', '').lower()
                if actual_open_side == 'buy':
                    # 开多了，应该卖出止盈
                    close_side = 'sell'
                    close_price = filled_price * (1 + self.config.take_profit_percentage)
                elif actual_open_side == 'sell':
                    # 开空了，应该买入止盈
                    close_side = 'buy'
                    close_price = filled_price * (1 - self.config.take_profit_percentage)
                else:
                    # 如果无法确定方向，使用配置的默认值
                    close_side = self.config.close_order_side
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit_percentage)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit_percentage)
                
                self.logger.log(f"[OPEN] [{order_id}] Order placed and FILLED: {self.config.quantity} @ {filled_price} ({actual_open_side})", "INFO")
                self.logger.log(f"[CLOSE] 下止盈单: {actual_open_side} → {close_side} @ {close_price:.2f}", "INFO")
                
                close_order = await self.place_close_order(
                    self.config.contract_id,
                    self.config.quantity,
                    close_price,
                    close_side
                )

                if close_order.get('status') != 'ok':
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order}", "ERROR")

            return True

        elif status in ['OPEN', 'PARTIALLY_FILLED']:
            # Cancel the order if it's still open
            try:
                cancel_result = await self.cancel_order(order_id)
                if cancel_result.get('status') == 'ok':
                    self.current_order_status = "CANCELED"
                else:
                    self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result}", "ERROR")

            except Exception as e:
                self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

            order_info = await self.get_order_info(order_id)
            filled_amount = float(order_info['data'].get('cumFillSize', 0))
            filled_price = float(order_info['data'].get('price', 0))
            actual_open_side = order_info['data'].get('side', '').lower()
            
            self.logger.log(f"[OPEN] [{order_id}] Order placed and PARTIALLY FILLED: {filled_amount} @ {filled_price} ({actual_open_side})", "INFO")
            if filled_amount > 0:
                # 根据实际开仓方向确定止盈方向
                if actual_open_side == 'buy':
                    # 开多了，应该卖出止盈
                    close_side = 'sell'
                    close_price = filled_price * (1 + self.config.take_profit_percentage)
                elif actual_open_side == 'sell':
                    # 开空了，应该买入止盈
                    close_side = 'buy'
                    close_price = filled_price * (1 - self.config.take_profit_percentage)
                else:
                    # 如果无法确定方向，使用配置的默认值
                    close_side = self.config.close_order_side
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit_percentage)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit_percentage)
                
                self.logger.log(f"[CLOSE] 部分成交止盈: {actual_open_side} → {close_side} @ {close_price:.2f}, 数量: {filled_amount}", "INFO")
                
                close_order = await self.place_close_order(
                    self.config.contract_id,
                    filled_amount,
                    close_price,
                    close_side
                )

                if close_order.get('status') != 'ok':
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order}", "ERROR")

            return True

        return False
    
    def _get_trade_direction_with_inventory_control(self, current_inventory: float, inventory_strategy: str) -> Optional[str]:
        """根据库存控制策略选择交易方向"""
        import random
        
        try:
            if inventory_strategy == 'normal':
                # 正常策略：使用配置的方向
                return self.config.direction
            
            elif inventory_strategy == 'reduce_same_side':
                # 减少同向开仓：50%概率做反向
                inventory_bias = self.inventory_manager.get_inventory_direction_bias(current_inventory)
                if inventory_bias and random.random() < 0.5:
                    self.logger.log(f"🔄 [NET POSITION CONTROL] 触发净敞口控制策略", "INFO")
                    self.logger.log(f"📊 当前净敞口: {current_inventory:.2f}, 策略: {inventory_strategy}", "INFO") 
                    self.logger.log(f"🎯 原方向: {self.config.direction} → 调整方向: {inventory_bias}", "INFO")
                    self.logger.log(f"📈 目标: 通过反向开仓减少净敞口 (单向持仓模式)", "INFO")
                    return inventory_bias
                else:
                    self.logger.log(f"📊 库存策略: {inventory_strategy}, 当前库存: {current_inventory:.2f}, 继续原方向: {self.config.direction}", "INFO")
                    return self.config.direction
            
            elif inventory_strategy == 'opposite_only':
                # 只做反向开仓
                inventory_bias = self.inventory_manager.get_inventory_direction_bias(current_inventory)
                if inventory_bias:
                    self.logger.log(f"⚠️ [RISK CONTROL] 净敞口达到危险层级，强制反向开仓", "WARNING")
                    self.logger.log(f"📊 当前净敞口: {current_inventory:.2f} (危险阈值: {self.inventory_manager.inventory_layers['danger']:.1f})", "WARNING")
                    self.logger.log(f"🚫 禁止原方向: {self.config.direction}", "WARNING")
                    self.logger.log(f"🎯 强制反向: {inventory_bias}", "WARNING")
                    self.logger.log(f"🛡️ 风控目标: 强制减少净敞口 (单向持仓)", "WARNING")
                    return inventory_bias
                else:
                    # 如果库存不大但策略要求反向，跳过交易
                    self.logger.log(f"⏸️ [SKIP] 库存策略要求反向但当前库存不足以确定方向，跳过交易", "INFO")
                    return None
            
            elif inventory_strategy == 'pause':
                # 暂停交易
                self.logger.log(f"🛑 [EMERGENCY] 净敞口达到紧急层级，暂停所有开仓", "ERROR")
                self.logger.log(f"📊 当前净敞口: {current_inventory:.2f} (紧急阈值: {self.inventory_manager.inventory_layers['emergency']:.1f})", "ERROR")
                self.logger.log(f"⚡ 紧急敞口减少机制已启动", "ERROR")
                return None
            
            else:
                # 未知策略，使用默认方向
                self.logger.log(f"未知库存策略: {inventory_strategy}，使用默认方向", "WARNING")
                return self.config.direction
                
        except Exception as e:
            self.logger.log(f"获取交易方向时出错: {e}", "ERROR")
            return self.config.direction  # 出错时使用默认方向
    
    def _log_trading_decision(self, current_inventory: float, inventory_strategy: str, trade_direction: str):
        """记录交易决策摘要"""
        try:
            # 判断是否为反向交易
            is_reverse = trade_direction != self.config.direction
            decision_type = "🔄 反向开仓" if is_reverse else "📈 正常开仓"
            
            # 计算库存利用率
            max_safe_inventory = self.inventory_manager.inventory_layers['emergency']
            inventory_usage = abs(current_inventory) / max_safe_inventory * 100
            
            # 控制台日志
            self.logger.log("=" * 80, "INFO")
            self.logger.log(f"📋 [TRADING DECISION] {decision_type}", "INFO")
            self.logger.log(f"📊 净敞口状态: {current_inventory:.2f} ({inventory_usage:.1f}% 利用率)", "INFO")
            self.logger.log(f"🎯 策略级别: {inventory_strategy}", "INFO")
            self.logger.log(f"📍 配置方向: {self.config.direction} → 执行方向: {trade_direction}", "INFO")
            if is_reverse:
                self.logger.log(f"💡 反向原因: 自动净敞口控制 (单向持仓模式)", "INFO")
            self.logger.log(f"💰 交易数量: {self.config.quantity}", "INFO")
            self.logger.log("=" * 80, "INFO")
            
            # 持久化到专门的库存决策日志文件
            decision_data = {
                'current_inventory': current_inventory,
                'inventory_strategy': inventory_strategy,
                'config_direction': self.config.direction,
                'trade_direction': trade_direction,
                'decision_type': decision_type,
                'inventory_usage': inventory_usage,
                'reverse_reason': '自动库存平衡机制' if is_reverse else '正常交易',
                'quantity': self.config.quantity
            }
            
            self.logger.log_inventory_decision(decision_data)
            
        except Exception as e:
            self.logger.log(f"记录交易决策日志时出错: {e}", "ERROR")

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders
                active_orders = await self.get_active_orders(self.config.contract_id)
                self.logger.log(f"Debug: Retrieved {len(active_orders)} active orders", "DEBUG")

                # 计算活跃订单的净敞口 (买单为正，卖单为负)
                active_buy_amount = 0.0
                active_sell_amount = 0.0
                self.active_close_orders = []  # 保持兼容性，但改变逻辑
                
                for order in active_orders:
                    try:
                        if isinstance(order, dict):
                            order_side = order.get('side', '').upper()
                            order_size = float(order.get('size', 0))
                            
                            if order_side == 'BUY':
                                active_buy_amount += order_size
                            elif order_side == 'SELL':
                                active_sell_amount += order_size
                            
                            # 为了兼容现有逻辑，把所有订单都加入 active_close_orders
                            self.active_close_orders.append({
                                'id': order.get('id'),
                                'price': order.get('price'),
                                'size': order.get('size'),
                                'side': order_side
                            })
                        else:
                            self.logger.log(f"Debug: Skipping non-dict order: {type(order)}", "DEBUG")
                    except Exception as e:
                        self.logger.log(f"Debug: Error processing order {order}: {e}", "DEBUG")
                
                # 计算活跃订单净敞口
                active_net_exposure = active_buy_amount - active_sell_amount

                # Get positions
                positions_data = await self.get_account_positions()

                if not positions_data or 'data' not in positions_data:
                    self.logger.log("Failed to get positions", "WARNING")
                    position_amt = 0
                else:
                    # The API returns positions under data.positionList
                    positions = positions_data.get('data', {}).get('positionList', [])
                    if positions:
                        # Find position for current contract
                        position = None
                        for p in positions:
                            if isinstance(p, dict) and p.get('contractId') == self.config.contract_id:
                                position = p
                                break

                        if position:
                            position_amt = abs(float(position.get('openSize', 0)))
                        else:
                            position_amt = 0
                    else:
                        position_amt = 0

                # 获取库存策略信息
                try:
                    current_inventory = await self.get_current_inventory()
                    inventory_strategy = self.inventory_manager.get_trading_strategy(current_inventory)
                    
                    self.logger.log(f"Position: {position_amt:.3f} | Active Buy: {active_buy_amount:.3f} | Active Sell: {active_sell_amount:.3f} | Net Exposure: {active_net_exposure:.3f} | Inventory: {current_inventory:.3f} | Strategy: {inventory_strategy}")
                    
                except Exception as e:
                    self.logger.log(f"Position: {position_amt:.3f} | Active Buy: {active_buy_amount:.3f} | Active Sell: {active_sell_amount:.3f} | Net Exposure: {active_net_exposure:.3f} | Inventory: Error({e})")

                # 新的仓位匹配检查：当前仓位应该约等于活跃订单的净敞口（符号相反）
                # 因为如果有多头仓位，应该有卖单来平仓
                expected_active_exposure = -current_inventory if 'current_inventory' in locals() else -position_amt
                exposure_mismatch = abs(active_net_exposure - expected_active_exposure)
                
                if exposure_mismatch > (2 * self.config.quantity):
                    self.logger.log("ERROR: Position-Order exposure mismatch detected", "ERROR")
                    self.logger.log("###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n", "ERROR")
                    self.logger.log("Please manually rebalance your position and active orders", "ERROR")
                    self.logger.log("请手动平衡当前仓位和活跃订单", "ERROR")
                    self.logger.log(f"Current position: {position_amt:.3f} | Expected active exposure: {expected_active_exposure:.3f} | Actual active exposure: {active_net_exposure:.3f} | Mismatch: {exposure_mismatch:.3f}\n", "ERROR")
                    self.logger.log("###### ERROR ###### ERROR ###### ERROR ###### ERROR #####", "ERROR")
                    if not self.shutdown_requested:
                        self.shutdown_requested = True
                    return

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            self.last_log_time = time.time()
            print("--------------------------------")

    async def run(self):
        """Main trading loop."""
        try:
            # Connect to WebSocket - both public and private needed
            self.ws_manager.connect_public()   # Needed for BTC price tracking
            self.ws_manager.connect_private()  # Needed for order updates
            
            # Subscribe to BTC ticker for volatility tracking with proper handler
            try:
                def btc_price_handler(message: str):
                    """Handle BTC price updates for volatility tracking."""
                    try:
                        import json
                        data = json.loads(message)
                        
                        # Extract ticker data
                        content = data.get("content", {})
                        ticker_data_list = content.get("data", [])
                        
                        # Handle both single ticker and list of tickers
                        if isinstance(ticker_data_list, list) and ticker_data_list:
                            ticker_data = ticker_data_list[0]  # Take the first ticker
                        else:
                            ticker_data = ticker_data_list
                        
                        if isinstance(ticker_data, dict):
                            contract_id = ticker_data.get("contractId")
                            last_price = ticker_data.get("lastPrice")
                            
                            if contract_id == "10000001" and last_price:  # BTC contract
                                # 直接同步记录价格（避免事件循环问题）
                                import time
                                from datetime import datetime, timezone
                                current_time = time.time()
                                
                                # 同步版本的价格记录
                                if len(self.btc_price_history) == 0 or self.btc_price_history[-1]['price'] != float(last_price):
                                    price_record = {
                                        'price': float(last_price),
                                        'timestamp': current_time
                                    }
                                    self.btc_price_history.append(price_record)
                                    
                                    # 详细记录到专门的价格日志文件
                                    self._log_btc_price_detail(price_record, ticker_data)
                                    
                                    # 保持最近5分钟的数据
                                    cutoff_time = current_time - 300
                                    self.btc_price_history = [
                                        record for record in self.btc_price_history 
                                        if record['timestamp'] > cutoff_time
                                    ]
                                
                    except Exception as e:
                        self.logger.log(f"Error handling BTC price update: {e}", "WARNING")
                
                self.ws_manager.subscribe_ticker("10000001", btc_price_handler)
                self.logger.log("已订阅BTC实时价格数据用于振幅检查", "INFO")
            except Exception as e:
                self.logger.log(f"订阅BTC价格数据失败: {e}", "WARNING")
            
            # Wait a moment for connection to establish
            await asyncio.sleep(2)

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.get_active_orders(self.config.contract_id)

                # 计算活跃订单（与状态日志逻辑保持一致）
                self.active_close_orders = []
                active_buy_amount = 0.0
                active_sell_amount = 0.0

                for order in active_orders:
                    try:
                        if isinstance(order, dict):
                            order_side = order.get('side', '').upper()
                            order_size = float(order.get('size', 0))
                            order_id = order.get('id', 'Unknown')
                            
                            if order_side == 'BUY':
                                active_buy_amount += order_size
                            elif order_side == 'SELL':
                                active_sell_amount += order_size
                            
                            # 为了兼容现有逻辑，把所有订单都加入 active_close_orders
                            self.active_close_orders.append({
                                'id': order.get('id'),
                                'price': order.get('price'),
                                'size': order.get('size'),
                                'side': order_side
                            })
                            
                            self.logger.log(f"Active order {order_id}: {order_side} {order_size}", "DEBUG")
                    except Exception as e:
                        self.logger.log(f"Debug: Error processing order in main loop: {e}", "DEBUG")

                # 首先检查紧急库存管理
                await self.emergency_inventory_management()
                
                # Periodic logging
                await self._log_status_periodically()
                wait_time = self._calculate_wait_time()

                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # 获取当前库存
                    current_inventory = await self.get_current_inventory()
                    inventory_strategy = self.inventory_manager.get_trading_strategy(current_inventory)
                    
                    # 根据库存策略决定是否交易
                    if inventory_strategy == 'pause':
                        self.logger.log(f"库存过大({current_inventory:.2f})，暂停开仓", "WARNING")
                        await asyncio.sleep(30)
                        continue
                    
                    # 当wait_time条件满足后，开始轮询检查BTC振幅直到安全
                    while not self.shutdown_requested:
                        is_volatility_safe = await self.check_btc_volatility_safe()
                        if is_volatility_safe:
                            # 振幅安全，立即下单
                            break
                        else:
                            # BTC振幅过大，等待30秒后重新检查，拆分为1秒间隔以响应退出信号
                            self.logger.log("BTC振幅过大，等待30秒后重新检查", "INFO")
                            for i in range(30):
                                if self.shutdown_requested:
                                    break
                                await asyncio.sleep(1)
                    
                    # 如果是因为shutdown_requested退出振幅检查循环，则跳过下单
                    if self.shutdown_requested:
                        break
                    
                    # 根据库存策略选择交易方向
                    trade_direction = self._get_trade_direction_with_inventory_control(current_inventory, inventory_strategy)
                    
                    if trade_direction:
                        # 记录交易决策摘要
                        self._log_trading_decision(current_inventory, inventory_strategy, trade_direction)
                        await self._place_and_monitor_open_order(trade_direction)
                        self.last_close_orders += 1
                    else:
                        self.logger.log("库存控制跳过本次交易", "INFO")
                        await asyncio.sleep(5)

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(traceback.format_exc(), "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                # Close HTTP client session
                if hasattr(self, 'client') and self.client:
                    await self.client.close()
            except Exception as e:
                self.logger.log(f"Error closing HTTP client session: {e}", "ERROR")
            
            try:
                # Close WebSocket connections
                if hasattr(self, 'ws_manager'):
                    self.ws_manager.disconnect_all()
            except Exception as e:
                self.logger.log(f"Error closing WebSocket connections: {e}", "ERROR")


async def main():
    """Main entry point."""
    try:
        # Load configuration from file
        app_config = load_config()
        trading_config = app_config['trading']
        
        # Create configuration
        config = TradingConfig(
            contract_id=trading_config['contract_id'],
            quantity=trading_config['quantity'],
            take_profit_percentage=trading_config['take_profit_percentage'],
            direction=trading_config['direction'],
            max_orders=trading_config['max_orders'],
            wait_time=trading_config['wait_time']
        )

        print(f"🚀 Starting EdgeX Trading Bot")
        print(f"📊 Contract: {trading_config['contract_id']} ({app_config['contracts'].get(trading_config['contract_id'], {}).get('name', 'Unknown')})")
        print(f"📈 Direction: {trading_config['direction']}")
        print(f"💰 Quantity: {trading_config['quantity']}")
        print(f"🎯 Take Profit: {trading_config['take_profit_percentage']*100:.1f}%")
        print(f"📝 Max Orders: {trading_config['max_orders']}")
        print(f"⏰ Wait Time: {trading_config['wait_time']}s")
        print("=" * 50)

        # Create and run the bot
        bot = EdgeXTradingBot(config, app_config)
        await bot.run()
        
    except FileNotFoundError as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Please create a config.json file with your trading parameters")
    except Exception as e:
        print(f"❌ Bot execution failed: {e}")
        # The bot's run method already handles graceful shutdown
        raise


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Lighter 循环买卖交易程序
策略：获取买一价格 -> 下买单 -> 等待成交 -> 市价平仓 -> 等待时间 -> 循环执行

✅ 支持循环交易模式，可配置等待时间和最大循环次数
"""
import asyncio
import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import lighter
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.api.order_api import OrderApi
from lighter.api.account_api import AccountApi
from lighter.signer_client import SignerClient


class SimpleBuySellTrader:
    """循环买卖交易机器人：买入 -> 成交 -> 平仓 -> 等待 -> 循环"""
    
    def __init__(self, config_path: str = "config.json"):
        # 加载配置
        self.config = self.load_config(config_path)
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # 交易参数
        self.symbol = self.config['trading']['SYMBOL']
        self.order_quantity = self.config['trading']['ORDER_QUANTITY']
        self.price_precision = self.config['trading']['PRICE_PRECISION']
        self.quantity_precision = self.config['trading']['QUANTITY_PRECISION']
        self.check_interval = self.config['trading']['CHECK_INTERVAL']
        
        # 循环交易参数
        self.cycle_mode = self.config['trading'].get('CYCLE_MODE', False)
        self.wait_time_between_cycles = self.config['trading'].get('WAIT_TIME_BETWEEN_CYCLES', 300)
        self.max_cycles = self.config['trading'].get('MAX_CYCLES', 0)  # 0表示无限循环
        
        # 超时配置
        self.order_timeout_minutes = self.config['trading'].get('ORDER_TIMEOUT_MINUTES', 10)
        
        # 优雅关闭标志
        self.should_exit = False
        self.active_orders = []  # 跟踪活跃订单
        
        # API配置
        self.base_url = self.config['api']['BASE_URL']
        self.api_key_private_key = self.config['api']['API_KEY_PRIVATE_KEY']
        self.account_index = self.config['api']['ACCOUNT_INDEX']
        self.api_key_index = self.config['api']['API_KEY_INDEX']
        
        # 安全设置
        self.test_mode = self.config['safety']['TEST_MODE']
        self.max_retry_attempts = self.config['safety']['MAX_RETRY_ATTEMPTS']
        self.timeout_seconds = self.config['safety']['TIMEOUT_SECONDS']
        
        # 交易状态
        self.buy_order_id = None
        self.buy_order_filled_price = None
        self.is_running = False
        self.price_decimals = 1  # 从市场信息中获取
        self.last_trade_record = None  # 成交检测基准记录
        self.current_cycle = 0  # 当前循环次数
        self.total_profit_loss = 0.0  # 累计盈亏
        
        # 初始化API客户端
        self.configuration = Configuration(host=self.base_url)
        self.api_client = ApiClient(self.configuration)
        self.order_api = OrderApi(self.api_client)
        self.account_api = AccountApi(self.api_client)
        self.signer_client = None
        self.market_info = None
        
        # 设置信号处理
        self.setup_signal_handlers()
        
    def setup_signal_handlers(self):
        """设置信号处理器，捕获Ctrl+C"""
        def signal_handler(sig, frame):
            self.logger.info("\n🛑 收到中断信号，正在安全退出...")
            self.should_exit = True
            
            # 如果是在事件循环中运行，创建取消订单的任务
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.cancel_all_orders())
            except RuntimeError:
                # 不在事件循环中，直接标记退出
                pass
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"加载配置文件失败: {e}")
    
    async def initialize(self) -> bool:
        """初始化交易客户端"""
        try:
            self.logger.info("🔧 初始化交易客户端...")
            
            # 检查私钥
            if self.api_key_private_key == "YOUR_PRIVATE_KEY_HERE":
                self.logger.error("❌ 请在config.json中设置正确的API_KEY_PRIVATE_KEY")
                return False
            
            # 初始化签名客户端
            self.signer_client = SignerClient(
                url=self.base_url,
                private_key=self.api_key_private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index
            )
            
            # 获取市场信息
            self.market_info = await self.get_market_info()
            if not self.market_info:
                self.logger.error("❌ 获取市场信息失败")
                return False
            
            self.logger.info(f"✅ 交易对: {self.symbol} (Market ID: {self.market_info.market_id})")
            
            if self.test_mode:
                self.logger.warning("🧪 测试模式启用 - 不会执行真实交易")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 初始化失败: {e}")
            return False
    
    async def get_market_info(self):
        """获取市场信息"""
        try:
            # 这里需要实现获取市场ID的逻辑
            # 暂时硬编码BTC的market_id为0
            class MarketInfo:
                def __init__(self, symbol):
                    self.symbol = symbol
                    self.market_id = 1 if symbol == "BTC" else 0
                    
            return MarketInfo(self.symbol)
            
        except Exception as e:
            self.logger.error(f"❌ 获取市场信息失败: {e}")
            return None
    
    async def get_bid_price(self) -> Optional[float]:
        """获取当前买一价格（使用最新成交价作为参考）"""
        try:
            self.logger.info("📊 获取当前价格...")
            
            # 获取市场详情，使用最新成交价格
            order_book_details = await self.order_api.order_book_details(
                market_id=self.market_info.market_id
            )
            
            if not order_book_details or not order_book_details.order_book_details:
                self.logger.error("❌ 获取市场信息失败")
                return None
            
            market_detail = order_book_details.order_book_details[0]
            last_trade_price = float(market_detail.last_trade_price)
            price_decimals = market_detail.price_decimals
            
            self.logger.info(f"📊 价格精度信息: price_decimals={price_decimals}")
            
            # 使用最新成交价作为买入价格（可能是稍微偏低的价格）
            bid_price = last_trade_price * 0.9999  # 略低于最新价格
            
            self.logger.info(f"💰 最新成交价: ${last_trade_price:,.{self.price_precision}f}")
            self.logger.info(f"📊 计算买入价: ${bid_price:,.{self.price_precision}f}")
            
            # 更新价格精度设置
            self.price_decimals = price_decimals
            
            return bid_price
            
        except Exception as e:
            self.logger.error(f"❌ 获取价格失败: {e}")
            return None
    
    async def place_buy_order(self, price: float) -> bool:
        """在指定价格下买单"""
        try:
            self.logger.info(f"📝 准备下买单: ${price:,.{self.price_precision}f} x {self.order_quantity}")
            
            if self.test_mode:
                self.logger.info("🧪 测试模式 - 模拟下单成功")
                self.buy_order_id = f"test_order_{int(time.time())}"
                return True
            
            # 转换为平台格式 (使用市场的精度信息)
            price_multiplier = 10 ** self.price_decimals if self.price_decimals else 10
            quantity_multiplier = 10 ** self.quantity_precision
            
            price_scaled = int(price * price_multiplier)
            quantity_scaled = int(self.order_quantity * quantity_multiplier)
            client_order_index = int(time.time() * 1000)
            
            self.logger.info(f"📊 订单参数: price_scaled={price_scaled}, quantity_scaled={quantity_scaled}")
            
            # 下买单
            order_result = await self.signer_client.create_order(
                market_index=self.market_info.market_id,
                client_order_index=client_order_index,
                base_amount=quantity_scaled,
                price=price_scaled,
                is_ask=False,  # False表示买单
                order_type=self.signer_client.ORDER_TYPE_LIMIT,
                time_in_force=self.signer_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=0,
                trigger_price=0
            )
            
            created_order, tx_hash, error = order_result
            
            if error:
                self.logger.error(f"❌ 买单下单失败: {error}")
                return False
            
            self.buy_order_id = str(client_order_index)
            # 记录活跃订单
            self.active_orders.append({
                'order_id': self.buy_order_id,
                'client_order_index': client_order_index,
                'order_time': datetime.now()
            })
            self.logger.info(f"✅ 买单下单成功! Order ID: {self.buy_order_id}, TX Hash: {tx_hash}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 下买单异常: {e}")
            return False
    
    async def _get_last_trade_order(self) -> dict:
        """获取账户最近一次成交订单的完整信息（参考lighter_grid_trader.py）"""
        try:
            self.logger.debug("🔍 获取最新成交订单记录...")
            
            # 创建认证令牌
            auth_token, err = self.signer_client.create_auth_token_with_expiry(
                lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
            )
            if err is not None:
                self.logger.error(f"❌ 创建认证令牌失败: {err}")
                return None
            
            # 获取账户非活跃订单（包含已成交订单）
            inactive_orders = await self.order_api.account_inactive_orders(
                account_index=self.account_index,
                market_id=self.market_info.market_id,
                limit=100,
                auth=auth_token
            )
            
            if not inactive_orders or not inactive_orders.orders:
                self.logger.debug("⚠️ 未找到历史交易记录")
                return None
            
            # 查找最近的已成交订单（严格按时间戳）
            latest_filled_order = None
            latest_timestamp = 0
            
            for order in inactive_orders.orders:
                status = getattr(order, 'status', '')
                if status == 'filled':  # 已成交订单
                    timestamp = getattr(order, 'timestamp', 0)
                    filled_base_amount = getattr(order, 'filled_base_amount', '0')
                    
                    # 验证订单有效性
                    if timestamp > 0 and float(filled_base_amount) > 0:
                        if timestamp > latest_timestamp:
                            latest_timestamp = timestamp
                            latest_filled_order = {
                                'timestamp': timestamp,
                                'price': float(getattr(order, 'price', 0)),
                                'filled_base_amount': filled_base_amount,
                                'is_ask': getattr(order, 'is_ask', False),
                                'client_order_index': getattr(order, 'client_order_index', 0),
                                'order_index': getattr(order, 'order_index', 0),
                                'filled_quote_amount': getattr(order, 'filled_quote_amount', '0')
                            }
            
            return latest_filled_order
            
        except Exception as e:
            self.logger.error(f"❌ 获取最新成交订单失败: {e}")
            return None

    async def check_order_filled(self) -> bool:
        """检查买单是否成交（使用成交记录对比方法）"""
        try:
            if self.test_mode:
                # 测试模式下模拟延迟后成交
                await asyncio.sleep(5)
                self.buy_order_filled_price = 50000.0  # 模拟成交价格
                self.logger.info("🧪 测试模式 - 模拟订单成交")
                return True
            
            # 使用lighter_grid_trader.py中的成交检测逻辑
            self.logger.debug("🔍 检查订单是否成交...")
            
            # 获取当前最新成交记录
            current_trade_record = await self._get_last_trade_order()
            
            if current_trade_record is None:
                self.logger.debug("📋 未找到成交记录，跳过检测")
                return False
            
            # 核心逻辑：对比检测新成交（时间戳或订单索引不同 = 新成交）
            if (self.last_trade_record is None) or (
                current_trade_record['timestamp'] > self.last_trade_record['timestamp'] or 
                current_trade_record['order_index'] != self.last_trade_record['order_index']
            ):
                # 检测到新成交
                filled_order_type = "卖单" if current_trade_record['is_ask'] else "买单"
                filled_price = current_trade_record['price']
                
                from datetime import datetime
                trade_time = datetime.fromtimestamp(current_trade_record['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"🎯 检测到新成交: {filled_order_type} ${filled_price:,.{self.price_precision}f} (时间: {trade_time})")
                
                # 更新记录
                self.last_trade_record = current_trade_record
                
                # 检查是否是我们的买单成交（简化判断：最新的买单成交）
                if not current_trade_record['is_ask']:  # 买单成交
                    self.buy_order_filled_price = filled_price
                    # 从活跃订单列表中移除
                    self.active_orders = [order for order in self.active_orders if order['order_id'] != self.buy_order_id]
                    self.logger.info(f"✅ 买单已成交! 成交价格: ${filled_price:,.{self.price_precision}f}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 检查订单成交状态失败: {e}")
            return False
    
    async def place_market_sell_order(self) -> bool:
        """市价平仓（参考lighter_grid_trader.py的_market_close_position方法）"""
        try:
            self.logger.info(f"💥 准备市价平仓卖出 {self.order_quantity} {self.symbol}")
            
            if self.test_mode:
                self.logger.info("🧪 测试模式 - 模拟市价卖单成功")
                return True
            
            # 获取当前市价作为参考价格
            order_book_details = await self.order_api.order_book_details(
                market_id=self.market_info.market_id
            )
            
            if not order_book_details or not order_book_details.order_book_details:
                self.logger.error("❌ 获取市场价格失败")
                return False
            
            market_detail = order_book_details.order_book_details[0]
            current_price = float(market_detail.last_trade_price)
            
            # 生成唯一的客户订单索引
            client_order_index = int(time.time() * 1000) % 281474976710655  # 不能大于2^48-1
            
            # 将持仓数量转换为平台格式
            quantity_scaled = int(self.order_quantity * (10 ** self.quantity_precision))
            
            # 价格设为略高于市价确保快速成交 (卖单需要略高价格)
            order_price = current_price * 0.9995  
            order_price = round(order_price, self.price_precision)
            price_scaled = int(order_price * (10 ** self.price_decimals))
            
            self.logger.info(f"💰 平仓订单参数:")
            self.logger.info(f"   市场价: ${current_price:,.2f}")
            self.logger.info(f"   订单价: ${order_price:,.2f}")
            self.logger.info(f"   方向: 卖出平多")
            
            # 使用绕过SDK装饰器的可靠方法
            self.logger.info("🛠️ 使用绕过SDK装饰器的可靠方法...")
            
            # 获取nonce
            api_key_index, nonce = self.signer_client.nonce_manager.next_nonce()
            
            # 切换API密钥
            err = self.signer_client.switch_api_key(api_key_index)
            if err:
                raise Exception(f"error switching api key: {err}")
            
            # 直接调用sign_create_order
            tx_info, error = self.signer_client.sign_create_order(
                market_index=self.market_info.market_id,
                client_order_index=client_order_index,
                base_amount=quantity_scaled,
                price=price_scaled,
                is_ask=True,  # 卖单
                order_type=self.signer_client.ORDER_TYPE_LIMIT,
                time_in_force=self.signer_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=1,  # 平仓单
                trigger_price=0,
                order_expiry=self.signer_client.DEFAULT_IOC_EXPIRY,  # IOC订单过期时间
                nonce=nonce
            )
            
            if error:
                self.logger.error(f"❌ 平仓订单签名失败: {error}")
                return False
                
            self.logger.info(f"✅ 平仓订单签名成功，发送交易...")
            
            # 直接调用send_tx发送交易
            tx_hash = await self.signer_client.send_tx(
                tx_type=self.signer_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            
            if tx_hash and hasattr(tx_hash, 'code') and tx_hash.code == 200:
                self.logger.info(f"✅ 市价平仓订单提交成功: 卖出平多 {self.order_quantity} {self.symbol}")
                
                # 计算盈亏
                if self.buy_order_filled_price:
                    profit_loss = (order_price - self.buy_order_filled_price) * self.order_quantity
                    profit_percent = ((order_price - self.buy_order_filled_price) / self.buy_order_filled_price) * 100
                    self.total_profit_loss += profit_loss  # 累计盈亏
                    self.logger.info(f"💰 交易完成! 盈亏: ${profit_loss:+.4f} ({profit_percent:+.2f}%)")
                
                return True
            else:
                self.logger.error(f"❌ 市价平仓订单提交失败: {tx_hash}")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ 市价平仓异常: {e}")
            return False
    
    async def cancel_all_orders(self):
        """取消所有活跃订单 - 使用 cancel_all_orders API"""
        try:
            self.logger.info("🗑️ 取消所有待成交订单...")
            
            if self.test_mode:
                self.logger.info("🧪 测试模式 - 模拟取消所有订单")
                self.active_orders.clear()
                return
            
            # 使用 cancel_all_orders API（参考 lighter_grid_trader.py）
            cancel_result = await self.signer_client.cancel_all_orders(
                time_in_force=self.signer_client.CANCEL_ALL_TIF_IMMEDIATE,
                time=0
            )
            
            if cancel_result:
                _, tx_hash, error = cancel_result
                if error:
                    self.logger.error(f"❌ 取消订单失败: {error}")
                elif tx_hash and hasattr(tx_hash, 'code') and tx_hash.code == 200:
                    self.logger.info(f"✅ 所有订单取消成功")
                    # 清空本地活跃订单记录
                    self.active_orders.clear()
                else:
                    self.logger.warning(f"⚠️ 取消订单状态未知: {tx_hash}")
                    # 即使状态未知也清空本地记录，避免重复尝试
                    self.active_orders.clear()
            else:
                self.logger.warning("⚠️ cancel_all_orders 返回空结果")
                
        except Exception as e:
            self.logger.error(f"❌ 取消所有订单异常: {e}")
            # 发生异常时也清空本地记录
            self.active_orders.clear()
    
    async def cancel_order(self, order_id: str) -> bool:
        """取消指定订单（用于超时重新下单场景）"""
        try:
            if self.test_mode:
                self.logger.info(f"🧪 测试模式 - 模拟取消订单 {order_id}")
                # 从活跃订单列表中移除
                self.active_orders = [order for order in self.active_orders if order['order_id'] != order_id]
                return True
            
            if not order_id:
                return True
            
            self.logger.info(f"🚫 取消单个订单: {order_id}")
            
            # 找到对应的client_order_index
            client_order_index = None
            for order in self.active_orders:
                if order['order_id'] == order_id:
                    client_order_index = order['client_order_index']
                    break
            
            if client_order_index is None:
                # 尝试从 order_id 中提取（如果是数字格式）
                try:
                    client_order_index = int(order_id)
                except ValueError:
                    self.logger.warning(f"⚠️ 找不到订单 {order_id} 的 client_order_index")
                    return False
            
            # 使用 signer_client 取消订单
            cancel_result = await self.signer_client.cancel_order(
                market_index=self.market_info.market_id,
                client_order_index=client_order_index
            )
            
            if cancel_result and len(cancel_result) >= 3:
                canceled_order, tx_hash, error = cancel_result
                if error:
                    self.logger.error(f"❌ 取消订单失败: {error}")
                    return False
                else:
                    self.logger.info(f"✅ 订单 {order_id} 已取消, TX Hash: {tx_hash}")
                    # 从活跃订单列表中移除
                    self.active_orders = [order for order in self.active_orders if order['order_id'] != order_id]
                    return True
            else:
                self.logger.error(f"❌ 取消订单返回结果异常: {cancel_result}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 取消订单异常: {e}")
            return False
    
    async def execute_single_cycle(self) -> bool:
        """执行单次交易循环"""
        try:
            # 检查是否收到退出信号
            if self.should_exit:
                self.logger.info("🛑 收到退出信号，停止交易循环")
                return False
            
            self.current_cycle += 1
            cycle_info = f"[{self.current_cycle}/{self.max_cycles if self.max_cycles > 0 else '∞'}]"
            self.logger.info(f"🔄 开始第 {self.current_cycle} 轮交易 {cycle_info}")
            
            # 重置订单状态
            self.buy_order_id = None
            self.buy_order_filled_price = None
            
            # 1. 获取买一价格
            bid_price = await self.get_bid_price()
            if bid_price is None:
                self.logger.error("❌ 获取买一价格失败，跳过本轮交易")
                return False
            
            # 2. 下买单
            if not await self.place_buy_order(bid_price):
                self.logger.error("❌ 下买单失败，跳过本轮交易")
                return False
            
            # 3. 等待成交（增加超时检测）
            self.logger.info(f"⏳ 等待买单成交，超时时间: {self.order_timeout_minutes} 分钟...")
            
            order_start_time = datetime.now()
            timeout_duration = timedelta(minutes=self.order_timeout_minutes)
            
            while True:
                # 检查是否收到退出信号
                if self.should_exit:
                    self.logger.info("🛑 收到退出信号，停止等待订单成交")
                    return False
                
                # 检查持仓情况，如果检测到持仓也要break
                try:
                    current_position = await self._get_current_position()
                    if current_position > 0.0001:  # 如果有持仓（大于0.0001防止浮点数精度问题）
                        self.logger.info(f"📊 检测到持仓 {current_position} {self.symbol}，买单可能已经成交")
                        break
                except Exception as e:
                    self.logger.warning(f"⚠️ 检查持仓失败: {e}")
                
                # 检查订单是否成交
                if await self.check_order_filled():
                    break
                
                # 检查是否超时
                if datetime.now() - order_start_time > timeout_duration:
                    self.logger.warning(f"⏰ 买单超过 {self.order_timeout_minutes} 分钟未成交，取消订单并重新下单")
                    
                    # 取消当前订单
                    await self.cancel_all_orders()
                    
                    # 重新获取价格并下单
                    new_bid_price = await self.get_bid_price()
                    if new_bid_price is None:
                        self.logger.error("❌ 重新获取价格失败")
                        return False
                    
                    if not await self.place_buy_order(new_bid_price):
                        self.logger.error("❌ 重新下单失败")
                        return False
                    
                    # 重置超时计时
                    order_start_time = datetime.now()
                    self.logger.info(f"🔄 已重新下单，继续等待成交...")
                    continue
                
                await asyncio.sleep(self.check_interval)
            
            # 4. 市价平仓
            if self.should_exit:
                self.logger.info("🛑 收到退出信号，跳过平仓")
                return False
            
            # 取消所有未成交的订单（防止有订单是部分成交）
            self.logger.info("🧹 准备市价平仓，先取消所有未成交订单...")
            await self.cancel_all_orders()
                
            if not await self.place_market_sell_order():
                self.logger.error("❌ 市价平仓失败")
                return False
            
            self.logger.info(f"✅ 第 {self.current_cycle} 轮交易完成")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 第 {self.current_cycle} 轮交易异常: {e}")
            return False
    
    async def run(self):
        """运行交易程序"""
        try:
            if self.cycle_mode:
                self.logger.info("🚀 启动循环买卖交易程序")
                self.logger.info(f"🔄 循环模式: 开启 | 等待时间: {self.wait_time_between_cycles}秒 | 最大循环: {self.max_cycles if self.max_cycles > 0 else '无限'}")
            else:
                self.logger.info("🚀 启动一次性买卖交易程序")
            
            # 1. 初始化
            if not await self.initialize():
                self.logger.error("❌ 初始化失败，程序退出")
                return
            
            # 2. 初始化成交检测基准记录
            self.logger.info("🔍 初始化成交检测基准...")
            self.last_trade_record = await self._get_last_trade_order()
            if self.last_trade_record:
                from datetime import datetime
                trade_time = datetime.fromtimestamp(self.last_trade_record['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                order_type = "卖单" if self.last_trade_record['is_ask'] else "买单"
                self.logger.info(f"📋 设置成交检测基准: {order_type} ${self.last_trade_record['price']:,.2f} (时间: {trade_time})")
            else:
                self.logger.info("📋 未发现历史成交记录，从空基准开始")
            
            # 3. 开始交易循环
            if self.cycle_mode:
                # 循环交易模式
                while not self.should_exit:
                    # 执行单次交易
                    success = await self.execute_single_cycle()
                    
                    # 如果收到退出信号，停止循环
                    if self.should_exit:
                        break
                    
                    # 如果交易失败，等待3秒后重试，不退出循环
                    if not success:
                        self.logger.warning("⚠️ 交易执行失败，3秒后重试...")
                        await asyncio.sleep(3)
                        continue
                    
                    # 检查是否达到最大循环次数
                    if self.max_cycles > 0 and self.current_cycle >= self.max_cycles:
                        self.logger.info(f"🏁 达到最大循环次数 {self.max_cycles}，程序退出")
                        break
                    
                    # 等待下一轮（预留检查退出信号）
                    # 在基础等待时间上随机加减60秒
                    random_offset = random.randint(-60, 60)
                    actual_wait_time = self.wait_time_between_cycles + random_offset
                    
                    self.logger.info(f"⏰ 等待 {actual_wait_time} 秒后开始下一轮交易... (基础: {self.wait_time_between_cycles}秒, 随机偏移: {random_offset:+d}秒)")
                    if self.total_profit_loss != 0:
                        self.logger.info(f"💰 累计盈亏: ${self.total_profit_loss:+.4f}")
                    
                    # 分段等待，以便及时响应退出信号
                    for _ in range(actual_wait_time):
                        if self.should_exit:
                            break
                        await asyncio.sleep(1)
            else:
                # 一次性交易模式（保持原有逻辑）
                await self.execute_single_cycle()
            
            if self.should_exit:
                self.logger.info("🛑 程序安全退出")
            else:
                self.logger.info("🎉 交易程序完成")
            
            if self.cycle_mode and self.total_profit_loss != 0:
                self.logger.info(f"💰 最终累计盈亏: ${self.total_profit_loss:+.4f}")
            
        except KeyboardInterrupt:
            self.logger.info("⚠️ 用户中断程序")
            await self.cancel_all_orders()
        except Exception as e:
            self.logger.error(f"❌ 交易程序异常: {e}")
        finally:
            # 在清理前再次检查并取消所有订单
            if self.active_orders:
                await self.cancel_all_orders()
            await self.cleanup()
    
    async def _get_current_position(self) -> float:
        """获取当前持仓数量 - 从 Lighter 官方 API 获取"""
        try:
            if not self.account_api:
                self.logger.error("❌ account_api 未初始化")
                return 0.0
                
            # 通过账户索引获取账户详情
            detailed_accounts = await self.account_api.account(
                by="index", 
                value=str(self.account_index)
            )
            
            if not detailed_accounts:
                self.logger.warning(f"⚠️ 未找到账户信息: account_index={self.account_index}")
                return 0.0
            
            # 遍历所有持仓，找到相应的交易对
            if hasattr(detailed_accounts, 'accounts') and detailed_accounts.accounts:
                accounts_list = detailed_accounts.accounts
                account_detail = accounts_list[0] if accounts_list else None
            else:
                self.logger.warning(f"⚠️ DetailedAccounts 没有 accounts 属性或为空")
                return 0.0
            
            if hasattr(account_detail, 'positions') and account_detail.positions:
                self.logger.info(f"🔍 检查持仓数据，总共 {len(account_detail.positions)} 个市场")
                for position in account_detail.positions:
                    self.logger.info(f"🔍 市场: {getattr(position, 'symbol', 'unknown')} - 持仓: {getattr(position, 'position', 'N/A')}")
                    if hasattr(position, 'symbol') and position.symbol == self.symbol:
                        # 找到对应的持仓
                        position_str = getattr(position, 'position', '0')
                        self.logger.info(f"🔍 原始持仓数据: {position_str} (type: {type(position_str)})")
                        
                        try:
                            position_amount = float(position_str)
                            self.logger.info(f"📊 从 Lighter 官方 API 获取 {self.symbol} 持仓: {position_amount}")
                            
                            # 检查是否真的有持仓
                            if position_amount != 0:
                                self.logger.info(f"✅ 检测到非零持仓: {position_amount}")
                            
                            return position_amount
                            
                        except (ValueError, TypeError) as e:
                            self.logger.error(f"❌ 持仓数据解析失败: {e}, 原始数据: {position_str}")
                            return 0.0
            
            # 如果没有找到相应的持仓，说明持仓为 0
            self.logger.info(f"📊 {self.symbol} 在 Lighter 官方数据中没有持仓，返回 0")
            return 0.0
            
        except Exception as e:
            self.logger.error(f"❌ 从 Lighter 官方 API 获取持仓失败: {e}")
            return 0.0

    async def cleanup(self):
        """清理资源"""
        try:
            self.logger.info("🧽 正在清理资源...")
            if self.api_client:
                await self.api_client.close()
            self.logger.info("✅ 资源清理完成")
        except Exception as e:
            self.logger.error(f"❌ 清理资源失败: {e}")


def print_usage():
    """打印使用说明"""
    print("Lighter 循环买卖交易程序")
    print("用法: python3 simple_buy_sell_trader.py [配置文件路径]")
    print("")
    print("参数:")
    print("  配置文件路径    指定交易配置文件 (默认: config.json)")
    print("")
    print("示例:")
    print("  python3 simple_buy_sell_trader.py")
    print("  python3 simple_buy_sell_trader.py my_traders_trader_1.json")
    print("  python3 simple_buy_sell_trader.py --help")

async def main():
    """主函数"""
    # 解析命令行参数
    config_path = "config.json"  # 默认配置文件路径
    
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ["-h", "--help", "help"]:
            print_usage()
            return
        else:
            config_path = arg
            print(f"📁 使用指定配置文件: {config_path}")
    else:
        print(f"📁 使用默认配置文件: {config_path}")
    
    try:
        trader = SimpleBuySellTrader(config_path)
        await trader.run()
    except Exception as e:
        print(f"❌ 程序启动失败: {e}")
        print("\n使用 --help 查看使用说明")


if __name__ == "__main__":
    asyncio.run(main())
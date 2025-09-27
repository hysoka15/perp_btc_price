import asyncio
import logging
import lighter
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.api.order_api import OrderApi
from typing import Optional

logger = logging.getLogger(__name__)

class LighterClient:
    """Lighter交易所客户端 - 获取BTC价格"""
    
    def __init__(self, base_url: str = "https://mainnet.zklighter.elliot.ai"):
        self.base_url = base_url
        self.configuration = Configuration(host=base_url)
        self.api_client = None
        self.order_api = None
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.api_client = ApiClient(self.configuration)
        self.order_api = OrderApi(self.api_client)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self.api_client:
            await self.api_client.close()
    
    async def get_btc_price(self) -> Optional[float]:
        """获取BTC价格 - 参考simple_buy_sell_trader.py的方法"""
        try:
            if not self.order_api:
                logger.error("API客户端未初始化")
                return None
            
            # 方法1: 从交易所统计获取BTC价格（最快方法）
            try:
                exchange_stats = await self.order_api.exchange_stats()
                if exchange_stats and hasattr(exchange_stats, 'order_book_stats'):
                    for market_stat in exchange_stats.order_book_stats:
                        if hasattr(market_stat, 'symbol') and market_stat.symbol == 'BTC':
                            if hasattr(market_stat, 'last_trade_price') and market_stat.last_trade_price:
                                price = float(market_stat.last_trade_price)
                                logger.info(f"Lighter BTC价格(统计): {price}")
                                return price
            except Exception as e:
                logger.warning(f"从交易所统计获取BTC价格失败: {str(e)}")
            
            # 方法2: 参考simple_buy_sell_trader.py的方法，从订单簿详情获取last_trade_price
            # 尝试不同的market_id找到BTC
            for market_id in [1, 0, 2, 3, 4, 5]:  # 从1开始，因为simple_buy_sell_trader.py中BTC是1
                try:
                    order_book_details = await self.order_api.order_book_details(market_id=market_id)
                    
                    if (order_book_details and 
                        hasattr(order_book_details, 'order_book_details') and 
                        order_book_details.order_book_details):
                        
                        market_detail = order_book_details.order_book_details[0]
                        
                        # 检查是否是BTC市场
                        if hasattr(market_detail, 'symbol') and market_detail.symbol == 'BTC':
                            if hasattr(market_detail, 'last_trade_price') and market_detail.last_trade_price:
                                price = float(market_detail.last_trade_price)
                                logger.info(f"Lighter BTC价格(订单簿last_trade_price): {price} (market_id: {market_id})")
                                return price
                                
                except Exception as e:
                    logger.debug(f"尝试market_id={market_id}失败: {str(e)}")
                    continue
            
            # 方法3: 如果以上都失败，尝试从最近交易获取（market_id=1，BTC市场）
            try:
                trades = await self.order_api.recent_trades(market_id=1, limit=1)
                if trades and hasattr(trades, 'trades') and trades.trades:
                    latest_trade = trades.trades[0]
                    if hasattr(latest_trade, 'price'):
                        price = float(latest_trade.price)
                        logger.info(f"Lighter BTC价格(最近交易): {price}")
                        return price
                        
            except Exception as e:
                logger.warning(f"从最近交易获取BTC价格失败: {str(e)}")
            
            # 如果以上方法都失败，返回None
            logger.error("所有获取Lighter BTC价格的方法都失败")
            return None
                
        except Exception as e:
            logger.error(f"获取Lighter BTC价格失败: {str(e)}")
            return None
    
    async def get_recent_trades(self, market_id: int = 0, limit: int = 1) -> Optional[list]:
        """获取最近交易记录"""
        try:
            if not self.order_api:
                logger.error("API客户端未初始化")
                return None
                
            trades = await self.order_api.recent_trades(market_id=market_id, limit=limit)
            
            if trades and hasattr(trades, 'trades') and trades.trades:
                return trades.trades
            else:
                logger.warning(f"Lighter没有最近交易数据，market_id: {market_id}")
                return None
                
        except Exception as e:
            logger.error(f"获取Lighter最近交易失败: {str(e)}")
            return None
    
    async def get_latest_price_from_trades(self, market_id: int = 0) -> Optional[float]:
        """从最近交易获取最新价格"""
        try:
            trades = await self.get_recent_trades(market_id, limit=1)
            
            if trades and len(trades) > 0:
                latest_trade = trades[0]
                if hasattr(latest_trade, 'price'):
                    price = float(latest_trade.price)
                    logger.info(f"Lighter BTC最新成交价: {price}")
                    return price
            
            logger.warning("无法从交易记录获取Lighter价格")
            return None
            
        except Exception as e:
            logger.error(f"从交易记录获取Lighter价格失败: {str(e)}")
            return None

# 同步接口封装
def get_lighter_btc_price() -> Optional[float]:
    """同步获取Lighter BTC价格的便捷函数"""
    async def _get_price():
        async with LighterClient() as client:
            price = await client.get_btc_price()
            return price
    
    try:
        # 检查是否已在事件循环中
        try:
            loop = asyncio.get_running_loop()
            # 如果已在事件循环中，创建新任务
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _get_price())
                return future.result(timeout=10)
        except RuntimeError:
            # 没有运行中的事件循环，直接运行
            return asyncio.run(_get_price())
    except Exception as e:
        logger.error(f"同步获取Lighter价格失败: {str(e)}")
        return None
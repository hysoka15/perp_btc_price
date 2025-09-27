import asyncio
import logging
import sys
import os
from typing import Optional

# 添加edgex-python-sdk路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'edgex-python-sdk'))

from edgex_sdk import Client, GetOrderBookDepthParams

logger = logging.getLogger(__name__)

class EdgeXClient:
    """EdgeX交易所客户端 - 获取BTC价格"""
    
    def __init__(self, base_url: str, account_id: str, stark_private_key: str):
        self.base_url = base_url
        self.account_id = int(account_id)
        self.stark_private_key = stark_private_key
        self.client = None
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.client = Client(
            base_url=self.base_url,
            account_id=self.account_id,
            stark_private_key=self.stark_private_key
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self.client:
            try:
                # 尝试关闭HTTP客户端会话
                await self.client.close()
            except Exception as e:
                logger.debug(f"关闭EdgeX客户端时出错: {str(e)}")
        self.client = None
    
    async def get_btc_price(self, contract_id: str = "10000001") -> Optional[float]:
        """获取BTC价格，参考runbot.py中的方法"""
        try:
            if not self.client:
                logger.error("EdgeX客户端未初始化")
                return None
            
            # 方法1：获取24小时行情数据 - 参考runbot.py
            try:
                quote_result = await self.client.quote.get_24_hour_quote(contract_id)
                
                if quote_result and 'data' in quote_result and quote_result.get('code') == 'SUCCESS':
                    quote_data = quote_result['data']
                    if quote_data and isinstance(quote_data, list) and len(quote_data) > 0:
                        ticker = quote_data[0]
                        
                        # 尝试多个价格字段
                        if 'lastPrice' in ticker:
                            price = float(ticker['lastPrice'])
                            logger.info(f"EdgeX BTC价格(lastPrice): {price}")
                            return price
                        elif 'close' in ticker:
                            price = float(ticker['close'])
                            logger.info(f"EdgeX BTC价格(close): {price}")
                            return price
                        elif 'price' in ticker:
                            price = float(ticker['price'])
                            logger.info(f"EdgeX BTC价格(price): {price}")
                            return price
                            
            except Exception as e:
                logger.warning(f"获取EdgeX 24小时行情失败: {str(e)}")
            
            # 方法2：从订单簿获取中间价 - 参考runbot.py
            try:
                depth_params = GetOrderBookDepthParams(contract_id=contract_id, limit=15)
                order_book = await self.client.quote.get_order_book_depth(depth_params)
                
                # 处理响应格式: {"code": "SUCCESS", "data": [{"asks": [...], "bids": [...]}]}
                if not isinstance(order_book, dict) or 'data' not in order_book:
                    logger.warning(f"EdgeX订单簿响应格式异常: {type(order_book)}")
                    return None

                order_book_data = order_book['data']
                if not isinstance(order_book_data, list) or len(order_book_data) == 0:
                    logger.warning(f"EdgeX订单簿数据为空: {type(order_book_data)}")
                    return None

                # 获取第一个订单簿条目
                order_book_entry = order_book_data[0]
                if not isinstance(order_book_entry, dict):
                    logger.warning(f"EdgeX订单簿条目格式异常: {type(order_book_entry)}")
                    return None

                # 提取买卖盘数据
                bids = order_book_entry.get('bids', [])
                asks = order_book_entry.get('asks', [])

                if not bids or not asks:
                    logger.warning("EdgeX订单簿缺少买卖盘数据")
                    return None

                # 计算最优买卖价
                best_bid = float(bids[0]['price']) if bids and len(bids) > 0 else 0
                best_ask = float(asks[0]['price']) if asks and len(asks) > 0 else 0

                if best_bid <= 0 or best_ask <= 0:
                    logger.warning(f"EdgeX订单簿价格异常: bid={best_bid}, ask={best_ask}")
                    return None

                # 计算中间价
                mid_price = (best_bid + best_ask) / 2
                logger.info(f"EdgeX BTC价格(中间价): {mid_price:.2f} (买一: {best_bid}, 卖一: {best_ask})")
                return mid_price
                        
            except Exception as e:
                logger.warning(f"获取EdgeX订单簿失败: {str(e)}")
            
            logger.error("无法获取EdgeX BTC价格")
            return None
                
        except Exception as e:
            logger.error(f"获取EdgeX BTC价格失败: {str(e)}")
            return None
    
    async def get_server_time(self) -> Optional[dict]:
        """获取服务器时间"""
        try:
            if not self.client:
                logger.error("EdgeX客户端未初始化")
                return None
                
            server_time = await self.client.get_server_time()
            return server_time
            
        except Exception as e:
            logger.error(f"获取EdgeX服务器时间失败: {str(e)}")
            return None
    
    async def get_contract_info(self) -> Optional[dict]:
        """获取合约信息"""
        try:
            if not self.client:
                logger.error("EdgeX客户端未初始化")
                return None
                
            metadata = await self.client.get_metadata()
            return metadata
            
        except Exception as e:
            logger.error(f"获取EdgeX合约信息失败: {str(e)}")
            return None

# 同步接口封装
def get_edgex_btc_price(base_url: str, account_id: str, stark_private_key: str, contract_id: str = "10000001") -> Optional[float]:
    """同步获取EdgeX BTC价格的便捷函数"""
    async def _get_price():
        async with EdgeXClient(base_url, account_id, stark_private_key) as client:
            return await client.get_btc_price(contract_id)
    
    try:
        # 检查是否已在事件循环中
        try:
            loop = asyncio.get_running_loop()
            # 如果已在事件循环中，创建新任务
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _get_price())
                return future.result(timeout=30)
        except RuntimeError:
            # 没有运行中的事件循环，直接运行
            return asyncio.run(_get_price())
    except Exception as e:
        logger.error(f"同步获取EdgeX价格失败: {str(e)}")
        return None
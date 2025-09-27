#!/usr/bin/env python3
"""
Lighter交易所详细调试程序
深入分析API返回的数据格式
"""

import asyncio
import logging
import json
import lighter
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.api.order_api import OrderApi

# 设置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lighter配置
LIGHTER_CONFIG = {
    'base_url': 'https://mainnet.zklighter.elliot.ai',
    'account_index': 76835,
    'api_key_index': 2
}

async def debug_lighter_data():
    """调试Lighter数据格式"""
    logger.info("开始详细调试Lighter数据...")
    
    try:
        configuration = Configuration(host=LIGHTER_CONFIG['base_url'])
        api_client = ApiClient(configuration)
        order_api = OrderApi(api_client)
        
        # 调试1: 详细查看订单簿数据
        logger.info("=" * 50)
        logger.info("调试1: 详细查看BTC订单簿数据")
        try:
            btc_order_book = await order_api.order_book_details(market_id=0)
            logger.info(f"订单簿原始响应类型: {type(btc_order_book)}")
            logger.info(f"订单簿原始响应: {btc_order_book}")
            
            if hasattr(btc_order_book, 'data'):
                book_data = btc_order_book.data
                logger.info(f"book_data类型: {type(book_data)}")
                logger.info(f"book_data内容: {book_data}")
                
                if book_data:
                    logger.info(f"book_data属性: {dir(book_data)}")
                    
                    # 检查各种可能的属性
                    for attr in ['bids', 'asks', 'buy', 'sell', 'buy_orders', 'sell_orders']:
                        if hasattr(book_data, attr):
                            value = getattr(book_data, attr)
                            logger.info(f"{attr}: {value} (类型: {type(value)})")
                            
                            if value and len(value) > 0:
                                logger.info(f"{attr}第一个元素: {value[0]} (类型: {type(value[0])})")
                else:
                    logger.warning("book_data为空")
            else:
                logger.warning("订单簿响应没有data属性")
                
        except Exception as e:
            logger.error(f"调试订单簿失败: {str(e)}")
        
        # 调试2: 详细查看最近交易数据
        logger.info("=" * 50)
        logger.info("调试2: 详细查看最近交易数据")
        try:
            recent_trades = await order_api.recent_trades(market_id=0, limit=5)
            logger.info(f"交易响应类型: {type(recent_trades)}")
            logger.info(f"交易响应: {recent_trades}")
            
            if hasattr(recent_trades, 'data'):
                trade_data = recent_trades.data
                logger.info(f"trade_data类型: {type(trade_data)}")
                logger.info(f"trade_data内容: {trade_data}")
                
                if trade_data and len(trade_data) > 0:
                    logger.info(f"trade_data长度: {len(trade_data)}")
                    
                    first_trade = trade_data[0]
                    logger.info(f"第一个交易类型: {type(first_trade)}")
                    logger.info(f"第一个交易内容: {first_trade}")
                    logger.info(f"第一个交易属性: {dir(first_trade)}")
                    
                    # 检查价格相关属性
                    for attr in ['price', 'executed_price', 'trade_price', 'fill_price', 'amount', 'quantity', 'size']:
                        if hasattr(first_trade, attr):
                            value = getattr(first_trade, attr)
                            logger.info(f"交易.{attr}: {value}")
                else:
                    logger.warning("trade_data为空")
            else:
                logger.warning("交易响应没有data属性")
                
        except Exception as e:
            logger.error(f"调试最近交易失败: {str(e)}")
        
        # 调试3: 尝试不同的market_id
        logger.info("=" * 50)
        logger.info("调试3: 尝试不同的market_id")
        for market_id in [0, 1, 2, 3]:
            try:
                logger.info(f"尝试market_id={market_id}...")
                order_book = await order_api.order_book_details(market_id=market_id)
                
                if order_book and hasattr(order_book, 'data') and order_book.data:
                    book_data = order_book.data
                    
                    has_bids = hasattr(book_data, 'bids') and book_data.bids
                    has_asks = hasattr(book_data, 'asks') and book_data.asks
                    
                    logger.info(f"  market_id={market_id}: 有买单={has_bids}, 有卖单={has_asks}")
                    
                    if has_bids and has_asks:
                        logger.info(f"  ✅ market_id={market_id} 有完整订单簿数据")
                        
                        # 尝试提取价格
                        try:
                            bid_entry = book_data.bids[0]
                            ask_entry = book_data.asks[0]
                            
                            logger.info(f"  买单格式: {type(bid_entry)}, 内容: {bid_entry}")
                            logger.info(f"  卖单格式: {type(ask_entry)}, 内容: {ask_entry}")
                            
                            # 尝试不同的价格提取方法
                            best_bid = None
                            best_ask = None
                            
                            # 方法1: 列表格式 [price, quantity]
                            if isinstance(bid_entry, (list, tuple)) and len(bid_entry) >= 1:
                                best_bid = float(bid_entry[0])
                                best_ask = float(ask_entry[0])
                            # 方法2: 对象格式
                            elif hasattr(bid_entry, 'price'):
                                best_bid = float(bid_entry.price)
                                best_ask = float(ask_entry.price)
                            # 方法3: 字典格式
                            elif isinstance(bid_entry, dict) and 'price' in bid_entry:
                                best_bid = float(bid_entry['price'])
                                best_ask = float(ask_entry['price'])
                            
                            if best_bid and best_ask:
                                mid_price = (best_bid + best_ask) / 2
                                logger.info(f"  💰 提取成功 - 买价: {best_bid}, 卖价: {best_ask}, 中间价: {mid_price}")
                                
                                # 如果是BTC相关市场，返回价格
                                if market_id in [0, 1]:  # 通常0或1是BTC市场
                                    await api_client.close()
                                    return mid_price
                            
                        except Exception as e:
                            logger.error(f"  从market_id={market_id}提取价格失败: {str(e)}")
                else:
                    logger.info(f"  market_id={market_id}: 无数据")
                    
            except Exception as e:
                logger.error(f"调试market_id={market_id}失败: {str(e)}")
        
        # 调试4: 检查所有可用市场
        logger.info("=" * 50)
        logger.info("调试4: 尝试获取所有市场列表")
        try:
            # 由于order_books()可能有验证错误，我们跳过这步，直接尝试exchange_stats
            try:
                exchange_stats = await order_api.exchange_stats()
                logger.info(f"交易所统计: {exchange_stats}")
            except Exception as e:
                logger.error(f"获取交易所统计失败: {str(e)}")
                
        except Exception as e:
            logger.error(f"调试所有市场失败: {str(e)}")
        
        await api_client.close()
        return None
        
    except Exception as e:
        logger.error(f"详细调试失败: {str(e)}")
        return None

async def main():
    """主函数"""
    logger.info("Lighter交易所详细调试")
    price = await debug_lighter_data()
    
    if price:
        logger.info(f"🎉 成功提取Lighter BTC价格: ${price}")
    else:
        logger.error("❌ 无法提取Lighter BTC价格")

if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
"""
Lighter交易所价格获取测试程序
使用CLAUDE.md中的账户配置进行测试
"""

import asyncio
import logging
import lighter
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.api.order_api import OrderApi
from lighter.api.account_api import AccountApi

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lighter配置 (来自CLAUDE.md)
LIGHTER_CONFIG = {
    'base_url': 'https://mainnet.zklighter.elliot.ai',
    'api_key_private_key': '0xc044fec1ca6f7dfafacd04e8a74ba7142639352a3fff322ad54540cb5e78ec26f17b46ac01ae3922',
    'account_index': 76835,
    'api_key_index': 2
}

async def test_lighter_connection():
    """测试Lighter连接和基础API"""
    logger.info("开始测试Lighter连接...")
    
    try:
        # 创建API客户端
        configuration = Configuration(host=LIGHTER_CONFIG['base_url'])
        api_client = ApiClient(configuration)
        account_api = AccountApi(api_client)
        order_api = OrderApi(api_client)
        
        logger.info(f"连接到Lighter: {LIGHTER_CONFIG['base_url']}")
        
        # 测试1: 获取账户信息
        try:
            logger.info("测试1: 获取账户信息...")
            account_info = await account_api.account(by="index", value=str(LIGHTER_CONFIG['account_index']))
            logger.info(f"✅ 账户信息获取成功: {account_info}")
        except Exception as e:
            logger.error(f"❌ 获取账户信息失败: {str(e)}")
        
        # 测试2: 获取API密钥信息
        try:
            logger.info("测试2: 获取API密钥信息...")
            api_keys = await account_api.apikeys(
                account_index=LIGHTER_CONFIG['account_index'], 
                api_key_index=LIGHTER_CONFIG['api_key_index']
            )
            logger.info(f"✅ API密钥信息获取成功: {api_keys}")
        except Exception as e:
            logger.error(f"❌ 获取API密钥失败: {str(e)}")
        
        # 测试3: 获取订单簿信息
        try:
            logger.info("测试3: 获取订单簿信息...")
            order_books = await order_api.order_books()
            logger.info(f"✅ 订单簿列表获取成功，共 {len(order_books.data) if hasattr(order_books, 'data') and order_books.data else 0} 个市场")
            
            if hasattr(order_books, 'data') and order_books.data:
                # 显示前几个市场
                for i, market in enumerate(order_books.data[:3]):
                    logger.info(f"  市场 {i}: {market}")
        except Exception as e:
            logger.error(f"❌ 获取订单簿列表失败: {str(e)}")
        
        # 测试4: 获取BTC市场详细信息 (market_id=0)
        try:
            logger.info("测试4: 获取BTC市场详细订单簿...")
            btc_order_book = await order_api.order_book_details(market_id=0)
            logger.info(f"✅ BTC订单簿详情获取成功")
            
            if hasattr(btc_order_book, 'data') and btc_order_book.data:
                book_data = btc_order_book.data
                logger.info(f"订单簿数据类型: {type(book_data)}")
                logger.info(f"订单簿属性: {dir(book_data)}")
                
                # 尝试获取买卖价格
                if hasattr(book_data, 'bids') and book_data.bids:
                    best_bid = book_data.bids[0]
                    logger.info(f"最佳买价: {best_bid}")
                    
                if hasattr(book_data, 'asks') and book_data.asks:
                    best_ask = book_data.asks[0]
                    logger.info(f"最佳卖价: {best_ask}")
                    
        except Exception as e:
            logger.error(f"❌ 获取BTC订单簿详情失败: {str(e)}")
            
        # 测试5: 获取最近交易
        try:
            logger.info("测试5: 获取最近交易...")
            recent_trades = await order_api.recent_trades(market_id=0, limit=5)
            logger.info(f"✅ 最近交易获取成功")
            
            if hasattr(recent_trades, 'data') and recent_trades.data:
                logger.info(f"最近交易数量: {len(recent_trades.data)}")
                for i, trade in enumerate(recent_trades.data[:3]):
                    logger.info(f"  交易 {i}: {trade}")
        except Exception as e:
            logger.error(f"❌ 获取最近交易失败: {str(e)}")
        
        # 关闭连接
        await api_client.close()
        logger.info("API客户端连接已关闭")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Lighter连接测试失败: {str(e)}")
        return False

async def test_lighter_price_extraction():
    """测试从Lighter提取BTC价格"""
    logger.info("开始测试Lighter价格提取...")
    
    try:
        configuration = Configuration(host=LIGHTER_CONFIG['base_url'])
        api_client = ApiClient(configuration)
        order_api = OrderApi(api_client)
        
        # 方法1: 从订单簿获取价格
        try:
            logger.info("方法1: 从订单簿获取价格...")
            order_book = await order_api.order_book_details(market_id=0)
            
            if order_book and hasattr(order_book, 'data') and order_book.data:
                book_data = order_book.data
                
                best_bid = None
                best_ask = None
                
                if hasattr(book_data, 'bids') and book_data.bids:
                    # 订单簿格式可能是 [price, quantity] 或者其他格式
                    bid_entry = book_data.bids[0]
                    if isinstance(bid_entry, (list, tuple)) and len(bid_entry) >= 2:
                        best_bid = float(bid_entry[0])
                    elif hasattr(bid_entry, 'price'):
                        best_bid = float(bid_entry.price)
                    else:
                        logger.info(f"买单格式: {type(bid_entry)}, 内容: {bid_entry}")
                        
                if hasattr(book_data, 'asks') and book_data.asks:
                    ask_entry = book_data.asks[0]
                    if isinstance(ask_entry, (list, tuple)) and len(ask_entry) >= 2:
                        best_ask = float(ask_entry[0])
                    elif hasattr(ask_entry, 'price'):
                        best_ask = float(ask_entry.price)
                    else:
                        logger.info(f"卖单格式: {type(ask_entry)}, 内容: {ask_entry}")
                
                if best_bid and best_ask:
                    mid_price = (best_bid + best_ask) / 2
                    logger.info(f"✅ 从订单簿获取价格成功:")
                    logger.info(f"   最佳买价: ${best_bid}")
                    logger.info(f"   最佳卖价: ${best_ask}")
                    logger.info(f"   中间价: ${mid_price}")
                    return mid_price
                else:
                    logger.warning("无法从订单簿提取有效价格")
            else:
                logger.warning("订单簿数据为空")
                
        except Exception as e:
            logger.error(f"从订单簿获取价格失败: {str(e)}")
        
        # 方法2: 从最近交易获取价格
        try:
            logger.info("方法2: 从最近交易获取价格...")
            trades = await order_api.recent_trades(market_id=0, limit=1)
            
            if trades and hasattr(trades, 'data') and trades.data:
                latest_trade = trades.data[0]
                
                price = None
                if hasattr(latest_trade, 'price'):
                    price = float(latest_trade.price)
                elif hasattr(latest_trade, 'executed_price'):
                    price = float(latest_trade.executed_price)
                elif isinstance(latest_trade, dict) and 'price' in latest_trade:
                    price = float(latest_trade['price'])
                else:
                    logger.info(f"交易格式: {type(latest_trade)}, 内容: {latest_trade}")
                
                if price:
                    logger.info(f"✅ 从最近交易获取价格成功: ${price}")
                    return price
                else:
                    logger.warning("无法从交易记录提取有效价格")
            else:
                logger.warning("最近交易数据为空")
                
        except Exception as e:
            logger.error(f"从最近交易获取价格失败: {str(e)}")
        
        await api_client.close()
        return None
        
    except Exception as e:
        logger.error(f"❌ Lighter价格提取测试失败: {str(e)}")
        return None

async def main():
    """主测试函数"""
    logger.info("=" * 60)
    logger.info("Lighter交易所连接和价格获取测试")
    logger.info("=" * 60)
    
    # 测试1: 连接测试
    logger.info("\n🔗 测试1: Lighter连接测试")
    connection_success = await test_lighter_connection()
    
    if connection_success:
        logger.info("✅ 连接测试通过")
    else:
        logger.error("❌ 连接测试失败")
        return
    
    # 测试2: 价格提取测试
    logger.info("\n💰 测试2: Lighter价格提取测试")
    price = await test_lighter_price_extraction()
    
    if price:
        logger.info(f"✅ 价格提取成功: ${price}")
        
        # 与币安价格对比
        try:
            import requests
            binance_resp = requests.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT", timeout=10)
            if binance_resp.status_code == 200:
                binance_price = float(binance_resp.json()["price"])
                price_diff = price - binance_price
                diff_percent = (price_diff / binance_price) * 100
                
                logger.info(f"📊 价格对比:")
                logger.info(f"   Lighter价格: ${price}")
                logger.info(f"   币安价格: ${binance_price}")
                logger.info(f"   价差: ${price_diff:+.2f} ({diff_percent:+.2f}%)")
        except Exception as e:
            logger.warning(f"获取币安价格失败: {str(e)}")
    else:
        logger.error("❌ 价格提取失败")
    
    logger.info("\n" + "=" * 60)
    logger.info("测试完成")

if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
"""
测试修复后的Lighter客户端
"""

import asyncio
import logging
from lighter_client import get_lighter_btc_price, LighterClient

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_lighter_price():
    """测试Lighter价格获取"""
    logger.info("开始测试修复后的Lighter价格获取...")
    
    try:
        async with LighterClient() as client:
            price = await client.get_btc_price()
            
            if price:
                logger.info(f"✅ Lighter BTC价格获取成功: ${price}")
                
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
                        
                        return price
                except Exception as e:
                    logger.warning(f"获取币安价格失败: {str(e)}")
                    return price
            else:
                logger.error("❌ Lighter价格获取失败")
                return None
                
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        return None

def test_sync_price():
    """测试同步接口"""
    logger.info("测试同步接口...")
    price = get_lighter_btc_price()
    
    if price:
        logger.info(f"✅ 同步接口获取成功: ${price}")
    else:
        logger.error("❌ 同步接口获取失败")
    
    return price

async def main():
    logger.info("=" * 50)
    logger.info("测试修复后的Lighter客户端")
    logger.info("=" * 50)
    
    # 测试异步接口
    logger.info("\n🔄 测试异步接口")
    async_price = await test_lighter_price()
    
    # 测试同步接口
    logger.info("\n🔄 测试同步接口")
    sync_price = test_sync_price()
    
    if async_price and sync_price:
        logger.info(f"\n🎉 所有测试通过!")
        logger.info(f"   异步价格: ${async_price}")
        logger.info(f"   同步价格: ${sync_price}")
        
        if abs(async_price - sync_price) < 100:  # 价差小于100美元认为正常
            logger.info("✅ 异步和同步接口结果一致")
        else:
            logger.warning(f"⚠️ 异步和同步接口结果差异较大: ${abs(async_price - sync_price)}")
    else:
        logger.error("❌ 测试失败")

if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
"""
测试修复后的EdgeX客户端
"""

import asyncio
import logging
from edgex_client import get_edgex_btc_price, EdgeXClient

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# EdgeX配置 - 从CLAUDE.md
EDGEX_CONFIG = {
    "account_id": "652994179613328022",
    "stark_private_key": "02d9aa779fe8c5767780108b8bd28ad87579039d41bd46cef2ffe09f50e1156a",
    "base_url": "https://pro.edgex.exchange",
    "contract_id": "10000001"  # BTCUSDT
}

async def test_edgex_price():
    """测试EdgeX价格获取"""
    logger.info("开始测试修复后的EdgeX价格获取...")
    
    try:
        async with EdgeXClient(
            EDGEX_CONFIG["base_url"],
            EDGEX_CONFIG["account_id"], 
            EDGEX_CONFIG["stark_private_key"]
        ) as client:
            price = await client.get_btc_price(EDGEX_CONFIG["contract_id"])
            
            if price:
                logger.info(f"✅ EdgeX BTC价格获取成功: ${price}")
                
                # 与币安价格对比
                try:
                    import requests
                    binance_resp = requests.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT", timeout=10)
                    if binance_resp.status_code == 200:
                        binance_price = float(binance_resp.json()["price"])
                        price_diff = price - binance_price
                        diff_percent = (price_diff / binance_price) * 100
                        
                        logger.info(f"📊 价格对比:")
                        logger.info(f"   EdgeX价格: ${price}")
                        logger.info(f"   币安价格: ${binance_price}")
                        logger.info(f"   价差: ${price_diff:+.2f} ({diff_percent:+.2f}%)")
                        
                        return price
                except Exception as e:
                    logger.warning(f"获取币安价格失败: {str(e)}")
                    return price
            else:
                logger.error("❌ EdgeX价格获取失败")
                return None
                
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        return None

def test_sync_price():
    """测试同步接口"""
    logger.info("测试同步接口...")
    price = get_edgex_btc_price(
        EDGEX_CONFIG["base_url"],
        EDGEX_CONFIG["account_id"],
        EDGEX_CONFIG["stark_private_key"],
        EDGEX_CONFIG["contract_id"]
    )
    
    if price:
        logger.info(f"✅ 同步接口获取成功: ${price}")
    else:
        logger.error("❌ 同步接口获取失败")
    
    return price

async def test_additional_methods():
    """测试其他方法"""
    logger.info("测试其他EdgeX方法...")
    
    try:
        async with EdgeXClient(
            EDGEX_CONFIG["base_url"],
            EDGEX_CONFIG["account_id"], 
            EDGEX_CONFIG["stark_private_key"]
        ) as client:
            
            # 测试服务器时间
            server_time = await client.get_server_time()
            if server_time:
                logger.info(f"✅ 服务器时间获取成功: {server_time}")
            else:
                logger.warning("⚠️ 服务器时间获取失败")
            
            # 测试合约信息
            contract_info = await client.get_contract_info()
            if contract_info:
                logger.info(f"✅ 合约信息获取成功: 包含 {len(contract_info)} 个字段")
            else:
                logger.warning("⚠️ 合约信息获取失败")
                
    except Exception as e:
        logger.warning(f"测试其他方法时出错: {str(e)}")

async def main():
    logger.info("=" * 50)
    logger.info("测试修复后的EdgeX客户端")
    logger.info("=" * 50)
    
    # 测试异步接口
    logger.info("\n🔄 测试异步接口")
    async_price = await test_edgex_price()
    
    # 测试同步接口
    logger.info("\n🔄 测试同步接口")
    sync_price = test_sync_price()
    
    # 测试其他方法
    logger.info("\n🔄 测试其他方法")
    await test_additional_methods()
    
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
#!/usr/bin/env python3
"""
测试价格采集器功能
运行一个简短的采集周期来验证系统功能
"""

import logging
import sys
import time
from datetime import datetime

from binance_client import BinanceClient
from aster_client import get_aster_btc_price
from database import get_database

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_price_collection():
    """测试价格采集功能"""
    logger.info("开始测试价格采集功能")
    
    # 初始化数据库
    db = get_database()
    logger.info("数据库连接成功")
    
    # 测试币安价格获取
    binance_client = BinanceClient()
    binance_price = binance_client.get_btc_price()
    
    if binance_price is None:
        logger.error("币安价格获取失败，测试终止")
        return False
    
    logger.info(f"币安BTC价格: {binance_price}")
    
    # 存储币安价格
    success = db.insert_price_data('binance', binance_price, binance_price, datetime.now())
    if not success:
        logger.error("币安价格存储失败")
        return False
    
    # 测试Aster价格获取
    try:
        aster_price = get_aster_btc_price(
            'ff85b8b9274891d65da610f8186bb02d4b0957de31b3322113ccae6f7b16ce7f',
            '90195d2412e0fb8dd06283d36223591458f83636deb629779aaf5eafe0fb026b'
        )
        
        if aster_price:
            logger.info(f"Aster BTC价格: {aster_price}")
            price_diff = aster_price - binance_price
            logger.info(f"Aster与币安价差: {price_diff:+.2f} USDT ({price_diff/binance_price*100:+.2f}%)")
            
            # 存储Aster价格
            success = db.insert_price_data('aster', aster_price, binance_price, datetime.now())
            if not success:
                logger.error("Aster价格存储失败")
                return False
        else:
            logger.warning("Aster价格获取失败")
    except Exception as e:
        logger.error(f"Aster价格获取异常: {str(e)}")
    
    # 测试数据查询
    latest_prices = db.get_latest_prices(10)
    logger.info(f"数据库中最新{len(latest_prices)}条价格记录:")
    for record in latest_prices[:5]:  # 只显示前5条
        logger.info(f"  {record['exchange']}: {record['price']} (价差: {record.get('price_diff', 'N/A')})")
    
    # 测试统计信息
    stats = db.get_statistics()
    logger.info(f"数据库统计: 总记录数 {stats['total_records']}, 活跃交易所 {stats['active_exchanges']}")
    
    logger.info("价格采集功能测试完成")
    return True

def test_web_api():
    """测试Web API功能"""
    logger.info("开始测试Web API功能")
    
    import requests
    
    base_url = "http://localhost:8080"
    
    try:
        # 测试统计API
        response = requests.get(f"{base_url}/api/statistics", timeout=5)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"统计API测试成功: {data['data']['total_records']} 条记录")
        else:
            logger.error(f"统计API测试失败: {response.status_code}")
            return False
        
        # 测试价格数据API
        response = requests.get(f"{base_url}/api/latest_prices?limit=5", timeout=5)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"价格数据API测试成功: {len(data['data'])} 条记录")
        else:
            logger.error(f"价格数据API测试失败: {response.status_code}")
            return False
        
        # 测试图表数据API
        response = requests.get(f"{base_url}/api/chart_data?hours=1", timeout=5)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"图表数据API测试成功: {len(data['data']['datasets'])} 个数据集")
        else:
            logger.error(f"图表数据API测试失败: {response.status_code}")
            return False
        
        logger.info("Web API功能测试完成")
        return True
        
    except requests.exceptions.ConnectionError:
        logger.error("无法连接到Web服务器，请确保web_server.py正在运行")
        return False
    except Exception as e:
        logger.error(f"Web API测试异常: {str(e)}")
        return False

def main():
    """主测试函数"""
    logger.info("=" * 60)
    logger.info("多交易所BTC价格对比系统 - 功能测试")
    logger.info("=" * 60)
    
    success_count = 0
    total_tests = 2
    
    # 测试1: 价格采集功能
    logger.info("\n[测试 1/2] 价格采集功能")
    if test_price_collection():
        success_count += 1
        logger.info("✅ 价格采集功能测试通过")
    else:
        logger.error("❌ 价格采集功能测试失败")
    
    # 测试2: Web API功能
    logger.info("\n[测试 2/2] Web API功能")
    if test_web_api():
        success_count += 1
        logger.info("✅ Web API功能测试通过")
    else:
        logger.error("❌ Web API功能测试失败")
    
    # 输出测试结果
    logger.info("\n" + "=" * 60)
    logger.info(f"测试完成: {success_count}/{total_tests} 项测试通过")
    
    if success_count == total_tests:
        logger.info("🎉 所有测试通过！系统功能正常")
        logger.info(f"🌐 Web界面访问地址: http://localhost:8080")
        logger.info("💡 启动完整系统命令:")
        logger.info("   nohup python3 price_collector.py > collector.log 2>&1 &")
        logger.info("   python3 web_server.py")
        return True
    else:
        logger.error("⚠️  部分测试失败，请检查系统配置")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
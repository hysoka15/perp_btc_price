#!/usr/bin/env python3
"""
历史数据补全采集器
检查过去一个月的数据，填充缺失的分钟级别数据
间隔3秒采集一次，专注于补全历史数据
"""

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from database import get_database
from binance_client import BinanceClient
from lighter_client import LighterClient
from edgex_client import EdgeXClient  
from aster_client import AsterClient

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backfill_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BackfillCollector:
    """历史数据补全采集器"""
    
    def __init__(self):
        self.db = get_database()
        self.running = True
        self.stats = {
            'total_checked': 0,
            'missing_found': 0,
            'collected': 0,
            'errors': 0
        }
        
        # 初始化交易所客户端
        self.init_clients()
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def init_clients(self):
        """初始化交易所客户端"""
        try:
            # 币安客户端
            self.binance_client = BinanceClient()
            
            # Aster客户端 - 从CLAUDE.md读取配置
            self.aster_client = AsterClient(
                api_key="ff85b8b9274891d65da610f8186bb02d4b0957de31b3322113ccae6f7b16ce7f",
                api_secret="90195d2412e0fb8dd06283d36223591458f83636deb629779aaf5eafe0fb026b"
            )
            
            # Lighter客户端
            self.lighter_client = LighterClient(
                base_url="https://mainnet.zklighter.elliot.ai"
            )
            
            # EdgeX客户端
            self.edgex_client = EdgeXClient(
                base_url="https://pro.edgex.exchange",
                account_id="652994179613328022",
                stark_private_key="02d9aa779fe8c5767780108b8bd28ad87579039d41bd46cef2ffe09f50e1156a"
            )
            
            logger.info("交易所客户端初始化完成")
            
        except Exception as e:
            logger.error(f"初始化交易所客户端失败: {e}")
            raise
    
    def signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，正在停止补全采集器...")
        self.running = False
    
    def get_missing_minutes(self, days_back: int = 30) -> List[datetime]:
        """获取缺失数据的分钟列表"""
        logger.info(f"检查过去 {days_back} 天的数据缺失情况...")
        
        # 计算时间范围
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(days=days_back)
        
        # 生成所有应该存在的分钟点
        expected_minutes = []
        current = start_time
        while current <= end_time:
            expected_minutes.append(current)
            current += timedelta(minutes=1)
        
        logger.info(f"预期分钟数: {len(expected_minutes)}")
        
        # 查询数据库中已有的分钟
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # 获取已有数据的分钟（去重）
                cursor.execute("""
                    SELECT DISTINCT strftime('%Y-%m-%d %H:%M', timestamp) as minute_key
                    FROM price_data 
                    WHERE timestamp >= ? AND timestamp <= ?
                    AND exchange IN ('binance', 'aster', 'lighter', 'edgex')
                """, (start_time, end_time))
                
                existing_minutes = {row[0] for row in cursor.fetchall()}
                
        except Exception as e:
            logger.error(f"查询已有数据失败: {e}")
            return []
        
        # 找出缺失的分钟
        missing_minutes = []
        for minute in expected_minutes:
            minute_key = minute.strftime('%Y-%m-%d %H:%M')
            if minute_key not in existing_minutes:
                missing_minutes.append(minute)
        
        logger.info(f"发现缺失分钟数: {len(missing_minutes)}")
        self.stats['missing_found'] = len(missing_minutes)
        
        # 按时间倒序排列（从最新的开始补全）
        missing_minutes.sort(reverse=True)
        
        return missing_minutes
    
    async def collect_for_minute(self, target_minute: datetime) -> bool:
        """为指定分钟采集数据"""
        logger.info(f"🔄 补全数据: {target_minute.strftime('%Y-%m-%d %H:%M')}")
        
        try:
            # 获取币安基准价格
            binance_price = self.binance_client.get_btc_price()
            if binance_price is None:
                logger.warning(f"无法获取币安基准价格，跳过 {target_minute}")
                return False
            
            # 插入币安数据
            success = self.db.insert_price_data(
                exchange="binance",
                price=binance_price,
                binance_price=binance_price,
                timestamp=target_minute
            )
            
            if success:
                logger.info(f"💾 BINANCE: {binance_price:.2f}")
            
            # 并发获取其他交易所价格
            tasks = [
                self.collect_aster_price(target_minute, binance_price),
                self.collect_lighter_price(target_minute, binance_price),
                self.collect_edgex_price(target_minute, binance_price)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = sum(1 for r in results if r is True)
            logger.info(f"✅ {target_minute.strftime('%H:%M')} 完成，成功采集 {success_count + 1}/4 个交易所")
            
            self.stats['collected'] += 1
            return True
            
        except Exception as e:
            logger.error(f"采集 {target_minute} 数据失败: {e}")
            self.stats['errors'] += 1
            return False
    
    async def collect_aster_price(self, timestamp: datetime, binance_price: float) -> bool:
        """采集Aster价格"""
        try:
            price = self.aster_client.get_btc_price()
            if price is not None:
                success = self.db.insert_price_data(
                    exchange="aster",
                    price=price,
                    binance_price=binance_price,
                    timestamp=timestamp
                )
                if success:
                    diff = price - binance_price
                    logger.info(f"💾 ASTER: {price:.2f} (价差: {diff:+.2f})")
                return success
        except Exception as e:
            logger.warning(f"Aster价格采集失败: {e}")
        return False
    
    async def collect_lighter_price(self, timestamp: datetime, binance_price: float) -> bool:
        """采集Lighter价格"""
        try:
            async with self.lighter_client as client:
                price = await client.get_btc_price()
                if price is not None:
                    success = self.db.insert_price_data(
                        exchange="lighter",
                        price=price,
                        binance_price=binance_price,
                        timestamp=timestamp
                    )
                    if success:
                        diff = price - binance_price
                        logger.info(f"💾 LIGHTER: {price:.2f} (价差: {diff:+.2f})")
                    return success
        except Exception as e:
            logger.warning(f"Lighter价格采集失败: {e}")
        return False
    
    async def collect_edgex_price(self, timestamp: datetime, binance_price: float) -> bool:
        """采集EdgeX价格"""
        try:
            async with self.edgex_client as client:
                price = await client.get_btc_price()
                if price is not None:
                    success = self.db.insert_price_data(
                        exchange="edgex",
                        price=price,
                        binance_price=binance_price,
                        timestamp=timestamp
                    )
                    if success:
                        diff = price - binance_price
                        logger.info(f"💾 EDGEX: {price:.2f} (价差: {diff:+.2f})")
                    return success
        except Exception as e:
            logger.warning(f"EdgeX价格采集失败: {e}")
        return False
    
    def print_progress(self, current: int, total: int):
        """打印进度"""
        percentage = (current / total) * 100 if total > 0 else 0
        logger.info(f"📊 进度: {current}/{total} ({percentage:.1f}%) | "
                   f"已采集: {self.stats['collected']} | "
                   f"错误: {self.stats['errors']}")
    
    async def run(self):
        """运行补全采集器"""
        logger.info("🚀 启动历史数据补全采集器")
        logger.info("📋 配置: 过去30天数据，每分钟检查，3秒间隔")
        
        try:
            # 获取缺失的分钟列表
            missing_minutes = self.get_missing_minutes(days_back=30)
            
            if not missing_minutes:
                logger.info("✅ 没有发现缺失数据，补全完成！")
                return
            
            logger.info(f"🎯 开始补全 {len(missing_minutes)} 个缺失的分钟数据")
            
            total_missing = len(missing_minutes)
            completed = 0
            
            # 逐个补全缺失数据
            for i, minute in enumerate(missing_minutes):
                if not self.running:
                    logger.info("收到停止信号，退出补全采集器")
                    break
                
                self.stats['total_checked'] += 1
                
                # 采集该分钟的数据
                success = await self.collect_for_minute(minute)
                
                if success:
                    completed += 1
                
                # 每10个打印一次进度
                if (i + 1) % 10 == 0 or (i + 1) == total_missing:
                    self.print_progress(i + 1, total_missing)
                
                # 3秒间隔
                if self.running and i < len(missing_minutes) - 1:
                    await asyncio.sleep(3)
            
            # 最终统计
            logger.info("=" * 50)
            logger.info("📊 补全采集器完成统计:")
            logger.info(f"  检查的分钟数: {self.stats['total_checked']}")
            logger.info(f"  发现缺失分钟: {self.stats['missing_found']}")
            logger.info(f"  成功补全分钟: {self.stats['collected']}")
            logger.info(f"  采集错误数: {self.stats['errors']}")
            completion_rate = (self.stats['collected'] / self.stats['missing_found'] * 100) if self.stats['missing_found'] > 0 else 0
            logger.info(f"  补全成功率: {completion_rate:.1f}%")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"补全采集器运行异常: {e}")
        finally:
            logger.info("历史数据补全采集器已停止")

async def main():
    """主函数"""
    collector = BackfillCollector()
    await collector.run()

if __name__ == "__main__":
    asyncio.run(main())
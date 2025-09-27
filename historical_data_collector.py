#!/usr/bin/env python3
"""
多交易所BTC价格历史数据采集器
每分钟采集一次四个交易所的BTC价格，用于建立历史数据库
适合长期后台运行，生成足够的历史数据用于价差分析
"""

import asyncio
import logging
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Optional

from binance_client import BinanceClient
from lighter_client import get_lighter_btc_price
from edgex_client import get_edgex_btc_price
from aster_client import get_aster_btc_price
from database import get_database

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('historical_collector.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class HistoricalDataCollector:
    """历史数据采集器 - 每分钟采集一次价格数据"""
    
    def __init__(self):
        self.running = False
        self.db = get_database()
        
        # 交易所配置 - 从CLAUDE.md中读取
        self.exchanges_config = {
            'binance': {
                'name': 'Binance',
                'enabled': True,
                'is_base': True
            },
            'lighter': {
                'name': 'Lighter',
                'enabled': True,
                'base_url': 'https://mainnet.zklighter.elliot.ai'
            },
            'edgex': {
                'name': 'EdgeX',
                'enabled': True,
                'base_url': 'https://pro.edgex.exchange',
                'contract_id': '10000001'
            },
            'aster': {
                'name': 'Aster',
                'enabled': True
            }
        }
        
        # 统计信息
        self.stats = {
            'total_collections': 0,
            'successful_collections': {},
            'failed_collections': {},
            'start_time': None,
            'next_collection_time': None
        }
        
        # 初始化统计计数器
        for exchange in self.exchanges_config:
            self.stats['successful_collections'][exchange] = 0
            self.stats['failed_collections'][exchange] = 0
        
        # 初始化客户端
        self.binance_client = BinanceClient()
        
        # 设置信号处理
        self.setup_signal_handlers()
    
    def setup_signal_handlers(self):
        """设置信号处理器，优雅退出"""
        def signal_handler(sig, frame):
            logger.info("收到退出信号，正在停止历史数据采集...")
            self.stop()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start(self):
        """启动历史数据采集"""
        if self.running:
            logger.warning("历史数据采集器已在运行")
            return
            
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        logger.info("🚀 启动多交易所BTC价格历史数据采集器")
        logger.info(f"📅 采集间隔: 每分钟")
        logger.info(f"🏦 启用的交易所: {[cfg['name'] for cfg in self.exchanges_config.values() if cfg['enabled']]}")
        logger.info(f"💾 数据库: SQLite")
        logger.info(f"📊 用途: 长期历史数据积累")
        
        # 启动采集循环
        self.run_collection_loop()
    
    def stop(self):
        """停止历史数据采集"""
        self.running = False
        logger.info("历史数据采集器已停止")
        self.print_final_stats()
    
    def run_collection_loop(self):
        """运行历史数据采集循环"""
        logger.info("⏰ 等待下一个整分钟开始采集...")
        
        while self.running:
            try:
                # 等待到下一个整分钟
                now = datetime.now()
                next_minute = now.replace(second=0, microsecond=0)
                if now.second > 0:
                    next_minute = next_minute.replace(minute=next_minute.minute + 1)
                    if next_minute.minute >= 60:
                        next_minute = next_minute.replace(hour=next_minute.hour + 1, minute=0)
                        if next_minute.hour >= 24:
                            next_minute = next_minute.replace(day=next_minute.day + 1, hour=0)
                
                self.stats['next_collection_time'] = next_minute
                wait_seconds = (next_minute - now).total_seconds()
                
                if wait_seconds > 0:
                    logger.info(f"⏳ 等待 {wait_seconds:.1f} 秒到下一个整分钟 ({next_minute.strftime('%H:%M:%S')})")
                    time.sleep(wait_seconds)
                
                # 开始采集
                collection_start = time.time()
                logger.info(f"🔄 开始第 {self.stats['total_collections'] + 1} 次历史数据采集 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # 获取基准价格（币安）
                base_price = self.get_binance_price()
                
                if base_price is None:
                    logger.error("❌ 无法获取币安基准价格，跳过本轮采集")
                    continue
                
                # 存储币安价格
                self.store_price('binance', base_price, base_price)
                
                # 并发获取其他交易所价格
                self.collect_other_exchanges_sync(base_price)
                
                self.stats['total_collections'] += 1
                collection_time = time.time() - collection_start
                
                logger.info(f"✅ 第 {self.stats['total_collections']} 次采集完成，耗时 {collection_time:.2f}秒")
                
                # 每小时输出一次统计信息
                if self.stats['total_collections'] % 60 == 0:
                    self.print_hourly_stats()
                
                # 每天清理一次旧数据（保留30天）
                if self.stats['total_collections'] % 1440 == 0:  # 1440分钟 = 1天
                    logger.info("🧹 开始清理30天前的旧数据...")
                    self.db.cleanup_old_data(days=30)
                    logger.info("✅ 数据清理完成")
                    
            except KeyboardInterrupt:
                logger.info("收到键盘中断信号")
                break
            except Exception as e:
                logger.error(f"采集循环异常: {str(e)}", exc_info=True)
                # 发生异常时等待60秒再继续
                time.sleep(60)
    
    def get_binance_price(self) -> Optional[float]:
        """获取币安BTC价格"""
        try:
            price = self.binance_client.get_btc_price()
            if price:
                self.stats['successful_collections']['binance'] += 1
                return price
            else:
                self.stats['failed_collections']['binance'] += 1
                return None
        except Exception as e:
            logger.error(f"获取币安价格失败: {str(e)}")
            self.stats['failed_collections']['binance'] += 1
            return None
    
    def collect_other_exchanges_sync(self, base_price: float):
        """同步方式采集其他交易所价格（避免事件循环冲突）"""
        def collect_lighter():
            if not self.exchanges_config['lighter']['enabled']:
                return
            try:
                price = get_lighter_btc_price()
                if price:
                    self.store_price('lighter', price, base_price)
                    self.stats['successful_collections']['lighter'] += 1
                else:
                    self.stats['failed_collections']['lighter'] += 1
            except Exception as e:
                logger.error(f"获取Lighter价格失败: {str(e)}")
                self.stats['failed_collections']['lighter'] += 1
        
        def collect_edgex():
            if not self.exchanges_config['edgex']['enabled']:
                return
            try:
                config = self.exchanges_config['edgex']
                price = get_edgex_btc_price(
                    config['base_url'],
                    config['account_id'],
                    config['stark_private_key'],
                    config['contract_id']
                )
                if price:
                    self.store_price('edgex', price, base_price)
                    self.stats['successful_collections']['edgex'] += 1
                else:
                    self.stats['failed_collections']['edgex'] += 1
            except Exception as e:
                logger.error(f"获取EdgeX价格失败: {str(e)}")
                self.stats['failed_collections']['edgex'] += 1
        
        def collect_aster():
            if not self.exchanges_config['aster']['enabled']:
                return
            try:
                config = self.exchanges_config['aster']
                price = get_aster_btc_price(
                    config['api_key'],
                    config['api_secret']
                )
                if price:
                    self.store_price('aster', price, base_price)
                    self.stats['successful_collections']['aster'] += 1
                else:
                    self.stats['failed_collections']['aster'] += 1
            except Exception as e:
                logger.error(f"获取Aster价格失败: {str(e)}")
                self.stats['failed_collections']['aster'] += 1
        
        # 创建并启动线程
        threads = []
        
        if self.exchanges_config['lighter']['enabled']:
            thread = threading.Thread(target=collect_lighter, daemon=True)
            threads.append(thread)
            thread.start()
        
        if self.exchanges_config['edgex']['enabled']:
            thread = threading.Thread(target=collect_edgex, daemon=True)
            threads.append(thread)
            thread.start()
        
        if self.exchanges_config['aster']['enabled']:
            thread = threading.Thread(target=collect_aster, daemon=True)
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成，最多等待30秒
        for thread in threads:
            thread.join(timeout=30)
    
    def store_price(self, exchange: str, price: float, base_price: float):
        """存储价格数据到数据库"""
        try:
            success = self.db.insert_price_data(
                exchange=exchange,
                price=price,
                binance_price=base_price,
                timestamp=datetime.now()
            )
            
            if success:
                price_diff = price - base_price
                diff_percent = (price_diff / base_price) * 100 if base_price > 0 else 0
                logger.info(f"💾 {exchange.upper()}: {price:.2f} (价差: {price_diff:+.2f}, {diff_percent:+.2f}%)")
            
        except Exception as e:
            logger.error(f"存储{exchange}价格数据失败: {str(e)}")
    
    def print_hourly_stats(self):
        """输出每小时统计信息"""
        if not self.stats['start_time']:
            return
            
        runtime = datetime.now() - self.stats['start_time']
        logger.info(f"\n📊 === 每小时统计 (运行时间: {runtime}) ===")
        logger.info(f"⏰ 总采集次数: {self.stats['total_collections']} 次 (每分钟1次)")
        
        for exchange in self.exchanges_config:
            if not self.exchanges_config[exchange]['enabled']:
                continue
                
            success = self.stats['successful_collections'][exchange]
            failed = self.stats['failed_collections'][exchange]
            total = success + failed
            success_rate = (success / total * 100) if total > 0 else 0
            
            logger.info(f"🏦 {exchange.upper()}: 成功 {success}, 失败 {failed}, 成功率 {success_rate:.1f}%")
        
        # 数据库统计
        db_stats = self.db.get_statistics()
        logger.info(f"💾 数据库记录总数: {db_stats.get('total_records', 0)}")
        
        # 预计下次采集时间
        if self.stats['next_collection_time']:
            logger.info(f"⏭️ 下次采集时间: {self.stats['next_collection_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("=" * 60)
    
    def print_final_stats(self):
        """输出最终统计信息"""
        logger.info("\n📋 === 最终统计报告 ===")
        self.print_hourly_stats()
        
        # 计算数据覆盖率
        runtime_hours = (datetime.now() - self.stats['start_time']).total_seconds() / 3600
        expected_collections = int(runtime_hours * 60)  # 每小时60次
        coverage = (self.stats['total_collections'] / expected_collections * 100) if expected_collections > 0 else 0
        
        logger.info(f"📈 数据覆盖率: {coverage:.1f}% ({self.stats['total_collections']}/{expected_collections})")
        
        # 输出各交易所状态
        exchange_status = self.db.get_exchange_status()
        if exchange_status:
            logger.info("\n🏦 交易所最新状态:")
            for status in exchange_status:
                logger.info(f"   {status['exchange']}: {status['status']}, 最后更新: {status['last_update']}, 最后价格: ${status['last_price']}")

def print_usage():
    """打印使用说明"""
    print("""
🚀 多交易所BTC价格历史数据采集器
=====================================

功能：
• 每分钟采集一次四个交易所的BTC价格
• 以币安为基准计算价差并存储到数据库
• 适合长期后台运行，积累历史数据
• 自动清理30天前的旧数据

交易所：
• Binance (基准)
• Lighter
• EdgeX
• Aster

使用方法：
python historical_data_collector.py

退出方式：
Ctrl+C 或发送 SIGTERM 信号

日志文件：
• 控制台输出：实时显示
• 文件日志：historical_collector.log

建议运行方式（后台）：
nohup python historical_data_collector.py > historical_collector.log 2>&1 &

""")

def main():
    """主函数"""
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        print_usage()
        return
    
    logger.info("📅 多交易所BTC价格历史数据采集器启动")
    
    try:
        collector = HistoricalDataCollector()
        collector.start()
        
    except KeyboardInterrupt:
        logger.info("用户终止程序")
    except Exception as e:
        logger.error(f"程序异常: {str(e)}", exc_info=True)
    finally:
        logger.info("历史数据采集器已退出")

if __name__ == "__main__":
    main()
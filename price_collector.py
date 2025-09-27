#!/usr/bin/env python3
"""
多交易所BTC价格采集器
每2秒采集一次币安、Lighter、EdgeX、Aster的BTC价格
以币安为基准，计算其他交易所的价差并存储到数据库
"""

import asyncio
import logging
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

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
        logging.FileHandler('price_collector.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class PriceCollector:
    """多交易所价格采集器"""
    
    def __init__(self):
        self.running = False
        self.db = get_database()
        
        # 交易所配置 - 从CLAUDE.md中读取
        self.exchanges_config = {
            'binance': {
                'name': 'Binance',
                'enabled': True,
                'is_base': True  # 币安作为基准价格
            },
            'lighter': {
                'name': 'Lighter',
                'enabled': True,  # 现在已修复，可以启用
                'base_url': 'https://mainnet.zklighter.elliot.ai'
            },
            'edgex': {
                'name': 'EdgeX',
                'enabled': True,
                'base_url': 'https://pro.edgex.exchange',
                'account_id': '652994179613328022',
                'stark_private_key': '02d9aa779fe8c5767780108b8bd28ad87579039d41bd46cef2ffe09f50e1156a',
                'contract_id': '10000001'  # BTCUSDT
            },
            'aster': {
                'name': 'Aster',
                'enabled': True,
                'api_key': 'ff85b8b9274891d65da610f8186bb02d4b0957de31b3322113ccae6f7b16ce7f',
                'api_secret': '90195d2412e0fb8dd06283d36223591458f83636deb629779aaf5eafe0fb026b'
            }
        }
        
        # 统计信息
        self.stats = {
            'total_collections': 0,
            'successful_collections': {},
            'failed_collections': {},
            'start_time': None
        }
        
        # 初始化各交易所客户端
        self.binance_client = BinanceClient()
        
        # 设置信号处理
        self.setup_signal_handlers()
    
    def setup_signal_handlers(self):
        """设置信号处理器，优雅退出"""
        def signal_handler(sig, frame):
            logger.info("收到退出信号，正在停止价格采集...")
            self.stop()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start(self):
        """启动价格采集"""
        if self.running:
            logger.warning("价格采集器已在运行")
            return
            
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        # 初始化统计计数器
        for exchange in self.exchanges_config:
            self.stats['successful_collections'][exchange] = 0
            self.stats['failed_collections'][exchange] = 0
        
        logger.info("启动多交易所BTC价格采集器")
        logger.info(f"采集间隔: 2秒")
        logger.info(f"启用的交易所: {[cfg['name'] for cfg in self.exchanges_config.values() if cfg['enabled']]}")
        
        # 启动采集循环
        self.run_collection_loop()
    
    def stop(self):
        """停止价格采集"""
        self.running = False
        logger.info("价格采集器已停止")
        self.print_final_stats()
    
    def run_collection_loop(self):
        """运行价格采集循环"""
        while self.running:
            try:
                start_time = time.time()
                
                # 获取基准价格（币安）
                base_price = self.get_binance_price()
                
                if base_price is None:
                    logger.error("无法获取币安基准价格，跳过本轮采集")
                    time.sleep(2)
                    continue
                
                # 存储币安价格
                self.store_price('binance', base_price, base_price)
                
                # 并发获取其他交易所价格
                self.collect_other_exchanges_async(base_price)
                
                self.stats['total_collections'] += 1
                
                # 每50次采集输出一次统计信息
                if self.stats['total_collections'] % 50 == 0:
                    self.print_stats()
                
                # 每1000次采集清理一次旧数据
                if self.stats['total_collections'] % 1000 == 0:
                    self.db.cleanup_old_data(days=7)
                
                # 控制采集间隔为2秒
                elapsed = time.time() - start_time
                sleep_time = max(0, 2.0 - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
            except KeyboardInterrupt:
                logger.info("收到键盘中断信号")
                break
            except Exception as e:
                logger.error(f"采集循环异常: {str(e)}", exc_info=True)
                time.sleep(2)
    
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
    
    def collect_other_exchanges_async(self, base_price: float):
        """并发采集其他交易所价格"""
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
        
        # 等待所有线程完成，最多等待1.5秒
        for thread in threads:
            thread.join(timeout=1.5)
    
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
                logger.info(f"{exchange.upper()}: {price:.2f} (基准: {base_price:.2f}, 价差: {price_diff:+.2f}, {diff_percent:+.2f}%)")
            
        except Exception as e:
            logger.error(f"存储{exchange}价格数据失败: {str(e)}")
    
    def print_stats(self):
        """输出统计信息"""
        if not self.stats['start_time']:
            return
            
        runtime = datetime.now() - self.stats['start_time']
        logger.info(f"\n===== 采集统计 (运行时间: {runtime}) =====")
        logger.info(f"总采集次数: {self.stats['total_collections']}")
        
        for exchange in self.exchanges_config:
            if not self.exchanges_config[exchange]['enabled']:
                continue
                
            success = self.stats['successful_collections'][exchange]
            failed = self.stats['failed_collections'][exchange]
            total = success + failed
            success_rate = (success / total * 100) if total > 0 else 0
            
            logger.info(f"{exchange.upper()}: 成功 {success}, 失败 {failed}, 成功率 {success_rate:.1f}%")
        
        # 数据库统计
        db_stats = self.db.get_statistics()
        logger.info(f"数据库记录总数: {db_stats.get('total_records', 0)}")
        logger.info("=" * 50)
    
    def print_final_stats(self):
        """输出最终统计信息"""
        logger.info("\n===== 最终统计 =====")
        self.print_stats()
        
        # 输出各交易所状态
        exchange_status = self.db.get_exchange_status()
        if exchange_status:
            logger.info("\n交易所状态:")
            for status in exchange_status:
                logger.info(f"{status['exchange']}: {status['status']}, 最后更新: {status['last_update']}, 最后价格: {status['last_price']}")

def main():
    """主函数"""
    logger.info("多交易所BTC价格采集器启动")
    
    try:
        collector = PriceCollector()
        collector.start()
        
    except KeyboardInterrupt:
        logger.info("用户终止程序")
    except Exception as e:
        logger.error(f"程序异常: {str(e)}", exc_info=True)
    finally:
        logger.info("价格采集器已退出")

if __name__ == "__main__":
    main()
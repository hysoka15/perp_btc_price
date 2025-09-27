import requests
import logging
import time
import hmac
import hashlib
from typing import Optional

logger = logging.getLogger(__name__)

class AsterClient:
    """Aster交易所客户端 - 获取BTC价格"""
    
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://fapi.asterdex.com"):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})
        self._time_offset = 0
        self._last_request_time = 0
        self._min_request_interval = 0.1  # 最小请求间隔100ms
        self._backoff_until = 0  # 限流时的等待时间
        self.sync_time()
    
    def sync_time(self):
        """获取服务器时间，校准本地时间戳"""
        try:
            url = f"{self.base_url}/fapi/v1/time"
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            server_time = resp.json()["serverTime"]
            local_time = int(time.time() * 1000)
            self._time_offset = server_time - local_time
            logger.debug(f"Aster时间同步完成，时间偏移: {self._time_offset}ms")
        except Exception as e:
            logger.warning(f"Aster时间同步失败: {str(e)}")
            self._time_offset = 0
    
    def _timestamp(self):
        """返回校准后的时间戳"""
        return int(time.time() * 1000) + self._time_offset
    
    def _wait_if_needed(self):
        """实现请求间隔控制和backoff等待"""
        current_time = time.time()
        
        # 检查是否在backoff期间
        if current_time < self._backoff_until:
            wait_time = self._backoff_until - current_time
            logger.info(f"Aster限流等待中，还需等待{wait_time:.1f}秒")
            time.sleep(wait_time)
        
        # 确保最小请求间隔
        time_since_last = current_time - self._last_request_time
        if time_since_last < self._min_request_interval:
            time.sleep(self._min_request_interval - time_since_last)
        
        self._last_request_time = time.time()

    def _check_rate_limit_headers(self, response):
        """检查响应头中的限流信息"""
        # 检查权重使用情况
        weight_1m = response.headers.get('X-MBX-USED-WEIGHT-1M')
        if weight_1m:
            weight_1m = int(weight_1m)
            if weight_1m > 800:  # 接近1200的限制时预警
                logger.warning(f"Aster API权重使用较高: {weight_1m}/1200")
                if weight_1m > 1000:
                    # 设置1分钟的backoff
                    self._backoff_until = time.time() + 60
                    logger.warning("Aster API权重过高，设置1分钟冷却期")

    def get_btc_price(self, symbol: str = "BTCUSDT") -> Optional[float]:
        """获取BTC合约价格"""
        self._wait_if_needed()
        
        try:
            # 只使用最简单的价格接口，避免多次调用
            url = f"{self.base_url}/fapi/v1/ticker/price"
            params = {"symbol": symbol}
            
            response = self.session.get(url, params=params, timeout=10)
            
            # 检查429错误
            if response.status_code == 429:
                logger.warning("Aster返回429限流错误，设置30秒冷却期")
                self._backoff_until = time.time() + 30
                return None
            
            response.raise_for_status()
            self._check_rate_limit_headers(response)
            
            data = response.json()
            if 'price' in data:
                price = float(data['price'])
                logger.info(f"Aster BTC价格: {price}")
                return price
                
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                logger.warning("Aster返回429限流错误，设置30秒冷却期")
                self._backoff_until = time.time() + 30
            else:
                logger.warning(f"获取Aster价格失败: {str(e)}")
        except Exception as e:
            logger.warning(f"获取Aster价格失败: {str(e)}")
        
        return None
    
    def get_exchange_info(self) -> Optional[dict]:
        """获取交易所信息"""
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"获取Aster交易所信息失败: {str(e)}")
            return None
    
    def get_server_time(self) -> Optional[dict]:
        """获取服务器时间"""
        try:
            url = f"{self.base_url}/fapi/v1/time"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"获取Aster服务器时间失败: {str(e)}")
            return None

# 同步接口封装
def get_aster_btc_price(api_key: str, api_secret: str, symbol: str = "BTCUSDT") -> Optional[float]:
    """同步获取Aster BTC价格的便捷函数"""
    try:
        client = AsterClient(api_key, api_secret)
        return client.get_btc_price(symbol)
    except Exception as e:
        logger.error(f"获取Aster价格失败: {str(e)}")
        return None
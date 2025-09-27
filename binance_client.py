import requests
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

class BinanceClient:
    """币安交易所客户端 - 获取BTC合约价格作为基准价"""
    
    def __init__(self):
        self.base_url = "https://fapi.binance.com"
        self.session = requests.Session()
        
    def get_btc_price(self) -> Optional[float]:
        """获取BTCUSDT合约价格作为基准价格"""
        try:
            url = f"{self.base_url}/fapi/v1/ticker/price"
            params = {"symbol": "BTCUSDT"}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            price = float(data["price"])
            
            logger.info(f"币安BTC合约价格: {price}")
            return price
            
        except Exception as e:
            logger.error(f"获取币安BTC价格失败: {str(e)}")
            return None
    
    def get_symbol_info(self, symbol: str = "BTCUSDT") -> Optional[dict]:
        """获取交易对信息"""
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            for symbol_info in data["symbols"]:
                if symbol_info["symbol"] == symbol:
                    return symbol_info
            return None
            
        except Exception as e:
            logger.error(f"获取币安交易对信息失败: {str(e)}")
            return None
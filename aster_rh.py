import requests
import time
import hmac
import random
import hashlib
import logging
import pandas as pd
import csv
import sys
import traceback
import os
import datetime


# Configure logging
def setup_logging():
    # 创建日志目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 创建当前日期时间的字符串，用于日志文件名
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 设置主日志文件
    main_log_file = os.path.join(log_dir, f"aster_{current_time}.log")
    
    # 设置错误日志文件
    error_log_file = os.path.join(log_dir, f"aster_error_{current_time}.log")
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 设置日志格式
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)
    
    # 创建主日志文件处理器
    file_handler = logging.FileHandler(main_log_file, encoding='utf-8')
    file_handler.setFormatter(log_format)
    root_logger.addHandler(file_handler)
    
    # 创建错误日志文件处理器，只记录错误和严重错误
    error_file_handler = logging.FileHandler(error_log_file, encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(log_format)
    root_logger.addHandler(error_file_handler)
    
    return main_log_file, error_log_file

# 设置日志
main_log_file, error_log_file = setup_logging()
logger = logging.getLogger('AsterDexClient')
logger.info(f"日志已设置。主日志文件: {main_log_file}, 错误日志文件: {error_log_file}")


# 添加一个专门记录交易错误的函数
def log_trade_error(error_type, account_id, group_id, details, exception=None):
    """记录交易错误到错误日志
    
    Args:
        error_type: 错误类型 (如 "开仓失败", "平仓失败")
        account_id: 账户ID
        group_id: 分组ID
        details: 错误详情
        exception: 异常对象
    """
    error_msg = f"[交易错误] {error_type} - 账户 {account_id} (分组 {group_id}): {details}"
    if exception:
        error_msg += f"\n异常: {str(exception)}\n{traceback.format_exc()}"
    
    logger.error(error_msg)
    
    # 额外记录到专门的文件 (可选)
    with open("failed_accounts.log", "a", encoding='utf-8') as f:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{timestamp} - {error_msg}\n")
        if exception:
            f.write(f"异常详情: {str(exception)}\n")
            f.write(f"堆栈跟踪:\n{traceback.format_exc()}\n")
        f.write("-" * 80 + "\n")


def wait_random(a, b):
    t = random.uniform(a, b)
    logger.info(f"随机等待 {t:.2f} 秒...")
    time.sleep(t)

# 添加API请求间隔时间
API_REQUEST_DELAY = 1.0  # 每次API请求后等待1秒

class AsterRestClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://fapi.asterdex.com", 
                 proxy_url: str = None, proxy_username: str = None, proxy_password: str = None):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.base_url = base_url
        self.session = requests.Session()
        
        # 配置代理
        if proxy_url:
            self._setup_proxy(proxy_url, proxy_username, proxy_password)
        
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})
        self._time_offset = 0
        self.sync_time()
    
    def _setup_proxy(self, proxy_url: str, username: str = None, password: str = None):
        """智能代理配置 - 优先SOCKS5h，备用HTTP"""
        try:
            logger.info(f"代理配置: {proxy_url} (用户名: {username})")
            
            success = False
            working_method = None
            
            if username and password:
                # 方案1: SOCKS5h (最佳选择，完全隐藏IP)
                socks5h_proxy = f"socks5h://{username}:{password}@{proxy_url}"
                try:
                    logger.info("尝试SOCKS5h代理配置...")
                    proxies = {
                        'http': socks5h_proxy,
                        'https': socks5h_proxy
                    }
                    self.session.proxies.clear()
                    self.session.proxies.update(proxies)
                    
                    # SOCKS5测试需要更长时间
                    if self._test_proxy_with_timeout("SOCKS5h", 15):
                        logger.info("✅ SOCKS5h代理配置成功 (完全隐藏IP)")
                        working_method = "SOCKS5h"
                        success = True
                    else:
                        logger.warning("❌ SOCKS5h测试失败，尝试备用方案")
                        
                except Exception as e:
                    logger.warning(f"SOCKS5h配置失败: {str(e)}")
                
                # 方案2: HTTP代理 (备用方案)
                if not success:
                    try:
                        logger.info("尝试HTTP代理配置...")
                        http_proxy = f"http://{username}:{password}@{proxy_url}"
                        proxies = {
                            'http': http_proxy,
                            'https': http_proxy
                        }
                        self.session.proxies.clear()
                        self.session.proxies.update(proxies)
                        
                        if self._test_proxy("HTTP"):
                            logger.info("✅ HTTP代理配置成功 (可能暴露HTTPS请求IP)")
                            working_method = "HTTP"
                            success = True
                        else:
                            logger.warning("❌ HTTP代理测试失败")
                            
                    except Exception as e:
                        logger.warning(f"HTTP代理配置失败: {str(e)}")
                
                # 方案3: 仅HTTP代理 (最后备用)
                if not success:
                    try:
                        logger.info("尝试仅HTTP代理配置...")
                        http_proxy = f"http://{username}:{password}@{proxy_url}"
                        proxies = {'http': http_proxy}  # 仅HTTP
                        self.session.proxies.clear()
                        self.session.proxies.update(proxies)
                        
                        if self._test_proxy("HTTP仅限"):
                            logger.warning("⚠️ 仅HTTP代理配置成功 (HTTPS请求将暴露真实IP)")
                            working_method = "HTTP仅限"
                            success = True
                            
                    except Exception as e:
                        logger.warning(f"仅HTTP代理配置失败: {str(e)}")
            
            if not success:
                logger.error("🚫 所有代理方案失败，使用直连")
                self.session.proxies.clear()
                working_method = "直连"
            
            # 最终验证
            if working_method:
                self._verify_ip_privacy(working_method)
            
        except Exception as e:
            logger.error(f"代理配置失败: {str(e)}")
            # 不抛出异常，允许程序继续运行
    
    def _test_proxy(self, method_name="代理"):
        """测试代理连接"""
        return self._test_proxy_with_timeout(method_name, 10)
    
    def _test_proxy_with_timeout(self, method_name="代理", timeout=10):
        """带自定义超时的代理测试"""
        try:
            test_url = "http://httpbin.org/ip"
            response = self.session.get(test_url, timeout=timeout)
            if response.status_code == 200:
                ip_info = response.json()
                detected_ip = ip_info.get('origin', 'unknown')
                logger.info(f"{method_name}测试成功，当前IP: {detected_ip}")
                return True
            else:
                logger.warning(f"{method_name}测试失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            logger.warning(f"{method_name}测试失败: {str(e)}")
            return False
    
    def _verify_ip_privacy(self, method_name):
        """验证IP隐私保护效果"""
        try:
            logger.info(f"🔍 验证{method_name}的IP隐私保护...")
            
            # 测试HTTPS请求
            response = self.session.get("https://httpbin.org/ip", timeout=10)
            if response.status_code == 200:
                ip_info = response.json()
                https_ip = ip_info.get('origin', 'unknown')
                logger.info(f"HTTPS请求IP: {https_ip}")
                
                # 测试Aster API
                response = self.session.get(f"{self.base_url}/fapi/v1/time", timeout=10)
                if response.status_code == 200:
                    if method_name == "SOCKS5h":
                        logger.info("🔒 Aster服务器看到: 代理IP (完全隐藏)")
                    elif method_name == "HTTP":
                        logger.warning("⚠️ Aster服务器可能看到: 代理IP或真实IP")
                    elif method_name == "HTTP仅限":
                        logger.warning("🔓 Aster服务器看到: 真实IP (HTTPS直连)")
                    else:
                        logger.error("🚫 Aster服务器看到: 真实IP (直连)")
                else:
                    logger.error(f"Aster API测试失败: {response.status_code}")
            else:
                logger.warning(f"HTTPS测试失败: {response.status_code}")
                
        except Exception as e:
            logger.warning(f"IP隐私验证失败: {str(e)}")

    def sync_time(self):
        # 获取服务器时间，校准本地时间戳
        url = f"{self.base_url}/fapi/v1/time"
        resp = self.session.get(url)
        resp.raise_for_status()
        server_time = resp.json()["serverTime"]
        local_time = int(time.time() * 1000)
        self._time_offset = server_time - local_time

    def _timestamp(self):
        # 返回校准后的时间戳
        return int(time.time() * 1000) + self._time_offset

    def _sign(self, params: dict) -> str:
        # 按照key顺序拼接参数
        query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
        signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
        return signature

    def get_account_balance(self, recvWindow: int = 20000):
        """获取账户余额信息"""
        max_retries = 4
        for retry in range(max_retries):
            try:
                url_time = f"{self.base_url}/fapi/v1/time"
                resp_time = requests.get(url_time)
                resp_time.raise_for_status()
                timestamp = resp_time.json()["serverTime"]
                
                params = {
                    "recvWindow": recvWindow,
                    "timestamp": timestamp
                }
                query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
                signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
                url = f"{self.base_url}/fapi/v2/balance?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"获取账户余额失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                    time.sleep(3)
                else:
                    logger.error(f"获取账户余额失败，已达最大重试次数: {str(e)}")
                    raise
        
    def get_max_quantity(self, symbol: str, leverage: int = 100):
        """计算最大可买卖数量"""
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 获取账户余额
                balances = self.get_account_balance()
                usdt_balance = 0
                
                # 找到USDT余额
                for asset in balances:
                    if asset.get("asset") == "USDT":
                        usdt_balance = float(asset.get("availableBalance", 0))
                        break
                
                # 获取当前BTC价格
                url = f"{self.base_url}/fapi/v1/ticker/price?symbol={symbol}"
                resp = requests.get(url)
                resp.raise_for_status()
                btc_price = float(resp.json().get("price", 0))
                
                if btc_price <= 0:
                    raise ValueError("无法获取有效的BTC价格")
                
                # 计算最大可买数量(考虑杠杆)
                max_usdt = usdt_balance * leverage
                max_quantity = max_usdt / btc_price
                
                # 按BTC的最小交易单位进行四舍五入
                # 通常BTC最小单位是0.001
                precision = 3
                max_quantity = round(max_quantity, precision)
                
                logger.info(f"账户USDT余额: {usdt_balance}, BTC价格: {btc_price}, 最大可交易数量: {max_quantity}")
                return max_quantity
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"计算最大交易数量失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                    time.sleep(3)
                else:
                    logger.error(f"计算最大交易数量失败，已达最大重试次数: {str(e)}")
                    raise

    def set_leverage(self, symbol: str, leverage: int, recvWindow: int = 20000):
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 1. 获取服务器时间
                url_time = f"{self.base_url}/fapi/v1/time"
                resp_time = requests.get(url_time)
                resp_time.raise_for_status()
                timestamp = resp_time.json()["serverTime"]
                
                # 2. 参数按字典序拼接
                params = {
                    "symbol": symbol,
                    "leverage": leverage,
                    "recvWindow": recvWindow,
                    "timestamp": timestamp
                }
                query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
                
                # 3. 用apiSecret做HMAC SHA256签名
                signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
                
                # 4. 拼接完整URL
                url = f"{self.base_url}/fapi/v1/leverage?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                resp = requests.post(url, headers=headers)
                resp.raise_for_status()
                result = resp.json()
                logger.info(f"设置杠杆: {result['symbol']} 杠杆: {result['leverage']}倍")
                return result
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"设置杠杆失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                    time.sleep(3)
                else:
                    logger.error(f"设置杠杆失败，已达最大重试次数: {str(e)}")
                    raise

    def create_order(self, symbol: str, side: str, quantity: float, leverage: int = 100, recvWindow: int = 20000):
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 先调整杠杆
                self.set_leverage(symbol, leverage, recvWindow)
                
                # 1. 获取服务器时间
                url_time = f"{self.base_url}/fapi/v1/time"
                resp_time = requests.get(url_time)
                resp_time.raise_for_status()
                timestamp = resp_time.json()["serverTime"]
                
                # 2. 参数按字典序拼接
                params = {
                    "symbol": symbol,
                    "side": side,
                    "type": "MARKET",
                    "quantity": quantity,
                    "recvWindow": recvWindow,
                    "timestamp": timestamp
                }
                query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
                
                # 3. 用apiSecret做HMAC SHA256签名
                signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
                
                # 4. 拼接完整URL
                url = f"{self.base_url}/fapi/v1/order?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                resp = requests.post(url, headers=headers)
                resp.raise_for_status()
                result = resp.json()
                logger.info(f"下单成功: {result['symbol']} {result['side']} {result['origQty']} 单号: {result['orderId']}")
                return result
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"下单失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                    time.sleep(3)
                else:
                    logger.error(f"下单失败，已达最大重试次数: {str(e)}")
                    raise

    def cancel_all_orders(self, symbol: str, recvWindow: int = 20000):
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 1. 获取服务器时间
                url_time = f"{self.base_url}/fapi/v1/time"
                resp_time = requests.get(url_time)
                resp_time.raise_for_status()
                timestamp = resp_time.json()["serverTime"]
                
                # 2. 参数按字典序拼接
                params = {
                    "symbol": symbol,
                    "recvWindow": recvWindow,
                    "timestamp": timestamp
                }
                query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
                
                # 3. 用apiSecret做HMAC SHA256签名
                signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
                
                # 4. 拼接完整URL
                url = f"{self.base_url}/fapi/v1/allOpenOrders?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                resp = requests.delete(url, headers=headers)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"取消所有订单失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                    time.sleep(3)
                else:
                    logger.error(f"取消所有订单失败，已达最大重试次数: {str(e)}")
                    raise

    def get_positions(self, symbol: str, recvWindow: int = 20000):
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 查询当前持仓
                url_time = f"{self.base_url}/fapi/v1/time"
                resp_time = requests.get(url_time)
                resp_time.raise_for_status()
                timestamp = resp_time.json()["serverTime"]
                params = {
                    "symbol": symbol,
                    "recvWindow": recvWindow,
                    "timestamp": timestamp
                }
                query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
                signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
                url = f"{self.base_url}/fapi/v2/positionRisk?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                
                positions = resp.json()
                for pos in positions:
                    amt = float(pos.get("positionAmt", 0))
                    if amt != 0:
                        logger.info(f"当前持仓: {pos['symbol']} 数量: {pos['positionAmt']} 方向: {'多' if float(pos['positionAmt']) > 0 else '空'} 未实现盈亏: {pos['unRealizedProfit']}")
                        
                return positions
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"获取持仓信息失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                    time.sleep(3)
                else:
                    logger.error(f"获取持仓信息失败，已达最大重试次数: {str(e)}")
                    raise

    def close_all_positions(self, symbol: str, recvWindow: int = 20000):
        # 查询所有持仓方向，逐一平仓
        try:
            positions = self.get_positions(symbol, recvWindow)
            closed_positions = []
            
            for pos in positions:
                amt = float(pos.get("positionAmt", 0))
                if amt == 0:
                    continue
                
                position_side = pos.get("positionSide", "BOTH")
                side = "SELL" if amt > 0 else "BUY"
                qty = abs(amt)
                
                # 使用重试机制进行平仓
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        # 1. 获取服务器时间
                        url_time = f"{self.base_url}/fapi/v1/time"
                        resp_time = requests.get(url_time)
                        resp_time.raise_for_status()
                        timestamp = resp_time.json()["serverTime"]
                        
                        # 2. 参数
                        params = {
                            "symbol": symbol,
                            "side": side,
                            "type": "MARKET",
                            "reduceOnly": "true",
                            "quantity": qty,
                            "recvWindow": recvWindow,
                            "timestamp": timestamp
                        }
                        
                        # positionSide 只在双向持仓时需要
                        if position_side != "BOTH":
                            params["positionSide"] = position_side
                            
                        query_string = "&".join([f"{k}={params[k]}" for k in sorted(params)])
                        signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
                        url = f"{self.base_url}/fapi/v1/order?{query_string}&signature={signature}"
                        headers = {"X-MBX-APIKEY": self.api_key}
                        
                        resp = requests.post(url, headers=headers)
                        resp.raise_for_status()
                        result = resp.json()
                        
                        closed_positions.append({
                            "symbol": pos["symbol"],
                            "side": side,
                            "quantity": qty,
                            "profit": pos["unRealizedProfit"]
                        })
                        
                        logger.info(f"平仓成功: {symbol} {side} {qty} 单号: {result['orderId']}")
                        break  # 如果成功，跳出重试循环
                        
                    except Exception as e:
                        if retry < max_retries - 1:
                            logger.warning(f"平仓失败，3秒后重试 ({retry+1}/{max_retries}): {str(e)}")
                            time.sleep(3)
                        else:
                            logger.error(f"平仓失败，已达最大重试次数: {str(e)}")
                            # 继续尝试平仓其他仓位
            
            if not closed_positions:
                logger.info("没有需要平仓的持仓")
                
            return closed_positions
            
        except Exception as e:
            logger.error(f"平仓过程中发生错误: {str(e)}")
            # 返回空列表表示平仓失败
            return []

    def get_usdt_balance(self):
        """获取USDT余额"""
        balances = self.get_account_balance()
        for asset in balances:
            if asset.get("asset") == "USDT":
                return float(asset.get("availableBalance", 0))
        return 0

def load_accounts_from_excel(excel_file):
    """从Excel文件加载账户信息"""
    try:
        df = pd.read_excel(excel_file)
        logger.info(f"共读取到 {len(df)} 个账户")
        
        if len(df) < 1:
            logger.error("未找到有效账户信息")
            return None
            
        return df
    except Exception as e:
        logger.error(f"加载账户信息失败: {str(e)}")
        return None

def close_all_positions_for_accounts(excel_file, symbol="BTCUSDT"):
    """为所有账户平仓"""
    accounts = load_accounts_from_excel(excel_file)
    if accounts is None:
        return
    
    logger.info(f"准备为所有账户平仓 {symbol}")
    
    # 检查是否有group列，用于分组输出结果
    has_group_column = 'group' in accounts.columns
    
    # 初始化汇总数据
    groups = {}
    total_initial = 0
    total_final = 0
    
    for idx, row in accounts.iterrows():
        account_id = row['编号']
        api_key = str(row['api_key']).strip()
        api_secret = str(row['api_secret']).strip()
        
        # 如果有分组信息，记录分组
        group_id = str(row['group']).strip() if has_group_column else "default"
        
        # 获取代理配置（如果有的话）
        proxy_url = None
        proxy_username = None
        proxy_password = None
        
        if 'proxy_url' in accounts.columns:
            proxy_url = str(row['proxy_url']).strip() if pd.notna(row['proxy_url']) else None
        if 'proxy_username' in accounts.columns:
            proxy_username = str(row['proxy_username']).strip() if pd.notna(row['proxy_username']) else None
        if 'proxy_password' in accounts.columns:
            proxy_password = str(row['proxy_password']).strip() if pd.notna(row['proxy_password']) else None
        
        # 初始化分组数据
        if group_id not in groups:
            groups[group_id] = {
                'initial_total': 0,
                'final_total': 0,
                'accounts': []
            }
        
        groups[group_id]['accounts'].append(account_id)
        
        try:
            # 创建客户端（带代理支持）
            client = AsterRestClient(api_key, api_secret, 
                                   proxy_url=proxy_url, 
                                   proxy_username=proxy_username, 
                                   proxy_password=proxy_password)
            logger.info(f"账户 {account_id} (分组 {group_id}) 开始平仓")
            
            # 获取平仓前余额
            balance_before = client.get_usdt_balance()
            logger.info(f"账户 {account_id} 平仓前USDT余额: {balance_before}")
            
            # 将余额添加到分组和总计
            groups[group_id]['initial_total'] += balance_before
            total_initial += balance_before
            
            # 平仓
            result = client.close_all_positions(symbol=symbol)
            
            # 获取平仓后余额
            balance_after = client.get_usdt_balance()
            logger.info(f"账户 {account_id} 平仓后USDT余额: {balance_after}")
            
            # 更新分组和总计
            groups[group_id]['final_total'] += balance_after
            total_final += balance_after
            
            # 计算变化
            balance_change = balance_after - balance_before
            change_percent = (balance_change/balance_before*100) if balance_before > 0 else 0
            logger.info(f"账户 {account_id} 余额变化: {balance_change:.8f} USDT ({change_percent:.4f}% 如果为正则盈利，为负则亏损)")
            
        except Exception as e:
            error_msg = f"紧急平仓失败: {str(e)}"
            log_trade_error("紧急平仓失败", account_id, group_id, error_msg, e)
        
        # 在处理下一个账户前等待一段时间，避免API请求过于频繁
        time.sleep(API_REQUEST_DELAY * 3)
    
    # 输出分组结果
    logger.info("\n===== 平仓结果分析 =====")
    
    # 显示各组结果
    for group_id, data in groups.items():
        change = data['final_total'] - data['initial_total']
        change_percent = (change/data['initial_total']*100) if data['initial_total'] > 0 else 0
        logger.info(f"分组 {group_id}: 初始余额 {data['initial_total']:.8f} USDT, 最终余额 {data['final_total']:.8f} USDT, 净变化: {change:.8f} USDT ({change_percent:.4f}%)")
    
    # 显示总体结果
    total_change = total_final - total_initial
    change_percent = (total_change/total_initial*100) if total_initial > 0 else 0
    logger.info(f"\n所有账户总计: 初始余额 {total_initial:.8f} USDT, 最终余额 {total_final:.8f} USDT")
    logger.info(f"总盈亏: {total_change:.8f} USDT ({change_percent:.4f}%)")

def run_hedge_trading(excel_file, symbol="BTCUSDT", leverage=100, position_percent=0.5, default_wait_time=610):
    """运行对冲交易"""
    # 批量处理钱包配置
    df = load_accounts_from_excel(excel_file)
    if df is None:
        return
    
    # 确保至少有2个账户
    account_count = len(df)
    if account_count < 2:
        logger.error("需要至少2个账户进行对冲操作")
        return
    
    # 检查Excel是否包含group和wait_time列
    has_group_column = 'group' in df.columns
    has_wait_time_column = 'wait_time' in df.columns
    
    if not has_group_column:
        logger.warning("Excel文件中未找到'group'列，将为每2个账户创建一个默认分组")
    
    if not has_wait_time_column:
        logger.warning(f"Excel文件中未找到'wait_time'列，将使用默认等待时间{default_wait_time}秒")
    
    # 初始化所有账户的客户端和数据
    clients = {}
    account_data = {}
    groups = {}
    
    # 处理所有账户
    for idx, row in df.iterrows():
        account_id = row['编号']
        api_key = str(row['api_key']).strip()
        api_secret = str(row['api_secret']).strip()
        
        # 获取group信息，如果不存在则根据顺序分配
        if has_group_column:
            group = str(row['group']).strip()
        else:
            group = str((idx // 2) + 1)  # 每2个账户一组
        
        # 获取wait_time信息，如果不存在则使用默认值
        if has_wait_time_column:
            wait_time = int(row['wait_time'])
        else:
            wait_time = default_wait_time
        
        # 获取代理配置（如果有的话）
        proxy_url = None
        proxy_username = None
        proxy_password = None
        
        if 'proxy_url' in df.columns:
            proxy_url = str(row['proxy_url']).strip() if pd.notna(row['proxy_url']) else None
        if 'proxy_username' in df.columns:
            proxy_username = str(row['proxy_username']).strip() if pd.notna(row['proxy_username']) else None
        if 'proxy_password' in df.columns:
            proxy_password = str(row['proxy_password']).strip() if pd.notna(row['proxy_password']) else None
        
        try:
            # 创建客户端（带代理支持）
            client = AsterRestClient(api_key, api_secret,
                                   proxy_url=proxy_url,
                                   proxy_username=proxy_username,
                                   proxy_password=proxy_password)
            # 获取初始余额
            initial_balance = client.get_usdt_balance()
            # 获取最大可交易数量
            max_qty = client.get_max_quantity(symbol, leverage)
            
            # 存储账户数据
            account_data[account_id] = {
                'client': client,
                'initial_balance': initial_balance,
                'max_quantity': max_qty,
                'group': group,
                'wait_time': wait_time,
                'final_balance': None
            }
            
            # 将账户添加到对应的分组
            if group not in groups:
                groups[group] = []
            groups[group].append(account_id)
            
            logger.info(f"账户 {account_id} 初始USDT余额: {initial_balance}, 最大可交易数量: {max_qty}, 分组: {group}, 等待时间: {wait_time}秒")
            
            # 每处理一个账户后等待一段时间，避免API请求过于频繁
            time.sleep(API_REQUEST_DELAY * 2)
            
        except Exception as e:
            error_msg = f"账户初始化失败: {str(e)}"
            log_trade_error("账户初始化失败", account_id, group, error_msg, e)
    
    # 过滤出有效的分组（至少包含2个账户）
    valid_groups = {}
    for group_id, account_ids in groups.items():
        if len(account_ids) >= 2:
            valid_groups[group_id] = account_ids
        else:
            logger.warning(f"分组 {group_id} 只有 {len(account_ids)} 个账户，至少需要2个账户才能进行对冲交易，将跳过此分组")
    
    if not valid_groups:
        logger.error("没有有效的对冲交易分组，至少需要一个包含2个以上账户的分组")
        return
    
    logger.info(f"共有 {len(valid_groups)} 个有效交易分组")
    
    # 创建一个线程列表，用于跟踪所有平仓线程
    closing_threads = []
    
    # 处理每个分组 - 错开开仓时间
    group_processed_count = 0
    for group_id, account_ids in valid_groups.items():
        # 如果不是第一个分组，随机等待1-2分钟，错开开仓时间
        if group_processed_count > 0:
            stagger_wait_time = random.uniform(30, 60)  # 1-2分钟的随机等待
            logger.info(f"错开开仓时间: 等待 {stagger_wait_time:.1f} 秒后开始处理分组 {group_id}...")
            time.sleep(stagger_wait_time)
        
        group_processed_count += 1
        group_accounts = [account_data[acc_id] for acc_id in account_ids]
        wait_time = group_accounts[0]['wait_time']  # 使用组内第一个账户的等待时间
        
        logger.info(f"\n===== 处理分组 {group_id} =====")
        logger.info(f"分组 {group_id} 包含 {len(account_ids)} 个账户，等待时间: {wait_time}秒")
        
        # 获取组内所有账户的最小可交易数量
        min_quantity = min(account['max_quantity'] for account in group_accounts)
        trade_quantity = min_quantity * position_percent
        trade_quantity = round(trade_quantity, 3)
        logger.info(f"分组 {group_id} 对冲交易数量: {trade_quantity} (最大值的{position_percent*100}%)")
        
        # 分配交易方向：第一个账户做多，其余账户做空
        long_account = account_ids[0]
        short_accounts = account_ids[1:]
        
        # 计算每个做空账户的交易量，确保精确匹配
        if len(short_accounts) > 0:
            # 保持原有精度
            total_short_quantity = trade_quantity
            
            # 计算每个账户的基础数量（不处理余数）
            base_short_quantity = total_short_quantity / len(short_accounts)
            base_short_quantity_rounded = round(base_short_quantity, 3)
            
            # 计算使用基础数量后的总量
            base_total = base_short_quantity_rounded * len(short_accounts)
            
            # 计算余量（可能有正负）
            remainder = round(total_short_quantity - base_total, 3)
            
            # 分配各账户的做空量
            short_quantities = [base_short_quantity_rounded] * len(short_accounts)
            
            # 如果有余量，分配给第一个做空账户
            if remainder != 0:
                short_quantities[0] = round(short_quantities[0] + remainder, 3)
            
            # 验证总和是否正确
            actual_short_total = sum(short_quantities)
            logger.info(f"分组 {group_id} 做多数量: {trade_quantity}, 做空总数量: {actual_short_total} (账户分配: {short_quantities})")
            
            if round(actual_short_total, 3) != round(trade_quantity, 3):
                logger.warning(f"分组 {group_id} 做空总量 {actual_short_total} 与做多量 {trade_quantity} 不一致，差额: {round(trade_quantity - actual_short_total, 6)}")
        else:
            logger.error(f"分组 {group_id} 没有做空账户，无法进行对冲交易")
            continue
        
        # 做多账户开仓
        try:
            logger.info(f"分组 {group_id} - 账户 {long_account} 开始做多 {symbol}, 数量: {trade_quantity}")
            account_data[long_account]['client'].create_order(
                symbol=symbol, 
                side="BUY", 
                quantity=trade_quantity, 
                leverage=leverage
            )
        except Exception as e:
            error_msg = f"做多失败: {str(e)}"
            log_trade_error("开仓失败(多)", long_account, group_id, error_msg, e)
            continue  # 如果做多失败，跳过这个组
        
        time.sleep(API_REQUEST_DELAY * 2)
        
        # 做空账户开仓
        short_success = True
        for i, short_account in enumerate(short_accounts):
            try:
                short_qty = short_quantities[i]
                logger.info(f"分组 {group_id} - 账户 {short_account} 开始做空 {symbol}, 数量: {short_qty}")
                account_data[short_account]['client'].create_order(
                    symbol=symbol, 
                    side="SELL", 
                    quantity=short_qty, 
                    leverage=leverage
                )
                time.sleep(API_REQUEST_DELAY * 2)
            except Exception as e:
                error_msg = f"做空失败: 数量 {short_qty}: {str(e)}"
                log_trade_error("开仓失败(空)", short_account, group_id, error_msg, e)
                short_success = False
        
        if not short_success:
            logger.warning(f"分组 {group_id} 有账户做空失败，该组对冲可能不完全")
        
        # 开启一个新线程来处理这个组的平仓
        import threading
        
        def close_group_positions(group_id, account_ids, wait_time):
            logger.info(f"分组 {group_id} 将在 {wait_time} 秒后平仓...")
            time.sleep(wait_time)
            
            logger.info(f"开始平仓分组 {group_id}...")
            for account_id in account_ids:
                try:
                    logger.info(f"分组 {group_id} - 账户 {account_id} 开始平仓")
                    account_data[account_id]['client'].close_all_positions(symbol=symbol)
                    
                    # 获取平仓后余额
                    final_balance = account_data[account_id]['client'].get_usdt_balance()
                    account_data[account_id]['final_balance'] = final_balance
                    
                    logger.info(f"分组 {group_id} - 账户 {account_id} 平仓完成")
                    time.sleep(API_REQUEST_DELAY * 2)
                except Exception as e:
                    error_msg = f"平仓失败: {str(e)}"
                    log_trade_error("平仓失败", account_id, group_id, error_msg, e)
            
            # 该分组的交易结果分析
            analyze_group_results(group_id, account_ids, account_data)
        
        # 启动平仓线程
        closing_thread = threading.Thread(
            target=close_group_positions, 
            args=(group_id, account_ids, wait_time), 
            daemon=True
        )
        closing_thread.start()
        closing_threads.append(closing_thread)
        
        logger.info(f"分组 {group_id} 开仓完成，平仓线程已启动")
    
    # 等待所有交易组完成
    # 计算最大等待时间：最长持仓时间 + 每组错开时间（最大值）× 组数 + 额外余量
    max_group_wait_time = max(account_data[acc_id]['wait_time'] for acc_id in account_data)
    max_stagger_time = 180  # 最大错开时间
    group_count = len(valid_groups)
    extra_buffer_time = 120  # 额外缓冲时间
    
    # 总最大等待时间 = 最长持仓时间 + 所有组错开时间 + 缓冲
    max_wait_time = max_group_wait_time + (max_stagger_time * (group_count - 1)) + extra_buffer_time
    
    logger.info(f"等待所有交易组完成，计算详情:")
    logger.info(f"- 最长持仓时间: {max_group_wait_time}秒")
    logger.info(f"- 组数: {group_count}，最大错开时间: {max_stagger_time}秒")
    logger.info(f"- 额外缓冲时间: {extra_buffer_time}秒")
    logger.info(f"- 总计最长等待时间: {max_wait_time}秒")
    
    time.sleep(max_wait_time)
    
    # 分析总体交易结果
    analyze_total_results(account_data)
    
    logger.info("所有对冲交易完成")

def analyze_group_results(group_id, account_ids, account_data):
    """分析特定组的交易结果"""
    logger.info(f"\n===== 分组 {group_id} 交易结果分析 =====")
    
    # 计算组内总初始余额
    initial_balances = [account_data[acc_id]['initial_balance'] for acc_id in account_ids]
    group_initial_total = sum(initial_balances)
    
    # 显示交易前账户余额
    logger.info(f"分组 {group_id} 交易前余额:")
    for acc_id in account_ids:
        initial = account_data[acc_id]['initial_balance']
        logger.info(f"账户 {acc_id}: {initial:.8f} USDT")
    logger.info(f"分组 {group_id} 交易前总余额: {group_initial_total:.8f} USDT")
    
    # 显示交易后账户余额
    logger.info(f"\n分组 {group_id} 交易后余额:")
    valid_final_balances = []
    for acc_id in account_ids:
        final = account_data[acc_id]['final_balance']
        if final is not None:
            logger.info(f"账户 {acc_id}: {final:.8f} USDT")
            valid_final_balances.append(final)
        else:
            logger.info(f"账户 {acc_id}: 无法获取余额")
    
    # 计算组内总盈亏
    if valid_final_balances:
        group_final_total = sum(valid_final_balances)
        group_total_change = group_final_total - group_initial_total
        logger.info(f"分组 {group_id} 交易后总余额: {group_final_total:.8f} USDT")
        logger.info(f"分组 {group_id} 总盈亏: {group_total_change:.8f} USDT ({group_total_change/group_initial_total*100:.4f}%)")
    else:
        logger.warning(f"分组 {group_id} 无法获取任何账户的最终余额，无法计算盈亏")

def analyze_total_results(account_data):
    """分析所有账户的总体交易结果"""
    logger.info("\n===== 总体交易结果分析 =====")
    
    # 按分组显示结果
    groups = {}
    for acc_id, data in account_data.items():
        group = data['group']
        if group not in groups:
            groups[group] = {
                'accounts': [],
                'initial_total': 0,
                'final_total': 0,
                'has_valid_final': False
            }
        groups[group]['accounts'].append(acc_id)
        groups[group]['initial_total'] += data['initial_balance']
        if data['final_balance'] is not None:
            groups[group]['final_total'] += data['final_balance']
            groups[group]['has_valid_final'] = True
    
    # 显示各组结果
    total_initial = 0
    total_final = 0
    for group_id, data in groups.items():
        total_initial += data['initial_total']
        if data['has_valid_final']:
            total_final += data['final_total']
            change = data['final_total'] - data['initial_total']
            logger.info(f"分组 {group_id}: 初始余额 {data['initial_total']:.8f} USDT, 最终余额 {data['final_total']:.8f} USDT, 净变化: {change:.8f} USDT ({change/data['initial_total']*100:.4f}%)")
        else:
            logger.info(f"分组 {group_id}: 初始余额 {data['initial_total']:.8f} USDT, 无法获取最终余额")
    
    # 显示总体结果
    if total_final > 0:
        total_change = total_final - total_initial
        logger.info(f"\n所有账户总计: 初始余额 {total_initial:.8f} USDT, 最终余额 {total_final:.8f} USDT")
        logger.info(f"总盈亏: {total_change:.8f} USDT ({total_change/total_initial*100:.4f}%)")

if __name__ == "__main__":
    try:
        # Excel文件名
        excel_file = 'accounts_rh.xlsx'  # 改为你的Excel文件名
        
        # 记录程序开始
        logger.info("=" * 80)
        logger.info(f"Aster DEX 对冲交易程序启动 - 版本 1.0")
        logger.info(f"Excel文件: {excel_file}")
        logger.info("=" * 80)
        
        # 检查参数，第一个参数是程序名称
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            # 如果命令是 closeall，直接全部平仓
            if command == "closeall":
                logger.info("执行直接平仓命令")
                try:
                    symbol = "BTCUSDT"
                    # 如果提供了交易对参数
                    if len(sys.argv) > 2:
                        symbol = sys.argv[2].upper()
                    
                    logger.info(f"开始执行所有账户平仓操作，交易对: {symbol}")
                    close_all_positions_for_accounts(excel_file, symbol)
                    logger.info(f"所有账户 {symbol} 平仓指令已执行完毕")
                except Exception as e:
                    error_msg = f"全局平仓过程中发生错误: {str(e)}"
                    logger.error(f"{error_msg}\n{traceback.format_exc()}")
                    # 记录到失败账户日志
                    with open("failed_accounts.log", "a", encoding='utf-8') as f:
                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        f.write(f"{timestamp} - [全局错误] {error_msg}\n")
                        f.write(f"堆栈跟踪:\n{traceback.format_exc()}\n")
                        f.write("-" * 80 + "\n")
                sys.exit(0)
        
        # 默认执行对冲交易
        logger.info("开始执行对冲交易策略")
        run_hedge_trading(excel_file)
        logger.info("对冲交易程序执行完毕")
        
    except Exception as e:
        error_msg = f"对冲交易程序发生全局错误: {str(e)}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        # 记录到失败账户日志
        with open("failed_accounts.log", "a", encoding='utf-8') as f:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{timestamp} - [全局错误] {error_msg}\n")
            f.write(f"堆栈跟踪:\n{traceback.format_exc()}\n")
            f.write("-" * 80 + "\n")
    finally:
        # 程序结束记录
        logger.info("=" * 80)
        logger.info(f"Aster DEX 对冲交易程序结束 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

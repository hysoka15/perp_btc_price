# 多交易所BTC价格历史数据采集系统

## 📋 系统概述

本系统包含两个独立的价格采集器：

1. **实时价格采集器** (`price_collector.py`) - 每2秒采集一次，用于实时监控
2. **历史数据采集器** (`historical_data_collector.py`) - 每分钟采集一次，用于长期历史数据积累

## 🏦 支持的交易所

- **Binance** (币安) - 作为基准价格
- **Lighter** - zkSync Era上的去中心化交易所
- **EdgeX** - 专业衍生品交易所
- **Aster** - 去中心化衍生品协议

## 📊 历史数据采集器特点

### 功能特性
- ⏰ **定时采集**: 每分钟整点采集一次
- 💾 **数据持久化**: 自动存储到SQLite数据库
- 🔄 **容错机制**: 单个交易所失败不影响其他交易所
- 📈 **价差计算**: 自动计算与币安的价格差异
- 🧹 **自动清理**: 30天后自动清理旧数据
- 📋 **统计报告**: 每小时输出采集统计

### 适用场景
- 长期价差分析
- 套利机会历史研究  
- 市场波动性统计
- 交易所流动性对比
- 价格预测模型训练

## 🚀 快速开始

### 1. 启动历史数据采集器

```bash
# 后台启动
./start_historical_collector.sh

# 查看状态
./status_historical_collector.sh

# 停止服务
./stop_historical_collector.sh
```

### 2. 查看实时日志

```bash
# 实时查看日志
tail -f historical_collector.log

# 查看最近50行
tail -50 historical_collector.log
```

### 3. 查看数据库统计

```bash
python3 -c "
from database import get_database
db = get_database()
stats = db.get_statistics()
print(f'总记录数: {stats[\"total_records\"]}')
"
```

## 📁 文件结构

```
├── historical_data_collector.py     # 主采集器程序
├── start_historical_collector.sh    # 启动脚本
├── stop_historical_collector.sh     # 停止脚本  
├── status_historical_collector.sh   # 状态检查脚本
├── historical_collector.log         # 日志文件
├── historical_collector.pid         # 进程ID文件
├── prices.db                        # SQLite数据库
└── logs/                            # 日志目录
```

## 🔧 配置说明

### 交易所配置
所有交易所配置都在 `historical_data_collector.py` 中，从 `CLAUDE.md` 读取：

```python
self.exchanges_config = {
    'binance': {
        'name': 'Binance',
        'enabled': True,
        'is_base': True  # 作为基准价格
    },
    'lighter': {
        'name': 'Lighter',
        'enabled': True,
        'base_url': 'https://mainnet.zklighter.elliot.ai'
    },
    # ... 其他交易所配置
}
```

### 采集间隔
- 默认: 每分钟整点采集
- 修改: 在 `run_collection_loop()` 方法中调整等待逻辑

### 数据保留
- 默认: 保留30天历史数据
- 修改: 调整 `cleanup_old_data(days=30)` 参数

## 📊 数据库结构

### price_data 表
```sql
CREATE TABLE price_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    price REAL NOT NULL,
    binance_price REAL,
    price_diff REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### 查询示例

```sql
-- 查看最近的价格数据
SELECT * FROM price_data 
ORDER BY timestamp DESC 
LIMIT 10;

-- 计算平均价差（最近24小时）
SELECT exchange, 
       AVG(price_diff) as avg_diff,
       COUNT(*) as records
FROM price_data 
WHERE timestamp > datetime('now', '-1 day')
GROUP BY exchange;

-- 查看价差波动
SELECT exchange,
       MIN(price_diff) as min_diff,
       MAX(price_diff) as max_diff,
       AVG(price_diff) as avg_diff
FROM price_data
WHERE timestamp > datetime('now', '-7 day')
GROUP BY exchange;
```

## 📈 监控和维护

### 系统监控
```bash
# 检查进程状态
./status_historical_collector.sh

# 监控系统资源
top -p $(cat historical_collector.pid)

# 查看数据库大小
du -h prices.db
```

### 日志管理
```bash
# 清理日志文件
> historical_collector.log

# 压缩历史日志
gzip historical_collector.log.old

# 设置日志轮转（可选）
logrotate /etc/logrotate.d/historical_collector
```

### 数据备份
```bash
# 备份数据库
cp prices.db prices_backup_$(date +%Y%m%d).db

# 导出CSV格式
sqlite3 prices.db -header -csv "SELECT * FROM price_data;" > prices_export.csv
```

## ⚡ 性能优化

### 内存优化
- 使用连接池减少数据库连接开销
- 批量插入数据减少I/O操作
- 定期清理内存中的价格历史

### 网络优化  
- 设置合理的HTTP超时时间
- 使用连接复用减少建连时间
- 实现重试机制处理网络异常

### 存储优化
- 定期压缩数据库文件
- 创建索引优化查询性能
- 分表存储大量历史数据

## 🛠️ 故障排除

### 常见问题

1. **采集器无法启动**
   ```bash
   # 检查依赖
   python3 -c "import sqlite3, requests, asyncio"
   
   # 检查端口占用
   ./status_historical_collector.sh
   ```

2. **某个交易所价格获取失败**
   ```bash
   # 查看具体错误日志
   grep -i "error\|failed" historical_collector.log
   
   # 单独测试交易所连接
   python3 -c "from binance_client import BinanceClient; print(BinanceClient().get_btc_price())"
   ```

3. **数据库错误**
   ```bash
   # 检查数据库完整性
   sqlite3 prices.db "PRAGMA integrity_check;"
   
   # 重建数据库索引
   sqlite3 prices.db "REINDEX;"
   ```

4. **磁盘空间不足**
   ```bash
   # 清理旧数据
   python3 -c "from database import get_database; get_database().cleanup_old_data(days=7)"
   
   # 压缩数据库
   sqlite3 prices.db "VACUUM;"
   ```

## 🔄 系统集成

### 与Web界面集成
历史数据采集器与现有的Web可视化界面共享同一个数据库，数据会自动在图表中显示。

### 与实时采集器协同
两个采集器可以同时运行：
- 实时采集器: 提供2秒级别的实时数据
- 历史采集器: 提供分钟级别的长期数据

### API接口
可以通过数据库直接查询历史数据，或者扩展Web服务器提供REST API。

## 📞 支持

如有问题，请检查：
1. 日志文件中的错误信息
2. 系统资源使用情况  
3. 网络连接状态
4. 数据库完整性

---

**注意**: 建议将历史数据采集器添加到系统启动项，确保服务器重启后自动恢复采集。
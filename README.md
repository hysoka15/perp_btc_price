# 多交易所BTC价格对比系统

实时对比多个交易所的BTC合约价格差距，提供可视化显示。

## 功能特性

- 🔄 每2秒自动获取价格数据
- 📊 以币安为基准，计算其他交易所的价差
- 📈 Web界面展示价差K线图
- 💾 SQLite数据库持久化存储
- ⚡ 实时更新价格和状态
- 🌐 支持多交易所：币安、Lighter、EdgeX、Aster

## 系统架构

```
├── binance_client.py     # 币安交易所API客户端
├── lighter_client.py     # Lighter交易所API客户端
├── edgex_client.py       # EdgeX交易所API客户端
├── aster_client.py       # Aster交易所API客户端
├── database.py           # 数据库管理
├── price_collector.py    # 价格采集主程序
├── web_server.py         # Web服务器
└── templates/
    └── index.html        # Web界面
```

## 安装和运行

### 1. 安装依赖

```bash
# 安装Python依赖
pip install -r requirements.txt

# 安装Lighter SDK
pip install -e ./lighter-python

# 安装EdgeX SDK  
pip install -e ./edgex-python-sdk
```

### 2. 配置环境变量

复制环境变量模板并填入你的API密钥：

```bash
cp .env.example .env
# 编辑 .env 文件，填入你的真实API密钥
```

**需要配置的环境变量:**
- `ASTER_API_KEY`: Aster交易所API密钥
- `ASTER_API_SECRET`: Aster交易所API秘钥
- `EDGEX_ACCOUNT_ID`: EdgeX账户ID
- `EDGEX_STARK_PRIVATE_KEY`: EdgeX Stark私钥
- `EDGEX_BASE_URL`: EdgeX API地址（默认: https://pro.edgex.exchange）

**⚠️ 重要安全提醒:**
- 绝不要将真实的API密钥提交到代码仓库
- 使用环境变量管理所有敏感信息
- 确保 `.env` 文件已被 `.gitignore` 忽略

### 3. 启动系统

```bash
# 启动价格采集器（后台运行）
nohup python3 price_collector.py > collector.log 2>&1 &

# 启动Web服务器
python3 web_server.py
```

### 4. 访问Web界面

打开浏览器访问：http://localhost:5000

## 使用说明

### Web界面功能

1. **价差K线图**
   - 横轴：时间
   - 纵轴：与币安的价差（USDT）
   - 正数：高于币安价格
   - 负数：低于币安价格

2. **时间范围选择**
   - 1小时、6小时、24小时、3天、7天

3. **实时数据**
   - 每5秒自动刷新
   - 显示交易所状态
   - 显示最新价格和价差

4. **统计信息**
   - 总记录数
   - 活跃交易所数量
   - 数据时间范围

### 数据存储

所有价格数据存储在SQLite数据库中：
- 文件名：`price_data.db`
- 表结构：价格数据、交易所状态
- 自动清理7天前的旧数据

## 监控和日志

### 日志文件
- `price_collector.log` - 价格采集日志
- `collector.log` - 后台运行日志

### 监控指标
- 各交易所成功率
- 价格获取频率
- 数据库写入状态

## 故障排除

### 常见问题

1. **无法获取某交易所价格**
   - 检查API密钥配置
   - 检查网络连接
   - 查看日志错误信息

2. **Web界面无数据**
   - 确认价格采集器正在运行
   - 检查数据库文件是否存在
   - 查看Web服务器日志

3. **图表不显示**
   - 检查浏览器控制台错误
   - 确认Chart.js加载成功
   - 验证API接口返回数据

### 重启服务

```bash
# 停止价格采集器
pkill -f price_collector.py

# 重新启动
nohup python3 price_collector.py > collector.log 2>&1 &

# 重启Web服务器
pkill -f web_server.py
python3 web_server.py
```

## 技术细节

### API接口

- `GET /` - 主页面
- `GET /api/chart_data?hours=24` - 获取图表数据
- `GET /api/exchange_status` - 获取交易所状态
- `GET /api/latest_prices` - 获取最新价格
- `GET /api/statistics` - 获取统计信息

### 数据格式

```json
{
  "success": true,
  "data": {
    "labels": ["时间1", "时间2", ...],
    "datasets": [
      {
        "label": "LIGHTER 价差",
        "data": [价差1, 价差2, ...],
        "borderColor": "#007bff"
      }
    ]
  }
}
```

## 开发说明

### 添加新交易所

1. 创建新的客户端文件（如 `newexchange_client.py`）
2. 实现价格获取方法
3. 在 `price_collector.py` 中添加配置
4. 更新Web界面颜色配置

### 修改采集频率

编辑 `price_collector.py` 中的睡眠时间：
```python
time.sleep(2)  # 改为其他秒数
```

### 自定义图表样式

编辑 `templates/index.html` 中的Chart.js配置。
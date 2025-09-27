# Railway部署指南

## 第一步：创建Railway项目

1. 访问 https://railway.app/
2. 用GitHub账号登录
3. 点击 "New Project"
4. 选择 "Deploy from GitHub repo" 
5. 搜索并选择你的仓库: `hysoka15/perp_btc_price`
6. 点击 "Deploy Now"

## 第二步：配置环境变量

在Railway项目面板中，点击 "Variables" 标签，添加以下环境变量：

```
# Aster交易所配置
ASTER_API_KEY=你的新aster_api_key
ASTER_API_SECRET=你的新aster_api_secret

# EdgeX交易所配置  
EDGEX_ACCOUNT_ID=你的edgex_account_id
EDGEX_STARK_PRIVATE_KEY=你的edgex_stark_private_key
EDGEX_BASE_URL=https://pro.edgex.exchange

# Lighter交易所配置（如果需要）
LIGHTER_PRIVATE_KEY=你的lighter_private_key
LIGHTER_ACCOUNT_INDEX=你的lighter_account_index

# Flask配置
FLASK_SECRET_KEY=random_secret_key_for_flask
ENABLE_RATE_LIMITING=true
MAX_REQUESTS_PER_MINUTE=60
```

## 第三步：确认部署配置

Railway会自动检测到以下文件：
- `requirements.txt` - Python依赖
- `web_server.py` - 主应用文件

确保以下配置正确：

### 1. 应用启动端口
`web_server.py` 已配置为从环境变量读取端口：
```python
port = int(os.environ.get('PORT', 8080))
app.run(host='0.0.0.0', port=port, debug=False)
```

### 2. 数据库文件
SQLite数据库文件 `price_data.db` 会在首次运行时自动创建。

## 第四步：启动后台数据采集

部署成功后，需要在Railway控制台执行以下命令启动价格采集器：

```bash
# 启动历史数据采集器（每分钟采集一次）
python3 historical_data_collector.py &

# 或者启动实时数据采集器（每2秒采集一次）  
python3 price_collector.py &
```

## 第五步：访问应用

部署完成后，Railway会提供一个公网URL，形如：
`https://your-app-name.up.railway.app`

访问该URL即可看到BTC价格对比图表。

## 注意事项

1. **数据持久化**: Railway的文件系统不是持久的，重启后数据库文件会丢失。建议后续升级为PostgreSQL。

2. **环境变量安全**: 确保使用新的API密钥，旧密钥已经废弃。

3. **监控日志**: 在Railway控制台的"Logs"标签可以查看应用运行日志。

4. **资源限制**: Railway免费版有使用限制，注意监控资源使用情况。

## 故障排除

如果部署失败，检查：
1. 环境变量是否正确设置
2. requirements.txt中的依赖是否都能正常安装
3. 查看Railway部署日志中的错误信息
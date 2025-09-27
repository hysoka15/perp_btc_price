项目需求：对比多个交易所直接btc合约价格的差距，要求可视化显示。

## 系统架构

1. **币安交易所**：作为基准价格源
   - 获取BTCUSDT合约价格作为基准

2. **其他交易所**：价格对比源
   - **Lighter**：使用lighter-python SDK
   - **EdgeX**：使用edgex-python-sdk  
   - **Aster**：使用REST API

## 配置说明

⚠️ **安全提醒**：所有API密钥均通过环境变量配置，请参考 `.env.example` 文件。

**交易所配置**：
- **Lighter**：base_url: https://mainnet.zklighter.elliot.ai
- **EdgeX**：base_url: https://pro.edgex.exchange  
- **Aster**：base_url: https://fapi.asterdex.com

**环境变量要求**：
- `ASTER_API_KEY` 和 `ASTER_API_SECRET`
- `EDGEX_ACCOUNT_ID` 和 `EDGEX_STARK_PRIVATE_KEY`
- 其他配置参数

3.每隔 2s读取一次全平台的价格，所有价格都要持久化，保存到数据库中

4.最后要可视化：像 k线图一样，横轴是时间，纵轴是各个交易所与币安的价差（币安作为基准，高于币安价格的为正数显示，低于的负数显示）
每个交易所不同时间段的价差，可以连成一条线

需要把这个可视化的图可以通过浏览器访问
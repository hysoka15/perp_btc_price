项目需求：对比多个交易所直接btc合约价格的差距，要求可视化显示。

1.读取币安交易所，btcusdt交易对的合约价格，作为基准价。
币安api:
bp6YKzhVlBZat5TnnDHG6UMPs73muOTWYrD3Hh9X9NZUasP0sp4EuE7v7Qgs6dqt
api_secret:
u0xAA5zH503tjrLJsBexJFfszAOvqvaauQEoiD0pQ6vX39M0bcHcqQ3muGAp2zTy

2.读取多个交易所的btc合约价格，比如 lighter,edgex，aster
lighter,可以参考lighter-python这个官方sdk，示例用法simple_buy_sell_trader.py
edgex可以参考官方sdk:edgex-python-sdk 示例用法runbot.py
aster可以参考用法：aster_rh.py

可以使用的账户参数：
lighter：
base-url:https://mainnet.zklighter.elliot.ai
API_KEY_PRIVATE_KEY：0xc044fec1ca6f7dfafacd04e8a74ba7142639352a3fff322ad54540cb5e78ec26f17b46ac01ae3922
ACCOUNT_INDEX:76835
API_KEY_INDEX:2

edgex:
"edgex": {
    "account_id": "652994179613328022",
    "stark_private_key": "02d9aa779fe8c5767780108b8bd28ad87579039d41bd46cef2ffe09f50e1156a",
    "base_url": "https://pro.edgex.exchange",
    "ws_url": "wss://quote.edgex.exchange"
  }

aster:
api_key:ff85b8b9274891d65da610f8186bb02d4b0957de31b3322113ccae6f7b16ce7f
api_secret:90195d2412e0fb8dd06283d36223591458f83636deb629779aaf5eafe0fb026b

3.每隔 2s读取一次全平台的价格，所有价格都要持久化，保存到数据库中

4.最后要可视化：像 k线图一样，横轴是时间，纵轴是各个交易所与币安的价差（币安作为基准，高于币安价格的为正数显示，低于的负数显示）
每个交易所不同时间段的价差，可以连成一条线

需要把这个可视化的图可以通过浏览器访问
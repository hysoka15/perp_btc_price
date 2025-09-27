import asyncio
import datetime
import lighter
import logging
import os

logging.basicConfig(level=logging.INFO)


# 使用环境变量配置敏感信息 - 安全最佳实践
L1_ADDRESS = '0xa9f604330ee1f7d75a4ebd245a4ffbc2499c80c7'
ACCOUNT_INDEX = 70691

if not L1_ADDRESS:
    raise ValueError("❌ 请设置环境变量 L1_ADDRESS")
if not ACCOUNT_INDEX:
    raise ValueError("❌ 请设置环境变量 ACCOUNT_INDEX")


async def print_api(method, *args, **kwargs):
    logging.info(f"{method.__name__}: {await method(*args, **kwargs)}")


async def account_apis(client: lighter.ApiClient):
    logging.info("ACCOUNT APIS")
    account_instance = lighter.AccountApi(client)
    await print_api(account_instance.account, by="l1_address", value=L1_ADDRESS)
    await print_api(account_instance.account, by="index", value=str(ACCOUNT_INDEX))
    await print_api(account_instance.accounts_by_l1_address, l1_address=L1_ADDRESS)
    await print_api(account_instance.apikeys, account_index=ACCOUNT_INDEX, api_key_index=2)
    await print_api(account_instance.public_pools, filter="all", limit=1, index=0)


async def block_apis(client: lighter.ApiClient):
    logging.info("BLOCK APIS")
    block_instance = lighter.BlockApi(client)
    await print_api(block_instance.block, by="height", value="1")
    await print_api(block_instance.blocks, index=0, limit=2, sort="asc")
    await print_api(block_instance.current_height)


async def candlestick_apis(client: lighter.ApiClient):
    logging.info("CANDLESTICK APIS")
    candlestick_instance = lighter.CandlestickApi(client)
    await print_api(
        candlestick_instance.candlesticks,
        market_id=0,
        resolution="1h",
        start_timestamp=int(datetime.datetime.now().timestamp() - 60 * 60 * 24),
        end_timestamp=int(datetime.datetime.now().timestamp()),
        count_back=2,
    )
    await print_api(
        candlestick_instance.fundings,
        market_id=0,
        resolution="1h",
        start_timestamp=int(datetime.datetime.now().timestamp() - 60 * 60 * 24),
        end_timestamp=int(datetime.datetime.now().timestamp()),
        count_back=2,
    )


async def order_apis(client: lighter.ApiClient):
    logging.info("ORDER APIS")
    order_instance = lighter.OrderApi(client)
    # await print_api(order_instance.exchange_stats)
    # await print_api(order_instance.order_book_details, market_id=0)
    # await print_api(order_instance.order_books)
    # await print_api(order_instance.recent_trades, market_id=0, limit=2)
    await print_api(order_instance.account_inactive_orders, account_index=ACCOUNT_INDEX, limit=20)


async def transaction_apis(client: lighter.ApiClient):
    logging.info("TRANSACTION APIS")
    transaction_instance = lighter.TransactionApi(client)
    await print_api(transaction_instance.block_txs, by="block_height", value="1")
    await print_api(
        transaction_instance.next_nonce,
        account_index=int(ACCOUNT_INDEX),
        api_key_index=0,
    )
    # use with a valid sequence index
    # await print_api(transaction_instance.tx, by="sequence_index", value="5")
    await print_api(transaction_instance.txs, index=0, limit=2)


async def main():
    client = lighter.ApiClient(configuration=lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
    # await account_apis(client)
    # await block_apis(client)
    # await candlestick_apis(client)
    await order_apis(client)
    # await transaction_apis(client)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())

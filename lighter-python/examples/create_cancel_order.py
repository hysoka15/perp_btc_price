import asyncio
import logging
import lighter
import os

logging.basicConfig(level=logging.DEBUG)

# 使用环境变量配置敏感信息 - 安全最佳实践
# 也可以访问 https://app.lighter.xyz/apikeys 获取主网API密钥
BASE_URL = os.getenv("BASE_URL", "https://mainnet.zklighter.elliot.ai")
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "2"))

if not API_KEY_PRIVATE_KEY:
    raise ValueError("❌ 请设置环境变量 API_KEY_PRIVATE_KEY")
if not ACCOUNT_INDEX:
    raise ValueError("❌ 请设置环境变量 ACCOUNT_INDEX")

async def print_api(method, *args, **kwargs):
    logging.info(f"{method.__name__}: {await method(*args, **kwargs)}")

def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


async def main():
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))

    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )

    err = client.check_client()
    if err is not None:
        print(f"CheckClient error: {trim_exception(err)}")
        return
    

    # create order
    tx, tx_hash, err = await client.create_order(
        market_index=1,
        client_order_index=0,
        base_amount=100,
        price=1149400,
        is_ask=False,
        order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
        time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
        reduce_only=0,
        trigger_price=0,
    )
    print(f"Create Order {tx=} {tx_hash=} {err=}")
    if err is not None:
        raise Exception(err)

    auth, err = client.create_auth_token_with_expiry(lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY)
    print(f"{auth=}")
    if err is not None:
        raise Exception(err)

    # cancel order
    # tx, tx_hash, err = await client.cancel_order(
    #     market_index=0,
    #     order_index=123,
    # )
    # print(f"Cancel Order {tx=} {tx_hash=} {err=}")
    # if err is not None:
    #     raise Exception(err)

    await client.close()
    await api_client.close()


if __name__ == "__main__":
    asyncio.run(main())

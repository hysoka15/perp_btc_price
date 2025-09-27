import asyncio
import logging
import lighter
import os

logging.basicConfig(level=logging.DEBUG)

# 使用环境变量配置敏感信息 - 安全最佳实践
BASE_URL = os.getenv("BASE_URL", "https://mainnet.zklighter.elliot.ai")
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "2"))

if not API_KEY_PRIVATE_KEY:
    raise ValueError("❌ 请设置环境变量 API_KEY_PRIVATE_KEY")
if not ACCOUNT_INDEX:
    raise ValueError("❌ 请设置环境变量 ACCOUNT_INDEX")



def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


async def main():
    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )

    tx = await client.create_market_order(
        market_index=1,
        client_order_index=0,
        base_amount=100,  # 0.1 ETH
        avg_execution_price=11464000,  # 170000 $1700
        is_ask=False,
    )
    print("Create Order Tx:", tx)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())

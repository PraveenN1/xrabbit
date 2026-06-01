import asyncio
from xrabbit import AsyncXRabbit, RabbitCredentials, ConnectionConfig

async def main():
    creds = RabbitCredentials(username="guest", password="guest")
    config = ConnectionConfig(host="localhost", port=5672)

    async with AsyncXRabbit(credentials=creds, config=config) as mq:
        await mq.publish(
            queue="async_test_queue", 
            message={"tx_id": 4004, "amount": 19.99}
        )
        print("🚀 Message published over async engine!")

if __name__ == "__main__":
    asyncio.run(main())
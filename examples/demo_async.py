import asyncio
from xrabbit import AsyncXRabbit, RabbitCredentials, ConnectionConfig

async def async_worker(msg):
    print(f"\n[Async Worker Started]: Processing payload: {msg}")
    await asyncio.sleep(1.5)
    print(f"[Async Worker Completed]: Successfully managed transaction {msg.get('tx_id')}")

async def main():
    creds = RabbitCredentials(username="guest", password="guest")
    config = ConnectionConfig(host="localhost", port=5672)

    async with AsyncXRabbit(credentials=creds, config=config) as mq:
        
        await mq.publish(
            queue="async_test_queue", 
            message={"tx_id": 4004, "amount": 19.99}
        )
        
        await mq.listen(queue="async_test_queue", callback=async_worker)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[-] Demo execution stopped.")
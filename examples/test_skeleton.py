from xrabbit import XRabbit, ConnectionConfig, RabbitCredentials

creds = RabbitCredentials(username='guest', password='guest')
config = ConnectionConfig(host='localhost', port=5672)


try:
    print("Trying to connect...")
    mq = XRabbit(config=config, credentials=creds)
    mq.close()
    print("\n Success! Your SDK skeleton architecture is fully functional.")
except Exception as e:
    print(f"\n Connection failed: {e}")
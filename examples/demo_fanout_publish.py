from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig, ExchangeConfig

creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)

mq = XRabbit(credentials=creds, config=config)

# 1. Define a Broadcast (Fanout) Exchange
broadcast_exchange = ExchangeConfig(name="marketing_events", type="fanout")
event_payload = {"alert": "Flash Sale! 50% off all items for the next hour!"}

print(broadcast_exchange.name)
# 2. Publish using clean keyword arguments (No queue parameter needed!)
mq.publish(exchange=broadcast_exchange, message=event_payload)

mq.close()
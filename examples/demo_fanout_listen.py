import sys
from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig, ExchangeConfig

if len(sys.argv) < 2:
    print("Usage: python examples/demo_fanout_listen.py [queue_name]")
    sys.exit(1)

target_queue = sys.argv[1]


def handle_broadcast(msg):
    print(f"\n[Received Broadcast on {target_queue}]: {msg['alert']}")


creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)

mq = XRabbit(credentials=creds, config=config)
broadcast_exchange = ExchangeConfig(name="marketing_events", type="fanout")

# Listen to our specific queue, but bind it to the marketing_events fanout exchange
mq.listen(queue=target_queue, callback=handle_broadcast, exchange=broadcast_exchange)

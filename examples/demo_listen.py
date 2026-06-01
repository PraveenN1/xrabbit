from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig

creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)


def process_incoming_order(order):
    print("\n--- New Message Received! ---")
    print(f"Processing Order Reference: #{order['order_id']}")
    print(f"Customer Name: {order['customer']}")
    print(f"Items Purchased: {', '.join(order['items'])}")
    print(f"Total Revenue: ${order['pricing']['total']:.2f}")
    print("-----------------------------\n")


mq = XRabbit(credentials=creds, config=config)

try:
    mq.listen(queue="customer_orders", callback=process_incoming_order)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    mq.close()

from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig

print("--- Testing Advanced Enterprise Messaging Features ---")

creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)

mq = XRabbit(credentials=creds, config=config)

# 1. Fire an urgent high-priority message
mq.publish(
    queue="priority_test_queue",
    message={"alert": "CRITICAL FIREWALL BREACH!"},
    priority=9
)
print("High priority message dispatched.")

# 2. Fire a message that self-destructs in 5 seconds
mq.publish(
    queue="ttl_test_queue",
    message={"token": "SECRET_OTP_5512"},
    expiration=5000  # 5000ms = 5 seconds
)
print("Volatile self-destructing message dispatched.")
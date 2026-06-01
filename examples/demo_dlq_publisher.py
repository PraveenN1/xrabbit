from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig

creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)

mq = XRabbit(credentials=creds, config=config)

mq.publish(queue="secure_email_servicev2", message={"status": "corrupted", "id": 999})
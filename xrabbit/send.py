import pika

credentials = pika.PlainCredentials("guest", "gue", erase_on_connect=True)
print("Credentials", credentials.__dict__)
parameters = pika.ConnectionParameters(
    host="localhost", port=5672, credentials=credentials
)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue="hello")

message = "Hello from Python!"
channel.basic_publish(exchange="", routing_key="hello", body=message)

print(f" [x] Sent '{message}'")

connection.close()

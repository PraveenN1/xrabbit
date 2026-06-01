from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig

creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)

print("--- Starting Context Manager Test ---")

with XRabbit(credentials=creds, config=config) as mq:
    mq.publish(queue="context_test_queue", message={"info": "Context managers rule!"})
    print("Inside the context block: Message sent successfully.")

print("--- Outside Context Scope: Verification complete ---")
from xrabbit import XRabbit, RabbitCredentials, ConnectionConfig

def processing_worker(msg):
    print(f"\n Processing incoming item: {msg}")
    
    # Simulate a sudden runtime code crash (e.g., bad database column data lookup)
    if msg.get("status") == "corrupted":
        raise ValueError("Database write failed! Data structure contains invalid parameters.")
        
    print("Successfully saved transaction data.")

creds = RabbitCredentials(username="guest", password="guest")
config = ConnectionConfig(host="localhost", port=5672)

mq = XRabbit(credentials=creds, config=config)

mq.listen(queue="secure_email_servicev2", callback=processing_worker, enable_dlq=True)
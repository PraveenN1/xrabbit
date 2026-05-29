from xrabbit import XRabbit,RabbitCredentials,ConnectionConfig

creds = RabbitCredentials(username='guest', password='guest')
config = ConnectionConfig(host='localhost', port=5672)

mq=XRabbit(credentials=creds,config=config)

order_payload={
    "order_id": 1234,
    "customer": "Praveen",
    "items": ["Custom Mechanical Keyword", "Coiled USB-C Cable"],
    "pricing":{
        "subtotal":12000.00,
        "tax":180.20,
        "total":12180.20
    },
    "express_shipping": True
}

mq.publish(queue="customer_orders",message=order_payload)
mq.close()
print("\n The structured message has safely hit RabbitMQ")
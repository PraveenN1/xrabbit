import json
import pika
from typing import Any

class XRabbitProducer:
    def __init__(self,channel:pika.adapters.blocking_connection.BlockingChannel):
        self._channel=channel
        
    def publish(self,queue:str,message:Any):
        """
        Intercepts the message payload and safely transmits it. 
        Automatically handles:
        1. Queue Declaration (Ensures the target queue exists)
        2. Serialization (Python dictionaries/lists -> JSON text strings)
        3. Persistence (Flags the message to be saved to disk against crashes)
        """
        
        self._channel.queue_declare(queue=queue, durable=True)
        
        if isinstance(message,(dict,list)):
            body = json.dumps(message)
            content_type = 'application/json'
        else:
            body = str(message)
            content_type = 'text/plain'
            
        self._channel.basic_publish(
            exchange='',             
            routing_key=queue,        
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type=content_type
            )
        )
        print(f"[+] XRabbitProducer sent message to queue '{queue}'")
        
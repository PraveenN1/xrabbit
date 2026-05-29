import json
import pika
from typing import Callable, Any

class XRabbitConsumer:
    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel):
        """The consumer uses the active connection channel from the client."""
        self._channel = channel

    def consume(self, queue: str, callback: Callable[[Any], None]):
        """
        Starts a blocking consumption loop on the given queue.
        - Automatically declares the queue (in case it doesn't exist yet).
        - Automatically deserializes incoming JSON payloads back into Python objects.
        - Gracefully manages worker safety via manual message acknowledgments.
        """

        self._channel.queue_declare(queue=queue, durable=True)

        self._channel.basic_qos(prefetch_count=1)

        def internal_callback(ch, method, properties, body):
          
            decoded_body = body.decode('utf-8')
            data = decoded_body

            if properties.content_type == 'application/json':
                try:
                    data = json.loads(decoded_body)
                except json.JSONDecodeError:
                    print("[!] Failed to decode incoming message payload as JSON.")

            try:
                callback(data)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"[XRabbit Worker Error]: Exception raised in your callback: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        self._channel.basic_consume(queue=queue, on_message_callback=internal_callback)
        
        print(f" [*] XRabbit watching queue '{queue}'. Press CTRL+C to exit.")
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            print("\n [-] Worker stopped via keyboard interrupt.")
            self._channel.stop_consuming()
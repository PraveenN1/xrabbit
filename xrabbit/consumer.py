import json
import pika
from typing import Callable, Any, Optional
from .configs import ExchangeConfig


class XRabbitConsumer:
    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel):
        """The consumer uses the active connection channel from the client."""
        self._channel = channel

    def listen(
        self,
        queue: str,
        callback: Callable[[Any], None],
        exchange: Optional[ExchangeConfig],
        routing_key: str = "",
    ):
        """
        Starts listening on a queue. If an exchange configuration is passed,
        it automatically sets up the binding rules.
        """

        self._channel.queue_declare(queue=queue, durable=True)

        if exchange:
            self._channel.exchange_declare(
                exchange=exchange.name,
                exchange_type=exchange.type,
                durable=exchange.durable,
            )
            self._channel.queue_bind(
                queue=queue, exchange=exchange.name, routing_key=routing_key
            )

        self._channel.basic_qos(prefetch_count=1)

        def internal_callback(ch, method, properties, body):

            decoded_body = body.decode("utf-8")
            data = decoded_body

            if properties.content_type == "application/json":
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

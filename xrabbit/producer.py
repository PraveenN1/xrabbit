import json
import pika
from typing import Any, Optional
from .configs import ExchangeConfig


class XRabbitProducer:
    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel):
        self._channel = channel

    def publish(
        self,
        queue: Optional[str] = None,
        message: Any = None,
        exchange: Optional[ExchangeConfig] = None,
        routing_key: str = "",
    ):
        """
        Publishes a message. Supports both simple queue direct targeting
        and advanced Exchange-based routing
        """

        if isinstance(message, (dict, list)):
            body = json.dumps(message)
            content_type = "application/json"
        else:
            body = str(message)
            content_type = "text/plain"

        exchange_name = ""
        target_routing_key = routing_key


        if exchange_name:
            exchange_name=exchange_name
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange.type,
                durable=exchange.durable,
            )
        elif queue:
            self._channel.queue_declare(queue=queue, durable=True)
            target_routing_key = queue

        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=target_routing_key,
            body=body.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2, content_type=content_type),
        )
        dest = (
            f"exchange '{exchange_name}' with key '{target_routing_key}'"
            if exchange
            else f"queue '{queue}'"
        )
        print(f"[+] XRabbitProducer routed message to {dest}")

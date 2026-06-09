import asyncio
import inspect
import json
import logging
from typing import Any, Callable, Optional
import aio_pika
from .configs import ConnectionConfig, RabbitCredentials, ExchangeConfig

logger = logging.getLogger("xrabbit.async")


class AsyncXRabbitProducer:
    def __init__(self, channel: aio_pika.abc.AbstractChannel):
        self._channel = channel

    async def publish(
        self,
        message: Any,
        *,
        queue: Optional[str] = None,
        exchange: Optional[ExchangeConfig] = None,
        routing_key: str = "",
        expiration: Optional[int] = None,
        priority: Optional[int] = None,
    ):
        """Asynchronously serializes and publishes messages over non-blocking sockets."""
        if isinstance(message, (dict, list)):
            body = json.dumps(message)
            content_type = "application/json"
        else:
            body = str(message)
            content_type = "text/plain"

        message_kwargs = {
            "body": body.encode("utf-8"),
            "delivery_mode": aio_pika.DeliveryMode.PERSISTENT,
            "content_type": content_type,
        }

        if expiration is not None:
            message_kwargs["expiration"] = (
                expiration / 1000.0
            )  # aio-pika accepts float seconds or timedelta
        if priority is not None:
            message_kwargs["priority"] = priority

        target_routing_key = routing_key

        if exchange:
            exchange_name = exchange.name
            exchange_obj = await self._channel.declare_exchange(
                name=exchange_name,
                type=exchange.type,
                durable=exchange.durable,
            )
        elif queue:
            exchange_name = ""
            exchange_obj = self._channel.default_exchange
            target_routing_key = queue

        else:
            raise ValueError(
                "You must supply either an exchange or a queue parameter to publish."
            )

        await exchange_obj.publish(
            aio_pika.Message(**message_kwargs),
            routing_key=target_routing_key,
        )
        dest = f"exchange '{exchange_name}'" if exchange else f"queue '{queue}'"
        print(f"[+] AsyncXRabbitProducer dispatched message to {dest}")


class AsyncXRabbitConsumer:
    def __init__(self, channel: aio_pika.abc.AbstractChannel):
        self._channel = channel

    async def listen(
        self,
        queue: str,
        callback: Callable[[Any], Any],
        *,
        exchange: Optional[ExchangeConfig] = None,
        routing_key: str = "",
        enable_dlq: bool = False,
    ):
        """Asynchronously registers a non-blocking message consumption stream."""
        queue_arguments = {}

        if enable_dlq:
            dlx_name = f"{queue}.dlx"
            dlq_name = f"{queue}.dlq"

            dlx = await self._channel.declare_exchange(
                name=dlx_name, type="direct", durable=True
            )
            dlq = await self._channel.declare_queue(name=dlq_name, durable=True)
            await dlq.bind(exchange=dlx, routing_key=queue)

            queue_arguments = {
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": queue,
            }

        primary_queue = await self._channel.declare_queue(
            name=queue, durable=True, arguments=queue_arguments
        )

        if exchange:
            exchange_obj = await self._channel.declare_exchange(
                name=exchange.name, type=exchange.type, durable=exchange.durable
            )
            await primary_queue.bind(exchange=exchange_obj, routing_key=routing_key)

        await self._channel.set_qos(prefetch_count=1)

        print(f" [*] AsyncXRabbit watching queue '{queue}'... Press CTRL+C to halt.")

        async with primary_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process(requeue=False):

                    decoded_body = message.body.decode("utf-8")
                    data = decoded_body

                    if message.content_type == "application/json":
                        try:
                            data = json.loads(decoded_body)
                        except json.JSONDecodeError:
                            pass

                    try:
                        if inspect.iscoroutinefunction(callback):
                            await callback(data)
                        else:
                            callback(data)
                    except Exception as e:
                        print(f"[AsyncXRabbit Worker Error]: {e}")
                        raise


class AsyncXRabbit:
    def __init__(
        self,
        config: Optional[ConnectionConfig] = None,
        credentials: Optional[RabbitCredentials] = None,
    ):
        self.config = config or ConnectionConfig()
        self.credentials = credentials or RabbitCredentials()

        self._connection: Optional[aio_pika.abc.AbstractConnection] = None
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self.producer: Optional[AsyncXRabbitProducer] = None
        self.consumer: Optional[AsyncXRabbitConsumer] = None

    async def connect(self):
        """Asynchronously initializes underlying TCP connection loops."""
        url = (
            f"amqp://{self.credentials.username}:{self.credentials.password}@"
            f"{self.config.host}:{self.config.port}/{self.config.virtual_host or ''}"
        )

        print(
            f"[*] AsyncXRabbit establishing connection loop to {self.config.host}:{self.config.port}..."
        )
        self._connection = await aio_pika.connect_robust(url)
        self._channel = await self._connection.channel()

        self.producer = AsyncXRabbitProducer(self._channel)
        self.consumer = AsyncXRabbitConsumer(self._channel)
        print("[*] AsyncXRabbit successfully connected and context channel opened.")
        return self

    async def publish(
        self,
        message: Any,
        *,
        queue: str = None,
        exchange: ExchangeConfig = None,
        routing_key: str = "",
    ):
        if not self.producer:
            raise RuntimeError("AsyncXRabbit is not connected.")
        await self.producer.publish(
            message=message, queue=queue, exchange=exchange, routing_key=routing_key
        )

    async def listen(
        self,
        queue: str,
        callback: Callable[[Any], Any],
        *,
        exchange: ExchangeConfig = None,
        routing_key: str = "",
        enable_dlq: bool = False,
    ):
        if not self.consumer:
            raise RuntimeError("AsyncXRabbit is not connected.")
        await self.consumer.listen(
            queue,
            callback,
            exchange=exchange,
            routing_key=routing_key,
            enable_dlq=enable_dlq,
        )

    async def close(self):
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            print("[-] AsyncXRabbit connection loop cleanly terminated.")

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

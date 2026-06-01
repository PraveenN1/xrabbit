import pika
import sys
import time
from typing import Any, Callable, Optional
from .configs import RabbitCredentials, ConnectionConfig, ExchangeConfig
from .producer import XRabbitProducer
from .consumer import XRabbitConsumer


class XRabbit:
    def __init__(
        self, config: ConnectionConfig = None, credentials: RabbitCredentials = None
    ):
        self.config = config or ConnectionConfig()
        self.credentials = credentials or RabbitCredentials()

        self._connection = None
        self._channel = None
        self.producer = None
        self.consumer = None

        self.connect()

    def connect(self):
        """Maps custom configuration dataclasses into concrete Pika structures."""
        print(
            f"[*] XRabbit establishing connection to {self.config.host}:{self.config.port}..."
        )

        pika_creds = pika.PlainCredentials(
            username=self.credentials.username,
            password=self.credentials.password,
            erase_on_connect=self.credentials.erase_on_connect,
        )

        pika_params = pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            virtual_host=self.config.virtual_host,
            heartbeat=self.config.heartbeat,
            credentials=pika_creds,
        )

        while True:
            try:
                print(
                    f"[*] XRabbit establishing connection to {self.config.host}:{self.config.port}..."
                )
                self._connection = pika.BlockingConnection(pika_params)
                self._channel = self._connection.channel()

                self.producer = XRabbitProducer(self._channel)
                self.consumer = XRabbitConsumer(self._channel)

                print("[*] XRabbit successfully connected and channel opened.")
                break

            except pika.exceptions.ProbableAuthenticationError:
                print("\n[XRabbit Connection Error]: Authentication Failed!")
                print(
                    f"Reason: The username '{self.credentials.username}' or '{self.credentials.password}' is incorrect."
                )
                print(
                    f"Fix: Verify the credentials match according your system variables"
                )
                sys.exit(1)

            except pika.exceptions.AMQPConnectionError:
                print("\n[XRabbit Connection Error]: Could not reach RabbitMQ!")
                print(
                    f"Reason: Cannot connect to host '{self.config.host}' on port {self.config.port}."
                )
                print(
                    "Fix: Ensure your RabbitMQ Docker container is running (`docker ps`) and port 5672 is correctly exposed."
                )
                time.sleep(5)
                continue

            except Exception as unknown_error:
                print(
                    f"\n[XRabbit Connection Error]: Unexpected error occurred: {unknown_error}"
                )
                sys.exit(1)

    def publish(
        self,
        message: Any,
        queue: Optional[str] = None,
        exchange: ExchangeConfig = None,
        routing_key: str = "",
    ):
        try:
            if not self.producer:
                raise RuntimeError("XRabbit is not connected.")
            self.producer.publish(
                queue=queue, message=message, exchange=exchange, routing_key=routing_key
            )
        except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed):
            print(
                "\n[XRabbit Runtime Alert]: Connection lost during publish! Attempting healing..."
            )
            self.connect()
            self.producer.publish(queue=queue, message=message, exchange=exchange, routing_key=routing_key)

    def listen(
        self,
        queue: str,
        callback: Callable[[Any], None],
        exchange: ExchangeConfig = None,
        routing_key: str = "",
    ):
        if not self.consumer:
            raise RuntimeError("XRabbit is not connected.")
        self.consume_queue = queue
        self.consume_callback = callback
        while True:
            try:
                self.consumer.listen(
                    queue, callback, exchange=exchange, routing_key=routing_key
                )
            except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed):
                print(
                    "\n[XRabbit Runtime Alert]: Active consumer link broken! Attempting to heal stream..."
                )
                self.connect()
                print("[+] Re-established socket. Resuming consumption stream...")
                continue

    def close(self):
        """Gracefully tears down network sockets to protect against hanging connections."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            print("[-] XRabbit connection safely terminated.")

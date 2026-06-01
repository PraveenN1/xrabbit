from dataclasses import dataclass
from typing import Optional

@dataclass
class RabbitCredentials:
    username: str = "username"
    password: str = "password"
    erase_on_connect: bool = False


@dataclass
class ConnectionConfig:
    host: str = "localhost"
    port: str = "5672"
    virtual_host: str = "/"
    heartbeat: int = 60


@dataclass
class ExchangeConfig:
    name: str
    type: str = "direct"  # Options: direct, fanout, topic
    durable: bool = True


@dataclass
class QueueConfig:
    name: str
    durable: bool = True
    dead_letter_exchange: Optional[str] = None
    dead_letter_routing_key: Optional[str] = None

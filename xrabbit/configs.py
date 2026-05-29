from dataclasses import dataclass

@dataclass
class RabbitCredentials:
    username: str = 'username'
    password: str = 'password'
    erase_on_connect: bool = False

@dataclass
class ConnectionConfig:
    host: str = 'localhost'
    port: str = '5672'
    virtual_host: str = '/'
    heartbeat: int = 60
# xrabbit

A production-grade, zero-boilerplate Python SDK wrapper around `pika` and `aio-pika` designed to make working with RabbitMQ clean, safe, and intuitive for both synchronous applications and asynchronous event loops.

`xrabbit` abstracts away low-level network loops, manual byte encoding/decoding, and messy error handling into a dead-simple, developer-friendly API.

## ✨ Key Features

* **Dual-Engine Architecture:** Native support for standard blocking architectures (`XRabbit`) and high-concurrency non-blocking loops (`AsyncXRabbit`).
* **Zero-Boilerplate Execution:** Reduce 20+ lines of standard setup to single-line operations.
* **Automatic Serialization:** Pass native Python dictionaries or lists directly; the SDK handles JSON serialization and string-to-binary wire conversion transparently.
* **Resource Safety (Context Managers):** Native `with` and `async with` block scopes guarantee TCP connections are cleanly reaped, entirely preventing broker connection leaks.
* **Automated Dead-Lettering (DLQ):** Activate enterprise-grade error isolation with a single toggle (`enable_dlq=True`) to quarantine poison-pill messages automatically without crashing your workers.
* **Built-in Self-Healing:** Seamlessly intercepts broken sockets or dropped brokers, triggering background retry loops instead of crashing your runtime context.

---

## ⚙️ Installation

To install `xrabbit` locally in editable mode for your development project:

```bash
pip install -e .

```

---

## 🚀 Quickstart Guide

### 1. Synchronous Paradigm (Classic Backend / Worker Scripts)

Throw raw Python data structures straight at your queue, and manage resource lifecycles elegantly using a standard Python context manager.

#### Publisher (`producer.py`)

```python
from xrabbit import XRabbit, ConnectionConfig, RabbitCredentials

# Automatically handles connection initialization and teardown!
with XRabbit() as mq:
    order_payload = {
        "order_id": 9941,
        "customer": "Praveen",
        "total": 130.20
    }
    mq.publish(queue="customer_orders", message=order_payload)
    print("🚀 Message routed successfully!")

```

#### Consumer (`worker.py`)

```python
from xrabbit import XRabbit

def process_order(order: dict):
    print(f"📦 Processing Order Reference: #{order['order_id']}")

with XRabbit() as mq:
    # Starts a persistent blocking listener loop
    mq.listen(queue="customer_orders", callback=process_order)

```

---

### 2. Asynchronous Paradigm (FastAPI / Concurrency Loops)

If you are building high-speed asynchronous APIs, use `AsyncXRabbit` to run entirely non-blocking messaging streams driven by `asyncio`.

#### Async Publisher

```python
import asyncio
from xrabbit import AsyncXRabbit

async def main():
    async with AsyncXRabbit() as mq:
        await mq.publish(queue="async_orders", message={"status": "processing"})
        print("⚡ Async message dispatched!")

asyncio.run(main())

```

#### Async Consumer

```python
import asyncio
from xrabbit import AsyncXRabbit

async def async_worker(msg: dict):
    print(f"⚡ [Async Worker]: Handling payload -> {msg}")
    await asyncio.sleep(1) # Yields control to the event loop non-blockingly

async def main():
    async with AsyncXRabbit() as mq:
        await mq.listen(queue="async_orders", callback=async_worker)

if __name__ == "__main__":
    asyncio.run(main())

```

---

## 🛡️ Enterprise Safety Valves

### 1. Dead-Letter Queue (DLQ) Protection

Prevent unhandled business logic exceptions from catching your workers in an infinite crashing retry loop. Turning on `enable_dlq=True` instructs the SDK to dynamically build an isolated holding pen infrastructure directly on the broker.

```python
from xrabbit import XRabbit

def fragile_callback(msg):
    if msg.get("status") == "corrupted":
        raise ValueError("Database write failed! Quarantining packet...")

with XRabbit() as mq:
    # Automatically provisions 'email_queue.dlx' and 'email_queue.dlq' on the broker
    mq.listen(queue="email_queue", callback=fragile_callback, enable_dlq=True)

```

If a message crashes your callback, `xrabbit` intercepts the exception, rejects the payload with `requeue=False`, and routes it directly to the background DLQ for inspection while keeping your worker online.

### 2. Resilience & Auto-Healing

If your RabbitMQ container restarts or blinks during runtime execution, `xrabbit` suppresses ugly network stack traces and heals itself in the background:

```text
🚨 [XRabbit Runtime Alert]: Active consumer link broken! Attempting to heal stream...
[*] XRabbit establishing connection to localhost:5672...
⚠️ [XRabbit Network Alert]: Could not reach broker. Retrying in 5 seconds...
[*] XRabbit establishing connection to localhost:5672...
[+] XRabbit successfully connected and channel opened.
[+] Re-established socket. Resuming consumption stream...

```

---

## 📁 Architecture Overview

```text
xrabbit/
├── pyproject.toml           # Package metadata and dependencies
├── xrabbit/                 # Core Source Package
│   ├── __init__.py          # Master API entry points & exporters
│   ├── configs.py           # Validated configuration & data dataclasses
│   ├── client.py            # Synchronous Engine Coordinator
│   ├── async_client.py      # Asynchronousio Engine Coordinator (aio-pika)
│   ├── producer.py          # Synchronous data output layer
│   └── consumer.py          # Synchronous worker stream runtime
└── examples/                # Quickstart verification scripts

```

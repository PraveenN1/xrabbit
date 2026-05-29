```markdown
# xrabbit 🐇

A production-grade, zero-boilerplate Python SDK wrapper around `pika` designed to make working with RabbitMQ clean, safe, and intuitive.

`xrabbit` abstracts away low-level network loops, manual byte encoding/decoding, and messy error handling into a dead-simple, developer-friendly API.

## ✨ Key Features

* **Zero-Boilerplate Execution:** Reduce 20+ lines of standard Pika setup to single-line operations.
* **Automatic Serialization:** Pass native Python dictionaries or lists directly; the SDK handles JSON serialization and string-to-binary wire conversion transparently.
* **Built-in Self-Healing:** Seamlessly intercepts broken sockets or dropped brokers. Automatically triggers background back-off retry loops instead of crashing your workers.
* **Type-Safe Configurations:** Uses standard Python dataclasses for full IDE autocomplete and robust connection configurations.
* **Production-Safe Routing:** Automatically declares queues as `durable` and flags messages as `persistent` to safeguard your data against system restarts.

---

## ⚙️ Installation

To install `xrabbit` locally in editable mode for your development project:

```bash
pip install -e .

```

---

## 🚀 Quickstart Guide

### 1. The Core Client Setup

By default, initializing `EZRabbit` automatically maps to your local Docker container on `localhost:5672` with standard `guest`/`guest` credentials.

```python
from xrabbit import EZRabbit, ConnectionConfig, RabbitCredentials

# Uses defaults automatically
mq = EZRabbit()

# Or customize it cleanly using type-safe dataclasses
# config = ConnectionConfig(host="192.168.1.50", port=5672)
# mq = EZRabbit(config=config)

```

### 2. Publishing Messages (The Producer)

Throw raw Python data structures straight at your queue. No manual `.encode('utf-8')` or `json.dumps()` required.

```python
from xrabbit import EZRabbit

mq = EZRabbit()

order_payload = {
    "order_id": 9941,
    "customer": "Praveen",
    "items": ["Custom Mechanical Keyboard", "USB-C Cable"],
    "total": 130.20
}

# Automatically creates the queue and serializes data to JSON bytes
mq.publish(queue="customer_orders", message=order_payload)
mq.close()

```

### 3. Listening for Data (The Consumer)

When a message arrives, `xrabbit` handles the thread prefetch choking, converts JSON strings back into native dicts, runs your function, and manages safe acknowledgments.

```python
from xrabbit import EZRabbit

def process_order(order: dict):
    print(f"📦 Processing Order Reference: #{order['order_id']}")
    print(f"👤 Customer: {order['customer']}")

mq = EZRabbit()

# Starts a persistent blocking listener loop
mq.listen(queue="customer_orders", callback=process_order)

```

---

## 🛡️ Resilience & Auto-Healing In Action

If your RabbitMQ container restarts or blinks during a runtime execution loop, `xrabbit` catches the exception, suppresses ugly stack traces, and handles recovery automatically:

```text
🚨 [XRabbit Runtime Alert]: Active consumer link broken! Attempting to heal stream...
[*] XRabbit establishing connection to localhost:5672...
⚠️ [XRabbit Network Alert]: Could not reach broker. Retrying in 5 seconds...
[*] XRabbit establishing connection to localhost:5672...
[+] XRabbit successfully connected and channel opened.
[+] Re-established socket. Resuming consumption stream...
[*] XRabbit watching queue 'customer_orders'. Press CTRL+C to exit.

```

---

## 📁 Architecture Overview

```text
xrabbit/
├── pyproject.toml           # Package metadata and dependencies
├── xrabbit/                 # Core Source Package
│   ├── configs.py           # Validated data layer
│   ├── client.py            # Central Engine Coordinator
│   ├── producer.py          # Data out transformation layer
│   └── consumer.py          # Data in transformation layer
└── examples/                # Quickstart verification scripts

```

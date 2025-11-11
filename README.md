# E-commerce Sales Generator

A Python script that generates realistic e-commerce sales data with order lifecycle management. Orders progress through realistic status transitions (pending → confirmed → processing → shipped) and can be output to console or Kafka.

## Table of Contents

- [E-commerce Sales Generator](#e-commerce-sales-generator)
  - [Table of Contents](#table-of-contents)
  - [Requirements](#requirements)
  - [Installation](#installation)
  - [Kafka Installation](#kafka-installation)
    - [Native Kafka Setup (No Docker)](#native-kafka-setup-no-docker)
  - [Sales Generator Usage](#sales-generator-usage)
    - [Basic Usage](#basic-usage)
    - [Command Options](#command-options)
  - [Quick Start](#quick-start)
    - [Example Output](#example-output)
  - [Additional Documentation](#additional-documentation)
  - [Stopping the Generator](#stopping-the-generator)

## Requirements

- Python 3.6+
- Faker library
- kafka-python library

## Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

Or install individually:

```bash
pip install faker kafka-python
```

## Kafka Installation

### Native Kafka Setup (No Docker)

Install and start Kafka natively:

```bash
# Install Kafka (downloads Apache Kafka 3.6.1)
python kafka_native_manager.py install

# Start Kafka and Zookeeper
python kafka_native_manager.py start

# Check status
python kafka_native_manager.py status

# Create a topic
python kafka_native_manager.py create-topic ecommerce-sales --partitions 3

# List topics
python kafka_native_manager.py list-topics

# Consume from topic
python kafka_native_manager.py consume ecommerce-sales

# Stop Kafka
python kafka_native_manager.py stop
```

The script downloads Kafka to `~/.kafka-local/` and runs:

- Zookeeper on port 2181
- Kafka on port 9092

## Sales Generator Usage

### Basic Usage

```bash
# Console output (default)
python sales_generator.py

# Send to Kafka
python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales

# Kafka + console output
python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales --output-console

# Custom timing
python sales_generator.py --duration 10 --min-delay 2 --max-delay 30
```

### Command Options

- `--duration MINUTES`: How long to generate data (default: 5)
- `--min-delay SECONDS`: Minimum delay between orders (default: 1)
- `--max-delay SECONDS`: Maximum delay between orders (default: 60)
- `--kafka-bootstrap-servers HOST:PORT`: Kafka broker(s)
- `--kafka-topic TOPIC`: Kafka topic name
- `--output-console`: Print to console when using Kafka

## Quick Start

Complete workflow from installation to data generation:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Install Kafka
python kafka_native_manager.py install

# 3. Start Kafka
python kafka_native_manager.py start

# 4. Create topic
python kafka_native_manager.py create-topic ecommerce-sales --partitions 3

# 5. Generate sales data (in one terminal)
python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales --duration 5

# 6. Consume events (in another terminal)
python kafka_native_manager.py consume ecommerce-sales

# 7. Stop Kafka when done
python kafka_native_manager.py stop
```

### Example Output

Each order generates multiple JSON events as it progresses through statuses:

```json
{
  "order_id": "ORD-93239605",
  "timestamp": "2025-11-09T15:55:40.669190Z",
  "product_name": "USB-C Cable",
  "quantity": 3,
  "price": 11.16,
  "total": 33.48,
  "customer_email": "campbellmichael@example.org",
  "payment_method": "bank_transfer",
  "status": "pending"
}
```

The same order will generate subsequent events with status updates: `confirmed`, `processing`, and `shipped`.

## Additional Documentation

For detailed technical information, see [TECHNICAL.md](TECHNICAL.md):

- Order lifecycle behavior and timing
- Complete data structure and schema
- Product catalog (15 products)
- Kafka configuration details
- Integration examples (Spark, Python)
- Statistics and monitoring
- Troubleshooting guide

## Stopping the Generator

- Runs automatically for specified duration
- Press `Ctrl+C` to stop manually
- Shows statistics summary on exit

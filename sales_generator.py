#!/usr/bin/env python3
"""
E-commerce Sales Generator
Generates random e-commerce sales data with realistic delays to mimic real-world traffic.
Orders follow a realistic lifecycle: pending → confirmed → processing → shipped
Supports output to console and/or Kafka topic
"""

import json
import random
import time
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional
from faker import Faker


# Product catalog
PRODUCTS = [
    {"id": "PROD-001", "name": "Wireless Bluetooth Headphones", "price_range": (29.99, 199.99)},
    {"id": "PROD-002", "name": "Smart Watch", "price_range": (99.99, 499.99)},
    {"id": "PROD-003", "name": "Laptop Stand", "price_range": (19.99, 89.99)},
    {"id": "PROD-004", "name": "USB-C Cable", "price_range": (9.99, 29.99)},
    {"id": "PROD-005", "name": "Mechanical Keyboard", "price_range": (59.99, 299.99)},
    {"id": "PROD-006", "name": "Wireless Mouse", "price_range": (19.99, 129.99)},
    {"id": "PROD-007", "name": "Phone Case", "price_range": (14.99, 49.99)},
    {"id": "PROD-008", "name": "Portable Charger", "price_range": (24.99, 79.99)},
    {"id": "PROD-009", "name": "LED Desk Lamp", "price_range": (29.99, 89.99)},
    {"id": "PROD-010", "name": "Webcam HD", "price_range": (39.99, 199.99)},
    {"id": "PROD-011", "name": "External Hard Drive", "price_range": (49.99, 199.99)},
    {"id": "PROD-012", "name": "Monitor 27 inch", "price_range": (199.99, 699.99)},
    {"id": "PROD-013", "name": "Gaming Chair", "price_range": (149.99, 499.99)},
    {"id": "PROD-014", "name": "Desk Organizer", "price_range": (12.99, 39.99)},
    {"id": "PROD-015", "name": "Bluetooth Speaker", "price_range": (29.99, 249.99)},
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]

# Order lifecycle: pending → confirmed → processing → shipped
# Orders can also be cancelled at any stage before shipping
ORDER_STATUS_FLOW = ["pending", "confirmed", "processing", "shipped"]

# Time ranges (in seconds) for status transitions
STATUS_TRANSITION_TIMES = {
    "pending": (10, 30),      # pending → confirmed: 10-30 seconds
    "confirmed": (15, 45),     # confirmed → processing: 15-45 seconds
    "processing": (20, 60),    # processing → shipped: 20-60 seconds
}

# Probability of order cancellation at each status check (before shipping)
# This simulates payment failures, inventory issues, customer cancellations, etc.
CANCELLATION_PROBABILITY = 0.08  # 8% chance of cancellation

# Cancellation reasons for different stages
CANCELLATION_REASONS = {
    "pending": ["payment_failed", "payment_declined", "customer_cancelled", "fraud_suspected"],
    "confirmed": ["inventory_unavailable", "customer_cancelled", "payment_verification_failed", "address_invalid"],
    "processing": ["customer_cancelled", "inventory_damaged", "shipping_address_unreachable", "customer_requested_cancellation"]
}

# Initialize Faker
fake = Faker()


def generate_order_id() -> str:
    """Generate a unique order ID."""
    return f"ORD-{random.randint(10000000, 99999999)}"


def generate_customer_id() -> str:
    """Generate a customer ID."""
    return f"CUST-{random.randint(100000, 999999)}"


def generate_address() -> Dict[str, str]:
    """Generate a realistic shipping address using Faker."""
    return {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": fake.zipcode(),
        "country": fake.country_code(representation="alpha-3")
    }


def generate_sale() -> Dict[str, Any]:
    """Generate a single random sale transaction. All new orders start with 'pending' status."""
    # Select a random product
    product = random.choice(PRODUCTS)
    
    # Generate sale details
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(product["price_range"][0], product["price_range"][1]), 2)
    total = round(quantity * unit_price, 2)
    
    # Create the sale object - always starts with "pending" status
    sale = {
        "order_id": generate_order_id(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "price": unit_price,
        "total": total,
        "customer_id": generate_customer_id(),
        "customer_email": fake.email(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_address": generate_address(),
        "status": "pending"
    }
    
    return sale


def get_next_status(current_status: str) -> str:
    """Get the next status in the order lifecycle."""
    current_index = ORDER_STATUS_FLOW.index(current_status)
    if current_index < len(ORDER_STATUS_FLOW) - 1:
        return ORDER_STATUS_FLOW[current_index + 1]
    return current_status  # Already at final status


def should_cancel_order(status: str) -> bool:
    """
    Randomly determine if an order should be cancelled at the current status.
    Orders can only be cancelled before shipping.
    """
    if status in ["shipped", "cancelled"]:
        return False
    
    return random.random() < CANCELLATION_PROBABILITY


def cancel_order(order: Dict[str, Any], current_time: float) -> Dict[str, Any]:
    """Cancel an order with an appropriate reason based on current status."""
    status = order["status"]
    
    # Select a random cancellation reason appropriate for the current status
    if status in CANCELLATION_REASONS:
        reason = random.choice(CANCELLATION_REASONS[status])
    else:
        reason = "order_cancelled"
    
    order["status"] = "cancelled"
    order["cancellation_reason"] = reason
    order["timestamp"] = datetime.utcnow().isoformat() + "Z"
    order["last_status_change"] = current_time
    
    return order


def should_advance_order(order: Dict[str, Any], current_time: float) -> bool:
    """Check if an order should advance to the next status based on elapsed time."""
    status = order["status"]
    
    # If already shipped or cancelled, don't advance
    if status in ["shipped", "cancelled"]:
        return False
    
    # Check if enough time has passed since last status change
    time_in_status = current_time - order["last_status_change"]
    min_time, max_time = STATUS_TRANSITION_TIMES[status]
    
    # Orders advance after their transition time has elapsed
    return time_in_status >= order["transition_time"]


def advance_order_status(order: Dict[str, Any], current_time: float) -> Dict[str, Any]:
    """
    Advance an order to the next status.
    Before advancing, there's a chance the order might be cancelled instead.
    """
    old_status = order["status"]
    
    # Check if order should be cancelled instead of advancing
    if should_cancel_order(old_status):
        return cancel_order(order, current_time)
    
    # Otherwise, advance normally
    new_status = get_next_status(old_status)
    
    order["status"] = new_status
    order["timestamp"] = datetime.utcnow().isoformat() + "Z"
    order["last_status_change"] = current_time
    
    # Set the next transition time if not at final status
    if new_status != "shipped":
        min_time, max_time = STATUS_TRANSITION_TIMES[new_status]
        order["transition_time"] = random.uniform(min_time, max_time)
    
    return order


def create_kafka_producer(bootstrap_servers: str):
    """
    Create and return a Kafka producer instance.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka broker addresses
        
    Returns:
        KafkaProducer instance or None if kafka-python is not installed
    """
    try:
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        return producer
    except ImportError:
        print("ERROR: kafka-python not installed. Install with: pip install kafka-python")
        return None
    except Exception as e:
        print(f"ERROR: Failed to create Kafka producer: {e}")
        return None


def send_to_kafka(producer, topic: str, order_id: str, data: Dict[str, Any]) -> bool:
    """
    Send data to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic: Kafka topic name
        order_id: Order ID to use as message key
        data: Data to send
        
    Returns:
        True if successful, False otherwise
    """
    try:
        future = producer.send(topic, key=order_id, value=data)
        future.get(timeout=10)  # Wait for acknowledgment
        return True
    except Exception as e:
        print(f"ERROR sending to Kafka: {e}")
        return False


def run_generator(duration_minutes: int = 5, min_delay: int = 1, max_delay: int = 60,
                  kafka_bootstrap_servers: Optional[str] = None, kafka_topic: Optional[str] = None,
                  output_console: bool = True):
    """
    Run the sales generator for a specified duration.
    Orders follow a realistic lifecycle: pending → confirmed → processing → shipped
    
    Args:
        duration_minutes: How long to run the generator (in minutes)
        min_delay: Minimum delay between new orders (in seconds)
        max_delay: Maximum delay between new orders (in seconds)
        kafka_bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        kafka_topic: Kafka topic name to send events to
        output_console: Whether to print to console (default: True)
    """
    # Initialize Kafka producer if configured
    kafka_producer = None
    if kafka_bootstrap_servers and kafka_topic:
        print(f"Initializing Kafka producer...")
        kafka_producer = create_kafka_producer(kafka_bootstrap_servers)
        if kafka_producer is None:
            print("WARNING: Kafka producer failed to initialize. Continuing with console output only.")
    
    print(f"Starting e-commerce sales generator with order lifecycle...")
    print(f"Duration: {duration_minutes} minutes")
    print(f"New order delay range: {min_delay}-{max_delay} seconds")
    print(f"Order lifecycle: pending → confirmed → processing → shipped")
    
    if kafka_producer:
        print(f"Kafka output: ENABLED")
        print(f"  Bootstrap servers: {kafka_bootstrap_servers}")
        print(f"  Topic: {kafka_topic}")
    else:
        print(f"Kafka output: DISABLED")
    
    print(f"Console output: {'ENABLED' if output_console else 'DISABLED'}")
    print(f"=" * 80)
    print()
    
    start_time = time.time()
    duration_seconds = duration_minutes * 60
    
    # Track all active orders (not yet shipped)
    active_orders = {}
    
    # Statistics
    new_orders_count = 0
    status_updates_count = 0
    shipped_orders_count = 0
    cancelled_orders_count = 0
    
    # Time until next new order
    next_order_time = time.time() + random.uniform(min_delay, max_delay)
    
    try:
        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time
            
            # Check if duration has elapsed
            if elapsed_time >= duration_seconds:
                break
            
            # Check if it's time to create a new order
            if current_time >= next_order_time:
                # Generate and print a new sale (always starts with "pending")
                sale = generate_sale()
                
                # Add metadata for tracking
                sale["last_status_change"] = current_time
                min_time, max_time = STATUS_TRANSITION_TIMES["pending"]
                sale["transition_time"] = random.uniform(min_time, max_time)
                
                # Store in active orders
                active_orders[sale["order_id"]] = sale
                
                # Prepare output (without internal tracking fields)
                output_sale = {k: v for k, v in sale.items() 
                              if k not in ["last_status_change", "transition_time"]}
                
                # Send to Kafka if configured
                if kafka_producer:
                    kafka_success = send_to_kafka(kafka_producer, kafka_topic, sale["order_id"], output_sale)
                    if not kafka_success:
                        print(f"WARNING: Failed to send order {sale['order_id']} to Kafka")
                
                # Print to console if enabled
                if output_console:
                    print(json.dumps(output_sale, indent=2))
                    print()
                
                new_orders_count += 1
                
                # Schedule next order
                next_order_time = current_time + random.uniform(min_delay, max_delay)
            
            # Check all active orders for status updates
            orders_to_update = []
            for order_id, order in active_orders.items():
                if should_advance_order(order, current_time):
                    orders_to_update.append(order_id)
            
            # Update orders that are ready to advance
            for order_id in orders_to_update:
                order = active_orders[order_id]
                advance_order_status(order, current_time)
                
                # Prepare output (without internal tracking fields)
                output_order = {k: v for k, v in order.items() 
                               if k not in ["last_status_change", "transition_time"]}
                
                # Send to Kafka if configured
                if kafka_producer:
                    kafka_success = send_to_kafka(kafka_producer, kafka_topic, order["order_id"], output_order)
                    if not kafka_success:
                        print(f"WARNING: Failed to send status update for {order['order_id']} to Kafka")
                
                # Print to console if enabled
                if output_console:
                    print(json.dumps(output_order, indent=2))
                    print()
                
                status_updates_count += 1
                
                # If order is now shipped or cancelled, remove from active orders
                if order["status"] == "shipped":
                    del active_orders[order_id]
                    shipped_orders_count += 1
                elif order["status"] == "cancelled":
                    del active_orders[order_id]
                    cancelled_orders_count += 1
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.1)
    
    except KeyboardInterrupt:
        print("\n\nGenerator stopped by user.")
    
    finally:
        # Close Kafka producer if it was created
        if kafka_producer:
            print("\nFlushing and closing Kafka producer...")
            kafka_producer.flush()
            kafka_producer.close()
            print("Kafka producer closed.")
        
        elapsed_time = time.time() - start_time
        print("=" * 80)
        print(f"Generator finished.")
        print(f"Total new orders created: {new_orders_count}")
        print(f"Total status updates: {status_updates_count}")
        print(f"Total orders shipped: {shipped_orders_count}")
        print(f"Total orders cancelled: {cancelled_orders_count}")
        print(f"Active orders (still in pipeline): {len(active_orders)}")
        print(f"Success rate: {(shipped_orders_count / new_orders_count * 100) if new_orders_count > 0 else 0:.1f}%")
        print(f"Cancellation rate: {(cancelled_orders_count / new_orders_count * 100) if new_orders_count > 0 else 0:.1f}%")
        print(f"Total time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        print(f"Average rate: {new_orders_count / (elapsed_time / 60):.2f} new orders per minute")


def main():
    """Main entry point for the sales generator."""
    parser = argparse.ArgumentParser(
        description="E-commerce Sales Generator - Generates random sales data with realistic delays",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate to console for 5 minutes
  python sales_generator.py
  
  # Generate to Kafka topic
  python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales
  
  # Generate to both Kafka and console
  python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales --output-console
  
  # Generate to Kafka only (no console output)
  python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales --no-console
        """
    )
    
    # Duration and timing arguments
    parser.add_argument(
        "--duration",
        type=int,
        default=5,
        help="Duration to run the generator in minutes (default: 5)"
    )
    parser.add_argument(
        "--min-delay",
        type=int,
        default=1,
        help="Minimum delay between sales in seconds (default: 1)"
    )
    parser.add_argument(
        "--max-delay",
        type=int,
        default=60,
        help="Maximum delay between sales in seconds (default: 60)"
    )
    
    # Kafka arguments
    parser.add_argument(
        "--kafka-bootstrap-servers",
        type=str,
        help="Kafka bootstrap servers (e.g., 'localhost:9092' or 'broker1:9092,broker2:9092')"
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        help="Kafka topic name to send events to"
    )
    
    # Output control arguments
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Disable console output (useful when only sending to Kafka)"
    )
    parser.add_argument(
        "--output-console",
        action="store_true",
        help="Enable console output (default when not using Kafka)"
    )
    
    args = parser.parse_args()
    
    # Validate Kafka arguments
    if args.kafka_bootstrap_servers and not args.kafka_topic:
        parser.error("--kafka-topic is required when --kafka-bootstrap-servers is specified")
    if args.kafka_topic and not args.kafka_bootstrap_servers:
        parser.error("--kafka-bootstrap-servers is required when --kafka-topic is specified")
    
    # Determine console output setting
    # Default: True if no Kafka, can be overridden with --no-console
    output_console = True
    if args.no_console:
        output_console = False
    elif args.output_console:
        output_console = True
    elif args.kafka_bootstrap_servers:
        # If sending to Kafka, default to no console unless explicitly enabled
        output_console = False
    
    run_generator(
        duration_minutes=args.duration,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        kafka_topic=args.kafka_topic,
        output_console=output_console
    )


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Native Kafka Manager
"""

import os
import sys
import subprocess
import time
import argparse
import urllib.request
import tarfile
import shutil
import signal
import json
from pathlib import Path


# Configuration
KAFKA_VERSION = "3.6.1"
SCALA_VERSION = "2.13"
KAFKA_DIR_NAME = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}"
KAFKA_DOWNLOAD_URL = f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{KAFKA_DIR_NAME}.tgz"
INSTALL_DIR = Path.home() / ".kafka-local"
KAFKA_HOME = INSTALL_DIR / KAFKA_DIR_NAME
PID_FILE = INSTALL_DIR / "kafka.pid"
ZK_PID_FILE = INSTALL_DIR / "zookeeper.pid"


def print_status(message, status="info"):
    """Print colored status messages."""
    colors = {
        "info": "\033[94m",      # Blue
        "success": "\033[92m",   # Green
        "warning": "\033[93m",   # Yellow
        "error": "\033[91m",     # Red
        "reset": "\033[0m"
    }
    
    icons = {
        "info": "ℹ️ ",
        "success": "✅",
        "warning": "⚠️ ",
        "error": "❌"
    }
    
    color = colors.get(status, colors["info"])
    icon = icons.get(status, "")
    reset = colors["reset"]
    
    print(f"{color}{icon} {message}{reset}")


def download_kafka():
    """Download and extract Kafka."""
    if KAFKA_HOME.exists():
        print_status(f"Kafka already downloaded at {KAFKA_HOME}", "success")
        return True
    
    print_status(f"Downloading Kafka {KAFKA_VERSION}...", "info")
    INSTALL_DIR.mkdir(parents=True, exist_ok=True)
    
    tar_file = INSTALL_DIR / f"{KAFKA_DIR_NAME}.tgz"
    
    try:
        # Download with progress
        def show_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, downloaded * 100 / total_size)
            sys.stdout.write(f"\r   Progress: {percent:.1f}% ({downloaded / 1024 / 1024:.1f} MB)")
            sys.stdout.flush()
        
        urllib.request.urlretrieve(KAFKA_DOWNLOAD_URL, tar_file, show_progress)
        print()  # New line after progress
        
        print_status("Extracting Kafka...", "info")
        with tarfile.open(tar_file, 'r:gz') as tar:
            tar.extractall(INSTALL_DIR)
        
        # Clean up tar file
        tar_file.unlink()
        
        print_status(f"Kafka installed at {KAFKA_HOME}", "success")
        return True
        
    except Exception as e:
        print_status(f"Failed to download Kafka: {e}", "error")
        return False


def is_port_in_use(port):
    """Check if a port is in use."""
    try:
        result = subprocess.run(
            f"lsof -i :{port} -t",
            shell=True,
            capture_output=True,
            text=True
        )
        return bool(result.stdout.strip())
    except:
        return False


def get_process_pid(pid_file):
    """Get PID from file if process is running."""
    if not pid_file.exists():
        return None
    
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        
        # Check if process is still running
        try:
            os.kill(pid, 0)
            return pid
        except OSError:
            # Process not running, clean up pid file
            pid_file.unlink()
            return None
    except:
        return None


def start_zookeeper():
    """Start Zookeeper."""
    if get_process_pid(ZK_PID_FILE):
        print_status("Zookeeper is already running", "warning")
        return True
    
    if is_port_in_use(2181):
        print_status("Port 2181 is already in use. Zookeeper may be running elsewhere.", "warning")
        return False
    
    print_status("Starting Zookeeper...", "info")
    
    zk_script = KAFKA_HOME / "bin" / "zookeeper-server-start.sh"
    zk_config = KAFKA_HOME / "config" / "zookeeper.properties"
    
    # Create log directory
    log_dir = INSTALL_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Start Zookeeper in background
    with open(log_dir / "zookeeper.log", "w") as log_file:
        process = subprocess.Popen(
            [str(zk_script), str(zk_config)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp
        )
    
    # Save PID
    with open(ZK_PID_FILE, 'w') as f:
        f.write(str(process.pid))
    
    # Wait for Zookeeper to be ready
    print_status("Waiting for Zookeeper to be ready...", "info")
    max_retries = 30
    for i in range(max_retries):
        if is_port_in_use(2181):
            print_status("Zookeeper is ready!", "success")
            return True
        time.sleep(1)
        if (i + 1) % 5 == 0:
            print(f"   Still waiting... ({i + 1}/{max_retries})")
    
    print_status("Zookeeper failed to start within timeout", "error")
    return False


def start_kafka(port=9092):
    """Start Kafka broker."""
    if get_process_pid(PID_FILE):
        print_status("Kafka is already running", "warning")
        return True
    
    if is_port_in_use(port):
        print_status(f"Port {port} is already in use. Kafka may be running elsewhere.", "warning")
        return False
    
    # Start Zookeeper first
    if not start_zookeeper():
        return False
    
    print_status(f"Starting Kafka on port {port}...", "info")
    
    kafka_script = KAFKA_HOME / "bin" / "kafka-server-start.sh"
    kafka_config = KAFKA_HOME / "config" / "server.properties"
    
    # Modify config for custom port if needed
    if port != 9092:
        with open(kafka_config, 'r') as f:
            config_content = f.read()
        
        # Update port
        config_content = config_content.replace(
            "listeners=PLAINTEXT://:9092",
            f"listeners=PLAINTEXT://:{port}"
        )
        
        custom_config = INSTALL_DIR / "server.properties"
        with open(custom_config, 'w') as f:
            f.write(config_content)
        kafka_config = custom_config
    
    # Create log directory
    log_dir = INSTALL_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Start Kafka in background
    with open(log_dir / "kafka.log", "w") as log_file:
        process = subprocess.Popen(
            [str(kafka_script), str(kafka_config)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp
        )
    
    # Save PID
    with open(PID_FILE, 'w') as f:
        f.write(str(process.pid))
    
    # Wait for Kafka to be ready
    print_status("Waiting for Kafka to be ready...", "info")
    max_retries = 60
    for i in range(max_retries):
        if is_port_in_use(port):
            time.sleep(2)  # Extra time for Kafka to fully initialize
            print_status("Kafka is ready!", "success")
            print_status(f"Bootstrap server: localhost:{port}", "info")
            return True
        time.sleep(1)
        if (i + 1) % 10 == 0:
            print(f"   Still waiting... ({i + 1}/{max_retries})")
    
    print_status("Kafka failed to start within timeout", "error")
    print_status(f"Check logs at: {log_dir / 'kafka.log'}", "info")
    return False


def stop_process(pid_file, name):
    """Stop a process by PID file."""
    pid = get_process_pid(pid_file)
    
    if not pid:
        print_status(f"{name} is not running", "warning")
        return
    
    print_status(f"Stopping {name} (PID: {pid})...", "info")
    
    try:
        # Try graceful shutdown first
        os.kill(pid, signal.SIGTERM)
        
        # Wait for process to stop
        for _ in range(10):
            try:
                os.kill(pid, 0)
                time.sleep(1)
            except OSError:
                # Process stopped
                break
        
        # Force kill if still running
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        
        # Clean up PID file
        if pid_file.exists():
            pid_file.unlink()
        
        print_status(f"{name} stopped", "success")
        
    except Exception as e:
        print_status(f"Error stopping {name}: {e}", "error")


def stop_kafka():
    """Stop Kafka and Zookeeper."""
    print_status("Stopping Kafka...", "info")
    stop_process(PID_FILE, "Kafka")
    time.sleep(2)
    stop_process(ZK_PID_FILE, "Zookeeper")


def status_kafka():
    """Show Kafka status."""
    print("\n" + "=" * 60)
    print("Kafka Status")
    print("=" * 60)
    
    kafka_pid = get_process_pid(PID_FILE)
    zk_pid = get_process_pid(ZK_PID_FILE)
    
    if kafka_pid:
        print_status("Kafka: RUNNING", "success")
        print(f"   PID: {kafka_pid}")
        print(f"   Port: 9092")
        print(f"   Bootstrap: localhost:9092")
    else:
        print_status("Kafka: NOT RUNNING", "error")
    
    if zk_pid:
        print_status("Zookeeper: RUNNING", "success")
        print(f"   PID: {zk_pid}")
        print(f"   Port: 2181")
    else:
        print_status("Zookeeper: NOT RUNNING", "error")
    
    # Check Kafka home
    if KAFKA_HOME.exists():
        print(f"\nKafka Home: {KAFKA_HOME}")
        print(f"Version: {KAFKA_VERSION}")
    else:
        print_status("\nKafka not installed. Run 'install' command first.", "warning")
    
    # List topics if Kafka is running
    if kafka_pid:
        print("\n📋 Topics:")
        list_topics_quiet()
    
    print("=" * 60 + "\n")


def create_topic(topic_name, partitions=3, replication_factor=1):
    """Create a Kafka topic."""
    if not get_process_pid(PID_FILE):
        print_status("Kafka is not running. Start it first.", "error")
        return False
    
    print_status(f"Creating topic: {topic_name}", "info")
    
    script = KAFKA_HOME / "bin" / "kafka-topics.sh"
    
    result = subprocess.run([
        str(script),
        "--create",
        "--topic", topic_name,
        "--bootstrap-server", "localhost:9092",
        "--partitions", str(partitions),
        "--replication-factor", str(replication_factor)
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print_status(f"Topic '{topic_name}' created successfully", "success")
        return True
    elif "already exists" in result.stderr:
        print_status(f"Topic '{topic_name}' already exists", "warning")
        return True
    else:
        print_status(f"Failed to create topic: {result.stderr}", "error")
        return False


def list_topics_quiet():
    """List topics without headers (for status command)."""
    if not get_process_pid(PID_FILE):
        return
    
    script = KAFKA_HOME / "bin" / "kafka-topics.sh"
    result = subprocess.run([
        str(script),
        "--list",
        "--bootstrap-server", "localhost:9092"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        topics = [t for t in result.stdout.strip().split('\n') if t and not t.startswith('__')]
        if topics:
            for topic in topics:
                print(f"   • {topic}")
        else:
            print("   (no topics created yet)")


def list_topics():
    """List all Kafka topics."""
    if not get_process_pid(PID_FILE):
        print_status("Kafka is not running", "error")
        return
    
    print_status("Kafka Topics:", "info")
    print("-" * 60)
    
    script = KAFKA_HOME / "bin" / "kafka-topics.sh"
    result = subprocess.run([
        str(script),
        "--list",
        "--bootstrap-server", "localhost:9092"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        topics = [t for t in result.stdout.strip().split('\n') if t and not t.startswith('__')]
        if topics:
            for topic in topics:
                print(f"\n📌 {topic}")
                # Get topic details
                detail_result = subprocess.run([
                    str(script),
                    "--describe",
                    "--topic", topic,
                    "--bootstrap-server", "localhost:9092"
                ], capture_output=True, text=True)
                
                for line in detail_result.stdout.split('\n'):
                    if 'PartitionCount' in line or 'ReplicationFactor' in line:
                        print(f"   {line.strip()}")
        else:
            print("   (no user topics found)")
    else:
        print_status(f"Error listing topics: {result.stderr}", "error")
    
    print("-" * 60)


def consume_topic(topic_name, from_beginning=True):
    """Consume messages from a topic."""
    if not get_process_pid(PID_FILE):
        print_status("Kafka is not running", "error")
        return
    
    print_status(f"Consuming from topic: {topic_name}", "info")
    print("   Press Ctrl+C to stop")
    print("-" * 60)
    
    script = KAFKA_HOME / "bin" / "kafka-console-consumer.sh"
    
    args = [
        str(script),
        "--topic", topic_name,
        "--bootstrap-server", "localhost:9092",
        "--property", "print.key=true",
        "--property", "key.separator=: "
    ]
    
    if from_beginning:
        args.append("--from-beginning")
    
    try:
        subprocess.run(args)
    except KeyboardInterrupt:
        print("\n" + "-" * 60)
        print_status("Stopped consuming", "info")


def view_logs(component="kafka", follow=False):
    """View logs."""
    log_dir = INSTALL_DIR / "logs"
    log_file = log_dir / f"{component}.log"
    
    if not log_file.exists():
        print_status(f"Log file not found: {log_file}", "error")
        return
    
    if follow:
        print_status(f"Following {component} logs (Ctrl+C to stop)...", "info")
        print("-" * 60)
        subprocess.run(["tail", "-f", str(log_file)])
    else:
        print_status(f"Recent {component} logs:", "info")
        print("-" * 60)
        subprocess.run(["tail", "-100", str(log_file)])


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Native Kafka Manager - Run Kafka without Docker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Install Kafka
  python kafka_native_manager.py install
  
  # Start Kafka
  python kafka_native_manager.py start
  
  # Check status
  python kafka_native_manager.py status
  
  # Create a topic
  python kafka_native_manager.py create-topic ecommerce-sales
  
  # List topics
  python kafka_native_manager.py list-topics
  
  # Consume from topic
  python kafka_native_manager.py consume ecommerce-sales
  
  # View logs
  python kafka_native_manager.py logs
  
  # Stop Kafka
  python kafka_native_manager.py stop
        """
    )
    
    parser.add_argument(
        "command",
        choices=["install", "start", "stop", "restart", "status", "create-topic", "list-topics", "consume", "logs"],
        help="Command to execute"
    )
    
    parser.add_argument(
        "topic",
        nargs="?",
        help="Topic name (for create-topic and consume commands)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=9092,
        help="Kafka port (default: 9092)"
    )
    
    parser.add_argument(
        "--partitions",
        type=int,
        default=3,
        help="Number of partitions for new topic (default: 3)"
    )
    
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor for new topic (default: 1)"
    )
    
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Follow logs in real-time"
    )
    
    parser.add_argument(
        "--component",
        choices=["kafka", "zookeeper"],
        default="kafka",
        help="Component to view logs for (default: kafka)"
    )
    
    args = parser.parse_args()
    
    # Execute command
    if args.command == "install":
        if download_kafka():
            print_status("\nKafka installation complete!", "success")
            print_status("Run 'python kafka_native_manager.py start' to start Kafka", "info")
        else:
            sys.exit(1)
    
    elif args.command == "start":
        # Ensure Kafka is installed
        if not KAFKA_HOME.exists():
            print_status("Kafka not installed. Installing now...", "info")
            if not download_kafka():
                sys.exit(1)
        
        if start_kafka(args.port):
            print_status("\n💡 Tip: Create a topic with:", "info")
            print(f"   python kafka_native_manager.py create-topic ecommerce-sales")
            print_status("\n💡 Then run the generator:", "info")
            print(f"   python sales_generator.py --kafka-bootstrap-servers localhost:{args.port} --kafka-topic ecommerce-sales")
        else:
            sys.exit(1)
    
    elif args.command == "stop":
        stop_kafka()
    
    elif args.command == "restart":
        stop_kafka()
        time.sleep(3)
        if not KAFKA_HOME.exists():
            download_kafka()
        start_kafka(args.port)
    
    elif args.command == "status":
        status_kafka()
    
    elif args.command == "create-topic":
        if not args.topic:
            print_status("Topic name required", "error")
            print("   Usage: python kafka_native_manager.py create-topic <topic-name>")
            sys.exit(1)
        if create_topic(args.topic, args.partitions, args.replication_factor):
            print_status(f"\n💡 Generate events with:", "info")
            print(f"   python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic {args.topic}")
    
    elif args.command == "list-topics":
        list_topics()
    
    elif args.command == "consume":
        if not args.topic:
            print_status("Topic name required", "error")
            print("   Usage: python kafka_native_manager.py consume <topic-name>")
            sys.exit(1)
        consume_topic(args.topic)
    
    elif args.command == "logs":
        view_logs(args.component, args.follow)


if __name__ == "__main__":
    main()


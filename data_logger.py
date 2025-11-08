# data_logger.py (NEW FILE for Node 4)
import mysql.connector
from kafka import KafkaConsumer
import requests
import time
import threading

# --- Configuration (MUST MATCH YOUR OTHER SCRIPTS) ---
BROKER = "192.168.193.192:9092"       # Kafka Broker IP
ADMIN_API = "http://127.0.0.1:8000"  # Admin API (running on same machine)
ADMIN_AUTH = ('admin', 'admin123')     # Admin credentials

DB_CONFIG = {
    "host": "localhost",
    "user": "kafkauser",
    "password": "Kafka123$team",
    "database": "kafka_dynamic"
}

# --- Global variables for managing the consumer ---
current_topics = set()
consumer = None
consumer_lock = threading.Lock()

def get_db_conn():
    """Establishes a new database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"[DataLogger] DB Connection Error: {e}")
        return None

def log_message_to_db(topic, message):
    """Writes a single message to the live_data table."""
    conn = get_db_conn()
    if not conn:
        print("[DataLogger] Cannot log message, no DB connection.")
        return
        
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO live_data (topic_name, message_content) VALUES (%s, %s)",
            (topic, message)
        )
        conn.commit()
    except Exception as e:
        print(f"[DataLogger] DB Insert Error: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_active_topics():
    """Fetches the list of active topics from the Admin API."""
    try:
        r = requests.get(f"{ADMIN_API}/topics/active", auth=ADMIN_AUTH)
        if r.ok:
            return set(r.json())
        else:
            print(f"[DataLogger] API Error: {r.status_code}")
    except Exception as e:
        print(f"[DataLogger] API Connection Error: {e}")
    return None

def start_consumer(topics):
    """Creates and returns a new KafkaConsumer."""
    if not topics:
        return None
    try:
        print(f"[DataLogger] Starting consumer for topics: {topics}")
        return KafkaConsumer(
            *list(topics),
            bootstrap_servers=BROKER,
            auto_offset_reset='earliest',
            consumer_timeout_ms=2000,
            group_id='data_logger_group' # Unique group ID
        )
    except Exception as e:
        print(f"[DataLogger] Kafka Connection Error: {e}")
        return None

def main():
    global current_topics, consumer
    print("[DataLogger] Starting Data Logger Service...")

    while True:
        # 1. Check for topic updates
        new_topics = get_active_topics()
        
        if new_topics is not None and new_topics != current_topics:
            print(f"[DataLogger] Topic change detected. Old: {current_topics}, New: {new_topics}")
            with consumer_lock:
                if consumer:
                    consumer.close()
                consumer = start_consumer(new_topics)
                current_topics = new_topics
        
        # 2. Consume messages
        with consumer_lock:
            if consumer:
                try:
                    for msg in consumer:
                        message_content = msg.value.decode('utf-8')
                        print(f"[DataLogger] Logging message from '{msg.topic}': {message_content}")
                        log_message_to_db(msg.topic, message_content)
                except Exception as e:
                    # This will happen on timeout, which is fine
                    pass
            else:
                # No topics, just wait
                time.sleep(5)
        
        # Poll for new topics every 10 seconds
        time.sleep(10)

if __name__ == "__main__":
    main()
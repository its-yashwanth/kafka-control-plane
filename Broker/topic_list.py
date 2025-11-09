from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import time

def list_topics():
    try:
        print("[INFO] Connecting to Kafka Broker on localhost:9092...")
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="broker_monitor"
        )
        print("[INFO] Connection established successfully.")
        
        topics = admin_client.list_topics()
        print(f"[INFO] Active Topics: {list(topics)}")

    except NoBrokersAvailable:
        print("[ERROR] Kafka broker not reachable. Please ensure Kafka is running.")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")

if __name__ == "__main__":
    print("[INFO] Kafka Topic Monitor Started.")
    while True:
        list_topics()
        print("[INFO] Waiting for 10 seconds before next check...\n")
        time.sleep(10)

# consumer.py (FINAL-FIXED Version)
from kafka import KafkaConsumer
import requests
import time
import threading

# --- Configuration ---
BROKER = "192.168.193.192:9092"
ADMIN_API = "http://192.168.193.220:8000"
USER_ID = ""
ADMIN_AUTH = ('admin', 'admin123')

# --- Shared State ---
consumer_lock = threading.Lock()
consumer = None
stop_consumer_event = threading.Event()
consumer_thread = None  # <-- MUST be declared as global here


# --- Consumer Thread ---
def consume_messages(stop_event):
    global consumer
    print("[Consumer] Starting message consumption loop...")
    while not stop_event.is_set():
        with consumer_lock:
            if consumer:
                try:
                    # Poll for messages with a timeout
                    for msg in consumer:
                        print(f"\n📥 [{msg.topic}]: {msg.value.decode()}\n")
                        # Check if stop is requested after processing a message
                        if stop_event.is_set():
                            break
                except Exception as e:
                    # This will trigger if consumer is closed, e.g., during resubscribe
                    pass
            else:
                # No active consumer, just wait
                pass

        # Check stop event outside the lock to prevent deadlock
        # and sleep if we're not stopping
        if not stop_event.is_set():
            time.sleep(1)

    print("[Consumer] Message consumption loop stopped.")


# --- Helper: Rebuilds the consumer with new subscriptions ---
def refresh_kafka_subscription():
    global consumer, stop_consumer_event, consumer_thread

    print("[Main] Refreshing subscriptions...")

    # 1. Stop the current consumer thread (if it exists)
    if consumer_thread and consumer_thread.is_alive():
        print("[Main] Stopping old consumer thread...")
        stop_consumer_event.set()
        consumer_thread.join()
        print("[Main] Old thread stopped.")

    # 2. Safely close the old consumer
    with consumer_lock:
        if consumer:
            print("[Main] Closing old KafkaConsumer object...")
            consumer.close()
            consumer = None
            print("[Main] Old consumer closed.")

        # 3. Fetch new subscriptions from the API
        try:
            r = requests.get(f"{ADMIN_API}/subscriptions/{USER_ID}", auth=ADMIN_AUTH)
            if r.status_code == 401:
                print("[Main] ERROR: Authentication failed. Check ADMIN_AUTH credentials.")
                return
            if not r.ok:
                print(f"[Main] ERROR: Could not fetch subscriptions (Status {r.status_code}).")
                return

            subscribed_topics = set(r.json())

            # 4. Create a new consumer if we have topics
            if subscribed_topics:
                print(f"[Main] Subscribing to: {subscribed_topics}")
                consumer = KafkaConsumer(
                    *list(subscribed_topics),
                    bootstrap_servers=BROKER,
                    auto_offset_reset='earliest',
                    consumer_timeout_ms=2000,  # Poll timeout
                    group_id=USER_ID
                )
            else:
                print("[Main] No active subscriptions. Standing by.")
                consumer = None

        except Exception as e:
            print(f"[Main] ERROR connecting to Kafka or API: {e}")

    # 5. Start the new consumer thread
    stop_consumer_event.clear()
    consumer_thread = threading.Thread(
        target=consume_messages, args=(stop_consumer_event,), daemon=True
    )
    consumer_thread.start()
    print("[Main] New consumer thread started. Refresh complete.")


# --- Main Thread (User Interface) ---
def main_menu():
    global USER_ID
    USER_ID = input("Enter your unique Consumer ID (e.g., consumer_01): ")
    print(f"Welcome, {USER_ID}!")

    # Start with an initial refresh
    refresh_kafka_subscription()

    while True:
        print("\n--- Consumer Menu ---")
        print(" 1. List available 'active' topics")
        print(" 2. Subscribe to a topic")
        print(" 3. Unsubscribe from a topic")
        print(" 4. List my current subscriptions")
        print(" q. Quit")
        choice = input("Enter choice: ").strip()

        try:
            if choice == '1':
                r = requests.get(f"{ADMIN_API}/topics/active", auth=ADMIN_AUTH)
                print("--- Available Active Topics ---")
                print(r.json() or "No active topics available.")

            elif choice == '2':
                topic_name = input("Enter topic to subscribe to: ")
                r = requests.post(
                    f"{ADMIN_API}/subscribe",
                    json={"user": USER_ID, "topic_name": topic_name},
                    auth=ADMIN_AUTH
                )
                if r.ok:
                    print("Subscribed. Re-configuring consumer...")
                    refresh_kafka_subscription()
                else:
                    print(f"ERROR: {r.json().get('detail')}")

            elif choice == '3':
                topic_name = input("Enter topic to unsubscribe from: ")
                r = requests.post(
                    f"{ADMIN_API}/unsubscribe",
                    json={"user": USER_ID, "topic_name": topic_name},
                    auth=ADMIN_AUTH
                )
                if r.ok:
                    print("Unsubscribed. Re-configuring consumer...")
                    refresh_kafka_subscription()
                else:
                    print(f"ERROR: {r.json().get('detail')}")

            elif choice == '4':
                r = requests.get(f"{ADMIN_API}/subscriptions/{USER_ID}", auth=ADMIN_AUTH)
                print(f"--- My Subscriptions ({USER_ID}) ---")
                print(r.json() or "You have no subscriptions.")

            elif choice == 'q':
                print("Shutting down...")
                if consumer_thread and consumer_thread.is_alive():
                    stop_consumer_event.set()
                    consumer_thread.join()
                if consumer:
                    consumer.close()
                print("Goodbye.")
                break

            else:
                print("Invalid choice.")

        except requests.exceptions.ConnectionError:
            print("ERROR: Cannot connect to Admin API. Is it running?")
        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(0.5)


if __name__ == "__main__":
    main_menu()

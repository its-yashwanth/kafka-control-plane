from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import threading
import requests
import time
import queue
import os 
import re 

# --- Configuration ---
BROKER = "192.168.193.192:9092"      
ADMIN_API = "http://192.168.193.220:8000"  
ADMIN_AUTH = ('admin', 'admin123')     # Admin credentials

# Topic Validation Regex ---
TOPIC_REGEX = re.compile(r"^[a-zA-Z0-9_-]+$")

# Shared queue between Input and Publisher threads
message_queue = queue.Queue()

# --- Thread 1: Topic Watcher ---
class TopicWatcher(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True, name="TopicWatcher")
        self.admin_client = KafkaAdminClient(bootstrap_servers=BROKER)
        self.created_topics = set() 
        print("[Watcher] Started. Monitoring for 'approved' and 'rejected' topics.")

    def run(self):
        while True:
            try:
                r = requests.get(f"{ADMIN_API}/topics", auth=ADMIN_AUTH)
                if not r.ok:
                    print(f"[Watcher] ERROR: Could not poll API (Status {r.status_code}). Will retry.")
                    time.sleep(5)
                    continue

                topics = r.json()
                
                db_active_set = {t['name'] for t in topics if t['status'] == 'active'}
                db_approved_set = {t['name'] for t in topics if t['status'] == 'approved'}
                db_rejected_set = {t['name'] for t in topics if t['status'] == 'rejected'}

                topics_to_discover = db_active_set - self.created_topics
                if topics_to_discover:
                    print(f"[Watcher] Discovered already-active topics: {topics_to_discover}")
                    self.created_topics.update(topics_to_discover)

                # --- 1. Find topics to CREATE ---
                topics_to_create = db_approved_set - self.created_topics
                for topic_name in topics_to_create:
                    print(f"[Watcher] Detected approved topic: '{topic_name}'. Attempting creation...")
                    try:
                        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                        self.admin_client.create_topics([new_topic])
                        print(f"[Watcher] ✅ SUCCESS: Created topic '{topic_name}' in Kafka.")
                        requests.post(f"{ADMIN_API}/topics/{topic_name}/set-active", auth=ADMIN_AUTH)
                        print(f"[Watcher] Set '{topic_name}' to 'active' in DB.")
                        self.created_topics.add(topic_name)
                    except TopicAlreadyExistsError:
                        print(f"[Watcher] WARN: Topic '{topic_name}' already exists. Setting 'active'.")
                        requests.post(f"{ADMIN_API}/topics/{topic_name}/set-active", auth=ADMIN_AUTH)
                        self.created_topics.add(topic_name)
                    except Exception as e:
                        print(f"[Watcher] ERROR creating topic '{topic_name}': {e}")
                
                # --- 2. Find topics to DELETE ---
                topics_to_delete = db_rejected_set.intersection(self.created_topics)
                for topic_name in topics_to_delete:
                    print(f"[Watcher] Detected rejected topic: '{topic_name}'. Attempting deletion...")
                    try:
                        self.admin_client.delete_topics([topic_name])
                        print(f"[Watcher] ✅ SUCCESS: Deleted topic '{topic_name}' from Kafka.")
                        self.created_topics.remove(topic_name)
                        
                        print(f"[Watcher] Confirming deletion of '{topic_name}' with Admin API...")
                        requests.post(f"{ADMIN_API}/topics/{topic_name}/confirm-delete", auth=ADMIN_AUTH)
                        print(f"[Watcher] Deletion confirmed.")
                        
                    except UnknownTopicOrPartitionError:
                        print(f"[Watcher] WARN: Topic '{topic_name}' already deleted/unknown. Removing from set.")
                        self.created_topics.remove(topic_name)
                    except Exception as e:
                        print(f"[Watcher] ERROR deleting topic '{topic_name}': {e}")

            except requests.exceptions.ConnectionError:
                print(f"[Watcher] ERROR polling API: Connection refused. Is admin_api.py running?")
            except Exception as e:
                print(f"[Watcher] ERROR in main loop: {e}")
            
            time.sleep(5)

# --- Thread 2: Input Listener ---
class InputListener(threading.Thread):
    def __init__(self, msg_queue):
        super().__init__(daemon=True, name="InputListener")
        self.msg_queue = msg_queue
        print("[Input] Started. Ready for input.")

    def run(self):
        while True:
            print("\n--- Producer Menu ---")
            print("  1. Request a new topic")
            print("  2. Send a single (live) message")
            print("  3. Send messages from a file")
            choice = input("Enter choice (1, 2, or 3): ")
            
            if choice == '1':
                topic_name = input("Enter new topic name to request: ")
                
                if not TOPIC_REGEX.match(topic_name):
                    print(f"[Input] ERROR: Invalid topic name. Use only letters, numbers, underscores (_), or hyphens (-).")
                    continue
                
                try:
                    r = requests.post(f"{ADMIN_API}/topics/request", json={"name": topic_name}, auth=ADMIN_AUTH)
                    
                    if r.status_code == 200:
                        print(f"[Input] ✅ SUCCESS: Topic request '{topic_name}' sent to admin for approval.")
                    elif r.status_code == 409: # 409 Conflict
                        print(f"[Input] ERROR: {r.json().get('detail')}")
                    else:
                        print(f"[Input] ERROR: {r.json().get('detail')}")

                except Exception as e:
                    print(f"[Input] ERROR connecting to API: {e}")
            
            elif choice == '2':
                topic_name = input("Enter topic to publish to: ")
                if not TOPIC_REGEX.match(topic_name):
                    print(f"[Input] ERROR: Invalid topic name. Skipping.")
                    continue
                
                message = input("Enter message: ")
                data = {"topic": topic_name, "message": message}
                self.msg_queue.put(data)
                print(f"[Input] Enqueued message for topic '{topic_name}'.")
            
            elif choice == '3':
                topic_name = input("Enter topic to publish file contents to: ")
                if not TOPIC_REGEX.match(topic_name):
                    print(f"[Input] ERROR: Invalid topic name. Skipping.")
                    continue
                
                file_path = input("Enter path to the text file (e.g., data.txt): ")
                
                if not os.path.exists(file_path):
                    print(f"[Input] ERROR: File not found at '{file_path}'. Please check the path.")
                    continue
                
                try:
                    with open(file_path, 'r') as f:
                        count = 0
                        for line in f:
                            message = line.strip()
                            if message:
                                data = {"topic": topic_name, "message": message}
                                self.msg_queue.put(data)
                                count += 1
                    print(f"[Input] ✅ SUCCESS: Enqueued {count} messages from '{file_path}' to topic '{topic_name}'.")
                except Exception as e:
                    print(f"[Input] ERROR reading file '{file_path}': {e}")
            
            else:
                print("[Input] Invalid choice. Try again.")
            
            time.sleep(0.5)

# --- Thread 3: Publisher ---
class Publisher(threading.Thread):
    def __init__(self, msg_queue):
        super().__init__(daemon=True, name="Publisher")
        self.msg_queue = msg_queue
        self.producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        print("[Publisher] Started. Waiting for messages in queue.")

    def run(self):
        while True:
            try:
                data = self.msg_queue.get()
                topic = data['topic']
                msg = data['message']
                
                print(f"[Publisher] Publishing to '{topic}': {msg}")
                self.producer.send(topic, msg)
                self.producer.flush() 
                self.msg_queue.task_done()
                
            except Exception as e:
                print(f"[Publisher] ERROR sending message: {e}")

# --- Main Thread ---
if __name__ == "__main__":
    print("Starting 3-Thread Producer...")
    
    TopicWatcher().start()
    InputListener(message_queue).start()
    Publisher(message_queue).start()
    
    while True:
        time.sleep(10)
# Dynamic Content Stream with Kafka

This project is a 4-node distributed system that solves the problem of manual topic management in Apache Kafka. It provides a complete, admin-controlled workflow for dynamically creating, approving, monitoring, and deleting Kafka topics in real-time.

---

## System Architecture

The system consists of four distinct nodes that communicate over a network (LAN or ZeroTier). The entire process is managed by a central "Control Plane" (Node 4), which acts as the single source of truth for the system.

![BD_diagram](https://github.com/user-attachments/assets/23b16479-d212-4513-ab15-f6e1c641c06e)

* *Node 1: Producer (Python)*
    * A multi-threaded application (Input Listener, Publisher, Topic Watcher).
    * Requests new topics from the Admin.
    * Publishes messages (from live input or files) to a shared internal queue.
    * Watches the Admin for approved or rejected topics to dynamically create or delete them from the Kafka Broker.

* *Node 2: Kafka Broker (Kafka)*
    * The central message pipeline (Kafka & Zookeeper).
    * Configured with auto.create.topics.enable=false to give the Admin full control.

* *Node 3: Consumer (Python)*
    * An interactive CLI that allows users to view available topics.
    * Manages real-time, dynamic subscriptions and un-subscriptions.
    * Consumes and displays messages from subscribed topics.

* *Node 4: Admin & Control Plane (Python/FastAPI/MySQL)*
    * The "brain" of the system, running three services:
    * *MySQL Database:* Stores all system metadata (topics, status, subscriptions).
    * *FastAPI Backend:* A secure, authenticated REST API that serves as the "control plane" for all other nodes.
    * *Data Logger:* A background service that subscribes to all active topics and logs all messages to the database.
    * *HTML/JS Frontend:* A secure web dashboard for the admin to approve/reject topics and monitor all system activity (logs, live data, subscriptions).

---

## 🛠 Technology Stack

| Category | Technology |
| :--- | :--- |
| *Backend* | Python 3, FastAPI |
| *Messaging* | Apache Kafka |
| *Database* | MySQL |
| *Frontend* | HTML5, CSS3, JavaScript (fetch API) |
| *Libraries* | kafka-python, requests, mysql-connector-python, uvicorn |
| *Security* | HTTP Basic Authentication |

---

## ✨ Features

* *Dynamic Topic Lifecycle:* Full workflow (Request → Approve → Active → Reject → Delete) controlled by the Admin.
* *Centralized Control Plane:* Admin Dashboard provides a single view to manage the entire system.
* *Secure Admin Panel:* All Admin API endpoints and the frontend are protected by HTTP Basic Authentication.
* *Real-time Monitoring:* The dashboard includes feeds for "Live Data" and "Admin Logs" pulled directly from the database.
* *Resilient Producer:* 3-thread architecture with an internal queue ensures that input is never blocked and publishing is handled separately.
* *Interactive Consumer:* Users can dynamically join or leave topics without restarting the script.
* *Input Validation:* The producer validates topic names against a regex to prevent invalid requests.
* *Graceful Error Handling:* The API provides clear error messages (e.g., 409 Conflict for duplicate requests).

---

## 🚀 Setup and Installation

### Prerequisites
* Git
* Python 3.8+ and pip
* Apache Kafka (e.g., version 2.8.x)
* MySQL Server

### 1. General Setup (All Nodes)
1.  Clone this repository: git clone https://github.com/suhithreddyc/10_Project2_BD
2.  Install ZeroTier or ensure all 4 nodes are on the same LAN.

### 2. Node 4: Admin + Control Plane

1.  *Install MySQL:*
    bash
    sudo apt install mysql-server
    sudo systemctl start mysql
    sudo mysql_secure_installation
    
2.  *Create Database & Tables:* Log in to MySQL (sudo mysql -u root -p) and run the SQL script (or commands) to create the kafka_dynamic database and all 5 tables (topics, user_subscriptions, admin_logs, live_data, and the kafkauser).
3.  *Install Python Dependencies:*
    bash
    cd admin-node/
    pip3 install -r requirements.txt
    
4.  *Edit IPs:* Open data_logger.py and ensure the BROKER and ADMIN_API IPs are correct.

### 3. Node 2: Kafka Broker
1.  Download and extract Kafka.
2.  *CRITICAL:* Replace the default config/server.properties with the one from this repository.
3.  *CRITICAL:* Edit config/server.properties and change advertised.listeners to the broker's real ZeroTier/LAN IP.

### 4. Node 1: Producer
1.  *Install Python Dependencies:*
    bash
    cd producer-node/
    pip3 install -r requirements.txt
    
2.  *Edit IPs:* Open producer.py and set the correct IPs for BROKER and ADMIN_API.

### 5. Node 3: Consumer
1.  *Install Python Dependencies:*
    bash
    cd consumer-node/
    pip3 install -r requirements.txt
    
2.  *Edit IPs:* Open consumer.py and set the correct IPs for BROKER and ADMIN_API.

---

## 🚦 Execution Order

Services *must* be started in this order:

1.  *Node 4 (DB):* Start the MySQL service: sudo systemctl start mysql
2.  *Node 2 (Broker):* Start Zookeeper: bin/zookeeper-server-start.sh ...
3.  *Node 2 (Broker):* Start Kafka Server: bin/kafka-server-start.sh ...
4.  *Node 4 (Admin):* Start the API Server (in one terminal):
    bash
    python3 admin_api.py
    
5.  *Node 4 (Admin):* Start the Data Logger (in a second terminal):
    bash
    python3 data_logger.py
    
6.  *Node 1 (Producer):* Start the Producer:
    bash
    python3 producer.py
    
7.  *Node 3 (Consumer):* Start the Consumer:
    bash
    python3 consumer.py
    

---

## 🎬 Demo Flow

1.  *Request:* In the *Producer* terminal, select option 1. Request a new topic and enter weather_feed.
2.  *Approve:* Open http://<ADMIN_IP>:8000 in a browser. Log in with admin / admin123. Click the "Approve" button for weather_feed.
3.  *Create:* The *Producer* terminal will show [Watcher] ✅ SUCCESS: Created topic 'weather_feed' in Kafka.
4.  *Monitor (Admin):* The *Admin Dashboard* will auto-refresh. weather_feed's status will change to active, and an "Approve" message will appear in the "Admin Logs" feed.
5.  *Subscribe:* In the *Consumer* terminal, select option 1 to see weather_feed is active. Select option 2 to subscribe to it.
6.  *Monitor (Admin):* The *Admin Dashboard* auto-refreshes. A "User subscribed" log appears, and the "User Subscriptions" table updates.
7.  *Send:* In the *Producer* terminal, select option 2. Send a single message. Send to topic weather_feed with message Sunny 27C.
8.  *Receive:* The *Consumer* terminal instantly prints: 📥 [weather_feed]: Sunny 27C.
9.  *Monitor (Admin):* The *Admin Dashboard* auto-refreshes. The "Live Data Feed" table shows the message Sunny 27C.
10. *Delete:* On the *Admin Dashboard*, click the "Reject/Delete" button for weather_feed.
11. *Confirm Delete:* The *Producer* terminal will show [Watcher] ✅ SUCCESS: Deleted topic 'weather_feed' from Kafka.
12. *Verify:* The topic weather_feed will completely disappear from the Admin Dashboard's "All Topics Status" table. The lifecycle is complete.

---

## 👨‍💻 Project Contributors

* *Thilak(PES2UG23CS654)*           *(Admin & Control Plane)
* *Varun Kumar(PES2UG23CS677)*      *(Broker Node)
* *Suhith(PES2UG23CS617)*           *(Consumer Node)
* *Yashwanth H S(PES2UG23CS716)*    *(Producer Node)

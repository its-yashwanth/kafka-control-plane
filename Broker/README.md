# Node 2: Kafka Broker — Varun Kumar S

This folder contains the configuration and scripts for running the **Kafka Broker** in the *Dynamic Content Streaming System* project.  
The Kafka Broker acts as the **central data pipeline** between producers and consumers, ensuring reliable and controlled message streaming.

---

## Responsibilities
- Set up and configure **Kafka & Zookeeper** for real-time message streaming.  
- Ensure controlled topic creation (`auto.create.topics.enable=false`).  
- Support dynamic topic creation and deletion through the **Kafka Admin API**.  
- Validate message routing between Producer and Consumer nodes.  
- Maintain stable communication in the distributed streaming system.

---

## Folder Structure

| File / Folder | Description |
|----------------|-------------|
| `config/server.properties` | Kafka Broker configuration file |
| `config/zookeeper.properties` | Zookeeper configuration file |
| `start_zookeeper.sh` | Script to start Zookeeper service |
| `start_kafka.sh` | Script to start Kafka Broker |
| `topic_list.py` | Python script to monitor active topics dynamically |
| `broker_test_log.txt` | Test log showing topic creation/deletion results |
| `kafka_setup_guide.md` | Detailed setup guide |

---

# Technology Stack
- **Language:** Python  
- **Message Broker:** Apache Kafka  
- **Coordination Service:** Zookeeper  
- **Database:** SQLite / MySQL (for Admin Node)  
- **Libraries:** `kafka-python`, `confluent-kafka`, `flask`, `threading`

---

##How to Run

### Start Zookeeper
```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

### Start Kafka Broker
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties

### (Optional) Verify Active Topics
cd /opt/kafka
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

### Run Topic Monitor
python3 topic_list.py



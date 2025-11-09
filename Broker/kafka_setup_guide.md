# Kafka Broker Setup (Node 2 - Varun Kumar S)

## 1. Environment
- Kafka version: 3.9.0
- Zookeeper port: 2181
- Kafka broker port: 9092

## 2. Configuration Changes
File: `config/server.properties`


## 3. Starting Commands
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

#Topic visibility
bin/kafka-topics.sh --list --bootstrap-server localhost:9092



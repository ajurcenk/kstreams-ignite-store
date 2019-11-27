# Apache Ignite KStreams state store demo
KStreams state store Apache Ignite implementation example.

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites
JDK 1.8, maven

### Installing 
```bash
# Clone the project
git clone https://github.com/ajurcenk/kstreams-ignite-store.git
# Change the folder 
cd kstreams-ignite-store
```

## Running the test
```bash
mvn clean test
```

## Running end to end example
 Start local Kafka cluster (the project uses local Kafka and local Zookeeper servers (localhost:9092 for Kafka, and localhost:2181 for Zookeeper)
 ```bash
# Change folder to bin
cd bin

# Create topics
./create_topics.sh

# Start streaming application
./start-streaming-app.sh

# Produce test data
./produce_test_data.sh

# Start Ignite application
./start-ignite-app.sh

```

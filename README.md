# StreamOps-A-Containerized-Kafka-Spark-Airflow-Pipeline

This repository implements a data streaming and processing pipeline using Apache Kafka, Apache Airflow, Apache Spark, and Cassandra. The system gathers user data via an external API, streams it to Kafka, processes the data in real-time using Spark, and finally stores it into a Cassandra database.

## Technologies Used

- [Kafka](https://kafka.apache.org/) ![Kafka](https://apache.org/logos/res/kafka/kafka_highres.png)
- [Airflow](https://airflow.apache.org/) ![Airflow](https://airflow.apache.org/docs/apache-airflow/stable/_images/powered-by-airflow-small.png)
- [Spark](https://spark.apache.org/) ![Spark](https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/Apache_Spark_logo.svg/512px-Apache_Spark_logo.svg.png)
- [Cassandra](http://cassandra.apache.org/) ![Cassandra](https://upload.wikimedia.org/wikipedia/commons/thumb/3/33/Apache_Cassandra_logo.svg/512px-Apache_Cassandra_logo.svg.png)
- [Docker](https://www.docker.com/) ![Docker](https://upload.wikimedia.org/wikipedia/commons/a/a1/Docker_logo.png)

## Project Overview
This project sets up an end-to-end data streaming pipeline using Kafka, Spark, and Airflow. The data is retrieved from an external API, formatted, and streamed into a Kafka topic (users_created). A Spark consumer then processes the data from Kafka, transforms it, and inserts it into Cassandra for storage.

## Components
Airflow DAG (kafka-stream.py): Fetches data from a REST API, formats it, and streams it to Kafka every minute.
Kafka: Used to stream the data between services.
Spark: Consumes the data from Kafka, processes it, and writes it to Cassandra.
Cassandra: A NoSQL database for storing the streamed user data.
Docker Compose: Manages all services (Kafka, Spark, Cassandra, Airflow) in containers.

# Project Structure
.
├── dags
│ └── kafka-stream.py # Airflow DAG to stream data from API to Kafka
├── script
│ └── entrypoint.sh # Entrypoint script for Airflow containers
├── docker-compose.yml # Docker Compose file to set up services
├── requirements.txt # Python dependencies for Airflow
└── spark-stream.py # Spark streaming application to consume from Kafka and insert into Cassandra


## Step-by-Step Setup

### 1. Build and Run Services Using Docker Compose
First, navigate to the project directory and build the services:

```bash
docker-compose up --build
```

This will start the following services:
- Zookeeper: Required by Kafka for managing brokers.
- Kafka Broker: The main Kafka server for message streaming.
- Schema Registry: Handles schema management for Kafka messages.
- Control Center: Provides a UI to manage Kafka clusters.
- Airflow Webserver & Scheduler: Handles workflow orchestration.
- Postgres: Database for Airflow metadata.
- Spark Master & Worker: For Spark streaming.
- Cassandra DB: Stores processed data from Spark.

### 2. Install Python Dependencies
Airflow will install the required Python packages automatically upon starting, but if you need to install them manually, you can use:

```bash
pip install -r requirements.txt
```

### 3. Airflow Configuration
Airflow will be available at http://localhost:8080. The default admin username and password are both set to admin.

Once the webserver starts, Airflow will begin executing the DAG defined in kafka-stream.py.

### 4. Run the Data Pipeline
Kafka Producer (kafka-stream.py): This script fetches data from a public API (https://randomuser.me/api/) and streams it to the Kafka topic users_created every minute. The data is formatted and then sent as a message to Kafka.

Spark Consumer (spark-stream.py): This script connects to Kafka, reads the data stream, and processes it. The processed data is then inserted into a Cassandra database for storage.

### 5. Airflow DAG Description
In the kafka-stream.py, the following steps occur:

get_data: Fetchs a random user from the external API.
format_data: Formats the user data into a structure suitable for Kafka.
stream_data: Sends the formatted data to Kafka every minute.

Airflow Task Breakdown
The Airflow task stream_data_from_api calls the stream_data function, which continuously streams data from the API to Kafka.

### 6. Kafka and Spark Setup
Kafka is configured with two main services: the broker and zookeeper. The broker listens on port 29092 for Kafka producers and consumers.
Spark connects to Kafka and listens for messages on the users_created topic. It transforms the data and writes it to Cassandra.

### 7. Cassandra Setup
The spark-stream.py script connects to the Cassandra database to store the processed data. It first ensures that the appropriate keyspace and table are created, and then inserts the user data received from Kafka.

### 8. Docker Compose Configuration
The Docker Compose file defines all the services needed for the pipeline. Key details include:
Airflow: Runs the DAG with the script mounted.
Kafka: Manages the message streaming system.
Spark: Consumes data from Kafka, processes it, and inserts it into Cassandra.
Cassandra: Stores processed user data.

### How to Test
Start all services using docker-compose up.

Access Airflow UI at http://localhost:8080.

Trigger the DAG user_automation manually from the Airflow UI or wait for it to trigger automatically (scheduled daily).

Check Kafka for messages in the users_created topic.

Verify that the processed data is inserted into Cassandra by querying the created_users table.

Example Query for Cassandra:
To check if the data was inserted into Cassandra, use cqlsh:

```bash
cqlsh localhost
USE spark_streams;
SELECT * FROM created_users;
```

### Files Explained
kafka-stream.py: Airflow DAG script to stream data from an API to Kafka.

spark-stream.py: Spark consumer script to read from Kafka, process data, and insert into Cassandra.

docker-compose.yml: The configuration for all the services in the project.

entrypoint.sh: Entrypoint for the Airflow containers to ensure correct setup before running the services.

## Conclusion
This project demonstrates how to set up a real-time data streaming pipeline using Kafka, Spark, and Cassandra, orchestrated by Airflow. You can modify this architecture for more complex data processing workflows and integrate other systems such as machine learning models, data lakes, or real-time dashboards.


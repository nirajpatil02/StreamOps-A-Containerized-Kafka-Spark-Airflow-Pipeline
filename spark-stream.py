import logging
from uuid import UUID
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Setup logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")

def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT);
        """)
        logging.info("Table 'created_users' created successfully!")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

def insert_data(session, **kwargs):
    logging.info(f"Inserting data: {kwargs}")

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (kwargs['id'], kwargs['first_name'], kwargs['last_name'], kwargs['gender'],
              kwargs['address'], kwargs['post_code'], kwargs['email'], kwargs['username'],
              kwargs['registered_date'], kwargs['phone'], kwargs['picture']))
        logging.info(f"Data inserted for {kwargs['first_name']} {kwargs['last_name']}")
    except Exception as e:
        logging.error(f"Could not insert data into Cassandra: {e}")

def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Connected to Kafka and stream started.")
        return spark_df
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Connected to Cassandra successfully!")
        return cas_session
    except Exception as e:
        logging.error(f"Error connecting to Cassandra: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),  # This was in your producer but missing in consumer
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    try:
        # Logging the raw Kafka data before transforming it
        spark_df.selectExpr("CAST(value AS STRING)").show(truncate=False)
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
        logging.info("Structured DataFrame from Kafka created successfully.")
        return sel
    except Exception as e:
        logging.error(f"Error processing Kafka stream into DataFrame: {e}")
        return None

def process_batch(df, epoch_id, session):
    logging.info(f"Processing batch for epoch {epoch_id} with {df.count()} rows...")
    rows = df.collect()
    if rows:
        for row in rows:
            try:
                # Handle UUID conversion safely
                user_uuid = UUID(row["id"]) if row["id"] else None
                insert_data(session, 
                          id=user_uuid,
                          first_name=row["first_name"],
                          # ... rest of the fields
                          )
            except (ValueError, TypeError) as e:
                logging.error(f"Invalid UUID format: {row['id']}. Error: {e}")
                continue

def main():
    # Create Spark and Cassandra connections
    spark_conn = create_spark_connection()
    if not spark_conn:
        return

    session = create_cassandra_connection()
    if not session:
        return

    # Create keyspace and table in Cassandra
    create_keyspace(session)
    create_table(session)

    # Start streaming
    spark_df = connect_to_kafka(spark_conn)
    if not spark_df:
        return

    selection_df = create_selection_df_from_kafka(spark_df)
    if not selection_df:
        return

    # Start the batch processing for streaming
    logging.info("Streaming is starting...")
    streaming_query = selection_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, session)) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    # Wait for termination
    streaming_query.awaitTermination()

if __name__ == "__main__":
    main()

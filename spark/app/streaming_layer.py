from array import ArrayType
import logging
import os

from pathlib import Path

# Spark imports
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from pyspark.sql.streaming import StreamingQuery

# Configuration constants
KAFKA_CORES = 2
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "rennes_traffic"
KEYSPACE = "rennes_traffic"
TABLE = "traffic_realtime"
BASE_PATH = Path("/opt/spark/datalake")
DATA_PATH = BASE_PATH / "rennes_traffic_data"
BASE_CHECKPOINT_PATH = BASE_PATH / "checkpoints"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RennesTrafficStreaming:
    """Spark Streming class to represent the streaming layer of the architecture"""
    
    def __init__(self):

        logger.info("Starting SparkSession...")
        
        # Initialize SparkSession
        self.spark = SparkSession.builder\
            .appName("RennesTrafficStreaming") \
            .master("spark://spark-master:7077") \
            .config("spark.driver.memory", "500m")\
            .config("spark.executor.memory", "500m")\
            .config("spark.cores.max", str(KAFKA_CORES))\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.sql.streaming.checkpointLocation", str(BASE_CHECKPOINT_PATH / "spark_streaming")) \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("SparkSession started successfully.")
        
    def create_traffic_schema(self):
        """Define schema for traffic data"""
        
        logger.debug("Defining traffic schema.")
        
        # Schema definition based on API response structure
        traffic_schema = StructType([
            StructField("datetime", StringType(), True),
            StructField("predefinedlocationreference", StringType(), True),
            StructField("averagevehiclespeed", DoubleType(), True),
            StructField("traveltime", IntegerType(), True),
            StructField("traveltimereliability", IntegerType(), True),     
            StructField("trafficstatus", StringType(), True),
            StructField("vehicleprobemeasurement", IntegerType(), True),   
            
            StructField("geo_point_2d", StructType([
                StructField("lat", DoubleType(), True),
                StructField("lon", DoubleType(), True)
            ]), True),
            
            StructField("geo_shape", StructType([
                StructField("type", StringType(), True),
                StructField("geometry", StructType([
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(ArrayType(DoubleType()))) 
                ]), True),
                StructField("properties", MapType(StringType(), StringType()), True)
            ]), True),
            
            StructField("gml_id", StringType(), True),                     
            StructField("id_rva_troncon_fcd_v1_1", IntegerType(), True),   
            StructField("hierarchie", StringType(), True),
            StructField("hierarchie_dv", StringType(), True),              
            StructField("denomination", StringType(), True),
            StructField("insee", StringType(), True),                       
            StructField("sens_circule", StringType(), True),
            StructField("vitesse_maxi", IntegerType(), True)
        ])
        
        return traffic_schema
    
    def read_from_kafka(self) -> DataFrame:
        """ Read streaming data from Kafka topic

        Returns:
            DataFrame: DataFrame representing the streaming data read from Kafka topic
        """
        
        logger.info(f"Reading data from Kafka topic: {KAFKA_TOPIC}")
        
        try:
            # Read from Kafka topic as streaming DataFrame
            df_kafka = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            logger.info("Kafka read started successfully.")
            return df_kafka
        
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
            
    def parse_kafka_stream(self) -> DataFrame:
        """ Parse Kafka stream and apply necessary transformations to extract relevant fields

        Returns:
            DataFrame: DataFrame representing the parsed and transformed streaming data from Kafka topic
        """
        
        logger.info("Starting Kafka stream parsing.")
        
        try:
            # Read raw stream from Kafka and parse JSON value using defined schema
            df_raw = self.read_from_kafka() \
                .select(
                    F.from_json(F.col("value").cast("string"), self.create_traffic_schema()).alias("data")
                ).select("data.*")
                
            logger.info("JSON parsing completed successfully.")
            
            # Apply transformations to extract relevant fields and flatten nested structures
            df_final = df_raw.select(
                F.col("datetime").alias("date_time"),
                F.col("denomination"),
                F.col("averagevehiclespeed").alias("avg_speed"),
                F.col("traveltime").alias("travel_time"),
                F.col("trafficstatus").alias("traffic_status"),
                F.col("geo_shape"),
                F.col("hierarchie"),
                F.col("hierarchie_dv"),
                F.col("vitesse_maxi").alias("max_speed")) \
                .withColumn("date_partition", F.date_format("date_time", "yyyy-MM-dd"))
                
            logger.info("Transformations applied to DataFrame.")
            
            return df_final
        
        except Exception as e:
            logger.error(f"Error processing Kafka stream: {e}")
            raise
    
    
    def write_to_parquet(self, df: DataFrame) -> StreamingQuery:
        """Write streaming DataFrame to local Parquet files

        Args:
            df (DataFrame): DataFrame to be written to Parquet files

        Returns:
            StreamingQuery: Streaming query representing the write operation
        """
        # Define checkpoint location for streaming query
        checkpoint_rennes = BASE_CHECKPOINT_PATH / "rennes_traffic"
        
        # Ensure data and checkpoint directories exist before starting the streaming query
        for dir_path in [DATA_PATH, checkpoint_rennes]:
            if not os.path.exists(dir_path):
                logger.info(f"Creating directory: {dir_path}")
                os.makedirs(dir_path, exist_ok=True)
        
        logger.info("Writing data to Parquet files.")
        
        # Start streaming query to write DataFrame to Parquet files with partitioning by date_time
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", DATA_PATH) \
            .option("checkpointLocation", checkpoint_rennes) \
            .partitionBy("date_partition") \
            .start()

        logger.info("Streaming query to Parquet started successfully.")
        
        return query
    
    def write_to_cassandra(self, df: DataFrame) -> StreamingQuery:
        """Write streaming DataFrame to Cassandra table

        Args:
            df (DataFrame): DataFrame to be written to Cassandra table

        Returns:
            StreamingQuery: Streaming query representing the write operation
        """
        
        logger.info(f"Writing streaming DataFrame to Cassandra table: {TABLE} in keyspace: {KEYSPACE}")
        
        # Start streaming query to write DataFrame to Cassandra table
        query = df.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", KEYSPACE) \
            .option("table", TABLE) \
            .option("checkpointLocation", BASE_CHECKPOINT_PATH / "cassandra_realtime") \
            .start()
            
        logger.info("Streaming query to Cassandra started successfully.")
            
        return query

    def stop(self):
        """Stop Spark session"""
        
        logger.info("Stopping SparkSession...")
        
        self.spark.stop()
        
        logger.info("SparkSession stopped.")

def main():
    """Main function to run the streaming application"""
    
    logger.info("Starting Rennes traffic streaming pipeline.")
    
    # Initialize streaming layer
    traffic_stream = RennesTrafficStreaming()
    
    # Parse Kafka stream and apply transformations
    df_final = traffic_stream.parse_kafka_stream()
    
    # Write streaming DataFrame to Cassandra table
    logger.info("Starting streaming query...")
    traffic_stream.write_to_cassandra(df_final)
    traffic_stream.write_to_parquet(df_final)
    
    try:
        # Await termination of streaming query to keep application running
        traffic_stream.spark.streams.awaitAnyTermination()
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Stopping streaming query...")
        
        for query in traffic_stream.spark.streams.active:
            query.stop()
            
        traffic_stream.stop()
        
        logger.info("Spark session stopped. Exiting application.")

if __name__ == "__main__":
    main()
    
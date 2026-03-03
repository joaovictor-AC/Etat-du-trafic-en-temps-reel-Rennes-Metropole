import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

class RennesTrafficStreaming:
    def __init__(self):

        logger.info("Starting SparkSession...")
        self.spark = SparkSession.builder\
            .appName("RennesTrafficStreaming") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.driver.memory", "1g")\
            .config("spark.executor.memory", "1g")\
            .config("spark.cores.max", "4")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("SparkSession started successfully.")
        
    def create_traffic_schema(self):
        """Define schema for traffic data"""
        
        logger.debug("Defining traffic schema.")
        
        traffic_schema = StructType([
            StructField("timestamp", StringType(), True),     
            StructField("street_name", StringType(), True),
            StructField("current_speed", IntegerType(), True),
            StructField("speed_limit", IntegerType(), True),
            StructField("lat", DoubleType(), True),           
            StructField("lon", DoubleType(), True),             
            StructField("status", StringType(), True)         
        ])
        
        return traffic_schema
    
    def read_from_kafka(self, topic: str):
        """
        Read streaming data from Kafka topic
        Returns DataFrame with raw JSON strings
        """
        
        logger.info(f"Reading data from Kafka topic: {topic}")
        
        try:
            df_kafka = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            logger.info("Kafka read started successfully.")
            return df_kafka
        
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
            
    def parse_kafka_stream(self):
        logger.info("Starting Kafka stream parsing.")
        
        try:
            df_parsed = self.read_from_kafka("rennes_traffic") \
                .select(
                    F.from_json(F.col("value").cast("string"), self.create_traffic_schema()).alias("data")
                ).select("data.*")
                
            logger.info("JSON parsing completed successfully.")
            
            df_transformed = df_parsed \
                .withColumn("timestamp_parsed", F.to_timestamp("timestamp")) \
                .withColumn("status_color",
                            F.when(F.col("current_speed") > F.col("speed_limit") * 0.8, "RED")
                             .otherwise("GREEN")
                             )\
                .withColumn("processing_time", F.current_timestamp())\
                .withColumn("date_partition", F.date_format("timestamp_parsed", "yyyy-MM-dd"))
                
            logger.info("Transformations applied to DataFrame.")
            
            return df_transformed
        
        except Exception as e:
            logger.error(f"Error processing Kafka stream: {e}")
            raise
    
    ############# JUST FOR DEBUGGING PURPOSES #############
    def write_to_console(self, df):
        """
        Write streaming DataFrame to console for debugging
        """
        
        logger.info("Writing data to console (debug mode).")
        
        try:
            query = df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .start()
            logger.info("Console write query started.")
            
            return query
        
        except Exception as e:
            logger.error(f"Error writing to console: {e}")
            raise
    ########################################################
    
    def write_to_parquet(self, df):
        """
        Write streaming DataFrame to local Parquet files
        """
        
        data_path = "/opt/spark/datalake/rennes_traffic_data"
        checkpoint_path = "/opt/spark/datalake/checkpoints/rennes_traffic"
        
        for dir_path in [data_path, checkpoint_path]:
            if not os.path.exists(dir_path):
                logger.info(f"Creating directory: {dir_path}")
                os.makedirs(dir_path, exist_ok=True)
        
        logger.info("Writing data to Parquet files.")
        
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", data_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("date_partition") \
            .start()
    
        logger.info("Parquet write query started.")
        
        return query
    
    def stop(self):
        """Stop Spark session"""
        
        logger.info("Stopping SparkSession...")
        
        self.spark.stop()
        
        logger.info("SparkSession stopped.")

def main():
    logger = logging.getLogger("Main")
    logger.info("Starting Rennes traffic streaming pipeline.")
    
    traffic_stream = RennesTrafficStreaming()
    df_final = traffic_stream.parse_kafka_stream()
    logger.info("Starting streaming query...")
    
    ##### JUST FOR DEBUGGING PURPOSES - WRITE TO CONSOLE #####
    traffic_stream.write_to_console(df_final)
    ##########################################################
    traffic_stream.write_to_parquet(df_final)
    
    try:
        traffic_stream.spark.streams.awaitAnyTermination()
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Stopping streaming query...")
        
        for query in traffic_stream.spark.streams.active:
            query.stop()
            
        traffic_stream.stop()
        
        logger.info("Spark session stopped. Exiting application.")

if __name__ == "__main__":
    main()
    
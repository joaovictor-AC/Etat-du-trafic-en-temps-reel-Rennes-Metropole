import logging
from datetime import date, timedelta
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _get_week_dates():
    """Retorna as datas da semana atual (segunda a domingo)"""
    today = date.today()
    start_week = today - timedelta(days=today.weekday())
    return [(start_week + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(today.weekday() + 1)]

CORES = 2
TODAY = date.today().strftime("%Y-%m-%d")
WEEK_DAYS = _get_week_dates()
BASE_PATH = f"/opt/spark/datalake/rennes_traffic_data"
INTERVAL_BATCH = 600

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


class RennesTrafficBatch:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RennesTrafficBatch") \
            .master("spark://spark-master:7077") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.cores.max", "2")\
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession for batch processing started successfully.")
        
        self.df_all = None
        self.df_today = None
        self.df_week = None
        
    def read_data(self):
        """Read data for batch processing"""
        logger.info("Reading data for batch processing...")
        
        try:
            self.df_all = self.spark.read.parquet(BASE_PATH)
            self.df_today = self.spark.read.parquet(f"{BASE_PATH}/date_partition={TODAY}")
            self.df_week = self.spark.read.parquet(*[f"{BASE_PATH}/date_partition={day}" for day in WEEK_DAYS])
            
        except Exception as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.warning("The directory datalake is not exists yet. Waiting 30 seconds for the data streaming ...")
            else:
                logger.error(f"Error reading data: {e}")
            time.sleep(30)
                
            return False
        
        logger.info("Data read successfully for batch processing.")
        return True
        
    def process_data(self):
        """
        Process data for batch layer
        """
        logger.info("Starting batch data processing...")

        df_daily_avg = self.df_today.groupBy("street_name") \
            .agg(
                F.round(F.avg("current_speed"), 2).alias("avg_speed_today"),
                F.max("speed_limit").alias("speed_limit")
            ) \
            .withColumn("record_date", F.lit(TODAY).cast("date"))
            
        df_hourly_avg = self.df_today.withColumn("hour", F.hour("timestamp_parsed")) \
            .groupBy("street_name", "hour") \
            .agg(F.round(F.avg("current_speed"), 2).alias("avg_speed_hour")) \
            .withColumn("record_date", F.lit(TODAY).cast("date"))
            
        df_weekly_ranking = self.df_week.filter(F.col("status_color") == "RED") \
            .groupBy("street_name") \
            .agg(F.count("*").alias("red_flag_count")) \
            .orderBy(F.desc("red_flag_count"))
            
        df_geo_zones = self.df_today \
            .withColumn("zone_lat", F.round(F.col("lat"), 2)) \
            .withColumn("zone_lon", F.round(F.col("lon"), 2)) \
            .groupBy("zone_lat", "zone_lon") \
            .agg(
                F.round(F.avg("current_speed"), 2).alias("zone_avg_speed"),
                F.countDistinct("street_name").alias("streets_in_zone")
            ) \
            .withColumn("record_date", F.lit(TODAY).cast("date"))
        
        targets = {
            "traffic_daily": df_daily_avg,
            "traffic_hourly": df_hourly_avg,
            "traffic_weekly_ranking": df_weekly_ranking,
            "traffic_zones": df_geo_zones
        }
        
        logger.info("Batch data processing completed.")
        
        return targets
        
    def write_to_cassandra(self, df, table: str):
        """
        Write DataFrame to Cassandra table
        """
        logger.info(f"Writing DataFrame to Cassandra table: {table}")
        
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(keyspace="traffic_keyspace", table=table) \
                .mode("append") \
                .save()
                
            logger.info("DataFrame written to Cassandra successfully.")
            
        except Exception as e:
            logger.error(f"Error writing to Cassandra: {e}")
            
    def stop(self):
        """Stop Spark session"""
        
        logger.info("Stopping SparkSession...")
        
        self.spark.stop()
        
        logger.info("SparkSession stopped.")
            
def main():
    batch_layer = RennesTrafficBatch()
    
    try:
        while True:
            start_time = time.time()
            
            if not batch_layer.read_data():
                continue
            
            targets = batch_layer.process_data()
            
            for table, df in targets.items():
                batch_layer.write_to_cassandra(df, table)
                
            sleep_duration = max(0, INTERVAL_BATCH - (time.time() - start_time))
            
            logger.info(f"Batch processing cycle completed. Sleeping for {int(sleep_duration // 60)} minutes.")
            time.sleep(sleep_duration)
            
    except KeyboardInterrupt:
        logger.info("Batch processing interrupted by user.")
        
        batch_layer.stop()
        
        logger.info("Spark session stopped. Exiting application.")
        
if __name__ == "__main__":
    main()
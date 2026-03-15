import logging
import json
import requests
import time
from typing import List

# Kafka imports
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Constants
API_URL = 'https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/etat-du-trafic-en-temps-reel/records'
FETCH_INTERVAL = 180 
API_LIMIT = 100
KAFKA_TOPIC = 'rennes_traffic'
KAFKA_BROKER = 'kafka:9092'
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1
API_CLE = 'Apikey 227b312ce19e501c260e8bb53f681b9a5fd34b57bbeaf3ba6da53e4d'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class RennesTrafficProducer:
    """Producer class to fetch traffic data and send to Kafka"""
    
    def __init__(self):

        # Ensure Kafka topic exists before creating producer        
        self.ensure_kafka_topic()
        
        try:
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            
            logging.info(f"Connected to Kafka broker: {KAFKA_BROKER}")
        
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    
    def ensure_kafka_topic(self):
        """Create Kafka topic if it doesn't exist"""
        
        try:
            # Admin client to manage Kafka topics 
            admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
            
            # Define the topic to be created
            topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)]
            
            # Attempt to create the topic
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            
            logger.info(f"Kafka topic '{KAFKA_TOPIC}' created successfully")
            
        except TopicAlreadyExistsError:
            logger.info(f"Kafka topic '{KAFKA_TOPIC}' already exists")
            
        except Exception as e:
            logger.error(f"Failed to create Kafka topic: {e}")
            raise
        
    def fetch_traffic_data(self) -> List[dict]:
        """Fetch traffic data from API

        Returns:
            List[dict]: List of traffic records fetched from API, or None if an error occurred
        """
        all_records = []
        offset = 0
        
        while True:
            try:
                # Make API request to fetch traffic data
                response = requests.get(API_URL, params={'limit': API_LIMIT, 'offset': offset}, headers={'Authorization': API_CLE})

                # Check if the request was successful
                response.raise_for_status()
                
                # Parse JSON response and extract records
                data = response.json()
                records = data.get('results', [])
                
                # Condition to break loop if no more records are returned by the API
                if not records:
                    logger.warning("No records found in API response")
                    break
                
                # Append fetched records to the cumulative list
                all_records.extend(records)
                logger.info(f"Fetched {len(records)} records from API (offset: {offset}) | Total fetched: {len(all_records)}")
                
                # Increment offset for next batch of records    
                offset += API_LIMIT
                
                # Sleep briefly to avoid overwhelming the API with requests
                time.sleep(.5)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data from API: {e}")
                break
            
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                break
            
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break

        if not all_records:
            logger.warning("No records could be retrieved from the API")
            return None
        
        logger.info(f"Successfully finished fetching. Grand total: {len(all_records)} records")
        return all_records
        
    def send_to_kafka(self, records: List[dict]) -> int:
        """Send records to Kafka topic

        Args:
            records (List[dict]): List of traffic records to be sent to Kafka

        Returns:
            int: Number of records successfully sent to Kafka
        """
        
        # Counter to track number of records successfully sent to Kafka
        sent_count = 0
        
        for record in records:
            
            try:
                self.producer.send(KAFKA_TOPIC, value=record)
                sent_count += 1
                
            except KafkaError as e:
                logger.error(f"Failed to send record to Kafka: {e}")
                
            except Exception as e:
                logger.error(f"Unexpected error while sending to Kafka: {e}")
            
        return sent_count
            
    def close(self):
        """Close the Kafka producer connection"""
        
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
            
def main():
    """Main function to run the producer"""
    
    try:
        # Initialize the producer
        producer = RennesTrafficProducer()
        
        logger.info("Starting Rennes Traffic Producer")
        logger.info(f"Kafka Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}, API URL: {API_URL}")
        logger.info(f"Fetch Interval: {FETCH_INTERVAL} seconds")
        
        # Main loop to continuously fetch data and send to Kafka
        while True:
            
            # Fetch traffic data from API
            records = producer.fetch_traffic_data()
            
            # Check if records were fetched successfully before attempting to send to Kafka
            if records:
                producer.send_to_kafka(records)
                logger.info(f"Sent {len(records)} records to Kafka topic '{KAFKA_TOPIC}'")
            else:
                logger.warning("No data to send to Kafka")
                break
                
            # Wait for the specified interval before fetching data again
            logger.info(f"Waiting {FETCH_INTERVAL // 60} minutes for next fetch...")
            time.sleep(FETCH_INTERVAL)
    
                
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user. Shutting down."
                    )
    except Exception as e:
        logger.error(f"Unexpected error in producer: {e}")
        
    finally:
        if producer:
            producer.close()
            
if __name__ == "__main__":
    main()
    
        
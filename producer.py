#!/usr/bin/env python3
"""
Rennes Traffic Data Producer for Lambda Architecture
Fetches traffic data from Rennes Metro API and sends to Kafka
Feeds both Speed Layer and Batch Layer
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


# Configuration from environment variables
# Port 29092 is the external port for connections from outside Docker
# Port 9092 is only for internal container-to-container communication
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'rennes_traffic')
API_URL = 'https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/etat-du-trafic-en-temps-reel/records'
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '180'))  # 3 minutes in seconds
API_LIMIT = int(os.getenv('API_LIMIT', '100'))

# Setup logging with simple format
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class RennesTrafficProducer:
    """Producer class to fetch traffic data and send to Kafka"""
    
    def __init__(self):
        """Initialize Kafka producer with UTF-8 encoding"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry on failure
                max_in_flight_requests_per_connection=1  # Maintain order
            )
            logger.info(f"Connected to Kafka broker: {KAFKA_BROKER}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def fetch_traffic_data(self) -> Optional[List[Dict]]:
        """
        Fetch traffic data from Rennes Metro API
        Returns list of records or None if failed
        """
        try:
            params = {'limit': API_LIMIT}
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('results', [])
            
            if not records:
                logger.warning("No records found in API response")
                return None
            
            logger.info(f"Fetched {len(records)} records from API")
            return records
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            return None
    
    def clean_and_transform_record(self, record: Dict) -> Optional[Dict]:
        """
        Clean and transform a single record to flat JSON schema
        Returns transformed record or None if invalid
        """
        try:
            # Extract coordinates safely
            geo_point = record.get('geo_point_2d', {})
            lat = geo_point.get('lat')
            lon = geo_point.get('lon')
            
            # Skip records without coordinates
            if lat is None or lon is None:
                return None
            
            # Create flat record with required schema
            transformed_record = {
                'timestamp': datetime.now().isoformat(),
                'street_name': record.get('denomination', 'Unknown Street'),
                'current_speed': record.get('averagevehiclespeed'),
                'speed_limit': record.get('vitesse_maxi'),
                'lat': lat,
                'lon': lon,
                'status': record.get('trafficstatus', 'unknown')
            }
            
            # Skip records with missing speed data
            if transformed_record['current_speed'] is None:
                return None
            
            return transformed_record
            
        except Exception as e:
            logger.warning(f"Failed to transform record: {e}")
            return None
    
    def send_records_to_kafka(self, records: List[Dict]) -> int:
        """
        Send cleaned records to Kafka topic
        Returns number of successfully sent records
        """
        sent_count = 0
        
        for record in records:
            # Clean and transform each record
            clean_record = self.clean_and_transform_record(record)
            
            if clean_record is None:
                continue
            
            try:
                # Send to Kafka with street name as key for partitioning
                street_key = clean_record['street_name']
                future = self.producer.send(
                    KAFKA_TOPIC, 
                    key=street_key,
                    value=clean_record
                )
                
                # Wait for send to complete (blocking)
                future.get(timeout=10)
                sent_count += 1
                
            except KafkaError as e:
                logger.error(f"Failed to send record to Kafka: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sending record: {e}")
        
        return sent_count
    
    def run_producer_loop(self):
        """
        Main producer loop - fetch data and send to Kafka every 3 minutes
        Runs indefinitely with error handling
        """
        logger.info(f"Starting Rennes Traffic Producer")
        logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
        logger.info(f"Fetch Interval: {FETCH_INTERVAL} seconds")
        
        while True:
            try:
                # Fetch traffic data from API
                records = self.fetch_traffic_data()
                
                if records:
                    # Send records to Kafka
                    sent_count = self.send_records_to_kafka(records)
                    logger.info(f"Sent {sent_count} records to Kafka topic '{KAFKA_TOPIC}'")
                else:
                    logger.warning("No data to send to Kafka")
                
                # Flush producer to ensure all messages are sent
                self.producer.flush()
                
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                logger.info("Continuing with next cycle...")
            
            # Wait for next fetch cycle
            logger.info(f"Waiting {FETCH_INTERVAL} seconds for next fetch...")
            time.sleep(FETCH_INTERVAL)
    
    def close(self):
        """Clean shutdown of producer"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


def main():
    """Main function to start the producer"""
    producer = None
    
    try:
        producer = RennesTrafficProducer()
        producer.run_producer_loop()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal (Ctrl+C)")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
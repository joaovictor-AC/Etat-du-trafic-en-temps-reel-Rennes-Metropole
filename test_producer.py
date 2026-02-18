#!/usr/bin/env python3
"""
Simple test script for the Rennes Traffic Producer
Tests API connection and data transformation without Kafka
"""

import json
import requests
from datetime import datetime

API_URL = 'https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/etat-du-trafic-en-temps-reel/records'

def test_api_connection():
    """Test API connection and data structure"""
    print("[TEST] Testing API connection...")
    
    try:
        response = requests.get(API_URL, params={'limit': 5}, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        records = data.get('results', [])
        
        if records:
            print(f"[SUCCESS] Got {len(records)} records from API")
            return records
        else:
            print("[ERROR] No records in API response")
            return None
            
    except Exception as e:
        print(f"[ERROR] API test failed: {e}")
        return None

def test_data_transformation(records):
    """Test data cleaning and transformation"""
    print("[TEST] Testing data transformation...")
    
    transformed_count = 0
    
    for i, record in enumerate(records):
        # Test transformation logic
        geo_point = record.get('geo_point_2d', {})
        lat = geo_point.get('lat')
        lon = geo_point.get('lon')
        
        if lat is None or lon is None:
            print(f"[SKIP] Record {i}: Missing coordinates")
            continue
        
        transformed_record = {
            'timestamp': datetime.now().isoformat(),
            'street_name': record.get('denomination', 'Unknown Street'),
            'current_speed': record.get('averagevehiclespeed'),
            'speed_limit': record.get('vitesse_maxi'),
            'lat': lat,
            'lon': lon,
            'status': record.get('trafficstatus', 'unknown')
        }
        
        if transformed_record['current_speed'] is None:
            print(f"[SKIP] Record {i}: Missing speed data")
            continue
        
        print(f"[SUCCESS] Record {i}: {json.dumps(transformed_record, indent=2)}")
        transformed_count += 1
    
    print(f"[RESULT] Successfully transformed {transformed_count}/{len(records)} records")
    return transformed_count > 0

def main():
    """Run all tests"""
    print("=== Rennes Traffic Producer Test ===")
    
    # Test API connection
    records = test_api_connection()
    if not records:
        return
    
    # Test data transformation  
    success = test_data_transformation(records)
    
    if success:
        print("\n[SUCCESS] All tests passed!")
        print("[INFO] Producer is ready to run when Kafka is available")
    else:
        print("\n[ERROR] Tests failed!")

if __name__ == "__main__":
    main()
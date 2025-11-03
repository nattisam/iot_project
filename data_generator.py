"""
Data generator module for IoT sensor data collector.

This module simulates IoT sensor readings and inserts them into Cassandra.
It generates continuous sensor data for multiple devices at regular intervals
and uses prepared statements for efficient batch insertion.
"""

import random
import time
import signal
import sys
from datetime import datetime
from typing import Tuple, Optional

from db_connection import get_session, close_connection
from cassandra.query import SimpleStatement, PreparedStatement


# Configuration constants
DEVICE_IDS = ['device_1', 'device_2', 'device_3']
SENSOR_TYPES = ['temperature', 'humidity', 'motion']

# Sensor value ranges
SENSOR_RANGES = {
    'temperature': (20.0, 35.0),  # Celsius
    'humidity': (30.0, 90.0),     # Percentage
    'motion': (0, 1)              # Binary (0 = no motion, 1 = motion detected)
}

# Generation settings
SLEEP_INTERVAL_MIN = 2  # seconds
SLEEP_INTERVAL_MAX = 3  # seconds

# Global variables for graceful shutdown
running = True
session = None
cluster = None
prepared_insert = None


def generate_sensor_reading(device_id: str) -> Tuple[str, str, float]:
    """
    Generate a random sensor reading for a given device.
    
    Args:
        device_id (str): The device identifier.
    
    Returns:
        tuple: (device_id, sensor_type, sensor_value)
    """
    # Randomly select a sensor type
    sensor_type = random.choice(SENSOR_TYPES)
    
    # Generate sensor value based on type and range
    min_val, max_val = SENSOR_RANGES[sensor_type]
    
    if sensor_type == 'motion':
        # Binary value (0 or 1)
        sensor_value = random.randint(int(min_val), int(max_val))
    else:
        # Continuous value with 2 decimal places
        sensor_value = round(random.uniform(min_val, max_val), 2)
    
    return device_id, sensor_type, sensor_value


def insert_sensor_reading(session, prepared_statement: PreparedStatement, 
                         device_id: str, timestamp: datetime, 
                         sensor_type: str, sensor_value: float) -> bool:
    """
    Insert a sensor reading into the Cassandra table using a prepared statement.
    
    Prepared statements are more efficient than regular queries because:
    - The query is parsed once on the server
    - Only parameter values are sent on subsequent executions
    - Better performance for repeated insertions
    
    Args:
        session: The Cassandra session object.
        prepared_statement: The prepared statement for insertion.
        device_id (str): The device identifier.
        timestamp (datetime): The timestamp of the reading.
        sensor_type (str): The type of sensor.
        sensor_value (float): The sensor reading value.
    
    Returns:
        bool: True if insertion was successful, False otherwise.
    """
    if session is None or prepared_statement is None:
        return False
    
    try:
        # Execute the prepared statement with the parameters
        session.execute(prepared_statement, {
            'device_id': device_id,
            'timestamp': timestamp,
            'sensor_type': sensor_type,
            'sensor_value': sensor_value
        })
        return True
    except Exception as e:
        print(f"✗ Error inserting reading: {str(e)}")
        return False


def prepare_insert_statement(session, keyspace_name: str = 'iot_data', 
                            table_name: str = 'sensor_readings') -> Optional[PreparedStatement]:
    """
    Prepare an INSERT statement for efficient repeated executions.
    
    Prepared statements improve performance by:
    - Parsing the query once on the server
    - Caching the prepared query
    - Reducing network overhead for subsequent inserts
    
    Args:
        session: The Cassandra session object.
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table. Defaults to 'sensor_readings'.
    
    Returns:
        PreparedStatement: The prepared statement object, or None if preparation fails.
    """
    if session is None:
        return None
    
    try:
        insert_query = f"""
            INSERT INTO {keyspace_name}.{table_name} 
            (device_id, timestamp, sensor_type, sensor_value)
            VALUES (?, ?, ?, ?)
        """
        
        prepared_statement = session.prepare(insert_query)
        print(f"✓ Prepared INSERT statement for {keyspace_name}.{table_name}")
        return prepared_statement
        
    except Exception as e:
        print(f"✗ Failed to prepare INSERT statement: {str(e)}")
        return None


def signal_handler(signum, frame):
    """
    Handle interrupt signals (Ctrl+C) for graceful shutdown.
    
    Args:
        signum: Signal number.
        frame: Current stack frame.
    """
    global running, session, cluster
    
    print("\n" + "=" * 70)
    print("Interrupt received. Shutting down gracefully...")
    print("=" * 70)
    
    running = False
    
    if cluster:
        close_connection(cluster)


def generate_and_insert_data(session, prepared_statement: PreparedStatement):
    """
    Generate sensor readings for all devices and insert them into Cassandra.
    
    This function:
    1. Generates a reading for each device
    2. Inserts the data with current timestamp
    3. Logs the inserted data
    
    Args:
        session: The Cassandra session object.
        prepared_statement: The prepared statement for insertion.
    """
    timestamp = datetime.now()
    
    for device_id in DEVICE_IDS:
        # Generate sensor reading
        device_id, sensor_type, sensor_value = generate_sensor_reading(device_id)
        
        # Insert into database
        success = insert_sensor_reading(
            session, 
            prepared_statement, 
            device_id, 
            timestamp, 
            sensor_type, 
            sensor_value
        )
        
        if success:
            # Format sensor value for display
            if sensor_type == 'temperature':
                value_str = f"{sensor_value}°C"
            elif sensor_type == 'humidity':
                value_str = f"{sensor_value}%"
            else:  # motion
                value_str = "Motion" if sensor_value == 1 else "No motion"
            
            # Log the inserted data
            print(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"Device: {device_id} | "
                  f"Type: {sensor_type:12s} | "
                  f"Value: {value_str}")
        else:
            print(f"✗ Failed to insert reading for {device_id}")


def main():
    """
    Main function to run the IoT sensor data generator.
    
    This function:
    1. Connects to Cassandra
    2. Prepares the INSERT statement
    3. Continuously generates and inserts sensor data
    4. Runs until interrupted (Ctrl+C)
    """
    global running, session, cluster, prepared_insert
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 70)
    print("IoT Sensor Data Collector - Data Generator")
    print("=" * 70)
    print()
    print("Configuration:")
    print(f"  - Devices: {', '.join(DEVICE_IDS)}")
    print(f"  - Sensor types: {', '.join(SENSOR_TYPES)}")
    print(f"  - Sleep interval: {SLEEP_INTERVAL_MIN}-{SLEEP_INTERVAL_MAX} seconds")
    print(f"  - Temperature range: {SENSOR_RANGES['temperature'][0]}-{SENSOR_RANGES['temperature'][1]}°C")
    print(f"  - Humidity range: {SENSOR_RANGES['humidity'][0]}-{SENSOR_RANGES['humidity'][1]}%")
    print(f"  - Motion: Binary (0 or 1)")
    print()
    print("-" * 70)
    print("Connecting to Cassandra...")
    print("-" * 70)
    
    # Connect to database
    session, cluster = get_session()
    
    if session is None:
        print("✗ Failed to connect to Cassandra. Exiting.")
        sys.exit(1)
    
    # Prepare the INSERT statement for efficiency
    print()
    print("-" * 70)
    print("Preparing INSERT statement...")
    print("-" * 70)
    
    prepared_insert = prepare_insert_statement(session)
    
    if prepared_insert is None:
        print("✗ Failed to prepare INSERT statement. Closing connection.")
        close_connection(cluster)
        sys.exit(1)
    
    print()
    print("-" * 70)
    print("Starting data generation...")
    print("Press Ctrl+C to stop")
    print("-" * 70)
    print()
    
    reading_count = 0
    
    # Main loop: generate and insert data continuously
    try:
        while running:
            # Generate and insert sensor readings for all devices
            generate_and_insert_data(session, prepared_insert)
            reading_count += len(DEVICE_IDS)
            
            # Sleep for random interval between min and max seconds
            sleep_time = random.uniform(SLEEP_INTERVAL_MIN, SLEEP_INTERVAL_MAX)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        # Handle Ctrl+C if signal handler doesn't catch it
        signal_handler(None, None)
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        running = False
    
    finally:
        # Clean up resources
        print()
        print(f"Total readings inserted: {reading_count}")
        print("Closing connection...")
        
        if cluster:
            close_connection(cluster)
        
        print()
        print("=" * 70)
        print("Data generator stopped.")
        print("=" * 70)


if __name__ == "__main__":
    main()


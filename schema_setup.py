"""
Schema setup module for IoT sensor data collector.

This module defines and creates the Cassandra table structure optimized
for storing time-series IoT sensor readings. The schema design takes
advantage of Cassandra's strengths for time-series data storage and queries.
"""

from db_connection import get_session, close_connection


def table_exists(session, keyspace_name='iot_data', table_name='sensor_readings'):
    """
    Check if a table exists in the specified keyspace.
    
    Args:
        session: The Cassandra session object.
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table to check. Defaults to 'sensor_readings'.
    
    Returns:
        bool: True if the table exists, False otherwise.
    """
    if session is None:
        return False
    
    try:
        # Query the system schema to check for table existence
        query = """
            SELECT table_name 
            FROM system_schema.tables 
            WHERE keyspace_name = %s AND table_name = %s
        """
        result = session.execute(query, [keyspace_name, table_name])
        
        return result.one() is not None
        
    except Exception as e:
        print(f"✗ Error checking if table exists: {str(e)}")
        return False


def create_table(session, keyspace_name='iot_data', table_name='sensor_readings'):
    """
    Create the sensor_readings table if it doesn't already exist.
    
    This function creates a table optimized for time-series IoT sensor data:
    
    Schema Design Rationale:
    ------------------------
    1. Partition Key (device_id): 
       - Data is partitioned by device_id, meaning all readings for a single
         device are stored together on the same node/partition
       - This enables efficient queries for a specific device's historical data
       - Prevents hotspots by distributing data across multiple devices
    
    2. Clustering Key (timestamp):
       - Within each partition (device), data is sorted by timestamp
       - Enables efficient time-range queries (e.g., "get all readings from
         device X between time T1 and T2")
       - Data is automatically ordered chronologically, making time-series
         queries fast without additional sorting
    
    3. Why This Design Works for Time-Series Data:
       - Cassandra excels at write-heavy workloads: each sensor reading can
         be inserted independently and efficiently
       - Time-based queries are optimized: fetching data for a device in a
         time range requires reading from a single partition in sorted order
       - Horizontal scalability: as you add more devices, Cassandra
         automatically distributes the partitions across nodes
       - Append-only pattern: sensor data is typically written once and
         rarely updated, which aligns with Cassandra's write-optimized design
    
    4. Additional Fields:
       - sensor_type: Categorizes the type of sensor (temperature, humidity, etc.)
       - sensor_value: The actual numeric reading from the sensor
    
    Args:
        session: The Cassandra session object.
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table to create. Defaults to 'sensor_readings'.
    
    Returns:
        bool: True if table was created or already exists, False otherwise.
    """
    if session is None:
        print("✗ Cannot create table: No active session available")
        return False
    
    try:
        # Check if table already exists
        if table_exists(session, keyspace_name, table_name):
            print(f"✓ Table '{table_name}' already exists in keyspace '{keyspace_name}'")
            return True
        
        # Create table with composite primary key optimized for time-series queries
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
                device_id TEXT,
                timestamp TIMESTAMP,
                sensor_type TEXT,
                sensor_value DOUBLE,
                PRIMARY KEY (device_id, timestamp)
            )
            WITH CLUSTERING ORDER BY (timestamp DESC)
        """
        
        """
        PRIMARY KEY Explanation:
        - device_id is the PARTITION KEY: determines which node/partition stores the data
        - timestamp is the CLUSTERING KEY: determines the sort order within each partition
        
        CLUSTERING ORDER BY (timestamp DESC):
        - Orders records by timestamp in descending order (newest first) within each partition
        - This is optimal for time-series queries where you typically want recent data first
        - Can be changed to ASC for ascending order if needed for your use case
        """
        
        session.execute(create_table_query)
        print(f"✓ Successfully created table '{table_name}' in keyspace '{keyspace_name}'")
        print(f"  - Partition key: device_id")
        print(f"  - Clustering key: timestamp (DESC)")
        print(f"  - Optimized for time-series IoT sensor data queries")
        return True
        
    except Exception as e:
        print(f"✗ Failed to create table '{table_name}': {str(e)}")
        return False


def setup_schema(host='localhost', port=9042, keyspace_name='iot_data', table_name='sensor_readings'):
    """
    Complete schema setup: connect to database and create the table.
    
    This is a convenience function that handles the entire schema setup process:
    1. Connects to Cassandra
    2. Creates keyspace if needed (via db_connection.get_session)
    3. Creates the sensor_readings table
    
    Args:
        host (str): The host address of the Cassandra node. Defaults to 'localhost'.
        port (int): The port number for CQL communication. Defaults to 9042.
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table to create. Defaults to 'sensor_readings'.
    
    Returns:
        tuple: (session, cluster) if successful, (None, None) if setup fails.
    """
    # Step 1: Get database connection and create keyspace if needed
    session, cluster = get_session(host, port, keyspace_name)
    
    if session is None:
        return None, None
    
    # Step 2: Create the table
    table_created = create_table(session, keyspace_name, table_name)
    
    if not table_created:
        print("✗ Failed to set up schema. Closing connection.")
        close_connection(cluster)
        return None, None
    
    return session, cluster


# Example usage when running as a script
if __name__ == "__main__":
    print("=" * 70)
    print("IoT Sensor Data Collector - Schema Setup")
    print("=" * 70)
    print()
    print("Creating table: sensor_readings")
    print("Schema: device_id (partition key), timestamp (clustering key)")
    print("Purpose: Optimized for time-series IoT sensor data storage")
    print("-" * 70)
    print()
    
    # Set up the schema
    session, cluster = setup_schema()
    
    if session:
        print()
        print("=" * 70)
        print("✓ Schema setup completed successfully!")
        print("=" * 70)
        print()
        print("Table structure:")
        print("  - device_id: TEXT (partition key)")
        print("  - timestamp: TIMESTAMP (clustering key, DESC)")
        print("  - sensor_type: TEXT")
        print("  - sensor_value: DOUBLE")
        print()
        print("Query pattern examples:")
        print("  - Get all readings for device 'device_001'")
        print("  - Get readings for device 'device_001' in time range")
        print("  - Get latest N readings for a device")
        print()
        
        # Clean up
        close_connection(cluster)
    else:
        print()
        print("✗ Schema setup failed. Please check your Cassandra cluster.")
        print("Make sure Cassandra is running and accessible at localhost:9042")


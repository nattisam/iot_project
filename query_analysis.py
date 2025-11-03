"""
Query and analysis module for IoT sensor data collector.

This module provides functions to query, retrieve, and analyze sensor data
from Cassandra. It includes data fetching functions and uses Pandas for
data manipulation and display.
"""

import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime

from db_connection import get_session, close_connection
from cassandra.query import SimpleStatement, PreparedStatement


def get_recent_readings(session, device_id: str, limit: int = 10, 
                       keyspace_name: str = 'iot_data', 
                       table_name: str = 'sensor_readings') -> pd.DataFrame:
    """
    Retrieve the latest sensor readings for a specific device.
    
    This function queries the sensor_readings table for the most recent
    readings for a given device, ordered by timestamp in descending order
    (newest first). The table schema with device_id as partition key and
    timestamp as clustering key makes this query very efficient.
    
    Args:
        session: The Cassandra session object.
        device_id (str): The device identifier to query.
        limit (int): Maximum number of readings to retrieve. Defaults to 10.
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table. Defaults to 'sensor_readings'.
    
    Returns:
        pd.DataFrame: A DataFrame containing the readings with columns:
                     [device_id, timestamp, sensor_type, sensor_value]
                     Returns empty DataFrame if query fails or no data found.
    """
    if session is None:
        print("✗ Cannot query: No active session available")
        return pd.DataFrame()
    
    try:
        # Query for recent readings ordered by timestamp DESC
        # Since timestamp is a clustering key with DESC order, this query
        # efficiently retrieves the most recent data from the partition
        query = f"""
            SELECT device_id, timestamp, sensor_type, sensor_value
            FROM {keyspace_name}.{table_name}
            WHERE device_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        
        # Use prepared statement for efficiency
        prepared = session.prepare(query)
        result = session.execute(prepared, [device_id, limit])
        
        # Convert result to list of dictionaries
        rows = []
        for row in result:
            rows.append({
                'device_id': row.device_id,
                'timestamp': row.timestamp,
                'sensor_type': row.sensor_type,
                'sensor_value': row.sensor_value
            })
        
        # Create DataFrame from results
        if rows:
            df = pd.DataFrame(rows)
            # Format timestamp for better display
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        else:
            print(f"ℹ No readings found for device '{device_id}'")
            return pd.DataFrame(columns=['device_id', 'timestamp', 'sensor_type', 'sensor_value'])
        
    except Exception as e:
        print(f"✗ Error retrieving recent readings: {str(e)}")
        return pd.DataFrame()


def get_average_value(session, device_id: str, sensor_type: str,
                     keyspace_name: str = 'iot_data',
                     table_name: str = 'sensor_readings') -> Optional[float]:
    """
    Compute the average value for a specific sensor type on a given device.
    
    This function calculates the average (mean) of all sensor readings
    of a particular type for a specific device. Useful for analyzing
    long-term trends or baseline values.
    
    Args:
        session: The Cassandra session object.
        device_id (str): The device identifier to query.
        sensor_type (str): The sensor type (e.g., 'temperature', 'humidity', 'motion').
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table. Defaults to 'sensor_readings'.
    
    Returns:
        float: The average sensor value, or None if no data found or query fails.
    """
    if session is None:
        print("✗ Cannot query: No active session available")
        return None
    
    try:
        # Query all readings for the device and sensor type
        # Note: Cassandra doesn't support AVG() aggregate directly in WHERE clauses
        # So we fetch all relevant rows and compute average in Python
        query = f"""
            SELECT sensor_value
            FROM {keyspace_name}.{table_name}
            WHERE device_id = ? AND sensor_type = ?
        """
        
        prepared = session.prepare(query)
        result = session.execute(prepared, [device_id, sensor_type])
        
        # Extract values and compute average using Pandas
        values = [row.sensor_value for row in result]
        
        if not values:
            print(f"ℹ No readings found for device '{device_id}' with sensor type '{sensor_type}'")
            return None
        
        # Calculate average using Pandas for consistency
        df = pd.DataFrame({'sensor_value': values})
        average = df['sensor_value'].mean()
        
        return round(average, 2)  # Round to 2 decimal places
        
    except Exception as e:
        print(f"✗ Error computing average value: {str(e)}")
        return None


def get_all_devices(session, keyspace_name: str = 'iot_data',
                    table_name: str = 'sensor_readings') -> List[str]:
    """
    Retrieve a list of all unique device IDs in the database.
    
    This function queries the sensor_readings table to find all distinct
    device IDs that have submitted readings. Useful for discovering
    available devices in the system.
    
    Args:
        session: The Cassandra session object.
        keyspace_name (str): Name of the keyspace. Defaults to 'iot_data'.
        table_name (str): Name of the table. Defaults to 'sensor_readings'.
    
    Returns:
        list: A list of unique device ID strings, or empty list if query fails.
    """
    if session is None:
        print("✗ Cannot query: No active session available")
        return []
    
    try:
        # Query for distinct device IDs
        # Note: In Cassandra, DISTINCT can be used with partition keys
        query = f"""
            SELECT DISTINCT device_id
            FROM {keyspace_name}.{table_name}
        """
        
        result = session.execute(query)
        
        # Extract device IDs into a list
        device_ids = [row.device_id for row in result]
        
        return sorted(device_ids)  # Return sorted list for consistency
        
    except Exception as e:
        print(f"✗ Error retrieving device list: {str(e)}")
        return []


def display_recent_readings(session):
    """
    Interactive function to display recent readings for a device.
    
    Prompts user for device ID and number of readings to display,
    then shows the results in a formatted table using Pandas.
    
    Args:
        session: The Cassandra session object.
    """
    print("\n" + "=" * 70)
    print("View Recent Readings")
    print("=" * 70)
    
    # Get device ID
    device_id = input("Enter device ID (e.g., device_1): ").strip()
    if not device_id:
        print("✗ Device ID cannot be empty")
        return
    
    # Get limit
    try:
        limit_str = input("Enter number of readings to retrieve (default: 10): ").strip()
        limit = int(limit_str) if limit_str else 10
        if limit <= 0:
            print("✗ Limit must be a positive integer")
            return
    except ValueError:
        print("✗ Invalid limit value. Using default: 10")
        limit = 10
    
    # Query and display
    print(f"\nFetching latest {limit} readings for '{device_id}'...")
    df = get_recent_readings(session, device_id, limit)
    
    if not df.empty:
        print(f"\n{'Recent Readings':^70}")
        print("-" * 70)
        
        # Format display
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        
        # Format timestamp for better display
        df_display = df.copy()
        df_display['timestamp'] = df_display['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(df_display.to_string(index=False))
        print("-" * 70)
        print(f"Total readings: {len(df)}")
    else:
        print("No readings found.")


def display_averages(session):
    """
    Interactive function to display average values for a device and sensor type.
    
    Prompts user for device ID and sensor type, then calculates and displays
    the average value.
    
    Args:
        session: The Cassandra session object.
    """
    print("\n" + "=" * 70)
    print("View Average Sensor Values")
    print("=" * 70)
    
    # Get device ID
    device_id = input("Enter device ID (e.g., device_1): ").strip()
    if not device_id:
        print("✗ Device ID cannot be empty")
        return
    
    # Get sensor type
    print("\nAvailable sensor types: temperature, humidity, motion")
    sensor_type = input("Enter sensor type: ").strip().lower()
    if not sensor_type:
        print("✗ Sensor type cannot be empty")
        return
    
    # Calculate and display average
    print(f"\nCalculating average {sensor_type} for '{device_id}'...")
    average = get_average_value(session, device_id, sensor_type)
    
    if average is not None:
        print("\n" + "-" * 70)
        print(f"Device: {device_id}")
        print(f"Sensor Type: {sensor_type}")
        
        # Format value based on sensor type
        if sensor_type == 'temperature':
            print(f"Average Value: {average}°C")
        elif sensor_type == 'humidity':
            print(f"Average Value: {average}%")
        else:
            print(f"Average Value: {average}")
        
        print("-" * 70)
    else:
        print("No data available for calculation.")


def display_all_devices(session):
    """
    Display a list of all devices in the database.
    
    Retrieves and displays all unique device IDs found in the sensor_readings table.
    
    Args:
        session: The Cassandra session object.
    """
    print("\n" + "=" * 70)
    print("All Devices in Database")
    print("=" * 70)
    
    print("\nFetching device list...")
    devices = get_all_devices(session)
    
    if devices:
        print(f"\nFound {len(devices)} device(s):")
        print("-" * 70)
        for i, device_id in enumerate(devices, 1):
            print(f"  {i}. {device_id}")
        print("-" * 70)
    else:
        print("\nNo devices found in the database.")


def show_menu():
    """
    Display the main menu options.
    """
    print("\n" + "=" * 70)
    print("IoT Sensor Data - Query & Analysis Menu")
    print("=" * 70)
    print("\nOptions:")
    print("  1. View recent readings for a device")
    print("  2. View average sensor value for a device")
    print("  3. List all devices")
    print("  4. Exit")
    print("-" * 70)


def run_cli():
    """
    Main CLI interface for query and analysis operations.
    
    Provides an interactive menu-driven interface to query and analyze
    sensor data from Cassandra. The interface loops until the user
    chooses to exit.
    """
    print("=" * 70)
    print("IoT Sensor Data Collector - Query & Analysis")
    print("=" * 70)
    print("\nConnecting to Cassandra...")
    
    # Connect to database
    session, cluster = get_session()
    
    if session is None:
        print("✗ Failed to connect to Cassandra. Exiting.")
        return
    
    print("\n✓ Connected successfully!")
    
    # Main menu loop
    while True:
        show_menu()
        
        try:
            choice = input("\nSelect an option (1-4): ").strip()
            
            if choice == '1':
                display_recent_readings(session)
            elif choice == '2':
                display_averages(session)
            elif choice == '3':
                display_all_devices(session)
            elif choice == '4':
                print("\n" + "=" * 70)
                print("Exiting...")
                print("=" * 70)
                break
            else:
                print("\n✗ Invalid option. Please select 1-4.")
                
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            break
        except Exception as e:
            print(f"\n✗ Error: {str(e)}")
    
    # Clean up
    if cluster:
        close_connection(cluster)


def main():
    """
    Main entry point for the query analysis module.
    """
    run_cli()


if __name__ == "__main__":
    main()


"""
Database connection module for IoT sensor data collector.

This module provides functionality to connect to Apache Cassandra,
create the necessary keyspace if it doesn't exist, and handle
connection errors gracefully.
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


def create_connection(host='localhost', port=9042):
    """
    Create and return a connection to a Cassandra cluster.
    
    Args:
        host (str): The host address of the Cassandra node. Defaults to 'localhost'.
        port (int): The port number for CQL communication. Defaults to 9042.
    
    Returns:
        tuple: A tuple containing (session, cluster) objects if successful,
               (None, None) if connection fails.
    """
    cluster = None
    session = None
    
    try:
        # Create cluster connection
        cluster = Cluster([host], port=port)
        
        # Create session
        session = cluster.connect()
        
        print(f"✓ Successfully connected to Cassandra cluster at {host}:{port}")
        return session, cluster
        
    except Exception as e:
        print(f"✗ Failed to connect to Cassandra cluster: {str(e)}")
        if cluster:
            cluster.shutdown()
        return None, None


def create_keyspace_if_not_exists(session, keyspace_name='iot_data', replication_factor=1):
    """
    Create a keyspace in Cassandra if it doesn't already exist.
    
    This function uses SimpleStrategy for replication, which is suitable
    for single-datacenter deployments.
    
    Args:
        session: The Cassandra session object.
        keyspace_name (str): Name of the keyspace to create. Defaults to 'iot_data'.
        replication_factor (int): Number of replicas for data. Defaults to 1.
    
    Returns:
        bool: True if keyspace was created or already exists, False otherwise.
    """
    if session is None:
        print("✗ Cannot create keyspace: No active session available")
        return False
    
    try:
        # Check if keyspace already exists
        # Query the system keyspace to check for existing keyspaces
        query = """
            SELECT keyspace_name 
            FROM system_schema.keyspaces 
            WHERE keyspace_name = %s
        """
        result = session.execute(query, [keyspace_name])
        
        if result.one():
            print(f"✓ Keyspace '{keyspace_name}' already exists")
            return True
        
        # Create keyspace if it doesn't exist
        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': {replication_factor}
            }}
        """
        
        session.execute(create_keyspace_query)
        print(f"✓ Successfully created keyspace '{keyspace_name}' with replication factor {replication_factor}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to create keyspace '{keyspace_name}': {str(e)}")
        return False


def get_session(host='localhost', port=9042, keyspace_name='iot_data', replication_factor=1):
    """
    Main function to establish connection and set up keyspace.
    
    This is the primary entry point for database setup. It connects to Cassandra,
    creates the keyspace if needed, and returns a session object ready for use.
    
    Args:
        host (str): The host address of the Cassandra node. Defaults to 'localhost'.
        port (int): The port number for CQL communication. Defaults to 9042.
        keyspace_name (str): Name of the keyspace to create/use. Defaults to 'iot_data'.
        replication_factor (int): Number of replicas for data. Defaults to 1.
    
    Returns:
        tuple: A tuple containing (session, cluster) objects if successful,
               (None, None) if setup fails.
    """
    # Step 1: Create connection
    session, cluster = create_connection(host, port)
    
    if session is None:
        return None, None
    
    # Step 2: Create keyspace if it doesn't exist
    keyspace_created = create_keyspace_if_not_exists(session, keyspace_name, replication_factor)
    
    if not keyspace_created:
        print("✗ Failed to set up keyspace. Closing connection.")
        cluster.shutdown()
        return None, None
    
    # Step 3: Set the keyspace as the default for this session
    try:
        session.set_keyspace(keyspace_name)
        print(f"✓ Using keyspace '{keyspace_name}'")
    except Exception as e:
        print(f"✗ Failed to set keyspace '{keyspace_name}': {str(e)}")
        cluster.shutdown()
        return None, None
    
    return session, cluster


def close_connection(cluster):
    """
    Properly close the Cassandra cluster connection.
    
    Args:
        cluster: The Cassandra cluster object to close.
    """
    if cluster:
        try:
            cluster.shutdown()
            print("✓ Connection closed successfully")
        except Exception as e:
            print(f"✗ Error closing connection: {str(e)}")


# Example usage when running as a script
if __name__ == "__main__":
    print("=" * 50)
    print("IoT Sensor Data Collector - Database Setup")
    print("=" * 50)
    
    # Connect and set up keyspace
    session, cluster = get_session()
    
    if session:
        print("\n✓ Database setup completed successfully!")
        print("You can now use the 'session' object to execute CQL queries.")
        
        # Clean up
        close_connection(cluster)
    else:
        print("\n✗ Database setup failed. Please check your Cassandra cluster.")
        print("Make sure Cassandra is running and accessible at localhost:9042")


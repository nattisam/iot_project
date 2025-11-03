"""
Streamlit Dashboard for IoT Sensor Data Visualization.

This dashboard provides real-time visualization of sensor data stored in Cassandra.
It demonstrates how Cassandra's distributed architecture and write-optimized design
make it ideal for IoT systems with high data ingestion rates and real-time dashboards.

Why Cassandra Works Well for IoT Dashboards:
--------------------------------------------
1. Write-Optimized: Cassandra excels at high write throughput, handling thousands
   of sensor readings per second across multiple devices without performance degradation.

2. Horizontal Scalability: As the number of IoT devices grows, Cassandra can scale
   horizontally by adding nodes. Each device's data is partitioned independently,
   allowing linear scaling without redesigning the schema.

3. Fast Reads for Time-Series: The partition key (device_id) + clustering key
   (timestamp) design allows efficient queries for a specific device's recent data,
   which is exactly what dashboards need.

4. Distributed Architecture: Data is automatically distributed across cluster nodes,
   ensuring high availability and fault tolerance - critical for production IoT systems.

5. Real-Time Queries: Prepared statements and efficient partitioning enable fast
   query responses even with millions of readings, supporting live-updating dashboards.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

from db_connection import get_session, close_connection
from query_analysis import get_recent_readings, get_average_value, get_all_devices


# Page configuration
st.set_page_config(
    page_title="IoT Sensor Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1E88E5;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db_session():
    """
    Get a cached database session.
    
    Using Streamlit's cache_resource ensures the session is reused
    across reruns, improving performance and connection management.
    
    Returns:
        tuple: (session, cluster) objects, or (None, None) if connection fails.
    """
    session, cluster = get_session()
    return session, cluster


def filter_readings_by_sensor_type(df: pd.DataFrame, sensor_type: str) -> pd.DataFrame:
    """
    Filter readings DataFrame by sensor type.
    
    Args:
        df: DataFrame containing sensor readings.
        sensor_type: The sensor type to filter by.
    
    Returns:
        Filtered DataFrame with only the specified sensor type.
    """
    if df.empty:
        return df
    
    filtered = df[df['sensor_type'] == sensor_type].copy()
    # Sort by timestamp ascending for proper time-series visualization
    filtered = filtered.sort_values('timestamp', ascending=True)
    return filtered


def format_sensor_value(value: float, sensor_type: str) -> str:
    """
    Format sensor value with appropriate unit.
    
    Args:
        value: The sensor value.
        sensor_type: The type of sensor.
    
    Returns:
        Formatted string with unit.
    """
    if sensor_type == 'temperature':
        return f"{value:.2f}°C"
    elif sensor_type == 'humidity':
        return f"{value:.2f}%"
    elif sensor_type == 'motion':
        return "Motion Detected" if value == 1 else "No Motion"
    else:
        return f"{value:.2f}"


def main():
    """
    Main dashboard application function.
    """
    # Header
    st.markdown('<h1 class="main-header">📊 IoT Sensor Data Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_interval = st.slider("Refresh interval (seconds)", min_value=5, max_value=10, value=5)
        
        st.markdown("---")
        st.markdown("### About")
        st.info("""
        This dashboard visualizes real-time IoT sensor data from Cassandra.
        
        **Cassandra Scalability Benefits:**
        - Handles high write throughput
        - Horizontal scaling for growth
        - Fast time-series queries
        - Distributed architecture
        """)
    
    # Connect to database
    session, cluster = get_db_session()
    
    if session is None:
        st.error("❌ Failed to connect to Cassandra. Please ensure Cassandra is running on localhost:9042")
        st.stop()
    
    # Get available devices
    devices = get_all_devices(session)
    
    if not devices:
        st.warning("⚠️ No devices found in the database. Please run the data generator first.")
        st.info("Run `python data_generator.py` to start generating sensor data.")
        if cluster:
            close_connection(cluster)
        st.stop()
    
    # Device and sensor type selection
    col1, col2 = st.columns(2)
    
    with col1:
        selected_device = st.selectbox(
            "📱 Select Device",
            options=devices,
            index=0 if devices else None
        )
    
    with col2:
        # Sensor types that are good for line charts (excluding motion which is binary)
        sensor_types = ['temperature', 'humidity']
        selected_sensor_type = st.selectbox(
            "🔌 Select Sensor Type",
            options=sensor_types,
            index=0
        )
    
    st.markdown("---")
    
    # Main content area
    if selected_device:
        # Fetch recent readings (get more for better visualization)
        num_readings = st.slider("Number of readings to display", min_value=10, max_value=100, value=50)
        
        # Fetch data
        with st.spinner(f"Fetching data for {selected_device}..."):
            df = get_recent_readings(session, selected_device, limit=num_readings)
        
        if not df.empty:
            # Filter by sensor type
            filtered_df = filter_readings_by_sensor_type(df, selected_sensor_type)
            
            if not filtered_df.empty:
                # Metrics row
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    latest_value = filtered_df.iloc[-1]['sensor_value']
                    st.metric(
                        label="📈 Latest Reading",
                        value=format_sensor_value(latest_value, selected_sensor_type)
                    )
                
                with col2:
                    average = get_average_value(session, selected_device, selected_sensor_type)
                    if average is not None:
                        st.metric(
                            label="📊 Average Value",
                            value=format_sensor_value(average, selected_sensor_type)
                        )
                    else:
                        st.metric(label="📊 Average Value", value="N/A")
                
                with col3:
                    min_value = filtered_df['sensor_value'].min()
                    st.metric(
                        label="📉 Minimum",
                        value=format_sensor_value(min_value, selected_sensor_type)
                    )
                
                with col4:
                    max_value = filtered_df['sensor_value'].max()
                    st.metric(
                        label="📈 Maximum",
                        value=format_sensor_value(max_value, selected_sensor_type)
                    )
                
                st.markdown("---")
                
                # Line chart
                st.subheader(f"📈 {selected_sensor_type.title()} Trend Over Time")
                
                # Create Plotly line chart
                fig = px.line(
                    filtered_df,
                    x='timestamp',
                    y='sensor_value',
                    title=f"{selected_sensor_type.title()} Readings for {selected_device}",
                    labels={
                        'timestamp': 'Time',
                        'sensor_value': f'{selected_sensor_type.title()} Value'
                    },
                    markers=True
                )
                
                # Customize chart
                fig.update_layout(
                    hovermode='x unified',
                    height=500,
                    xaxis_title="Time",
                    yaxis_title=f"{selected_sensor_type.title()} Value",
                    template="plotly_white"
                )
                
                # Add average line
                if average is not None:
                    fig.add_hline(
                        y=average,
                        line_dash="dash",
                        line_color="red",
                        annotation_text=f"Average: {format_sensor_value(average, selected_sensor_type)}",
                        annotation_position="right"
                    )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                with st.expander("📋 View Raw Data"):
                    display_df = filtered_df.copy()
                    display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    display_df['sensor_value'] = display_df['sensor_value'].apply(
                        lambda x: format_sensor_value(x, selected_sensor_type)
                    )
                    display_df = display_df.rename(columns={
                        'device_id': 'Device ID',
                        'timestamp': 'Timestamp',
                        'sensor_type': 'Sensor Type',
                        'sensor_value': 'Value'
                    })
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                
            else:
                st.warning(f"⚠️ No {selected_sensor_type} readings found for {selected_device}")
                st.info("Try selecting a different sensor type or wait for data to be generated.")
        else:
            st.warning(f"⚠️ No readings found for {selected_device}")
            st.info("Run `python data_generator.py` to start generating sensor data.")
    
    # Auto-refresh functionality
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()
    
    # Manual refresh button
    if st.button("🔄 Refresh Now"):
        st.rerun()
    
    # Footer with connection info
    st.markdown("---")
    with st.expander("ℹ️ Connection Information"):
        st.write(f"**Connected to:** localhost:9042")
        st.write(f"**Keyspace:** iot_data")
        st.write(f"**Table:** sensor_readings")
        st.write(f"**Devices available:** {len(devices)}")
        st.write(f"**Auto-refresh:** {'Enabled' if auto_refresh else 'Disabled'}")
        if auto_refresh:
            st.write(f"**Refresh interval:** {refresh_interval} seconds")


if __name__ == "__main__":
    main()


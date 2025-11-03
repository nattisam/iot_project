# IoT Sensor Data Collector

A Python-based IoT sensor data collection and visualization system using Apache Cassandra for time-series data storage.

## 📋 Overview

This project demonstrates how to collect, store, query, and visualize IoT sensor data using Apache Cassandra. It includes modules for database connection, schema setup, data generation, querying, and a real-time dashboard.

## ✨ Features

- **Database Connection**: Automated Cassandra connection and keyspace creation
- **Schema Setup**: Optimized time-series table structure for sensor data
- **Data Generation**: Simulates continuous IoT sensor readings (temperature, humidity, motion)
- **Query & Analysis**: Functions to retrieve and analyze sensor data
- **Real-Time Dashboard**: Streamlit dashboard with live-updating charts and metrics

## 🛠️ Tech Stack

- **Python 3.x**
- **Apache Cassandra** - NoSQL database for time-series data
- **Streamlit** - Web dashboard framework
- **Pandas** - Data manipulation and analysis
- **Plotly** - Interactive charts and visualizations

## 📦 Installation

### Prerequisites

1. **Java 8+** (required for Cassandra)
2. **Python 3.7+**
3. **Apache Cassandra** (see `CASSANDRA_SETUP.md` for installation guide)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/nattisam/iot_project.git
   cd iot_project
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Cassandra:
   ```bash
   # Navigate to Cassandra bin directory
   cd C:\cassandra\apache-cassandra-4.1.0\bin
   cassandra.bat
   ```

## 🚀 Usage

### 1. Set up Database Schema
```bash
python schema_setup.py
```

### 2. Generate Sensor Data (Run in separate terminal)
```bash
python data_generator.py
```

### 3. Query and Analyze Data
```bash
python query_analysis.py
```

### 4. Launch Dashboard
```bash
streamlit run dashboard.py
```

The dashboard will open at `http://localhost:8501`

## 📁 Project Structure

```
iot_project/
├── db_connection.py      # Cassandra connection and keyspace setup
├── schema_setup.py       # Table schema creation
├── data_generator.py     # Simulate IoT sensor readings
├── query_analysis.py    # Query and analysis functions
├── dashboard.py          # Streamlit dashboard
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## 📊 Database Schema

**Table:** `sensor_readings`
- `device_id` (TEXT) - Partition key
- `timestamp` (TIMESTAMP) - Clustering key (DESC)
- `sensor_type` (TEXT) - Type of sensor (temperature, humidity, motion)
- `sensor_value` (DOUBLE) - Sensor reading value

## 🔧 Key Modules

- **db_connection.py**: Handles Cassandra connection, keyspace creation
- **schema_setup.py**: Creates optimized time-series table structure
- **data_generator.py**: Generates continuous sensor readings with prepared statements
- **query_analysis.py**: Provides query functions (recent readings, averages, device list)
- **dashboard.py**: Real-time visualization dashboard with live charts

## 📝 Notes

- The schema is optimized for time-series IoT data with `device_id` as partition key and `timestamp` as clustering key
- Data generator uses prepared statements for efficient insertion
- Dashboard auto-refreshes every 5-10 seconds for live updates
- All modules include comprehensive error handling and logging

## 📄 License

This project is for educational and demonstration purposes.


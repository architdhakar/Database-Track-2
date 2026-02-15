# Adaptive Ingestion System 

> **Course Project:** CS 432 Databases (Assignment 1)  
> **Topic:** Adaptive Ingestion & Hybrid Backend Placement

## 📖 Overview
This project implements an **autonomous data ingestion engine** that dynamically routes incoming JSON records to the optimal storage backend (**MySQL** or **MongoDB**) based on data characteristics.

It features a **3-Stage Producer-Consumer Pipeline** capable of handling high-throughput data streams, detecting schema drift in real-time, and **automatically migrating data** between SQL and NoSQL stores when stability criteria change.

## ✨ Key Features
*   **Hybrid Storage**: Automatically splits a single record into Structured (SQL) and Semi-Structured (MongoDB) components.
*   **Adaptive Classification**: Uses heuristics (Frequency, Type Stability, Nesting, Uniqueness) to decide storage target.
*   **Schema Evolution**: Automatically `ALTERs` SQL tables to add new columns.
*   **Automated Migration**: If a field becomes "unstable" (e.g., changes type), the system **migrates existing data from SQL to MongoDB** and drops the SQL column to preserve integrity.
*   **Concurrency**: Multi-threaded architecture (Ingestor, Processor, Router) ensures ingestion never blocks processing.
*   **Zero Data Potential Loss**: Uses thread-safe Queues and Backpressure.

## 🏗 Architecture
The system follows a threaded pipeline architecture:
`Ingestion Thread` $\rightarrow$ `Raw Queue` $\rightarrow$ `Processing Thread` $\rightarrow$ `Write Queue` $\rightarrow$ `Router Thread`

See [architecture.txt](architecture.txt) for a diagram and [system_concepts.md](system_concepts.md) for detailed logic of each component.

##  Prerequisites
*   **Python 3.8+**
*   **MySQL Server** (Running locally or remotely)
*   **MongoDB** (Running locally or remotely)

##  Installation
1.  **Clone the repository**:
    ```bash
    git clone <repo-url>
    cd Database-Track-2
    ```

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment**:
    Create a `.env` file in the root directory:
    ```env
    # MongoDB
    MONGO_URI=mongodb://localhost:27017/
    MONGO_DB_NAME=adaptive_db

    # MySQL
    SQL_HOST=localhost
    SQL_PORT=3306
    SQL_USER=root
    SQL_PASSWORD=password
    SQL_DB_NAME=adaptive_db

    # Data Source (Simulation)
    STREAM_URL=http://127.0.0.1:8000/record
    ```

## 🚀 Quick Start

**Prerequisites:** MySQL and MongoDB must be running locally.

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Configure Environment
Create a `.env` file (or edit the existing one):
```env
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=adaptive_db
SQL_HOST=localhost
SQL_PORT=3306
SQL_USER=root
SQL_PASSWORD=your_password
SQL_DB_NAME=adaptive_db
```

### Step 3: Start Simulation Server
Open a **separate terminal** and run:
```bash
uvicorn simulation_code:app --reload --port 8000
```
Leave this running. You should see: `Uvicorn running on http://127.0.0.1:8000`

### Step 4: Run the Adaptive Engine
In the **original terminal**, run:
```bash
python3 main.py
```

The system will:
- ✓ Check if the simulation server is running
- ✓ Connect to MySQL and MongoDB
- ✓ Load previous metadata (if any)
- ✓ Start processing data streams

### Step 5: Interact with the System
Once you see `SYSTEM READY`, you can type commands in the prompt:

#### Available Commands

| Command | Description | Example |
|---------|-------------|---------|
| `status` | Shows system uptime, total records processed, and active field count | `>> status` |
| `stats <field>` | Displays detailed analytics for a specific field including frequency ratio, type stability, uniqueness, and detected type | `>> stats age` |
| `queue` | Shows the number of records currently waiting in the ingestion buffer | `>> queue` |
| `help` | Lists all available commands with brief descriptions | `>> help` |
| `exit` | Gracefully shuts down all worker threads and closes database connections | `>> exit` |

**Example Session:**
```bash
>> status
System Uptime: 45 seconds
Total Records Processed: 2150
Active Fields Tracked: 38

>> stats device_model
Stats for 'device_model': {'frequency_ratio': 0.92, 'type_stability': 'stable', 
'detected_type': 'str', 'is_nested': False, 'unique_ratio': 0.004, 'count': 1978}

>> queue
Current Queue Size: 12 records pending processing.

>> exit
Initiating shutdown...
```

**Note:** The system automatically adapts as data arrives. Watch for messages like:
- `[SQL Handler] Evolving Schema: Adding column 'field_name'`
- `[Router] MIGRATION: Field drifted from SQL to MongoDB`

## 🧠 Logic & Heuristics
*   **Nested Data** $\rightarrow$ MongoDB (Always)
*   **Unstable Types** (e.g., Int then String) $\rightarrow$ MongoDB (Always)
*   **Sparse Data** (Frequency < 80%) $\rightarrow$ MongoDB
*   **High Cardinality** (Unique Ratio = 1.0) $\rightarrow$ SQL (as `UNIQUE` column)
*   **Standard** $\rightarrow$ SQL

For a deep dive into the code logic, read [system_concepts.md](system_concepts.md).

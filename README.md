# Adaptive Ingestion System 🚀

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

## 🛠 Prerequisites
*   **Python 3.8+**
*   **MySQL Server** (Running locally or remotely)
*   **MongoDB** (Running locally or remotely)

## 📦 Installation
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

## 🚀 Usage

### 1. Start the Data Simulation Server
In a separate terminal, run the mock data generator:
```bash
uvicorn simulation_code:app --reload --port 8000
```

### 2. Run the Adaptive Engine
```bash
python main.py
```
*   The system will connect to the stream and databases.
*   You will see logs indicating "Schema Evolution" or "Migration Alert".

### 3. Interactive CLI
While the engine is running, you can type commands:
*   `status`: Shows current queue sizes and thread health.
*   `stats <field_name>`: Shows analysis metrics (e.g., `stats age`).
*   `queue`: Shows raw queue size.
*   `exit`: Gracefully shuts down the system.

## 🧠 Logic & Heuristics
*   **Nested Data** $\rightarrow$ MongoDB (Always)
*   **Unstable Types** (e.g., Int then String) $\rightarrow$ MongoDB (Always)
*   **Sparse Data** (Frequency < 80%) $\rightarrow$ MongoDB
*   **High Cardinality** (Unique Ratio = 1.0) $\rightarrow$ SQL (as `UNIQUE` column)
*   **Standard** $\rightarrow$ SQL

For a deep dive into the code logic, read [system_concepts.md](system_concepts.md).
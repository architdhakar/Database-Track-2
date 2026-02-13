# System Components & Logic

## 1. Core Architecture: Producer-Consumer Pipeline
**Concept**: *Concurrency & Decoupling*
The system is built on a **3-Stage Pipeline** using Python's `threading` and `queue` modules.
- **Stage 1 (Ingestion)**: IO-bound. Fetches data from the network. Decoupled from processing to prevent network lag from slowing down analysis.
- **Stage 2 (Processing)**: CPU-bound. Analyzes data structure and makes classification decisions.
- **Stage 3 (Routing)**: IO-bound. Writes to databases and handles complex migrations.
- **Buffers (Queues)**: Thread-safe FIFO queues (`queue.Queue`) connect the stages, handling backpressure (blocking if full) to prevent memory overflows.

## 2. Component Logic

### A. Normalizer (`core/normalizer.py`)
**Goal**: Data Sanitization & Standardization.
**Logic**:
- **Snake Case Handling**: Converts `camelCase` keys to `snake_case` (e.g., `deviceModel` -> `device_model`) to ensure SQL compatibility.
- **Value Cleaning**: Trims whitespace from strings to preventing "dirty read" duplication (e.g., `" London"` vs `"London"`).
- **Timestamping**: Adds `sys_ingested_at` to every record for lineage tracking.

### B. Analyzer (`core/analyzer.py`)
**Goal**: Schema Inference (Schema-on-Read).
**Logic**:
- **Type Detection**: Uses Python's dynamic `type()` inference.
- **Uniqueness Tracking**: Implementation of a **HyperLogLog-inspired** counter (capped set) to estimate cardinality. If a field has 1000 unique values in a batch of 1000, it calculates a `unique_ratio` of 1.0.
- **Nesting Detection**: Recursive check `isinstance(val, (dict, list))`.

### C. Classifier (`core/classifier.py`)
**Goal**: Decision Making (Heuristics).
**Logic**: A **Rule-Based System** determines the storage backend.
1.  **Mandatory Rules**: `username`, `timestamp` -> **BOTH** (for Joining).
2.  **Structural Rule**: `is_nested=True` -> **MongoDB** (JSON document store).
3.  **Stability Rule**: `types_count > 1` -> **MongoDB** (prevents SQL column type conflicts).
4.  **Sparsity Rule**: `frequency < 80%` -> **MongoDB** (avoids NULL-heavy SQL tables).
5.  **Optimization Rule**: `unique_ratio == 1.0` -> **SQL (UNIQUE/PRIMARY KEY)**.

### D. Router (`core/router.py`)
**Goal**: Execution & Schema Evolution.
**Logic**:
- **Schema Evolution**: Dynamically executes `ALTER TABLE` commands in MySQL when a new valid column is detected.
- **Migration Logic (The "Adaptive" Part)**:
    - **Drift Detection**: Compares `previous_decision` vs `new_decision`.
    - **Step 1 (Extraction)**: `SELECT ... FROM table` (Fetches existing SQL data).
    - **Step 2 (Load)**: `db.collection.update_many` (Upserts data into MongoDB).
    - **Step 3 (Cleanup)**: `ALTER TABLE ... DROP COLUMN` (Removes the column from SQL).
    - **Transactional Integrity**: While not a full distributed transaction (XA), the system performs operations sequentially to minimize data loss risk.

### E. Query Engine (`core/query_engine.py`)
**Goal**: Observability (Operating System Concept).
**Logic**:
- Acts as an **Interrupt Handler**.
- While the "Kernel" (Worker Threads) is busy processing, the Query Engine allows the "User Space" (CLI) to inspect the state (`queue.qsize()`, `analyzer.stats`) in real-time without stopping the processors.

## 3. Database Concepts Implemented
- **Hybrid Storage**: Using Relational (MySQL) for structured, analytical data and Document Store (MongoDB) for flexible, semi-structured data.
- **Polyglot Persistence**: Selecting the right tool for the right data shape.
- **Vertical Partitioning**: Splitting a single logical record (User Profile) across two physical backends based on attribute characteristics.

# Sentinel Spark-Based URL Processing

This project implements a Spark-based architecture for processing URLs for the Wayback Machine, as outlined in the provided architectural plan.

## Getting Started

### 1. Prerequisites

- **Python 3.8+**
- **Java 8 or higher** (required for Apache Spark)
- **Apache Spark 3.5+**: You'll need `spark-submit` available in your PATH.
  - Download Spark from the [official Apache Spark website](https://spark.apache.org/downloads.html).
  - Extract the archive and set `SPARK_HOME` environment variable.
  - Add `$SPARK_HOME/bin` to your system's PATH.
- **Docker** (for running TimescaleDB)

### 2. Environment Setup

1.  **Create a `.env` file** in the project root with your database credentials:

    ```
    POSTGRES_DB=sentinel_db
    POSTGRES_USER=sentinel_user
    POSTGRES_PASSWORD=sentinel_password

    # Spark settings (for reference, though spark-submit handles master in this setup)
    SPARK_MASTER=local[*]
    ```

2.  **Install Python dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

### 3. Database Setup (TimescaleDB)

Navigate to the `docker/postgres/` directory and start the database container:

```bash
cd docker/postgres/
docker-compose up -d
```

This will start a PostgreSQL with TimescaleDB instance accessible on `localhost:5434`.

### 4. Running the File Ingestion Pipeline

1.  **Start the file watcher** from the project root:

    ```bash
    python ingestion/file_watcher.py
    ```

    This script will monitor the `indexnow/data/` directory for new TSV files.

2.  **Create a dummy TSV file** (e.g., `sample.tsv`) in the `indexnow/data/` directory. For example:

    ```tsv
    url	title
    http://example.com/page1	Example Page 1
    http://example.org/page2	Example Page 2
    ```

    (Make sure to use a tab character between `url` and `title`, and between the URL and title for each row.)

    The `file_watcher.py` will detect the new file, move it to `data/staging/`, trigger the Spark job (`spark/jobs/url_processor.py`), and then move the processed file to `data/processed/`.

### 5. Project Structure

```
sentinel/
├── spark/
│   ├── jobs/
│   │   ├── url_processor.py
│   │   ├── feature_engineer.py
│   │   └── prioritizer.py
│   ├── utils/
│   │   ├── db_utils.py
│   │   └── spark_utils.py
│   └── config/
│       └── spark_config.py
├── ingestion/
│   ├── file_watcher.py
│   └── staging_manager.py
├── api/
│   ├── main.py
│   ├── models.py
│   ├── schemas.py
│   └── routers/
├── config/
│   ├── database.py
│   └── settings.py
├── data/
│   ├── raw/
│   ├── staging/
│   ├── processed/
│   └── exports/
├── docker/
│   ├── spark/
│   ├── postgres/
│   │   └── docker-compose.yml
│   └── api/
└── requirements.txt
└── README.md
└── .env (create this file manually)
```

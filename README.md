# Sentinel: URL Archival and Reputation System

Sentinel is a distributed URL processing and analysis system built with Apache Spark and Python. It assesses the archival potential and trustworthiness of URLs by analyzing content quality, outlinks, archival history, and request-level characteristics. The goal is to preserve valuable URLs while filtering out spam or low-quality ones.

## Features

-   **Distributed Processing**: Utilizes Apache Spark for the scalable handling of large URL batches, enabling efficient parallel processing.
-   **Dual Scoring Models**: Implements both classical heuristics and a LightGBM machine learning-based model, which can be run separately or together for comparative analysis.
-   **Feature Analysis**:
    -   **Content Signals**: Analyzes on-page text and metadata to evaluate quality, flagging trust indicators (e.g., structured data, privacy pages) and spam signals (e.g., keyword stuffing, suspicious scripts).
    -   **Outlink Classification**: Categorizes outbound links as quality, spam, internal, or external, and leverages an internal database to assess the reputation of linked domains.
    -   **Request Metadata**: Captures essential HTTP data, including final status codes after redirects, response latency, and the presence of security headers like HSTS and CSP.
    -   **Wayback Machine CDX History**: Queries the Internet Archive's CDX API to retrieve a URL's archival history, assessing its longevity, capture frequency, and historical success rate.
-   **Adaptive Domain Reputation**: Maintains a PostgreSQL-backed database that stores and updates scores for domain volatility, content diversity, and overall trust, feeding this intelligence back into the scoring models.
-   **"Good Data" Ingestion**: Optionally incorporates curated URL lists from trusted sources (Wikipedia, MediaCloud) to benchmark model accuracy and establish a high-quality baseline.
-   **Comprehensive Reporting**: Generates detailed batch summaries, model comparison reports, and system performance profiles for each run.
-   **Containerized Database**: Provides a Docker-based PostgreSQL setup, tuned for high-throughput data ingestion, ensuring a consistent and reproducible environment.

## Architecture

1.  **Input**: The pipeline is triggered when TSV files, each containing a list of URLs, are placed into the `data/input` directory.
2.  **File Watcher**: The `spark/file_watcher.py` script continuously monitors the input directory. Upon detecting a new file, it packages the project's Python modules and submits a Spark job.
3.  **Core Processing** (`spark_logic/process_urls.py`):
    *   Loads raw URLs from the input file and, if enabled, curated "good data" lists.
    *   Fetches existing domain reputation and diversity scores from PostgreSQL to inform the initial analysis.
    *   Applies one or both scoring models ('classical' and 'lightgbm') to generate a provisional score for each URL.
    *   (Optional) For a small, random sample of URLs, it performs a deep-dive analysis by making live web requests using modules like `use_content.py` and `use_outlinks.py`.
    *   Aggregates domain-level statistics from the current batch and updates the central domain reputation table in PostgreSQL.
    *   Rescores all URLs, incorporating the newly updated domain statistics to refine their scores.
    *   Writes the final outputs, which can include Parquet files, TSV files, entries in the PostgreSQL database, and a human-readable summary report.
4.  **Database**: The PostgreSQL database acts as the system's persistent memory, storing both the final URL scores and the aggregated domain reputation data.

## Scoring Models

Sentinel features two distinct scoring engines that provide different lenses through which to evaluate URLs.

### Classical Heuristics

This model employs a fast and transparent rule-based system. It calculates a score based on a weighted combination of features extracted from the URL's structure and lexical patterns. It is highly interpretable and serves as the system's baseline.

### LightGBM Machine Learning Model

This model uses a pre-trained LightGBM regressor to predict a URL's score based on over 40 engineered features. It excels at identifying complex, non-linear relationships in the data that a purely heuristic model might miss. The system also includes a training mode, allowing a new LightGBM model to be trained using the classical scores as ground-truth labels.

## Setup

### Prerequisites

-   Python 3.9+
-   Java JDK 8+
-   Docker + Docker Compose
-   `pip` and `yoyo-migrations`

### Database Setup

```bash
cd sentinel/docker
docker-compose up -d db
docker-compose ps
````

This launches a performance-tuned PostgreSQL 15-alpine container on port `5434`.

### Local Environment

```bash
cd sentinel
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Environment variables like `MEDIACLOUD_URL` are private and cannot be provided. Use `.env.example` as a reference but provide your own values.

Run migrations to set up the database schema:

```bash
yoyo apply
```

Download the PostgreSQL JDBC driver (e.g., `postgresql-42.7.7.jar`) and update its absolute path in `spark/config/spark_config.py`.

## Usage

### Watcher

First, create the necessary input and output directories:

```bash
mkdir -p data/input data/output
```

Then, run the file watcher script:

```bash
python spark/file_watcher.py
```

Drop a TSV file into the `data/input` directory to trigger processing. The format is a timestamp and a URL, separated by a tab:

```tsv
2025-09-01T00:00:00Z    https://example.com
```

Processing will be initiated automatically by the watcher.

## Configuration

The system's behavior is primarily controlled by flags in `sentinel/spark/config/spark_config.py`.

* **`USE_METHODS`**: A list specifying scoring methods to use (at maximum it can be `["classical", "lightgbm"]`).
* **`TRAIN_MODEL`**: If `True`, trains a new LightGBM model instead of scoring.
* **`USE_GOOD_DATA`**: If `True`, fetches data from MediaCloud and Wikipedia for benchmarking.
* **`READ_REPUTATION`**: If `True`, loads domain scores from PostgreSQL to use as a feature.
* **`WRITE_REPUTATION`**: If `True`, updates the domain reputation table after processing.
* **`WRITE_DB` / `WRITE_PARQUET` / `WRITE_TSV`**: Boolean flags to control output formats.
* **`WRITE_SUMMARY`**: If `True`, generates a human-readable summary file.
* **`USE_FEATURES`**: If `True`, enables the deep-dive feature analysis on a sample of URLs.
* **`FEATURES_SAMPLE_PERCENTAGE`**: Sets the size of the sample for deep analysis.
* **MediaCloud Configuration**: The `MEDIACLOUD_DAYS` variable controls how many days of MediaCloud data are fetched. Since the private MediaCloud URL cannot be provided in `.env`, it is recommended to **set this to `0`** to disable MediaCloud integration and allow the pipeline to run without errors.

```python
MEDIACLOUD_DAYS = 0
```

## Conda Environment

The environment `spark_env` includes:

```
# packages in environment at /opt/anaconda3/envs/spark_env:
# Name                     Version            Build            Channel
absl-py                    2.3.1              pypi_0           pypi
annotated-types            0.7.0              pypi_0           pypi
basedpyright               1.29.5             pypi_0           pypi
beautifulsoup4             4.13.4             pypi_0           pypi
bs4                        0.0.2              pypi_0           pypi
bzip2                      1.0.8              h80987f9_6
ca-certificates            2025.2.25          hca03da5_0
certifi                    2025.7.9           pypi_0           pypi
charset-normalizer         3.4.2              pypi_0           pypi
contourpy                  1.3.2              pypi_0           pypi
cycler                     0.12.1             pypi_0           pypi
expat                      2.7.1              h313beb8_0
filelock                   3.18.0             pypi_0           pypi
fonttools                  4.58.5             pypi_0           pypi
idna                       3.10               pypi_0           pypi
importlib-metadata         8.7.0              pypi_0           pypi
joblib                     1.5.1              pypi_0           pypi
kiwisolver                 1.4.8              pypi_0           pypi
libcxx                     17.0.6             he5c5206_4
libffi                     3.4.4              hca03da5_1
lightgbm                   4.6.0              pypi_0           pypi
llvmlite                   0.44.0             pypi_0           pypi
mako                       1.3.10             pypi_0           pypi
markupsafe                 3.0.2              pypi_0           pypi
matplotlib                 3.10.3             pypi_0           pypi
ncurses                    6.4                h313beb8_0
nodejs-wheel-binaries      22.17.0            pypi_0           pypi
numba                      0.61.2             pypi_0           pypi
numpy                      2.2.6              pypi_0           pypi
openssl                    3.0.16             h02f6b3c_0
packaging                  25.0               pypi_0           pypi
pandas                     2.3.0              pypi_0           pypi
pandas-stubs               2.3.0.250703       pypi_0           pypi
pillow                     11.3.0             pypi_0           pypi
pip                        25.1               pyhc872135_2
psutil                     7.0.0              pypi_0           pypi
psycopg2-binary            2.9.10             pypi_0           pypi
py4j                       0.10.9.9           pypi_0           pypi
pyarrow                    20.0.0             pypi_0           pypi
pydantic                   2.11.7             pypi_0           pypi
pydantic-core              2.33.2             pypi_0           pypi
pyparsing                  3.2.3              pypi_0           pypi
pyspark                    4.0.0              pypi_0           pypi
pyspark-util               0.1.2              pypi_0           pypi
pyspark-utils              1.8.0              pypi_0           pypi
python                     3.11.13            h19e8193_0
python-dateutil            2.9.0.post0        pypi_0           pypi
python-dotenv              1.1.1              pypi_0           pypi
pytz                       2025.2             pypi_0           pypi
readline                   8.2                h1a28f6b_0
requests                   2.32.4             pypi_0           pypi
requests-file              2.1.0              pypi_0           pypi
scikit-learn               1.7.1              pypi_0           pypi
scipy                      1.16.1             pypi_0           pypi
setuptools                 78.1.1             py311hca03da5_0
six                        1.17.0             pypi_0           pypi
soupsieve                  2.7                pypi_0           pypi
sqlalchemy                 2.0.41             pypi_0           pypi
sqlite                     3.45.3             h80987f9_0
sqlparse                   0.5.3              pypi_0           pypi
surt                       0.3.1              pypi_0           pypi
tabulate                   0.9.0              pypi_0           pypi
threadpoolctl              3.6.0              pypi_0           pypi
tk                         8.6.14             h6ba3021_1
tldextract                 5.3.0              pypi_0           pypi
types-pytz                 2025.2.0.20250516  pypi_0           pypi
typing-extensions          4.14.1             pypi_0           pypi
typing-inspection          0.4.1              pypi_0           pypi
tzdata                     2025.2             pypi_0           pypi
urllib3                    2.5.0              pypi_0           pypi
watchdog                   6.0.0              pypi_0           pypi
wheel                      0.45.1             py311hca03da5_0
xxhash                     3.5.0              pypi_0           pypi
xz                         5.6.4              h80987f9_1
yoyo-migrations            9.0.0              pypi_0           pypi
zipp                       3.23.0             pypi_0           pypi
zlib                       1.2.13             h18a0788_1
```

## System Profiling

The system includes an integrated profiler (`testing/profile_job.py`) that automatically monitors and reports on the resource consumption of each job triggered by the file watcher.

## Project Structure

```
sentinel/
├── assets/                 # Images and assets for README
├── data/
│   ├── input/              # Watched directory for incoming TSV files
│   ├── output/             # Directory for job outputs (Parquet, TSV, summaries)
│   └── storage/
│       ├── good_data/      # Storage for curated URL lists
│       └── lists/          # Custom lists (spam keywords, NSFW domains, etc.)
├── docker/                 # Docker Compose configuration for PostgreSQL
├── features/               # Modules for deep feature analysis (content, outlinks, etc.)
├── migrations/             # yoyo-migrations scripts for database schema
├── models/
│   ├── code/               # Scoring logic (classical heuristics, LightGBM model)
│   └── trained/            # Saved machine learning model files
├── spark/
│   ├── config/             # Spark configuration
│   └── file_watcher.py     # Script to monitor input directory and trigger jobs
├── spark_logic/
│   └── process_urls.py     # Core Spark processing script
├── testing/                # Profiling and data generation scripts
├── utils/                  # Utility scripts (DB connection, data conversion)
├── writers/                # Modules for writing output data (DB, Parquet, summary)
├── .env                    # Environment variables (DB connection, API keys)
├── .env.example            # Template for environment variables
├── README.md               # This file
└── yoyo.ini                # Configuration for yoyo-migrations
```

## License

This project is licensed under the GNU Affero General Public License v3.0. See the `LICENSE` file for details.

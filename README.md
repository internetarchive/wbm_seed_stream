# Usage

This project includes a startup script that allows you to run the API server and data collectors either together or separately.

## Prerequisites

- Python with `uvicorn` installed
- Collectors directory with `manager.py` file
- Main API application in `main.py`

## Basic Usage

```bash
./start.sh [OPTIONS]
```

## Options

### `--api-only`
Runs only the API server using uvicorn.

```bash
./start.sh --api-only
```

This will start the FastAPI server with auto-reload enabled.

### `--collectors-only`
Runs only the data collectors without starting the API server.

```bash
./start.sh --collectors-only
```

This will execute the collector manager in the `collectors` directory.

### `--collectors <collector1.py> <collector2.py> ...`
Specifies which collectors to run. Can be used with `--collectors-only` or in default mode.

```bash
# Run specific collectors only
./start.sh --collectors-only --collectors weather.py stock.py

# Run API with specific collectors
./start.sh --collectors news.py social.py
```

## Examples

### Start everything (default behavior)
```bash
./start.sh
```
Starts both the API server and all collectors concurrently.

### Start only the API server
```bash
./start.sh --api-only
```

### Start only collectors
```bash
./start.sh --collectors-only
```

### Start specific collectors only
```bash
./start.sh --collectors-only --collectors data_collector.py metrics_collector.py
```

### Start API with specific collectors
```bash
./start.sh --collectors web_scraper.py api_poller.py
```

## Process Management

When running in default mode (both API and collectors), the script:
- Starts both processes in the background
- Captures their process IDs
- Sets up signal handlers to gracefully shut down both processes when interrupted (Ctrl+C)
- Waits for both processes to complete

## Error Handling

The script includes validation to prevent conflicting options:
- Cannot use both `--api-only` and `--collectors-only` simultaneously
- Unknown options will display usage information and exit

## Directory Structure Expected

```
project/
├── main.py              # FastAPI application
├── start.sh             # This startup script
└── collectors/
    ├── manager.py       # Collector manager
    ├── collector1.py    # Individual collectors
    └── collector2.py
```
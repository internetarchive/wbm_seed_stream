

This project includes a startup script that allows you to run the API server and data collectors either together or separately.



- Python with `uvicorn` installed
- Collectors directory with `manager.py` file
- Main API application in `main.py`



```bash
./start.sh [OPTIONS]
```




Runs only the API server using uvicorn.

```bash
./start.sh --api-only
```

This will start the FastAPI server with auto-reload enabled.


Runs only the data collectors without starting the API server.

```bash
./start.sh --collectors-only
```

This will execute the collector manager in the `collectors` directory.


Specifies which collectors to run. Can be used with `--collectors-only` or in default mode.

```bash

./start.sh --collectors-only --collectors weather.py stock.py


./start.sh --collectors news.py social.py
```




```bash
./start.sh
```
Starts both the API server and all collectors concurrently.


```bash
./start.sh --api-only
```


```bash
./start.sh --collectors-only
```


```bash
./start.sh --collectors-only --collectors data_collector.py metrics_collector.py
```


```bash
./start.sh --collectors web_scraper.py api_poller.py
```



When running in default mode (both API and collectors), the script:
- Starts both processes in the background
- Captures their process IDs
- Sets up signal handlers to gracefully shut down both processes when interrupted (Ctrl+C)
- Waits for both processes to complete



The script includes validation to prevent conflicting options:
- Cannot use both `--api-only` and `--collectors-only` simultaneously
- Unknown options will display usage information and exit



```
project/
├── main.py              
├── start.sh             
└── collectors/
    ├── manager.py       
    ├── collector1.py    
    └── collector2.py
```
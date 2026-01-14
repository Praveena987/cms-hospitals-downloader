# CMS Hospitals Data Pipeline

A Python-based utility to discover, download, and process "Hospital" themed datasets from the CMS Provider Data Catalog.

## Features
- **Discovery:** Uses the CMS JSON API to filter for "Hospitals" themes.
- **Asynchronous Execution:** Uses `aiohttp` and `asyncio.Semaphore` for high-performance parallel downloads.
- **Incremental Sync:** Maintains a local `state/cms_hospitals_state.json` to track `modified` timestamps, ensuring files are only downloaded if updated.
- **Header Normalization:** Automatically converts mixed-case/special-character headers into `snake_case`.

## How to Run
1. Install dependencies: `pip install aiohttp`
2. Execute the script: `python main.py`
3. Outputs will be saved in the `/output` directory, and sync state in `/state`.

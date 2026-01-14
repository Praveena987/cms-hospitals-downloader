#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CMS Provider Data Catalog - Hospitals theme downloader
- Discovers datasets with theme "Hospitals"
- Downloads CSV distributions in parallel
- Converts CSV headers to snake_case
- Skips unchanged datasets using local run-state (modified timestamp)
- Cross-platform: Windows / Linux
Author: <your name/email>
"""

import asyncio
import aiohttp
import csv
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_URL = "https://data.cms.gov/provider-data/api/1"
DATASETS_URL = f"{BASE_URL}/metastore/schemas/dataset/items"

# Tweakable settings
CONCURRENCY = int(os.getenv("CMS_CONCURRENCY", "6")) # parallel downloads
TIMEOUT_SECS = int(os.getenv("CMS_HTTP_TIMEOUT", "120"))
OUTPUT_DIR = Path(os.getenv("CMS_OUTPUT_DIR", "output"))
STATE_DIR = Path(os.getenv("CMS_STATE_DIR", "state"))
STATE_FILE = STATE_DIR / "cms_hospitals_state.json"
USER_AGENT = os.getenv("CMS_USER_AGENT", "cms-hospitals-downloader/1.0 (+github.com/your-handle)")

# ---------- Utilities ----------

def utc_now_iso() -> str:
return datetime.now(timezone.utc).isoformat()

def ensure_dirs():
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

def load_state() -> Dict[str, Any]:
if STATE_FILE.exists():
with open(STATE_FILE, "r", encoding="utf-8") as f:
return json.load(f)
return {"last_run": None, "datasets": {}}

def save_state(state: Dict[str, Any]) -> None:
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
tmp = STATE_FILE.with_suffix(".tmp")
with open(tmp, "w", encoding="utf-8") as f:
json.dump(state, f, indent=2, sort_keys=True)
tmp.replace(STATE_FILE)

# Normalize column headers → snake_case
_non_alnum = re.compile(r"[^0-9a-zA-Z]+")
_underscore_collapse = re.compile(r"_+")

def to_snake_case(label: str) -> str:
# Normalize Unicode (remove accents), lowercase
nfkd = unicodedata.normalize("NFKD", label)
# Strip diacritics
no_diacritics = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
lowered = no_diacritics.lower()
# Replace non-alphanumeric with underscores
s = _non_alnum.sub("_", lowered)
# Collapse multiple underscores and trim
s = _underscore_collapse.sub("_", s).strip("_")
# Avoid empty names
return s or "column"

def choose_text_encoding(sample: bytes) -> str:
# Simple heuristic: try utf-8-sig first, then fallback to latin-1
try:
sample.decode("utf-8-sig")
return "utf-8-sig"
except UnicodeDecodeError:
return "latin-1"

# ---------- HTTP fetch ----------

class HttpClient:
def __init__(self, session: aiohttp.ClientSession):
self.session = session

async def get_json(self, url: str) -> Any:
async with self.session.get(url, timeout=TIMEOUT_SECS) as resp:
resp.raise_for_status()
return await resp.json()

async def download_bytes(self, url: str) -> bytes:
async with self.session.get(url, timeout=TIMEOUT_SECS) as resp:
resp.raise_for_status()
return await resp.read()

# ---------- Core logic ----------

async def fetch_hospital_datasets(client: HttpClient) -> List[Dict[str, Any]]:
"""
Pull all dataset items; filter to those whose 'theme' contains 'Hospitals'.
See API docs: /metastore/schemas/dataset/items
"""
items = await client.get_json(DATASETS_URL)

hospitals: List[Dict[str, Any]] = []
for item in items:
themes = item.get("theme") or []
# theme can be a list of strings
if any(isinstance(t, str) and t.strip().lower() == "hospitals" for t in themes):
hospitals.append(item)
return hospitals

def distributions_to_download(
item: Dict[str, Any],
state: Dict[str, Any],
) -> List[Tuple[str, str, str]]:
"""
Returns list of (download_url, dataset_id, dataset_title) for CSV media in this dataset
only if dataset 'modified' is newer than state.
"""
ds_id = item.get("identifier") or item.get("id") or "unknown"
ds_title = (item.get("title") or f"dataset_{ds_id}").strip()
modified = (item.get("modified") or "").strip()
# Compare state
ds_state = state["datasets"].get(ds_id)
if ds_state and ds_state.get("modified") == modified:
# unchanged since last run
return []

dist = item.get("distribution") or []
results: List[Tuple[str, str, str]] = []
for d in dist:
media = (d.get("mediaType") or "").lower()
url = d.get("downloadURL") or d.get("accessURL")
if media == "text/csv" and url:
results.append((url, ds_id, ds_title))
return results

async def process_distribution(
client: HttpClient,
url: str,
dataset_id: str,
dataset_title: str,
out_dir: Path,
semaphore: asyncio.Semaphore,
) -> Optional[Path]:
"""
Download CSV, normalize header to snake_case, write to output.
Returns path to processed file or None on error.
"""
async with semaphore:
try:
raw = await client.download_bytes(url)
except Exception as e:
print(f"[ERROR] Download failed for {url} ({dataset_id}): {e}", file=sys.stderr)
return None

# Detect encoding
encoding = choose_text_encoding(raw[:4096])
text = raw.decode(encoding, errors="replace")

# Read with csv.reader and write normalized header, streaming rows
out_dir.mkdir(parents=True, exist_ok=True)
# Create a stable filename from the distribution URL
stem = Path(url.split("?")[0]).stem or "file"
out_path = out_dir / f"{stem}__snake.csv"

try:
with open(out_path, "w", encoding="utf-8", newline="") as fout:
writer = csv.writer(fout)
# Use csv to parse the entire content safely
reader = csv.reader(text.splitlines())
first = True
for row in reader:
if first:
normalized = [to_snake_case(col) for col in row]
writer.writerow(normalized)
first = False
else:
writer.writerow(row)
except Exception as e:
print(f"[ERROR] Processing failed for {url} ({dataset_id}): {e}", file=sys.stderr)
return None

print(f"[OK] {dataset_id} :: {dataset_title} → {out_path}")
return out_path

async def main():
ensure_dirs()
state = load_state()

connector = aiohttp.TCPConnector(limit=CONCURRENCY)
headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
client = HttpClient(session)

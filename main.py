#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CMS Provider Data Catalog - Hospitals theme downloader
- Discovers datasets with theme "Hospitals"
- Downloads CSV distributions in parallel
- Converts CSV headers to snake_case
- Skips unchanged datasets using local run-state (modified timestamp)
- Cross-platform: Windows / Linux
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

# ------------------ Config ------------------

BASE_URL = "https://data.cms.gov/provider-data/api/1"
DATASETS_URL = f"{BASE_URL}/metastore/schemas/dataset/items"

CONCURRENCY = int(os.getenv("CMS_CONCURRENCY", "6"))
TIMEOUT_SECS = int(os.getenv("CMS_HTTP_TIMEOUT", "120"))

OUTPUT_DIR = Path(os.getenv("CMS_OUTPUT_DIR", "output"))
STATE_DIR = Path(os.getenv("CMS_STATE_DIR", "state"))
STATE_FILE = STATE_DIR / "cms_hospitals_state.json"

USER_AGENT = os.getenv(
    "CMS_USER_AGENT",
    "cms-hospitals-downloader/1.0 (+github.com/your-handle)"
)

# ------------------ Utilities ------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> None:
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


# ------------------ Column normalization ------------------

_non_alnum = re.compile(r"[^0-9a-zA-Z]+")
_underscore_collapse = re.compile(r"_+")


def to_snake_case(label: str) -> str:
    nfkd = unicodedata.normalize("NFKD", label)
    no_diacritics = "".join(
        ch for ch in nfkd if not unicodedata.combining(ch)
    )
    lowered = no_diacritics.lower()
    s = _non_alnum.sub("_", lowered)
    s = _underscore_collapse.sub("_", s).strip("_")
    return s or "column"


def choose_text_encoding(sample: bytes) -> str:
    try:
        sample.decode("utf-8-sig")
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "latin-1"


# ------------------ HTTP Client ------------------

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


# ------------------ Core Logic ------------------

async def fetch_hospital_datasets(
    client: HttpClient,
) -> List[Dict[str, Any]]:
    items = await client.get_json(DATASETS_URL)

    hospitals: List[Dict[str, Any]] = []
    for item in items:
        themes = item.get("theme") or []
        if any(
            isinstance(t, str) and t.strip().lower() == "hospitals"
            for t in themes
        ):
            hospitals.append(item)
    return hospitals


def distributions_to_download(
    item: Dict[str, Any],
    state: Dict[str, Any],
) -> List[Tuple[str, str, str, str]]:
    ds_id = item.get("identifier") or item.get("id") or "unknown"
    ds_title = (item.get("title") or f"dataset_{ds_id}").strip()
    modified = (item.get("modified") or "").strip()

    ds_state = state["datasets"].get(ds_id)
    if ds_state and ds_state.get("modified") == modified:
        return []

    results: List[Tuple[str, str, str, str]] = []
    for d in item.get("distribution") or []:
        media = (d.get("mediaType") or "").lower()
        url = d.get("downloadURL") or d.get("accessURL")
        if media == "text/csv" and url:
            results.append((url, ds_id, ds_title, modified))

    return results


async def process_distribution(
    client: HttpClient,
    url: str,
    dataset_id: str,
    dataset_title: str,
    out_dir: Path,
    semaphore: asyncio.Semaphore,
) -> Optional[Path]:
    async with semaphore:
        try:
            raw = await client.download_bytes(url)
        except Exception as e:
            print(
                f"[ERROR] Download failed for {url} ({dataset_id}): {e}",
                file=sys.stderr,
            )
            return None

    encoding = choose_text_encoding(raw[:4096])
    text = raw.decode(encoding, errors="replace")

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(url.split("?")[0]).stem or "file"
    out_path = out_dir / f"{stem}__snake.csv"

    try:
        with open(out_path, "w", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout)
            reader = csv.reader(text.splitlines())

            first = True
            for row in reader:
                if first:
                    writer.writerow([to_snake_case(c) for c in row])
                    first = False
                else:
                    writer.writerow(row)

    except Exception as e:
        print(
            f"[ERROR] Processing failed for {url} ({dataset_id}): {e}",
            file=sys.stderr,
        )
        return None

    print(f"[OK] {dataset_id} :: {dataset_title} → {out_path}")
    return out_path


# ------------------ Main ------------------

async def main() -> None:
    ensure_dirs()
    state = load_state()

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }

    async with aiohttp.ClientSession(
        connector=connector, headers=headers
    ) as session:
        client = HttpClient(session)

        print("[*] Fetching dataset catalog …")
        items = await fetch_hospital_datasets(client)
        print(f"[*] Found {len(items)} hospital dataset(s).")

        semaphore = asyncio.Semaphore(CONCURRENCY)
        tasks: List[asyncio.Task] = []

        for item in items:
            for url, ds_id, ds_title, modified in distributions_to_download(
                item, state
            ):
                task = asyncio.create_task(
                    process_distribution(
                        client,
                        url,
                        ds_id,
                        ds_title,
                        OUTPUT_DIR,
                        semaphore,
                    )
                )
                tasks.append(task)
                state["datasets"][ds_id] = {
                    "modified": modified,
                    "last_seen": utc_now_iso(),
                }

        if tasks:
            await asyncio.gather(*tasks)

        state["last_run"] = utc_now_iso()
        save_state(state)


if __name__ == "__main__":
    asyncio.run(main())

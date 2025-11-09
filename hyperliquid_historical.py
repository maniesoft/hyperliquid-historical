#!/usr/bin/env python3
"""
Hyperliquid Historical Downloader (fixed + logging)

Features
- Authenticated S3 access with Requester Pays (no UNSIGNED)
- Subcommands: list, download, decompress, to_csv
- Date/hour range selection (e.g., from 20250101 0 to 20251231 23)
- Optional --all to discover all coins present in the range
- Parallel downloads with retries
- Streaming LZ4 decompression to avoid high RAM usage
- Robust logging to console and optional logfile

Requirements
- boto3, botocore, lz4

Examples
  # List all L2 objects for SOL & BTC in Jan 1, 2025 00-02
  python hyperliquid_historical_fixed.py list SOL BTC -sd 20250101 -sh 0 -ed 20250101 -eh 2

  # Download all coins found in the range (requester pays)
  python hyperliquid_historical_fixed.py download --all -sd 20250101 -sh 0 -ed 20250102 -eh 23 \
      --out ./data --workers 12

  # Decompress everything you downloaded
  python hyperliquid_historical_fixed.py decompress -sd 20250101 -sh 0 -ed 20250102 -eh 23 --out ./data

  # Convert JSONL-like L2 files to CSV (best-effort parser)
  python hyperliquid_historical_fixed.py to_csv -sd 20250101 -sh 0 -ed 20250102 -eh 23 --out ./data
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import lz4.frame

BUCKET = "hyperliquid-archive"
PREFIX = "market_data"
DATATYPE = "l2Book"  # This script focuses on L2 snapshots

# -------------- Logging --------------

def setup_logging(verbosity: int, logfile: Optional[str] = None) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile, encoding="utf-8"))

    logging.basicConfig(level=level, format=log_format, datefmt=datefmt, handlers=handlers)

logger = logging.getLogger(__name__)

# -------------- Time Range Helpers --------------

def parse_yyyymmdd(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")


def iter_dates_hours(sd: str, sh: int, ed: str, eh: int) -> Iterator[Tuple[str, int]]:
    start = parse_yyyymmdd(sd).replace(hour=sh)
    end = parse_yyyymmdd(ed).replace(hour=eh)
    if end < start:
        raise ValueError("End must be >= Start")
    cur = start
    while cur <= end:
        yield cur.strftime("%Y%m%d"), cur.hour
        cur += timedelta(hours=1)

# -------------- S3 --------------

@dataclass
class S3Paths:
    bucket: str = BUCKET
    prefix: str = PREFIX
    datatype: str = DATATYPE

    def key(self, date: str, hour: int, coin: str) -> str:
        return f"{self.prefix}/{date}/{hour}/{self.datatype}/{coin}.lz4"

    def hour_prefix(self, date: str, hour: int) -> str:
        return f"{self.prefix}/{date}/{hour}/{self.datatype}/"


def make_s3_client() -> any:
    # Force region to us-east-1 for this bucket; helps avoid signature/redirect issues
    cfg = Config(region_name="us-east-1", retries={"max_attempts": 10, "mode": "adaptive"})
    return boto3.client("s3", config=cfg)


def list_hour_objects(s3, paths: S3Paths, date: str, hour: int) -> List[str]:
    """List coin object keys for a given date/hour under l2Book."""
    prefix = paths.hour_prefix(date, hour)
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    try:
        for page in paginator.paginate(Bucket=paths.bucket, Prefix=prefix, RequestPayer="requester"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".lz4") and f"/{paths.datatype}/" in key:
                    keys.append(key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "AccessDenied":
            logger.error("AccessDenied while listing %s (did you configure credentials?)", prefix)
        else:
            logger.exception("ClientError while listing %s", prefix)
    return keys


def discover_all_coins(s3, paths: S3Paths, sd: str, sh: int, ed: str, eh: int) -> List[str]:
    """Return a sorted list of coins discovered within the range."""
    coins = set()
    for date, hour in iter_dates_hours(sd, sh, ed, eh):
        keys = list_hour_objects(s3, paths, date, hour)
        for key in keys:
            # market_data/YYYYMMDD/H/l2Book/COIN.lz4
            try:
                coin = Path(key).name.replace(".lz4", "")
                coins.add(coin)
            except Exception:
                logger.debug("Failed to parse coin from key %s", key)
    out = sorted(coins)
    logger.info("Discovered %d coins in range.", len(out))
    return out

# -------------- Download --------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def download_one(s3, bucket: str, key: str, dest: Path, max_retries: int = 5) -> bool:
    ensure_dir(dest.parent)
    attempt = 0
    while True:
        try:
            # Pre-flight HEAD to distinguish 403 vs 404 vs exists
            try:
                s3.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
            except ClientError as he:
                code = he.response.get("Error", {}).get("Code")
                if code == "404" or code == "NotFound":
                    logger.error("NOT FOUND (404): s3://%s/%s", bucket, key)
                    return False
                if code == "403" or code == "AccessDenied":
                    logger.error("ACCESS DENIED (403) on HEAD: s3://%s/%s — check coin name, date/hour, and Requester-Pays permissions", bucket, key)
                else:
                    logger.warning("HEAD error %s on %s — will try GET anyway", code, key)

            logger.debug("GET s3://%s/%s -> %s", bucket, key, dest)
            s3.download_file(bucket, key, str(dest), ExtraArgs={"RequestPayer": "requester"})
            return True
        except (EndpointConnectionError, ClientError) as e:
            attempt += 1
            code = getattr(e, "response", {}).get("Error", {}).get("Code") if isinstance(e, ClientError) else type(e).__name__
            if attempt <= max_retries:
                sleep = min(60, 2 ** attempt)
                logger.warning("Download failed (%s). Retry %d/%d in %ds: %s", code, attempt, max_retries, sleep, key)
                time.sleep(sleep)
                continue
            logger.error("Giving up after %d retries on %s (%s)", max_retries, key, code)
            return False


def download_range(
    coins: List[str], sd: str, sh: int, ed: str, eh: int, out_dir: Path, workers: int
) -> None:
    s3 = make_s3_client()
    paths = S3Paths()

    jobs: List[Tuple[str, Path]] = []
    invalid = {"Tickers", "TICKERS", "tickers"}  # guard against accidental pseudo-coin names
    for date, hour in iter_dates_hours(sd, sh, ed, eh):
        for coin in coins:
            if coin in invalid:
                logger.warning("Skipping invalid coin name '%s' (did you mean actual symbols like BTC, SOL, ETH?)", coin)
                continue
            key = paths.key(date, hour, coin)
            dest = out_dir / date / str(hour) / paths.datatype / f"{coin}.lz4"
            jobs.append((key, dest))

    logger.info("Planned %d downloads.", len(jobs))

    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(download_one, s3, paths.bucket, key, dest): (key, dest)
            for key, dest in jobs
        }
        for fut in as_completed(futures):
            key, dest = futures[fut]
            try:
                if fut.result():
                    ok += 1
                else:
                    logger.error("FAILED: s3://%s/%s", BUCKET, key)
            except Exception:
                logger.exception("Exception during download of %s", key)
    logger.info("Downloads complete: %d/%d succeeded.", ok, len(jobs))

# -------------- Decompress --------------

def decompress_file(src_lz4: Path, dst_raw: Path) -> bool:
    try:
        ensure_dir(dst_raw.parent)
        with lz4.frame.open(src_lz4, mode="rb") as fin, open(dst_raw, "wb") as fout:
            while True:
                chunk = fin.read(1024 * 1024)
                if not chunk:
                    break
                fout.write(chunk)
        return True
    except Exception:
        logger.exception("Decompression failed: %s", src_lz4)
        return False


def decompress_tree(base_dir: Path, workers: int) -> None:
    lz_files = [p for p in base_dir.rglob("*.lz4")]
    logger.info("Found %d .lz4 files to decompress.", len(lz_files))

    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = []
        for src in lz_files:
            dst = src.with_suffix("")  # drop .lz4
            futures.append(ex.submit(decompress_file, src, dst))
        for fut in as_completed(futures):
            try:
                if fut.result():
                    ok += 1
            except Exception:
                logger.exception("Unhandled error during decompression")
    logger.info("Decompression complete: %d/%d succeeded.", ok, len(lz_files))

# -------------- CSV Conversion (best-effort) --------------
# The raw files are assumed to be JSON lines. This parser is defensive and will
# try to extract common fields. Unknown lines are skipped with a warning.

COMMON_FIELDS = [
    "ts",           # timestamp if present
    "symbol",       # coin/symbol
    "side",         # bid/ask if present
    "price",
    "size",
]


def extract_rows_from_jsonl_line(line: str) -> List[Dict[str, object]]:
    """Best-effort extraction. Return zero or more rows for the CSV.
    This tries a few patterns to accommodate schema differences.
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("Skipping non-JSON line of length %d", len(line))
        return []

    rows: List[Dict[str, object]] = []

    # Pattern A: { "symbol": "BTC", "ts": ..., "bids": [[price,size],...], "asks": [...] }
    if all(k in obj for k in ("symbol", "ts")) and ("bids" in obj or "asks" in obj):
        sym = obj.get("symbol")
        ts = obj.get("ts")
        for side in ("bids", "asks"):
            levels = obj.get(side) or []
            side_name = "bid" if side == "bids" else "ask"
            for lvl in levels:
                # lvl may be [price, size] or {"price":..., "size":...}
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    price, size = lvl[0], lvl[1]
                elif isinstance(lvl, dict):
                    price, size = lvl.get("price"), lvl.get("size")
                else:
                    continue
                rows.append({
                    "ts": ts,
                    "symbol": sym,
                    "side": side_name,
                    "price": price,
                    "size": size,
                })
        return rows

    # Pattern B: nested raw format e.g. { "raw": {"data": {"levels": {"bids": [...], "asks": [...]}} , "ts": ..., "symbol": ...}}
    raw = obj.get("raw", {}) if isinstance(obj, dict) else {}
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    levels = data.get("levels", {}) if isinstance(data, dict) else {}
    if isinstance(levels, dict) and ("bids" in levels or "asks" in levels):
        sym = obj.get("symbol") or data.get("symbol")
        ts = obj.get("ts") or data.get("ts")
        for side in ("bids", "asks"):
            lvls = levels.get(side) or []
            side_name = "bid" if side == "bids" else "ask"
            for lvl in lvls:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    price, size = lvl[0], lvl[1]
                elif isinstance(lvl, dict):
                    price, size = lvl.get("price"), lvl.get("size")
                else:
                    continue
                rows.append({
                    "ts": ts,
                    "symbol": sym,
                    "side": side_name,
                    "price": price,
                    "size": size,
                })
        return rows

    # Unknown schema -> try to pick flat numeric fields named similarly
    if isinstance(obj, dict) and any(k in obj for k in ("price", "size")):
        rows.append({k: obj.get(k) for k in COMMON_FIELDS})
        return rows

    logger.debug("No known schema matched for line; skipping.")
    return []


def convert_jsonl_to_csv(jsonl_path: Path, csv_path: Path) -> int:
    ensure_dir(csv_path.parent)
    written = 0
    with open(jsonl_path, "r", encoding="utf-8", errors="ignore") as fin, open(csv_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=COMMON_FIELDS)
        writer.writeheader()
        for line in fin:
            rows = extract_rows_from_jsonl_line(line)
            for r in rows:
                # Ensure all fields exist
                for k in COMMON_FIELDS:
                    r.setdefault(k, None)
                writer.writerow(r)
                written += 1
    return written


def convert_tree_to_csv(base_dir: Path, workers: int) -> None:
    raw_files = [p for p in base_dir.rglob("*.lz4")]
    if raw_files:
        logger.warning("There are still .lz4 files present. Run 'decompress' first. We'll ignore .lz4 files.")

    jsonl_files = [p for p in base_dir.rglob("*/*/l2Book/*.?") if p.suffix.lower() not in (".lz4", ".csv")]
    logger.info("Found %d candidate raw files for CSV conversion.", len(jsonl_files))

    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = []
        for src in jsonl_files:
            csv_path = src.with_suffix(".csv")
            futures.append(ex.submit(convert_jsonl_to_csv, src, csv_path))
        for fut in as_completed(futures):
            try:
                count = fut.result()
                ok += 1
                logger.debug("Converted file with %d rows.", count)
            except Exception:
                logger.exception("Conversion error")
    logger.info("CSV conversion complete: %d/%d files converted.", ok, len(jsonl_files))

# -------------- CLI --------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hyperliquid historical L2 helper (Requester-Pays ready)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("t", nargs="*", help="Tickers (coins), e.g., BTC SOL. Ignored when --all is set.")
    parent.add_argument("-sd", required=True, help="Start date YYYYMMDD")
    parent.add_argument("-sh", type=int, default=0, help="Start hour 0-23")
    parent.add_argument("-ed", required=True, help="End date YYYYMMDD")
    parent.add_argument("-eh", type=int, default=23, help="End hour 0-23")
    parent.add_argument("--all", action="store_true", help="Discover all coins in the range by listing S3")
    parent.add_argument("--out", default="./hl_data", help="Output directory")
    parent.add_argument("--workers", type=int, default=8, help="Parallel workers (downloads/decompress/convert)")
    parent.add_argument("-v", action="count", default=0, help="Increase verbosity (-v, -vv)")
    parent.add_argument("--logfile", default=None, help="Optional logfile path")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", parents=[parent], help="List available S3 objects (L2)")
    sub.add_parser("probe", parents=[parent], help="Build manifest by HEAD-probing (no ListBucket)")
    sub.add_parser("download", parents=[parent], help="Download L2 .lz4 objects")
    sub.add_parser("decompress", parents=[parent], help="Decompress .lz4 to raw JSONL")
    sub.add_parser("to_csv", parents=[parent], help="Convert raw to CSV (best effort)")

    return parser


def probe_manifest(s3, paths: S3Paths, coins: List[str], sd: str, sh: int, ed: str, eh: int, out_manifest: Path) -> int:
    checked = 0
    found = 0
    ensure_dir(out_manifest.parent)
    with open(out_manifest, "w", encoding="utf-8") as f:
        for date, hour in iter_dates_hours(sd, sh, ed, eh):
            for coin in coins:
                key = paths.key(date, hour, coin)
                try:
                    s3.head_object(Bucket=paths.bucket, Key=key, RequestPayer="requester")
                    f.write(f"{key}\n")
                    found += 1
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code")
                    if code in ("404", "NotFound"):
                        logger.debug("404: %s", key)
                    elif code in ("403", "AccessDenied"):
                        logger.debug("403: %s", key)
                    else:
                        logger.warning("HEAD %s: %s", key, code)
                finally:
                    checked += 1
    logger.info("Probe complete: %d/%d objects exist. Manifest: %s", found, checked, out_manifest)
    return found


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.v, args.logfile)

    out_dir = Path(args.out)
    ensure_dir(out_dir)

    s3 = make_s3_client()
    paths = S3Paths()

    # Resolve coins
    if args.all:
        coins = discover_all_coins(s3, paths, args.sd, args.sh, args.ed, args.eh)
        if not coins:
            logger.error("No coins discovered in the requested range.")
            return 2
    else:
        coins = args.t
        if not coins:
            parser.error("You must specify coins (e.g., BTC SOL) or use --all")
            return 2

    if args.cmd == "probe":
        manifest = out_dir / "manifest.keys"
        probe_manifest(s3, paths, coins, args.sd, args.sh, args.ed, args.eh, manifest)
        return 0

    if args.cmd == "list":
        total = 0
        for date, hour in iter_dates_hours(args.sd, args.sh, args.ed, args.eh):
            keys = list_hour_objects(s3, paths, date, hour)
            # filter by coins
            keys = [k for k in keys if Path(k).stem in set(coins)]
            for k in keys:
                print(f"s3://{paths.bucket}/{k}")
            total += len(keys)
        logger.info("Listed %d objects.", total)
        return 0

    if args.cmd == "download":
        download_range(coins, args.sd, args.sh, args.ed, args.eh, out_dir, args.workers)
        return 0

    if args.cmd == "decompress":
        decompress_tree(out_dir, args.workers)
        return 0

    if args.cmd == "to_csv":
        convert_tree_to_csv(out_dir, args.workers)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())

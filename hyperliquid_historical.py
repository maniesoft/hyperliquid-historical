#!/usr/bin/env python3
"""
Hyperliquid Historical Downloader (L2 + Trades) — fixed + logging

Features
- Authenticated S3 access with Requester Pays
- Subcommands: list, probe, download, decompress, to_csv
- L2 mode (default): coins + date/hour -> l2Book snapshots from s3://hyperliquid-archive
- Trades mode (--trades): same CLI dates/hours, coins ignored; auto-filter derives from date range
  Auto-widen now: DAY -> MONTH (no YEAR). YEAR widen only if --allow-year-widen is set.
- Parallel downloads with retries, streaming LZ4 decompression, robust logging

Requirements: boto3, botocore, lz4
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import lz4.frame

# -------------- Global constants --------------

BUCKET = "hyperliquid-archive"
PREFIX = "market_data"
DATATYPE = "l2Book"  # L2 snapshots

# -------------- Trades datasets --------------

TRADE_BUCKET = "hl-mainnet-node-data"
TRADE_SOURCES = ("node_fills_by_block", "node_fills", "node_trades")

# -------------- Logging --------------

def setup_logging(verbosity: int, logfile: Optional[str] = None) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, datefmt="%Y-%m-%d %H:%M:%S", handlers=handlers)

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

# -------------- S3 paths --------------

@dataclass
class S3Paths:
    bucket: str = BUCKET
    prefix: str = PREFIX
    datatype: str = DATATYPE
    def key(self, date: str, hour: int, coin: str) -> str:
        return f"{self.prefix}/{date}/{hour}/{self.datatype}/{coin}.lz4"
    def hour_prefix(self, date: str, hour: int) -> str:
        return f"{self.prefix}/{date}/{hour}/{self.datatype}/"

def make_s3_client():
    cfg = Config(region_name="us-east-1", retries={"max_attempts": 10, "mode": "adaptive"})
    return boto3.client("s3", config=cfg)

# -------------- L2 listing & discovery --------------

def list_hour_objects(s3, paths: S3Paths, date: str, hour: int) -> List[str]:
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
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            logger.error("AccessDenied while listing %s (credentials ok? requester-pays?)", prefix)
        else:
            logger.exception("ClientError while listing %s", prefix)
    return keys

def discover_all_coins(s3, paths: S3Paths, sd: str, sh: int, ed: str, eh: int) -> List[str]:
    coins = set()
    for date, hour in iter_dates_hours(sd, sh, ed, eh):
        for key in list_hour_objects(s3, paths, date, hour):
            coins.add(Path(key).name.removesuffix(".lz4"))
    out = sorted(coins)
    logger.info("Discovered %d coins in range.", len(out))
    return out

# -------------- Common utilities --------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def download_one(s3, bucket: str, key: str, dest: Path, max_retries: int = 5) -> bool:
    ensure_dir(dest.parent)
    attempt = 0
    while True:
        try:
            try:
                s3.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
            except ClientError as he:
                code = he.response.get("Error", {}).get("Code")
                if code in ("404", "NotFound"):
                    logger.error("NOT FOUND (404): s3://%s/%s", bucket, key)
                    return False
                if code in ("403", "AccessDenied"):
                    logger.error("ACCESS DENIED (403) on HEAD: s3://%s/%s", bucket, key)
                else:
                    logger.warning("HEAD %s on %s — continuing to GET", code, key)
            logger.debug("GET s3://%s/%s -> %s", bucket, key, dest)
            s3.download_file(bucket, key, str(dest), ExtraArgs={"RequestPayer": "requester"})
            return True
        except (EndpointConnectionError, ClientError) as e:
            attempt += 1
            code = getattr(e, "response", {}).get("Error", {}).get("Code") if isinstance(e, ClientError) else type(e).__name__
            if attempt <= max_retries:
                sleep = min(60, 2 ** attempt)
                logger.warning("Download failed (%s). Retry %d/%d in %ds: %s", code, attempt, max_retries, sleep, key)
                time.sleep(sleep); continue
            logger.error("Giving up after %d retries on %s (%s)", max_retries, key, code)
            return False

# -------------- L2 download --------------

def download_range(coins: List[str], sd: str, sh: int, ed: str, eh: int, out_dir: Path, workers: int) -> None:
    s3 = make_s3_client()
    paths = S3Paths()
    jobs: List[Tuple[str, Path]] = []
    invalid = {"Tickers", "TICKERS", "tickers"}
    for date, hour in iter_dates_hours(sd, sh, ed, eh):
        for coin in coins:
            if coin in invalid:
                logger.warning("Skipping invalid coin '%s'", coin); continue
            key = paths.key(date, hour, coin)
            dest = out_dir / date / str(hour) / paths.datatype / f"{coin}.lz4"
            jobs.append((key, dest))
    logger.info("Planned %d L2 downloads.", len(jobs))
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(download_one, s3, paths.bucket, key, dest): (key, dest) for key, dest in jobs}
        for fut in as_completed(futs):
            key, dest = futs[fut]
            try:
                if fut.result(): ok += 1
                else: logger.error("FAILED: s3://%s/%s", BUCKET, key)
            except Exception:
                logger.exception("Exception during download of %s", key)
    logger.info("L2 downloads complete: %d/%d succeeded.", ok, len(jobs))

# -------------- Decompress --------------

def decompress_file(src_lz4: Path, dst_raw: Path) -> bool:
    try:
        ensure_dir(dst_raw.parent)
        with lz4.frame.open(src_lz4, "rb") as fin, open(dst_raw, "wb") as fout:
            for chunk in iter(lambda: fin.read(1024 * 1024), b""):
                if not chunk: break
                fout.write(chunk)
        return True
    except Exception:
        logger.exception("Decompression failed: %s", src_lz4); return False

def decompress_tree(base_dir: Path, workers: int) -> None:
    lz_files = [p for p in base_dir.rglob("*.lz4")]
    logger.info("Found %d .lz4 files to decompress.", len(lz_files))
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(decompress_file, src, src.with_suffix("")) for src in lz_files]
        for fut in as_completed(futs):
            try:
                if fut.result(): ok += 1
            except Exception:
                logger.exception("Unhandled error during decompression")
    logger.info("Decompression complete: %d/%d succeeded.", ok, len(lz_files))

# -------------- CSV Conversion (best-effort for L2) --------------

COMMON_FIELDS = ["ts","symbol","side","price","size"]

def extract_rows_from_jsonl_line(line: str) -> List[Dict[str, object]]:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("Skipping non-JSON line (len=%d)", len(line)); return []
    rows: List[Dict[str, object]] = []

    if isinstance(obj, dict) and all(k in obj for k in ("symbol","ts")) and ("bids" in obj or "asks" in obj):
        sym, ts = obj.get("symbol"), obj.get("ts")
        for side_key in ("bids","asks"):
            side = "bid" if side_key == "bids" else "ask"
            for lvl in obj.get(side_key) or []:
                if isinstance(lvl, (list, tuple)) and len(lvl)>=2:
                    price,size = lvl[0], lvl[1]
                elif isinstance(lvl, dict):
                    price,size = lvl.get("price"), lvl.get("size")
                else:
                    continue
                rows.append({"ts":ts,"symbol":sym,"side":side,"price":price,"size":size})
        return rows

    raw = obj.get("raw", {}) if isinstance(obj, dict) else {}
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    levels = data.get("levels", {}) if isinstance(data, dict) else {}
    if isinstance(levels, dict) and ("bids" in levels or "asks" in levels):
        sym = obj.get("symbol") or data.get("symbol")
        ts = obj.get("ts") or data.get("ts")
        for side_key in ("bids","asks"):
            side = "bid" if side_key=="bids" else "ask"
            for lvl in levels.get(side_key) or []:
                if isinstance(lvl,(list,tuple)) and len(lvl)>=2:
                    price,size = lvl[0], lvl[1]
                elif isinstance(lvl,dict):
                    price,size = lvl.get("price"), lvl.get("size")
                else:
                    continue
                rows.append({"ts":ts,"symbol":sym,"side":side,"price":price,"size":size})
        return rows

    if isinstance(obj, dict) and any(k in obj for k in ("price","size")):
        rows.append({k: obj.get(k) for k in COMMON_FIELDS}); return rows
    return []

def convert_jsonl_to_csv(jsonl_path: Path, csv_path: Path) -> int:
    ensure_dir(csv_path.parent)
    written = 0
    with open(jsonl_path, "r", encoding="utf-8", errors="ignore") as fin, \
         open(csv_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=COMMON_FIELDS)
        writer.writeheader()
        for line in fin:
            for r in extract_rows_from_jsonl_line(line):
                for k in COMMON_FIELDS: r.setdefault(k, None)
                writer.writerow(r); written += 1
    return written

def convert_tree_to_csv(base_dir: Path, workers: int) -> None:
    raw_files = [p for p in base_dir.rglob("*.lz4")]
    if raw_files:
        logger.warning("There are still .lz4 files present. Run 'decompress' first.")
    jsonl_files = [p for p in base_dir.rglob("*/*/l2Book/*") if p.is_file() and p.suffix.lower() not in (".lz4",".csv")]
    logger.info("Found %d candidate raw files for CSV conversion.", len(jsonl_files))
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(convert_jsonl_to_csv, src, src.with_suffix(".csv")) for src in jsonl_files]
        for fut in as_completed(futs):
            try:
                fut.result(); ok += 1
            except Exception:
                logger.exception("Conversion error")
    logger.info("CSV conversion complete: %d/%d files converted.", ok, len(jsonl_files))

# -------------- Trades helpers --------------

def _date_contains_candidates(sd: str, sh: int, ed: str, eh: int, include_year: bool = False) -> List[str]:
    """
    Build candidate substrings for --contains based on the date range.
    Default: only YYYYMMDD and YYYYMM (NO YYYY) to avoid 'whole year' scans.
    Set include_year=True to also include YYYY.
    """
    seen = set()
    for d, _h in iter_dates_hours(sd, sh, ed, eh):
        seen.add(d)       # YYYYMMDD
        seen.add(d[:6])   # YYYYMM
        if include_year:
            seen.add(d[:4])  # YYYY
    # prefer most specific first
    return sorted(seen, key=len, reverse=True)

def trades_list_keys(s3, src_prefix: str, contains: Optional[str]) -> List[str]:
    bucket = TRADE_BUCKET
    prefix = f"{src_prefix.rstrip('/')}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, RequestPayer="requester"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if contains and contains not in key:
                    continue
                keys.append(key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        logger.error("Trades list failed (%s) for %s", code, prefix)
    return keys

def trades_download(s3, src_prefix: str, contains: Optional[str], out_dir: Path, workers: int) -> None:
    keys = trades_list_keys(s3, src_prefix, contains)
    logger.info("Trades: planning %d downloads from %s (filter=%r)", len(keys), src_prefix, contains)
    ensure_dir(out_dir)
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(download_one, s3, TRADE_BUCKET, k, out_dir / k) for k in keys]
        for fut in as_completed(futs):
            try:
                if fut.result(): ok += 1
            except Exception:
                logger.exception("Trades download crash")
    logger.info("Trades downloads complete: %d/%d", ok, len(keys))

# -------------- CLI --------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hyperliquid historical downloader (L2 + Trades) — Requester-Pays ready",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("t", nargs="*", help="Tickers (coins), e.g., BTC SOL. Ignored in --trades mode or when --all is set.")
    parent.add_argument("-sd", required=True, help="Start date YYYYMMDD")
    parent.add_argument("-sh", type=int, default=0, help="Start hour 0-23")
    parent.add_argument("-ed", required=True, help="End date YYYYMMDD")
    parent.add_argument("-eh", type=int, default=23, help="End hour 0-23")
    parent.add_argument("--all", action="store_true", help="Discover all coins in the range (L2 mode only)")
    parent.add_argument("--out", default="./hl_data", help="Output directory for L2 mode")
    parent.add_argument("--workers", type=int, default=8, help="Parallel workers")
    parent.add_argument("-v", action="count", default=0, help="Increase verbosity (-v, -vv)")
    parent.add_argument("--logfile", default=None, help="Optional logfile path")

    # Trades switches (same subcommands; coins ignored)
    parent.add_argument("--trades", action="store_true", help="Switch to trades mode (downloads from hl-mainnet-node-data)")
    parent.add_argument("--src", choices=TRADE_SOURCES, default="node_fills_by_block", help="Trades dataset when --trades is set")
    parent.add_argument("--contains", default=None, help="Optional substring filter for trades keys (e.g., 20250301 or 202503)")
    parent.add_argument("--tout", default="./trades", help="Output directory for trades (when --trades)")
    parent.add_argument("--allow-year-widen", action="store_true",
                        help="Allow auto-filter to widen to YYYY if day/month give no hits (default: off)")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", parents=[parent], help="List available L2 objects (via ListBucket)")
    sub.add_parser("probe", parents=[parent], help="L2: build manifest via HEAD; Trades: build filtered key list")
    sub.add_parser("download", parents=[parent], help="Download L2 or Trades depending on --trades")
    sub.add_parser("decompress", parents=[parent], help="Decompress all .lz4 under --out or --tout")
    sub.add_parser("to_csv", parents=[parent], help="Convert decompressed L2 files to CSV (best effort)")

    return parser

# -------------- Probe (L2) --------------

def probe_manifest(s3, paths: S3Paths, coins: List[str], sd: str, sh: int, ed: str, eh: int, out_manifest: Path) -> int:
    checked = 0; found = 0
    ensure_dir(out_manifest.parent)
    with open(out_manifest, "w", encoding="utf-8") as f:
        for date, hour in iter_dates_hours(sd, sh, ed, eh):
            for coin in coins:
                key = paths.key(date, hour, coin)
                try:
                    s3.head_object(Bucket=paths.bucket, Key=key, RequestPayer="requester")
                    f.write(f"{key}\n"); found += 1
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code")
                    if code in ("404","NotFound"): logger.debug("404: %s", key)
                    elif code in ("403","AccessDenied"): logger.debug("403: %s", key)
                    else: logger.warning("HEAD %s: %s", key, code)
                finally:
                    checked += 1
    logger.info("Probe complete: %d/%d objects exist. Manifest: %s", found, checked, out_manifest)
    return found

# -------------- Main --------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(args.v, args.logfile)

    s3 = make_s3_client()
    paths = S3Paths()

    l2_out = Path(args.out); trades_out = Path(args.tout)
    ensure_dir(l2_out); ensure_dir(trades_out)

    if not args.trades:
        if args.all:
            coins = discover_all_coins(s3, paths, args.sd, args.sh, args.ed, args.eh)
            if not coins:
                logger.error("No coins discovered in the requested range."); return 2
        else:
            coins = args.t
            if not coins:
                parser.error("You must specify coins (e.g., BTC SOL) or use --all (L2 mode)."); return 2
    else:
        coins = []

    if args.cmd == "list":
        if args.trades:
            logger.error("The 'list' subcommand lists L2 only. Use 'probe' or 'download' with --trades.")
            return 2
        total = 0
        for date, hour in iter_dates_hours(args.sd, args.sh, args.ed, args.eh):
            keys = list_hour_objects(s3, paths, date, hour)
            keys = [k for k in keys if Path(k).stem in set(coins)]
            for k in keys: print(f"s3://{paths.bucket}/{k}")
            total += len(keys)
        logger.info("Listed %d objects.", total)
        return 0

    if args.cmd == "probe":
        if args.trades:
            contains = args.contains
            if not contains:
                for cand in _date_contains_candidates(args.sd, args.sh, args.ed, args.eh, include_year=args.allow_year_widen):
                    if trades_list_keys(s3, args.src, cand):
                        contains = cand; logger.info("Trades filter auto-selected: '%s'", cand); break
            keys = trades_list_keys(s3, args.src, contains)
            manifest = trades_out / "trades.manifest.keys"
            ensure_dir(manifest.parent)
            with open(manifest, "w", encoding="utf-8") as f:
                for k in keys: f.write(f"{k}\n")
            logger.info("Trades probe complete: %d keys -> %s", len(keys), manifest)
        else:
            manifest = l2_out / "manifest.keys"
            probe_manifest(s3, paths, coins, args.sd, args.sh, args.ed, args.eh, manifest)
        return 0

    if args.cmd == "download":
        if args.trades:
            contains = args.contains
            if not contains:
                for cand in _date_contains_candidates(args.sd, args.sh, args.ed, args.eh, include_year=args.allow_year_widen):
                    test = trades_list_keys(s3, args.src, cand)
                    if test:
                        contains = cand
                        logger.info("Trades filter auto-selected: '%s' (found %d keys)", cand, len(test))
                        break
                if not contains:
                    logger.warning("No trades found via day/month filters%s; falling back to unfiltered listing (may be large)",
                                   " (year allowed)" if args.allow_year_widen else "")
            trades_download(s3, args.src, contains, trades_out, args.workers)
        else:
            download_range(coins, args.sd, args.sh, args.ed, args.eh, l2_out, args.workers)
        return 0

    if args.cmd == "decompress":
        base = trades_out if args.trades else l2_out
        decompress_tree(base, args.workers); return 0

    if args.cmd == "to_csv":
        convert_tree_to_csv(l2_out, args.workers); return 0

    return 0

if __name__ == "__main__":
    sys.exit(main())

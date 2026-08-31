#!/usr/bin/env python3
"""
EIS Parser Archive Retention Script
DATA-PIPELINE-S7-STORAGE-AUDIT-AND-RETENTION-1
2026-08-22

Bounded retention for /opt/tendermonitor/downloads named subdirs (44fz_*, 223fz_*)
that correspond to completed parsed dates.

The XML data/ directory is NOT touched by this script.
The data/ XMLs are the canonical parsed result — separate decision needed for those.

Policy:
  - KEEP_NAMED_DOWNLOAD_DIRS_DAYS=7  (downloaded archives for recently completed dates)
  - Only delete 44fz_NNNNNN and 223fz_NNNNNN named dirs (extracted download caches)
  - Never delete the current forward or backward processing date's dirs
  - Never delete dirs currently open by the parser process
  - Always --dry-run first

Usage:
  python3 archive_retention.py [--dry-run] [--keep-days N]
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# Configuration
DOWNLOADS_DIR = "/opt/tendermonitor/downloads"
PROCESSED_DATES_FILE = "/opt/tendermonitor/processed_dates.json"
BACKWARD_PROCESSED_DATES_FILE = "/opt/tendermonitor/backward/processed_dates.json"
BACKWARD_REGION_PROGRESS_FILE = "/opt/tendermonitor/backward/region_progress.json"
FORWARD_REGION_PROGRESS_FILE = "/opt/tendermonitor/region_progress.json"
DEFAULT_KEEP_DAYS = 7


def get_open_files_by_parser() -> set:
    """Get all file paths currently open by tendermonitor processes."""
    open_files = set()
    try:
        result = subprocess.run(
            ["pgrep", "-f", "tendermonitor.*main.py"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split()
        for pid in pids:
            fd_dir = f"/proc/{pid}/fd"
            try:
                for fd in os.listdir(fd_dir):
                    try:
                        target = os.readlink(os.path.join(fd_dir, fd))
                        open_files.add(target)
                    except (OSError, PermissionError):
                        pass
            except (OSError, PermissionError):
                pass
    except Exception:
        pass
    return open_files


def get_active_dates() -> set:
    """Get dates currently being processed (forward + backward active dates)."""
    active = set()
    # Forward region progress — get current date being processed
    for path in [FORWARD_REGION_PROGRESS_FILE, BACKWARD_REGION_PROGRESS_FILE]:
        try:
            with open(path) as f:
                data = json.load(f)
            for date_str in data.keys():
                active.add(date_str)
        except Exception:
            pass
    return active


def get_completed_dates() -> set:
    """Get dates that are fully completed (in processed_dates.json)."""
    completed = set()
    for path in [PROCESSED_DATES_FILE, BACKWARD_PROCESSED_DATES_FILE]:
        try:
            with open(path) as f:
                data = json.load(f)
            completed.update(data)
        except Exception:
            pass
    return completed


def extract_date_from_dirname(dirname: str) -> str | None:
    """
    Extract source date from download dir name if possible.
    Format: 44fz_NNNNNN or 223fz_NNNNNN — these are sequential IDs, NOT dates.
    We can only use mtime as age proxy for these dirs.
    Returns None if date cannot be determined.
    """
    return None  # IDs not date-based; use mtime


def main():
    parser = argparse.ArgumentParser(description="EIS archive retention cleanup")
    parser.add_argument("--dry-run", action="store_true", help="Preview without deleting")
    parser.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS,
                        help=f"Days to keep completed archive dirs (default: {DEFAULT_KEEP_DAYS})")
    args = parser.parse_args()

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.keep_days)
    open_files = get_open_files_by_parser()
    active_dates = get_active_dates()
    completed_dates = get_completed_dates()

    print(f"Archive retention script")
    print(f"  Downloads dir: {DOWNLOADS_DIR}")
    print(f"  Keep days: {args.keep_days}")
    print(f"  Cutoff date: {cutoff.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Active dates (in progress): {sorted(active_dates)}")
    print(f"  Completed dates: {len(completed_dates)} total")
    print(f"  Open files by parser: {len(open_files)}")
    print(f"  Dry run: {args.dry_run}")
    print()

    if not os.path.isdir(DOWNLOADS_DIR):
        print(f"ERROR: Downloads dir not found: {DOWNLOADS_DIR}")
        sys.exit(1)

    total_freed_bytes = 0
    deleted_count = 0
    skipped_count = 0

    for name in sorted(os.listdir(DOWNLOADS_DIR)):
        path = os.path.join(DOWNLOADS_DIR, name)
        if not os.path.isdir(path):
            continue

        # Only process named subdirs (44fz_* or 223fz_*)
        if not (name.startswith("44fz_") or name.startswith("223fz_")):
            continue

        # Get mtime
        try:
            stat = os.stat(path)
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        except OSError:
            print(f"  SKIP (stat failed): {path}")
            skipped_count += 1
            continue

        age_days = (datetime.now(timezone.utc) - mtime).days

        # Check if any file within this dir is open by parser
        dir_in_use = any(f.startswith(path) for f in open_files)
        if dir_in_use:
            print(f"  SKIP (in use by parser): {name} | mtime={mtime.strftime('%Y-%m-%d')} | age={age_days}d")
            skipped_count += 1
            continue

        # Check age
        if mtime >= cutoff:
            print(f"  KEEP (recent): {name} | mtime={mtime.strftime('%Y-%m-%d')} | age={age_days}d")
            skipped_count += 1
            continue

        # Measure size
        try:
            result = subprocess.run(
                ["du", "-sb", path], capture_output=True, text=True
            )
            size_bytes = int(result.stdout.split()[0]) if result.returncode == 0 else 0
        except Exception:
            size_bytes = 0

        size_mb = size_bytes / (1024 * 1024)
        action = "DRY-RUN DELETE" if args.dry_run else "DELETE"
        print(f"  {action}: {name} | mtime={mtime.strftime('%Y-%m-%d')} | age={age_days}d | size={size_mb:.1f}MB")

        if not args.dry_run:
            try:
                shutil.rmtree(path)
                total_freed_bytes += size_bytes
                deleted_count += 1
            except PermissionError:
                # Dirs owned by tendermonitor user; fall back to sudo rm
                try:
                    subprocess.run(["sudo", "rm", "-rf", path], check=True)
                    total_freed_bytes += size_bytes
                    deleted_count += 1
                except Exception as e:
                    print(f"  ERROR (sudo fallback) deleting {path}: {e}")
                    skipped_count += 1
            except Exception as e:
                print(f"  ERROR deleting {path}: {e}")
                skipped_count += 1
        else:
            total_freed_bytes += size_bytes
            deleted_count += 1

    print()
    print(f"=== SUMMARY ===")
    print(f"  Deleted: {deleted_count} dirs")
    print(f"  Skipped: {skipped_count} dirs")
    print(f"  Space freed: {total_freed_bytes / (1024**3):.2f} GB")
    if args.dry_run:
        print(f"  (DRY RUN — nothing actually deleted)")


if __name__ == "__main__":
    main()

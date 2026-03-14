"""
download_emshr.py
-----------------
Download the NOAA HOMR EMSHR-Lite fixed-width file.

Usage:
    python scripts/download_emshr.py           # skip if already downloaded
    python scripts/download_emshr.py --force   # re-download even if file exists

Exit codes:
    0 — success
    1 — primary file download failed
"""

import argparse
import logging
import sys
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Paths — always resolved relative to this script so cwd doesn't matter
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "HOMR"
LOG_DIR = ROOT / "logs"

EMSHR_URL  = "https://www.ncei.noaa.gov/access/homr/file/emshr_lite.txt"
FORMAT_URL = "https://www.ncei.noaa.gov/access/homr/file/emshr_lite_format.txt"

CHUNK_SIZE = 1024 * 1024  # 1 MB


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("download_emshr")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(LOG_DIR / "downloads.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------
def download_file(url: str, dest: Path, logger: logging.Logger,
                  force: bool = False, required: bool = True) -> bool:
    """
    Stream-download *url* to *dest*.

    Returns True on success, False on failure.
    If *required* is False a 404/error only emits a warning instead of an error.
    """
    if dest.exists() and not force:
        logger.info("SKIP  %s already exists (use --force to re-download)", dest.name)
        return True

    logger.info("GET   %s", url)
    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            if resp.status_code == 404:
                msg = "404 Not Found: %s"
                if required:
                    logger.error(msg, url)
                else:
                    logger.warning(msg, url)
                return False
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            dest.parent.mkdir(parents=True, exist_ok=True)

            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    fh.write(chunk)
                    downloaded += len(chunk)

            size_mb = downloaded / 1024 / 1024
            logger.info("DONE  %s saved (%.1f MB)", dest.name, size_mb)
            return True

    except requests.RequestException as exc:
        if required:
            logger.error("FAIL  %s  —  %s", dest.name, exc)
        else:
            logger.warning("WARN  %s  —  %s", dest.name, exc)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Download NOAA HOMR EMSHR-Lite files.")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if the file already exists.")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=== download_emshr.py  (ROOT=%s) ===", ROOT)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Primary file — required
    ok = download_file(EMSHR_URL, RAW_DIR / "emshr_lite.txt",
                       logger, force=args.force, required=True)
    if not ok:
        logger.error("Primary download failed. Cannot proceed.")
        return 1

    # Format documentation — optional
    download_file(FORMAT_URL, RAW_DIR / "emshr_lite_format.txt",
                  logger, force=args.force, required=False)

    logger.info("=== download_emshr.py complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())

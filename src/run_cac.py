"""
run_cac.py
----------
Standalone script to regenerate the cac_summary tab only.
Reads existing data from meta_spend and airtable_leads tabs —
no Meta API or Airtable calls needed.

Usage:
    python src/run_cac.py
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

LOG_FILE = Path(__file__).parent.parent / "sync.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)

logger = logging.getLogger("run_cac")

load_dotenv()


def _sheet_to_records(ws) -> list[dict]:
    """Convert a worksheet's rows into a list of dicts keyed by header."""
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        return []
    header = all_values[0]
    return [dict(zip(header, row)) for row in all_values[1:]]


def run():
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        logger.error("GOOGLE_SHEET_ID not set.")
        sys.exit(1)

    from gsheet_writer import _get_client
    from cac_summary import write_cac_summary
    from meta_api import get_active_names

    logger.info("Connecting to Google Sheets …")
    client = _get_client()
    sh = client.open_by_key(sheet_id)

    logger.info("Reading meta_spend …")
    meta_records = _sheet_to_records(sh.worksheet("meta_spend"))
    logger.info("  → %d rows", len(meta_records))

    logger.info("Reading airtable_leads …")
    airtable_records = _sheet_to_records(sh.worksheet("airtable_leads"))
    logger.info("  → %d rows", len(airtable_records))

    logger.info("Fetching active campaign/adset/ad names from Meta …")
    active_names = get_active_names()

    logger.info("Rewriting cac_summary …")
    ws_cac = sh.worksheet("cac_summary")
    write_cac_summary(meta_records, airtable_records, ws_cac, active_names)
    logger.info("Done.")


if __name__ == "__main__":
    run()

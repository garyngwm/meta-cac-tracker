"""
main.py
-------
Orchestrator for the Meta CAC Tracker daily sync pipeline.

Steps:
  1. Detect first run vs subsequent run (via Google Sheet state).
  2. Pull Meta Ads data.
  3. Pull Airtable leads.
  4. Upsert both datasets into Google Sheets.
  5. Log outcome to sync.log.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging setup — writes to both stdout and sync.log
# ---------------------------------------------------------------------------

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

logger = logging.getLogger("main")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------

# Load .env when running locally; in CI the env vars are injected directly.
load_dotenv()

_REQUIRED_VARS = [
    "META_APP_ID",
    "META_APP_SECRET",
    "META_ACCESS_TOKEN",
    "META_AD_ACCOUNT_ID",
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
    "AIRTABLE_TABLE_NAME",
    "GOOGLE_SHEET_ID",
    "GOOGLE_CREDENTIALS_JSON",
]


def _check_env() -> None:
    missing = [v for v in _REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run() -> None:
    run_start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Meta CAC Tracker sync started at %s", run_start.isoformat())

    _check_env()

    sheet_id = os.environ["GOOGLE_SHEET_ID"]

    # ------------------------------------------------------------------
    # Step 1: Detect first run
    # ------------------------------------------------------------------
    logger.info("Step 1/4 — Checking Google Sheet state …")
    try:
        from gsheet_writer import is_meta_spend_empty
        first_run = is_meta_spend_empty(sheet_id)
    except Exception as exc:
        logger.error("Could not read Google Sheet to detect first run: %s", exc)
        raise

    if first_run:
        logger.info("First run detected — will pull 90 days of historical Meta data.")
    else:
        logger.info("Subsequent run detected — will pull previous day only.")

    # ------------------------------------------------------------------
    # Step 2: Pull Meta Ads data
    # ------------------------------------------------------------------
    logger.info("Step 2/4 — Pulling Meta Ads data …")
    try:
        from meta_api import pull_meta_data
        meta_records = pull_meta_data(is_first_run=first_run)
    except Exception as exc:
        logger.error("Meta API pull failed: %s", exc)
        raise

    # ------------------------------------------------------------------
    # Step 3: Pull Airtable leads
    # ------------------------------------------------------------------
    logger.info("Step 3/4 — Pulling Airtable leads …")
    try:
        from airtable_api import pull_airtable_data
        airtable_records = pull_airtable_data()
    except Exception as exc:
        logger.error("Airtable pull failed: %s", exc)
        raise

    # ------------------------------------------------------------------
    # Step 4: Write to Google Sheets
    # ------------------------------------------------------------------
    logger.info("Step 4/5 — Writing meta_spend and airtable_leads …")
    try:
        from gsheet_writer import write_meta_spend, write_airtable_leads, _get_client
        write_meta_spend(meta_records, sheet_id)
        write_airtable_leads(airtable_records, sheet_id)
    except Exception as exc:
        logger.error("Google Sheets write failed: %s", exc)
        raise

    # ------------------------------------------------------------------
    # Step 5: Rewrite CAC summary
    # ------------------------------------------------------------------
    logger.info("Step 5/5 — Rewriting CAC summary …")
    try:
        from meta_api import get_active_names
        from cac_summary import write_cac_summary
        from gsheet_writer import read_all_meta_spend, read_all_airtable_leads
        active_names = get_active_names()
        all_meta_records = read_all_meta_spend(sheet_id)
        all_airtable_records = read_all_airtable_leads(sheet_id)
        logger.info(
            "Loaded full history — %d meta rows, %d airtable leads",
            len(all_meta_records), len(all_airtable_records),
        )
        client = _get_client()
        sh = client.open_by_key(sheet_id)
        ws_cac = sh.worksheet("cac_summary")
        write_cac_summary(all_meta_records, all_airtable_records, ws_cac, active_names)
    except Exception as exc:
        logger.error("CAC summary write failed: %s", exc)
        raise

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    duration = (datetime.now(timezone.utc) - run_start).total_seconds()
    logger.info(
        "Sync complete in %.1fs — %d Meta rows, %d Airtable leads, CAC summary rewritten",
        duration,
        len(meta_records),
        len(airtable_records),
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    run()

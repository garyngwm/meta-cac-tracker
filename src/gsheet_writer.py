"""
gsheet_writer.py
----------------
Upserts rows into Google Sheets tabs.

Rules:
- meta_spend   : upsert key = ad_id + "_" + date
- airtable_leads: upsert key = AirtableID
- cac_summary  : NEVER touched — contains manual formulas
"""

import json
import logging
import os
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Column definitions — order matters (matches sheet header row)
META_SPEND_COLUMNS = [
    "upsert_key",    # hidden key column A — used for upsert matching
    "date",
    "month",
    "campaign_name",
    "adset_name",
    "ad_name",
    "ad_id",
    "spend",
    "impressions",
    "clicks",
    "reach",
    "cpm",
    "cpc",
    "ctr",
]

AIRTABLE_LEADS_COLUMNS = [
    "AirtableID",    # column A — upsert key
    "Name",
    "Created",
    "Source",
    "Campaign",
    "AdSet",
    "Ads",
    "Placement",
    "Stage",
    "Outlet",
    "ShowUpDate",
    "ConvertedDate",
    "Status",
    "Month",
]


def _get_client() -> gspread.Client:
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
    return gspread.authorize(creds)


def _ensure_header(ws: gspread.Worksheet, columns: list[str]) -> None:
    """Write header row if the sheet is empty."""
    existing = ws.row_values(1)
    if not existing:
        ws.append_row(columns, value_input_option="RAW")
        logger.info("Wrote header row to sheet '%s'", ws.title)


def _record_to_row(record: dict, columns: list[str]) -> list[Any]:
    return [str(record.get(col, "")) for col in columns]


def upsert_rows(
    ws: gspread.Worksheet,
    records: list[dict],
    columns: list[str],
    key_col: str,
) -> None:
    """
    Upsert records into worksheet.

    - Matches on key_col value in the first column (column A).
    - Updates existing rows in place.
    - Appends new rows at the bottom.
    """
    if not records:
        logger.info("No records to upsert into '%s'", ws.title)
        return

    _ensure_header(ws, columns)

    # Load all existing data (skip header row)
    all_values = ws.get_all_values()
    header = all_values[0] if all_values else []
    data_rows = all_values[1:] if len(all_values) > 1 else []

    # Determine key column index in the sheet
    try:
        key_col_idx = header.index(key_col)
    except ValueError:
        # Header doesn't match — re-write header and treat all records as new
        logger.warning("Header mismatch in '%s', re-writing header.", ws.title)
        ws.update("A1", [columns])
        data_rows = []
        key_col_idx = 0

    # Build index: key_value → sheet row number (1-indexed, header is row 1)
    existing_index: dict[str, int] = {}
    for i, row in enumerate(data_rows):
        if key_col_idx < len(row):
            existing_index[row[key_col_idx]] = i + 2  # +2: 1-indexed + skip header

    # Determine the key column index in our columns list
    record_key_idx = columns.index(key_col)

    updates: list[tuple[int, list]] = []
    inserts: list[list] = []

    for record in records:
        row_values = _record_to_row(record, columns)
        key_value = row_values[record_key_idx]
        if key_value in existing_index:
            updates.append((existing_index[key_value], row_values))
        else:
            inserts.append(row_values)

    # Batch update existing rows
    if updates:
        cell_updates = []
        for sheet_row, row_values in updates:
            for col_idx, cell_value in enumerate(row_values):
                cell_updates.append(
                    gspread.Cell(row=sheet_row, col=col_idx + 1, value=cell_value)
                )
        ws.update_cells(cell_updates, value_input_option="RAW")
        logger.info("Updated %d existing rows in '%s'", len(updates), ws.title)

    # Append new rows
    if inserts:
        ws.append_rows(inserts, value_input_option="RAW")
        logger.info("Appended %d new rows to '%s'", len(inserts), ws.title)


def write_meta_spend(records: list[dict], sheet_id: str) -> None:
    client = _get_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet("meta_spend")
    upsert_rows(ws, records, META_SPEND_COLUMNS, key_col="upsert_key")


def write_airtable_leads(records: list[dict], sheet_id: str) -> None:
    client = _get_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet("airtable_leads")
    upsert_rows(ws, records, AIRTABLE_LEADS_COLUMNS, key_col="AirtableID")


def read_all_meta_spend(sheet_id: str) -> list[dict]:
    """Reads all rows from meta_spend and returns them as a list of dicts.
    Used by the CAC summary to get full historical spend, not just this run's pull.
    """
    client = _get_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet("meta_spend")
    return ws.get_all_records()


def read_all_airtable_leads(sheet_id: str) -> list[dict]:
    """Reads all rows from airtable_leads and returns them as a list of dicts.
    Used by the CAC summary to get full historical leads, not just the 120-day pull.
    """
    client = _get_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet("airtable_leads")
    return ws.get_all_records()


def is_meta_spend_empty(sheet_id: str) -> bool:
    """Returns True if meta_spend has no data rows (used to detect first run)."""
    client = _get_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet("meta_spend")
    values = ws.get_all_values()
    row_count = len(values)
    logger.info("meta_spend row count (including header): %d", row_count)
    # Empty or header-only
    return row_count <= 1

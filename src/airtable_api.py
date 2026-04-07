"""
airtable_api.py
---------------
Pulls lead records from Airtable.

- Pulls from 'Fb (CAC)' view — already filtered to FB leads.
- Handles pagination automatically via pyairtable.
- Computes Month (Mmm YYYY) from Created date.
- Standardises all dates to YYYY-MM-DD.
"""

import logging
import os
import re
from datetime import datetime
from typing import Optional

from pyairtable import Api

logger = logging.getLogger(__name__)


def _parse_date(value) -> str:
    """Normalise any date-like string to YYYY-MM-DD. Returns '' on failure.
    Handles list values from Airtable lookup fields by taking the first element.
    """
    if isinstance(value, list):
        value = value[0] if value else ""
    if not value:
        return ""
    value = str(value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return value
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.debug("Could not parse date value: %s", value)
    return value


def _unwrap(value) -> str:
    """Unwraps list values from Airtable lookup fields into a plain string."""
    if isinstance(value, list):
        return value[0] if value else ""
    return str(value) if value else ""


def _month_from_date(date_str: str) -> str:
    """Returns 'Mmm YYYY' (e.g. 'Apr 2026') from a YYYY-MM-DD string."""
    if not date_str:
        return ""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %Y")
    except ValueError:
        return ""


def pull_airtable_data() -> list[dict]:
    """
    Returns a list of dicts, one per lead in the Fb (CAC) view.
    """
    api_key = os.environ["AIRTABLE_API_KEY"]
    base_id = os.environ["AIRTABLE_BASE_ID"]
    table_name = os.environ["AIRTABLE_TABLE_NAME"]

    api = Api(api_key)
    table = api.table(base_id, table_name)

    logger.info("Fetching Airtable records from base %s / table %s / view 'Fb (CAC)' …", base_id, table_name)
    all_records = table.all(view="Fb (CAC)")
    logger.info("Fetched %d records from view", len(all_records))

    leads: list[dict] = []

    for record in all_records:
        fields = record.get("fields", {})
        airtable_id = record["id"]

        created = _parse_date(fields.get("created", ""))

        leads.append(
            {
                "AirtableID": airtable_id,
                "Name": fields.get("Name", ""),
                "Created": created,
                "Source": fields.get("source", ""),
                "Campaign": fields.get("campaign", ""),
                "AdSet": fields.get("fb_ads", ""),
                "Ads": fields.get("ga_matchtype", ""),
                "Placement": fields.get("fb_placement", ""),
                "Stage": _unwrap(fields.get("Stage", "")),
                "Outlet": _unwrap(fields.get("Outlet", "")),
                "ShowUpDate": _parse_date(fields.get("Trial 1 - Showup Date (from Trial Arranged)", "")),
                "ConvertedDate": _parse_date(fields.get("Date Joined Membership", "")),
                "Status": _unwrap(fields.get("Current Trial Status", "")),
                "Month": _month_from_date(created),
            }
        )

    logger.info("Airtable pull complete: %d leads fetched", len(leads))
    return leads

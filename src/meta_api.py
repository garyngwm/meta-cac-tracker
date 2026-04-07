"""
meta_api.py
-----------
Pulls Meta Ads insights at ad level using the facebook-business SDK.

First run  : pulls last 90 days (detected via is_first_run flag from main.py).
Subsequent : pulls previous day only.
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional

from datetime import datetime

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logger = logging.getLogger(__name__)

# Error codes that indicate an expired / invalid token
_TOKEN_ERROR_CODES = {190, 102, 463, 467}

INSIGHT_FIELDS = [
    "date_start",
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


def _init_api() -> None:
    FacebookAdsApi.init(
        app_id=os.environ["META_APP_ID"],
        app_secret=os.environ["META_APP_SECRET"],
        access_token=os.environ["META_ACCESS_TOKEN"],
    )


def _get_date_range(is_first_run: bool) -> tuple[str, str]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    if is_first_run:
        since = today - timedelta(days=90)
        logger.info("First run detected — pulling last 90 days (%s to %s)", since, yesterday)
    else:
        since = yesterday
        logger.info("Subsequent run — pulling previous day (%s)", yesterday)
    return str(since), str(yesterday)


def get_active_names() -> dict[str, set]:
    """
    Returns sets of currently ACTIVE campaign, adset, and ad names.
    Used to filter the CAC summary to only show running campaigns.
    """
    _init_api()
    account = AdAccount(os.environ["META_AD_ACCOUNT_ID"])

    active: dict[str, set] = {"campaigns": set(), "adsets": set(), "ads": set()}

    try:
        for c in account.get_campaigns(fields=["name", "effective_status"]):
            if c.get("effective_status") == "ACTIVE":
                active["campaigns"].add(c["name"])

        for a in account.get_ad_sets(fields=["name", "effective_status"]):
            if a.get("effective_status") == "ACTIVE":
                active["adsets"].add(a["name"])

        for ad in account.get_ads(fields=["name", "effective_status"]):
            if ad.get("effective_status") == "ACTIVE":
                active["ads"].add(ad["name"])

    except FacebookRequestError as exc:
        code = exc.api_error_code()
        if code in _TOKEN_ERROR_CODES:
            logger.error(
                "META TOKEN EXPIRED OR INVALID (error %s). Cannot fetch active names.",
                code,
            )
        else:
            logger.error("Meta API error fetching active names — %s: %s", code, exc.api_error_message())
        raise

    logger.info(
        "Active names — %d campaigns, %d adsets, %d ads",
        len(active["campaigns"]), len(active["adsets"]), len(active["ads"]),
    )
    return active


def pull_meta_data(is_first_run: bool = False) -> list[dict]:
    """
    Returns a list of dicts, one per ad per day, with the fields listed in
    INSIGHT_FIELDS plus a composite upsert_key = ad_id + "_" + date.
    """
    _init_api()

    since, until = _get_date_range(is_first_run)

    params = {
        "level": "ad",
        "time_increment": 1,
        "time_range": {"since": since, "until": until},
    }

    ad_account_id = os.environ["META_AD_ACCOUNT_ID"]
    account = AdAccount(ad_account_id)

    try:
        cursor = account.get_insights(fields=INSIGHT_FIELDS, params=params)
        rows = list(cursor)
    except FacebookRequestError as exc:
        code = exc.api_error_code()
        msg = exc.api_error_message()
        if code in _TOKEN_ERROR_CODES:
            logger.error(
                "META TOKEN EXPIRED OR INVALID (error %s: %s). "
                "Generate a new long-lived token and update META_ACCESS_TOKEN. "
                "See README for refresh instructions.",
                code,
                msg,
            )
        else:
            logger.error("Meta API error %s: %s", code, msg)
        raise

    records: list[dict] = []
    for row in rows:
        raw_date = row.get("date_start", "")
        ad_id = row.get("ad_id", "")
        try:
            month = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%b %Y")
        except ValueError:
            month = ""
        records.append(
            {
                "upsert_key": f"{ad_id}_{raw_date}",
                "date": raw_date,
                "month": month,
                "campaign_name": row.get("campaign_name", ""),
                "adset_name": row.get("adset_name", ""),
                "ad_name": row.get("ad_name", ""),
                "ad_id": ad_id,
                "spend": row.get("spend", "0"),
                "impressions": row.get("impressions", "0"),
                "clicks": row.get("clicks", "0"),
                "reach": row.get("reach", "0"),
                "cpm": row.get("cpm", "0"),
                "cpc": row.get("cpc", "0"),
                "ctr": row.get("ctr", "0"),
            }
        )

    logger.info("Pulled %d Meta ad rows (%s → %s)", len(records), since, until)
    return records

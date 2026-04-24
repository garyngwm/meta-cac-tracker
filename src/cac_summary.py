"""
cac_summary.py
--------------
Computes and writes the CAC summary tab.

Full rewrite every run — clears and regenerates from scratch.
Data is joined from meta_records (spend) and airtable_records (leads).

Sections: Campaign → AdSet → Ad, each with monthly breakdown + lifetime Overall.
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

import gspread

logger = logging.getLogger(__name__)

COLUMNS = ["Month", "Name", "Spend", "Leads", "Show-up", "Conversions", "CPL", "CPSU", "CAC", "L-SU%", "Conv%"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_div(numerator: float, denominator: float):
    if not denominator:
        return ""
    return round(numerator / denominator, 2)


def _pct(numerator: float, denominator: float):
    if not denominator:
        return ""
    return round(numerator / denominator * 100, 2)


def _month_sort_key(month_str: str) -> datetime:
    try:
        return datetime.strptime(month_str, "%b %Y")
    except ValueError:
        return datetime.min


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _normalize_ad_id(value) -> str:
    """Normalises ad_id to plain integer string — handles floats from gspread."""
    if not value:
        return ""
    try:
        return str(int(float(str(value).strip())))
    except (ValueError, TypeError):
        return str(value).strip()


def _build_ad_id_lookup(meta_records: list[dict]) -> dict[str, dict]:
    """
    Builds a lookup: ad_id -> {campaign_name, adset_name, ad_name}
    from meta_spend records. Used for iOS-safe attribution in Airtable leads.
    """
    lookup: dict[str, dict] = {}
    for r in meta_records:
        aid = _normalize_ad_id(r.get("ad_id"))
        if aid:
            lookup[aid] = {
                "campaign_name": str(r.get("campaign_name") or "").strip(),
                "adset_name":    str(r.get("adset_name") or "").strip(),
                "ad_name":       str(r.get("ad_name") or "").strip(),
            }
    return lookup


def _aggregate(
    meta_records: list[dict],
    airtable_records: list[dict],
    meta_key: str,
    airtable_key: str,
    active_names: set | None = None,
    ad_id_lookup: dict | None = None,
) -> dict[tuple, dict]:
    """
    Joins spend (meta) and leads/showups/conversions (airtable) on (month, name).
    Returns dict keyed by (month, name).

    Attribution priority:
    1. ad_id lookup (iOS-safe, set by Meta directly)
    2. UTM name field fallback (for older leads without ad_id)
    """
    spend_by: dict[tuple, float] = defaultdict(float)
    for r in meta_records:
        month = str(r.get("month") or "").strip()
        name = str(r.get(meta_key) or "").strip()
        if not month or not name:
            continue
        if active_names is not None and name not in active_names:
            continue
        try:
            spend_by[(month, name)] += float(r.get("spend") or 0)
        except (ValueError, TypeError):
            pass

    leads_by: dict[tuple, dict] = defaultdict(lambda: {"leads": 0, "showups": 0, "conversions": 0})
    for r in airtable_records:
        month = str(r.get("Month") or "").strip()

        # Priority 1: ad_id lookup (iOS-safe)
        aid = _normalize_ad_id(r.get("ad_id"))
        if ad_id_lookup and aid and aid in ad_id_lookup:
            name = str(ad_id_lookup[aid].get(meta_key) or "").strip()
        else:
            # Priority 2: UTM name fallback
            name = str(r.get(airtable_key) or "").strip()

        if not month or not name:
            continue
        if active_names is not None and name not in active_names:
            continue
        leads_by[(month, name)]["leads"] += 1
        if r.get("ShowUpDate"):
            leads_by[(month, name)]["showups"] += 1
        if r.get("ConvertedDate"):
            leads_by[(month, name)]["conversions"] += 1

    all_keys = set(spend_by.keys()) | set(leads_by.keys())
    result = {}
    for key in all_keys:
        lb = leads_by.get(key, {"leads": 0, "showups": 0, "conversions": 0})
        result[key] = {
            "spend": spend_by.get(key, 0.0),
            "leads": lb["leads"],
            "showups": lb["showups"],
            "conversions": lb["conversions"],
        }
    return result


# ---------------------------------------------------------------------------
# Section builder
# ---------------------------------------------------------------------------

def _build_section(data: dict, level_label: str) -> list[list]:
    rows: list[list] = []

    # Section title + column headers
    rows.append([f"── {level_label.upper()} LEVEL ──"])
    rows.append(COLUMNS)

    months = sorted({k[0] for k in data}, key=_month_sort_key)

    for month in months:
        month_data = {k: v for k, v in data.items() if k[0] == month}
        names = sorted(k[1] for k in month_data)

        m_spend = m_leads = m_showups = m_conv = 0.0

        for name in names:
            d = month_data[(month, name)]
            spend, leads, showups, conv = d["spend"], d["leads"], d["showups"], d["conversions"]
            m_spend += spend
            m_leads += leads
            m_showups += showups
            m_conv += conv

            rows.append([
                month, name,
                round(spend, 2), leads, showups, conv,
                _safe_div(spend, leads),
                _safe_div(spend, showups),
                _safe_div(spend, conv),
                _pct(showups, leads),
                _pct(conv, leads),
            ])

        # Monthly total
        rows.append([
            month, "— Total —",
            round(m_spend, 2), int(m_leads), int(m_showups), int(m_conv),
            _safe_div(m_spend, m_leads),
            _safe_div(m_spend, m_showups),
            _safe_div(m_spend, m_conv),
            _pct(m_showups, m_leads),
            _pct(m_conv, m_leads),
        ])
        rows.append([""] * len(COLUMNS))  # blank spacer between months

    # ------------------------------------------------------------------
    # Overall (lifetime) section
    # ------------------------------------------------------------------
    rows.append([""] * len(COLUMNS))
    rows.append([f"── {level_label.upper()} — OVERALL (Lifetime) ──"])
    rows.append(COLUMNS)

    overall: dict[str, dict] = defaultdict(lambda: {"spend": 0.0, "leads": 0, "showups": 0, "conversions": 0})
    for (_, name), d in data.items():
        overall[name]["spend"] += d["spend"]
        overall[name]["leads"] += d["leads"]
        overall[name]["showups"] += d["showups"]
        overall[name]["conversions"] += d["conversions"]

    grand_spend = grand_leads = grand_showups = grand_conv = 0.0

    for name in sorted(overall):
        d = overall[name]
        grand_spend += d["spend"]
        grand_leads += d["leads"]
        grand_showups += d["showups"]
        grand_conv += d["conversions"]

        rows.append([
            "Overall", name,
            round(d["spend"], 2), d["leads"], d["showups"], d["conversions"],
            _safe_div(d["spend"], d["leads"]),
            _safe_div(d["spend"], d["showups"]),
            _safe_div(d["spend"], d["conversions"]),
            _pct(d["showups"], d["leads"]),
            _pct(d["conversions"], d["leads"]),
        ])

    # Grand total row
    rows.append([
        "Overall", "── Grand Total ──",
        round(grand_spend, 2), int(grand_leads), int(grand_showups), int(grand_conv),
        _safe_div(grand_spend, grand_leads),
        _safe_div(grand_spend, grand_showups),
        _safe_div(grand_spend, grand_conv),
        _pct(grand_showups, grand_leads),
        _pct(grand_conv, grand_leads),
    ])

    # Spacer before next section
    rows.append([""] * len(COLUMNS))
    rows.append([""] * len(COLUMNS))
    rows.append([""] * len(COLUMNS))

    return rows


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def write_cac_summary(
    meta_records: list[dict],
    airtable_records: list[dict],
    ws: gspread.Worksheet,
    active_names: dict[str, set] | None = None,
) -> None:
    """
    Clears and fully rewrites the cac_summary worksheet.
    Called from main.py with the in-memory records already pulled this run.
    If active_names is provided, only currently active campaigns/adsets/ads are included.
    """
    logger.info("Computing CAC summary …")

    # Build ad_id lookup once — used across all three sections
    ad_id_lookup = _build_ad_id_lookup(meta_records)
    logger.info("Built ad_id lookup: %d unique ads", len(ad_id_lookup))

    all_rows: list[list] = []

    sections = [
        ("Campaign", "campaign_name", "Campaign", active_names.get("campaigns") if active_names else None),
        ("AdSet",    "adset_name",    "AdSet",    active_names.get("adsets")    if active_names else None),
        ("Ad",       "ad_name",       "Ads",      active_names.get("ads")       if active_names else None),
    ]

    for level_label, meta_key, airtable_key, names_filter in sections:
        data = _aggregate(meta_records, airtable_records, meta_key, airtable_key, names_filter, ad_id_lookup)
        all_rows.extend(_build_section(data, level_label))
        logger.info(
            "%s level: %d unique (month, name) combinations",
            level_label, len(data),
        )

    ws.clear()
    if all_rows:
        # Convert all values to strings/numbers safe for Sheets
        safe_rows = [
            [str(cell) if not isinstance(cell, (int, float)) else cell for cell in row]
            for row in all_rows
        ]
        ws.update("A1", safe_rows, value_input_option="RAW")

    logger.info("CAC summary written: %d rows total", len(all_rows))

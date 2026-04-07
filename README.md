# Meta CAC Tracker

A daily automated pipeline that pulls Meta Ads spend and Airtable lead data, writes both to Google Sheets, and computes a CAC (Customer Acquisition Cost) summary — broken down by campaign, adset, and ad level.

Runs automatically every day at **9am SGT (1am UTC)** via GitHub Actions.

---

## What This Project Does

| Step | Action |
|------|--------|
| 1 | Detects first run vs subsequent run by checking whether `meta_spend` already has data |
| 2 | Pulls Meta Ads data at ad level, daily granularity, via the Marketing API |
| 3 | Pulls lead data from a specified Airtable view |
| 4 | Upserts both datasets into Google Sheets (`meta_spend` and `airtable_leads` tabs) |
| 5 | Clears and regenerates the `cac_summary` tab with monthly + lifetime CAC metrics |

On first run, the pipeline backfills the last 90 days of Meta data. On subsequent runs, it pulls the previous day only.

---

## Project Structure

```
/meta-cac-tracker
  /src
    meta_api.py        # Meta Marketing API pull + active campaign detection
    airtable_api.py    # Airtable pull
    gsheet_writer.py   # Google Sheets upsert logic
    cac_summary.py     # CAC summary computation and write
    main.py            # Full pipeline orchestrator
    run_cac.py         # Standalone script to regenerate cac_summary only
  /.github/workflows
    daily_sync.yml     # GitHub Actions cron job
  .env.example         # Template for required environment variables
  .gitignore
  requirements.txt
  README.md
```

---

## Google Sheets Output

The sheet must have three tabs named exactly as below (case-sensitive):

| Tab | Written by | Description |
|-----|------------|-------------|
| `meta_spend` | Upsert (key: `ad_id + date`) | Daily ad-level spend data from Meta |
| `airtable_leads` | Upsert (key: `AirtableID`) | Lead records synced from Airtable |
| `cac_summary` | Full rewrite every run | Auto-generated CAC summary — do not manually edit |

Headers for `meta_spend` and `airtable_leads` are written automatically on first run.

The `cac_summary` tab contains three stacked sections (Campaign → AdSet → Ad), each with a month-by-month breakdown and a lifetime Overall section. Only currently active campaigns/adsets/ads are included.

---

## Required APIs

### 1. Meta Marketing API

**What you need:** App ID, App Secret, long-lived Access Token, Ad Account ID

**Steps:**
1. Go to [developers.facebook.com](https://developers.facebook.com) and log in
2. Click **My Apps → Create App** and select **Business** type
3. From the app dashboard, go to **Settings → Basic** — note your `App ID` and `App Secret`
4. Add the **Marketing API** product to your app
5. Go to **Tools → Graph API Explorer**, select your app, click **Generate Access Token**, and add the `ads_read` permission
6. Convert to a long-lived token (~60 days) by calling:
   ```
   GET https://graph.facebook.com/v19.0/oauth/access_token
     ?grant_type=fb_exchange_token
     &client_id={APP_ID}
     &client_secret={APP_SECRET}
     &fb_exchange_token={SHORT_LIVED_TOKEN}
   ```
   Copy the `access_token` from the response
7. Your **Ad Account ID** is in Meta Ads Manager — top-left account selector. Format: `act_XXXXXXXXX`

> **Token expiry:** Long-lived tokens expire every ~60 days. When expired, the pipeline logs `META TOKEN EXPIRED OR INVALID`. Repeat steps 5–6 and update `META_ACCESS_TOKEN` in your `.env` or GitHub Secrets.

---

### 2. Airtable

**What you need:** Personal Access Token, Base ID, Table Name

**Steps:**
1. Go to [airtable.com/create/tokens](https://airtable.com/create/tokens) and click **Create token**
2. Add the scope `data.records:read`
3. Under **Access**, select the base you want to pull from
4. Copy the generated token — this is your `AIRTABLE_API_KEY`

**Finding your Base ID:**
Open your Airtable base in the browser. The URL looks like:
```
https://airtable.com/appXXXXXXXXXXXXXX/tblYYYYYYYY/...
```
The `appXXXX...` segment is your `AIRTABLE_BASE_ID`.

**Table Name:** The exact display name of the table as it appears in Airtable (e.g. `Lead`).

---

### 3. Google Sheets

**What you need:** Service account credentials JSON, Sheet ID

**Steps:**
1. Go to [console.cloud.google.com](https://console.cloud.google.com) and create or select a project
2. Go to **APIs & Services → Library** and enable:
   - **Google Sheets API**
   - **Google Drive API**
3. Go to **IAM & Admin → Service Accounts → Create Service Account**
4. Give it a name (e.g. `meta-cac-tracker`), click through and save
5. Click the service account → **Keys tab → Add Key → Create new key → JSON**
6. A `.json` file will download — this contains your `GOOGLE_CREDENTIALS_JSON`
7. Open the file and find the `"client_email"` field
8. Open your Google Sheet → **Share** → paste that email address → give it **Editor** access

**Finding your Sheet ID:**
The Sheet ID is the string between `/d/` and `/edit` in your Google Sheets URL:
```
https://docs.google.com/spreadsheets/d/{YOUR_SHEET_ID}/edit
```

**Setting `GOOGLE_CREDENTIALS_JSON`:**
Open the downloaded `.json` file in a text editor, copy the entire contents, and paste it into your `.env` wrapped in single quotes:
```
GOOGLE_CREDENTIALS_JSON='{"type":"service_account","project_id":"...","private_key":"...",...}'
```
For GitHub Actions, paste the full JSON directly into the secret value field.

---

### 4. Anthropic (Reserved for Future Use)

**What you need:** API Key

1. Go to [console.anthropic.com](https://console.anthropic.com)
2. Navigate to **API Keys → Create Key**
3. Copy and store it as `ANTHROPIC_API_KEY`

---

## Environment Variable Setup

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```dotenv
META_APP_ID=""
META_APP_SECRET=""
META_ACCESS_TOKEN=""
META_AD_ACCOUNT_ID=""        # format: act_XXXXXXXXX

AIRTABLE_API_KEY=""
AIRTABLE_BASE_ID=""
AIRTABLE_TABLE_NAME=""

GOOGLE_SHEET_ID=""
GOOGLE_CREDENTIALS_JSON=''   # full JSON contents of service account key, single-quoted

ANTHROPIC_API_KEY=""         # reserved for future use
```

> Never commit `.env` or your credentials JSON file to Git — both are listed in `.gitignore`.

---

## How to Run Locally

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Run the full pipeline (steps 1–5):**
Pulls fresh data from Meta and Airtable, upserts `meta_spend` and `airtable_leads`, then regenerates `cac_summary`.
```bash
python src/main.py
```

**Regenerate CAC summary only (step 5):**
Reads whatever is already in `meta_spend` and `airtable_leads` — no Meta or Airtable API calls. Use this when you want to refresh the summary without re-pulling data.
```bash
python src/run_cac.py
```

> Always use `main.py` for the daily sync. `run_cac.py` is a convenience script only — it does not update the underlying data.

Logs are written to `sync.log` in the project root (gitignored) and printed to stdout.

---

## GitHub Actions Setup

The pipeline runs automatically at **9am SGT (1am UTC)** daily via `.github/workflows/daily_sync.yml`.

**Adding secrets to your repository:**
1. Go to your GitHub repo → **Settings → Secrets and variables → Actions**
2. Click **New repository secret** for each of the following:

| Secret Name | Value |
|-------------|-------|
| `META_APP_ID` | Your Meta App ID |
| `META_APP_SECRET` | Your Meta App Secret |
| `META_ACCESS_TOKEN` | Your long-lived Meta access token |
| `META_AD_ACCOUNT_ID` | Your ad account ID (`act_XXXXXXXXX`) |
| `AIRTABLE_API_KEY` | Your Airtable Personal Access Token |
| `AIRTABLE_BASE_ID` | Your Airtable Base ID |
| `AIRTABLE_TABLE_NAME` | Your Airtable table name |
| `GOOGLE_SHEET_ID` | Your Google Sheet ID |
| `GOOGLE_CREDENTIALS_JSON` | Full contents of your service account JSON |
| `ANTHROPIC_API_KEY` | Your Anthropic API key |

To trigger a run manually: **Actions → Daily Meta CAC Sync → Run workflow**.

---

## Compliance & Security

| Rule | Detail |
|------|--------|
| Never commit `.env` | Listed in `.gitignore` |
| Never commit credentials JSON | All `*.json` credential files are gitignored |
| Meta token rotation | Expires every ~60 days. Renew via Graph API Explorer and update `META_ACCESS_TOKEN`. Pipeline logs a clear error when expired. |
| Airtable token | Personal Access Tokens do not expire unless revoked. Rotate periodically as best practice. |
| Google service account | Keys do not expire but should be rotated annually. Delete old keys in Cloud Console after rotating. |
| `sync.log` | Gitignored — not pushed to remote. Review locally or via GitHub Actions stdout. |

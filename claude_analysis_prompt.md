# Claude Analysis Prompt — Meta CAC Tracker

Use this prompt when pasting your CAC summary data into Claude Desktop for analysis.
Export the `cac_summary` tab as a CSV and attach it, or paste the data directly.

---

## Prompt Template

```
I'm running Meta Ads for an aesthetics clinic. Below is my CAC tracker data pulled from Meta Ads and our CRM (Airtable). It includes spend, leads, show-ups, conversions, CPL (cost per lead), CPSU (cost per show-up), CAC (cost per acquisition), lead-to-show-up rate, and conversion rate — broken down by campaign, adset, and ad level.

[PASTE CSV DATA HERE or attach file]

Please analyse this data across the following dimensions:

---

**1. Month-on-Month Analysis**
- How has overall spend, leads, show-ups, and conversions changed each month?
- Which months had the best and worst CAC, CPL, and conversion rates?
- Is performance trending up or down over time?

**2. Week-on-Week Analysis** (if data is granular enough)
- Are there any notable week-on-week shifts in CTR, CPL, or CAC?
- Any signs of ad fatigue or audience saturation?

**3. What's Working Well**
- Which campaigns, adsets, or ads have the lowest CAC and highest conversion rates?
- Which have strong lead-to-show-up rates?
- Which should we consider scaling budget on?

**4. What's Not Working**
- Which campaigns, adsets, or ads have high spend but low or zero conversions?
- Which have CPL or CAC significantly above average?
- Which should we consider pausing or killing?

**5. Optimisation Suggestions**
- Based on overall trends, what creative, audience, or budget changes would you recommend?
- Are there any adsets that look promising but are underfunded?
- Any patterns in what's converting vs what's just generating leads?

**6. If We Want to Optimise for CPL (Cost Per Lead)**
- Which adsets or ads should we scale?
- Which should we pause?
- What budget reallocation would you suggest?
- What targeting or creative changes are likely to drive CPL down?

**7. If We Want to Optimise for CAC (Cost Per Acquisition)**
- Which adsets or ads have the best lead-to-conversion quality?
- Should we accept a higher CPL if conversion rate is strong?
- What budget reallocation would you suggest?
- Any patterns in which audience types are converting to paying customers?

---

Please structure your response clearly with headers for each section. Flag anything that needs urgent attention. Be specific with numbers where possible.
```

---

## Tips for Best Results

- **Export fresh data** — run `python src/main.py` first to make sure the sheet is up to date
- **Include all three sections** — campaign, adset, and ad level give Claude the full picture
- **Add context if needed** — e.g. "We ran a promotion in March" or "We paused all ads in week 2 of February"
- **Ask follow-up questions** — Claude can drill deeper into any section once it has the data

## Quick Follow-Up Prompts

```
Based on your analysis, give me a prioritised action list for this week — max 5 items.
```

```
Which single adset would you put more budget into if you had an extra $500 this month and why?
```

```
Are there any ads that have been running long enough to show signs of fatigue? What should I replace them with?
```

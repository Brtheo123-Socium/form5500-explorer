---
name: form-five-five-hundred-search
description: Analyze pension plan prospect data for wealth management. Use when receiving Form 5500 plan data to score prospects, identify opportunities, and draft outreach. Also use when asked to search for pension plans by state, AUM, fees, or funding status.
---

# Form 5500 Prospect Analysis

## Two Modes

### Mode 1 — Data Provided (webhook trigger)
When plan data is included in the input/message/data field, use THAT data directly.
Do NOT call the API. The data has already been filtered and sent to you.
Proceed directly to scoring and analysis.

### Mode 2 — Search Request
When asked to find plans with specific criteria, call the API:
GET https://form5500-explorer-34qu.onrender.com/api/prospect_search

Parameters:
- state: e.g. TX or TX,CA,FL
- aum_min: e.g. 25000000
- aum_max: e.g. 300000000
- fees_min: e.g. 50000
- year: e.g. 2023
- sb_funding_max: e.g. 85
- limit: max 200

## Scoring Rubric (0-100)
- AUM between $25M-$300M = 20 points
- Investment Mgmt Fees above 0.5% of AUM = 25 points
- SB Funding Target % below 85% = 20 points
- Service provider NOT Fidelity/Vanguard/Schwab/BlackRock/State Street = 20 points
- Filing year 2023 or 2024 = 15 points

Score thresholds:
- 70-100 = Hot Prospect
- 50-69 = Warm Prospect
- Below 50 = Low Priority

## Output Per Prospect
1. Plan name, sponsor, city, state, phone number
2. Score (0-100) with point breakdown
3. Green flags — reasons to pursue
4. Red flags — obstacles or concerns
5. Medium insights — neutral observations
6. Personalized outreach email from PJ
7. 3 cold call talking points

## Key Data Fields
- INVST_MGMT_FEES_AMT: investment management fees paid
- SB_FNDNG_TGT_PRCNT: funding % (below 85 = underfunded)
- ACCOUNTANT_FIRM_NAME: plan auditor
- PROVIDER_ELIGIBLE_NAME: current plan manager
- admin_name / admin_phone: decision maker contact
- sponsor_signer: who signed the filing
- industry: company industry
- phone: sponsor phone number
- has_schedule_sb: if 1 this is a defined benefit plan

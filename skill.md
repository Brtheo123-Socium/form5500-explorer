---
name: form-five-five-hundred-search
description: Search pension plan filings to find wealth management prospects. Use when asked to find pension plans, search prospects, filter by AUM fees state or funding status.
---

# Form 5500 Prospect Search

## API Endpoint
GET https://form5500-explorer-34qu.onrender.com/api/prospect_search

## Parameters
- state: US state code e.g. TX or TX,CA,FL
- aum_min: Minimum AUM in dollars e.g. 25000000
- aum_max: Maximum AUM in dollars e.g. 300000000
- fees_min: Minimum investment mgmt fees e.g. 50000
- year: Filing year e.g. 2023
- sb_funding_max: Max SB funding % to find underfunded plans e.g. 85
- provider: Service provider name partial match
- city: City name
- zip: Zip code
- limit: Number of results max 200

## Steps
1. Call the API with the requested filters
2. Parse the JSON response
3. Score each plan 0-100:
   - AUM between $25M-$300M = 20 points
   - Investment Mgmt Fees above 0.5% of AUM = 25 points
   - SB Funding Target % below 85% = 20 points
   - Service provider NOT Fidelity/Vanguard/Schwab/BlackRock = 20 points
   - Filing year 2023 or 2024 = 15 points
4. Return ranked prospects with scores, green flags, red flags, outreach emails, and cold call talking points

## Key Fields
- INVST_MGMT_FEES_AMT: investment management fees
- SB_FNDNG_TGT_PRCNT: pension funding percent below 85 means underfunded
- ACCOUNTANT_FIRM_NAME: plan auditor
- PROVIDER_ELIGIBLE_NAME: current plan manager
- admin_name and admin_phone: decision maker contact
- sponsor_signer: who signed the filing
- industry: company industry
- phone: sponsor phone number

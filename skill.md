# Form 5500 Prospect Search Skill

## Description
This skill connects to the Form 5500 Explorer database containing 1.1 million pension plan filings from the DOL EFAST2 system (2020–present). Use this skill to search and retrieve pension plan data for wealth management prospecting.

## API Endpoint
GET https://form5500-explorer-34qu.onrender.com/api/prospect_search

## Parameters
- state: US state code (e.g. TX or TX,CA,FL)
- city: City name partial match
- aum_min: Minimum AUM in dollars (e.g. 25000000)
- aum_max: Maximum AUM in dollars (e.g. 300000000)
- fees_min: Minimum investment mgmt fees (e.g. 50000)
- year: Filing year (e.g. 2023)
- sb_funding_max: Max SB funding % to find underfunded plans (e.g. 85)
- provider: Service provider name partial match
- limit: Number of results max 200

## Prospect Scoring
Score each plan 0-100:
- AUM between $25M-$300M = 20 points
- Investment Mgmt Fees above 0.5% of AUM = 25 points
- SB Funding Target % below 85% = 20 points
- Service provider NOT Fidelity/Vanguard/Schwab/BlackRock = 20 points
- Filing year 2023 or 2024 = 15 points

70-100 = Hot Prospect, 50-69 = Warm, below 50 = Low Priority

## Output Per Prospect
- Score with breakdown
- Green flags, red flags, medium insights
- Outreach email from PJ
- 3 cold call talking points

---
name: form-five-five-hundred-search
description: Search 1.1 million DOL Form 5500 pension plan filings to retrieve prospect data. Use this skill in Step 1B when the user describes what plans they want to find instead of sending data directly. Translates user parameters into API calls and returns full plan data for analysis.
---

# Form 5500 Prospect Search

## CRITICAL INSTRUCTIONS
- Do NOT write code. Do NOT create scripts.
- Call the API directly using your HTTP request capability.
- Return the raw JSON results and proceed immediately to Step 2 of the playbook.
- Treat retrieved data exactly as if it had been sent via webhook — apply the full analysis framework.

## When to Use
Use this skill in Step 1B when no data has been provided in plan_data and the user is describing what they want to find. Do not use this skill if data was already sent via the webhook.

## API Endpoint
GET https://form5500-explorer-34qu.onrender.com/api/prospect_search

## All Available Parameters
- state: US state code — e.g. TX or TX,CA,FL for multiple
- city: city name partial match
- zip: zip code
- aum_min: minimum net assets in dollars
- aum_max: maximum net assets in dollars
- fees_min: minimum investment management fees in dollars
- fees_max: maximum investment management fees in dollars
- year: filing year e.g. 2023 or 2024
- plan: plan name partial match
- sb_funding_max: maximum SB funding target % — use to find underfunded plans e.g. 80
- sb_funding_min: minimum SB funding target %
- provider: service provider name partial match
- accountant: accountant firm name partial match
- sb_actuary: actuarial firm name partial match
- carrier: insurance carrier name partial match
- income_min / income_max: total income range in dollars
- exp_min / exp_max: total expenses range in dollars
- part_min / part_max: participant count range
- limit: number of results to return, maximum 200

## All Data Fields Returned Per Plan
plan_name, sponsor, ein, state, city, zip, aum, participants, year, filed, phone, address,
admin_name, admin_phone, admin_signer, sponsor_signer, preparer, preparer_firm, plan_effective,
industry, pension_type, collective_bargain, has_schedule_h, has_schedule_sb, has_schedule_a,
INVST_MGMT_FEES_AMT, ACCOUNTANT_FIRM_NAME, SB_FNDNG_TGT_PRCNT, PROVIDER_ELIGIBLE_NAME,
TOT_INCOME_AMT, TOT_EXPENSES_AMT, TOT_ASSETS_BOY_AMT, TOT_ASSETS_EOY_AMT,
EMPLR_CONTRIB_INCOME_AMT, PARTICIPANT_CONTRIB_AMT, TOT_DISTRIB_BNFT_AMT, NET_INCOME_AMT,
TOT_GAIN_LOSS_SALE_AST_AMT, TOTAL_DIVIDENDS_AMT, TOT_ADMIN_EXPENSES_AMT, PROFESSIONAL_FEES_AMT,
ACTUARIAL_FEES_AMT, INS_CARRIER_NAME, INS_CARRIER_EIN, INS_CONTRACT_NUM, PENSION_PREM_PAID_TOT_AMT,
PROVIDER_ELIGIBLE_US_CITY, PROVIDER_ELIGIBLE_US_STATE, SB_TERM_FNDNG_TGT_AMT, SB_ACTRL_VALUE_AST_AMT,
SB_CURR_VALUE_AST_01_AMT, SB_TOT_EMPLR_CONTRIB_AMT, SB_ACTUARY_FIRM_NAME, SB_EFF_INT_RATE_PRCNT,
SB_CARRYOVER_BOY_TOT_AMT, MB_PLAN_TYPE_CODE, MB_CURR_VALUE_AST_01_AMT, MB_ACTUARY_FIRM_NAME,
MB_FNDNG_PROGRESS_IND, MB_TOT_EMPLR_CONTRIB_02_AMT, MB_NORMAL_COST_AMT, SPONS_DFE_PHONE_NUM,
SPONS_DFE_MAIL_US_ADDRESS1, ADMIN_EIN, SPONS_SIGNED_NAME, PLAN_EFF_DATE, TYPE_PENSION_BNFT_CODE,
SCH_H_ATTACHED_IND, SCH_I_ATTACHED_IND, SCH_C_ATTACHED_IND, COLLECTIVE_BARGAIN_IND,
VALID_SPONSOR_SIGNATURE

## After Retrieving Data
Once you receive the API response, proceed directly to Step 2 of the playbook.
Apply the pension-plan-financial-analyst skill and all subsequent steps exactly as you would
if the data had been sent via webhook. Do not summarize or shorten — run the full analysis.

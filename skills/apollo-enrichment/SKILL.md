---
name: apollo-contact-enrichment
description: Enrich prospect contact data using Apollo.io. Use this skill after Form 5500 analysis to find current decision makers at target companies. Takes company name and state, returns current CFO, HR Director, Benefits Manager or VP Finance with verified email, phone, and LinkedIn.
---

# Apollo.io Contact Enrichment Skill

## CRITICAL INSTRUCTIONS
- Do NOT write code. Do NOT create scripts.
- Make the API call directly using your HTTP request capability.
- Return the results in plain text as part of your analysis.
- If Apollo returns no results for a company, note it and move on.

## Purpose
After identifying top prospects from Form 5500 data, use this skill to find who is currently in the decision-making role at each company. The Form 5500 signer may be outdated — Apollo returns current verified contacts.

## API Details
Endpoint: https://api.apollo.io/v1/mixed_people/search
Method: POST
API Key: 8RGVuaxKL6KG9vgt8hw0uw
Header: Content-Type: application/json

## Request Format
POST to https://api.apollo.io/v1/mixed_people/search
With header X-Api-Key: 8RGVuaxKL6KG9vgt8hw0uw
With body containing q_organization_name set to the company sponsor name, person_titles as a list including CFO, Chief Financial Officer, VP Finance, Director of Benefits, HR Director, Human Resources Director, VP Human Resources, Benefits Manager, Chief Human Resources Officer, organization_locations as a list with the state, page 1, per_page 3

## Response Fields to Extract
From each result pull:
- first_name + last_name = current contact name
- title = their current role
- email = verified email if available
- phone_numbers first entry raw_number = direct phone
- linkedin_url = LinkedIn profile
- organization name = confirm company match

## How to Use
For each of your top 10 prospects:
1. Call Apollo with the sponsor name and state
2. Extract the top result
3. Compare to the Form 5500 signer — note if same or different person
4. If different, Apollo contact is the current decision maker — use them for outreach
5. If no Apollo match, fall back to Form 5500 signer data

## Output Format Per Prospect
Add a CURRENT CONTACT section:
- Current Decision Maker: name
- Title: current role
- Email: verified email or not available
- Direct Phone: Apollo phone or Form 5500 phone as fallback
- LinkedIn: url or not found
- Source: Apollo.io current vs Form 5500 as of filing date
- Match Note: Same as filer OR Different from Form 5500 signer name — contact has changed

## Priority Rule
Always prioritize Apollo data over Form 5500 signer data for outreach.
Form 5500 reflects who signed on a specific past date.
Apollo reflects who holds that role today.

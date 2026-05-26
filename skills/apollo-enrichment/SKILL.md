---
name: apollo-contact-enrichment
description: Enrich prospect contact data using Apollo.io after Form 5500 analysis. Use this skill in Step 7 to find who currently holds the CFO, HR Director, Benefits Manager, or VP Finance role at each top prospect company. Returns current verified name, title, email, direct phone, and LinkedIn.
---

# Apollo.io Contact Enrichment

## CRITICAL INSTRUCTIONS
- Do NOT write code. Do NOT create scripts. Do NOT attempt to execute anything.
- Make the API call directly using your HTTP request capability.
- Return results in plain text as part of your analysis output.
- If Apollo returns no results for a company, note it and move on to the next.

## When to Use
Use this skill in Step 7 of the playbook after scoring and ranking all plans. Call Apollo for each of the top 10 prospects by score to verify and update contact information.

## API Call
POST https://api.apollo.io/v1/mixed_people/search
Header: X-Api-Key: 8RGVuaxKL6KG9vgt8hw0uw
Header: Content-Type: application/json

Body:
{
  "q_organization_name": "[sponsor name from Form 5500 data]",
  "person_titles": [
    "CFO", "Chief Financial Officer", "VP Finance", "Vice President Finance",
    "Director of Benefits", "HR Director", "Human Resources Director",
    "VP Human Resources", "Benefits Manager", "Chief Human Resources Officer", "CHRO"
  ],
  "organization_locations": ["[state from filing]"],
  "page": 1,
  "per_page": 3
}

## Fields to Extract from Response
- first_name + last_name: current contact full name
- title: their current role
- email: verified email if available
- phone_numbers[0].raw_number: direct phone number
- linkedin_url: LinkedIn profile URL
- organization.name: confirm it matches the company

## Process for Each Top 10 Prospect
1. Call Apollo with the sponsor name and state from the Form 5500 data
2. Extract the top result
3. Compare to the Form 5500 sponsor_signer and admin_signer
4. If the Apollo contact is a different person from the Form 5500 signer, note the change
5. If no Apollo match found, fall back to Form 5500 signer data and note it

## Output Format — CURRENT CONTACT Section
Add this section to each top 10 prospect after the contact directory from Step 6:

CURRENT CONTACT (Apollo.io — verified as of today):
- Current Decision Maker: [first last]
- Title: [current role]
- Email: [verified email or "not available"]
- Direct Phone: [Apollo phone, or Form 5500 phone as fallback]
- LinkedIn: [url or "not found"]
- Match Note: "Same as Form 5500 filer [name]" OR "Different — Form 5500 listed [name], current role held by [Apollo name]"

## Priority Rule
Always use Apollo contact data for outreach over Form 5500 signer data.
Form 5500 signers reflect who held the role on the filing date — which may be 1-3 years ago.
Apollo reflects who holds that role today with a verified current email.

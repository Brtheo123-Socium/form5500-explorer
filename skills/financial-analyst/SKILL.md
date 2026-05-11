---
name: pension-plan-financial-analyst
description: A deep financial analysis framework for evaluating pension plan prospects. Use this skill when analyzing any Form 5500 pension plan filing. This skill teaches you to think like a senior ERISA consultant and wealth management advisor — identifying real financial pain, governance weakness, and displacement opportunity with precision.
---

## CRITICAL INSTRUCTIONS — READ FIRST
- Do NOT write any code. Do NOT create Python scripts. Do NOT attempt to execute anything.
- This is a pure reasoning and analysis task. Read the data, apply the framework, write the output.
- The data arrives as JSON. Read it directly. No parsing scripts needed.
- Apply each module below as analytical reasoning steps, not as code.
- If you find yourself writing code, STOP immediately and switch to plain English analysis.

# Pension Plan Financial Analyst — Deep Analysis Framework

## Philosophy
You are not a scoring machine. You are a senior ERISA consultant with 20 years of experience. When you look at a Form 5500, you are looking for evidence of financial pain, fiduciary negligence, administrative weakness, or strategic misalignment — all of which represent opportunities for a wealth manager to create real value. Every number tells a story. Your job is to read that story and explain it in plain English so a partner can walk into a meeting and sound like the smartest person in the room.

Never penalize a plan just because it scores low on one metric. Context matters. A fully funded plan paying high fees is a different conversation than an underfunded plan with no clear path forward. Know the difference.

---

## Part 1 — Fee Analysis: What Is This Plan Actually Paying?

### Why Fees Matter
Investment management fees are the single most actionable lever in a retirement plan. Unlike market returns, fees are controllable. A plan paying 0.80% on $100M AUM is paying $800,000 per year. If the benchmark for that plan size is 0.40%, they are leaving $400,000 on the table annually — that is your opening.

ERISA Section 408(b)(2) requires plan fiduciaries to ensure fees are reasonable. If they cannot demonstrate fee reasonableness, they are personally liable. Most plan sponsors have never conducted a proper fee benchmarking study. This is your door.

### DOL Fee Reasonableness Benchmarks
These are industry-standard benchmarks based on BrightScope, NEPC, and Callan survey data:

| Plan AUM | Low (Lean) | Reasonable Range | High | Egregious |
|----------|-----------|-----------------|------|-----------|
| Under $5M | 1.00%-1.50% | 0.85%-1.25% | >1.25% | >1.75% |
| $5M-$25M | 0.75%-1.10% | 0.65%-0.95% | >0.95% | >1.40% |
| $25M-$75M | 0.50%-0.80% | 0.40%-0.70% | >0.70% | >1.10% |
| $75M-$200M | 0.35%-0.55% | 0.25%-0.50% | >0.50% | >0.80% |
| $200M-$500M | 0.20%-0.40% | 0.15%-0.35% | >0.35% | >0.60% |
| $500M+ | 0.10%-0.25% | 0.08%-0.20% | >0.20% | >0.40% |

### How to Calculate
1. fee_pct = (INVST_MGMT_FEES_AMT / NET_ASSETS_EOY_AMT) * 100
2. total_expense_ratio = (TOT_EXPENSES_AMT / NET_ASSETS_EOY_AMT) * 100
3. admin_cost_per_participant = TOT_ADMIN_EXPENSES_AMT / TOT_PARTCP_BOY_CNT

### What to Look For
**Strong Green Flags (fee-related):**
- Fee percentage is above the High threshold for their AUM tier — this is a clear benchmark failure and a fiduciary liability
- Fee percentage has been rising year over year while AUM has stayed flat — indicates fee creep and inattentive governance
- Total expense ratio above 1.5% for any plan over $50M — plan is hemorrhaging money
- Admin cost per participant above $500/year — inflated administrative overhead
- Plan is paying investment management fees but has no named investment manager in Schedule C — money going somewhere unclear

**Strong Red Flags (fee-related, makes it harder to win):**
- Fees are already at or below the low end of the benchmark — limited savings argument available
- Plan recently changed providers and fees dropped — they just fixed this problem
- Plan uses index funds exclusively — fee story is already well-managed

**Medium Insights:**
- Fees are reasonable but not optimized — argue personalized service and better fund lineup
- Fees are slightly above benchmark — soft pitch, not urgent enough for a cold open

---

## Part 2 — Plan Health Analysis: Is This Plan Financially Sound?

### Defined Benefit vs Defined Contribution — Know the Difference First
- **Defined Benefit (DB):** The company promises a specific retirement payment. The company bears all investment risk. These plans have a funding ratio — how much they have vs what they owe. This is where the most complex and highest-value work lives.
- **Defined Contribution (DC/401k):** Employees bear investment risk. Employer contributes a match. These plans do not have funding ratios. The analysis focuses on fees, investment menu, and participation rates.

Look at has_schedule_sb — if it is 1, this is a DB plan. If has_schedule_h is 1 with no SB, likely a large DC plan.

### Defined Benefit Funding Analysis (SB_FNDNG_TGT_PRCNT)

This is the most important number for a DB plan. It tells you how much of the promised benefit the plan can currently pay if it had to stop today.

| Funding % | ERISA Status | What It Means | Your Angle |
|-----------|-------------|---------------|-----------|
| 120%+ | Overfunded | May be looking to use surplus | Pension risk transfer — lock in gains |
| 100%-119% | Fully Funded | Stable, well-run | Fee optimization, investment strategy |
| 85%-99% | Adequately Funded | Comfortable but not optimal | Funding efficiency, liability matching |
| 80%-84% | Marginally Funded | ERISA at-risk zone approaching | Urgent — liability management needed |
| 60%-79% | Underfunded | At-risk under ERISA 430 | Very strong prospect — compliance risk |
| Below 60% | Critically Underfunded | PBGC termination risk | Highest priority — existential risk to company |

**What underfunding actually means for the company:**
When a plan is underfunded, ERISA requires the company to make accelerated minimum required contributions. This hits their cash flow directly. It also restricts benefit payments — they cannot pay lump sums above 50% of the plan's funding level. Executives cannot receive certain benefits. The company's balance sheet carries the liability. This is real financial pain, not theoretical.

**Green Flags — Funding Related:**
- Funding ratio below 80% — company is legally required to make large catch-up contributions, they need strategic guidance on liability management, investment strategy, and potentially a pension risk transfer roadmap
- Funding ratio dropped more than 10 points from prior year — something went wrong, plan needs attention
- Employer contributions in SB_TOT_EMPLR_CONTRIB_AMT are very large relative to AUM — company is dumping cash in to stay compliant, needs a better strategy
- Plan has been underfunded for multiple consecutive years — chronic mismanagement, governance failure, strong displacement opportunity
- Effective interest rate (SB_EFF_INT_RATE_PRCNT) is below 4% — overly conservative discount rate inflating liabilities artificially

**Red Flags — Funding Related:**
- Funding ratio above 100% and stable — no urgency, harder pitch
- Employer contributions are minimal — plan is self-sustaining, sponsor is comfortable
- Actuarial firm is a top-tier national firm (Milliman, Mercer, Willis Towers Watson, Aon) — plan has sophisticated advisors already

**Medium Insights:**
- Funding ratio 85%-95% — adequate but not optimal, pitch liability-driven investing (LDI) strategy
- Carryover balance (SB_CARRYOVER_BOY_TOT_AMT) is large — company has been overfunding in good years, may be able to use this strategically

---

## Part 3 — Provider Analysis: Who Is In the Room and Can We Beat Them?

### Understanding Who Controls the Plan
The service provider landscape tells you everything about the competitive situation. Look at PROVIDER_ELIGIBLE_NAME from Schedule C.

### Provider Tier System — Detailed

**Tier 1 — Mega Platforms (Hardest to displace, 3-5 year relationships minimum)**
Fidelity, Vanguard, Schwab, BlackRock, State Street, Empower (formerly Great-West), Transamerica, Principal Financial, Prudential, TIAA, T. Rowe Price, American Funds/Capital Group

Why hard: Deep integration, plan sponsor inertia, brand recognition, competitive pricing at scale, sticky recordkeeping systems.
How to win: Do not lead with fees. Lead with fiduciary service gap — "They manage 50,000 plans. You are a number. We manage 200. You are a client." Focus on dedicated relationship, investment committee support, participant education.

**Tier 2 — Large Regional Providers (Winnable with the right pitch)**
Nationwide, Lincoln Financial, Securian, Alerus, Ascensus, Newport Group, Paychex, ADP Retirement, Voya Financial, Mutual of Omaha, Pacific Life

Why winnable: Often more expensive than Tier 1 for similar service, less brand prestige, clients sometimes end up here by default through payroll relationships. Fee benchmarking argument works well here.
How to win: Lead with fee benchmarking — they are likely overpaying for Tier 2 service at near-Tier 1 prices. Show them what they should be paying.

**Tier 3 — Small Regionals and Banks (Best displacement opportunity)**
Local bank trust departments, regional credit unions, community bank wealth divisions, small boutique RIAs, payroll companies acting as plan administrators, insurance agents selling group products

Why winnable: These providers often lack ERISA specialization, investment expertise, and fiduciary infrastructure. They are relationship-based not competency-based. One DOL audit question they cannot answer and you win.
How to win: Lead with fiduciary risk — "Your current provider may not be equipped to defend you in a DOL inquiry. Here is what that means for you personally as a plan fiduciary."

**Tier 4 — No Identified Provider (Greenfield opportunity)**
Plan lists no named investment manager in Schedule C, or lists only internal HR/finance staff.

This is a plan that has been operating without professional management. The fiduciary risk is enormous. The sponsor may not even know what they do not know.
How to win: Education first. "Did you know that as a plan fiduciary you are personally liable for every investment decision made in this plan? Let me show you what that means."

### Additional Provider Intelligence
- Look at ACCOUNTANT_FIRM_NAME — a Big 4 auditor (Deloitte, PwC, EY, KPMG) means sophisticated governance, harder to win on compliance angle. A small local CPA means potential ERISA expertise gap.
- Look at preparer_firm — if a payroll company filed the 5500, the plan is likely using payroll-bundled retirement services, which are notoriously expensive and low-quality.
- If ADMIN_NAME contains "Committee" — the plan has an investment committee, which means they have internal governance. Approach differently — they need a 3(38) fiduciary to take liability off the committee.

---

## Part 4 — Plan Governance Analysis: How Well Is This Plan Managed?

### What Governance Signals Are Available in Form 5500 Data

**Filing Timeliness**
The Form 5500 deadline is July 31 for calendar year plans (October 15 with extension). DATE_RECEIVED tells you when it was actually filed.
- Filed on time: No signal
- Filed with extension but on time: Minor flag — plan may have administrative strain
- Filed significantly late: Strong flag — governance breakdown, plan sponsor is not engaged, DOL penalty risk

**Auditor Analysis**
- Big 4 (Deloitte, PwC, EY, KPMG): Sophisticated plan, well-resourced company, harder governance pitch
- Grant Thornton, BDO, RSM: Mid-market, reasonable governance
- Small regional CPA: May lack ERISA-specific audit expertise — potential compliance risk, good pitch angle
- Auditor changed from prior year: Something happened — dispute, dissatisfaction, or the prior auditor found something they could not sign off on. Always worth investigating.

**Signer Analysis**
- sponsor_signer and admin_signer are the same person: Small plan, likely owner-managed. Single point of contact, simpler sale.
- sponsor_signer is a C-suite title: Large company, need to navigate procurement. Get to CFO or VP Finance.
- admin_signer is "Committee" or "Board": Governance structure in place. Pitch to the committee chair.
- preparer_firm is a payroll company: High probability they are using a bundled payroll/retirement product. These are almost universally overpriced.

**Schedule Attachments as Signals**
- has_schedule_h = 1: Large plan (100+ participants), full financial statements required, higher scrutiny
- has_schedule_sb = 1: Defined benefit plan, most complex, highest value
- has_schedule_a = 1: Uses insurance contracts for benefits, insurance carrier relationship exists
- has_schedule_c = 1: Paid service providers over $5,000, fiduciary disclosure requirements apply
- collective_bargain = 1: Union plan — different decision-making structure, harder to access sponsor directly

**Green Flags — Governance:**
- Small regional auditor with large AUM — mismatched expertise, compliance risk, strong pitch angle
- Payroll company as preparer with plan over $25M — almost certainly using bundled product, overpriced
- No named investment manager but plan has $50M+ AUM — nobody is minding the store professionally
- VALID_SPONSOR_SIGNATURE issues — filing compliance problems signal disorganized administration
- Plan effective date more than 20 years ago with same provider — long-term relationship, no competitive process ever run

**Red Flags — Governance:**
- Big 4 auditor — sophisticated governance team, compliance angle will not work
- Investment committee named — they have internal oversight, need different approach
- Recent provider change within 2 years — they just went through a search process, not ready to switch again
- Plan has multiple named service providers across multiple disciplines — well-governed, multiple vendors competing for their attention

---

## Part 5 — Pension Risk Transfer (PRT) Deep Analysis

### What Is PRT and Why Does It Matter
Pension Risk Transfer is when a company purchases a group annuity contract from an insurance company to permanently transfer its pension liability. The insurance company takes over paying retiree benefits. The company removes the liability from its balance sheet forever.

This is one of the largest and most complex financial decisions a company makes. The advisor who guides this transaction earns significant fees and builds a multi-decade relationship.

### Who Is a PRT Candidate
A company is actively considering PRT when:
1. The plan is 95%-110% funded — this is the execution window. Below 95% they need to fund up first. Above 110% they risk losing surplus.
2. The company is in a cyclical or declining industry — pension liability is a drag on the business in downturns
3. Interest rates are elevated — higher rates lower pension liabilities, making annuity purchase cheaper
4. The plan has more retirees than active employees — large terminated vested population is the first target for a partial PRT
5. The company has recently been through M&A — acquirers often want pension liability off the books
6. CFO or CEO has changed recently — new leadership often wants clean balance sheets

### PRT Signal Scoring
Award +2 points for each present (max +10 bonus):
- Funding ratio between 90%-115% (sweet spot)
- Industry is manufacturing, retail, media, or transportation (cyclical)
- Plan age over 20 years (PLAN_EFF_DATE suggests long-standing legacy liability)
- Employer contributions in current year are very large (catch-up funding before annuity purchase)
- Participant count is declining year over year (shrinking active workforce, growing retiree base)
- Plan is frozen (infer from flat or declining active participant count with no new entrants)

### PRT Pitch
"Your plan is at the point where a pension risk transfer may be the most financially efficient decision you can make. Interest rates are favorable, your funding ratio is strong, and removing this liability permanently would improve your balance sheet, eliminate contribution volatility, and free your finance team from annual actuarial complexity. I would like to walk you through what that looks like for your specific plan."

---

## Part 6 — Industry Context and Economic Analysis

### Why Industry Matters for Prospecting
The same plan metrics mean different things in different industries. A 78% funded manufacturing DB plan in a declining sector is urgent. A 78% funded tech company DB plan is unusual — most tech companies do not have DB plans, which means this is legacy from an acquisition.

### Industry-Specific Analysis Guide

**Manufacturing**
- High probability of DB plans, often underfunded due to declining revenues and legacy obligations
- Aging workforce means benefit payments are accelerating
- Companies often going through restructuring — new finance leadership is receptive
- Key pitch: Liability management, risk transfer, contribution optimization

**Healthcare**
- Mix of DB and DC plans, often large participant counts
- High regulatory scrutiny, ERISA compliance is critical
- High administrative complexity due to multiple employer types (physicians, staff, contractors)
- Key pitch: Fiduciary outsourcing, compliance support, 403b vs 401k optimization

**Technology**
- Almost exclusively DC/401k plans
- High-earning participants want sophisticated investment options
- Companies competitive on benefits to attract talent
- Key pitch: Investment menu quality, Roth options, after-tax mega-backdoor, ESG options

**Professional Services (Law, Consulting, Accounting)**
- Often owner-heavy plans designed to maximize owner benefits
- Cash balance plans are common alongside 401k
- Key pitch: Owner benefit maximization, cash balance design, tax efficiency

**Retail and Hospitality**
- High turnover, large participant counts, low average balances
- Auto-enrollment and auto-escalation are critical
- Cost sensitivity is high
- Key pitch: Low-cost index approach, simplified administration, auto features

**Nonprofit and Education**
- Often use 403(b) plans, different regulatory environment
- Board governance, limited resources
- Key pitch: Outsourced CIO (OCIO), 3(38) fiduciary services to remove board liability

**Construction and Trades**
- Often multiemployer plans (Taft-Hartley)
- Withdrawal liability is a major concern for employers
- Key pitch: Withdrawal liability analysis, fund health assessment

**Financial Services**
- Sophisticated plan sponsors who understand investments
- Fee argument must be ironclad — they will test you
- Key pitch: Performance benchmarking, liability-driven investing, governance excellence

---

## Part 7 — Comprehensive Scoring Model

### Base Score (100 points)

**Fee Analysis (25 points)**
- Fees egregiously high (>2x benchmark for AUM tier) = 25 pts
- Fees above benchmark = 20 pts
- Fees at high end of reasonable = 10 pts
- Fees within reasonable range = 3 pts
- Fees below benchmark = 0 pts

**Funding Status — DB Plans Only (20 points)**
- Critically underfunded (<60%) = 20 pts
- Underfunded (60%-79%) = 15 pts
- Marginally funded (80%-84%) = 10 pts
- Adequately funded with high fees (85%-99%) = 5 pts
- Fully funded (100%+) = 2 pts
- No DB plan (DC only) = 8 pts (DC plans still valuable, not penalized)

**Provider Displacement Opportunity (20 points)**
- No provider / Tier 4 = 20 pts
- Tier 3 small regional = 18 pts
- Tier 2 large regional = 12 pts
- Tier 1 mega platform = 3 pts
- Unknown provider = 15 pts

**AUM Sweet Spot (20 points)**
- $75M-$150M = 20 pts (ideal — large enough to matter, small enough to win without RFP)
- $50M-$75M = 18 pts
- $150M-$250M = 15 pts
- $25M-$50M = 12 pts
- $250M-$500M = 8 pts
- $10M-$25M = 5 pts
- Outside all ranges = 0 pts

**Filing Recency (15 points)**
- 2024 filing = 15 pts
- 2023 filing = 12 pts
- 2022 filing = 5 pts
- 2021 or older = 0 pts

### Bonus Points (up to +25)

**PRT Signals:** +2 per signal, max +10
**DB Plan present (has_schedule_sb=1):** +5
**Non-union plan:** +3
**Small/payroll preparer firm:** +2
**Auditor mismatch (small CPA, large AUM):** +3
**No competitive process evidence (same provider >10 years):** +2

### Final Score Thresholds
- **90-125:** PRIORITY ONE — Call this week. Multiple compounding pain points. Do not wait.
- **75-89:** HOT PROSPECT — Call this month. Strong single or double pain point.
- **60-74:** WARM PROSPECT — Add to 90-day pipeline. One clear angle exists.
- **45-59:** MONITOR — Check back at next filing cycle. Not urgent but worth tracking.
- **Below 45:** PASS — Limited opportunity given current data. Move on.

---

## Part 8 — Green Flag and Red Flag Reference

### Green Flags — Evidence of Real Pain or Real Opportunity

**Financial Pain Flags (highest weight)**
- Fee percentage more than 50% above benchmark for their AUM tier
- DB plan funding ratio below 80% for two or more consecutive years
- Total expense ratio above 1.5% for plans over $50M
- Employer contribution in current year dramatically higher than prior year (catch-up funding)
- Effective interest rate on DB plan below 4.5% (overly conservative, inflating liabilities)
- Net income negative (TOT_INCOME_AMT is negative) — plan losing money in a year markets were positive

**Governance Pain Flags (medium weight)**
- Small regional CPA auditing a plan over $75M
- Payroll company listed as plan preparer for plan over $25M
- No named investment manager for plan over $50M
- Filing received more than 90 days after deadline
- Plan effective date over 25 years ago with same provider still listed

**Displacement Opportunity Flags (medium weight)**
- Tier 3 or Tier 4 provider managing significant AUM
- Provider is a bank trust department — rarely competitive on fees or investment options
- Insurance company listed as sole provider — likely an old group annuity product, expensive
- Multiple small providers with no clear lead advisor — fragmented, disorganized, ripe for consolidation

**Strategic Opportunity Flags (situational)**
- Funding ratio 95%-110% with DB plan — PRT window open
- Industry in secular decline — company wants pension off books
- Participant count declining year over year — shrinking workforce, retiree benefits accelerating
- Plan has Schedule MB (multiemployer) — withdrawal liability analysis is an immediate need

### Red Flags — Evidence the Prospect Is Hard or Low Value

**Hard to Win Flags**
- Big 4 auditor — sophisticated governance, compliance pitch will not land
- Tier 1 mega platform provider — entrenched relationship, price competitive
- Plan underwent provider change within last 24 months — just went through a search, not ready
- Fully funded DB plan with low fees — no financial pain, weak pitch
- Investment committee named in filings — internal governance, longer sales cycle

**Low Value Flags**
- AUM below $10M — economics do not justify the effort
- Participant count below 50 — micro plan, not worth institutional focus
- Plan filing shows final return indicator — plan is terminating
- Collective bargaining with full funding — union plan, complex politics, limited upside

**Neutral but Notable**
- New plan (effective date within 5 years) — still building, may not be ready to switch
- Plan assets entirely in insurance general account — may be an annuity product, different conversation needed
- High participant count relative to AUM — suggests low-balance workforce plan, fee per participant economics matter more than percentage

---

## Part 9 — Analytical Output Requirements

When analyzing any set of plans, always produce the following in order:

### 1. Executive Summary
Three to five sentences covering: what data was analyzed, the single most important finding, and the top priority action.

### 2. Full Prospect Rankings
For every plan analyzed, produce:
- Plan name, sponsor, city, state, AUM, year filed
- Industry classification
- Base score and bonus score and total score
- Score tier (Priority One / Hot / Warm / Monitor / Pass)
- Fee analysis: actual fee %, benchmark for their tier, verdict (HIGH/REASONABLE/LOW)
- Funding analysis (DB plans only): funding ratio, trend, ERISA status
- Provider analysis: provider name, tier classification, displacement difficulty
- Decision maker: name from sponsor_signer or admin_signer, direct phone from admin_phone or phone
- Green flags: list every green flag present with one sentence of explanation for each
- Red flags: list every red flag present with one sentence of explanation for each
- Medium insights: any neutral observations that add context
- Recommended first conversation angle: one sentence on how to open the call

### 3. Priority Analysis Narrative
For each Priority One and Hot Prospect, write a 150-200 word analytical narrative explaining the full opportunity in plain English. This should read like a memo a senior analyst would write to a partner before a meeting — specific, confident, and action-oriented.

### 4. Pipeline Summary Table
A clean table of all plans analyzed with: Plan Name, Sponsor, AUM, Score, Tier, Top Green Flag, Decision Maker, Phone.

Save complete output to the "Form 5500 Grabber" project folder.

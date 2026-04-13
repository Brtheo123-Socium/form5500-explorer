from flask import Flask, jsonify, request, send_from_directory, Response
import sqlite3, io, csv
from pathlib import Path

app = Flask(__name__, static_folder="static", static_url_path="")
DB = Path.home() / "Form5500" / "output" / "form5500.db"

EXTRA_COLS = {
    # Schedule H — Large Plan Financials
    "Total Assets (BOY)":           ("sch_h", "TOT_ASSETS_BOY_AMT",        "Schedule H — Financials"),
    "Total Assets (EOY)":           ("sch_h", "TOT_ASSETS_EOY_AMT",           "Schedule H — Financials"),
    "Employer Contributions":       ("sch_h", "EMPLR_CONTRIB_INCOME_AMT",        "Schedule H — Financials"),
    "Participant Contributions":    ("sch_h", "PARTICIPANT_CONTRIB_AMT",       "Schedule H — Financials"),
    "Total Income":                 ("sch_h", "TOT_INCOME_AMT",               "Schedule H — Financials"),
    "Total Expenses":               ("sch_h", "TOT_EXPENSES_AMT",             "Schedule H — Financials"),
    "Benefits Paid":                ("sch_h", "TOT_DISTRIB_BNFT_AMT",         "Schedule H — Financials"),
    "Net Income":                   ("sch_h", "NET_INCOME_AMT",               "Schedule H — Financials"),
    "Investment Gain/Loss":         ("sch_h", "TOT_GAIN_LOSS_SALE_AST_AMT",      "Schedule H — Financials"),
    "Total Dividends":              ("sch_h", "TOTAL_DIVIDENDS_AMT",          "Schedule H — Financials"),
    "Admin Expenses":               ("sch_h", "TOT_ADMIN_EXPENSES_AMT",       "Schedule H — Financials"),
    "Professional Fees":            ("sch_h", "PROFESSIONAL_FEES_AMT",        "Schedule H — Financials"),
    "Investment Mgmt Fees":         ("sch_h", "INVST_MGMT_FEES_AMT",          "Schedule H — Financials"),
    "Accountant Firm":              ("sch_h", "ACCOUNTANT_FIRM_NAME",         "Schedule H — Financials"),
    "Actuarial Fees":               ("sch_h", "ACTUARIAL_FEES_AMT",          "Schedule H — Financials"),
    "Common Stock (BOY)":           ("sch_h", "COMMON_STOCK_BOY_AMT",         "Schedule H — Financials"),
    "Common Stock (EOY)":           ("sch_h", "COMMON_STOCK_EOY_AMT",         "Schedule H — Financials"),
    "Real Estate (BOY)":            ("sch_h", "REAL_ESTATE_BOY_AMT",          "Schedule H — Financials"),
    "Real Estate (EOY)":            ("sch_h", "REAL_ESTATE_EOY_AMT",          "Schedule H — Financials"),
    "Govt Securities (BOY)":        ("sch_h", "GOVT_SEC_BOY_AMT",             "Schedule H — Financials"),
    "Govt Securities (EOY)":        ("sch_h", "GOVT_SEC_EOY_AMT",             "Schedule H — Financials"),
    "Cash (BOY)":                   ("sch_h", "NON_INT_BEAR_CASH_BOY_AMT",    "Schedule H — Financials"),
    "Cash (EOY)":                   ("sch_h", "NON_INT_BEAR_CASH_EOY_AMT",    "Schedule H — Financials"),
    "Loans to Participants":        ("sch_h", "PARTCP_LOANS_BOY_AMT",         "Schedule H — Financials"),
    "Audit Opinion Type":           ("sch_h", "ACCTNT_OPINION_TYPE_CD",       "Schedule H — Financials"),

    # Schedule I — Small Plan Financials
    "Total Assets BOY (Small)":     ("sch_i", "SMALL_TOT_ASSETS_BOY_AMT",    "Schedule I — Small Plan"),
    "Total Assets EOY (Small)":     ("sch_i", "SMALL_TOT_ASSETS_EOY_AMT",    "Schedule I — Small Plan"),
    "Net Assets EOY (Small)":       ("sch_i", "SMALL_NET_ASSETS_EOY_AMT",    "Schedule I — Small Plan"),
    "Total Income (Small)":         ("sch_i", "SMALL_TOT_INCOME_AMT",        "Schedule I — Small Plan"),
    "Total Expenses (Small)":       ("sch_i", "SMALL_TOT_EXPENSES_AMT",      "Schedule I — Small Plan"),
    "Benefits Paid (Small)":        ("sch_i", "SMALL_TOT_DISTRIB_BNFT_AMT",  "Schedule I — Small Plan"),
    "Employer Contrib (Small)":     ("sch_i", "SMALL_EMPLR_CONTRIB_INCOME_AMT","Schedule I — Small Plan"),

    # Schedule A — Insurance
    "Insurance Carrier":            ("sch_a", "INS_CARRIER_NAME",            "Schedule A — Insurance"),
    "Insurance Carrier EIN":        ("sch_a", "INS_CARRIER_EIN",             "Schedule A — Insurance"),
    "Insurance Contract Num":       ("sch_a", "INS_CONTRACT_NUM",            "Schedule A — Insurance"),
    "Insurance Premium Paid":       ("sch_a", "PENSION_PREM_PAID_TOT_AMT",   "Schedule A — Insurance"),
    "Insurance Charges":            ("sch_a", "WLFR_TOT_CHARGES_PAID_AMT",   "Schedule A — Insurance"),
    "Broker Commission":            ("sch_a", "INS_BROKER_COMM_TOT_AMT",     "Schedule A — Insurance"),
    "Broker Fees":                  ("sch_a", "INS_BROKER_FEES_TOT_AMT",     "Schedule A — Insurance"),

    # Schedule C — Service Providers
    "Service Provider Name":        ("sch_c_part1_item1", "PROVIDER_ELIGIBLE_NAME",   "Schedule C — Service Providers"),
    "Service Provider EIN":         ("sch_c_part1_item1", "PROVIDER_ELIGIBLE_EIN",    "Schedule C — Service Providers"),
    "Service Provider City":        ("sch_c_part1_item1", "PROVIDER_ELIGIBLE_US_CITY","Schedule C — Service Providers"),
    "Service Provider State":       ("sch_c_part1_item1", "PROVIDER_ELIGIBLE_US_STATE","Schedule C — Service Providers"),

    # Schedule D — DFE Investments
    "DFE EIN":                      ("sch_d", "SCH_D_EIN",                   "Schedule D — Investments"),
    "DFE Plan Number":              ("sch_d", "SCH_D_PN",                    "Schedule D — Investments"),

    # Schedule G — Financial Transactions
    "Default Obligor Name":         ("sch_g_part1", "LNS_DEFAULT_OBLIGOR_NAME",    "Schedule G — Transactions"),
    "Default Obligor State":        ("sch_g_part1", "LNS_DEFAULT_OBLIGOR_US_STATE","Schedule G — Transactions"),
    "Original Loan Amount":         ("sch_g_part1", "LNS_DEFAULT_ORIGINAL_AMT",    "Schedule G — Transactions"),
    "Unpaid Balance":               ("sch_g_part1", "LNS_DEFAULT_UNPAID_BAL_AMT",  "Schedule G — Transactions"),
    "Amount Received":              ("sch_g_part1", "LNS_DEFAULT_INT_RCVD_AMT",    "Schedule G — Transactions"),
    "Overdue Amount":               ("sch_g_part1", "LNS_DEFAULT_INT_OVERDUE_AMT", "Schedule G — Transactions"),

    # Schedule MB — Multiemployer Actuarial
    "MB Plan Type":                 ("sch_mb", "MB_PLAN_TYPE_CODE",          "Schedule MB — Actuarial"),
    "MB Value Date":                ("sch_mb", "MB_VALUE_DATE",              "Schedule MB — Actuarial"),
    "MB Current Value Assets":      ("sch_mb", "MB_CURR_VALUE_AST_01_AMT",   "Schedule MB — Actuarial"),
    "MB Actuarial Firm":            ("sch_mb", "MB_ACTUARY_FIRM_NAME",       "Schedule MB — Actuarial"),
    "MB Funding Progress":          ("sch_mb", "MB_FNDNG_PROGRESS_IND",      "Schedule MB — Actuarial"),
    "MB Employer Contributions":    ("sch_mb", "MB_TOT_EMPLR_CONTRIB_02_AMT","Schedule MB — Actuarial"),
    "MB Normal Cost":               ("sch_mb", "MB_NORMAL_COST_AMT",   "Schedule MB — Actuarial"),

    # Schedule SB — Single Employer Actuarial
    "SB Funding Target":            ("sch_sb", "SB_TERM_FNDNG_TGT_AMT",     "Schedule SB — Actuarial"),
    "SB Actuarial Value Assets":    ("sch_sb", "SB_ACTRL_VALUE_AST_AMT",    "Schedule SB — Actuarial"),
    "SB Current Value Assets":      ("sch_sb", "SB_CURR_VALUE_AST_01_AMT",  "Schedule SB — Actuarial"),
    "SB Funding Target %":          ("sch_sb", "SB_FNDNG_TGT_PRCNT",        "Schedule SB — Actuarial"),
    "SB Employer Contributions":    ("sch_sb", "SB_TOT_EMPLR_CONTRIB_AMT",  "Schedule SB — Actuarial"),
    "SB Actuarial Firm":            ("sch_sb", "SB_ACTUARY_FIRM_NAME",       "Schedule SB — Actuarial"),
    "SB Effective Interest Rate":   ("sch_sb", "SB_EFF_INT_RATE_PRCNT",     "Schedule SB — Actuarial"),
    "SB Carryover Balance":         ("sch_sb", "SB_CARRYOVER_BOY_TOT_AMT",  "Schedule SB — Actuarial"),
}

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def query(sql, params=()):
    conn = get_db()
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows

def scalar(sql, params=()):
    conn = get_db()
    val  = conn.execute(sql, params).fetchone()[0]
    conn.close()
    return val

def col_exists(table, col):
    try:
        conn = get_db()
        conn.execute(f"SELECT [{col}] FROM [{table}] LIMIT 1")
        conn.close()
        return True
    except:
        return False

@app.route("/api/meta")
def meta():
    states   = [r["SPONS_DFE_MAIL_US_STATE"] for r in
                query("SELECT DISTINCT SPONS_DFE_MAIL_US_STATE FROM filings "
                      "WHERE SPONS_DFE_MAIL_US_STATE IS NOT NULL ORDER BY 1")]
    years    = [r["FORM_YEAR"] for r in
                query("SELECT DISTINCT FORM_YEAR FROM filings "
                      "WHERE FORM_YEAR IS NOT NULL ORDER BY 1")]
    total    = scalar("SELECT COUNT(*) FROM filings")
    aum_max  = scalar("SELECT MAX(NET_ASSETS_EOY_AMT) FROM filings") or 1_000_000_000
    part_max = scalar("SELECT MAX(TOT_PARTCP_BOY_CNT) FROM filings") or 100_000

    catalog = {}
    for fname, (table, col, group) in EXTRA_COLS.items():
        if not col_exists(table, col):
            continue
        if group not in catalog:
            catalog[group] = []
        catalog[group].append({"name": fname})

    return jsonify({
        "states": states, "years": years, "total_rows": total,
        "aum_min": 0, "aum_max": int(aum_max),
        "part_min": 0, "part_max": int(part_max),
        "column_catalog": catalog,
    })

def build_where(q):
    conds, params = [], []
    if q.get("plan","").strip():
        conds.append("f.PLAN_NAME LIKE ?")
        params.append(f"%{q['plan'].strip()}%")
    if q.get("sponsor","").strip():
        conds.append("f.SPONSOR_DFE_NAME LIKE ?")
        params.append(f"%{q['sponsor'].strip()}%")
    if q.get("ein","").strip():
        conds.append("f.SPONS_DFE_EIN LIKE ?")
        params.append(f"%{q['ein'].strip()}%")
    states = q.getlist("state") if hasattr(q,"getlist") else []
    if states:
        conds.append(f"f.SPONS_DFE_MAIL_US_STATE IN ({','.join('?'*len(states))})")
        params.extend(states)
    years = q.getlist("year") if hasattr(q,"getlist") else []
    if years:
        conds.append(f"f.FORM_YEAR IN ({','.join('?'*len(years))})")
        params.extend([int(y) for y in years])
    if q.get("aum_min","").strip():
        conds.append("f.NET_ASSETS_EOY_AMT >= ?")
        params.append(float(q["aum_min"]))
    if q.get("aum_max","").strip():
        conds.append("f.NET_ASSETS_EOY_AMT <= ?")
        params.append(float(q["aum_max"]))
    if q.get("part_min","").strip():
        conds.append("f.TOT_PARTCP_BOY_CNT >= ?")
        params.append(float(q["part_min"]))
    if q.get("part_max","").strip():
        conds.append("f.TOT_PARTCP_BOY_CNT <= ?")
        params.append(float(q["part_max"]))
    if q.get("city","").strip():
        conds.append("f.SPONS_DFE_MAIL_US_CITY LIKE ?")
        params.append(f"%{q['city'].strip()}%")
    if q.get("zip","").strip():
        conds.append("f.SPONS_DFE_MAIL_US_ZIP LIKE ?")
        params.append(f"%{q['zip'].strip()}%")
    if q.get("status","").strip():
        conds.append("f.FILING_STATUS = ?")
        params.append(q["status"].strip())
    if q.get("income_min","").strip():
        conds.append("schhh.TOT_INCOME_AMT >= ?")
        params.append(float(q["income_min"]))
    if q.get("income_max","").strip():
        conds.append("schhh.TOT_INCOME_AMT <= ?")
        params.append(float(q["income_max"]))
    if q.get("fees_min","").strip():
        conds.append("schhh.INVST_MGMT_FEES_AMT >= ?")
        params.append(float(q["fees_min"]))
    if q.get("fees_max","").strip():
        conds.append("schhh.INVST_MGMT_FEES_AMT <= ?")
        params.append(float(q["fees_max"]))
    if q.get("exp_min","").strip():
        conds.append("schhh.TOT_EXPENSES_AMT >= ?")
        params.append(float(q["exp_min"]))
    if q.get("exp_max","").strip():
        conds.append("schhh.TOT_EXPENSES_AMT <= ?")
        params.append(float(q["exp_max"]))
    if q.get("accountant","").strip():
        conds.append("schhh.ACCOUNTANT_FIRM_NAME LIKE ?")
        params.append(f"%{q['accountant'].strip()}%")
    if q.get("sb_funding_min","").strip():
        conds.append("schsb.SB_FNDNG_TGT_PRCNT >= ?")
        params.append(float(q["sb_funding_min"]))
    if q.get("sb_funding_max","").strip():
        conds.append("schsb.SB_FNDNG_TGT_PRCNT <= ?")
        params.append(float(q["sb_funding_max"]))
    if q.get("sb_actuary","").strip():
        conds.append("schsb.SB_ACTUARY_FIRM_NAME LIKE ?")
        params.append(f"%{q['sb_actuary'].strip()}%")
    if q.get("sb_contrib_min","").strip():
        conds.append("schsb.SB_TOT_EMPLR_CONTRIB_AMT >= ?")
        params.append(float(q["sb_contrib_min"]))
    if q.get("sb_contrib_max","").strip():
        conds.append("schsb.SB_TOT_EMPLR_CONTRIB_AMT <= ?")
        params.append(float(q["sb_contrib_max"]))
    if q.get("provider","").strip():
        conds.append("schc.PROVIDER_ELIGIBLE_NAME LIKE ?")
        params.append(f"%{q['provider'].strip()}%")
    if q.get("provider_ein","").strip():
        conds.append("schc.PROVIDER_ELIGIBLE_EIN LIKE ?")
        params.append(f"%{q['provider_ein'].strip()}%")
    if q.get("provider_state","").strip():
        conds.append("schc.PROVIDER_ELIGIBLE_US_STATE LIKE ?")
        params.append(f"%{q['provider_state'].strip()}%")
    if q.get("carrier","").strip():
        conds.append("scha.INS_CARRIER_NAME LIKE ?")
        params.append(f"%{q['carrier'].strip()}%")
    if q.get("prem_min","").strip():
        conds.append("scha.PENSION_PREM_PAID_TOT_AMT >= ?")
        params.append(float(q["prem_min"]))
    if q.get("prem_max","").strip():
        conds.append("scha.PENSION_PREM_PAID_TOT_AMT <= ?")
        params.append(float(q["prem_max"]))

    # Extra column filters
    extra_cols = q.getlist("extra_col") if hasattr(q,"getlist") else []
    extra_vals = q.getlist("extra_val") if hasattr(q,"getlist") else []
    joined = set()
    for ec, ev in zip(extra_cols, extra_vals):
        if ec in EXTRA_COLS and ev.strip():
            table, col, _ = EXTRA_COLS[ec]
            alias = table.replace("_","")
            conds.append(f"{alias}.[{col}] LIKE ?")
            params.append(f"%{ev.strip()}%")
            joined.add(table)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    return where, params

def get_base_joins():
    """Always-on joins needed for filter conditions on schedule tables."""
    return (
        "LEFT JOIN sch_h schhh ON schhh.rowid = (SELECT MIN(rowid) FROM sch_h WHERE ACK_ID = f.ACK_ID) "
        "LEFT JOIN sch_sb schsb ON schsb.rowid = (SELECT MIN(rowid) FROM sch_sb WHERE ACK_ID = f.ACK_ID) "
        "LEFT JOIN sch_c_part1_item1 schc ON schc.rowid = (SELECT MIN(rowid) FROM sch_c_part1_item1 WHERE ACK_ID = f.ACK_ID) "
        "LEFT JOIN sch_a scha ON scha.rowid = (SELECT MIN(rowid) FROM sch_a WHERE ACK_ID = f.ACK_ID)"
    )

def get_joins(extra_names):
    seen, joins = set(), []
    for name in extra_names:
        if name not in EXTRA_COLS: continue
        table, col, _ = EXTRA_COLS[name]
        if table not in seen:
            alias = table.replace("_","")
            joins.append(f"LEFT JOIN {table} {alias} ON {alias}.rowid = (SELECT MIN(rowid) FROM {table} WHERE ACK_ID = f.ACK_ID)")
            seen.add(table)
    return " ".join(joins)

def get_extra_select(extra_names):
    parts, seen = [], set()
    for name in extra_names:
        if name not in EXTRA_COLS: continue
        table, col, _ = EXTRA_COLS[name]
        if not col_exists(table, col): continue
        alias = table.replace("_","")
        key = f"{alias}.{col}"
        if key not in seen:
            parts.append(f'{alias}.[{col}] AS "{name}"')
            seen.add(key)
    return (", " + ", ".join(parts)) if parts else ""

@app.route("/api/search")
def search():
    q           = request.args
    extra_names = q.getlist("ecol")
    joins       = get_joins(extra_names)
    extra_sel   = get_extra_select(extra_names)
    where, params = build_where(q)

    sort  = q.get("sort","NET_ASSETS_EOY_AMT")
    valid = {"NET_ASSETS_EOY_AMT","TOT_PARTCP_BOY_CNT","FORM_YEAR","PLAN_NAME","DATE_RECEIVED"}
    if sort not in valid: sort = "NET_ASSETS_EOY_AMT"
    direc  = "ASC" if q.get("dir")=="asc" else "DESC"
    limit  = min(int(q.get("limit",100)),1000)
    offset = int(q.get("offset",0))

    total = scalar(f"SELECT COUNT(*) FROM filings f {joins} {where}", params)
    rows  = query(
        f"SELECT f.FORM_YEAR,f.PLAN_NAME,f.SPONSOR_DFE_NAME,f.SPONS_DFE_EIN,"
        f"f.SPONS_DFE_MAIL_US_CITY,f.SPONS_DFE_MAIL_US_STATE,"
        f"f.NET_ASSETS_EOY_AMT,f.TOT_PARTCP_BOY_CNT,f.FILING_STATUS,f.DATE_RECEIVED{extra_sel}"
        f" FROM filings f {joins} {where}"
        f" ORDER BY f.{sort} {direc} NULLS LAST"
        f" LIMIT {limit} OFFSET {offset}", params)
    return jsonify({"total": total, "rows": rows})

@app.route("/api/export")
def export_csv():
    q           = request.args
    extra_names = q.getlist("ecol")
    joins       = get_joins(extra_names)
    extra_sel   = get_extra_select(extra_names)
    where, params = build_where(q)
    rows = query(
        f"SELECT f.*{extra_sel} FROM filings f {joins} {where}"
        f" ORDER BY f.NET_ASSETS_EOY_AMT DESC NULLS LAST", params)
    if not rows:
        return Response("No data", mimetype="text/plain")
    buf = io.StringIO()
    w   = csv.DictWriter(buf, fieldnames=rows[0].keys())
    w.writeheader(); w.writerows(rows)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment; filename=form5500_export.csv"})

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

if __name__ == "__main__":
    print("\n  Form 5500 Explorer running at:  http://localhost:8080\n")
    app.run(debug=False, port=8080, host="0.0.0.0")

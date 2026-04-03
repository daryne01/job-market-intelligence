import streamlit as st
import subprocess
import pandas as pd
from io import StringIO

st.set_page_config(
    page_title="UK Data & AI Job Market Intelligence",
    page_icon="📊",
    layout="wide"
)

CONTAINER = "jobmarket_postgres"
USER = "jobmarket"
DB = "jobmarket_db"

def query(sql):
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB, "-t", "--csv"],
        input=sql, capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        return pd.DataFrame()
    try:
        return pd.read_csv(StringIO(result.stdout), header=None)
    except:
        return pd.DataFrame()

def qh(sql):
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB, "--csv"],
        input=sql, capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        return pd.DataFrame()
    try:
        return pd.read_csv(StringIO(result.stdout))
    except:
        return pd.DataFrame()

# ── Header ────────────────────────────────────────────────
st.title("🇬🇧 UK Data & AI Job Market Intelligence")
st.markdown("Live insights from Reed.co.uk · Updated daily · Powered by OpenAI + Home Office Sponsor Register")
st.divider()

# ── Metrics ───────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
total     = query("SELECT COUNT(*) FROM raw_jobs;")
sponsors  = query("SELECT COUNT(*) FROM raw_jobs WHERE on_sponsor_register = TRUE;")
contracts = query("SELECT COUNT(*) FROM processed_jobs WHERE contract_type = 'contract';")
visa      = query("SELECT COUNT(*) FROM processed_jobs WHERE sponsors_visa = TRUE;")
col1.metric("Total Jobs",                  int(total.iloc[0,0])     if not total.empty     else 0)
col2.metric("Confirmed Sponsor Companies", int(sponsors.iloc[0,0])  if not sponsors.empty  else 0)
col3.metric("Contract Roles",              int(contracts.iloc[0,0]) if not contracts.empty else 0)
col4.metric("Mention Visa Sponsorship",    int(visa.iloc[0,0])      if not visa.empty      else 0)
st.divider()

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Skills Demand",
    "💷 Salary Intelligence",
    "🛂 Sponsoring Companies",
    "🔍 Find & Apply"
])

# ── Tab 1: Skills ─────────────────────────────────────────
with tab1:
    st.subheader("Most In-Demand Skills")
    df = qh("SELECT skill, job_count, percentage FROM analytics.skill_demand LIMIT 20;")
    if not df.empty:
        st.bar_chart(df.set_index('skill')['job_count'])
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("No data yet.")

# ── Tab 2: Salary ─────────────────────────────────────────
with tab2:
    st.subheader("Salary Bands by Seniority")
    df = qh("SELECT seniority, location, avg_salary_min, avg_salary_max, avg_salary, job_count FROM analytics.salary_bands ORDER BY avg_salary DESC LIMIT 20;")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
        avg = qh("SELECT seniority, ROUND(AVG(avg_salary)) as avg_salary FROM analytics.salary_bands GROUP BY seniority ORDER BY avg_salary DESC;")
        if not avg.empty:
            st.bar_chart(avg.set_index('seniority'))
    else:
        st.info("No salary data yet.")

# ── Tab 3: Sponsors ───────────────────────────────────────
with tab3:
    st.subheader("🛂 Jobs from Visa-Sponsoring Companies")
    st.markdown("Verified against the **UK Home Office Register of Licensed Sponsors**")
    df = qh("""
        SELECT title, company, location, salary_raw,
               seniority, work_mode, contract_type,
               sponsors_visa, on_sponsor_register, url
        FROM analytics.sponsor_jobs ORDER BY date_posted DESC;
    """)
    if not df.empty:
        st.metric("Total Sponsoring Jobs", len(df))
        st.dataframe(df.drop(columns=['url']), hide_index=True, use_container_width=True)
        st.subheader("Apply")
        for _, row in df.iterrows():
            c1, c2 = st.columns([5, 1])
            with c1:
                st.markdown(f"**{row['title']}** at **{row['company']}** · {row['location']}")
            with c2:
                st.link_button("Apply →", str(row['url']))
    else:
        st.info("No sponsoring jobs found.")

# ── Tab 4: Find & Apply ───────────────────────────────────
with tab4:
    st.subheader("🔍 Find & Apply to Jobs")

    st.markdown("**Filters**")

    col1, col2 = st.columns(2)
    with col1:
        search_term = st.text_input("Search by title or company")
    with col2:
        sponsorship_filter = st.selectbox(
            "Sponsorship",
            options=[
                "All jobs",
                "On Home Office Register only",
                "Mentions visa sponsorship only",
                "Either HO Register OR mentions visa"
            ]
        )

    col1, col2, col3 = st.columns(3)
    with col1:
        seniority_filter = st.multiselect(
            "Seniority",
            options=["junior", "mid", "senior", "lead", "unknown"],
            default=["junior", "unknown"],
            help="'unknown' includes graduate and entry-level roles"
        )
    with col2:
        work_filter = st.multiselect(
            "Work Mode",
            options=["remote", "hybrid", "onsite", "unknown"],
            default=["remote", "hybrid", "onsite", "unknown"]
        )
    with col3:
        contract_filter = st.multiselect(
            "Contract Type",
            options=["permanent", "contract", "unknown"],
            default=["permanent", "contract", "unknown"]
        )

    # ── Build WHERE ───────────────────────────────────────
    where = ["1=1"]

    if sponsorship_filter == "On Home Office Register only":
        where.append("r.on_sponsor_register = TRUE")
    elif sponsorship_filter == "Mentions visa sponsorship only":
        where.append("p.sponsors_visa = TRUE")
    elif sponsorship_filter == "Either HO Register OR mentions visa":
        where.append("(r.on_sponsor_register = TRUE OR p.sponsors_visa = TRUE)")

    if seniority_filter:
        vals = "','".join(seniority_filter)
        where.append(f"(p.seniority IN ('{vals}') OR p.seniority IS NULL)")

    if work_filter:
        vals = "','".join(work_filter)
        where.append(f"p.work_mode IN ('{vals}')")

    if contract_filter:
        vals = "','".join(contract_filter)
        where.append(f"p.contract_type IN ('{vals}')")

    where_sql = " AND ".join(where)

    jobs_df = qh(f"""
        SELECT
            r.title, r.company, r.location, r.salary_raw,
            p.seniority, p.work_mode, p.contract_type,
            p.sponsors_visa, r.on_sponsor_register, r.url
        FROM raw_jobs r
        JOIN processed_jobs p ON p.raw_job_id = r.id
        WHERE {where_sql}
        ORDER BY r.scraped_at DESC;
    """)

    if not jobs_df.empty:
        if search_term:
            mask = (
                jobs_df['title'].str.contains(search_term, case=False, na=False) |
                jobs_df['company'].str.contains(search_term, case=False, na=False)
            )
            jobs_df = jobs_df[mask]

        for col in ['sponsors_visa', 'on_sponsor_register']:
            jobs_df[col] = jobs_df[col].map(
                {'t': '✅', 'f': '❌', True: '✅', False: '❌',
                 'True': '✅', 'False': '❌'}
            ).fillna('❌')

        st.metric("Jobs Found", len(jobs_df))

        display = jobs_df.drop(columns=['url']).copy()
        display.columns = ['Title', 'Company', 'Location', 'Salary',
                           'Seniority', 'Work Mode', 'Contract',
                           'Mentions Visa', 'On HO Register']
        st.dataframe(display, hide_index=True, use_container_width=True)

        st.subheader("Apply to These Roles")
        for _, row in jobs_df.iterrows():
            c1, c2 = st.columns([5, 1])
            with c1:
                visa_badge = "🛂" if row['sponsors_visa'] == '✅' else ""
                ho_badge = "✅ HO Sponsor" if row['on_sponsor_register'] == '✅' else ""
                st.markdown(
                    f"**{row['title']}** at **{row['company']}** · "
                    f"{row['location']} · {row['salary_raw']} {visa_badge} {ho_badge}"
                )
            with c2:
                url = str(row['url'])
                if url.startswith('http'):
                    st.link_button("Apply →", url)
    else:
        st.info("No jobs match your filters.")

st.divider()
st.caption("Reed.co.uk · Home Office Sponsor Register · Python · PostgreSQL · OpenAI · dbt · Streamlit")
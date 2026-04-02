import subprocess

CONTAINER = "jobmarket_postgres"
USER = "jobmarket"
DB = "jobmarket_db"

def run_sql(sql, description=""):
    if description:
        print(f"Running: {description}")
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB],
        input=sql, capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  ✓ Done")
    else:
        print(f"  ✗ Error: {result.stderr[:200]}")
    return result

# Create analytics schema
run_sql("CREATE SCHEMA IF NOT EXISTS analytics;", "Creating analytics schema")

# Model 1: Skill Demand
run_sql("""
DROP TABLE IF EXISTS analytics.skill_demand;
CREATE TABLE analytics.skill_demand AS
SELECT
    skill,
    COUNT(*) as job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM (
    SELECT UNNEST(skills) as skill
    FROM processed_jobs
    WHERE skills IS NOT NULL
) skills_expanded
GROUP BY skill
ORDER BY job_count DESC;
""", "Building skill_demand model")

# Model 2: Salary Bands
run_sql("""
DROP TABLE IF EXISTS analytics.salary_bands;
CREATE TABLE analytics.salary_bands AS
SELECT
    p.seniority,
    r.location,
    ROUND(AVG(p.salary_min)) as avg_salary_min,
    ROUND(AVG(p.salary_max)) as avg_salary_max,
    ROUND(AVG((p.salary_min + p.salary_max) / 2)) as avg_salary,
    COUNT(*) as job_count
FROM processed_jobs p
JOIN raw_jobs r ON p.raw_job_id = r.id
WHERE p.salary_min IS NOT NULL
AND p.salary_max IS NOT NULL
AND p.seniority IS NOT NULL
AND p.seniority != 'unknown'
GROUP BY p.seniority, r.location
ORDER BY avg_salary DESC;
""", "Building salary_bands model")

# Model 3: Sponsor Jobs
run_sql("""
DROP TABLE IF EXISTS analytics.sponsor_jobs;
CREATE TABLE analytics.sponsor_jobs AS
SELECT
    r.title,
    r.company,
    r.location,
    r.salary_raw,
    r.url,
    r.date_posted,
    r.on_sponsor_register,
    p.seniority,
    p.work_mode,
    p.contract_type,
    p.sponsors_visa,
    p.skills
FROM raw_jobs r
JOIN processed_jobs p ON p.raw_job_id = r.id
WHERE r.on_sponsor_register = TRUE
OR p.sponsors_visa = TRUE
ORDER BY r.date_posted DESC;
""", "Building sponsor_jobs model")

# Model 4: Job Volume
run_sql("""
DROP TABLE IF EXISTS analytics.job_volume;
CREATE TABLE analytics.job_volume AS
SELECT
    DATE(scraped_at) as date,
    COUNT(*) as jobs_posted,
    COUNT(*) FILTER (WHERE on_sponsor_register = TRUE) as sponsoring_jobs,
    COUNT(*) FILTER (WHERE on_sponsor_register = FALSE) as non_sponsoring_jobs
FROM raw_jobs
GROUP BY DATE(scraped_at)
ORDER BY date DESC;
""", "Building job_volume model")

# Verify all models
print("\nVerifying models...")
for table in ['skill_demand', 'salary_bands', 'sponsor_jobs', 'job_volume']:
    result = run_sql(f"SELECT COUNT(*) FROM analytics.{table};", f"Checking {table}")
    print(f"  {table}: {result.stdout.strip()}")

print("\nAll models built successfully.")
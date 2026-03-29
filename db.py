import subprocess

CONTAINER = "jobmarket_postgres"
USER = "jobmarket"
DB = "jobmarket_db"

def run_sql(sql, fetch=False):
    """Run SQL through Docker. Use fetch=True to get results back."""
    cmd = ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB]
    
    if fetch:
        cmd += ["-t", "--csv"]
    
    result = subprocess.run(
        cmd,
        input=sql,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"SQL Error: {result.stderr}")
    
    return result.stdout

def insert_job(title, company, location, salary_raw, description, date_posted, source, url):
    """Insert a raw job into the database."""
    sql = f"""
    INSERT INTO raw_jobs (title, company, location, salary_raw, description, date_posted, source, url)
    VALUES (
        $${title}$$,
        $${company}$$,
        $${location}$$,
        $${salary_raw}$$,
        $${description}$$,
        '{date_posted}',
        '{source}',
        $${url}$$
    )
    ON CONFLICT (url) DO NOTHING;
    """
    run_sql(sql)
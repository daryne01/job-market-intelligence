import subprocess
import requests
import csv
import io
from dotenv import load_dotenv

load_dotenv()

CONTAINER = "jobmarket_postgres"
USER = "jobmarket"
DB = "jobmarket_db"

SPONSOR_URL = "https://assets.publishing.service.gov.uk/media/69ce2d13837d4b59e502d119/2026-04-02_-_Worker_and_Temporary_Worker.csv"

def run_sql(sql):
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB],
        input=sql, capture_output=True, text=True
    )
    return result

def create_sponsor_table():
    sql = """
    CREATE TABLE IF NOT EXISTS sponsor_register (
        id           SERIAL PRIMARY KEY,
        organisation TEXT,
        town         TEXT,
        county       TEXT,
        type_rating  TEXT,
        route        TEXT,
        updated_at   TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_sponsor_org ON sponsor_register(organisation);
    """
    result = run_sql(sql)
    print("Table ready." if result.returncode == 0 else f"Error: {result.stderr}")

def download_and_store():
    print("Downloading latest Home Office sponsor register (2 April 2026)...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(SPONSOR_URL, headers=headers, timeout=120)
    print(f"Status: {response.status_code} | Size: {len(response.content)} bytes")

    if response.status_code != 200:
        print("Download failed.")
        return

    run_sql("TRUNCATE TABLE sponsor_register;")
    content = response.content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(content))

    batch = []
    count = 0

    for row in reader:
        org = (row.get('Organisation Name') or '').replace("'", "''")
        town = (row.get('Town/City') or '').replace("'", "''")
        county = (row.get('County') or '').replace("'", "''")
        type_rating = (row.get('Type & Rating') or '').replace("'", "''")
        route = (row.get('Route') or '').replace("'", "''")

        if org:
            batch.append(f"('{org}','{town}','{county}','{type_rating}','{route}')")
            count += 1

        if len(batch) == 500:
            run_sql(f"INSERT INTO sponsor_register (organisation,town,county,type_rating,route) VALUES {','.join(batch)};")
            batch = []
            print(f"  Inserted {count} sponsors...")

    if batch:
        run_sql(f"INSERT INTO sponsor_register (organisation,town,county,type_rating,route) VALUES {','.join(batch)};")

    print(f"Total sponsors stored: {count}")

def cross_reference():
    print("\nCross-referencing companies with sponsor register...")
    run_sql("ALTER TABLE raw_jobs ADD COLUMN IF NOT EXISTS on_sponsor_register BOOLEAN DEFAULT FALSE;")
    run_sql("""
        UPDATE raw_jobs r SET on_sponsor_register = TRUE
        FROM sponsor_register s
        WHERE LOWER(TRIM(r.company)) = LOWER(TRIM(s.organisation));
    """)
    result = run_sql("""
        SELECT
            COUNT(*) FILTER (WHERE on_sponsor_register = true) as on_register,
            COUNT(*) FILTER (WHERE on_sponsor_register = false) as not_on_register
        FROM raw_jobs;
    """)
    print(result.stdout)

if __name__ == "__main__":
    create_sponsor_table()
    download_and_store()
    cross_reference()
    print("Sponsor register pipeline complete.")
import subprocess
import os
from dotenv import load_dotenv

load_dotenv()

CONTAINER = "jobmarket_postgres"
USER = "jobmarket"
DB = "jobmarket_db"

def run_sql(sql):
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB],
        input=sql,
        capture_output=True,
        text=True
    )
    return result

def init_database():
    with open("data/schema.sql", "r") as f:
        schema = f.read()
    
    result = run_sql(schema)
    
    if result.returncode == 0:
        print("Database initialised successfully.")
        print("Tables created: raw_jobs, processed_jobs")
    else:
        print(f"Error: {result.stderr}")

if __name__ == "__main__":
    init_database()
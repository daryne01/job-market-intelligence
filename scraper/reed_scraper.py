import requests
import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

REED_API_KEY = os.getenv("REED_API_KEY")
CONTAINER = "jobmarket_postgres"
USER = "jobmarket"
DB = "jobmarket_db"

SEARCH_TERMS = [
    "data engineer",
    "AI engineer",
    "machine learning engineer",
    "data platform engineer",
    "analytics engineer"
]

def run_sql(sql):
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB],
        input=sql,
        capture_output=True,
        text=True
    )
    return result

def search_jobs(keyword, location="United Kingdom", results_to_take=100):
    """Search Reed API for jobs matching keyword."""
    url = "https://www.reed.co.uk/api/1.0/search"
    
    params = {
        "keywords": keyword,
        "locationName": location,
        "resultsToTake": results_to_take
    }
    
    response = requests.get(
        url,
        params=params,
        auth=(REED_API_KEY, "")
    )
    
    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(f"Error fetching {keyword}: {response.status_code}")
        return []

def get_job_details(job_id):
    """Get full job description from Reed API."""
    url = f"https://www.reed.co.uk/api/1.0/jobs/{job_id}"
    
    response = requests.get(
        url,
        auth=(REED_API_KEY, "")
    )
    
    if response.status_code == 200:
        return response.json()
    return None

def clean_text(text):
    """Remove characters that break SQL."""
    if not text:
        return ""
    return str(text).replace("'", "''").replace("\x00", "")

def save_job(job):
    """Save a single job to the database."""
    title = clean_text(job.get("jobTitle", ""))
    company = clean_text(job.get("employerName", ""))
    location = clean_text(job.get("locationName", ""))
    salary_raw = f"{job.get('minimumSalary', '')} - {job.get('maximumSalary', '')}"
    url = f"https://www.reed.co.uk/jobs/{job.get('jobId', '')}"
    date_posted = job.get("date", datetime.now().strftime("%Y-%m-%d"))
    
    # Get full description
    details = get_job_details(job.get("jobId"))
    description = clean_text(details.get("jobDescription", "")) if details else ""
    
    sql = f"""
    INSERT INTO raw_jobs (title, company, location, salary_raw, description, date_posted, source, url)
    VALUES (
        '{title}',
        '{company}',
        '{location}',
        '{salary_raw}',
        '{description[:5000]}',
        NOW(),
        'reed',
        '{url}'
    )
    ON CONFLICT (url) DO NOTHING;
    """
    
    result = run_sql(sql)
    return result.returncode == 0

def run_scraper():
    print(f"Starting scraper at {datetime.now().strftime('%H:%M:%S')}")
    total_saved = 0
    
    for keyword in SEARCH_TERMS:
        print(f"\nSearching: '{keyword}'...")
        jobs = search_jobs(keyword)
        print(f"Found {len(jobs)} jobs")
        
        saved = 0
        for job in jobs:
            if save_job(job):
                saved += 1
        
        print(f"Saved {saved} new jobs for '{keyword}'")
        total_saved += saved
    
    print(f"\nScraper finished. Total new jobs saved: {total_saved}")
    
    # Show count in database
    result = run_sql("SELECT COUNT(*) FROM raw_jobs;")
    print(f"Total jobs in database: {result.stdout.strip()}")

if __name__ == "__main__":
    run_scraper()
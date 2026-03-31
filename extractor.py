import subprocess
import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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

def get_unprocessed_jobs():
    """Get all jobs that haven't been processed by OpenAI yet."""
    sql = """
    SELECT r.id, r.title, r.company, r.description, r.salary_raw
    FROM raw_jobs r
    LEFT JOIN processed_jobs p ON r.id = p.raw_job_id
    WHERE p.id IS NULL
    AND r.description IS NOT NULL
    AND r.description != '';
    """
    result = run_sql(sql)
    
    jobs = []
    lines = result.stdout.strip().split('\n')
    
    # Skip header and separator lines
    for line in lines[2:]:
        if line.strip() and '(' not in line:
            parts = line.split('|')
            if len(parts) >= 5:
                jobs.append({
                    'id': parts[0].strip(),
                    'title': parts[1].strip(),
                    'company': parts[2].strip(),
                    'description': parts[3].strip(),
                    'salary_raw': parts[4].strip()
                })
    return jobs

def extract_with_openai(job):
    """Send job description to OpenAI and extract structured data."""
    
    prompt = f"""
You are a data extraction assistant. Extract information from this job posting and return ONLY a JSON object with no extra text.

Job Title: {job['title']}
Company: {job['company']}
Salary Info: {job['salary_raw']}
Description: {job['description'][:3000]}

Extract and return this exact JSON structure:
{{
    "skills": ["list", "of", "technical", "skills", "mentioned"],
    "salary_min": <minimum salary as integer, null if not found>,
    "salary_max": <maximum salary as integer, null if not found>,
    "seniority": "<junior|mid|senior|lead|null>",
    "work_mode": "<remote|hybrid|onsite|null>",
    "experience_years": <years of experience required as integer, null if not found>,
    "sponsors_visa": <true if job mentions visa sponsorship, false otherwise>,
    "contract_type": "<permanent|contract|temporary|null>"
}}

Rules:
- skills must be technical skills only (Python, SQL, AWS, dbt, Spark, etc.)
- sponsors_visa is true if description mentions: sponsor, visa, certificate of sponsorship, skilled worker
- Return ONLY the JSON, no explanation
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    
    raw = response.choices[0].message.content.strip()
    
    # Clean up response in case OpenAI adds markdown
    raw = raw.replace("```json", "").replace("```", "").strip()
    
    return json.loads(raw)

def save_processed_job(raw_job_id, data):
    """Save extracted data to processed_jobs table."""
    skills = data.get("skills", [])
    skills_sql = "{" + ",".join([f'"{s}"' for s in skills]) + "}"
    
    salary_min = data.get("salary_min") or "NULL"
    salary_max = data.get("salary_max") or "NULL"
    experience = data.get("experience_years") or "NULL"
    seniority = data.get("seniority") or "unknown"
    work_mode = data.get("work_mode") or "unknown"
    sponsors = str(data.get("sponsors_visa", False)).lower()
    contract_type = data.get("contract_type") or "unknown"

    sql = f"""
    ALTER TABLE processed_jobs 
    ADD COLUMN IF NOT EXISTS sponsors_visa BOOLEAN DEFAULT FALSE;
    
    ALTER TABLE processed_jobs 
    ADD COLUMN IF NOT EXISTS contract_type TEXT;

    INSERT INTO processed_jobs 
    (raw_job_id, skills, salary_min, salary_max, seniority, work_mode, experience_years, sponsors_visa, contract_type)
    VALUES (
        {raw_job_id},
        '{skills_sql}',
        {salary_min},
        {salary_max},
        '{seniority}',
        '{work_mode}',
        {experience},
        {sponsors},
        '{contract_type}'
    );
    """
    run_sql(sql)

def run_extractor():
    print("Starting OpenAI extraction...")
    
    jobs = get_unprocessed_jobs()
    print(f"Found {len(jobs)} unprocessed jobs")
    
    success = 0
    failed = 0
    
    for i, job in enumerate(jobs):
        try:
            print(f"Processing {i+1}/{len(jobs)}: {job['title']} at {job['company']}")
            data = extract_with_openai(job)
            save_processed_job(job['id'], data)
            
            # Show sponsorship result
            if data.get('sponsors_visa'):
                print(f"  ✓ SPONSORS VISA | {data.get('contract_type')} | Skills: {', '.join(data.get('skills', [])[:3])}")
            else:
                print(f"  ✓ {data.get('contract_type')} | Skills: {', '.join(data.get('skills', [])[:3])}")
            
            success += 1
            
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            failed += 1
    
    print(f"\nExtraction complete.")
    print(f"Successfully processed: {success}")
    print(f"Failed: {failed}")
    
    # Show sponsorship summary
    result = run_sql("""
        SELECT 
            COUNT(*) FILTER (WHERE sponsors_visa = true) as sponsoring,
            COUNT(*) FILTER (WHERE sponsors_visa = false) as not_sponsoring,
            COUNT(*) FILTER (WHERE contract_type = 'contract') as contracts,
            COUNT(*) FILTER (WHERE contract_type = 'permanent') as permanent
        FROM processed_jobs;
    """)
    print(f"\nDatabase summary:")
    print(result.stdout)

if __name__ == "__main__":
    run_extractor()
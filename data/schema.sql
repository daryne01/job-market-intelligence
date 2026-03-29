-- Raw layer (bronze) - untouched scraped data
CREATE TABLE IF NOT EXISTS raw_jobs (
    id          SERIAL PRIMARY KEY,
    title       TEXT,
    company     TEXT,
    location    TEXT,
    salary_raw  TEXT,
    description TEXT,
    date_posted DATE,
    source      TEXT,
    url         TEXT UNIQUE,
    scraped_at  TIMESTAMP DEFAULT NOW()
);

-- Processed layer (silver) - LLM extracted fields
CREATE TABLE IF NOT EXISTS processed_jobs (
    id                  SERIAL PRIMARY KEY,
    raw_job_id          INTEGER REFERENCES raw_jobs(id),
    skills              TEXT[],
    salary_min          INTEGER,
    salary_max          INTEGER,
    seniority           TEXT,
    work_mode           TEXT,
    experience_years    INTEGER,
    processed_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_jobs_scraped_at ON raw_jobs(scraped_at);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_source ON raw_jobs(source);
CREATE INDEX IF NOT EXISTS idx_processed_jobs_seniority ON processed_jobs(seniority);

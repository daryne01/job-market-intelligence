{{ config(materialized='table') }}

SELECT
    DATE(scraped_at) as date,
    COUNT(*) as jobs_posted,
    COUNT(*) FILTER (WHERE on_sponsor_register = TRUE) as sponsoring_jobs,
    COUNT(*) FILTER (WHERE on_sponsor_register = FALSE) as non_sponsoring_jobs
FROM {{ source('public', 'raw_jobs') }}
GROUP BY DATE(scraped_at)
ORDER BY date DESC
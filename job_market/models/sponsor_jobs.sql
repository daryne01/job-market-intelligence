{{ config(materialized='table') }}

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
FROM {{ source('public', 'raw_jobs') }} r
JOIN {{ source('public', 'processed_jobs') }} p ON p.raw_job_id = r.id
WHERE r.on_sponsor_register = TRUE
OR p.sponsors_visa = TRUE
ORDER BY r.date_posted DESC
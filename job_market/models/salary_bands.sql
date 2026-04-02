{{ config(materialized='table') }}

SELECT
    p.seniority,
    r.location,
    ROUND(AVG(p.salary_min)) as avg_salary_min,
    ROUND(AVG(p.salary_max)) as avg_salary_max,
    ROUND(AVG((p.salary_min + p.salary_max) / 2)) as avg_salary,
    COUNT(*) as job_count
FROM {{ source('public', 'processed_jobs') }} p
JOIN {{ source('public', 'raw_jobs') }} r ON p.raw_job_id = r.id
WHERE p.salary_min IS NOT NULL
AND p.salary_max IS NOT NULL
AND p.seniority IS NOT NULL
AND p.seniority != 'unknown'
GROUP BY p.seniority, r.location
ORDER BY avg_salary DESC
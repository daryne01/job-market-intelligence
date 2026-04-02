{{ config(materialized='table') }}

SELECT
    skill,
    COUNT(*) as job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM (
    SELECT UNNEST(skills) as skill
    FROM {{ source('public', 'processed_jobs') }}
    WHERE skills IS NOT NULL
) skills_expanded
GROUP BY skill
ORDER BY job_count DESC
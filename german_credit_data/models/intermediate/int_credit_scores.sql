WITH base_data AS (
    SELECT 
        *,
        -- Example scoring logic: higher amounts and durations results in lower scores
        (credit_amount / 1000) * 0.3 +
        (duration / 12) * 0.5 +
        (CASE WHEN job_category = 'Management' THEN 10 ELSE 5 END) AS base_score
    FROM {{ ref('stg_german_credit_data') }}
)

SELECT 
    *,
    ROUND(base_score) as credit_score
FROM base_data

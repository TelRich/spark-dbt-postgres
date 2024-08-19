SELECT
    credit_amount,
    duration,
    age,
    gender,
    housing,
    job_category,
    risk_segment,
    credit_score,
    age_group,
    default_risk_percentage
FROM {{ ref('int_default_risk') }}


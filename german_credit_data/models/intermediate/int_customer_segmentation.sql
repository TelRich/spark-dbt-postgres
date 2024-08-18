WITH scored_data AS (
    SELECT
        *, 
        CASE
            WHEN credit_score >= 80 THEN 'Low Risk'
            WHEN credit_score >= 50 THEN 'Medium Risk'
            ELSE 'High Risk'
        END AS risk_segment
    FROM {{ ref('int_credit_scores') }}
)

SELECT
    *,
    CASE
        WHEN age < 30 THEN 'Young'
        WHEN age BETWEEN 30 AND 50 THEN 'Middle-aged'
        ELSE 'Senior'
    END AS age_group
FROM scored_data
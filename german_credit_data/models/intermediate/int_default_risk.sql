WITH risk_data AS (
    SELECT
        *,
        CASE
            WHEN risk_segment = 'High Risk' and job_category = 'Unskilled' THEN 0.7
            WHEN risk_segment = 'Medium Risk' THEN 0.3
            ELSE 0.1
        END AS default_risk
    FROM {{ ref('int_customer_segmentation') }}
)

SELECT
    *,
    ROUND(default_risk * 100) AS default_risk_percentage
FROM risk_data
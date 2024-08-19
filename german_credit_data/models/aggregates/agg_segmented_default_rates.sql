SELECT
    risk_segment,
    age_group,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN default_risk_percentage > 50 THEN 1 ELSE 0 END) AS high_risk_customers,
    ROUND(SUM(CASE WHEN default_risk_percentage > 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_percentage
FROM {{ ref('int_default_risk') }}
GROUP BY risk_segment, age_group
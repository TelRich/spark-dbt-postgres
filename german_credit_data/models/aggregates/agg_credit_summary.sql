SELECT
    risk_segment,
    AVG(credit_amount) AS avg_credit_amount,
    AVG(credit_score) AS avg_credit_score,
    AVG(default_risk_percentage) AS avg_default_risk
FROM {{ ref('int_default_risk') }}
GROUP BY risk_segment
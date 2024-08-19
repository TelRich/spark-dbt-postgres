WITH behaviour_data AS (
    SELECT
        *,
        CASE
            WHEN purpose = 'car' AND credit_amount > 5000 THEN 'High Credit for Car'
            WHEN purpose = 'furniture/equipment' AND credit_amount < 2000 THEN 'Low Credit for Furniture'
            ELSE 'Standard Credit'
        END AS credit_behaviour,
        CASE 
            WHEN checking_account = 'little' THEN 'Small Balance'
            WHEN checking_account = 'moderate' THEN 'Moderate Balance'
            WHEN checking_account = 'rich' THEN 'High Balance'
            ELSE 'Unknown Balance'
        END AS checking_account_category
    FROM {{ ref('stg_german_credit_data') }}
)

SELECT
    *
FROM behaviour_data
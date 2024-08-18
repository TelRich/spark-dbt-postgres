with raw_data as (
    SELECT 
        "Credit amount" AS credit_amount,
        "Duration" AS duration,
        "Age" AS age,
        "Sex" AS gender,
        "Housing" AS housing,
        "Purpose" AS purpose,
        "Checking account" AS checking_account,
        "Saving accounts" AS saving_accounts,
        CASE
            WHEN "Job" = 1 THEN 'Unskilled'
            WHEN "Job" = 2 THEN 'Skilled'
            WHEN "Job" = 3 THEN 'Highly Skilled'
            WHEN "Job" = 4 THEN 'Management'
            ELSE 'Unknown'
        END AS job_category
    FROM {{ source('warehouse', 'stage_loan') }}
)

SELECT * FROM raw_data

{% if target.name == 'dev' %}
LIMIT 5
{% endif %}
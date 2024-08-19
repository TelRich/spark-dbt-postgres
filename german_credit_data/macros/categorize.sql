{% macro categorize_risk(credit_score) %}
  CASE  
    WHEN {{ credit_score }} >= 80 THEN 'Low Risk'
    WHEN {{ credit_score }} >= 50 THEN 'Medium Risk'
    ELSE 'High Risk'
  END
{% endmacro %}
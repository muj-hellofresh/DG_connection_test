-- test SQL query (using the error rates)
-- testing the git push

SELECT ep.country,
       ep.hellofresh_week_where_error_happened,
       dd.date_string AS date_entered,
       ep.dc,
       ep.mapped_accountable_team,
       ep.error_reported_through AS error_type,
       ep.sku_code,
       ep.sku_clean_name AS sku_name,
       ep.ingredient_category,
       ep.mapped_error_category,
       ep.mapped_error_subcategory,
       ep.mapped_complaint,
       COUNT(*) error_count,
       SUM(ep.compensation_amount_local) AS compensation_aud
FROM materialized_views.cc_errors_processed ep
         JOIN dimensions.date_dimension dd
              ON dd.sk_date = ep.fk_entered_date
WHERE 1=1
  AND hellofresh_week_where_error_happened >= '2022-W01'
  AND country IN ('AU', 'AO')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1,2,3,4,5,6,13 DESC;
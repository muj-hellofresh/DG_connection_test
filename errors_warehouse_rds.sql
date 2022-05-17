SELECT hellofresh_week_where_error_happened, mapped_accountable_team,
       CASE
           WHEN mapped_complaint = 'Supplier Issue' OR 'Ordering Issue' THEN 'Supplier/Ordering Issue'
           WHEN mapped_complaint = 'Forecast Swing' OR 'Ordering Error' THEN 'Ordering Issue'
           WHEN mapped_complaint = 'Supplier Failed Delivery' OR 'Supplier Short Delivered' or 'Supplier Late Delivery' THEN 'Supplier Issue'
           WHEN mapped_complaint = 'Mouldy/Spoiled/Rotten' THEN 'Mouldy/Spoiled/Rotten'
           WHEN mapped_complaint = 'Damaged' THEN 'Damaged'
           ELSE 'Other' END AS complaint_type,
       CASE
           WHEN ingredient_category = 'PHF' THEN 'PHF'
           WHEN ingredient_category = 'PTN' THEN 'PTN'
           ELSE 'Other' END AS sku_category,
       COUNT(*) as complaints
FROM warehouse.dwh_errors_corrected
WHERE country IN ('AU')
  AND hellofresh_week_where_error_happened >= '2020-W39'
  AND mapped_accountable_team IN ('Procurement')
GROUP BY country, hellofresh_week_where_error_happened, mapped_accountable_team, complaint_type, sku_category;

----------

SELECT hellofresh_week_where_error_happened, mapped_accountable_team,
       CASE
           WHEN mapped_complaint = 'Stock Issue' THEN 'Stock Issue'
           WHEN mapped_complaint = 'Warehouse / Inventory - Stock Issue' OR 'Tech / Production Issue' THEN 'Production Stock Issue'
           WHEN mapped_complaint = 'Missing' THEN 'Missing'
           WHEN mapped_complaint = 'Incorrect' THEN 'Incorrect'
           ELSE 'Other' END AS complaint_type,
       COUNT(*) as complaints
FROM warehouse.dwh_errors_corrected
WHERE country IN ('AU')
  AND hellofresh_week_where_error_happened >= '2021-W01'
  -- AND mapped_accountable_team IN ('Production')
GROUP BY country, hellofresh_week_where_error_happened, mapped_accountable_team, complaint_type;
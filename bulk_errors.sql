SELECT ep.hellofresh_week_where_error_happened AS HF_WEEK,
       ep.dc AS dc,
       SUM(CASE WHEN ep.compensation_amount_local = 0 THEN 1
                ELSE 0 END) AS substitute_count,
       SUM(CASE WHEN ep.compensation_amount_local > 0 THEN 1
                ELSE 0 END) AS missing_count,
       SUM(ep.compensation_amount_local) AS compensation_amt_aud
FROM materialized_views.cc_errors_processed ep
         JOIN dimensions.product_dimension AS pd    -- joining the product dimensions table to ensure only counting the 'mealboxes' product
              ON pd.sk_product = ep.fk_product AND
                 pd.is_mealbox = true
WHERE ep.country = 'AU' AND
        ep.hellofresh_week_where_error_happened >= '2021-W01' AND
        ep.error_reported_through = 'bulk_upload' AND
        --ep.error_category = 'operations_use_only' AND  -- for 2021-W25 onwards
        ep.error_subcategory = '_ingredients' AND
        ep.ingredient_family NOT LIKE 'unmapped' AND
        ep.dc NOT LIKE 'unmapped'
GROUP BY 1,2
ORDER BY 1,2;



SELECT ep.country AS country,
       ep.hellofresh_week_where_error_happened AS HF_WEEK,
       COUNT(*)
FROM materialized_views.cc_errors_processed ep
         JOIN dimensions.product_dimension AS pd    -- joining the product dimensions table to ensure only counting the 'mealboxes' product
              ON pd.sk_product = ep.fk_product AND
                 pd.is_mealbox = true
WHERE ep.country IN ('AU', 'NZ') AND
        ep.hellofresh_week_where_error_happened >= '2018-W01' AND
        ep.error_reported_through = 'bulk_upload' AND -- question mark?
  --ep.error_category = 'operations_use_only' AND  -- for 2021-W25 onwards
        ep.error_subcategory = '_ingredients' AND
        ep.ingredient_family NOT LIKE 'unmapped' AND
        ep.dc NOT LIKE 'unmapped'
GROUP BY 1,2
ORDER BY 1,2;


-- query by Julio

WITH dates AS (
    SELECT hellofresh_week,
    CAST(hellofresh_year AS STRING)                                  as hf_year,
    CONCAT(CAST(hellofresh_year AS STRING), '-M',
    LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
    CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)


SELECT
    ep.country
       ,d.hellofresh_week
     ,SUM(CASE WHEN ep.error_reported_through = 'bulk_upload' THEN 1 ELSE 0 END ) AS Bulk
 --    ,SUM(CASE WHEN ep.error_reported_through = 'customer_complaint' THEN 1 ELSE 0 END ) AS Customer
 --    ,SUM(CASE WHEN ep.error_reported_through = 'self_reported' OR ep.error_reported_through = 'self_reported_CERT' THEN 1 ELSE 0 END ) AS self_reported
 --    ,COUNT(ep.complaint_id)
FROM materialized_views.cc_errors_processed AS ep
JOIN dates AS d
    ON ep.hellofresh_week_where_error_happened = d.hellofresh_week
WHERE ep.country IN ('AU', 'NZ')
  AND ep.hellofresh_week_where_error_happened >= '2018-W01'
GROUP BY 1,2
ORDER BY 1,2 ASC;

-- rough calculation, need to confirm if the counting method of error is correct
-- calculating the number of errors over the past 3 years per category


WITH dates AS (
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)

SELECT d.hf_quarter,
       ep.dc,
--       ep.hellofresh_week_where_error_happened AS hf_week,
       SUM(CASE WHEN ep.error_reported_through LIKE 'bulk%' THEN 1 END) AS bulk_error,
       SUM(CASE WHEN ep.error_reported_through LIKE 'cust%' THEN 1 END) AS cc_error,
       SUM(CASE WHEN ep.error_reported_through LIKE '%CERT' THEN 1 END) AS cert_error,
       SUM(CASE WHEN ep.error_reported_through LIKE 'self_reported' THEN 1 END) AS self_reported_error
FROM materialized_views.cc_errors_processed ep
JOIN dates d
ON ep.hellofresh_week_where_error_happened = d.hellofresh_week
WHERE ep.country = 'AU'
  AND ep.hellofresh_week_where_error_happened >= '2017-W01'
--  AND ep.dc NOT LIKE 'unmapped'
GROUP BY 1,2
ORDER BY 1,2;

-- Calculating the box count

--boxes shipped per week overall using mat_view
SELECT bs.hellofresh_delivery_week,
       bs.box_count
FROM materialized_views.mbi_anz_boxes_shipped_with_costs bs
WHERE bs.hellofresh_delivery_week >= '2019-W01'
  AND bs.country = 'AU'
ORDER BY 1;

--boxes shipped per qaurter per dc using fact_tables

WITH dates AS (
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)

SELECT d.hf_quarter,
       --CASE
         --  WHEN gd.subdivision_1_iso_code LIKE 'NT' THEN 'Perth'
         -- WHEN gd.subdivision_1_iso_code IN ('VIC', 'SA') THEN 'Melbourne'
         --  ELSE 'Sydney' END AS dc,
       SUM(bs.box_shipped) quarterly_boxes
FROM fact_tables.boxes_shipped bs
JOIN dimensions.product_dimension pd
    ON bs.fk_product = pd.sk_product
JOIN dates d
    ON bs.hellofresh_delivery_week = d.hellofresh_week
--JOIN dimensions.geography_dimension gd
--    ON gd.sk_geography = bs.fk_geography
WHERE 1=1
  AND bs.hellofresh_delivery_week >= '2018-W01'
  AND bs.country = 'AU'
--  AND gd.subdivision_1_iso_code IS NOT NULL
  AND pd.is_mealbox = true
GROUP BY 1
ORDER BY 1;


SELECT hellofresh_week,
       CAST(hellofresh_year AS STRING)                                  as hf_year,
       CONCAT(CAST(hellofresh_year AS STRING), '-M',
              LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
       CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
FROM dimensions.date_dimension dd
WHERE dd.year >= 2018 AND dd.year <= 2022
GROUP BY 1, 2--, 3, 4
ORDER BY 1;

-- calculating the error rate per category (for each type of error separately)
-- going to use the error_subcategory column
WITH t1 AS (
    SELECT error_subcategory sub_cat,
       COUNT(*) OVER (PARTITION BY error_subcategory) AS cat_count,
        COUNT(*) OVER () AS tot_count
FROM materialized_views.cc_errors_processed ep
WHERE 1=1
AND ep.country = 'AU'
AND ep.hellofresh_week_where_error_happened >= '2017-W01'
AND ep.error_reported_through LIKE 'bulk%')

SELECT DISTINCT *
FROM t1
ORDER BY 2 DESC;

-- count by complaint (more granular than sub_cat)
WITH dates AS (
         SELECT hellofresh_week,
                CAST(hellofresh_year AS STRING)                                  as hf_year,
                CONCAT(CAST(hellofresh_year AS STRING), '-M',
                       LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
                CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
         FROM dimensions.date_dimension dd
         WHERE dd.year >= 2018
         GROUP BY 1, 2, 3, 4),

comp_count AS (
    SELECT hf_year,
           CASE
               WHEN ep.ingredient_category = 'PHF' THEN 'PHF'
               WHEN ep.ingredient_category = 'PTN' THEN 'PTN'
               ELSE 'Other' END AS op_category    ,
       COUNT(*) complaint_count
    FROM materialized_views.cc_errors_processed ep
    JOIN dates d
    ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
    WHERE 1=1
      AND ep.country = 'AU'
      AND ep.hellofresh_week_where_error_happened >= '2021-W01'
      AND ep.error_reported_through LIKE 'bulk%'
      AND ep.mapped_detailed_accountable_team LIKE 'Op%'
    GROUP BY 1,2
    ORDER BY 1,3 DESC),


yearly_count AS (
    SELECT hf_year,
           COUNT(*) year_count
    FROM materialized_views.cc_errors_processed ep
             JOIN dates d
                  ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
    WHERE 1=1
      AND ep.country = 'AU'
      AND ep.hellofresh_week_where_error_happened >= '2021-W01'
      AND ep.error_reported_through LIKE 'bulk%'
    GROUP BY 1
    ORDER BY 1)

SELECT cc.hf_year,
       cc.op_category,
       cc.complaint_count,
       yc.year_count,
       cc.complaint_count/yc.year_count AS comp_percent
FROM comp_count cc
JOIN yearly_count yc
ON cc.hf_year = yc.hf_year
ORDER BY 1,5 DESC;

-- count by sub category
WITH dates AS (
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     comp_count AS (
         SELECT d.hf_year,
                ep.mapped_detailed_accountable_team AS accountable_team, --originally was error_subcategory
               -- dc,
                COUNT(*) complaint_count
         FROM materialized_views.cc_errors_processed ep
                  JOIN dates d
                       ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
         WHERE 1=1
           AND ep.country = 'AU'
           AND ep.hellofresh_week_where_error_happened >= '2018-W01'
           AND (ep.error_reported_through LIKE 'cust%' OR ep.error_reported_through LIKE 'self%') --change to whatever error type required
         GROUP BY 1,2--,3
         ORDER BY 1,3 DESC),


     yearly_count AS (
         SELECT hf_year,
                COUNT(*) year_count
         FROM materialized_views.cc_errors_processed ep
                  JOIN dates d
                       ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
         WHERE 1=1
           AND ep.country = 'AU'
           AND ep.hellofresh_week_where_error_happened >= '2018-W01'
           AND (ep.error_reported_through LIKE 'cust%' OR ep.error_reported_through LIKE 'self%') --change to whatever error type required
         GROUP BY 1
         ORDER BY 1)

SELECT cc.hf_year,
       cc.accountable_team,
      --cc.dc,
       cc.complaint_count,
       yc.year_count,
       cc.complaint_count/yc.year_count AS comp_percent
FROM comp_count cc
         JOIN yearly_count yc
              ON cc.hf_year = yc.hf_year
ORDER BY 1,5 DESC;

-------
--rough work

SELECT ep.mapped_detailed_accountable_team,
       ep.mapped_error_category,
       ep.mapped_error_subcategory,
       --ep.mapped_complaint,
       COUNT(*) AS error_count
FROM materialized_views.cc_errors_processed ep
WHERE ep.country = 'AU'
AND ep.hellofresh_week_where_error_happened >= '2021-W01'
AND (ep.error_reported_through LIKE 'cust%' OR ep.error_reported_through LIKE 'self%')
--AND ep.mapped_detailed_accountable_team LIKE 'Stra%'
GROUP BY 1,2,3
ORDER BY 1,4 DESC;

-------

WITH dates AS (
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)

SELECT hf_year,
       error_subcategory AS sub_category,
       COUNT(*) complaint_count
FROM materialized_views.cc_errors_processed ep
         JOIN dates d
              ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
WHERE 1=1
  AND ep.country = 'AU'
  AND ep.hellofresh_week_where_error_happened >= '2017-W01'
  AND ep.error_reported_through LIKE 'bulk%'
  AND ep.error_subcategory LIKE '_ingre%'
GROUP BY 1,2
ORDER BY 1,3 DESC;





-----------------------------------------------------------

--calculating ppm (parts per million) i.e. error per week/boxes per week * 1Mil
-- query to get the weekly count of errors per error_type in AU
WITH dates AS ( --using this query to attach quarter and year to the rows
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     box_count AS ( --box count query. Used to get the weekly box count which is then used to calculate the error rate
         SELECT bs.hellofresh_delivery_week,
                bs.box_count
         FROM materialized_views.mbi_anz_boxes_shipped_with_costs bs
         WHERE bs.hellofresh_delivery_week >= '2019-W01'
           AND bs.country = 'AU'
         ORDER BY 1
     )

SELECT d.hf_year,
       d.hf_quarter,
       d.hellofresh_week AS hf_week,
       CASE
           WHEN ep.error_reported_through LIKE 'self%' THEN 'CERT' --ensuring all self reported errors are flagged as CERT
           ELSE ep.error_reported_through END AS error_type,
       COUNT(*) AS complaint_count --counting the errors per error_type (bulk, customer_care, CERT)
FROM materialized_views.cc_errors_processed ep
         JOIN dates d
              ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
WHERE 1=1
  AND ep.country = 'AU'
  AND ep.hellofresh_week_where_error_happened >= '2019-W01'
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;



--query to get the weekly count of errors per category/accountable_team for each error_type separately
WITH dates AS (-- to attach year and quarter to every row
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)


  SELECT d.hf_year,
       d.hf_quarter,
       d.hellofresh_week AS hf_week,
       ep.mapped_accountable_team, --can be changed to ep.error_subcategory for error categories
       CASE
           WHEN ep.error_reported_through LIKE 'bulk%' THEN 'bulk'
           WHEN ep.error_reported_through LIKE 'cust%' THEN 'cc'
           ELSE 'cert' END AS error_type,
         /*CASE
           WHEN ep.ingredient_category = 'PHF' THEN 'PHF'
           WHEN ep.ingredient_category = 'PTN' THEN 'PTN'
           ELSE 'Other' END AS team_cat,*/
       COUNT(*) AS complaint_count
FROM materialized_views.cc_errors_processed ep
         JOIN dates d
              ON d.hellofresh_week = ep.hellofresh_week_where_error_happened
WHERE 1=1
  AND ep.country = 'AU'
  AND ep.hellofresh_week_where_error_happened >= '2019-W01'
  AND ep.error_reported_through LIKE 'self%'  --change this to 'cust%' for cc and 'self%' for CERT
--  AND ep.mapped_accountable_team LIKE  'Proc%'
  --AND ep.mapped_accountable_team IS NOT NULL
GROUP BY 1,2,3,4--,5,6
ORDER BY 1,2,3,4--,5,6;




SELECT ep.ingredient_category,
       COUNT(*)
FROM materialized_views.cc_errors_processed ep
WHERE ep.country = 'AU'
AND ep.hellofresh_week_where_error_happened >= '2021-W01'
AND ep.error_reported_through LIKE 'bulk%'
AND ep.mapped_accountable_team LIKE 'Proc%'
GROUP BY 1
ORDER BY 2 DESC;
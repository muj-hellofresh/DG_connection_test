WITH dates AS (
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),


     fact_tables_count AS (
        SELECT d.hf_month,
               COUNT(*) AS boxes_count
        FROM fact_tables.boxes_shipped AS bs
        JOIN dimensions.product_dimension AS pd
            ON bs.fk_product = pd.sk_product
            AND pd.is_mealbox = TRUE
        JOIN dates AS d
            ON d.hellofresh_week = bs.hellofresh_delivery_week
        WHERE d.hf_month >= '2021-M01'
        AND bs.country = 'AU'
        GROUP BY 1),

-- boxes shipped using mat view table
-- use the output of this to compare with the boxes shipped calculation using fact tables and dimensions

mat_view_count AS (
    SELECT bsc.marketing_month AS hf_month,
           SUM(bsc.box_count) AS box_count_mat_view
    FROM materialized_views.mbi_anz_boxes_shipped_with_costs AS bsc
    WHERE 1=1
    AND bsc.country = 'AU'
    AND bsc.marketing_month >= '2021-M01'
    GROUP BY 1)

SELECT ftc.hf_month,
       (ftc.boxes_count-mvc.box_count_mat_view) AS diff
FROM fact_tables_count AS ftc
JOIN mat_view_count AS mvc
ON ftc.hf_month = mvc.hf_month
ORDER BY 1;

SELECT *
FROM materialized_views.mbi_anz_customer_statuses
WHERE country = 'AU'
AND is_current_status = TRUE;




SELECT bsc.country,
       bsc.hellofresh_delivery_week AS hf_week,
       bsc.box_count AS box_count_mat_view
FROM materialized_views.mbi_anz_boxes_shipped_with_costs AS bsc
WHERE 1=1
  AND bsc.country IN ('AU','NZ','AO')
  AND bsc.hellofresh_delivery_week >= '2021-W01'
ORDER BY 1,2;
--GROUP BY 1
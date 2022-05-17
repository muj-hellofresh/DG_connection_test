-- trying to replicate the steps used in Global's Active Preference Calculation (link to the confluence page given below)
-- https://hellofresh.atlassian.net/wiki/spaces/SCMAX/pages/2808119775/Active+Preference+Calculation

WITH
    dates AS (-- to attach year/quarter/month to every week
        SELECT hellofresh_week,
               CAST(hellofresh_year AS STRING)                                  as hf_year,
               CONCAT(CAST(hellofresh_year AS STRING), '-M',
                      LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
               CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
        FROM dimensions.date_dimension dd
        WHERE dd.year >= 2018
        GROUP BY 1, 2, 3, 4
    ),

    recipe_pref AS
        ( -- has the recipe preference name against each slot for each week (starting 2020-W35)
            -- recipes which are mapped to multiple prefs have multiple prefs separated by a "-"
            SELECT pds.hellofresh_week,
                   pds.country,
                   pds.recipe_index,
                   group_concat(pds.preference, '-') AS recipe_preference
            FROM uploads.pa_anz_preference_default_slots pds
            WHERE pds.country IN ('AU','NZ')
            GROUP BY pds.country,
                     pds.hellofresh_week,
                     pds.recipe_index
            -- ORDER BY 1,2,3
        ),

     table1 AS (
         SELECT ro.country,
                d.hf_year,
                d.hf_quarter,
                d.hf_month,
                d.hellofresh_week,
                ro.recipe_index,
                ro.fk_subscription,
                rp.recipe_preference,
                ro.subscription_preference_name AS customer_preference,
                ro.quantity
                -- MIN(rd.title) AS recipe_title
         FROM fact_tables.recipes_ordered ro
                  /*INNER JOIN dimensions.recipe_dimension rd
                    ON ro.fk_recipe = rd.sk_recipe*/
                  INNER JOIN dimensions.product_dimension pd
                             ON ro.fk_product = pd.sk_product
                  INNER JOIN dates d -- for date dimension
                             ON d.hellofresh_week = ro.hellofresh_week
                  INNER JOIN recipe_pref rp -- gives us the name(s) of the pref the recipes belong to for each slot
                             ON rp.hellofresh_week = ro.hellofresh_week
                                 AND rp.country = ro.country
                                 AND rp.recipe_index = ro.recipe_index
         WHERE 1 = 1
           AND ro.is_menu_addon = false
           AND ro.hellofresh_week >= '2020-W35'
           AND ro.country IN ('AU', 'NZ')
           AND ro.subscription_preference_name IS NOT NULL
           AND ro.subscription_preference_name NOT IN ('', 'express')
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10--, 11, 12, 13, 14, 15, 16
-- ORDER BY 1, 2, 3, 4, 5, 6, 7, 8,9,10
     )


SELECT country,
       hellofresh_week,
       recipe_index,
       STRING_SPLIT(recipe_preference,'-')
       customer_preference,
       quantity
FROM table1
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8,9,10;




SELECT MAX(ro.hellofresh_week)
FROM fact_tables.recipes_ordered ro
WHERE ro.country = 'AU';
--AND ro.hellofresh_week >= '2022-W09';




WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

 table_1 AS (
    SELECT ro.country,
           ro.hellofresh_week,
           ro.fk_subscription,
           CASE
               WHEN ro.reason = 'user-selection' THEN 1
               ELSE 0 END AS swap
    FROM fact_tables.recipes_ordered ro
    JOIN dates
    WHERE 1 = 1
      AND ro.country = 'AU'
      AND ro.hellofresh_week >= '2021-W01'
    GROUP BY 1, 2, 3, 4
),

  table_2 AS (
      SELECT *,
             LEAD(hellofresh_week) OVER(PARTITION BY fk_subscription ORDER BY hellofresh_week) AS next_order_week,
             LEAD(swap) OVER(PARTITION BY fk_subscription ORDER BY hellofresh_week) AS next_order_swap
      FROM table_1
  )

SELECT hellofresh_week,
       AVG(next_order_swap)
FROM table_2
WHERE swap = 1
GROUP BY 1
ORDER BY 1;
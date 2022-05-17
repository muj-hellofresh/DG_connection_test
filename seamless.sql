-- seamless monthly favourite tables (EP)
-- find seamless customer uptake within customers who took monthly favourite + find a baseline to compare to
--fact_tables.recipes_ordered -- country = 'AO'
--,uploads.mbi_anz_upcharge_slots --  recipe_type = 'monthly special'
--,dimensions.subscription_dimension
--,materialized_views.vas_2_surcharge_seamless

--getting the recipe_type against each recipe - use this to filter out the subscribers ordering the "monthly special" recipes
WITH monthly_special AS (
SELECT ro.hellofresh_week AS hf_week,
       ro.recipe_index,
       ro.fk_subscription,
       sd.subscription_id,
       us.recipe_type
FROM fact_tables.recipes_ordered ro
JOIN uploads.mbi_anz_upcharge_slots us
ON ro.recipe_index = us.recipe_index AND
   ro.hellofresh_week = us.hf_week AND
   ro.country = us.country
JOIN dimensions.subscription_dimension sd
ON sd.sk_subscription = ro.fk_subscription
WHERE 1=1
AND ro.is_menu_addon = FALSE
AND us.recipe_type LIKE 'month%'
AND ro.country = 'AO'
AND ro.hellofresh_week >= '2021-W01'
AND ro.reason LIKE 'user%' -- only filtering the swap customers
),

ms_seamless AS --which customers who took monthly special also take seamless
(SELECT ms.hf_week,
       ms.subscription_id,
       IF(ss.new_sku IS NULL,0, 1) AS seamless
FROM monthly_special ms
LEFT JOIN materialized_views.vas_2_surcharge_seamless ss
ON ms.subscription_id = ss.subscription_id AND ss.country = 'AO'
AND ms.hf_week = ss.week_id
)



SELECT mss.hf_week, -- calculating the total weekly monthly_special customers and the number of monthly_special customers who also took seamless
       COUNT(*) AS tot_month_special,
       SUM(mss.seamless) AS month_special_seamless
FROM ms_seamless mss
GROUP BY 1
ORDER BY 1;

------

WITH sub as ( --getting the subscribers without any monthly fav filter
SELECT DISTINCT ro.hellofresh_week AS hf_week,
--       ro.recipe_index,
       ro.fk_subscription,
       sd.subscription_id
--       us.recipe_type
FROM fact_tables.recipes_ordered ro
         JOIN uploads.mbi_anz_upcharge_slots us
              ON ro.recipe_index = us.recipe_index AND
                 ro.hellofresh_week = us.hf_week AND
                 ro.country = us.country
         JOIN dimensions.subscription_dimension sd
              ON sd.sk_subscription = ro.fk_subscription
WHERE 1=1
  AND ro.is_menu_addon = FALSE
--  AND us.recipe_type LIKE 'month%'  --removing monthly_special filter
  AND ro.country = 'AO'
  AND ro.hellofresh_week >= '2021-W01'
  AND ro.reason LIKE 'user%' -- only filtering the swap customers
),

sub_seamless AS
(SELECT sub.hf_week,  --which customers (overall) take seamless
       sub.subscription_id,
       ss.new_sku
FROM sub
LEFT JOIN materialized_views.vas_2_surcharge_seamless ss
ON sub.subscription_id = ss.subscription_id
AND sub.hf_week = ss.week_id
),

t1 AS
(SELECT sub_s.hf_week, -- adding a binary seamless column
       sub_s.subscription_id,
       IF(sub_s.new_sku IS NULL,0, 1) AS seamless
FROM sub_seamless sub_s)

SELECT t1.hf_week, -- calculating the total weekly customers and the number of customers who also took seamless
       COUNT(*) AS tot_cust,
       SUM(t1.seamless) AS seamless_cust
FROM t1
GROUP BY 1
ORDER BY 1;


-----------------------
--calculating the "seamless revenue" among monthly fav customers vs general AO customer base

--monthly fav customers
WITH monthly_special AS (
    SELECT ro.hellofresh_week AS hf_week,
           ro.recipe_index,
           ro.fk_subscription,
           sd.subscription_id,
           us.recipe_type
    FROM fact_tables.recipes_ordered ro
             JOIN uploads.mbi_anz_upcharge_slots us
                  ON ro.recipe_index = us.recipe_index AND
                     ro.hellofresh_week = us.hf_week AND
                     ro.country = us.country
             JOIN dimensions.subscription_dimension sd
                  ON sd.sk_subscription = ro.fk_subscription
    WHERE 1=1
      AND ro.is_menu_addon = FALSE
      AND us.recipe_type LIKE 'month%'
      AND ro.country = 'AO'
      AND ro.hellofresh_week >= '2021-W01'
      AND ro.reason LIKE 'user%' -- only filtering the swap customers
),

mf_seamless_rev AS
(SELECT ms.hf_week,
       ms.subscription_id,
       ss.revenue_local_currency AS seamless_rev
FROM monthly_special ms
LEFT JOIN materialized_views.vas_2_surcharge_seamless ss
    ON ms.subscription_id = ss.subscription_id AND ss.country = 'AO'
    AND ms.hf_week = ss.week_id
)

SELECT msr.hf_week,
       SUM(msr.seamless_rev)/COUNT(*) AS seamless_avg_rev
FROM mf_seamless_rev msr
GROUP BY 1
ORDER BY 1;



--general AO customer base
WITH sub as (
    SELECT DISTINCT ro.hellofresh_week AS hf_week,
--       ro.recipe_index,
                    ro.fk_subscription,
                    sd.subscription_id
--       us.recipe_type
    FROM fact_tables.recipes_ordered ro
             JOIN uploads.mbi_anz_upcharge_slots us
                  ON ro.recipe_index = us.recipe_index AND
                     ro.hellofresh_week = us.hf_week AND
                     ro.country = us.country
             JOIN dimensions.subscription_dimension sd
                  ON sd.sk_subscription = ro.fk_subscription
    WHERE 1=1
      AND ro.is_menu_addon = FALSE
--  AND us.recipe_type LIKE 'month%'  --removing monthly_special filter
      AND ro.country = 'AO'
      AND ro.hellofresh_week >= '2021-W01'
      AND ro.reason LIKE 'user%' -- only filtering the swap customers
),

     overall_seamless_rev AS
         (SELECT sub.hf_week,
                 sub.subscription_id,
                 ss.revenue_local_currency AS seamless_rev
          FROM sub
                   LEFT JOIN materialized_views.vas_2_surcharge_seamless ss
                             ON sub.subscription_id = ss.subscription_id AND ss.country = 'AO'
                                 AND sub.hf_week = ss.week_id
         )

SELECT osr.hf_week,
       SUM(osr.seamless_rev)/COUNT(*) AS seamless_avg_rev
FROM overall_seamless_rev osr
GROUP BY 1
ORDER BY 1;


---------------

-- calculating the reorder rate for monthly special

WITH dates as (
         SELECT hellofresh_week,
                CAST(hellofresh_year AS STRING)                                  as hf_year,
                CONCAT(CAST(hellofresh_year AS STRING), '-M',
                       LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
                CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
         FROM dimensions.date_dimension dd
         WHERE dd.year >= 2018
         GROUP BY 1, 2, 3, 4
     ),

 t1 AS (
SELECT d.hf_month,
       ro.hellofresh_week AS hf_week,
       --ro.recipe_index,
       --ro.fk_recipe,
       ro.fk_subscription,
       sd.subscription_id,
       us.recipe_type,
       CASE
           WHEN us.surcharge_2 > 0 THEN 'surcharge'
           WHEN us.recipe_type LIKE 'month%' THEN 'monthly fav'
           ELSE 'core' END AS prod_type,
       COUNT(*)
FROM fact_tables.recipes_ordered ro
         JOIN uploads.mbi_anz_upcharge_slots us
              ON ro.recipe_index = us.recipe_index AND
                 ro.hellofresh_week = us.hf_week AND
                 ro.country = us.country
         JOIN dimensions.subscription_dimension sd
              ON sd.sk_subscription = ro.fk_subscription
         JOIN dates d
             ON ro.hellofresh_week = d.hellofresh_week
WHERE 1=1
  AND ro.is_menu_addon = FALSE
--  AND us.recipe_type LIKE 'month%'
  AND ro.recipe_index < 900 -- Filters out modularity
  AND ro.country = 'AU'
  AND ro.hellofresh_week >= '2021-W32' --starting the week from when the monthly fav was launched
GROUP BY 1,2,3,4,5,6
),

  t2 AS (
SELECT t1.hf_month,
       t1.recipe_type,
       t1.subscription_id,
       COUNT(DISTINCT t1.hf_week) week_count, -- need to revert to this to correct it - need to find the total number of weeks per month
       COUNT (*) order_freq
FROM t1
GROUP BY 1,2,3
)


SELECT t2.hf_month,
       t2.recipe_type,
       --MAX(t2.week_count) PARTITION BY (hf.month), -- need to revert to this to correct it
       SUM(t2.order_freq)/COUNT(*) AS re_order_rate
FROM t2
GROUP BY 1,2
ORDER BY 1,2;


WITH dates as (
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4
)

SELECT d.hf_month,
       COUNT(DISTINCT d.hellofresh_week)
FROM dates d
WHERE d.hellofresh_week BETWEEN '2021-W32' AND '2021-W50'
GROUP BY 1
ORDER BY 1;
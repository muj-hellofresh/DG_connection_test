-- Details of the task


-- get the *Control Group*
-- Control A: customers who did not order any veggie recipes
-- Control B: Customers on "veggie preference"

-- We cannot have the following control groups as there is no way to identify/differentiate the "additional" (6th) veggie slot
-- Control B: Veggie Customers NOT taking 6th slot
-- Control C: Veggie Customers taking 6th slot



-- get the following *Metrics*
-- AOV
-- AOR
-- Pause Rate
-- Cancel Rate
-- Seamless Uptake
-- Overall Veggie Box Uptake?


WITH aor_calculation as (
    SELECT cl.country,
           cl.customer_id,
           cl.hf_week,
           cl.last_hf_week,
           cl.box_count,
           cl.boxes_shipped,
           SUM(cl.boxes_shipped) OVER (PARTITION BY cl.country, cl.customer_id ORDER BY hf_week ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS five_wk_aor,
            SUM(cl.boxes_shipped) OVER (PARTITION BY cl.country, cl.customer_id ORDER BY hf_week ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING) AS ten_wk_aor
    FROM materialized_views.mbi_anz_customer_loyalty_weekly_extended AS cl
    WHERE 1 = 1
      AND cl.country IN ('AU', 'NZ', 'AO')
      AND cl.hf_week >= '2020-W01'
      AND cl.boxes_shipped IN (0,1) -- only taking customers who have ordered 1 or no box -- excl customers ordering more than 1 box per week
    --ORDER BY cl.hf_week
),

     rev AS (-- calculation of AOV (Avg Order Value) - Rev/number of boxes
         SELECT bs.country,
                bs.hellofresh_delivery_week                  hf_week,
                sd.customer_id                               customer_id,
                SUM(bs.box_shipped)                          box_count, -- will be used to aggregate the total box_count over a time period
                SUM(bs.full_retail_price_local_currency / (1 + bs.vat_percent) +
                    bs.shipping_fee_excl_vat_local_currency) rev
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND bs.hellofresh_delivery_week >= '2020-W01'
           AND bs.country IN ('AU', 'NZ', 'AO')
           AND pd.is_mealbox = TRUE
         GROUP BY 1, 2, 3
         --ORDER BY 1
     ),

     recipe_preference AS ( -- using this CTE instead of uploads.pa_anz_preference_default_slots to get the recipe pref per slot
         select ro.country
              , ro.hellofresh_week
              , ro.subscription_preference_name AS preference
              , ro.recipe_index
         from fact_tables.recipes_ordered ro
         where 1 = 1
           and ro.country in ('AU', 'NZ', 'AO')
           and ro.hellofresh_week >= '2020-W01'
           and ro.is_menu_addon is false
           and ro.reason = 'preference'
           and ro.subscription_preference_name NOT IN ('')
         group by 1, 2, 3, 4
     )


SELECT hellofresh_week,
       COUNT(recipe_index)
FROM recipe_preference
WHERE country = 'NZ'
AND preference = 'veggie'
AND hellofresh_week >= '2021-W01'
GROUP BY 1
ORDER BY 1;
ORDER BY 1,2,4;


-- left join the recipe ordered table with the upcharge slots to get the recipe types (filtered on veggie only)
-- not joining with the recipe_preference CTE as that CTE is made up of actual recipes ordered, not of recipes available
-- sum/count the recipe type (veggie)
-- customers having 0 veggie recipes = Control A
-- veggie preference (user) customers = Control B


-- use the customer_id for A & B to get the
-- weekly avg AOV (using the rev CTE)
-- avg AOR (using the AOR CTE)
-- cancel and pause rates for these two cohorts
-- in the sub_status table join on fk_sub instead of cust_id

WITH veggie_count AS (
    SELECT ro.hellofresh_week,
           sd.customer_id,
           ro.subscription_preference_name,
           COUNT(us.recipe_type) AS veggie_count
    FROM fact_tables.recipes_ordered ro
             LEFT JOIN uploads.mbi_anz_upcharge_slots us
                       ON ro.country = us.country
                           AND ro.hellofresh_week = us.hf_week
                           AND ro.recipe_index = us.recipe_index
                           AND us.recipe_type = 'veggie'
             INNER JOIN dimensions.subscription_dimension sd
                        ON ro.fk_subscription = sd.sk_subscription
    WHERE 1 = 1
      AND ro.country = 'NZ'
      AND ro.hellofresh_week >= '2018-W37' -- the veggie slot count info for NZ starts here
      AND ro.is_menu_addon = FALSE
    GROUP BY 1, 2, 3
),

customer_group AS (
    SELECT vc.hellofresh_week,
           vc.customer_id,
           CASE
               WHEN vc.subscription_preference_name = 'veggie' THEN 'veggie'
               WHEN vc.veggie_count = 0 THEN 'control'
               ELSE 'NA'
               END AS customer_group
    FROM veggie_count vc
),

     rev AS (-- calculation of AOV (Avg Order Value) - Rev/number of boxes
         SELECT bs.country,
                bs.hellofresh_delivery_week                  hf_week,
                sd.customer_id                               customer_id,
                SUM(bs.box_shipped)                          box_count, -- will be used to aggregate the total box_count over a time period
                SUM(bs.full_retail_price_local_currency / (1 + bs.vat_percent) +
                    bs.shipping_fee_excl_vat_local_currency) rev
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND bs.hellofresh_delivery_week >= '2018-W37' -- the veggie slot count info for NZ starts here
           AND bs.country = 'NZ'
           AND pd.is_mealbox = TRUE
         GROUP BY 1, 2, 3
         --ORDER BY 1
     )

SELECT cg.hellofresh_week,
       cg.customer_group,
       SUM(r.rev)/COUNT(r.rev) AS AOV
FROM customer_group cg
JOIN rev r
ON cg.hellofresh_week = r.hf_week
AND cg.customer_id = r.customer_id
WHERE cg.customer_group NOT IN ('NA')
GROUP BY 1,2
ORDER BY 1,2;



-- getting the total number of veggie slots active per week (including plant based slots as well)
SELECT us.hf_week,
       SUM(IF(us.recipe_type = 'veggie', 1, 0)) AS slot_veg,
       SUM(IF(us.recipe_type = 'plant based', 1, 0)) AS slot_pb
FROM uploads.mbi_anz_upcharge_slots us
WHERE 1=1
AND us.country = 'NZ'
AND us.box_type = 'mealboxes' -- there are 'snacks' as well
GROUP BY 1
ORDER BY 1;




-- Getting the AOV delta
-- Getting the 5wk AOR

-- grouping customers by % of veggie recipes ordered per box
WITH veg_qty_percent AS (
    SELECT ro.hellofresh_week,
           sd.customer_id,
           ro.subscription_preference_name,
           SUM(IF(us.recipe_type IS NOT NULL, ro.quantity,0)) AS veg_qty,
           SUM(IF(us.recipe_type IS NOT NULL, ro.quantity,0))/SUM(ro.quantity) AS veg_percent
    FROM fact_tables.recipes_ordered ro
             LEFT JOIN uploads.mbi_anz_upcharge_slots us
                       ON ro.country = us.country
                           AND ro.hellofresh_week = us.hf_week
                           AND ro.recipe_index = us.recipe_index
                           AND us.recipe_type IN ('veggie', 'plant based')
             INNER JOIN dimensions.subscription_dimension sd
                        ON ro.fk_subscription = sd.sk_subscription
    WHERE 1 = 1
      AND ro.country = 'NZ'
      AND ro.hellofresh_week >= '2018-W37'
      AND ro.is_menu_addon = FALSE
    GROUP BY 1,2,3
),

     customer_group AS (
         SELECT vqp.hellofresh_week,
                vqp.customer_id,
                CASE
                    WHEN vqp.veg_percent = 0 THEN 'control'
                    WHEN vqp.hellofresh_week < '2020-W35' AND vqp.veg_percent >= 0.5 THEN 'veggie'
                    WHEN vqp.hellofresh_week >= '2020-W35' AND vqp.subscription_preference_name = 'veggie' THEN 'veggie'
                    ELSE 'NA'
                    END AS customer_group
         FROM veg_qty_percent vqp
     ),

customer_group_old AS ( --can be removed from the query
    SELECT vp.hellofresh_week,
           vp.customer_id,
           CASE
               WHEN vp.veg_percent = 0 THEN 'control'
               WHEN vp.veg_percent >= 0.5 THEN 'veggie'
               ELSE 'NA'
               END AS customer_group
    FROM veg_qty_percent vp
),

     rev AS (-- calculation of AOV (Avg Order Value) - Rev/number of boxes
         SELECT bs.country,
                bs.hellofresh_delivery_week                  hf_week,
                sd.customer_id                               customer_id,
                SUM(bs.box_shipped)                          box_count, -- will be used to aggregate the total box_count over a time period
                SUM(bs.full_retail_price_local_currency / (1 + bs.vat_percent) +
                    bs.shipping_fee_excl_vat_local_currency) rev
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND bs.hellofresh_delivery_week >= '2018-W37' -- the veggie slot count info for NZ starts here
           AND bs.country = 'NZ'
           AND pd.is_menu_addon = FALSE
           -- AND pd.is_mealbox = TRUE
         GROUP BY 1, 2, 3
         --ORDER BY 1
     ),


     aor_calculation as (-- calculation of 5wk & 10wk AOR
         SELECT cl.country,
                cl.customer_id,
                cl.hf_week,
                cl.last_hf_week,
                cl.box_count,
                cl.boxes_shipped,
                SUM(cl.boxes_shipped) OVER (PARTITION BY cl.country, cl.customer_id ORDER BY hf_week ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS five_wk_aor,
                SUM(cl.boxes_shipped) OVER (PARTITION BY cl.country, cl.customer_id ORDER BY hf_week ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING) AS ten_wk_aor
         FROM materialized_views.mbi_anz_customer_loyalty_weekly_extended AS cl
         WHERE 1 = 1
           AND cl.country = 'NZ'
           AND cl.hf_week >= '2018-W37'
           AND cl.boxes_shipped IN (0, 1) -- only taking customers who have ordered 1 or no box -- excl customers ordering more than 1 box per week
     ),

     cancel_pause AS ( -- used to calculate the cancel/pause rate (sum the cancel/pause vol and divide my total active customer base in the week)
         SELECT ss.country,
                ss.previous_week as hf_week,
                sd.customer_id,
                CASE WHEN ss.status = 'user_paused' THEN 1 END AS user_pause,
                CASE WHEN ss.status = 'interval_paused' THEN 1 END AS interval_pause,
                CASE WHEN ss.status = 'canceled' THEN 1 END AS cancel
         FROM fact_tables.subscription_statuses ss
                  JOIN dimensions.subscription_dimension sd
                       ON ss.fk_subscription = sd.sk_subscription
         WHERE 1=1
           AND ss.country IN ('NZ')
           AND ss.hellofresh_week >= '2018-W37'
           AND ss.status <> 'active'
           AND ss.previous_status = 'active'
     ),

     cogs AS ( -- recipe cogs  (total per week) -- add box size to this to have cogs per box size (2P 4P etc)
         SELECT country,
                hf_week,
                recipe_slot,
                recipe_size     AS box_size,
                SUM(total_cost) AS weekly_cogs
         FROM uploads.pa_anz__recipe
         WHERE country IN ('NZ')
           AND hf_week >= '2018-W37'
         GROUP BY 1, 2, 3, 4
     ),



     recipe_count AS ( --total recipes per week - to be used to calculate unit cogs of the recipes
         SELECT ro.country,
                ro.hellofresh_week,
                ro.recipe_index,
                pd.box_size,
                SUM(ro.quantity) AS weekly_count
         FROM fact_tables.recipes_ordered ro
                  JOIN dimensions.product_dimension pd
                       ON pd.sk_product = ro.fk_product
                           AND pd.country = ro.country
         WHERE ro.country IN ('NZ')
           AND ro.hellofresh_week >= '2018-W37'
         GROUP BY 1, 2, 3,4
     ),

     unit_cogs AS ( -- unit cogs of the recipe per week
         SELECT rc.country,
                rc.hellofresh_week              AS hf_week,
                rc.recipe_index,
                rc.box_size,
                c.weekly_cogs / rc.weekly_count AS unit_cogs
         FROM recipe_count rc
                  JOIN cogs c
                       ON rc.hellofresh_week = c.hf_week
                           AND rc.recipe_index = c.recipe_slot
                           AND rc.country = c.country
                           AND CAST(rc.box_size AS INT) = c.box_size
     ),

     customer_cogs AS ( -- total recipe cogs per customer per week
         SELECT ro.country,
                ro.hellofresh_week,
                sd.customer_id,
                SUM(uc.unit_cogs * ro.quantity)               AS cogs
         FROM fact_tables.recipes_ordered ro
                  JOIN dimensions.product_dimension pd
                       ON ro.fk_product = pd.sk_product
                  JOIN dimensions.subscription_dimension sd
                       ON ro.fk_subscription = sd.sk_subscription
                  JOIN unit_cogs uc
                       ON uc.country = ro.country
                           AND uc.hf_week = ro.hellofresh_week
                           AND uc.recipe_index = ro.recipe_index
                           AND uc.box_size = pd.box_size
         WHERE ro.country IN ('NZ')
           AND ro.is_menu_addon = FALSE
           AND ro.hellofresh_week >= '2018-W37'
         GROUP BY 1, 2, 3
     )

SELECT cg.hellofresh_week,
       cg.customer_group,
       AVG(vqp.veg_qty) AS avg_veg_qty,
       AVG(vqp.veg_percent) AS avg_veg_percent,
       AVG(ac.five_wk_aor) AS avg_AOR_5wk,
       AVG(r.rev) AS AOV,
       AVG(cc.cogs/r.rev) AS cogs_percent,
       SUM(cp.user_pause)/COUNT(cg.customer_id) AS pause_rate,
       SUM(cp.cancel)/COUNT(cg.customer_id) AS cancel_rate
FROM customer_group cg
         INNER JOIN rev r
                    ON cg.hellofresh_week = r.hf_week
                        AND cg.customer_id = r.customer_id
         INNER JOIN aor_calculation ac
                    ON cg.hellofresh_week = ac.hf_week
                        AND cg.customer_id = ac.customer_id
         INNER JOIN customer_cogs cc
                    ON cc.hellofresh_week = cg.hellofresh_week
                    AND cc.customer_id = cg.customer_id
         INNER JOIN veg_qty_percent vqp
                    ON vqp.hellofresh_week = cg.hellofresh_week
                    AND vqp.customer_id = cg.customer_id
         LEFT JOIN cancel_pause cp
                   ON cp.hf_week = cg.hellofresh_week
                       AND cp.customer_id = cg.customer_id

WHERE cg.customer_group NOT IN ('NA')
GROUP BY 1,2
ORDER BY 1,2


;

--- resegmentation

WITH veg_qty_percent AS (
    SELECT ro.hellofresh_week,
           sd.customer_id,
           ro.subscription_preference_name,
           SUM(IF(us.recipe_type IS NOT NULL, ro.quantity,0)) AS veg_qty,
           SUM(IF(us.recipe_type IS NOT NULL, ro.quantity,0))/SUM(ro.quantity) AS veg_percent
    FROM fact_tables.recipes_ordered ro
             LEFT JOIN uploads.mbi_anz_upcharge_slots us
                       ON ro.country = us.country
                           AND ro.hellofresh_week = us.hf_week
                           AND ro.recipe_index = us.recipe_index
                           AND us.recipe_type IN ('veggie', 'plant based')
             INNER JOIN dimensions.subscription_dimension sd
                        ON ro.fk_subscription = sd.sk_subscription
    WHERE 1 = 1
      AND ro.country = 'NZ'
      AND ro.hellofresh_week >= '2018-W37'
      AND ro.is_menu_addon = FALSE
    GROUP BY 1,2,3
),

     customer_group AS (
         SELECT vp.hellofresh_week,
                vp.customer_id,
                CASE
                    WHEN vp.veg_percent = 0 THEN 'control'
                    WHEN vp.hellofresh_week < '2020-W33' AND vp.veg_percent >= 0.5 THEN 'veggie'
                    WHEN vp.hellofresh_week >= '2020-W33' AND vp.subscription_preference_name = 'veggie' THEN 'veggie'
                    ELSE 'NA'
                    END AS customer_group
         FROM veg_qty_percent vp
     )

SELECT *
FROM customer_group
WHERE hellofresh_week BETWEEN '2020-W32' AND '2020-W34'
ORDER BY 1,2,3;





SELECT
-- calculating avg % veggie among veggie customers over the weeks
       vp.hellofresh_week,
       AVG(vp.percent_veg) AS avg_veg_percent
FROM veg_percent vp
INNER JOIN customer_group cg
  ON cg.customer_id = vp.customer_id
  AND cg.hellofresh_week = vp.hellofresh_week
WHERE cg.customer_group = 'veggie'   -- only taking "veggie" category customers
GROUP BY 1
ORDER BY 1
;



SELECT cg.hellofresh_week,
       cg.customer_group,
       SUM(ac.five_wk_aor)/COUNT(ac.five_wk_aor) AS avg_AOR_5wk,
       SUM(r.rev)/COUNT(r.rev) AS AOV,
       SUM(cp.user_pause)/COUNT(cg.customer_id) AS pause_rate,
       SUM(cp.cancel)/COUNT(cg.customer_id) AS cancel_rate
FROM customer_group cg
INNER JOIN rev r
  ON cg.hellofresh_week = r.hf_week
  AND cg.customer_id = r.customer_id
INNER JOIN aor_calculation ac
  ON cg.hellofresh_week = ac.hf_week
  AND cg.customer_id = ac.customer_id
LEFT JOIN cancel_pause cp
  ON cp.hf_week = cg.hellofresh_week
  AND cp.customer_id = cg.customer_id
WHERE cg.customer_group NOT IN ('NA')
GROUP BY 1,2
ORDER BY 1,2
;


-- cancel and pause rates for these two cohorts
-- in the sub_status table join on fk_sub instead of cust_id

WITH cancel_pause AS ( -- used to calculate the cancel/pause rate
         SELECT ss.country,
                ss.previous_week as hf_week,
                sd.customer_id,
                CASE WHEN ss.status = 'user_paused' THEN 1 END AS user_pause,
                CASE WHEN ss.status = 'interval_paused' THEN 1 END AS interval_pause,
                CASE WHEN ss.status = 'canceled' THEN 1 END AS cancel
         FROM fact_tables.subscription_statuses ss
                  JOIN dimensions.subscription_dimension sd
                       ON ss.fk_subscription = sd.sk_subscription
                  JOIN dimensions.product_dimension pd -- can remove this as meal_box = True not required
                       ON ss.fk_product = pd.sk_product
         WHERE 1=1
           AND ss.country IN ('AU', 'NZ', 'AO')
           AND ss.hellofresh_week >= '2020-W01'
           AND ss.status <> 'active'
           AND ss.previous_status = 'active'
           AND pd.is_mealbox = TRUE -- can remove this as its one row per customer
)

SELECT *
FROM cancel_pause
ORDER BY 1,2,3;





--- Cancel Rate (using cancel reasons)

-- Find the Top (5) cancellation reasons per customer preferences
-- tables of interest:
-- fact_tables.cancellation_survey

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     cancellation_reason AS ( -- getting all the cancellation reasons in 2021
         SELECT cs.country,
                dd.hellofresh_week,
                sd.customer_id,
                CASE
                    WHEN cs.answer IN ('too-expensive', 'It is out of my budget')
                        THEN 'Budget'
                    WHEN cs.answer IN ('trial', 'I had a trial')
                        THEN 'Trial'
                    WHEN cs.answer IN ('delivery-weekly', 'I do not want a delivery every week')
                        THEN 'Delivery Frequency'
                    WHEN cs.answer LIKE 'recipe-variety' OR cs.answer LIKE 'Recipes%'
                        THEN 'Recipes'
                    WHEN cs.answer IN ('quantity-packaging', 'There is too much packaging')
                        THEN 'Packaging Quantity'
                    WHEN cs.answer IN ('delivery-content-issues', 'I had issues with my deliveries or contents',
                                       'ingredients-not-separated')
                        THEN 'Delivery/Content Issues'
                    WHEN cs.answer IN ('difficult-manage-account', 'It is too difficult to manage my account')
                        THEN 'User Experience (Account)'
                    WHEN cs.answer IN
                         ('failed payment', 'CC-Failed-Payments', 'I had issues with my payment or offers', 'payment')
                        THEN 'Payment'
                    WHEN cs.answer IN ('box-size-meals', 'The size of the box or number of meals does not work for me')
                        THEN 'Meal Size'
                    WHEN cs.answer IN ('I am using other meal kit providers', 'other-providers')
                        THEN 'Switch to Competitor'
                    WHEN cs.answer IN ('It is time consuming')
                        THEN 'Time Consuming'
                    ELSE cs.answer
                    END AS cancellation_reason
         FROM fact_tables.cancellation_survey cs
                  JOIN dimensions.date_dimension dd
                       ON cs.fk_submitted_date = dd.sk_date
             -- AND dd.hellofresh_week BETWEEN '2021-W00' AND '2022-W00' -- need to update this in Robot to allow for automation
                  JOIN dimensions.subscription_dimension sd
                       ON cs.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND cs.country IN ('NZ')
           AND cs.answer_type NOT IN ('bulk', 'textarea')
         GROUP BY 1, 2, 3, 4
     ),

     cust_pref AS ( -- getting the customer preferences
         SELECT ro.country,
                ro.hellofresh_week,
                sd.customer_id,
                ro.subscription_preference_name
         FROM fact_tables.recipes_ordered ro
                  JOIN dimensions.subscription_dimension sd
                       ON ro.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND ro.country IN ('NZ')
           AND ro.hellofresh_week >= '2018-W37'
           AND ro.is_menu_addon = FALSE
           AND ro.subscription_preference_name NOT IN ('', 'express')
         GROUP BY 1, 2, 3, 4
     ),

     cancellation_count AS ( -- counting the cancellation reason freq per customer preference
         SELECT cr.country,
                d.hf_year,
                d.hf_quarter,
                --cr.hellofresh_month,
                cr.hellofresh_week,
                --cr.customer_id,
                cp.subscription_preference_name AS cust_subscription,
                cr.cancellation_reason,
                COUNT(*)                        AS freq
         FROM cancellation_reason cr
                  JOIN cust_pref cp
                       ON cr.country = cp.country
                           AND cr.hellofresh_week = cp.hellofresh_week
                           AND cr.customer_id = cp.customer_id
                  JOIN dates d
                       ON cr.hellofresh_week = d.hellofresh_week
         GROUP BY 1, 2, 3, 4,5,6--,7
     )

SELECT *
FROM cancellation_count
ORDER BY 1,2,3,4,5;



-- pause reasons query shared by Nick

WITH pauses as (select
                    country
                     , split_part(event_action,'|',3) as reason
                     , split_part(event_label, '|', 4) as paused_week
                     --, split_part(event_label, '|', 5) as subscription_id
                     ,uid
                from BIG_QUERY.pa_events
                where 1 = 1
                  and event_action like '%pauseReasons|select|%'
                  and not (split_part(event_action,'|',3) like '%-%' or split_part(event_action,'|',3) like '%.%')
                  and country in ('NZ')     -- to get rid of false country values
                  and split_part(event_label, '|', 4) >=  '2018-W37'
                  -- to look for data of a specific week, week in which the user triggered the pause action, and NOT the week paused
                group by 1, 2, 3, 4)
SELECT
    paused_week
     ,reason
     ,count(distinct uid) as customers
FROM pauses
GROUP BY 1,2
order by 1,2,3 desc

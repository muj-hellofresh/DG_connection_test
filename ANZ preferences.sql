-- test change for git test



-- tag customers with a preference recipe_ordered

-- tables of interest
-- recipe ordered -> subscription preference name & reason (default vs choice) need to remember to remove add-ons
-- boxes_shipped -> to get the actual boxes volumes
-- subscription statuses
-- for purchasing patterns -> upload.mbi_anz_upcharge_slots and uploads.pa_anz_preference_default_slots
-- for retention behavior -> fact_tables.subscription_statuses
-- for for stickiness use pa_anz_preference_default_slots (for preference defaults) go to recipes ordered, exclude addons, left  join to the default slots table on country/week/preference/slot

-- use materialized_views.mbi_anz_customer_loyalty_weekly_extended to calculate AOR
-- calculating AOR (Avg Order Rate) for a set number of week e.g. 5wk

-- the starting date of the analysis will be 2020-W35 as there wasn't any preference allocation prior that

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     aor_calculation as (
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
     ),


     stick_raw AS ( -- this will basically serve as the base table and all the other tables will add columns to this
         SELECT ro.country,
                ro.hellofresh_week,
--ro.fk_subscription,
                sd.customer_id,
                pd.box_size,
                ro.subscription_preference_name AS preference,
                SUM(ro.quantity) AS tot_recipe_count,  -- counting total number of recipes ordered by the customer
                SUM(IF(rp.preference IS NULL, 0, ro.quantity)) AS stick_count -- counting the number of recipes ordered which belonged to default cust_pref

         FROM fact_tables.recipes_ordered ro
/*LEFT JOIN uploads.pa_anz_preference_default_slots ds
ON ds.country = ro.country
AND ds.hellofresh_week = ro.hellofresh_week
AND ds.recipe_index = ro.recipe_index
AND ds.preference = ro.subscription_preference_name*/
                  LEFT JOIN recipe_preference rp
                            ON rp.country = ro.country
                                AND rp.hellofresh_week = ro.hellofresh_week
                                AND rp.recipe_index = ro.recipe_index
                                AND rp.preference = ro.subscription_preference_name
                  LEFT JOIN dimensions.subscription_dimension sd
                            ON sd.sk_subscription = ro.fk_subscription
                                AND sd.country = ro.country
                  LEFT JOIN dimensions.product_dimension pd
                            ON pd.sk_product = ro.fk_product
                                AND pd.country = ro.country
         WHERE 1 = 1
           AND ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week >= '2020-W35'
           AND ro.is_menu_addon = FALSE
-- AND ro.subscription_preference_name IS NOT NULL -- including the null subscription preferences as well
         GROUP BY 1,2,3,4,5
         ORDER BY 1,2,3,4,5),


     cogs AS ( -- recipe cogs  (total per week) -- add box size to this to have cogs per box size (2P 4P etc)
         SELECT country,
                hf_week,
                recipe_slot,
                recipe_size     AS box_size,
                SUM(total_cost) AS weekly_cogs
         FROM uploads.pa_anz__recipe
         WHERE country IN ('AU', 'NZ', 'AO')
           AND hf_week >= '2020-W01'
         GROUP BY 1, 2, 3, 4
--ORDER BY 1,2,3,
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
         WHERE ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week >= '2020-W01'
         GROUP BY 1, 2, 3,4
--ORDER BY 1,2,3
     ),

     unit_cogs AS ( -- unit cogs of the recipe per week
         SELECT rc.country,
                rc.hellofresh_week              AS hf_week,
                rc.recipe_index,
                rc.box_size,
                c.weekly_cogs / rc.weekly_count AS unit_cogs
         FROM recipe_count rc
                  LEFT JOIN cogs c -- changed from INNER to LEFT JOIN
                            ON rc.hellofresh_week = c.hf_week
                                AND rc.recipe_index = c.recipe_slot
                                AND rc.country = c.country
                                AND CAST(rc.box_size AS INT) = c.box_size
     ),

     customer_cogs AS ( -- total recipe cogs per customer per week
         SELECT ro.country,
                ro.hellofresh_week,
                CASE
                    WHEN (ro.subscription_preference_name = '' OR RO.subscription_preference_name IS NULL ) AND RO.country = 'AO' THEN 'basic'
                    WHEN (ro.subscription_preference_name = '' OR RO.subscription_preference_name IS NULL ) THEN 'chefschoice'
                    ELSE ro.subscription_preference_name
                    END AS preference,
                sd.customer_id,
--pd.box_size,
--ro.recipe_index,
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
         WHERE ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week >= '2020-W01'
         GROUP BY 1, 2, 3, 4
     ),

     nps_rating AS ( -- used to calculate the nps score
         SELECT nps.country,
                nps.latest_delivery_before_submission_week AS hf_week,
                nps.customer_id,
                CASE
                    WHEN nps.survey_type = 'initial' AND nps.nps_group = 'Promoter' THEN 1
                    WHEN nps.survey_type = 'initial' AND nps.nps_group = 'Neutral' THEN 0
                    WHEN nps.survey_type = 'initial' AND nps.nps_group = 'Detractor' THEN -1 END AS i_nps,
                CASE
                    WHEN nps.survey_type = 'recurring' AND nps.nps_group = 'Promoter' THEN 1
                    WHEN nps.survey_type = 'recurring' AND nps.nps_group = 'Neutral' THEN 0
                    WHEN nps.survey_type = 'recurring' AND nps.nps_group = 'Detractor' THEN -1 END AS r_nps
         FROM materialized_views.isa_nps AS nps
         WHERE nps.country IN ('AU', 'NZ', 'AO')
           AND nps.latest_delivery_before_submission_week >= '2020-W01'
--ORDER BY 1,2,3;
     ),

     cancel_pause AS ( -- used to calculate the cancel/pause rate
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
     ),



     master_table AS (

-- quarter/month/week, cust_id, box_size, pref, tot_recipe ordered, stick_count, box_count
-- joined with the AOR and AOV and COGS subqueries to get AOV and AOR and COGS each customer_id per week

         SELECT sr.country
              , d.hf_year
              , d.hf_quarter
              , d.hf_month
              , d.hellofresh_week AS hf_week
              , sr.customer_id
              , ac.customer_id AS aor_id
              , sr.box_size
              , sr.preference
              , sr.tot_recipe_count
              , sr.stick_count
              , r.box_count
              , r.rev
              , ac.five_wk_aor
              , ac.ten_wk_aor
              , cc.cogs
              , nps.i_nps
              , nps.r_nps
              , cp.cancel
              , cp.user_pause
              , cp.interval_pause

         FROM stick_raw sr
                  JOIN dates d
                       ON d.hellofresh_week = sr.hellofresh_week
                  JOIN rev r
                       ON r.customer_id = sr.customer_id
                           AND r.hf_week = sr.hellofresh_week
                           AND r.country = sr.country
                  LEFT JOIN aor_calculation ac
                            ON ac.customer_id = sr.customer_id
                                AND ac.hf_week = sr.hellofresh_week
                                AND ac.country = sr.country
                  LEFT JOIN customer_cogs cc -- made a change here from Join to Left Join -- need to change back to inner join
                            ON sr.country = cc.country
                                AND sr.hellofresh_week = cc.hellofresh_week
                                AND sr.customer_id = cc.customer_id
                  LEFT JOIN nps_rating nps
                            ON nps.country = sr.country
                                AND nps.hf_week = sr.hellofresh_week
                                AND nps.customer_id = sr.customer_id
                  LEFT JOIN cancel_pause cp
                            ON cp.country = sr.country
                                AND cp.hf_week = sr.hellofresh_week
                                AND cp.customer_id = sr.customer_id
     ),



     table1 AS ( -- this table returns the the SUM of metrics per week. The calculation of actual values can be done in gsheets
         SELECT mt.country,
                mt.hf_year,
                mt.hf_quarter,
    RIGHT(mt.hf_quarter,2)                         AS quarter,
    mt.hf_month,
    RIGHT(mt.hf_month,3)                           AS month_raw,
    mt.hf_week,
    RIGHT(mt.hf_week,3)                            AS week,
    mt.preference,
    SUM(mt.box_count)                              AS pref_box_count,
    COUNT(DISTINCT mt.aor_id)                      AS aor_box_count,
    SUM(CAST(mt.box_size AS INT))                  AS sum_box_size,
    SUM(mt.tot_recipe_count)                       AS sum_recipe_per_box,
    SUM(mt.rev)                                    AS sum_rev,
    SUM(mt.five_wk_aor)                            AS sum_five_wk_order_rate,
    SUM(mt.ten_wk_aor)                             AS sum_ten_wk_order_rate,
    SUM(mt.stick_count)                            AS sum_stick_count,
    SUM(mt.rev - mt.cogs)                          AS sum_PC1_val,
    SUM(mt.i_nps)                                  AS sum_i_nps,
    COUNT(mt.i_nps)                                AS count_i_nps,
    SUM(mt.r_nps)                                  AS sum_r_nps,
    COUNT(mt.r_nps)                                AS count_r_nps,
    SUM(mt.cancel)                                 AS sum_cancel_count,
    SUM(mt.user_pause)                             AS sum_u_pause_count,
    SUM(mt.interval_pause)                         AS sum_i_pause_rate
FROM master_table mt
--WHERE mt.preference <> ''
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ),

    recipe_score AS (
SELECT ro.country,
    d.hf_quarter,
    d.hf_month,
    ro.hellofresh_week,
    ro.subscription_preference_name AS preference,
    COUNT(rrc.rating_value) AS rating_count,
    SUM(rrc.rating_value) AS total_rating_score,
    SUM(IF(rrc.rating_value = 1,1,0)) AS total_ones
FROM fact_tables.recipes_ordered ro
    JOIN dimensions.subscription_dimension sd
ON ro.fk_subscription = sd.sk_subscription
    JOIN dates d
    ON d.hellofresh_week = ro.hellofresh_week
    LEFT JOIN materialized_views.recipe_rating_corrected AS rrc
    ON ro.country = rrc.country
    AND ro.hellofresh_week = rrc.hellofresh_week
    AND ro.recipe_index = rrc.recipe_index
    AND ro.fk_recipe = rrc.fk_recipe
    AND sd.customer_id = rrc.customer_id
WHERE 1=1
  AND ro.country IN  ('AU', 'NZ', 'AO')
  AND ro.hellofresh_week >= '2020-W01'
--AND ro.subscription_preference_name <> ''
  AND ro.is_menu_addon = FALSE
GROUP BY 1,2,3,4,5
--ORDER BY 1,2,3,4,5
    )

SELECT t1.*,
       rs.rating_count,
       rs.total_rating_score,
       rs.total_ones
FROM table1 AS t1
         JOIN recipe_score rs
              ON rs.country = t1.country
                  AND rs.hellofresh_week = t1.hf_week
                  AND rs.preference = t1.preference
ORDER BY 1, 2, 3, 4, 5;
















---------------------------------------------------------------------------------------
-- calculating conversion per preference
-- tables of interest: fact_tables.marketing_template_conversions -- this has the activation date
-- fact_tables.recipes_ordered -- this has the customer preference
-- bob_live_au.subscription_anonymized -- this also has the preference (meals_preset) on which customers convert on
-- id_subscription = subscription_id in subscription dimension

SELECT mtc.country,
       mtc.conversion_type,
       COUNT(*)
FROM fact_tables.marketing_template_conversions mtc
JOIN dimensions.date_dimension dd
ON mtc.fk_date = dd.sk_date
    AND dd.hellofresh_week BETWEEN '2021-W00' AND '2022-W00'
WHERE 1=1
AND mtc.country = 'AU'
GROUP BY 1,2
ORDER BY 1,2


;

SELECT COUNT(*)
FROM bob_live_au.subscription_anonymized
JOIN dimensions.date_dimension dd
ON sd.fk_created_at_date = dd.sk_date
   AND dd.hellofresh_week BETWEEN '2021-W00' AND '2022-W00'




;
WITH activation_week AS (
    SELECT mtc.country,
           dd.hellofresh_week,
           cd.customer_id
    FROM fact_tables.marketing_template_conversions mtc
             JOIN dimensions.date_dimension dd
                  ON mtc.fk_date = dd.sk_date
                      AND dd.hellofresh_week BETWEEN '2020-W00' AND '2021-W00'
             JOIN dimensions.customer_dimension cd
                  ON mtc.fk_customer = cd.sk_customer
    WHERE 1 = 1
      AND mtc.country IN ('AU')
      AND mtc.conversion_type = 'activation'
    GROUP BY 1, 2, 3
--ORDER BY 1,2
),

     pref_name AS (
         SELECT ro.country,
                sd.customer_id,
                ro.subscription_preference_name,
                MIN(ro.hellofresh_week) AS first_hf_week
         FROM fact_tables.recipes_ordered ro
                  JOIN dimensions.subscription_dimension sd
                       ON ro.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND ro.country IN ('AU')
           AND ro.hellofresh_week BETWEEN '2020-W00' AND '2021-W00'
         GROUP BY 1,2,3
     )

SELECT aw.country,
       pn.subscription_preference_name,
       COUNT(*)
FROM activation_week aw
JOIN pref_name pn
ON aw.country = pn.country
AND aw.customer_id = pn.customer_id
WHERE 1=1
AND pn.subscription_preference_name IS NOT NULL
AND pn.subscription_preference_name NOT IN ('')
GROUP BY 1,2
ORDER BY 1,2

;


SELECT aw.country,
       --aw.hellofresh_week,
       pn.subscription_preference_name,
       COUNT(pn.subscription_preference_name) AS weekly_sub_count
FROM activation_week aw
LEFT JOIN pref_name pn
ON aw.country = pn.country
AND aw.customer_id = pn.customer_id
AND aw.hellofresh_week = pn.hellofresh_week
AND pn.subscription_preference_name IS NOT NULL
AND pn.subscription_preference_name NOT IN ('')
GROUP BY 1,2--,3
ORDER BY 1,2--,3

;
-------------------------------------------------------------------------------------------

-- calculating recipe score

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)


SELECT ro.country,
       d.hf_quarter,
       d.hf_month,
       ro.hellofresh_week,
       ro.subscription_preference_name AS preference,
       COUNT(rrc.rating_value) AS rating_count,
       SUM(rrc.rating_value) AS total_rating,
       SUM(IF(rrc.rating_value = 1,1,0)) AS total_ones
FROM fact_tables.recipes_ordered ro
JOIN dimensions.subscription_dimension sd
  ON ro.fk_subscription = sd.sk_subscription
JOIN dates d
  ON d.hellofresh_week = ro.hellofresh_week
LEFT JOIN materialized_views.recipe_rating_corrected AS rrc
  ON ro.country = rrc.country
  AND ro.hellofresh_week = rrc.hellofresh_week
  AND ro.recipe_index = rrc.recipe_index
  AND ro.fk_recipe = rrc.fk_recipe
  AND sd.customer_id = rrc.customer_id
WHERE 1=1
AND ro.country IN  ('AU', 'NZ')
AND ro.hellofresh_week >= '2021-W01'
AND ro.subscription_preference_name <> ''
AND ro.is_menu_addon = FALSE
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;




-------------------------------------------------------------------

-- What’s the NPS for each preference, how did it evolve since pref. launch?
-- How does it compare to overall NPS. Main detractors/promoters

-- join recipes_ordered to default_slots ON country, week, recipe_index (this will give us the pref name of each recipe ordered)
-- Note: a recipe will have multiple preferences, therefore, expect to have multiple rows for each recipe ordered (1 row for each pref the slot/index is mapped to)
-- use fk_subscription to join with subscription_dimension to get the customer_id against each recipe_ordered
-- join nps_cc to these tables ON week, customer_id (gives us the nps_score against each recipe ordered)


WITH dates AS (-- to attach year and quarter to every row
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     nps_score AS (
         SELECT ncc.country,
                ncc.customer_id,
                ncc.last_delivery_hf_week,
                SUM(ncc.score) / COUNT(*) AS score
         FROM materialized_views.isa_nps_comment_categorizations ncc
         GROUP BY 1, 2, 3
     ),

     nps_raw AS (
         SELECT ro.country,
                d.hf_quarter,
                d.hf_month,
                ro.hellofresh_week AS hf_week,
                sd.customer_id,
                ro.fk_recipe,
                ds.preference,
                ns.score           AS nps_score
         FROM fact_tables.recipes_ordered ro
                  JOIN uploads.pa_anz_preference_default_slots ds
                       ON ro.country = ds.country AND
                          ro.hellofresh_week = ds.hellofresh_week AND
                          ro.recipe_index = ds.recipe_index
                  JOIN dimensions.subscription_dimension sd
                       ON ro.fk_subscription = sd.sk_subscription AND
                          ro.country = sd.country
                  LEFT JOIN nps_score ns
                            ON sd.country = ns.country AND
                               sd.customer_id = ns.customer_id AND
                               ro.hellofresh_week = ns.last_delivery_hf_week
                  JOIN dates d
                       ON ro.hellofresh_week = d.hellofresh_week
         WHERE 1 = 1
           AND ro.country = 'AU'
           AND ro.hellofresh_week >= '2021-W01'
--ORDER BY 1,2,3,4,5
     )

SELECT nr.country,
       nr.hf_month,
       nr.preference,
       --nr.nps_score,
       SUM(CASE -- calculating the NPS % using the nps_score
               WHEN nr.nps_score >= 9 THEN 1
               WHEN nr.nps_score >= 7 THEN 0
               ELSE -1 END) / COUNT(*) AS nps_score
FROM nps_raw nr
WHERE nr.nps_score IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;



-------------------------------------------------------------------
-- test area
--cancellation reasons
SELECT cmt.cancellation_reasons,
       COUNT(*)
FROM materialized_views.cancellation_master_table cmt
WHERE cmt.country = 'AU'
  AND cmt.hellofresh_week >= '2021-W01'
  AND cmt.answer_type IN ('radio', 'check_box')
GROUP BY 1
ORDER BY 2 DESC;


-- for nps use materialized_views.nps_scores

SELECT nps.country,
       nps.latest_delivery_before_submission_week AS hf_week,
       nps.customer_id,
       CASE
           WHEN nps.survey_type = 'initial' AND nps.nps_group = 'Promoter' THEN 1
           WHEN nps.survey_type = 'initial' AND nps.nps_group = 'Neutral' THEN 0
           WHEN nps.survey_type = 'initial' AND nps.nps_group = 'Detractor' THEN -1 END AS i_nps,
       CASE
           WHEN nps.survey_type = 'recurring' AND nps.nps_group = 'Promoter' THEN 1
           WHEN nps.survey_type = 'recurring' AND nps.nps_group = 'Neutral' THEN 0
           WHEN nps.survey_type = 'recurring' AND nps.nps_group = 'Detractor' THEN -1 END AS r_nps
FROM materialized_views.isa_nps AS nps
WHERE nps.country IN ('AU', 'NZ')
AND nps.latest_delivery_before_submission_week >= '2021-W01'
ORDER BY 1,2,3;


-- for cancellation/pause rate use fact_tables.subscription_statuses
SELECT ss.country,
       ss.previous_week as hf_week,
       sd.customer_id,
       CASE WHEN ss.status = 'user_paused' THEN 1 END AS user_pause,
       CASE WHEN ss.status = 'interval_paused' THEN 1 END AS interval_pause,
       CASE WHEN ss.status = 'canceled' THEN 1 END AS cancel
FROM fact_tables.subscription_statuses ss
JOIN dimensions.subscription_dimension sd
  ON ss.fk_subscription = sd.sk_subscription
JOIN dimensions.product_dimension pd
  ON ss.fk_product = pd.sk_product
WHERE 1=1
AND ss.country IN ('AU', 'NZ')
AND ss.hellofresh_week >= '2021-W01'
AND ss.status <> 'active'
AND ss.previous_status = 'active'
AND pd.is_mealbox = TRUE
ORDER BY 1,2,3;



-- for PC2 use materialized_views.box_level_profit_contribution
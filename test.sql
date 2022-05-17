-- active customers who havent ever ordered a pork recipe
-- get current active customer list from boxes_shipped -> it has fk_sub and fk_customer
-- inner join to link it to recipe ordered
-- link recipe_ordered to recipe dimension on fk_recipe and filter recipes having "Pork" in their title

-- use the cookbook upload table uploads.gp_octopus_recipe_cookbook for ingredient-wise details on the recipes (main protein etc)


WITH active_cust AS (
    SELECT sd.customer_id
    FROM fact_tables.boxes_shipped bs
    JOIN dimensions.subscription_dimension sd
    ON bs.fk_subscription = sd.sk_subscription
    WHERE bs.country = 'AU'
      AND bs.hellofresh_delivery_week = '2022-W02'
),


pork_subscriptions AS ( -- using the cookbook upload
    SELECT DISTINCT sd.customer_id,
       rc.main_protein
FROM fact_tables.recipes_ordered ro
JOIN dimensions.subscription_dimension sd
ON ro.fk_subscription = sd.sk_subscription
    AND ro.country = sd.country
JOIN uploads.gp_octopus_recipe_cookbook rc -- this has protein name
ON rc.week = ro.hellofresh_week
    AND rc.country = ro.country
    AND rc.index = ro.recipe_index
WHERE 1=1
AND ro.country = 'AU'
--AND ro.hellofresh_week >= '2021-W01'
AND (rc.main_protein LIKE 'Pork' OR
     (rc.main_protein NOT LIKE 'Pork' AND (rc.title LIKE '%Bacon%' OR rc.title LIKE '%Chorizo%' OR  rc.title LIKE '%Proscuitto%' OR rc.title LIKE '%Ham%')) )
    ),


table1 AS (
    SELECT ac.customer_id,
       ps.main_protein
FROM active_cust ac
LEFT JOIN pork_subscriptions ps
ON ac.customer_id = ps.customer_id)

SELECT main_protein,
       COUNT(*)
FROM table1
GROUP BY 1;

-----------------------------------


SELECT bs.country,
       COUNT(*)
FROM fact_tables.boxes_shipped bs
JOIN dimensions.product_dimension pd
     ON bs.fk_product = pd.sk_product
         AND pd.product_sku LIKE '%XMB%'
WHERE 1=1
AND bs.hellofresh_delivery_week >= '2021-W42'
AND bs.country IN ('AU', 'NZ')
GROUP BY 1
ORDER BY 1;



WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)

SELECT d.*
FROM dates d
WHERE d.hellofresh_week >= '2021-W01'
ORDER BY 1;




select
    ro.subscription_preference_name
     ,count(distinct ro.box_id) as customers

from fact_tables.recipes_ordered ro
where ro.country in ('AU')
  and ro.is_menu_addon is false
  and ro.hellofresh_week = '2020-W01'
group by 1
order by 1;




WITH subscription_details AS (
    SELECT dd.hellofresh_year
         , dd.hellofresh_month
         , dd.hellofresh_week
         , s.id_subscription
         , s.meals_preset
    FROM bob_live_au.subscription_anonymized s
             JOIN dimensions.subscription_dimension sd
                  ON s.id_subscription = sd.subscription_id
                      AND sd.country = 'AU'
             JOIN dimensions.date_dimension dd
                  ON sd.fk_created_at_date = dd.sk_date
                      AND dd.hellofresh_week BETWEEN '2020-W35' AND '2022-W00'
    WHERE 1 = 1
      AND s.meals_preset IS NOT NULL
      AND s.meals_preset NOT IN ('')
      GROUP BY 1, 2, 3, 4, 5   -- ensuring we only have one row per subscription_id
)

SELECT sd.hellofresh_year,
       sd.hellofresh_month,
       sd.hellofresh_week,
       sd.meals_preset AS subscription_pref,
       COUNT(*) AS sub_count
FROM subscription_details sd
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;


SELECT ro.subscription_preference_name,
       COUNT(*)
FROM fact_tables.recipes_ordered ro
WHERE ro.hellofresh_week = '2022-W05'
AND ro.country = 'AU'
GROUP BY 1
ORDER BY 1;



SELECT ep.error_reported_through,
       -- ep.error_subcategory,
       ep.mapped_accountable_team,
       --ep.mapped_detailed_accountable_team,
       ep.ingredient_category,
       COUNT(*)
FROM materialized_views.cc_errors_processed ep
WHERE ep.country = 'AU'
AND ep.hellofresh_week_where_error_happened >= '2021-W45'
GROUP BY 1,2,3
ORDER BY 1,2,3;


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
    )

SELECT d.hellofresh_week,
       d.hf_quarter
FROM dates d
WHERE hellofresh_week BETWEEN '2020-W43' AND '2022-W04'
ORDER BY 1;




SELECT cs.fk_subscription,
       cs.answer_type,
       cs.answer,
       ccd.level_1,
       ccd.level_2,
       ccd.level_3,
       ccd.level_4
FROM fact_tables.cancellation_survey cs
LEFT JOIN fact_tables.cancellation_survey_comment_categories cscc
    ON cs.country = cscc.country
    AND cs.fk_subscription = cscc.fk_subscription
LEFT JOIN dimensions.comment_category_dimension ccd
    ON cscc.fk_comment_category = ccd.sk_comment_category
WHERE 1=1
AND cs.country = 'AU'
AND cs.answer_type = 'textarea'
;

WITH dates AS (-- to attach year/quarter/month to every week
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
       d.hf_month,
       d.hellofresh_week
FROM dates d
WHERE d.hellofresh_week BETWEEN '2022-W22' AND '2023-W52'
ORDER BY 1,2,3,4;



SELECT *
FROM uploads.pa_anz_preference_default_slots ds
WHERE ds.hellofresh_week >= '2022-W01'
ORDER BY 1,2,3,4;



WITH recipe_preference AS ( -- using this CTE instead of uploads.pa_anz_preference_default_slots to get the recipe pref per slot
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

SELECT *
FROM recipe_preference
WHERE country = 'AU'
order by 1,2,3,4;


WITH test AS (
    SELECT ro.box_id
    FROM fact_tables.recipes_ordered ro
             JOIN dimensions.product_dimension pd
                  ON ro.fk_product = pd.sk_product
    WHERE pd.product_name LIKE '%fruit%'
      AND ro.hellofresh_week = '2022-W10'
      AND ro.country = 'AU'
),

  dc AS (
      SELECT gol.country,
             gol.box_id,
             CASE
                 WHEN gol.dc = 'Sydney' THEN 'Esky'
                 WHEN gol.dc = 'Melbourne' THEN 'Tuckerbox'
                 WHEN gol.dc = 'Perth' THEN 'Casa'
                 WHEN gol.dc IN ('Chilli Bin', 'NZ') THEN 'Chilli Bin'
                 END AS dc
      FROM views_analysts.gor_odl_landing gol
               INNER JOIN test
                          ON gol.box_id = test.box_id
      WHERE gol.country IN ('AU', 'NZ', 'AO')
        AND gol.hellofresh_week >= '2021-W01'
      GROUP BY 1, 2, 3
  )

SELECT dc,
       COUNT(*)
FROM dc
GROUP BY 1
ORDER BY 2 DESC;






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
                  JOIN cogs c
                       ON rc.hellofresh_week = c.hf_week
                           AND rc.recipe_index = c.recipe_slot
                           AND rc.country = c.country
                           AND CAST(rc.box_size AS INT) = c.box_size
     ),

     customer_cogs AS ( -- total recipe cogs per customer per week
         SELECT ro.country,
                ro.hellofresh_week,
                ro.subscription_preference_name AS preference,
                sd.customer_id,
                --pd.box_size,
                --ro.recipe_index,
                SUM(uc.unit_cogs)               AS cogs
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
                  JOIN aor_calculation ac
                       ON ac.customer_id = sr.customer_id
                           AND ac.hf_week = sr.hellofresh_week
                           AND ac.country = sr.country
                  JOIN customer_cogs cc
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
         WHERE mt.preference <> ''
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
           AND ro.subscription_preference_name <> ''
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


with cust_loc AS (
    SELECT BS.hellofresh_delivery_week,
           CD.customer_id,
           BS.box_id,
           BS.country,
           DR.region_handle,
           DR.postal_code,
           GD.zipcode,
           GD.latitude  AS cus_lat,
           GD.longitude AS cus_lon
    FROM fact_tables.boxes_shipped BS
             INNER JOIN dimensions.product_dimension PD
                        ON BS.fk_product = PD.sk_product
             INNER JOIN dimensions.customer_dimension CD
                        ON CD.sk_customer = BS.fk_customer
             LEFT JOIN dimensions.geo_dimension GD
                       ON GD.sk_geo = BS.fk_geo
             LEFT JOIN (
        WITH bc_depot AS (
            SELECT PC.postal_code,
                   PC.delivery_region_id,
                   DR.region_code AS country,
                   DR.region_handle,
                   PC.published_at_ms,
                   ROW_NUMBER()
                                     OVER (PARTITION BY PC.postal_code,DR.region_code ORDER BY PC.published_at_ms DESC) AS rownb
            FROM logistics_configurator.delivery_region_postal_code_latest PC
                     LEFT JOIN logistics_configurator.delivery_region_latest DR
                               ON DR.id = PC.delivery_region_id
            WHERE region_code IN ('AU', 'NZ', 'AO')
        )
        SELECT BD.postal_code,
               BD.country,
               BD.region_handle,
               BD.delivery_region_id,
               BD.published_at_ms
        FROM bc_depot BD
        WHERE rownb = 1) DR
                       ON DR.postal_code = GD.zipcode AND DR.country = GD.country
    WHERE BS.country IN ('AU', 'AO', 'NZ')
      AND BS.box_shipped = 1
      -- AND PD.is_mealbox IS TRUE
      AND BS.hellofresh_delivery_week >= '2022-W01'
      AND region_handle != 'gift-cards'
ORDER BY 1, 2, 3, 4, 5
    )



SELECT country,
       hellofresh_delivery_week,
       COUNT (DISTINCT box_id) AS Box_count
FROM cust_loc
GROUP BY 1,2
ORDER BY 1,2;



WITH recipe_weight AS (
    SELECT r.country,
           r.hf_week,
           r.market,     -- e.g. Sydney has diff weight/suppliers than Perth
           r.recipe_slot,
           r.recipe_size                                          AS box_size,
           SUM((r.weight * r.qty_2ppl) + (r.weight * r.qty_4ppl)) AS recipe_weight_grams
    FROM uploads.pa_anz__recipe r
    WHERE 1 = 1
      AND r.country IN ('AU', 'NZ', 'AO')
      AND r.hf_week >= '2019-W01'
    GROUP BY 1, 2, 3, 4, 5
--ORDER BY 1,2,4,5
),

  geo_markets AS (  -- to map the "markets" to the sk_geo (from boxes_shipped)
      SELECT gd.sk_geo,
             CASE
                 WHEN gd.region_1 IN
                      ('New South Wales', 'Australian Capital Territory', 'Northern Territory', 'Queensland')
                     THEN 'Sydney'
                 WHEN gd.region_1 IN ('Victoria', 'South Australia') THEN 'Melbourne'
                 WHEN gd.region_1 IN ('Western Australia') THEN 'Perth'
                 ELSE 'Nz' END AS market
      FROM dimensions.geo_dimension gd -- to get the region1 of the box - link to "market"
      WHERE gd.country IN ('AU', 'NZ', 'AO')
  )

SELECT ro.country,
       ro.hellofresh_week AS hf_week,
       ro.box_id,
       pd.box_size,
       SUM(rw.recipe_weight_grams/1000) AS box_weight_kg
FROM fact_tables.recipes_ordered ro
INNER JOIN dimensions.product_dimension pd  -- to get the box_size
  ON ro.fk_product = pd.sk_product
INNER JOIN fact_tables.boxes_shipped bs     -- to get the geo of the box and link to gd
  ON ro.box_id = bs.box_id
INNER JOIN geo_markets gm                   -- to link to "market"
  ON bs.fk_geo = gm.sk_geo
INNER JOIN recipe_weight rw                 -- to get the weight of the box
  ON rw.country = ro.country
    AND rw.hf_week = ro.hellofresh_week
    AND rw.recipe_slot = ro.recipe_index
    AND rw.box_size = CAST(pd.box_size AS INT)
    AND rw.market = gm.market
WHERE 1=1
AND ro.country IN ('AU', 'NZ', 'AO')
--AND ro.hellofresh_week = '2022-W01'
AND ro.box_id = 'AU11732787'
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;





WITH recipe_weight AS (
    SELECT r.country,
           r.hf_week,
           CASE
               WHEN r.market = 'Sydney' THEN 'Esky'
               WHEN r.market = 'Melbourne' THEN 'Tuckerbox'
               WHEN r.market = 'Perth' THEN 'Casa'
               WHEN r.market = 'Nz' THEN 'Chilli Bin'
               ELSE '' END AS dc,                       -- e.g. Esky has diff weight/suppliers than Casa
           r.recipe_slot,
           SUM(r.weight * r.qty_2ppl * r.supplier_split) AS weight_2p_g,
           SUM(r.weight * r.qty_4ppl* r.supplier_split) AS weight_4p_g
    FROM uploads.pa_anz__recipe r
    WHERE 1 = 1
      AND r.country IN ('AU', 'NZ', 'AO')
      AND r.hf_week >= '2022-W01'
    GROUP BY 1, 2, 3, 4
),

     test_table AS (
         SELECT ro.country,
                ro.hellofresh_week AS hf_week,
                ro.box_id,
                sd.customer_id,
                pd.box_size,
                pd.number_of_recipes,
                CASE
                    WHEN CAST(pd.box_size AS INT) = 2 THEN SUM(rw.weight_2p_g / 1000)
                    WHEN CAST(pd.box_size AS INT) = 4 THEN SUM(rw.weight_4p_g / 1000)
                    WHEN CAST(pd.box_size AS INT) = 6
                        THEN SUM((rw.weight_2p_g + rw.weight_4p_g) / 1000) -- EP usually has 6P boxes
                    END            AS box_weight_kg
         FROM fact_tables.recipes_ordered ro
                  INNER JOIN dimensions.subscription_dimension sd
                             ON sd.sk_subscription = ro.fk_subscription
                                 AND sd.country = ro.country
                  INNER JOIN dimensions.product_dimension pd
                             ON ro.fk_product = pd.sk_product
                  INNER JOIN fact_tables.boxes_shipped bs
                             ON ro.box_id = bs.box_id

                  LEFT JOIN uploads.pa_anz__hfm_scm_mappings addon_index
                            ON addon_index.country =
                               ro.country
                                AND addon_index.product_name = pd.product_name
                                AND addon_index.dwh_index = ro.recipe_index

                  INNER JOIN uploads.pa_anz_customer_dc_au dc
                             ON dc.customer_id = sd.customer_id
                                 AND dc.country = sd.country
                                 AND dc.hellofresh_week = ro.hellofresh_week
                  INNER JOIN recipe_weight rw
                             ON rw.country = ro.country
                                 AND rw.hf_week = ro.hellofresh_week
                                 AND ((rw.recipe_slot =
                                       addon_index.pythia_index
                                     AND ro.is_menu_addon IS TRUE)
                                     OR
                                      (rw.recipe_slot = ro.recipe_index))
                                 AND rw.dc = dc.dc
                                 AND ro.country = dc.country
         WHERE 1 = 1
           AND ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week >= '2022-W01'
           AND (pd.box_size IN ('2','4','6') OR RO.is_menu_addon)
         GROUP BY 1, 2, 3, 4, 5, 6)

SELECT country,
       hf_week,
       customer_id,
       COUNT(*)
FROM test_table tt
WHERE box_id IN ('AU36229294', 'AU36229295', 'AU36205005')
GROUP BY 1,2,3
-- HAVING COUNT(*) > 1
ORDER BY 1,2,4 DESC;




SELECT DISTINCT order_number,
                status
FROM order_payment_history
WHERE 1=1
AND status = 'payment_failed'
AND CAST(time AS date) BETWEEN '20200208' AND '20200212'
;


WITH failed_payments AS (
    SELECT order_number,
           status,
           MIN(time) AS failed_time
    FROM order_payment_history
    WHERE status = 'payment_failed'
    GROUP BY 1, 2
),
     successful_payments AS (
         SELECT order_number,
                status,
                time AS paid_time
         FROM order_payment_history
         WHERE status = 'order_paid'
     )

SELECT fp.order_number,
       CASE
           WHEN sp.paid_time IS NOT NULL THEN CAST(DATEDIFF(day, sp.paid_time, fp.failed_time) AS STRING)
           ELSE 'never_paid'
           END AS 'time_to_pay'
FROM failed_payments fp
LEFT JOIN successful_payments sp
ON fp.order_number = sp.order_number;



WITH order_rows AS (
    SELECT o.customer_id,
           c.name,
           c.phone_number,
           o.order_number,
           ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.delivery_date DESC) AS order_row_num
    FROM orders o
             JOIN customers c
                  ON o.customer_id = c.customer_id
)

SELECT customer_id,
       name,
       phone_number,
       order_number
FROM order_rows
WHERE order_row_num = 1;


SELECT dl.delivery_route,
       COUNT(*)
FROM deliveries.delivery_list dl
GROUP BY 1
ORDER BY 2 DESC;


SELECT r.recipe_size,
       COUNT(*)
FROM uploads.pa_anz__recipe r
WHERE 1=1
AND r.country IN ('NZ')
GROUP BY 1
ORDER BY 1;


WITH      cogs AS ( -- recipe cogs  (total per week) -- add box size to this to have cogs per box size (2P 4P etc)
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
                       JOIN cogs c
                            ON rc.hellofresh_week = c.hf_week
                                AND rc.recipe_index = c.recipe_slot
                                AND rc.country = c.country
                                AND CAST(rc.box_size AS INT) = c.box_size
          ),

          customer_cogs AS ( -- total recipe cogs per customer per week
              SELECT ro.country,
                     ro.hellofresh_week,
                     ro.subscription_preference_name AS preference,
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
          )

SELECT cc.*
FROM cogs cc
ORDER BY 1,2,3,4;









WITH SUBSCRIPTION_CHANGES AS (
    SELECT
    SS.country
    ,SS.hellofresh_week
    ,SS.fk_subscription
    ,SS.previous_week
    ,SS.status_active
    ,SS.status_canceled
    ,SS.status_user_paused + SS.status_interval_paused AS status_paused
    FROM fact_tables.subscription_statuses SS
    WHERE SS.country IN ('AU','NZ','AO')
    AND SS.hellofresh_week >= '2021-W01'
    AND ss.previous_status = 'active'

)

SELECT country,
       hellofresh_week,
       AVG(status_paused)
FROM SUBSCRIPTION_CHANGES
WHERE country = 'AU'
GROUP BY 1,2
ORDER BY 1,2;


SELECT pd.country,
       pd.product_name,
       COUNT(*)
FROM dimensions.product_dimension pd
WHERE pd.country IN ('AU', 'NZ')
AND pd.product_name LIKE '%-christmas-%'
GROUP BY 1,2
ORDER BY 3 DESC;



SELECT bs.country,
       sd.customer_id,
       pd.product_name,
       COUNT(*) AS qty
FROM fact_tables.boxes_shipped bs
JOIN dimensions.subscription_dimension sd
ON bs.fk_subscription = sd.sk_subscription
JOIN dimensions.product_dimension pd
ON bs.fk_product = pd.sk_product
  AND pd.product_name LIKE '%-christmas-%'
WHERE bs.country IN ('AU', 'NZ')
AND bs.hellofresh_delivery_week BETWEEN '2021-W50' AND '2022-W00'
    )
GROUP BY 1,2,3;








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
                  LEFT JOIN cogs c
                            ON rc.hellofresh_week = c.hf_week
                                AND rc.recipe_index = c.recipe_slot
                                AND rc.country = c.country
                                AND CAST(rc.box_size AS INT) = c.box_size
     ),

     customer_cogs AS ( -- total recipe cogs per customer per week
         SELECT ro.country,
                ro.hellofresh_week,
                ro.subscription_preference_name AS preference,
                sd.customer_id,
                --pd.box_size,
                --ro.recipe_index,
                SUM(uc.unit_cogs)               AS cogs
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
                  JOIN aor_calculation ac
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
WHERE mt.preference <> ''
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ),

    recipe_score AS (
SELECT ro.country,
    d.hf_quarter,
    d.hf_month,
    ro.hellofresh_week,
    ro.subscription_preference_name AS preference,
    COUNT (rrc.rating_value) AS rating_count,
    SUM (rrc.rating_value) AS total_rating_score,
    SUM (IF(rrc.rating_value = 1, 1, 0)) AS total_ones
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
  AND ro.country IN ('AU'
    , 'NZ'
    , 'AO')
  AND ro.hellofresh_week >= '2020-W01'
  AND ro.subscription_preference_name <> ''
  AND ro.is_menu_addon = FALSE
GROUP BY 1, 2, 3, 4, 5
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




SELECT ep.country,
       ep.
FROM materialized_views.cc_errors_processed ep
WHERE 1=1
AND hellofresh_week_where_error_happened >= '2022-W01'
AND country = 'AU';



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




SELECT bs.country                    AS country,
        bs.hellofresh_delivery_week   AS week,
       SUM(pd.is_mealbox)            AS total_boxes
FROM fact_tables.boxes_shipped AS bs
         JOIN dimensions.product_dimension AS pd
              ON pd.country = bs.country
                  AND pd.sk_product = bs.fk_product
                  AND pd.is_mealbox = TRUE
WHERE bs.country IN ('AU')
  AND bs.hellofresh_delivery_week >= '2022-W19'
GROUP BY 1 , 2
ORDER BY 1 , 2;
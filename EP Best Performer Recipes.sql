-- calculating the avg recipe score (rating_value) per week for each recipe
WITH recipe_score AS (
    SELECT rrc.hellofresh_week,
           rrc.recipe_index,
           rrc.fk_recipe,
           AVG(rrc.rating_value) AS weekly_avg_rating,
           SUM(rrc.rating_value) AS weekly_total_rating,
           COUNT(rrc.rating_value) AS weekly_count
    FROM materialized_views.recipe_rating_corrected AS rrc
    WHERE rrc.country = 'AO' AND
            rrc.hellofresh_week >= '2021-W01' AND
            rrc.is_surcharge = 0
    GROUP BY 1,2,3),

-- calculating the total weekly COGS of recipes.
--joining recipe score on/with each weekly slot

     recipe_cogs AS (
         SELECT hf_week,
                recipe_slot,
                recipe_name,
                SUM(total_cost) AS weekly_cogs
         FROM uploads.pa_anz__recipe
         WHERE country = 'AO' AND
                 hf_week >= '2021-W01'
         GROUP BY 1,2,3),

     weekly_score_cogs AS (
         SELECT rc.hf_week,
                rc.recipe_slot,
                rs.fk_recipe,
                rc.weekly_cogs,
                rs.weekly_avg_rating,
                rs.weekly_total_rating,
                rs.weekly_count
         FROM recipe_cogs as rc
                  JOIN recipe_score AS rs
                       ON rs.hellofresh_week = rc.hf_week AND
                          rs.recipe_index = rc.recipe_slot),

--no. of recipes ordered per week
     recipe_count AS (
         SELECT ro.hellofresh_week,
                ro.recipe_index,
                ro.fk_recipe,
                IF(us.recipe_type = 'speedy', 'regular',us.recipe_type) AS recipe_type,
                rd.cuisine,
                rd.title AS recipe_name,
                IF(ro.recipe_index <= 3, 'C3','non_C3') AS C3,
                wsc.weekly_total_rating,
                wsc.weekly_count,
                wsc.weekly_cogs,
                SUM(ro.quantity) AS recipe_vol -- use sum(ro.quantity) to get the recipe volume
         FROM fact_tables.recipes_ordered AS ro
                  INNER JOIN uploads.mbi_anz_upcharge_slots AS us      --using this table to get the recipe type (regular/veggie)
                             ON us.recipe_index = ro.recipe_index AND
                                us.country = ro.country AND
                                us.hf_week = ro.hellofresh_week AND
                                us.box_type = 'mealboxes'
                  INNER JOIN dimensions.recipe_dimension AS rd
                             ON rd.sk_recipe = ro.fk_recipe -- join to dimensions.recipe_dimension on fk_recipe = sk_recipe to get the recipe title
                  INNER JOIN weekly_score_cogs wsc
                             ON wsc.hf_week = ro.hellofresh_week AND
                                wsc.fk_recipe = ro.fk_recipe
         WHERE ro.country = 'AO' AND
                 ro.hellofresh_week >= '2021-W01' AND
                 ro.is_menu_addon = false AND         --removing addon recipes
                 us.surcharge_2 = 0 AND          --removing surcharge recipes
                 ro.recipe_index < 900      --removing modular recipes
         GROUP BY 1,2,3,4,5,6,7,8,9,10),



--no. of boxes shipped per week
     box_count AS (
         SELECT bs.hellofresh_delivery_week,
                SUM(pd.is_mealbox) AS no_of_boxes_shipped
         FROM fact_tables.boxes_shipped AS bs
                  JOIN dimensions.product_dimension AS pd
                       ON pd.sk_product = bs.fk_product AND
                          pd.is_mealbox = true
-- join to dimensions.product_dimension on fk_product = sk_product and is_mealbox is true to filter out non-box entries
-- use sum(is_mealbox) to get the box count
         WHERE bs.country = 'AO' AND
                 bs.hellofresh_delivery_week >= '2021-W01'
         GROUP BY 1
     ),

-- the weekly recipe uptake

     weekly_recipe_uptake AS (
         SELECT rc.hellofresh_week,
                rc.recipe_index,
                rc.recipe_name,
                rc.C3,
                rc.recipe_type,
                rc.recipe_vol,
                rc.weekly_total_rating,
                rc.weekly_cogs,
                rc.weekly_count,
                bc.no_of_boxes_shipped,
                (rc.recipe_vol/bc.no_of_boxes_shipped) AS recipe_uptake
         FROM recipe_count AS rc
                  JOIN box_count AS bc
                       ON rc.hellofresh_week = bc.hellofresh_delivery_week
     )

-- the average uptake, score, and COGS across all the weeks the recipe was scheduled

SELECT wru.recipe_name,
       wru.recipe_type,
       wru.C3,
       SUM(wru.weekly_cogs)/SUM(wru.recipe_vol) AS weighted_recipe_COGS,
       SUM(wru.weekly_total_rating)/SUM(wru.weekly_count) AS weighted_recipe_score,
       AVG(recipe_uptake) AS avg_recipe_uptake,
       SUM(wru.recipe_vol)/SUM(wru.no_of_boxes_shipped) AS weighted_avg_uptake,
       COUNT(DISTINCT wru.hellofresh_week) AS no_of_weeks
FROM weekly_recipe_uptake AS wru
GROUP BY 1,2,3
ORDER BY 1,2,3;
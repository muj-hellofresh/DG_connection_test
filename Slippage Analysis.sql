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

    recipe_slot_preference_mapping AS ( -- using this CTE instead of uploads.pa_anz_preference_default_slots to get the recipe pref per slot
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

    recipe_pref AS
        ( -- has the recipe preference name against each slot for each week (starting 2020-W35)
            -- recipes which are mapped to multiple prefs have multiple pref names separated by a "-"
            SELECT rspm.hellofresh_week,
                   rspm.country,
                   rspm.recipe_index,
                   group_concat(rspm.preference, '-') AS recipe_preference  -- gives us all the preferences linked to a recipe in a single cell separated by a '-'
            FROM recipe_slot_preference_mapping rspm
            WHERE rspm.country IN ('AU', 'NZ')
            GROUP BY rspm.country,
                     rspm.hellofresh_week,
                     rspm.recipe_index
            -- ORDER BY 1,2,3
        ),


sub_pref_recipe_pref AS ( -- adding the recipe preference(s) against every recipe ordered in recipe_ordered (ro) table
    SELECT ro.country,
           d.hf_year,
           d.hf_quarter,
           d.hf_month,
           d.hellofresh_week AS hf_week,
           ro.recipe_index,
           ro.fk_subscription,
           CONCAT('-', rp.recipe_preference, '-') AS recipe_pref_concated, -- '-' added on the front and back of the recipe_pref list for splitting & iteration purposes
           ro.subscription_preference_name        AS customer_preference,
           ro.quantity
           -- MIN(rd.title) AS recipe_title
    FROM fact_tables.recipes_ordered ro
             /*INNER JOIN dimensions.recipe_dimension rd
                    ON ro.fk_recipe = rd.sk_recipe*/
             INNER JOIN dimensions.product_dimension pd
                        ON ro.fk_product = pd.sk_product
             INNER JOIN dates d -- for date dimension
                        ON d.hellofresh_week = ro.hellofresh_week
        --AND d.hf_month >= '2021-M11'
             INNER JOIN recipe_pref rp -- gives us the name(s) of the pref the recipes belong to for each slot
                        ON rp.hellofresh_week = ro.hellofresh_week
                            AND rp.country = ro.country
                            AND rp.recipe_index = ro.recipe_index
    WHERE 1 = 1
      AND ro.is_menu_addon = false     -- removing the add ons
      AND ro.hellofresh_week >= '2021-W01'
      AND ro.country IN ('AU', 'NZ')
      AND ro.reason = 'user-selection' -- only selecting user selected recipes i.e. swapped recipes - taking out the default selections
      AND ro.subscription_preference_name IS NOT NULL
      AND ro.subscription_preference_name NOT IN ('', 'express')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10--, 11, 12, 13, 14, 15, 16
    ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

  pref_expansion AS (
      SELECT sprp.*,
             -- checking for "stickiness"
             CASE
                 WHEN SPLIT_PART(sprp.recipe_pref_concated, '-', 1) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 2) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 3) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 4) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 5) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 6) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 7) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 8) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 9) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 10) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 11) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 12) = sprp.customer_preference
                     OR
                      SPLIT_PART(sprp.recipe_pref_concated, '-', 13) = sprp.customer_preference
                     THEN 1
                 ELSE 0 END                                                                   AS stickiness,
             -- creating a separate column for every preference
             CASE WHEN sprp.recipe_pref_concated LIKE '%-chefschoice-%' THEN 1 ELSE 0 END     AS chefschoice,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-family-%' THEN 1 ELSE 0 END          AS family,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-veggie-%' THEN 1 ELSE 0 END          AS veggie,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-lowcalorie-%' THEN 1 ELSE 0 END      AS lowcalorie,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-quickestrecipes-%' THEN 1 ELSE 0 END AS quickestrecipes,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-glutenfree-%' THEN 1 ELSE 0 END      AS glutenfree,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-nofish-%' THEN 1 ELSE 0 END          AS nofish,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-nopork-%' THEN 1 ELSE 0 END          AS nopork,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-noporknofish-%' THEN 1 ELSE 0 END    AS noporknofish,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-lowcarb-%' THEN 1 ELSE 0 END         AS lowcarb,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-protein-%' THEN 1 ELSE 0 END         AS protein,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-pescatarian-%' THEN 1 ELSE 0 END     AS pescatarian,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-flexitarian-%' THEN 1 ELSE 0 END     AS flexitarian,
             CASE WHEN sprp.recipe_pref_concated LIKE '%-quick-%' THEN 1 ELSE 0 END           AS quick
      FROM sub_pref_recipe_pref sprp
      ORDER BY 1 DESC, 2, 3, 4
  )

SELECT pe.country,
       pe.hf_year,
       pe.hf_quarter,
       RIGHT(pe.hf_quarter,2) AS quarter,
       pe.hf_month,
       --pe.hf_week,
       pe.customer_preference,
       SUM(pe.quantity) AS recipe_qty,
       SUM(pe.stickiness) AS stick_qty,
       SUM(pe.chefschoice) AS chefschoice_qty,
       SUM(pe.family) AS family_qty,
       SUM(pe.veggie) AS veggie_qty,
       SUM(pe.lowcalorie) AS lowcalorie_qty,
       SUM(pe.quickestrecipes) AS quickestrecipes_qty,
       SUM(pe.glutenfree) AS glutenfree_qty,
       SUM(pe.nofish) AS nofish_qty,
       SUM(pe.nopork) AS noprok_qty,
       SUM(pe.noporknofish) AS noporknofish_qty,
       SUM(pe.lowcarb) AS lowcarb_qty,
       SUM(pe.protein) AS protein_qty,
       SUM(pe.pescatarian) AS pescatarian_qty,
       SUM(pe.flexitarian) AS flexitarian_qty,
       SUM(pe.quick) AS quick_qty,
       SUM(pe.chefschoice+pe.family+pe.veggie+pe.lowcalorie+pe.quickestrecipes+pe.glutenfree+pe.nofish+
           pe.nopork+pe.noporknofish+pe.lowcarb+pe.protein+pe.pescatarian+pe.flexitarian+pe.quick) AS pref_count_sum
FROM pref_expansion pe
WHERE pe.stickiness = 0 -- make 0 to only take into consideration instances where customers have "slipped" from their subscribed preference
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6;
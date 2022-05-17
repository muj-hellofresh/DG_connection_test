-- sum the V3 ad V4 COGS
-- ignore the recipe size and DC for starters
-- only take the add-on recipe slots (>900) to check for wastage on add-ons

-- tables of interest
-- pythia - recipe_actuals (for recipe count) - use the recipe count for unit cost (V3 and V4)
-- recipe configurator -- for modularity type (ADD, SWAP, Upgrade)
-- octopus - recipe_cookbook - this has all the SKUs of a given recipe - use this to find
-- if particular SKUs have higher wastage
-- The recipe cookbook can also be used to group the add-ons by protein types - check wastage on protein level
-- extra: can also check DC level to see if particular DCs have higher wastage

WITH recipe_sku AS ( -- using this to get main_protein, main_carb, and other SKU level info for the recipes
    SELECT rcb.week
         ,   rcb.`index` AS recipe_slot
         ,   rcb.main_protein
         ,   rcb.protein_cut
    FROM octopus.recipe_cookbook rcb
    WHERE 1=1
      AND rcb.`index` >= 900
      AND rcb.country = 'au'
      AND rcb.week BETWEEN '2021-W00' AND '2022-W00'
      AND rcb.main_protein IS NOT NULL
    GROUP BY 1,2,3,4
),

  recipe_count AS ( -- using this to count the no. of recipes sold per week for each slot (will be used for unit COGS)
      SELECT ra.week
           ,   ra.recipe AS recipe_slot
           -- ,   ra.dc
           ,   SUM(ra.meal_nr) AS meal_count
      FROM pythia.recipe_actuals ra -- recipe_actuals in AU only
      WHERE 1=1
        AND ra.week BETWEEN '2021-W00' AND '2022-W00'
        AND ra.recipe >= 900
      GROUP BY 1,2-- ,3
  ),

  modularity_type AS ( -- using this to get the modularity type i.e. ADD, SWAP, Upgrade
      SELECT rc.hf_week
           ,   rc.slot_number AS recipe_slot
           ,   rc.recipe_type
           ,   TRIM(SUBSTRING_INDEX(rc.recipe_type, '-', 1)) AS modularity_type
           ,   TRIM(SUBSTRING_INDEX(rc.recipe_type, '-', -1)) AS modularity_detail
      FROM pythia.recipe_configurator rc
      WHERE 1=1
        AND rc.hf_week BETWEEN '2021-W00' AND '2022-W00'
        AND rc.country = 'au'
        AND rc.slot_number >= 900
  ),

  combined_table AS (
      SELECT r.hf_week,
             -- r.dc,
             r.recipe_slot,
             r.version,
             mt.recipe_type,
             mt.modularity_type,
             mt.modularity_detail,
             rs.main_protein,
             rs.protein_cut,
             rc.meal_count     AS recipe_count,
             SUM(r.total_cost) AS total_cost
      FROM cogs.recipe r
               JOIN recipe_count rc
                    ON r.hf_week = rc.week
                        AND r.recipe_slot = rc.recipe_slot
                        -- AND r.dc = rc.dc -- need to cast the DCs into proper format before matching
               JOIN modularity_type mt
                    ON r.hf_week = mt.hf_week
                        AND r.recipe_slot = mt.recipe_slot
               JOIN recipe_sku rs
                    ON r.hf_week = rs.week
                        AND r.recipe_slot = rs.recipe_slot
      WHERE 1 = 1
        AND r.country = 'AU'
        AND r.brand = 'HF'
        AND r.hf_week BETWEEN '2021-W00' AND '2022-W00'
        AND r.version IN (3, 4)
        AND r.recipe_slot >= 900
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
  ),


  cost_table AS (
      SELECT ct.hf_week
           , ct.recipe_slot
-- ,   ct.version
           , ct.recipe_type
           , ct.modularity_type
           , ct.modularity_detail
           , ct.main_protein
           , ct.protein_cut
           , ct.recipe_count
           , ct.total_cost                                                                         AS V4_cost
           , LAG(ct.total_cost) OVER (PARTITION BY ct.hf_week, ct.recipe_slot ORDER BY ct.version) AS V3_cost
           , ct.total_cost - LAG(ct.total_cost) OVER (PARTITION BY ct.hf_week, ct.recipe_slot ORDER BY ct.version) AS cost_diff
      FROM combined_table ct
  )

SELECT ct.*
FROM cost_table ct
WHERE ct.V3_cost IS NOT NULL
ORDER BY 1,2,3,4,5,6;



SELECT -- ct.hf_week
-- ,   ct.recipe_slot
 --  ct.modularity_type
 --   ct.modularity_detail
      ct.recipe_type
--  ,   ct.main_protein
--   ct.main_carb
,   SUM(ct.cost_diff)/SUM(ct.recipe_count) AS avg_wastage
FROM cost_table ct
WHERE ct.V3_cost IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;



-- checking for dc in recipe cogs
SELECT r.dc,
       COUNT(*)
FROM cogs.recipe r
WHERE r.country = 'AU'
  AND r.brand = 'HF'
  AND r.hf_week >= '2021-W01'
GROUP BY 1
ORDER BY 1;

-- checking for dc in recipe actuals
SELECT ra.dc,
       SUM(ra.meal_nr)
FROM pythia.recipe_actuals ra
WHERE ra.week >= '2021-W01'
GROUP BY 1
ORDER BY 1;







-- recipe count per add-on slot
SELECT ra.week
,   ra.recipe AS recipe_slot
,   SUM(ra.meal_nr)
FROM pythia.recipe_actuals ra -- recipe_actuals in AU only
WHERE 1=1
AND ra.week >= '2021-W01'
AND ra.recipe >= 900
GROUP BY 1,2
ORDER BY 1,2;

-- modularity type (ADD, SWAP, Upgrade)
SELECT rc.hf_week
,   rc.slot_number AS recipe_slot
,   rc.recipe_type
,   TRIM(SUBSTRING_INDEX(rc.recipe_type, '-', 1))
FROM pythia.recipe_configurator rc
WHERE 1=1
AND rc.hf_week >= '2021-W01'
AND rc.country = 'au'
AND rc.slot_number >= 900
ORDER BY 1,2,3

-- getting main protein / SKU level info of the recipes
SELECT rcb.week
,   rcb.`index`
,   rcb.main_protein
,   rcb.main_carb
-- ,   rcb.
FROM octopus.recipe_cookbook rcb
WHERE 1=1
AND rcb.`index` >= 900
AND rcb.country = 'au'
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
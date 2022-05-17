-- full mealbox donation count (counted using boxes_shipped is_donation column)
-- the full mealboxes are only donated at the end of the year/start of new year
-- donations only happened in 2017-19
-- no full box donation recorded in 2020/21

SELECT bs.hellofresh_delivery_week,
       COUNT(*)
FROM fact_tables.boxes_shipped bs
WHERE 1=1
  AND bs.country = 'AU'
--  AND bs.hellofresh_delivery_week >= '2020-W01'
  AND bs.is_donation = TRUE
GROUP BY 1
ORDER BY 1 DESC;


--trying to group the number of donations per week per flex amount
-- getting duplicated values, need to correct the following query

WITH donation_amount AS -- checking the recipe_in_menu table for donations
    (SELECT m.country
         , m.week
         , m.index
         , m.fk_recipe
         , m.product
         , if(m.country = 'NZ', m.charge_amount / 1.15, m.charge_amount) as charge_amount
         , if(m.country = 'NZ',c.flex_amount / 1.15,c.flex_amount) as flex_amount
    FROM fact_tables.recipe_in_menu m, m.charge_flexible_amounts c
    WHERE m.country in ('AU', 'NZ', 'AO')
      AND m.week >= '2020-W01'
      --and m.charge_amount > 0
      --and c.people = 1
      AND product LIKE '%dona%'
    group by 1, 2, 3, 4, 5, 6, 7)


SELECT ro.hellofresh_week,
       da.flex_amount,
       COUNT(*) AS donation_count
FROM fact_tables.recipes_ordered ro
         JOIN dimensions.product_dimension pd
             ON ro.fk_product = pd.sk_product
         JOIN donation_amount da
             ON da.fk_recipe = ro.fk_recipe
                    AND da.week = ro.hellofresh_week
                    AND da.index = ro.recipe_index
WHERE 1=1
AND ro.country = 'AU'
AND ro.hellofresh_week >= '2021-W01'
AND pd.product_name LIKE '%dona%'
GROUP BY 1,2
ORDER BY 1,2;


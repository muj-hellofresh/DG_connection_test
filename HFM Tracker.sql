-- Box Actuals

WITH box_dc AS (

-- CTE for DC info per box
-- aim is to get one row per box delivered with country, box_id, and DC
-- will use the views_analysts.gor_odl_landing table to get the DC info
-- doing this step because views_analysts.gor_odl_landing table had duplicated rows per box in some instances

    SELECT gol.country,
           gol.box_id,
           CASE
               WHEN gol.dc = 'Sydney' THEN 'Esky'
               WHEN gol.dc = 'Melbourne' THEN 'Tuckerbox'
               WHEN gol.dc = 'Perth' THEN 'Casa'
               WHEN gol.dc IN ('Chilli Bin', 'NZ') THEN 'Chilli Bin'
               END AS dc
    FROM views_analysts.gor_odl_landing gol
    WHERE gol.country IN ('AU', 'NZ', 'AO')
      AND gol.hellofresh_week >= '2021-W01'
    GROUP BY 1, 2, 3
),
     addon_cust as (
         select ro.country
              , ro.hellofresh_week
              , count(distinct sd.subscription_id) as addon_customers
         from fact_tables.recipes_ordered ro
                  inner join dimensions.subscription_dimension sd on sd.sk_subscription = ro.fk_subscription
         where ro.country in ('AU', 'NZ', 'AO')
           and ro.hellofresh_week >= '2021-W01'
           and ro.is_menu_addon is true
         group by 1, 2
     )


SELECT bs.country,
       bs.hellofresh_delivery_week,
       ifnull(a.addon_customers,0),
       bd.dc,
       sum(pd.is_mealbox)
FROM fact_tables.boxes_shipped AS bs
         LEFT JOIN dimensions.product_dimension AS pd
                   ON pd.country = bs.country AND pd.sk_product = bs.fk_product AND is_mealbox = TRUE
         left join addon_cust a
                   on a.hellofresh_week = bs.hellofresh_delivery_week and a.country = bs.country
         LEFT JOIN box_dc bd -- getting the DC for every box -- change this to left join -- we can report the DC level data at a week's lag
                    ON bs.box_id = bd.box_id
WHERE bs.hellofresh_delivery_week >= '2021-W01'
  AND bs.country IN ('AU', 'NZ', 'AO')
GROUP BY 1, 2, 3, 4
HAVING sum(pd.is_mealbox) IS NOT NULL
ORDER BY 1, 2, 3, 4;


------------------------------------------------------------------------------
-- Tracker Actuals

with id as (
    select
        IF (m.brand = 'EP','AO',UPPER(m.country)) as country
         ,m.hf_week
         ,m.recipe_id
         ,m.slot
         ,sum(m.quantity) as volume
         ,sum(m.revenue) as revenue
         ,sum(m.cost) as cogs
    from octopus.menusslots m
    where m.hf_week >= '2021-W40'
      and m.slot between 50 and 899
    group by 1,2,3,4
) ,
     au_vol as (
         select
             'AU' as country
              ,a.week
              ,a.recipe
              -- ,a.dc -- new addition for dc
              ,sum(a.meal_nr) as volume
         from pythia.recipe_actuals a -- has a column for dc
         where a.box_type <> 'mealboxes'
           and a.week >= '2021-W40'
         group by 1,2,3-- ,4
     ),
     nz_vol as (
         select
             'NZ' as country
              ,a.week
              ,a.recipe
              -- ,a.dc -- new addition for dc
              ,sum(a.meal_nr) as volume
         from pythia.recipe_actuals_nz a
         where a.box_type <> 'mealboxes'
           and a.week >= '2021-W01'
         group by 1,2,3-- ,4
     ),
     ep_vol as (
         select
             'AO' as country
              ,a.week
              ,a.recipe
              -- ,a.dc -- new addition for dc
              ,sum(a.meal_nr) as volume
         from pythia.recipe_actuals_ao a
         where a.box_type <> 'mealboxes'
           and a.week >= '2021-W01'
         group by 1,2,3-- ,4
     ),
     total_vols as (
         select
             *
         from au_vol
         union all
         select
             *
         from nz_vol
         union all
         select
             *
         from ep_vol
     ),
     cogs as (
         select
             if(r.brand <> 'HF','AO',r.country) as country
              ,r.hf_week
              ,r.recipe_slot
              ,r.recipe_id
              -- ,LOWER(r.market) AS dc-- new addition for dc
              ,sum(if(r.version=3,r.total_cost,0)) as v3_cogs
              ,sum(if(r.version=4,r.total_cost,0)) as v4_cogs
         from cogs.recipe r
         where r.recipe_slot between 50 and 899
           and r.hf_week >= '2021-W01'
         group by 1,2,3,4-- ,5
     )
select
    t.country
     /*,CASE
          WHEN t.dc = 'sydney' THEN 'Esky'
          WHEN t.dc = 'melbourne' THEN 'Tuckerbox'
          WHEN t.dc = 'perth' THEN 'Casa'
          WHEN t.dc = 'nz' THEN 'Chilli Bin'
        END AS dc*/
     ,t.week
     ,id.recipe_id
     ,t.recipe as slot
     ,t.volume
     ,rc.surcharge_2 * t.volume as total_revenue
     ,if(c.v4_cogs = 0,c.v3_cogs,c.v4_cogs) as cogs
from total_vols t
         inner join pythia.recipe_configurator rc on rc.country = t.country and rc.hf_week = t.week and rc.slot_number = t.recipe
         left join cogs c on c.country = t.country and c.hf_week = t.week and c.recipe_slot = t.recipe -- AND t.dc = c.dc
         left join id on id.country = t.country and t.week = id.hf_week and t.recipe = id.slot
where t.week >= '2021-W40'
  #AND c.v3_cogs > 0
ORDER BY 1,2,3;-- ,5;



-------------------------------------
-- adding DC level

with id as (
    select
    IF (m.brand = 'EP','AO',UPPER(m.country)) as country
        ,m.hf_week
        ,m.recipe_id
        ,m.slot
        ,sum(m.quantity) as volume
        ,sum(m.revenue) as revenue
        ,sum(m.cost) as cogs
    from octopus.menusslots m
    where m.hf_week >= '2021-W40'
    and m.slot between 50 and 899
    group by 1,2,3,4
    ) ,
    au_vol as (
    select
    'AU' as country
        ,a.week
        ,a.recipe
        ,a.dc -- new addition for dc
        ,sum(a.meal_nr) as volume
    from pythia.recipe_actuals a -- has a column for dc
    where a.box_type <> 'mealboxes'
    and a.week >= '2021-W40'
    group by 1,2,3,4
    ),
    nz_vol as (
    select
    'NZ' as country
        ,a.week
        ,a.recipe
        ,a.dc -- new addition for dc
        ,sum(a.meal_nr) as volume
    from pythia.recipe_actuals_nz a
    where a.box_type <> 'mealboxes'
    and a.week >= '2021-W01'
    group by 1,2,3,4
    ),
    ep_vol as (
    select
    'AO' as country
        ,a.week
        ,a.recipe
        ,a.dc -- new addition for dc
        ,sum(a.meal_nr) as volume
    from pythia.recipe_actuals_ao a
    where a.box_type <> 'mealboxes'
    and a.week >= '2021-W01'
    group by 1,2,3,4
    ),
    total_vols as (
    select
    *
    from au_vol
    union all
    select
    *
    from nz_vol
    union all
    select
    *
    from ep_vol
    ),
    cogs as (
    select
    if(r.brand <> 'HF','AO',r.country) as country
        ,r.hf_week
        ,r.recipe_slot
        ,r.recipe_id
        ,LOWER(r.market) AS dc-- new addition for dc
        ,sum(if(r.version=3,r.total_cost,0)) as v3_cogs
        ,sum(if(r.version=4,r.total_cost,0)) as v4_cogs
    from cogs.recipe r
    where r.recipe_slot between 50 and 899
    and r.hf_week >= '2021-W01'
    group by 1,2,3,4,5
    )
select
    t.country
    ,CASE
         WHEN t.dc = 'sydney' THEN 'Esky'
         WHEN t.dc = 'melbourne' THEN 'Tuckerbox'
         WHEN t.dc = 'perth' THEN 'Casa'
         WHEN t.dc = 'nz' THEN 'Chilli Bin'
       END AS dc
     ,t.week
     ,id.recipe_id
     ,t.recipe as slot
     ,t.volume
     ,rc.surcharge_2 * t.volume as total_revenue
     ,if(c.v4_cogs = 0,c.v3_cogs,c.v4_cogs) as cogs
from total_vols t
         inner join pythia.recipe_configurator rc on rc.country = t.country and rc.hf_week = t.week and rc.slot_number = t.recipe
         left join cogs c on c.country = t.country and c.hf_week = t.week and c.recipe_slot = t.recipe AND t.dc = c.dc
         left join id on id.country = t.country and t.week = id.hf_week and t.recipe = id.slot
where t.week >= '2021-W40'
      #AND c.v3_cogs > 0
ORDER BY 1,2,3,5;

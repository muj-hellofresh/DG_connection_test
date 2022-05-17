--original query by Nick for the weekly metric data update

with recipe_slots as (SELECT country
                           , hf_week
                           , box_type
                           , recipe_index
                           , recipe_type
                           , case
                                 when surcharge_2 = 0 then recipe_type
                                 when recipe_index >= 900 then recipe_type
                                 else concat(if(recipe_type = 'pub bistro','steak night',recipe_type), cast((row_number()
                                     over (partition by hf_week, recipe_type,country order by recipe_index)) as varchar)) end recipe_type_num
                           , CASE
                                 WHEN charge_type = 'per_serve' THEN surcharge_2 * 2
                                 WHEN charge_type = 'fixed' THEN surcharge_2
        END                                                                                                                                                           2p_price
                           , CASE
                                 WHEN charge_type = 'per_serve' THEN surcharge_2 * 1
                                 WHEN charge_type = 'fixed' THEN surcharge_2
        END                                                                                                                                                           1p_price
                           , CASE
                                 WHEN charge_type = 'per_serve' THEN surcharge_2 * 3
                                 WHEN charge_type = 'fixed' THEN surcharge_2
        END                                                                                                                                                           3p_price
                           , CASE
                                 WHEN charge_type = 'per_serve' THEN surcharge_4 * 4
                                 WHEN charge_type = 'fixed' THEN surcharge_4
        END                                                                                                                                                           4P_price
                           , CASE
                                 WHEN charge_type = 'per_serve' THEN surcharge_2 * 6
                                 WHEN charge_type = 'fixed' THEN surcharge_2
        END                                                                                                                                                           6P_price
                      FROM uploads.mbi_anz_upcharge_slots
                      WHERE box_type = 'mealboxes'
                        and country in ('AU', 'NZ', 'AO')
                        and hf_week >= '2018-W01'
),
     addon_slots as (
         select m.country
              , m.week
              , m.index
              , m.fk_recipe
              , m.product
              , if(m.country = 'NZ', m.charge_amount / 1.15, m.charge_amount) as charge_amount
              , if(m.country = 'NZ',c.flex_amount / 1.15,c.flex_amount) as flex_amount
         from fact_tables.recipe_in_menu m, m.charge_flexible_amounts c
         where m.country in ('AU', 'NZ', 'AO')
           and m.week >= '2020-W01'
           --and m.charge_amount > 0
           --and c.people = 1
         group by 1, 2, 3, 4, 5, 6, 7
     ),
     addon_slot_2 as (
         select
             i.country
              ,i.hellofresh_week
              ,i.fk_recipe
              ,avg(i.inc_full_price_revenue_local_currency) as price
         from materialized_views.incremental_profit_contribution i
         where i.country in ('AU','NZ','AO')
           and i.recipe_type = 'Addon'
           and i.hellofresh_week >= '2020-W01'
         group by 1,2,3
     ),
     addon_slot_3 as (
         select m.country
              , m.week
              , m.index
              , m.fk_recipe
              , m.product
              , if(m.country = 'NZ', m.charge_amount / 1.15, m.charge_amount) as charge_amount
              --, if(m.country = 'NZ',c.flex_amount / 1.15,c.flex_amount) as flex_amount
         from fact_tables.recipe_in_menu m
         where m.country in ('AU', 'NZ', 'AO')
           and m.week >= '2020-W01'
           and m.charge_amount > 0
           --and c.people = 1
         group by 1, 2, 3, 4, 5, 6
     ),
     cogs as (
         select r.country
              , r.hf_week
              , r.recipe_slot
              , cast(r.recipe_size as string) as recipe_size
              , sum(r.total_cost) as total_cost
         from uploads.pa_anz__recipe r
         group by 1, 2, 3, 4
     ),

     dates as (
         SELECT hellofresh_week,
                CAST(hellofresh_year AS STRING)                                  as hf_year,
                CONCAT(CAST(hellofresh_year AS STRING), '-M',
                       LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
                CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
         FROM dimensions.date_dimension dd
         WHERE dd.year >= 2018
         GROUP BY 1, 2, 3, 4
     )
select ro.country
     , d.hf_year
     , d.hf_quarter
     , d.hf_month
     , ro.hellofresh_week
     , case
           when ro.is_menu_addon is true then 'Addon'
           when (2p_price > 0 or ro.recipe_index >= 900) then 'Surcharge'
           else 'Regular'
    end                                                               product_type
     , if(ro.is_menu_addon is true, pd.product_name, if(ro.recipe_index>=900,rs.recipe_type,rs.recipe_type_num)) as recipe_type
     , ro.recipe_index
     , if(ro.is_menu_addon is false,
          rd.title,
          CASE
              when pd.product_name like '%snack%' and ro.recipe_index = 1 and quantity = 4
                  then 'snack_refuel_2p'
              when pd.product_name like '%snack%' and ro.recipe_index = 1 and quantity = 8
                  then 'snack_refuel_4p'
              when pd.product_name like '%snack%' and ro.recipe_index = 2 and quantity = 4
                  then 'snack_recharge_2p'
              when pd.product_name like '%snack%' and ro.recipe_index = 2 and quantity = 8
                  then 'snack_recharge_4p'
              when ((pd.product_name like '%fruit%' and ro.recipe_index = 2 and
                     ro.hellofresh_week >= '2020-W05')
                  or (pd.product_name like '%fruit%' and
                      (ro.recipe_index = 1 or pd.box_size = 'XS') and
                      ro.hellofresh_week <= '2020-W04')) then 'fruit_small'
              when ((pd.product_name like '%fruit%' and ro.recipe_index = 1 and
                     ro.hellofresh_week >= '2020-W05')
                  or (pd.product_name like '%fruit%' and
                      (ro.recipe_index = 2 or pd.box_size = 'S') and
                      ro.hellofresh_week <= '2020-W04')) then 'fruit_regular'
              when pd.product_name like '%appetizers%' and ro.recipe_index = 1 then 'bread'
              when pd.product_name like '%appetizers%' and ro.recipe_index = 2 then 'veggies'
              when pd.product_name like '%appetizers%' and ro.recipe_index = 3 then 'salad'
              when pd.product_name like '%appetizers%' and ro.recipe_index = 4 then 'bread'
              when pd.product_name like '%desserts%' then 'dessert'
              when pd.product_name like '%soup%'
                  then concat('soup', cast(ro.recipe_index as string))
              when pd.product_name like '%ready%'
                  then concat('rth', cast(ro.recipe_index as string))
              else pd.product_name
              end ) as title
     , pd.box_size
     , sum(ro.quantity)                                            as volume
     , sum(case
               when ro.is_menu_addon is true then ro.quantity * ifnull(a.flex_amount, ifnull(a2.price,a3.charge_amount))
               when pd.box_size = '2' then 2p_price * ro.quantity
               when pd.box_size = '4' then 4P_price * ro.quantity
               when pd.box_size = '6' then 6P_price * ro.quantity
               when pd.box_size = '1' then 1P_price * ro.quantity
               when pd.box_size = '3' then 3P_price * ro.quantity
               else 0
    end)                                                           as revenue
     , sum(c.total_cost)                                           as total_cost
from fact_tables.recipes_ordered ro
         inner join dimensions.product_dimension pd on pd.sk_product = ro.fk_product
         inner join dimensions.recipe_dimension rd on rd.sk_recipe = ro.fk_recipe
         left join recipe_slots rs
                   on rs.recipe_index = ro.recipe_index and rs.country = ro.country and rs.hf_week = ro.hellofresh_week
         left join addon_slots a on a.country = ro.country and a.week = ro.hellofresh_week and a.index = ro.recipe_index and a.fk_recipe = ro.fk_recipe
         left join cogs c
                   on c.country = ro.country and c.hf_week = ro.hellofresh_week and c.recipe_size = pd.box_size and
                      c.recipe_slot = ro.recipe_index
         inner join dates d on d.hellofresh_week = ro.hellofresh_week
         left join addon_slot_2 a2 on a2.country = ro.country and a2.hellofresh_week = ro.hellofresh_week and a2.fk_recipe = ro.fk_recipe
         left join addon_slot_3  a3 on a3.country = ro.country and a3.week = ro.hellofresh_week and a3.fk_recipe = ro.fk_recipe
where ro.country in ('AU', 'NZ', 'AO')
  and ro.hellofresh_week >= '2020-W01'
group by 1,2,3,4,5,6,7,8,9,10
order by 1,2,3,4,5,6,7,8,9,10;
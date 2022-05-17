
-- calculating the pause rate (for the festive period)

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
      AND SS.hellofresh_week >= '2021-W45'
      AND ss.previous_status = 'active'

)

SELECT country,
       hellofresh_week,
       AVG(status_paused) AS pause_rate
FROM SUBSCRIPTION_CHANGES
WHERE country IN ('AU', 'NZ')
GROUP BY 1,2
ORDER BY 1,2;



-- Pause Reasons


SELECT *
FROM fact_tables.boxes_shipped bs
JOIN dimensions.product_dimension pd
ON bs.fk_product = pd.sk_product
AND bs.country = pd.country
WHERE bs.country = 'AU'
;

SELECT *
FROM fact_tables.boxes_shipped bs
LIMIT 10;

;


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
                  and country in ('AU', 'NZ')     -- to get rid of false country values
                  and split_part(event_label, '|', 4) >=  '2018-W37'
                  -- to look for data of a specific week, week in which the user triggered the pause action, and NOT the week paused
                group by 1, 2, 3, 4)
SELECT country
     ,paused_week
     ,reason
     ,count(distinct uid) as customers
FROM pauses
WHERE paused_week BETWEEN '2021-W48' AND '2022-W02'
GROUP BY 1,2,3
ORDER BY 1,2,3,4 DESC;



--- Box Count
SELECT bsc.country,
       bsc.hellofresh_delivery_week,
       bsc.box_count--) AS avg_yearly_box_count
FROM materialized_views.mbi_anz_boxes_shipped_with_costs AS bsc
WHERE 1=1
  AND bsc.country IN ('AU','NZ')
  AND bsc.hellofresh_delivery_week BETWEEN '2021-W49' AND '2022-W00'
--GROUP BY 1
ORDER BY 1,2;



--- AOV
-- taking the Avg AOV for Q4

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)


SELECT bs.country,
       d.hf_quarter,
       SUM(bs.full_retail_price_local_currency / (1 + bs.vat_percent) +
           bs.shipping_fee_excl_vat_local_currency) / SUM(pd.is_mealbox) AS aov  -- summing the total amt and dividing by number of mealboxes
FROM fact_tables.boxes_shipped bs
         JOIN dimensions.product_dimension pd
              ON bs.fk_product = pd.sk_product
         JOIN dimensions.subscription_dimension sd
              ON bs.fk_subscription = sd.sk_subscription
         JOIN dates d
              ON d.hellofresh_week = bs.hellofresh_delivery_week
                AND d.hf_quarter = '2021-Q4'
WHERE 1 = 1
  AND bs.country IN ('AU', 'NZ')
  -- AND pd.is_menu_addon = FALSE
  -- AND pd.is_mealbox = TRUE
GROUP BY 1, 2
ORDER BY 1,2;
--ORDER BY 1


--- seamless

WITH seamless_count AS (
    SELECT ss.country,
           ss.week_id,
           COUNT(*) seamless_count
    FROM materialized_views.vas_2_surcharge_seamless ss
    WHERE 1 = 1
      AND ss.country IN ('AU', 'NZ')
      AND ss.week_id BETWEEN '2021-W00' AND '2022-W00'
      AND ss.revenue_local_currency > 0 -- to exclude customers who used seamless to downgrade box size
    GROUP BY 1, 2
),

    box_count AS
        (
            SELECT bsc.country,
                   bsc.hellofresh_delivery_week,
                   bsc.box_count
            FROM materialized_views.mbi_anz_boxes_shipped_with_costs AS bsc
            WHERE 1=1
              AND bsc.country IN ('AU','NZ')
              AND bsc.hellofresh_delivery_week BETWEEN '2021-W00' AND '2022-W00'
            -- GROUP BY 1
            -- ORDER BY 1
        )

SELECT bc.country,
       bc.hellofresh_delivery_week,
       sc.seamless_count/bc.box_count AS seamless_uptake
FROM box_count bc
JOIN seamless_count sc
ON bc.country = sc.country
AND bc.hellofresh_delivery_week = sc.week_id
ORDER BY 1,2;



---- customers who paused to take christmas box

WITH pause_w50 AS (-- customers who paused after being active in W50
    SELECT SS.country
         , SS.hellofresh_week
         , sd.customer_id
         , SS.previous_week
         , SS.status_active
         , SS.status_canceled
         , SS.status_user_paused + SS.status_interval_paused AS status_paused
    FROM fact_tables.subscription_statuses SS
             JOIN dimensions.subscription_dimension sd
                  ON SS.fk_subscription = sd.sk_subscription
    WHERE SS.country IN ('AU', 'NZ')
      AND SS.previous_week = '2021-W50'
      AND ss.previous_status = 'active'
      AND (SS.status_user_paused + SS.status_interval_paused) = 1
),

  xmb_2021 AS (-- customers who took christmas box in 2021

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
      GROUP BY 1, 2, 3
  )

SELECT -- customer overlap (divide by active customers in W50 to get %)
       p.country,
       COUNT(p.customer_id)
FROM pause_w50 p
INNER JOIN xmb_2021 x
ON p.country = x.country
AND p.customer_id = x.customer_id
GROUP BY 1
ORDER BY 1;



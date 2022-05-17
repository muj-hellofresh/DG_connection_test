WITH bs AS (
    SELECT
        bs.country
         ,bs.fk_customer
         ,hellofresh_delivery_week

    FROM fact_tables.boxes_shipped AS bs
             LEFT JOIN dimensions.product_dimension AS pd
                 AND pd.product_sku LIKE '%CB%' -- open for discussion (come back to it)
                       ON pd.sk_product = bs.fk_product
)


SELECT
    bs.country
     ,cd.customer_id
     ,MIN(bs.hellofresh_delivery_week) AS first_delivery_week
     ,MIN(dd.hellofresh_running_week)+9 AS running_delivery_week_10
     ,MIN(dd.hellofresh_running_week)+51 AS running_delivery_week_52

FROM bs -- on the left bec its a bigger table
         INNER JOIN dimensions.customer_dimension AS cd
            ON cd.sk_customer = bs.fk_customer
         LEFT JOIN dimensions.date_dimension AS dd
            ON bs.hellofresh_delivery_week = dd.hellofresh_week

WHERE LEFT(hellofresh_delivery_week,4) = '2019' -- alternatively hellofresh_delivery_week BETWEEN '2019-W01' AND '2019-W52'
  AND bs.country IN ('AU', 'NZ')



GROUP BY bs.country,cd.customer_id; -- can be changed to 1,2 but this is more readable and more scalable



----------------------------------------------------------------


WITH hello_month AS (
    SELECT hellofresh_week
        , hellofresh_month
    FROM dimensions.date_dimension
    GROUP BY 1,2
    )
        , revenue AS (
    SELECT hello_month.hellofresh_month
        ,SUM(bs.full_retail_price_local_currency/(1 + bs.vat_percent)
    + bs.shipping_fee_excl_vat_local_currency
    ) AS gross_revenue

    FROM fact_tables.boxes_shipped AS bs
    LEFT JOIN hello_month
    ON hello_month.hellofresh_week = bs.hellofresh_delivery_week

    WHERE bs.hellofresh_delivery_week BETWEEN '2021-W01' AND '2021-W25'
    AND bs.country = 'AU'

    GROUP BY 1
    )
        , surcharge_skus AS (
    SELECT hello_month.hellofresh_month
        , pd.product_sku

    FROM fact_tables.boxes_shipped AS bs
    INNER JOIN dimensions.product_dimension AS pd
    ON bs.fk_product = pd.sk_product
    AND NOT pd.is_mealbox

    LEFT JOIN hello_month
    ON hello_month.hellofresh_week = bs.hellofresh_delivery_week

    WHERE bs.hellofresh_delivery_week BETWEEN '2021-W01' AND '2021-W25'
    AND bs.country = 'AU'

    GROUP BY 1,2

    )
SELECT
    CONCAT(
            '2021-M'
        , LPAD(CAST(r.hellofresh_month AS STRING), 2, '0')
        ) AS hellofresh_month
     , r.gross_revenue
     , GROUP_CONCAT(ss.product_sku) AS surcharge_skus

FROM revenue AS r
         LEFT JOIN surcharge_skus AS ss
                   ON r.hellofresh_month = ss.hellofresh_month

GROUP BY 1,2
ORDER BY 1;


--------------------

SELECT bs.country
     ,cd.customer_id
     ,bs.hellofresh_delivery_week
     ,gd.region_1
     , bs.price_paid_on_website_local_currency
     ,AVG(bs.price_paid_on_website_local_currency) OVER (PARTITION BY gd.region_1, bs.hellofresh_delivery_week) AS avg_price_state_week

FROM fact_tables.boxes_shipped bs
LEFT JOIN dimensions.customer_dimension cd
ON bs.fk_customer = cd.sk_customer
INNER JOIN dimensions.geo_dimension gd
ON bs.fk_geo = gd.sk_geo
INNER JOIN dimensions.product_dimension pd
ON bs.fk_product = pd.sk_product
    AND pd.is_mealbox --= TRUE
WHERE bs.country = 'AU'
AND bs.hellofresh_delivery_week BETWEEN '2019-W00' AND '2020-W00'
ORDER BY 2;


-----------------------------------------------------------------

SELECT pc.country,
       cd.state_island,
       AVG(pc.gross_ccv) AS avg_gross_ccv,
       AVG(pc.net_ccv)   AS avg_net_ccv,
       AVG(pc.cav)       AS avg_cav
FROM materialized_views.mbi_anz_profitability_campaign AS pc
         INNER JOIN materialized_views.mbi_anz_customer_dimension AS cd
                    ON pc.customer_id = cd.customer_id
                        AND pc.country = cd.country
                        AND cd.state_island IS NOT NULL
WHERE 1 = 1
  AND pc.channel = 'HelloShare Online'
  AND pc.iso_week_utc BETWEEN '2020-W00' AND '2021-W00'
  AND pc.country = 'AU'
GROUP BY 1, 2
ORDER BY 1, 2


-------------------------------------------------------------------------

SELECT
FROM materialized_views.mbi_anz_customer_loyalty_weekly_extended AS clwe
JOIN materialized_views.mbi_anz_customer_statuses AS cs
JOIN materialized_views.mbi_anz_conversions AS c

;

-- filtering our the customers who activated in 2020
SELECT c.country,
       c.iso_week_utc,
       c.customer_id,
       c.conversion_type
FROM materialized_views.mbi_anz_conversions AS c
WHERE c.conversion_type = 'activation'
AND c.iso_week_utc BETWEEN '2020-W00' AND '2021-W00'
ORDER BY 1,2,3;

-- filtering out the first week of pausing for the customers
SELECT cs.country,
       cs.customer_id,
       MIN(cs.hf_week),
       cs.status
FROM materialized_views.mbi_anz_customer_statuses AS cs
WHERE cs.status = 'paused'
ORDER BY 1,2;

------------------------------------------------------------------
WITH t1 AS (
SELECT abw.hellofresh_year,
       SUM(abw.gr) / SUM(abw.box_count)                                  AS aov,
       SUM((abw.gr_meal_box + abw.gr_seasonal_box)) / SUM(abw.box_count) AS aov_box,
       SUM(abw.gr_shipping) / SUM(abw.box_count)                         AS aov_shipping,
       SUM(abw.gr_add_ons) / SUM(abw.box_count)                          AS aov_add_on,
       SUM(abw.gr_surcharge) / SUM(abw.box_count)                        AS aov_surcharge

FROM materialized_views.mbi_anz_aov_breakdown_weekly abw
WHERE abw.hellofresh_year IN (2019, 2020)
  AND abw.country = 'AU'
GROUP BY 1),

t2 AS (
SELECT abw.hellofresh_year,
       SUM(abw.gr) / SUM(abw.box_count)                                  AS aov,
       SUM((abw.gr_meal_box + abw.gr_seasonal_box)) / SUM(abw.box_count) AS aov_box,
       SUM(abw.gr_shipping) / SUM(abw.box_count)                         AS aov_shipping,
       SUM(abw.gr_add_ons) / SUM(abw.box_count)                          AS aov_add_on,
       SUM(abw.gr_surcharge) / SUM(abw.box_count)                        AS aov_surcharge

FROM materialized_views.mbi_anz_aov_breakdown_weekly abw
WHERE abw.hellofresh_year IN (2019, 2020)
  AND abw.country = 'AU'
GROUP BY 1)

SELECT t1.aov - t2.aov,
       t1.aov_box - t2.aov_box,
       t1.aov_shipping - t2.aov_shipping,
       t1.aov_add_on - t2.aov_add_on,
       t1.aov_surcharge - t2.aov_surcharge
FROM t1
CROSS JOIN t2
WHERE t1.hellofresh_year = 2019;




SELECT
FROM materialized_views.mbi_anz_running_weeks;

SELECT
FROM materialized_views.mbi_anz_customer_loyalty_weekly_extended




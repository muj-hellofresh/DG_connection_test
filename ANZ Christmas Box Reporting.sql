-- breakdown of order freq by loyalty (both AU and NZ)
-- link the upload table to materialized_views.mbi_anz_customer_loyalty_weekly_extended maclw on country, customer_id, and order_week
-- get the loyalty level from the customer_loyalty_weekly table at the time of XMB ordering

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),


     delivered_boxes AS ( -- use this to filter out just the delivered XMB boxes from all the orders (as the orders might have some refunds/cancels etc)
         SELECT d.hf_year,
                bs.country,
                sd.customer_id
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                           AND pd.product_sku LIKE '%XMB%'
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
                  JOIN dates d
                       ON bs.hellofresh_delivery_week = d.hellofresh_week
         WHERE bs.country IN ('AU', 'NZ')
           AND bs.hellofresh_delivery_week >= '2019-W40'
         GROUP BY 1, 2, 3
     ),

     orders AS ( -- all the orders for the XMB (including cancelled and refunds etc)
         SELECT d.hf_year,
                o.country,
                o.order_created_at_hf_week AS hf_week,
                sd.customer_id
         FROM fact_tables.orders o
                  JOIN dimensions.subscription_dimension sd
                       ON o.fk_subscription = sd.sk_subscription
                  JOIN dimensions.product_dimension pd
                       ON o.fk_product = pd.sk_product
                           AND pd.product_sku LIKE '%XMB%'
                  JOIN dates d
                       ON o.order_created_at_hf_week = d.hellofresh_week
         WHERE o.country IN ('AU', 'NZ')
           AND o.order_created_at_hf_week >= '2019-W40'
         GROUP BY 1,2,3,4
     ),

     customer_list AS ( -- the list of customers who received an XMB along with the hf_week they placed the order in
         SELECT o.hf_year,
                o.country,
                o.hf_week,
                o.customer_id
         FROM delivered_boxes db
                  INNER JOIN orders o
                             ON o.hf_year = db.hf_year
                                 AND o.country = db.country
                                 AND o.customer_id = db.customer_id
     )


SELECT cl.hf_year,
       cl.country,
       CASE
           WHEN maclw.box_count <= 1 THEN '[1]'
           WHEN maclw.box_count <= 3 THEN '[2-3]'
           WHEN maclw.box_count <= 10 THEN '[4-10]'
           WHEN maclw.box_count <= 25 THEN '[11-25]'
           WHEN maclw.box_count <= 51 THEN '[26-51]'
           WHEN maclw.box_count <= 103 THEN '[52-103]'
           WHEN maclw.box_count >= 104 THEN '[104+]'
           ELSE '[0]' END AS loyalty_level,
       COUNT(*) AS box_count
FROM customer_list cl
         LEFT JOIN materialized_views.mbi_anz_customer_loyalty_weekly_extended maclw
                   ON cl.country = maclw.country
                       AND cl.hf_week = maclw.hf_week
                       AND cl.customer_id = maclw.customer_id
GROUP BY 1,2,3
ORDER BY 1,2,3;






-- % Customer uptake (over active customer base from selling period i.e. 2021-W42 to 2021-W50)
-- total XMB shipped / total boxes shipped for distinct customer_ids?

-- XMB box count (overall)
WITH xmb_customers AS (
    SELECT bs.country,
           sd.customer_id
    FROM fact_tables.boxes_shipped bs
             JOIN dimensions.product_dimension pd
                  ON bs.fk_product = pd.sk_product
                      AND pd.product_sku LIKE '%XMB%'
             JOIN dimensions.subscription_dimension sd
                  ON bs.fk_subscription = sd.sk_subscription
    WHERE bs.hellofresh_delivery_week >= '2021-W50'
      AND bs.country IN ('AU', 'NZ')
    GROUP BY 1, 2
),

     xmb_customer_count AS (
         SELECT xc.country,
                COUNT(*) AS xmb_customer_count
         FROM xmb_customers xc
         GROUP BY 1
     ),

     active_customers AS ( -- active customer count over the XMB launch period
         SELECT bs.country,
                sd.customer_id
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                           AND pd.is_mealbox = TRUE
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE bs.country IN ('AU', 'NZ')
           AND bs.hellofresh_delivery_week BETWEEN '2021-W42' AND '2021-W50'
         GROUP BY bs.country,
                  sd.customer_id
     ),

    active_customer_count AS (
        SELECT ac.country,
               COUNT(*) tot_cust_count
        FROM active_customers ac
        GROUP BY 1
    )

SELECT acc.country,
       acc.tot_cust_count,
       xcc.xmb_customer_count,
       xcc.xmb_customer_count/acc.tot_cust_count AS cust_uptake
FROM active_customer_count acc
JOIN xmb_customer_count xcc
ON acc.country = xcc.country;






-- % Customers that received both an xmas box & W52 menu
-- rephrase: % of XMB customers who also ordered in W52
-- XMB boxes shipped, LEFT JOIN the boxes shipped in 2021-W52 on customer_id
-- non_null values / total values

WITH xmb_customers AS (
    SELECT bs.country,
           sd.customer_id
    FROM fact_tables.boxes_shipped bs
             JOIN dimensions.product_dimension pd
                  ON bs.fk_product = pd.sk_product
                      AND pd.product_sku LIKE '%XMB%'
             JOIN dimensions.subscription_dimension sd
                  ON bs.fk_subscription = sd.sk_subscription
    WHERE bs.hellofresh_delivery_week >= '2021-W42'
      AND bs.country IN ('AU', 'NZ')
    GROUP BY 1, 2
),

     w52_customers AS (
         SELECT bs.country,
                sd.customer_id
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                           AND pd.is_mealbox = TRUE
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE bs.hellofresh_delivery_week = '2021-W52'
           AND bs.country IN ('AU', 'NZ')
         GROUP BY 1, 2
     )


SELECT xc.country,
       COUNT(xc.customer_id) AS xmb_cust_count,
       COUNT(wc.customer_id) AS w52_cust_count,
       COUNT(wc.customer_id)/COUNT(xc.customer_id) AS percent_retention
FROM xmb_customers xc
LEFT JOIN w52_customers wc
  ON xc.country = wc.country
  AND xc.customer_id = wc.customer_id
GROUP BY 1
ORDER BY 1;




-- % Customers that purchased an xmas box in 2021 who also purchased an xmas box in 2020 (and are still active)
-- boxes_shipped XMB 2021, LEFT JOIN boxes_shipped XMB 2020 on customer_id, LEFT JOIN boxes_shipped (overall latest week) on customer_id
-- non_null values / total

WITH active_customers AS (
    SELECT bs.country,
           sd.customer_id
    FROM fact_tables.boxes_shipped bs
             JOIN dimensions.product_dimension pd
                  ON bs.fk_product = pd.sk_product
                      AND pd.is_mealbox = TRUE
             JOIN dimensions.subscription_dimension sd
                 ON bs.fk_subscription = sd.sk_subscription
    WHERE bs.country IN ('AU', 'NZ')
      AND bs.hellofresh_delivery_week = '2022-W01' -- change this date to get the active customer list for a determined week
),

     xmb_customers_21 AS (
         SELECT bs.country,
                sd.customer_id
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                           AND pd.product_sku LIKE '%XMB%'
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE bs.country IN ('AU', 'NZ')
           AND bs.hellofresh_delivery_week >= '2021-W42'
     ),


     xmb_customers_20 AS (
         SELECT bs.country,
                sd.customer_id
         FROM fact_tables.boxes_shipped bs
                  JOIN dimensions.product_dimension pd
                       ON bs.fk_product = pd.sk_product
                           AND pd.product_sku LIKE '%XMB%'
                  JOIN dimensions.subscription_dimension sd
                       ON bs.fk_subscription = sd.sk_subscription
         WHERE bs.country IN ('AU', 'NZ')
           AND bs.hellofresh_delivery_week BETWEEN '2020-W42' AND '2021-W00'
     ),

  active_customers_repeat AS ( -- % of xmb21 customers who also ordered xmb20 box i.e. are repeat customers and are also active for the said week
      SELECT xc21.country,
             COUNT(xc21.customer_id)                           AS cust_count_xmb21,
             COUNT(xc20.customer_id)                           AS cust_count_xmb20,
             COUNT(xc20.customer_id) / COUNT(xc21.customer_id) AS repeat_cust_percent
      FROM xmb_customers_21 xc21
               INNER JOIN active_customers ac
                          ON xc21.country = ac.country
                              AND xc21.customer_id = ac.customer_id
               LEFT JOIN xmb_customers_20 xc20
                         ON xc21.country = xc20.country
                             AND xc21.customer_id = xc20.customer_id
      GROUP BY 1
      ORDER BY 1
  ),

  repeat_customers AS ( -- % of xmb21 customers who also ordered xmb20 box i.e. repeat customers
      SELECT xc21.country,
             COUNT(xc21.customer_id)                           AS cust_count_xmb21,
             COUNT(xc20.customer_id)                           AS cust_count_xmb20,
             COUNT(xc20.customer_id) / COUNT(xc21.customer_id) AS repeat_cust_percent
      FROM xmb_customers_21 xc21
               LEFT JOIN xmb_customers_20 xc20
                         ON xc21.country = xc20.country
                             AND xc21.customer_id = xc20.customer_id
      GROUP BY 1
      ORDER BY 1
  )

SELECT *
FROM repeat_customers;





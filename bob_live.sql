WITH all_sales_au AS (
    SELECT sku
         , soi.created_at
         , concat('W', week(soi.created_at, 6))  as week
         , sois.name
         , IF(LEFT(postcode, 1) = 6, 'PH', 'SY') as region
         , COUNT(fk_sales_order)                 as box_count
    FROM bob_live_au.sales_order_item soi
             JOIN sales_order_item_status sois on soi.fk_sales_order_item_status = sois.id_sales_order_item_status
             JOIN subscription s on soi.fk_subscription = s.id_subscription
             JOIN customer_address a on s.fk_customer_address_shipping = a.id_customer_address
    WHERE sku LIKE '%AU-XMB21-%'
    GROUP BY 1, 2, 3, 4, 5
)

SELECT asu.name,
       SUM(box_count)
FROM all_sales_au asu
WHERE name NOT IN ('invalid', 'refunded')
GROUP BY 1
ORDER BY 1;
-- ----------------------------------------------------
-- XMB chaos
-- NZ
SELECT COUNT(fk_sales_order) as actually_shipped
     , SUM(CASE WHEN sois.name NOT IN ('refunded','invalid') THEN 1 ELSE 0 END) as status_as_shipped
     , SUM(CASE WHEN sois.name = 'refunded' THEN 1 ELSE 0 END) as shipped_but_status_as_refunded
     -- , SUM(CASE WHEN sois.name = 'invalid' THEN 1 ELSE 0 END) as shipped_but_status_as_invalid
     , SUM(CASE WHEN sois.name = 'chargeback' THEN 1 ELSE 0 END) as shipped_but_status_as_chargeback
FROM bob_live_nz.sales_order_item soi
         JOIN bob_live_nz.subscription s on soi.fk_subscription = s.id_subscription
         JOIN bob_live_nz.customer_address a on s.fk_customer_address_shipping = a.id_customer_address
         LEFT JOIN  bob_live_nz.sales_order_item_status sois on soi.fk_sales_order_item_status = sois.id_sales_order_item_status
WHERE sku LIKE '%NZ-XMB21-%'
  AND soi.created_at > '2021-01-01'
  AND shipped = 1;

-- AU
SELECT COUNT(fk_sales_order) as actually_shipped
     , SUM(CASE WHEN sois.name NOT IN ('refunded','invalid') THEN 1 ELSE 0 END) as status_as_shipped
     , SUM(CASE WHEN sois.name = 'refunded' THEN 1 ELSE 0 END) as shipped_but_status_as_refunded
     -- , SUM(CASE WHEN sois.name = 'invalid' THEN 1 ELSE 0 END) as shipped_but_status_as_invalid
     , SUM(CASE WHEN sois.name = 'chargeback' THEN 1 ELSE 0 END) as shipped_but_status_as_chargeback
FROM bob_live_au.sales_order_item soi
         JOIN bob_live_au.subscription s on soi.fk_subscription = s.id_subscription
         JOIN bob_live_au.customer_address a on s.fk_customer_address_shipping = a.id_customer_address
         LEFT JOIN  bob_live_au.sales_order_item_status sois on soi.fk_sales_order_item_status = sois.id_sales_order_item_status
WHERE sku LIKE '%AU-XMB21-%'
  AND soi.created_at > '2021-01-01'
  AND shipped = 1;


-- getting customer_ids and order dates for all customers who ordered a Christmas Box (AU)

SELECT DATE(soi.created_at) AS date_backwards,
       s.fk_customer AS customer_id
FROM bob_live_au.sales_order_item soi
JOIN subscription s
 ON soi.fk_subscription = s.id_subscription
JOIN sales_order_item_status sois -- joining the status table to remove invalid and refunded orders
 ON soi.fk_sales_order_item_status = sois.id_sales_order_item_status
WHERE soi.sku LIKE '%AU-XMB21-%' -- using this to filter only Christmas Box 2021 Orders in AU
AND sois.name NOT IN ('invalid', 'refunded');


-- getting customer_ids and order dates for all customers who ordered a Christmas Box (NZ)

SELECT DATE(soi.created_at) AS date_backwards,
       s.fk_customer AS customer_id
FROM bob_live_nz.sales_order_item soi
         JOIN subscription s
              ON soi.fk_subscription = s.id_subscription
         JOIN sales_order_item_status sois -- joining the status table to remove invalid and refunded orders
              ON soi.fk_sales_order_item_status = sois.id_sales_order_item_status
WHERE soi.sku LIKE '%NZ-XMB21-%' -- using this to filter only Christmas Box 2021 Orders in NZ
  AND sois.name NOT IN ('invalid', 'refunded');



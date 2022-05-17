---- Emissions Tracking Dashboard (Sustainability)
-- the aim of this exercise is to calculate the emissions released when delivering a box - on a box level for every box
-- we are going to get the locations of 3 things: a. DC (hellofresh), depo (BeCool), and customer
-- we are going to get the lat and long of the locations
-- we are going to calculate the distance a box travels from the DC to the Customer (line haul & last mile) using python

-- trying to query the following 2 queries directly from python and then join the two tables on box_id (which is unique for every country)


-- getting the lat, long of customer
-- and the post code of the depo as well? have to confirm this?!
-- the following query runs in DWH
SELECT BS.hellofresh_delivery_week,
       CD.customer_id,
       BS.box_id,
       BS.country,
       DR.region_handle,
       DR.postal_code,
       GD.zipcode,
       GD.latitude  AS cus_lat,
       GD.longitude AS cus_lon
FROM fact_tables.boxes_shipped BS
         INNER JOIN dimensions.product_dimension PD
                    ON BS.fk_product = PD.sk_product
         INNER JOIN dimensions.customer_dimension CD
                    ON CD.sk_customer = BS.fk_customer
         LEFT JOIN dimensions.geo_dimension GD
                   ON GD.sk_geo = BS.fk_geo
         LEFT JOIN (
    WITH bc_depot AS (
        SELECT PC.postal_code,
               PC.delivery_region_id,
               DR.region_code AS country,
               DR.region_handle,
               PC.published_at_ms,
               ROW_NUMBER()
                       OVER (PARTITION BY PC.postal_code,DR.region_code ORDER BY PC.published_at_ms DESC) AS rownb
        FROM logistics_configurator.delivery_region_postal_code_latest PC
                 LEFT JOIN logistics_configurator.delivery_region_latest DR
                           ON DR.id = PC.delivery_region_id
        WHERE region_code IN ('AU', 'NZ', 'AO')
    )
    SELECT BD.postal_code,
           BD.country,
           BD.region_handle,
           BD.delivery_region_id,
           BD.published_at_ms
    FROM bc_depot BD
    WHERE rownb = 1) DR
                   ON DR.postal_code = GD.zipcode AND DR.country = GD.country
WHERE BS.country IN ('AU', 'AO', 'NZ')
  AND BS.box_shipped = 1
  -- AND PD.is_mealbox IS TRUE
  AND BS.hellofresh_delivery_week >= '2022-W01'
  AND region_handle != 'gift-cards'
ORDER BY 1, 2, 3, 4, 5;


-----------------------------------------------------------


-- getting the lat, long of DC (hard coding the lat long of the Hellofresh DCs)
-- removing the scale data table
-- the following query runs in RDS

WITH dc_loc AS (
SELECT DL.week,
       UPPER(DL.country)                                        AS country,
       DL.customer_id                                           AS customer_id,
       DL.id                                                    AS box_id,
       CASE WHEN DL.dc = 'Esky' THEN 2145
            WHEN DL.dc = 'Casa' THEN 6155
            WHEN DL.dc = 'Tuckerbox' THEN 3023
            WHEN DL.dc = 'Chilli Bin' THEN 1060
            ELSE '' END                                              AS dc_pc,
       DL.customer_postcode                                     AS cus_pc,
       CASE WHEN DL.dc = 'Esky' THEN -33.822487225090086
            WHEN DL.dc = 'Casa' THEN -32.06714937055341
            WHEN DL.dc = 'Tuckerbox' THEN -37.762661767980504
            WHEN DL.dc = 'Chilli Bin' THEN -36.901845231643364
            ELSE '' END                                              AS dc_lat,
       CASE WHEN DL.dc = 'Esky' THEN 150.91673936444454
            WHEN DL.dc = 'Casa' THEN 115.92537581546303
            WHEN DL.dc = 'Tuckerbox' THEN 144.7341119844085
            WHEN DL.dc = 'Chilli Bin' THEN 174.82138508541232
            ELSE '' END                                              AS dc_lon
FROM deliveries.delivery_list DL
WHERE DL.week >= '2022-W01'
ORDER BY 1,2,3
)

SELECT country,
       week,
       customer_id,
       COUNT (DISTINCT box_id)
FROM dc_loc
WHERE box_id IN ('AU36229294', 'AU36229295', 'AU36205005')
-- AND country = 'AU'
GROUP BY 1,2,3
ORDER BY 1,4 DESC;


-------------------------------------------------------------

-- Getting the weight of the boxes (from DWH)
-- will INNER JOIN this query to the dc location query from RDS on box_id - that way we will capture only the mealboxes and meal-addons
-- will then group by the customer_ids to get the weekly weight ordered per customer


WITH recipe_weight AS (
    SELECT r.country,
           r.hf_week,
           CASE
               WHEN r.market = 'Sydney' THEN 'Esky'
               WHEN r.market = 'Melbourne' THEN 'Tuckerbox'
               WHEN r.market = 'Perth' THEN 'Casa'
               WHEN r.market = 'Nz' THEN 'Chilli Bin'
               ELSE '' END AS dc,                       -- e.g. Esky has diff weight/suppliers than Casa
           r.recipe_slot,
           SUM(r.weight * r.qty_2ppl * r.supplier_split) AS weight_2p_g,
           SUM(r.weight * r.qty_4ppl* r.supplier_split) AS weight_4p_g
    FROM uploads.pa_anz__recipe r
    WHERE 1 = 1
      AND r.country IN ('AU', 'NZ', 'AO')
      AND r.hf_week = '2022-W01'
    GROUP BY 1, 2, 3, 4
),

     test_table AS (
         SELECT ro.country,
                ro.hellofresh_week AS hf_week,
                ro.box_id,
                sd.customer_id,
                pd.box_size,
                pd.number_of_recipes,
                CASE
                    WHEN CAST(pd.box_size AS INT) = 2 THEN SUM(rw.weight_2p_g / 1000)
                    WHEN CAST(pd.box_size AS INT) = 4 THEN SUM(rw.weight_4p_g / 1000)
                    WHEN CAST(pd.box_size AS INT) = 6
                        THEN SUM((rw.weight_2p_g + rw.weight_4p_g) / 1000) -- EP usually has 6P boxes
                    END            AS box_weight_kg
         FROM fact_tables.recipes_ordered ro
                  INNER JOIN dimensions.subscription_dimension sd
                             ON sd.sk_subscription = ro.fk_subscription
                                 AND sd.country = ro.country
                  INNER JOIN dimensions.product_dimension pd
                             ON ro.fk_product = pd.sk_product
                  INNER JOIN fact_tables.boxes_shipped bs
                             ON ro.box_id = bs.box_id

                  LEFT JOIN uploads.pa_anz__hfm_scm_mappings addon_index
                            ON addon_index.country =
                               ro.country
                                AND addon_index.product_name = pd.product_name
                                AND addon_index.dwh_index = ro.recipe_index

                  INNER JOIN uploads.pa_anz_customer_dc_au dc
                             ON dc.customer_id = sd.customer_id
                                 AND dc.country = sd.country
                                 AND dc.hellofresh_week = ro.hellofresh_week
                  INNER JOIN recipe_weight rw
                             ON rw.country = ro.country
                                 AND rw.hf_week = ro.hellofresh_week
                                 AND ((rw.recipe_slot =
                                       addon_index.pythia_index
                                     AND ro.is_menu_addon IS TRUE)
                                     OR
                                      (rw.recipe_slot = ro.recipe_index))
                                 AND rw.dc = dc.dc
                                 AND ro.country = dc.country
         WHERE 1 = 1
           AND ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week >= '2022-W01'
           AND (pd.box_size IN ('2','4','6') OR RO.is_menu_addon)
         GROUP BY 1, 2, 3, 4, 5, 6)

SELECT country,
       hf_week,
       customer_id,
       COUNT(*)
FROM test_table tt
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY 1,2,4 DESC;


----- commented version of the code is given below:


WITH recipe_weight AS (
    SELECT r.country,
           r.hf_week,
           CASE
               WHEN r.market = 'Sydney' THEN 'Esky'
               WHEN r.market = 'Melbourne' THEN 'Tuckerbox'
               WHEN r.market = 'Perth' THEN 'Casa'
               WHEN r.market = 'Nz' THEN 'Chilli Bin'
               ELSE '' END AS dc,                       -- e.g. Esky has diff weight/suppliers than Casa
           r.recipe_slot,
           SUM(r.weight * r.qty_2ppl * r.supplier_split) AS weight_2p_g,
           SUM(r.weight * r.qty_4ppl* r.supplier_split) AS weight_4p_g
    FROM uploads.pa_anz__recipe r
    WHERE 1 = 1
      AND r.country IN ('AU', 'NZ', 'AO')
      AND r.hf_week = '2022-W01'
    GROUP BY 1, 2, 3, 4
),

     test_table AS (
         SELECT ro.country,
                ro.hellofresh_week AS hf_week,
                ro.box_id,
                sd.subscription_id, -- will be used to aggregate over in python to get weight ordered per customer
                pd.box_size,        -- can be removed as not really required
                pd.number_of_recipes,
                CASE
                    WHEN CAST(pd.box_size AS INT) = 2 THEN SUM(rw.weight_2p_g / 1000)
                    WHEN CAST(pd.box_size AS INT) = 4 THEN SUM(rw.weight_4p_g / 1000)
                    WHEN CAST(pd.box_size AS INT) = 6
                        THEN SUM((rw.weight_2p_g + rw.weight_4p_g) / 1000) -- EP usually has 6P boxes
                    END            AS box_weight_kg
         FROM fact_tables.recipes_ordered ro
                  INNER JOIN dimensions.subscription_dimension sd -- using this to get customer_id, which will be used to connect to pa_anz_customer_dc_au to get the DC
                             ON sd.sk_subscription = ro.fk_subscription
                                 AND sd.country = ro.country
                  INNER JOIN dimensions.product_dimension pd -- to get the box_size
                             ON ro.fk_product = pd.sk_product
                  INNER JOIN fact_tables.boxes_shipped bs -- to get the geo of the box and link to gd (to get the
                             ON ro.box_id = bs.box_id

                  LEFT JOIN uploads.pa_anz__hfm_scm_mappings addon_index -- using this to get a pythia_index (50+) for add-ons - will give a NULL for core
                            ON addon_index.country =
                               ro.country -- this step is required as ro has same recipe indices for core and addons
                                AND addon_index.product_name =
                                    pd.product_name -- joining on the product names will ensure that only the addons get joined
                                AND addon_index.dwh_index = ro.recipe_index

                  INNER JOIN uploads.pa_anz_customer_dc_au dc -- this has the dc per customer -- waiting for the new table to be uploaded
                             ON dc.customer_id = sd.customer_id
                                 AND dc.country = sd.country
                                 AND dc.hellofresh_week = ro.hellofresh_week
                  INNER JOIN recipe_weight rw
                             ON rw.country = ro.country
                                 AND rw.hf_week = ro.hellofresh_week
                                 AND ((rw.recipe_slot =
                                       addon_index.pythia_index -- this connects rw.recipe_slot to pythia_index if the recipe is an addon
                                     AND ro.is_menu_addon IS TRUE)
                                     OR
                                      (rw.recipe_slot = ro.recipe_index)) -- otherwise it connects the rw.recipe_slot to the ro.recipe_index (for core)
                                 AND rw.dc = dc.dc
                                 AND ro.country = dc.country
         WHERE 1 = 1
           AND ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week = '2022-W01'
           AND (pd.box_size IN ('2','4','6') OR RO.is_menu_addon) -- to ensure that both mealboxes and addons are included
         GROUP BY 1, 2, 3, 4, 5, 6)

SELECT country,
       hf_week,
       subscription_id,
       COUNT(*)
FROM test_table tt
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY 1,2,4 DESC;


ORDER BY 1,2,3,4,5,6




--   geo_markets AS (  -- to map the "markets" to the sk_geo (from boxes_shipped)
--       SELECT gd.sk_geo,
--              CASE
--                  WHEN gd.region_1 IN
--                       ('New South Wales', 'Australian Capital Territory', 'Northern Territory', 'Queensland')
--                      THEN 'Sydney'
--                  WHEN gd.region_1 IN ('Victoria', 'South Australia') THEN 'Melbourne'
--                  WHEN gd.region_1 IN ('Western Australia') THEN 'Perth'
--                  ELSE 'Nz' END AS market
--       FROM dimensions.geo_dimension gd -- to get the region1 of the box - link to "market"
--       WHERE gd.country IN ('AU', 'NZ', 'AO')
--   )





-- getting the lat, long of DC (hard coding the lat long of the Hellofresh DCs)
-- without removing the scale data table
-- the following query runs in RDS
SELECT SC.week,
       UPPER(DL.country)                                        AS country,
       DL.id                                                    AS box_id,
       SC.Weight,
       CASE WHEN DL.dc = 'Esky' THEN 2145
            WHEN DL.dc = 'Casa' THEN 6155
            WHEN DL.dc = 'Tuckerbox' THEN 3023
            WHEN DL.dc = 'Chilli Bin' THEN 1060
            ELSE '' END                                              AS dc_pc,
       DL.customer_postcode                                     AS cus_pc,
       CASE WHEN DL.dc = 'Esky' THEN -33.822487225090086
            WHEN DL.dc = 'Casa' THEN -32.06714937055341
            WHEN DL.dc = 'Tuckerbox' THEN -37.762661767980504
            WHEN DL.dc = 'Chilli Bin' THEN -36.901845231643364
            ELSE '' END                                              AS dc_lat,
       CASE WHEN DL.dc = 'Esky' THEN 150.91673936444454
            WHEN DL.dc = 'Casa' THEN 115.92537581546303
            WHEN DL.dc = 'Tuckerbox' THEN 144.7341119844085
            WHEN DL.dc = 'Chilli Bin' THEN 174.82138508541232
            ELSE '' END                                              AS dc_lon
FROM deliveries.delivery_list DL
         LEFT JOIN production.scaledata SC ON SC.BoxId = DL.id
WHERE DL.week >= '2022-W01';
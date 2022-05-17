-- Query to calculate the NPS score that matches with the Tableau dashboard
-- The tableau dashboard can be accessed @ https://tableau.hellofresh.io/#/views/NPS_0/NPSOverview?:iid=1

WITH count_table AS (
    SELECT nps.entity_code,
       nps.survey_type,
       nps.submitted_month,
       --nps.submitted_month_name,
       SUM(CASE WHEN nps.nps_group = 'Promoter' THEN 1 ELSE 0 END) AS promoter_count,
       SUM(CASE WHEN nps.nps_group = 'Detractor' THEN 1 ELSE 0 END) AS detractor_count,
       SUM(CASE WHEN nps.nps_group = 'Neutral' THEN 1 ELSE 0 END) AS neutral_count
    FROM materialized_views.isa_nps AS nps
    WHERE 1=1
        AND nps.entity_code IN ('HF-AU', 'HF-NZ', 'EP-AU')
        AND nps.submitted_month >= '2018-M01'
        --AND is_mealbox = TRUE
    GROUP BY 1,2,3)

SELECT entity_code,
       survey_type,
       submitted_month,
       --submitted_month_name,
       (promoter_count - detractor_count)/(promoter_count+neutral_count+detractor_count) AS nps_score
FROM count_table
WHERE submitted_month >= '2021-M12'
ORDER BY 1,2,3;










-- query for selecting top 10 categories in promoters/detractors of NPS for AU/NZ/EP initial & recurring

WITH category_count AS (
    SELECT ncc.entity AS entity,
           ncc.survey_type AS survey_type,
           ncc.submitted_month AS hf_month,
           ncc.nps_group AS nps_type,
           ncc.level_4 AS category,
           COUNT(*) AS freq
    FROM materialized_views.isa_nps_comment_categorizations AS ncc
    WHERE 1=1
        AND ncc.country IN ('AU', 'NZ', 'AO') -- AO for EP
        AND ncc.submitted_month BETWEEN '2021-M12' AND '2022-M01' -- use this part to change the date period
        AND ncc.level_4 IS NOT NULL
    GROUP BY 1,2,3,4,5)
,
  category_rank AS ( -- ranking the categories in terms of freq
      SELECT entity,
             survey_type,
             hf_month,
             nps_type,
             category,
             freq,
             RANK() OVER (
            PARTITION BY
                entity,
                survey_type,
                hf_month,
                nps_type
            ORDER BY freq DESC) AS category_ranking,
              SUM(freq) OVER (
            PARTITION BY
                entity,
                survey_type,
                hf_month,
                nps_type
        ) AS total_customer
      FROM category_count
      ),

     top_ten_categories AS ( -- just taking out the top 10 categories
         SELECT *,
                 (cr.freq / cr.total_customer) AS per
          FROM category_rank AS cr
          WHERE cr.category_ranking <= 10

         ),


     date_filtered AS ( -- using this to filter out data for the required time period
         SELECT *
         FROM materialized_views.isa_nps_comment_categorizations AS ncc
         WHERE ncc.submitted_month BETWEEN '2021-M06' AND '2021-M12'
     )


SELECT *
FROM top_ten_categories
ORDER BY 1, 2, 3, 4, 7; -- use this part to get the top 10 categories for all three entities
;


SELECT ttc.category, --getting the comments
       df.`comment`
FROM date_filtered df
JOIN top_ten_categories ttc
  ON df.entity = ttc.entity
  AND df.level_4 = ttc.category
  AND df.survey_type = ttc.survey_type
  AND df.nps_group = ttc.nps_type
WHERE df.country = 'NZ' -- change this to AU/NZ/AO to toggle between the three entities
    AND df.nps_group = 'Promoters' -- change this to Promoters/Detractors to toggle between the two
  AND df.survey_type = 'initial' -- change this to initial/recurring to toggle between the two NPS types
ORDER BY 1;




-- query to count the most frequent promoters/detractors (category) per country
-- can also count the frequency of promoters/detractors per box per country
-- (filtered by negative or positive sentiment)

    WITH date_filtered AS (
    SELECT *
    FROM materialized_views.isa_nps_comment_categorizations AS ncc
    WHERE ncc.delivery_hellofresh_week >= '2021-W17' AND
            ncc.country IN ('AU', 'NZ')
      AND ncc.tendency LIKE 'Pos%'),


     detractor_count AS (
         SELECT df.country AS COUNTRY,
                df.level_4  AS promoter,
                COUNT(*) AS promoter_freq
         FROM date_filtered df
         WHERE product_name_first_box LIKE 'Class%' /* add the type of box here */
         GROUP BY 1, 2),


     detractor_ranked AS (
         SELECT dc.country,
                dc.promoter,
                dc.promoter_freq,
                ROW_NUMBER() OVER (PARTITION BY country ORDER BY dc.promoter_freq DESC) AS ranking
         FROM detractor_count dc),

     top_detractors AS (
         SELECT dr.country,
                dr.promoter,
                dr.promoter_freq,
                dr.ranking
         FROM detractor_ranked dr
         WHERE ranking <= 10)


SELECT df.delivery_hellofresh_week AS hf_week, --getting the comments
       df.country,
       df.level_4 AS promoter,
       df.`comment`
FROM date_filtered df
         JOIN top_detractors td
              ON df.level_4 = td.promoter AND
                 df.country = td.country
WHERE df.product_name_first_box LIKE 'Class%' AND /* add the type of box here */
        df.country = 'NZ'
ORDER BY 1,3;



-- Downloading all the 'negative' comments from 2021-M01 onwards
SELECT ncc.comment,
       ncc.categorized_comments AS category
FROM materialized_views.isa_nps_comment_categorizations AS ncc
WHERE 1=1
AND ncc.entity = 'HF-AU'
AND ncc.submitted_month >= '2021-M01'
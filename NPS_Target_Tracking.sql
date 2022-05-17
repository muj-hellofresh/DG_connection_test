-- Query to calculate the NPS score that matches with the Tableau dashboard
-- The tableau dashboard can be accessed @ https://tableau.hellofresh.io/#/views/NPS_0/NPSOverview?:iid=1

WITH count_table AS (
    SELECT nps.entity_code,
           nps.survey_type,
           nps.latest_delivery_before_submission_month AS hf_month,
           --nps.submitted_month_name,
           SUM(CASE WHEN nps.nps_group = 'Promoter' THEN 1 ELSE 0 END) AS promoter_count,
           SUM(CASE WHEN nps.nps_group = 'Detractor' THEN 1 ELSE 0 END) AS detractor_count,
           SUM(CASE WHEN nps.nps_group = 'Neutral' THEN 1 ELSE 0 END) AS neutral_count
    FROM materialized_views.isa_nps AS nps
    WHERE 1=1
      AND nps.entity_code IN ('HF-AU', 'HF-NZ', 'EP-AU')
      AND nps.latest_delivery_before_submission_month >= '2018-M01'
      --AND is_mealbox = TRUE
    GROUP BY 1,2,3)

SELECT entity_code,
       survey_type,
       hf_month,
       --submitted_month_name,
       (promoter_count - detractor_count)/(promoter_count+neutral_count+detractor_count) AS nps_score
FROM count_table
WHERE 1=1
AND hf_month >= '2021-M01'
AND entity_code = 'HF-AU'
AND survey_type = 'initial'

ORDER BY 1,2,3;

----------
-- exporting data for overall NPS
-- both iNPS and rNPS for all three markets (AU, NZ, AO) - starting from 2021-M01
-- data drill down at both quarterly and monthly levels
-- will be exporting the number of promoters, detractors, and neutrals into a GSheet
-- and calculating the NPS score there (or in the GDS dashboard)

SELECT IF(nps.country = 'AO', 'EP', nps.country) AS country,
       LEFT(nps.latest_delivery_before_submission_quarter,4) AS hf_year,
       nps.latest_delivery_before_submission_quarter AS hf_quarter,
       nps.latest_delivery_before_submission_month AS hf_month, -- calculating from the latest delivery before submission
       nps.survey_type, -- inital or recurring
       nps.nps_group,
       COUNT(DISTINCT nps.response_id) AS response_count -- calculating the distinct responses in each nps_group
FROM materialized_views.isa_nps nps
WHERE 1=1
AND nps.country IN ('AU', 'NZ', 'AO')
AND nps.latest_delivery_before_submission_month >= '2021-M01'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6
;

-- NPS per team/category
-- doing a left join on isa_nps to get the comments categories against the customers who left a comment in the first place
-- how do we calculate how many people responded (response rate) - something to do with boxes_shipped? - need to get this from marketing
-- using "latest deliver week/month/quarter before submission" column
-- counting distinct response_ids in each nps_group for NPS calculation
-- assigning the teams_accountable according to the following category assignment (NPS Categorization tab)
-- https://docs.google.com/spreadsheets/d/1US5Enq5v1w2ATIx5_Vq7A6Y3SeP-Q617UAqoEccfaIk/edit#gid=527252117

WITH nps_teams AS (
    SELECT IF(nps.country = 'AO', 'EP', nps.country) AS country,
           LEFT(nps.latest_delivery_before_submission_quarter,4) AS hf_year,
           nps.latest_delivery_before_submission_quarter AS hf_quarter,
           nps.latest_delivery_before_submission_month AS hf_month,
           CONCAT(RIGHT(nps.latest_delivery_before_submission_month,2),'-',LEFT(nps.latest_delivery_before_submission_month,4)) AS delivery_month,
           nps.survey_type,
           nps.nps_group,
           CASE
               WHEN ncc.level_2 = 'Ingredients (procurement)' THEN 'Ops - Procurement'
               WHEN ncc.level_2 = 'Logistics' THEN 'Ops - Logistics'
               WHEN ncc.level_2 = 'Pick & Pack' THEN 'Ops - Production'
               WHEN (ncc.level_3 = 'Product Offering' AND ncc.level_4 NOT IN ('Meal Choice')) OR ncc.level_2 = 'Price/Value' THEN 'Products'
               WHEN ncc.level_3 = 'Product Offering' AND ncc.level_4 = 'Meal Choice' THEN 'Menu Planning'
               WHEN ncc.level_2 = 'Recipes' OR ncc.level_2 = 'Customer experience' THEN 'Culinary'
               WHEN ncc.level_2 = 'Tech Product' THEN 'Tech & Design'
               WHEN ncc.level_2 = 'Marketing' THEN 'Marketing'
               WHEN ncc.level_2 = 'Business Model' OR ncc.level_2 = 'Other' THEN 'CX'
               WHEN ncc.level_2 = 'Customer Service' THEN 'Customer Care'
               WHEN ncc.level_3 = 'Ecological Issues' THEN 'Sustainability'
               ELSE 'No Category'
            END AS team_accountable,
           ncc.level_4 AS category,
           ncc.tendency,
           COUNT(DISTINCT nps.response_id) AS response_count

    FROM materialized_views.isa_nps nps
             LEFT JOIN materialized_views.isa_nps_comment_categorizations ncc
                       ON nps.country = ncc.country
                           AND ncc.country IN ('AU', 'NZ', 'AO')
                           AND nps.latest_delivery_before_submission_week = ncc.latest_delivery_before_submission_week
                           AND nps.response_id = ncc.response_id
                           AND nps.survey_type = ncc.survey_type
    WHERE 1 = 1
      AND nps.country IN ('AU', 'NZ', 'AO')
      AND nps.latest_delivery_before_submission_week >= '2021-W01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT *
FROM nps_teams
ORDER BY 1, 2, 3, 4,5,6,7,8,9,10;





-- calculating the number of customers who received their first box per week
-- use this to calculate the response rate for the iNPS

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4)

SELECT IF(clwe.country = 'AO','EP',clwe.country) AS country,
       d.hf_year,
       d.hf_quarter,
       d.hf_month,
       COUNT(*) AS first_box_count
FROM materialized_views.mbi_anz_customer_loyalty_weekly_extended clwe
JOIN dates d
ON clwe.hf_week = d.hellofresh_week
WHERE clwe.hf_week >= '2021-W01'
  AND clwe.box_count = 1
  AND clwe.boxes_shipped = 1
  AND clwe.country IN ('AO', 'NZ', 'AU')
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;



-------
-- calculating the number of comments per month/quarter
-- this should be less than the response count as not all responses leave a comment

SELECT IF(ncc.country = 'AO', 'EP', ncc.country) AS country,
       ncc.latest_delivery_before_submission_quarter AS hf_quarter,
       ncc.latest_delivery_before_submission_month AS hf_month,
       ncc.survey_type,
       ncc.nps_group,
       COUNT(DISTINCT ncc.response_id) AS response_count
FROM materialized_views.isa_nps_comment_categorizations ncc
WHERE 1 = 1
  AND ncc.country IN ('AU', 'NZ', 'AO')
  AND ncc.latest_delivery_before_submission_week >= '2021-W01'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;
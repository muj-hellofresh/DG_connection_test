-- Find the Top (5) cancellation reasons per customer preferences
-- tables of interest:
-- fact_tables.cancellation_survey

WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     cancellation_reason AS ( -- getting all the cancellation reasons in 2021
         SELECT cs.country,
                dd.hellofresh_week,
                sd.customer_id,
                CASE
                    WHEN cs.answer IN ('too-expensive', 'It is out of my budget')
                        THEN 'Budget'
                    WHEN cs.answer IN ('trial', 'I had a trial')
                        THEN 'Trial'
                    WHEN cs.answer IN ('delivery-weekly', 'I do not want a delivery every week')
                        THEN 'Delivery Frequency'
                    WHEN cs.answer LIKE 'recipe-variety' OR cs.answer LIKE 'Recipes%'
                        THEN 'Recipes'
                    WHEN cs.answer IN ('quantity-packaging', 'There is too much packaging')
                        THEN 'Packaging Quantity'
                    WHEN cs.answer IN ('delivery-content-issues', 'I had issues with my deliveries or contents',
                                       'ingredients-not-separated')
                        THEN 'Delivery/Content Issues'
                    WHEN cs.answer IN ('difficult-manage-account', 'It is too difficult to manage my account')
                        THEN 'User Experience (Account)'
                    WHEN cs.answer IN
                         ('failed payment', 'CC-Failed-Payments', 'I had issues with my payment or offers', 'payment')
                        THEN 'Payment'
                    WHEN cs.answer IN ('box-size-meals', 'The size of the box or number of meals does not work for me')
                        THEN 'Meal Size'
                    WHEN cs.answer IN ('I am using other meal kit providers', 'other-providers')
                        THEN 'Switch to Competitor'
                    WHEN cs.answer IN ('It is time consuming')
                        THEN 'Time Consuming'
                    ELSE cs.answer
                    END AS cancellation_reason
         FROM fact_tables.cancellation_survey cs
                  JOIN dimensions.date_dimension dd
                       ON cs.fk_submitted_date = dd.sk_date
                           -- AND dd.hellofresh_week BETWEEN '2021-W00' AND '2022-W00' -- need to update this in Robot to allow for automation
                  JOIN dimensions.subscription_dimension sd
                       ON cs.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND cs.country IN ('AU', 'NZ')
           AND cs.answer_type NOT IN ('bulk', 'textarea')
         GROUP BY 1, 2, 3, 4
     ),

     cust_pref AS ( -- getting the customer preferences
         SELECT ro.country,
                ro.hellofresh_week,
                sd.customer_id,
                ro.subscription_preference_name
         FROM fact_tables.recipes_ordered ro
                  JOIN dimensions.subscription_dimension sd
                       ON ro.fk_subscription = sd.sk_subscription
         WHERE 1 = 1
           AND ro.country IN ('AU', 'NZ')
           AND ro.hellofresh_week >= '2021-W01'
           AND ro.is_menu_addon = FALSE
           AND ro.subscription_preference_name NOT IN ('', 'express')
         GROUP BY 1, 2, 3, 4
     ),

  cancellation_count AS ( -- counting the cancellation reason freq per customer preference
      SELECT cr.country,
             d.hf_year,
             d.hf_quarter,
             --cr.hellofresh_month,
             --cr.hellofresh_week,
             --cr.customer_id,
             cp.subscription_preference_name AS cust_subscription,
             cr.cancellation_reason,
             COUNT(*)                        AS freq
      FROM cancellation_reason cr
               JOIN cust_pref cp
                    ON cr.country = cp.country
                        AND cr.hellofresh_week = cp.hellofresh_week
                        AND cr.customer_id = cp.customer_id
               JOIN dates d
                    ON cr.hellofresh_week = d.hellofresh_week
      GROUP BY 1, 2, 3, 4,5--,6,7
  )

SELECT *
FROM cancellation_count
ORDER BY 1,2,3,4,5;




/*
  cancellations_ranked AS ( -- ranking the cancellation reasons according to their frequencies
      SELECT cc.country,
             cc.cust_subscription,
             cc.cancellation_reason,
             cc.freq,
             RANK() OVER (PARTITION BY cc.country, cc.cust_subscription ORDER BY cc.freq DESC) AS ranking
      FROM cancellation_count cc
  ) */


-- pause reasons query shared by Nick

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
                  and country in ('AU')     -- to get rid of false country values
                  and split_part(event_label, '|', 4) >=  '2020-W01'
                  -- to look for data of a specific week, week in which the user triggered the pause action, and NOT the week paused
                group by 1, 2, 3, 4)
SELECT
    paused_week
     ,reason
     ,count(distinct uid) as customers
FROM pauses
GROUP BY 1,2
order by 1,2,3 desc;



-- trying to get the category for textarea and bulk type cancellation comments

SELECT cscc.fk_subscription,
       dd.hellofresh_week,
       COUNT(*)
FROM fact_tables.cancellation_survey_comment_categories cscc
INNER JOIN dimensions.date_dimension dd
ON cscc.fk_imported_at = dd.sk_date
WHERE cscc.country = 'AU'
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY 2 DESC;




WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     cancel_answer AS (
         SELECT cs.country,
                dd.hellofresh_week,
                cs.fk_subscription,
                cs.answer_type,
                cs.answer
         FROM fact_tables.cancellation_survey cs
                  INNER JOIN dimensions.date_dimension dd
                             ON cs.fk_submitted_date = dd.sk_date
         WHERE 1 = 1
           AND cs.country IN ('AU', 'NZ')
           AND cs.answer_type IN ('bulk', 'textarea')
           AND dd.hellofresh_week >= '2021-W01'
         GROUP BY 1, 2, 3, 4, 5
     ),

     cancel_category AS (
         SELECT cscc.country,
                dd.hellofresh_week,
                cscc.fk_subscription,
                ccd.level_1,
                ccd.level_2,
                ccd.level_3,
                ccd.level_4
         FROM fact_tables.cancellation_survey_comment_categories cscc
                  INNER JOIN dimensions.date_dimension dd
                             ON cscc.fk_imported_at = dd.sk_date
                  INNER JOIN dimensions.comment_category_dimension ccd
                             ON cscc.fk_comment_category = ccd.sk_comment_category
         WHERE cscc.country IN ('AU', 'NZ')
           AND dd.hellofresh_week >= '2021-W01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7
     )

SELECT ca.country,
       d.hf_quarter,
       cc.level_2,
       cc.level_3,
       cc.level_4,
       ca.answer
       --COUNT(ca.fk_subscription) AS cancel_count
FROM cancel_answer ca
INNER JOIN cancel_category cc
ON ca.country = cc.country
AND ca.hellofresh_week = cc.hellofresh_week
AND ca.fk_subscription = cc.fk_subscription
INNER JOIN dates d
ON ca.hellofresh_week = d.hellofresh_week
--GROUP BY 1,2,3--,4,5
ORDER BY 1,2,3,4,5-- DESC;



SELECT ccd.level_1,
       ccd.level_2,
       ccd.level_3,
       LOWER(ccd.level_4)
FROM dimensions.comment_category_dimension ccd
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;



    CASE
                    WHEN cs.answer IN ('too-expensive', 'It is out of my budget')
                        THEN 'Budget'
                    WHEN cs.answer IN ('trial', 'I had a trial')
                        THEN 'Trial'
                    WHEN cs.answer IN ('delivery-weekly', 'I do not want a delivery every week')
                        THEN 'Delivery Frequency'
                    WHEN cs.answer LIKE 'recipe-variety' OR cs.answer LIKE 'Recipes%'
                        THEN 'Recipes'
                    WHEN cs.answer IN ('quantity-packaging', 'There is too much packaging')
                        THEN 'Packaging Quantity'
                    WHEN cs.answer IN ('delivery-content-issues', 'I had issues with my deliveries or contents',
                                       'ingredients-not-separated')
                        THEN 'Delivery/Content Issues'
                    WHEN cs.answer IN ('difficult-manage-account', 'It is too difficult to manage my account')
                        THEN 'User Experience (Account)'
                    WHEN cs.answer IN
                         ('failed payment', 'CC-Failed-Payments', 'I had issues with my payment or offers', 'payment')
                        THEN 'Payment'
                    WHEN cs.answer IN ('box-size-meals', 'The size of the box or number of meals does not work for me')
                        THEN 'Meal Size'
                    WHEN cs.answer IN ('I am using other meal kit providers', 'other-providers')
                        THEN 'Switch to Competitor'
                    WHEN cs.answer IN ('It is time consuming')
                        THEN 'Time Consuming'
                    ELSE cs.answer
END AS cancellation_reason
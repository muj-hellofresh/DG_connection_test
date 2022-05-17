-- SWAP RATE
-- use swap rate of the 'following week' to reflect the appeal of the menu
-- the idea is that if a person saw the menu in a week (i.e. swapped) and the menu was appealing enough, they would look
-- at the menu (i.e. swap) the next time they place an order as well
-- what we are calculating, therefore, is the swap rate of the following week and use this as a metric for menu appeal
-- for the current week


WITH dates AS (-- to attach year/quarter/month to every week
    SELECT hellofresh_week,
           CAST(hellofresh_year AS STRING)                                  as hf_year,
           CONCAT(CAST(hellofresh_year AS STRING), '-M',
                  LPAD(CAST(hellofresh_month AS STRING), 2, '0'))           as hf_month,
           CONCAT(CAST(hellofresh_year AS STRING), '-', hellofresh_quarter) as hf_quarter
    FROM dimensions.date_dimension dd
    WHERE dd.year >= 2018
    GROUP BY 1, 2, 3, 4),

     swap_current_week AS ( -- this gives one row per customer per week and whether they swapped or not
         SELECT IF(ro.country = 'AO', 'EP', ro.country) AS country,
                d.hf_quarter,
                d.hf_month,
                ro.hellofresh_week,
                ro.fk_subscription,
                CASE
                    WHEN ro.reason = 'user-selection' THEN 1
                    ELSE 0 END AS swap
         FROM fact_tables.recipes_ordered ro
            JOIN dates d
            ON ro.hellofresh_week = d.hellofresh_week
         WHERE 1 = 1
           AND ro.country IN ('AU', 'NZ', 'AO')
           AND ro.hellofresh_week >= '2021-W01'
         GROUP BY 1, 2, 3, 4, 5, 6
     ),

     swap_following_week AS ( -- same as swap_current_week but 2 additional columns
         SELECT *,
                LEAD(hellofresh_week) OVER(PARTITION BY fk_subscription ORDER BY hellofresh_week) AS next_order_week, -- week of the next order by this customer
                LEAD(swap) OVER(PARTITION BY fk_subscription ORDER BY hellofresh_week) AS next_order_swap -- whether the customer "swapped" in the next time they ordered
         FROM swap_current_week
     )

SELECT country,
       hf_quarter,
       hf_month,
       hellofresh_week, -- aggregating over the current week/month
       SUM(next_order_swap) AS total_swaps,
       COUNT(next_order_swap) AS total_customers
FROM swap_following_week
WHERE swap = 1
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

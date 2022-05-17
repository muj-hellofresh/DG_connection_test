------- Global Dashboard Audit

-- use the mat_view box_level_contribution (box level metrics) to calculate the PC2% - compare the figure with the global dashboard figures
-- also perform an audit of the data integrity in the table by observing the outliers

-- use the mat_view incremental_profit (recipe level surcharges) to calculate PC2% on recipe surcharge level
-- audit the data integrity by observing outliers
-- compare PC2% (and other metrics if any) with global dashboard figures

SELECT blpc.country,
       blpc.hellofresh_week,
       sd.customer_id,
       SUM(blpc.total_revenue_loc - blpc.ingredients_total_cost) / SUM(blpc.total_revenue_loc) AS PC1,
       SUM(blpc.total_revenue_loc - blpc.total_cost) / SUM(blpc.total_revenue_loc)             AS PC2
FROM materialized_views.box_level_profit_contribution blpc
JOIN dimensions.product_dimension pd
                  ON blpc.fk_product = pd.sk_product
                      AND pd.is_mealbox = TRUE
JOIN dimensions.subscription_dimension sd
                  ON blpc.fk_subscription = sd.sk_subscription
WHERE 1 = 1
AND blpc.country IN ('AU', 'NZ')
AND blpc.hellofresh_week >= '2021-W01'
GROUP BY 1, 2, 3
ORDER BY 1,2,3;



-- use the mat_view incremental_profit (recipe level surcharges) to calculate PC2% on recipe surcharge level
-- audit the data integrity by observing outliers
-- compare PC2% (and other metrics if any) with global dashboard figures


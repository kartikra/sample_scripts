SELECT revenue_year,revenue_month,signup_type,segment, 
SUM(revenue_net_monthly) AS current_year_net_revenue, 
SUM(prior_year_revenue_net_monthly) AS previous_year_net_reveune
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND prior_year_revenue_net > 0
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;


SELECT revenue_year,revenue_month,signup_type,segment, 
SUM(revenue_net_monthly) AS current_year_net_revenue, 
SUM(prior_year_revenue_net_monthly) AS previous_year_net_reveune
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND prior_year_revenue_net > 0 AND prior_year_total_months=12
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;


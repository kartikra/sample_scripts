select 
ROUND(Enroll_Month/100) AS enroll_year
,MOD(Enroll_Month,100) AS enroll_month
,signup_type
,segment
,sum(gross_revenue_M13_to_24) gross_revenue_M13_to_24
,sum(gross_revenue_M1_to_12) gross_revenue_M1_to_12
,sum(revenue_net_M13_to_24) revenue_net_M13_to_24
,sum(revenue_net_M1_to_12) revenue_net_M1_to_12
-- ,sum(revenue_net_M25_to_36)
-- ,sum(revenue_net_M37_to_48)
-- ,sum(revenue_net_GT_48)

-- ,sum(gross_revenue_M25_to_36)
-- ,sum(gross_revenue_M37_to_48)
-- ,sum(gross_revenue_GT_48)
FROM
   (
	select Account_Id,revenue_month,signup_type,segment,Enroll_Month

	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 1 and 100 THEN revenue_net END),0) As revenue_net_M1_to_12
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 101 and 200 THEN revenue_net END),0) As revenue_net_M13_to_24
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 201 and 300 THEN revenue_net END),0) As revenue_net_M25_to_36
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 301 and 400 THEN revenue_net END),0) As revenue_net_M37_to_48
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month > 400 THEN revenue_net END),0) As revenue_net_GT_48

	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 1 and 100 THEN gross_revenue END),0) As gross_revenue_M1_to_12
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 101 and 200 THEN gross_revenue END),0) As gross_revenue_M13_to_24
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 201 and 300 THEN gross_revenue END),0) As gross_revenue_M25_to_36
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month between 301 and 400 THEN gross_revenue END),0) As gross_revenue_M37_to_48
	,COALESCE(SUM(CASE WHEN revenue_month - Enroll_Month > 400 THEN gross_revenue END),0) As gross_revenue_GT_48

	from temp.`BO_ACCT_MASTER` A
	where currency_type IN ('USD','CAD') and Enroll_Month <= 201401  
       
    group by 1,2,3,4,5

    
   ) T
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;
   SELECT 
	 A.revenue_year as curr_year
	,A.signup_type
	,A.segment
    ,COUNT(DISTINCT A.Account_Id) AS paid_curr_year
    ,COUNT(DISTINCT CASE WHEN B.Account_Id IS NOT NULL THEN A.Account_Id END)  AS paid_curr_and_next_year
    
    from temp.`BO_ACCT_MASTER` A
    LEFT JOIN temp.`BO_ACCT_MASTER` B ON (A.Account_Id=B.Account_Id AND B.revenue_year - A.revenue_year = 1 AND B.revenue_net > 0 )
    where A.currency_type IN ('USD','CAD') AND A.revenue_net > 0  AND A.Enroll_Month <= 201412
    group by 1,2,3


SELECT revenue_year, revenue_month, signup_type,segment, COUNT(Account_Id)
FROM
    (
	SELECT Account_Id, revenue_year,
	signup_type,segment, MIN(revenue_month) AS revenue_month
	FROM temp.`BO_ACCT_SUMMARY` 
	WHERE currency_type IN ('USD','CAD') AND prior_year_revenue_net > 0
	AND ROUND(Enroll_Month / 100) = revenue_year - 1
	GROUP BY 1,2,3,4
    ) T
GROUP BY 1,2,3,4;


SELECT revenue_year,revenue_month,signup_type,segment, 
COUNT(DISTINCT CASE WHEN revenue_net_M13_to_24 > 0 THEN Account_Id ELSE '' END) AS M13_to_24_accounts, 
COUNT(DISTINCT CASE WHEN revenue_net_M1_to_12 > 0 THEN Account_Id ELSE '' END) AS M1_to_12_accounts

FROM temp.`BO_ACCT_ROLLING_SUMMARY`
WHERE currency_type IN ('USD','CAD')
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;


SELECT revenue_year,revenue_month,signup_type,segment,  
SUM(revenue_net_M13_to_24) AS M13_to_24_net_revenue, 
 SUM(revenue_net_M1_to_12) AS M1_to_12_net_reveune, 
 SUM(gross_revenue_M13_to_24) AS M13_to_24_gross_revenue,  
 SUM(gross_revenue_M1_to_12) AS M1_to_12_gross_revenue 
 FROM temp.`BO_ACCT_ROLLING_SUMMARY`
 WHERE currency_type IN ('USD') 
 GROUP BY 1,2,3,4 
 ORDER BY 1,2,3,4;


select T.reference_time_period
,MAX(`local rep sold`) AS `local rep sold`
,COALESCE(MAX(`local self serve`),0) AS `local self serve`
,COALESCE(MAX(`mid market`),0) AS `mid market`
,COALESCE(MAX(`national`),0) AS `national`
,SUM( COALESCE(`local rep sold`,0)  + COALESCE(`local self serve`,0) + COALESCE(`mid market`,0) +  COALESCE(`national`,0)) as `total net revenue`
  FROM
    (
	select ( (revenue_year)*100+revenue_month) as reference_time_period
	 ,CASE WHEN segment='Local' THEN  SUM(COALESCE(revenue_net_M13_to_24,0)) END as `local rep sold`
	 ,CASE WHEN segment='Self-Serve' THEN  SUM(COALESCE(revenue_net_M13_to_24,0)) END as `local self serve`
	 ,CASE WHEN segment='Mid-Market' THEN  SUM(COALESCE(revenue_net_M13_to_24,0)) END as `mid market`
	 ,CASE WHEN segment='National' THEN  SUM(COALESCE(revenue_net_M13_to_24,0)) END as `national`
	 FROM temp.`BO_ACCT_ROLLING_SUMMARY`
	 WHERE currency_type IN ('USD','CAD') 
	 GROUP BY revenue_year,revenue_month,segment
    ) T

 GROUP BY 1
 ORDER BY 1;


select T.reference_time_period
,MAX(`local rep sold`) AS `local rep sold`
,COALESCE(MAX(`local self serve`),0) AS `local self serve`
,COALESCE(MAX(`mid market`),0) AS `mid market`
,COALESCE(MAX(`national`),0) AS `national`
,SUM( COALESCE(`local rep sold`,0)  + COALESCE(`local self serve`,0) + COALESCE(`mid market`,0) +  COALESCE(`national`,0)) as `total net revenue`
  FROM
    (
	select ( (revenue_year)*100+revenue_month) as reference_time_period
	 ,CASE WHEN segment='Local' THEN  SUM(COALESCE(revenue_net_M1_to_12,0)) END as `local rep sold`
	 ,CASE WHEN segment='Self-Serve' THEN  SUM(COALESCE(revenue_net_M1_to_12,0)) END as `local self serve`
	 ,CASE WHEN segment='Mid-Market' THEN  SUM(COALESCE(revenue_net_M1_to_12,0)) END as `mid market`
	 ,CASE WHEN segment='National' THEN  SUM(COALESCE(revenue_net_M1_to_12,0)) END as `national`
	 FROM temp.`BO_ACCT_ROLLING_SUMMARY`
	 WHERE currency_type IN ('USD','CAD') 
	 GROUP BY revenue_year,revenue_month,segment
    ) T

 GROUP BY 1
 ORDER BY 1;




 
select T.reference_time_period
,MAX(`local rep sold`) AS `local rep sold`
,COALESCE(MAX(`local self serve`),0) AS `local self serve`
,COALESCE(MAX(`mid market`),0) AS `mid market`
,COALESCE(MAX(`national`),0) AS `national`
,SUM( COALESCE(`local rep sold`,0)  + COALESCE(`local self serve`,0) + COALESCE(`mid market`,0) +  COALESCE(`national`,0)) as `total net revenue`
  FROM
    (
	select ( (revenue_year)*100+revenue_month) as reference_time_period
	 ,CASE WHEN segment='Local' THEN  SUM(COALESCE(revenue_net_GT_24,0)) END as `local rep sold`
	 ,CASE WHEN segment='Self-Serve' THEN  SUM(COALESCE(revenue_net_GT_24,0)) END as `local self serve`
	 ,CASE WHEN segment='Mid-Market' THEN  SUM(COALESCE(revenue_net_GT_24,0)) END as `mid market`
	 ,CASE WHEN segment='National' THEN  SUM(COALESCE(revenue_net_GT_24,0)) END as `national`
	 FROM temp.`BO_ACCT_ROLLING_SUMMARY`
	 WHERE currency_type IN ('USD','CAD') 
	 GROUP BY revenue_year,revenue_month,segment
    ) T

 GROUP BY 1
 ORDER BY 1;





select T.reference_time_period
,MAX(`local rep sold`) AS `local rep sold`
,COALESCE(MAX(`local self serve`),0) AS `local self serve`
,COALESCE(MAX(`mid market`),0) AS `mid market`
,COALESCE(MAX(`national`),0) AS `national`
,SUM( COALESCE(`local rep sold`,0)  + COALESCE(`local self serve`,0) + COALESCE(`mid market`,0) +  COALESCE(`national`,0)) as `total net revenue`
  FROM
    (
	select revenue_month as reference_time_period
	 ,CASE WHEN segment='Local' THEN  COUNT(DISTINCT Account_Id) END as `local rep sold`
	 ,CASE WHEN segment='Self-Serve' THEN  COUNT(DISTINCT Account_Id) END as `local self serve`
	 ,CASE WHEN segment='Mid-Market' THEN  COUNT(DISTINCT Account_Id) END as `mid market`
	 ,CASE WHEN segment='National' THEN  COUNT(DISTINCT Account_Id) END as `national`
	FROM temp.`BO_ACCT_MASTER` A
	WHERE currency_type IN ('USD','CAD') and Enroll_Month <= 201311  
	AND revenue_month - Enroll_Month >= 14
	GROUP BY 1,2,3;
    ) T

 GROUP BY 1
 ORDER BY 1;


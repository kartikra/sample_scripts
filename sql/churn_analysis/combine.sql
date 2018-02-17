
SELECT 2014,signup_type,segment
,SUM(revenue_net_2015_total) AS net_current_year, SUM(revenue_net_2014_total) AS net_previous_year
,SUM(gross_revenue_2015_total) AS gross_current_year, SUM(gross_revenue_2014_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201401 AND 201412
GROUP BY 1,2,3
UNION
SELECT 2013, signup_type,segment,SUM(revenue_net_2014_total) AS current_year, SUM(revenue_net_2013_total) AS previous_year
,SUM(gross_revenue_2014_total) AS gross_current_year, SUM(gross_revenue_2013_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201301 AND 201312
GROUP BY 1,2,3
UNION
SELECT 2012, signup_type,segment, SUM(revenue_net_2013_total) AS current_year, SUM(revenue_net_2012_total) AS previous_year
,SUM(gross_revenue_2013_total) AS gross_current_year, SUM(gross_revenue_2012_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201201 AND 201212
GROUP BY 1,2,3
UNION
SELECT 2011,signup_type,segment, SUM(revenue_net_2012_total) AS current_year, SUM(revenue_net_2011_total) AS previous_year
,SUM(gross_revenue_2012_total) AS gross_current_year, SUM(gross_revenue_2011_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201101 AND 201112
GROUP BY 1,2,3
UNION
SELECT 2010,signup_type,segment, SUM(revenue_net_2011_total) AS current_year, SUM(revenue_net_2010_total) AS previous_year
,SUM(gross_revenue_2011_total) AS gross_current_year, SUM(gross_revenue_2010_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201001 AND 201012
GROUP BY 1,2,3
UNION
SELECT 2009,signup_type,segment, SUM(revenue_net_2010_total) AS current_year, SUM(revenue_net_2009_total) AS previous_year
,SUM(gross_revenue_2010_total) AS gross_current_year, SUM(gross_revenue_2009_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 200901 AND 200912
GROUP BY 1,2,3
UNION
SELECT 2008,signup_type,segment, SUM(revenue_net_2009_total) AS current_year, SUM(revenue_net_2008_total) AS previous_year
,SUM(gross_revenue_2009_total) AS gross_current_year, SUM(gross_revenue_2008_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 200801 AND 200812
GROUP BY 1,2,3
;




SELECT 2014,signup_type,segment
,AVG(revenue_net_2015_total/revenue_months_2015_total) AS net_current_year, AVG(revenue_net_2014_total/revenue_months_2014_total) AS net_previous_year
,AVG(gross_revenue_2015_total/revenue_months_2015_total) AS gross_current_year, AVG(gross_revenue_2014_total/revenue_months_2014_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201401 AND 201412
GROUP BY 1,2,3
UNION
SELECT 2013, signup_type,segment,AVG(revenue_net_2014_total/revenue_months_2014_total) AS current_year, AVG(revenue_net_2013_total/revenue_months_2013_total) AS previous_year
,AVG(gross_revenue_2014_total/revenue_months_2014_total) AS gross_current_year, AVG(gross_revenue_2013_total/revenue_months_2013_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201301 AND 201312
GROUP BY 1,2,3
UNION
SELECT 2012, signup_type,segment, AVG(revenue_net_2013_total/revenue_months_2013_total) AS current_year, AVG(revenue_net_2012_total/revenue_months_2012_total) AS previous_year
,AVG(gross_revenue_2013_total/revenue_months_2013_total) AS gross_current_year, AVG(gross_revenue_2012_total/revenue_months_2012_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201201 AND 201212
GROUP BY 1,2,3
UNION
SELECT 2011,signup_type,segment, AVG(revenue_net_2012_total/revenue_months_2012_total) AS current_year, AVG(revenue_net_2011_total/revenue_months_2011_total) AS previous_year
,AVG(gross_revenue_2012_total/revenue_months_2012_total) AS gross_current_year, AVG(gross_revenue_2011_total/revenue_months_2011_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201101 AND 201112
GROUP BY 1,2,3
UNION
SELECT 2010,signup_type,segment, AVG(revenue_net_2011_total/revenue_months_2011_total) AS current_year, AVG(revenue_net_2010_total/revenue_months_2010_total) AS previous_year
,AVG(gross_revenue_2011_total/revenue_months_2011_total) AS gross_current_year, AVG(gross_revenue_2010_total/revenue_months_2010_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 201001 AND 201012
GROUP BY 1,2,3
UNION
SELECT 2009,signup_type,segment, AVG(revenue_net_2010_total/revenue_months_2010_total) AS current_year, AVG(revenue_net_2009_total/revenue_months_2009_total) AS previous_year
,AVG(gross_revenue_2010_total/revenue_months_2010_total) AS gross_current_year, AVG(gross_revenue_2009_total/revenue_months_2009_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 200901 AND 200912
GROUP BY 1,2,3
UNION
SELECT 2008,signup_type,segment, AVG(revenue_net_2009_total/revenue_months_2009_total) AS current_year, AVG(revenue_net_2008_total/revenue_months_2008_total) AS previous_year
,AVG(gross_revenue_2009_total/revenue_months_2009_total) AS gross_current_year, AVG(gross_revenue_2008_total/revenue_months_2008_total) AS gross_previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND Enroll_Month BETWEEN 200801 AND 200812
GROUP BY 1,2,3
;



SELECT 2014,signup_type,segment, SUM(revenue_net_2015_total) AS current_year, SUM(revenue_net_2014_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2014_total > 0
GROUP BY 1,2,3
UNION
SELECT 2013, signup_type,segment,SUM(revenue_net_2014_total) AS current_year, SUM(revenue_net_2013_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2013_total > 0
GROUP BY 1,2,3
UNION
SELECT 2012, signup_type,segment, SUM(revenue_net_2013_total) AS current_year, SUM(revenue_net_2012_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2012_total > 0
GROUP BY 1,2,3
UNION
SELECT 2011,signup_type,segment, SUM(revenue_net_2012_total) AS current_year, SUM(revenue_net_2011_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2011_total > 0
GROUP BY 1,2,3
UNION
SELECT 2010,signup_type,segment, SUM(revenue_net_2011_total) AS current_year, SUM(revenue_net_2010_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2010_total > 0
GROUP BY 1,2,3
UNION
SELECT 2009,signup_type,segment, SUM(revenue_net_2010_total) AS current_year, SUM(revenue_net_2009_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2009_total > 0
GROUP BY 1,2,3
UNION
SELECT 2008,signup_type,segment, SUM(revenue_net_2009_total) AS current_year, SUM(revenue_net_2008_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2008_total > 0
GROUP BY 1,2,3
;



SELECT 2014,signup_type,segment, SUM(revenue_net_2015_total) AS current_year, SUM(revenue_net_2014_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_months_2014_total = 12
GROUP BY 1,2,3
UNION
SELECT 2013, signup_type,segment,SUM(revenue_net_2014_total) AS current_year, SUM(revenue_net_2013_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_months_2013_total = 12
GROUP BY 1,2,3
UNION
SELECT 2012, signup_type,segment, SUM(revenue_net_2013_total) AS current_year, SUM(revenue_net_2012_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_months_2012_total = 12
GROUP BY 1,2,3
UNION
SELECT 2011,signup_type,segment, SUM(revenue_net_2012_total) AS current_year, SUM(revenue_net_2011_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_months_2011_total = 12
GROUP BY 1,2,3
UNION
SELECT 2010,signup_type,segment, SUM(revenue_net_2011_total) AS current_year, SUM(revenue_net_2010_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_months_2010_total = 12
GROUP BY 1,2,3
UNION
SELECT 2009,signup_type,segment, SUM(revenue_net_2010_total) AS current_year, SUM(revenue_net_2009_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_months_2009_total = 12
GROUP BY 1,2,3
UNION
SELECT 2008,signup_type,segment, SUM(revenue_net_2009_total) AS current_year, SUM(revenue_net_2008_total) AS previous_year
FROM temp.`BO_ACCT_SUMMARY` 
WHERE currency_type IN ('USD','CAD') AND revenue_net_2008_total = 12
GROUP BY 1,2,3
;

CREATE TABLE temp.`BO_ACCT_SUMMARY` (
   `account_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `Enroll_Month` int(11) DEFAULT NULL,
   `revenue_months_2008_total` int(11) DEFAULT NULL,
   `revenue_months_2009_total` int(11) DEFAULT NULL,
   `revenue_months_2010_total` int(11) DEFAULT NULL,
   `revenue_months_2011_total` int(11) DEFAULT NULL,
   `revenue_months_2012_total` int(11) DEFAULT NULL,
   `revenue_months_2013_total` int(11) DEFAULT NULL,
   `revenue_months_2014_total` int(11) DEFAULT NULL,
   `revenue_months_2015_total` int(11) DEFAULT NULL,
   `gross_revenue_2008_total` double DEFAULT NULL,
   `gross_revenue_2009_total` double DEFAULT NULL,
   `gross_revenue_2010_total` double DEFAULT NULL,
   `gross_revenue_2011_total` double DEFAULT NULL,
   `gross_revenue_2012_total` double DEFAULT NULL,
   `gross_revenue_2013_total` double DEFAULT NULL,
   `gross_revenue_2014_total` double DEFAULT NULL,
   `gross_revenue_2015_total` double DEFAULT NULL,
   `revenue_net_2008_total` double DEFAULT NULL,
   `revenue_net_2009_total` double DEFAULT NULL,
   `revenue_net_2010_total` double DEFAULT NULL,
   `revenue_net_2011_total` double DEFAULT NULL,
   `revenue_net_2012_total` double DEFAULT NULL,
   `revenue_net_2013_total` double DEFAULT NULL,
   `revenue_net_2014_total` double DEFAULT NULL,
   `revenue_net_2015_total` double DEFAULT NULL,

   PRIMARY KEY (`account_id`,`currency_type`,`signup_type`,`segment`),
   KEY `Index1` (`Enroll_Month`),
   KEY `Index2` (`signup_type`),
   KEY `Index3` (`segment`),
   KEY `Index4` (`account_id`),
   KEY `Index5` (`currency_type`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;
    

INSERT INTO temp.`BO_ACCT_SUMMARY`    
    SELECT 
	 A.Account_Id
	,A.currency_type
	,A.signup_type
	,A.segment
	,A.Enroll_Month

	,COALESCE(MAX(CASE WHEN revenue_year=2008 THEN total_calendar_months END),0) As revenue_months_2008_total
	,COALESCE(MAX(CASE WHEN revenue_year=2009 THEN total_calendar_months END),0) As revenue_months_2009_total
	,COALESCE(MAX(CASE WHEN revenue_year=2010 THEN total_calendar_months END),0) As revenue_months_2010_total
	,COALESCE(MAX(CASE WHEN revenue_year=2011 THEN total_calendar_months END),0) As revenue_months_2011_total
	,COALESCE(MAX(CASE WHEN revenue_year=2012 THEN total_calendar_months END),0) As revenue_months_2012_total
	,COALESCE(MAX(CASE WHEN revenue_year=2013 THEN total_calendar_months END),0) As revenue_months_2013_total
	,COALESCE(MAX(CASE WHEN revenue_year=2014 THEN total_calendar_months END),0) As revenue_months_2014_total
	,COALESCE(MAX(CASE WHEN revenue_year=2015 THEN total_calendar_months END),0) As revenue_months_2015_total

	,COALESCE(SUM(CASE WHEN revenue_year=2008 THEN gross_revenue END),0) As gross_revenue_2008_total
	,COALESCE(SUM(CASE WHEN revenue_year=2009 THEN gross_revenue END),0) As gross_revenue_2009_total
	,COALESCE(SUM(CASE WHEN revenue_year=2010 THEN gross_revenue END),0) As gross_revenue_2010_total
	,COALESCE(SUM(CASE WHEN revenue_year=2011 THEN gross_revenue END),0) As gross_revenue_2011_total
	,COALESCE(SUM(CASE WHEN revenue_year=2012 THEN gross_revenue END),0) As gross_revenue_2012_total
	,COALESCE(SUM(CASE WHEN revenue_year=2013 THEN gross_revenue END),0) As gross_revenue_2013_total
	,COALESCE(SUM(CASE WHEN revenue_year=2014 THEN gross_revenue END),0) As gross_revenue_2014_total
	,COALESCE(SUM(CASE WHEN revenue_year=2015 THEN gross_revenue END),0) As gross_revenue_2015_total

	,COALESCE(SUM(CASE WHEN revenue_year=2008 THEN revenue_net END),0) As revenue_net_2008_total
	,COALESCE(SUM(CASE WHEN revenue_year=2009 THEN revenue_net END),0) As revenue_net_2009_total
	,COALESCE(SUM(CASE WHEN revenue_year=2010 THEN revenue_net END),0) As revenue_net_2010_total
	,COALESCE(SUM(CASE WHEN revenue_year=2011 THEN revenue_net END),0) As revenue_net_2011_total
	,COALESCE(SUM(CASE WHEN revenue_year=2012 THEN revenue_net END),0) As revenue_net_2012_total
	,COALESCE(SUM(CASE WHEN revenue_year=2013 THEN revenue_net END),0) As revenue_net_2013_total
	,COALESCE(SUM(CASE WHEN revenue_year=2014 THEN revenue_net END),0) As revenue_net_2014_total
	,COALESCE(SUM(CASE WHEN revenue_year=2015 THEN revenue_net END),0) As revenue_net_2015_total

     FROM temp.`BO_ACCT_MASTER` A    

    GROUP BY 1,2,3,4,5;


ANALYZE TABLE temp.`BO_ACCT_SUMMARY`;

